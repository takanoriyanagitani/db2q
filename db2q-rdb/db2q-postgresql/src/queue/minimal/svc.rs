use core::time::Duration;
use std::time::{Instant, SystemTime};
use tokio::sync::mpsc;
use tokio::time::Interval;

use futures_util::{StreamExt, TryStreamExt};

use tokio_stream::wrappers::ReceiverStream;

use tonic::{Code, Request, Response, Status};

use deadpool::managed::PoolError;
use deadpool_postgres::tokio_postgres;
use deadpool_postgres::{Client, GenericClient, Pool};
use tokio_postgres::{Row, RowStream};

use db2q::queue::cmd::count::CountReq;
use db2q::queue::cmd::keys::KeysReq;
use db2q::queue::cmd::next::NextReq;
use db2q::queue::cmd::push::PushBackReq;
use db2q::queue::cmd::wait_next::WaitNextReq;
use db2q::uuid::Uuid;

use db2q::db2q::proto::queue::v1::q_svc::{CountRequest, CountResponse};
use db2q::db2q::proto::queue::v1::q_svc::{KeysRequest, KeysResponse};
use db2q::db2q::proto::queue::v1::q_svc::{NextRequest, NextResponse};
use db2q::db2q::proto::queue::v1::q_svc::{PopFrontRequest, PopFrontResponse};
use db2q::db2q::proto::queue::v1::q_svc::{PushBackRequest, PushBackResponse};
use db2q::db2q::proto::queue::v1::q_svc::{WaitNextRequest, WaitNextResponse};
use db2q::db2q::proto::queue::v1::queue_service_server::QueueService;

use super::topic2table::Topic2Table;

pub struct Svc<T> {
    pool: Pool,
    topic2table: T,
}

impl<T> Svc<T>
where
    T: Send + Sync + 'static,
{
    async fn get_client(&self) -> Result<Client, Status> {
        Self::pool2client(&self.pool).await
    }

    async fn pool2client(pool: &Pool) -> Result<Client, Status> {
        match pool.get().await {
            Ok(client) => Ok(client),
            Err(PoolError::Timeout(t)) => Err(Status::unavailable(format!("timeout: {t:#?}"))),
            Err(PoolError::Closed) => Err(Status::failed_precondition("All connection closed")),
            Err(e) => Err(Status::internal(format!("Unexpected error: {e}"))),
        }
    }

    async fn push<C>(&self, checked_name: &str, client: &C, val: &[u8]) -> Result<u64, Status>
    where
        C: GenericClient,
    {
        let query = format!(
            r#"
                INSERT INTO {checked_name} (
                    val
                )
                VALUES ($1::BYTEA)
            "#
        );
        client
            .execute(&query, &[&val])
            .await
            .map_err(|e| match e.is_closed() {
                true => Status::unavailable(format!("connection closed: {e}")),
                _ => Status::internal(format!("Unable to insert: {e}")),
            })
    }

    async fn count<C>(&self, checked_name: &str, client: &C) -> Result<u64, Status>
    where
        C: GenericClient,
    {
        let query = format!(
            r#"
                SELECT
                    COUNT(*) AS cnt
                FROM {checked_name}
            "#
        );
        let row = client
            .query_one(&query, &[])
            .await
            .map_err(|e| match e.is_closed() {
                true => Status::unavailable(format!("connection closed: {e}")),
                _ => Status::internal(format!("Unable to count: {e}")),
            })?;
        let cnt: i64 = row
            .try_get(0)
            .map_err(|e| Status::internal(format!("No column got: {e}")))?;
        Ok(cnt as u64)
    }

    async fn next<C>(checked_name: &str, prev: i64, client: &C) -> Result<(i64, Vec<u8>), Status>
    where
        C: GenericClient,
    {
        let query = format!(
            r#"
                SELECT
                    key::BIGINT,
                    val::BYTEA
                FROM {checked_name}
                WHERE key > $1::BIGINT
                ORDER BY key
                LIMIT 1
            "#
        );
        let row = client
            .query_opt(&query, &[&prev])
            .await
            .map_err(|e| match e.is_closed() {
                true => Status::unavailable(format!("connection closed: {e}")),
                _ => Status::internal(format!("Unable to select: {e}")),
            })?
            .ok_or_else(|| {
                Status::not_found(format!("No more queue items. previous key: {prev}"))
            })?;
        let next_key: i64 = row
            .try_get(0)
            .map_err(|e| Status::internal(format!("Unable to get a key: {e}")))?;
        let next_val: Vec<u8> = row
            .try_get(1)
            .map_err(|e| Status::internal(format!("Unable to get a value: {e}")))?;
        Ok((next_key, next_val))
    }

    pub async fn wait_next(
        &self,
        checked_name: &str,
        req: WaitNextReq,
    ) -> Result<ReceiverStream<Result<WaitNextResponse, Status>>, Status> {
        let prev: i64 = req.as_previous_key().map(|u| u as i64).unwrap_or(-1);
        let start: Instant = Instant::now();
        let mut i: Interval = tokio::time::interval(req.as_interval());
        let (tx, rx) = mpsc::channel(1);
        let name: String = checked_name.into();
        let pool: Pool = self.pool.clone();
        let timeout: Duration = req.as_timeout();
        tokio::spawn(async move {
            let mut retry_cnt: u64 = 0;
            match Self::pool2client(&pool).await {
                Ok(client) => loop {
                    let check: Duration = start.elapsed();
                    match check < timeout {
                        true => {}
                        false => {
                            let e = Status::deadline_exceeded(format!(
                                "timeout. table={name}, retried={retry_cnt}"
                            ));
                            match tx.send(Err(e)).await {
                                Ok(_) => {}
                                Err(e) => log::warn!("Unable to send: {e}"),
                            }
                            return;
                        }
                    }
                    i.tick().await; // 1st tick has 0 latency
                    match Self::next(name.as_str(), prev, &client).await {
                        Ok(t) => {
                            let elapsed: Duration = start.elapsed();
                            let (i, v) = t;
                            let reply = WaitNextResponse {
                                next: Some(NextResponse { next: i, value: v }),
                                elapsed: elapsed.try_into().ok(),
                                retried: retry_cnt,
                            };
                            match tx.send(Ok(reply)).await {
                                Ok(_) => {}
                                Err(e) => log::warn!("Unable to send: {e}"),
                            };
                            return;
                        }
                        Err(e) => match e.code() {
                            Code::NotFound => {
                                retry_cnt += 1;
                                continue;
                            }
                            _ => {
                                match tx.send(Err(e)).await {
                                    Ok(_) => {}
                                    Err(e) => log::warn!("Unable to send: {e}"),
                                }
                                return;
                            }
                        },
                    }
                },
                Err(e) => match tx.send(Err(e)).await {
                    Ok(_) => {}
                    Err(e) => {
                        log::warn!("Unable to send: {e}");
                    }
                },
            }
        });
        Ok(ReceiverStream::new(rx))
    }

    async fn first<C>(&self, checked_name: &str, client: &C) -> Result<(i64, Vec<u8>), Status>
    where
        C: GenericClient,
    {
        let query = format!(
            r#"
                SELECT
                    key::BIGINT,
                    val::BYTEA
                FROM {checked_name}
                ORDER BY key
                LIMIT 1
            "#
        );
        let row = client
            .query_opt(&query, &[])
            .await
            .map_err(|e| match e.is_closed() {
                true => Status::unavailable(format!("connection closed: {e}")),
                _ => Status::internal(format!("Unable to select: {e}")),
            })?
            .ok_or_else(|| Status::not_found("Empty queue"))?;
        let next_key: i64 = row
            .try_get(0)
            .map_err(|e| Status::internal(format!("Unable to get a key: {e}")))?;
        let next_val: Vec<u8> = row
            .try_get(1)
            .map_err(|e| Status::internal(format!("Unable to get a value: {e}")))?;
        Ok((next_key, next_val))
    }

    pub async fn keys<C>(
        &self,
        checked_name: &str,
        client: &C,
        limit: u64,
    ) -> Result<ReceiverStream<Result<KeysResponse, Status>>, Status>
    where
        C: GenericClient,
    {
        let query = format!(
            r#"
                SELECT
                    key::BIGINT
                FROM {checked_name}
                ORDER BY key
                LIMIT {limit}
            "#
        );
        let empty_params: Vec<i8> = vec![];
        let row_stream: RowStream =
            client
                .query_raw(&query, &empty_params)
                .await
                .map_err(|e| match e.is_closed() {
                    true => Status::unavailable(format!("connection closed: {e}")),
                    _ => Status::internal(format!("Unable to get keys: {e}")),
                })?;
        let keys_stream = row_stream.map(|r: Result<_, _>| {
            r.and_then(|row: Row| {
                let key: i64 = row.try_get(0)?;
                Ok(key)
            })
            .map_err(|e| Status::internal(format!("Unable to get a key: {e}")))
            .map(|key: i64| KeysResponse { key: key as u64 })
        });

        let (tx, rx) = mpsc::channel(1);

        tokio::spawn(async move {
            let t = &tx;
            let rslt: Result<_, _> = keys_stream
                .try_for_each(|key: KeysResponse| async move {
                    t.send(Ok(key))
                        .await
                        .map_err(|e| Status::cancelled(format!("Unable to send: {e}")))
                })
                .await;
            match rslt {
                Ok(_) => {}
                Err(e) => log::warn!("Error while sending keys: {e}"),
            }
        });
        Ok(ReceiverStream::new(rx))
    }
}

#[tonic::async_trait]
impl<T> QueueService for Svc<T>
where
    T: Send + Sync + 'static + Topic2Table,
{
    async fn push_back(
        &self,
        req: Request<PushBackRequest>,
    ) -> Result<Response<PushBackResponse>, Status> {
        let pbr: PushBackRequest = req.into_inner();
        let checked: PushBackReq = pbr.try_into()?;
        let topic_id: Uuid = checked.as_topic_id();
        let name: String = self.topic2table.id2name(topic_id);
        let value: &[u8] = checked.as_value();
        let client: Client = self.get_client().await?;
        self.push(&name, &client, value).await?;
        let pushed: SystemTime = SystemTime::now();
        let reply = PushBackResponse {
            pushed: Some(pushed.into()),
        };
        Ok(Response::new(reply))
    }

    async fn pop_front(
        &self,
        _req: Request<PopFrontRequest>,
    ) -> Result<Response<PopFrontResponse>, Status> {
        Err(Status::unimplemented(
            "No plan to pop(delete) a queue(row) from a table for now",
        ))
    }

    async fn count(&self, req: Request<CountRequest>) -> Result<Response<CountResponse>, Status> {
        let cr: CountRequest = req.into_inner();
        let checked: CountReq = cr.try_into()?;
        let topic_id: Uuid = checked.as_topic_id();
        let name: String = self.topic2table.id2name(topic_id);
        let client: Client = self.get_client().await?;
        let cnt: u64 = self.count(&name, &client).await?;
        let reply = CountResponse { count: cnt };
        Ok(Response::new(reply))
    }

    async fn next(&self, req: Request<NextRequest>) -> Result<Response<NextResponse>, Status> {
        let nr: NextRequest = req.into_inner();
        let checked: NextReq = (&nr).try_into()?;
        let topic_id: Uuid = checked.as_topic_id();
        let name: String = self.topic2table.id2name(topic_id);
        let client: Client = self.get_client().await?;
        let prev_key: Option<u64> = checked.as_previous_key();
        let (next_key, next_val) = match prev_key {
            None => self.first(name.as_str(), &client).await,
            Some(prev) => Self::next(name.as_str(), prev as i64, &client).await,
        }?;
        let reply = NextResponse {
            next: next_key,
            value: next_val,
        };
        Ok(Response::new(reply))
    }

    type WaitNextStream = ReceiverStream<Result<WaitNextResponse, Status>>;

    async fn wait_next(
        &self,
        req: Request<WaitNextRequest>,
    ) -> Result<Response<Self::WaitNextStream>, Status> {
        let wnr: WaitNextRequest = req.into_inner();
        let checked: WaitNextReq = (&wnr).try_into()?;
        let topic_id: Uuid = checked.as_topic_id();
        let name: String = self.topic2table.id2name(topic_id);
        let reply: Self::WaitNextStream = self.wait_next(name.as_str(), checked).await?;
        Ok(Response::new(reply))
    }

    type KeysStream = ReceiverStream<Result<KeysResponse, Status>>;

    async fn keys(&self, req: Request<KeysRequest>) -> Result<Response<Self::KeysStream>, Status> {
        let kr: KeysRequest = req.into_inner();
        let checked: KeysReq = (&kr).try_into()?;
        let topic_id: Uuid = checked.as_topic_id();
        let name: String = self.topic2table.id2name(topic_id);
        let client: Client = self.get_client().await?;
        let keys_max: u64 = checked.as_max_keys();
        let reply: Self::KeysStream = self.keys(name.as_str(), &client, keys_max).await?;
        Ok(Response::new(reply))
    }
}

pub fn queue_svc_new<T>(pool: &Pool, topic2table: T) -> impl QueueService
where
    T: Send + Sync + 'static + Topic2Table,
{
    Svc {
        pool: pool.clone(),
        topic2table,
    }
}
