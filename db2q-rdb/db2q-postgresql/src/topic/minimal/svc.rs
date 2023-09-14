use std::time::SystemTime;

use futures_util::stream::{StreamExt, TryStreamExt};

use tonic::{Request, Response, Status};
use tonic_types::{ErrorDetails, StatusExt};

use deadpool::managed::PoolError;
use deadpool_postgres::tokio_postgres;
use deadpool_postgres::{Client, GenericClient, Pool};
use tokio_postgres::Row;

use db2q::uuid::Uuid;

use db2q::topic::cmd::create::CreateReq;
use db2q::topic::cmd::drop::DropReq;
use db2q::topic::cmd::list::ListReq;

use db2q::db2q::proto::queue::v1::Uuid as Guid;

use db2q::db2q::proto::queue::v1::topic_service_server::TopicService;
use db2q::db2q::proto::queue::v1::topic_svc::{CreateRequest, CreateResponse};
use db2q::db2q::proto::queue::v1::topic_svc::{DropRequest, DropResponse};
use db2q::db2q::proto::queue::v1::topic_svc::{ListRequest, ListResponse};

use crate::topic::minimal::topic2table::TopicConv;

pub struct Svc<T> {
    pool: Pool,
    topic_conv: T,
}

impl<T> Svc<T>
where
    T: TopicConv,
{
    async fn get_client(&self) -> Result<Client, Status> {
        match self.pool.get().await {
            Ok(client) => Ok(client),
            Err(PoolError::Timeout(t)) => Err(Status::unavailable(format!("timeout: {t:#?}"))),
            Err(PoolError::Closed) => Err(Status::failed_precondition("All connection closed")),
            Err(e) => Err(Status::internal(format!("Unexpected error: {e}"))),
        }
    }

    async fn create<C>(&self, checked_name: &str, client: &C) -> Result<u64, Status>
    where
        C: GenericClient,
    {
        let query = format!(
            r#"
                CREATE TABLE {checked_name} (
                    key BIGSERIAL PRIMARY KEY,
                    val BYTEA NOT NULL
                )
            "#
        );
        client
            .execute(&query, &[])
            .await
            .map_err(|e| match e.is_closed() {
                true => Status::unavailable(format!("connection closed: {e}")),
                _ => Status::internal(format!("Unexpected error: {e}")),
            })
    }

    async fn drop<C>(&self, checked_name: &str, client: &C) -> Result<u64, Status>
    where
        C: GenericClient,
    {
        let query = format!(
            r#"
                DROP TABLE IF EXISTS {checked_name}
            "#
        );
        client
            .execute(&query, &[])
            .await
            .map_err(|e| match e.is_closed() {
                true => Status::unavailable(format!("connection closed: {e}")),
                _ => Status::internal(format!("Unexpected error: {e}")),
            })
    }

    async fn list<C>(&self, client: &C) -> Result<Vec<Guid>, Status>
    where
        C: GenericClient,
    {
        let query: &str = r#"
            SELECT
                table_name
            FROM information_schema.tables
            WHERE table_schema='public'
            ORDER BY table_name
        "#;
        let params: Vec<String> = vec![];
        let row_stream =
            client
                .query_raw(query, params)
                .await
                .map_err(|e| match e.is_closed() {
                    true => Status::unavailable(format!("connection closed: {e}")),
                    false => Status::internal(format!("Unexpected error: {e}")),
                })?;
        let mapd = row_stream
            .map(|r: Result<Row, _>| {
                r.map_err(|e| match e.is_closed() {
                    true => Status::unavailable(format!("connection closed: {e}")),
                    false => Status::internal(format!("Unable to get a row: {e}")),
                })
            })
            .map(|r: Result<Row, Status>| {
                r.and_then(|row: Row| {
                    let name: &str = row.try_get(0).map_err(|e| {
                        Status::internal(format!("Unable to get a table name: {e}"))
                    })?;
                    let topic: Uuid = self.topic_conv.name2id(name)?;
                    let g: Guid = topic.into();
                    Ok(g)
                })
            });
        let topics: Vec<Guid> = mapd.try_collect().await?;
        Ok(topics)
    }
}

#[tonic::async_trait]
impl<T> TopicService for Svc<T>
where
    T: Send + Sync + 'static + TopicConv,
{
    async fn create(
        &self,
        req: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, Status> {
        let cr: CreateRequest = req.into_inner();
        let checked: CreateReq = (&cr).try_into()?;
        let topic_id: Uuid = checked.as_topic_id();
        let name: String = self.topic_conv.id2name(topic_id);
        let client: Client = self.get_client().await?;
        self.create(name.as_str(), &client).await?;
        let created: SystemTime = SystemTime::now();
        let reply = CreateResponse {
            created: Some(created.into()),
        };
        Ok(Response::new(reply))
    }

    async fn drop(&self, req: Request<DropRequest>) -> Result<Response<DropResponse>, Status> {
        let cr: DropRequest = req.into_inner();
        let checked: DropReq = (&cr).try_into()?;
        let topic_id: Uuid = checked.as_topic_id();
        let name: String = self.topic_conv.id2name(topic_id);
        let client: Client = self.get_client().await?;
        self.drop(name.as_str(), &client).await?;
        let dropped: SystemTime = SystemTime::now();
        let reply = DropResponse {
            dropped: Some(dropped.into()),
        };
        Ok(Response::new(reply))
    }

    async fn list(&self, req: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        let lr: ListRequest = req.into_inner();
        let checked: ListReq = (&lr).try_into()?;
        let reqid: Uuid = checked.as_request_id();
        let client: Client = self.get_client().await?;
        let topics: Vec<Guid> = self.list(&client).await.map_err(|e: Status| {
            Status::with_error_details(
                e.code(),
                e.message(),
                ErrorDetails::with_request_info(reqid.to_string(), ""),
            )
        })?;
        let reply = ListResponse { topics };
        Ok(Response::new(reply))
    }
}

pub fn topic_svc_new<T>(pool: &Pool, topic_conv: T) -> impl TopicService
where
    T: Send + Sync + 'static + TopicConv,
{
    Svc {
        pool: pool.clone(),
        topic_conv,
    }
}
