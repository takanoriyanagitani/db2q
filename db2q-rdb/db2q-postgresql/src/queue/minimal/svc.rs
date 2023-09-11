use std::time::SystemTime;

use tonic::{Request, Response, Status};

use deadpool::managed::PoolError;
use deadpool_postgres::{Client, GenericClient, Pool};

use db2q::queue::cmd::push::PushBackReq;
use db2q::uuid::Uuid;

use db2q::db2q::proto::queue::v1::q_svc::{PopFrontRequest, PopFrontResponse};
use db2q::db2q::proto::queue::v1::q_svc::{PushBackRequest, PushBackResponse};
use db2q::db2q::proto::queue::v1::queue_service_server::QueueService;

use super::topic2table::Topic2Table;

pub struct Svc<T> {
    pool: Pool,
    topic2table: T,
}

impl<T> Svc<T> {
    async fn get_client(&self) -> Result<Client, Status> {
        match self.pool.get().await {
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
