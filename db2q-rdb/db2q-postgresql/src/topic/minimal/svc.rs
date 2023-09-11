use std::time::SystemTime;

use tonic::{Request, Response, Status};

use deadpool::managed::PoolError;
use deadpool_postgres::{Client, GenericClient, Pool};

use db2q::topic::cmd::create::CreateReq;
use db2q::topic::cmd::drop::DropReq;
use db2q::uuid::Uuid;

use db2q::db2q::proto::queue::v1::topic_service_server::TopicService;
use db2q::db2q::proto::queue::v1::topic_svc::{CreateRequest, CreateResponse};
use db2q::db2q::proto::queue::v1::topic_svc::{DropRequest, DropResponse};

use crate::topic::minimal::topic2table::Topic2Table;

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
}

#[tonic::async_trait]
impl<T> TopicService for Svc<T>
where
    T: Send + Sync + 'static + Topic2Table,
{
    async fn create(
        &self,
        req: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, Status> {
        let cr: CreateRequest = req.into_inner();
        let checked: CreateReq = (&cr).try_into()?;
        let topic_id: Uuid = checked.as_topic_id();
        let name: String = self.topic2table.id2name(topic_id);
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
        let name: String = self.topic2table.id2name(topic_id);
        let client: Client = self.get_client().await?;
        self.drop(name.as_str(), &client).await?;
        let dropped: SystemTime = SystemTime::now();
        let reply = DropResponse {
            dropped: Some(dropped.into()),
        };
        Ok(Response::new(reply))
    }
}

pub fn topic_svc_new<T>(pool: &Pool, topic2table: T) -> impl TopicService
where
    T: Send + Sync + 'static + Topic2Table,
{
    Svc {
        pool: pool.clone(),
        topic2table,
    }
}
