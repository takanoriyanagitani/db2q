use tonic::{Request, Response, Status};

use deadpool::managed::PoolError;
use deadpool_postgres::{Client, GenericClient, Pool};

use db2q::uuid::Uuid;

use db2q::count::cmd::exact::ExactReq;
use db2q::db2q::proto::queue::v1::cnt_svc::{ExactRequest, ExactResponse};
use db2q::db2q::proto::queue::v1::count_service_server::CountService;

use crate::common::minimal::topic2table::Topic2Table;

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

    pub async fn count<C>(&self, checked_name: &str, client: &C) -> Result<u64, Status>
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
                _ => Status::internal(format!("Unable to insert: {e}")),
            })?;
        let cnt: i64 = row
            .try_get(0)
            .map_err(|e| Status::internal(format!("No column got: {e}")))?;
        Ok(cnt as u64)
    }
}

#[tonic::async_trait]
impl<T> CountService for Svc<T>
where
    T: Send + Sync + 'static + Topic2Table,
{
    async fn exact(&self, req: Request<ExactRequest>) -> Result<Response<ExactResponse>, Status> {
        let er: ExactRequest = req.into_inner();
        let checked: ExactReq = (&er).try_into()?;
        let topic_id: Uuid = checked.as_topic();
        let name: String = self.topic2table.id2name(topic_id);
        let client: Client = self.get_client().await?;
        let cnt: u64 = self.count(name.as_str(), &client).await?;
        let reply = ExactResponse { count: cnt };
        Ok(Response::new(reply))
    }
}

pub fn count_svc_new<T>(pool: &Pool, topic2table: T) -> impl CountService
where
    T: Send + Sync + 'static + Topic2Table,
{
    Svc {
        pool: pool.clone(),
        topic2table,
    }
}
