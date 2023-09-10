use tonic::{Request, Response, Status};

use deadpool_postgres::Pool;

use db2q::topic::cmd::create::CreateReq;
use db2q::uuid::Uuid;

use db2q::db2q::proto::queue::v1::topic_service_server::TopicService;
use db2q::db2q::proto::queue::v1::topic_svc::{CreateRequest, CreateResponse};

use crate::topic::minimal::topic2table::Topic2Table;

pub struct Svc<T> {
    pool: Pool,
    topic2table: T,
}

impl<T> Svc<T>
{
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
        todo!()
    }
}
