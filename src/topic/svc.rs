use core::ops::Deref;

use tonic::{Request, Response, Status};

use crate::db2q::proto::queue::v1::topic_service_server::TopicService;

use crate::db2q::proto::queue::v1::topic_svc::{CreateRequest, CreateResponse};
use crate::db2q::proto::queue::v1::topic_svc::{DropRequest, DropResponse};
use crate::db2q::proto::queue::v1::topic_svc::{ListRequest, ListResponse};

#[tonic::async_trait]
impl<T> TopicService for T
where
    T: Sync + Send + 'static + Deref,
    <T as Deref>::Target: TopicService,
{
    async fn create(
        &self,
        req: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, Status> {
        self.deref().create(req).await
    }

    async fn drop(&self, req: Request<DropRequest>) -> Result<Response<DropResponse>, Status> {
        self.deref().drop(req).await
    }

    async fn list(&self, req: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        self.deref().list(req).await
    }
}
