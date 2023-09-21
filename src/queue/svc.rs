use core::ops::Deref;

use tonic::{Request, Response, Status};

use crate::db2q::proto::queue::v1::queue_service_server::QueueService;

use crate::db2q::proto::queue::v1::q_svc::KeysRequest;
use crate::db2q::proto::queue::v1::q_svc::WaitNextRequest;
use crate::db2q::proto::queue::v1::q_svc::{CountRequest, CountResponse};
use crate::db2q::proto::queue::v1::q_svc::{NextRequest, NextResponse};
use crate::db2q::proto::queue::v1::q_svc::{PopFrontRequest, PopFrontResponse};
use crate::db2q::proto::queue::v1::q_svc::{PushBackRequest, PushBackResponse};

#[tonic::async_trait]
impl<Q> QueueService for Q
where
    Q: Sync + Send + 'static + Deref,
    <Q as Deref>::Target: QueueService,
{
    type KeysStream = <<Q as Deref>::Target as QueueService>::KeysStream;

    type WaitNextStream = <<Q as Deref>::Target as QueueService>::WaitNextStream;

    async fn push_back(
        &self,
        req: Request<PushBackRequest>,
    ) -> Result<Response<PushBackResponse>, Status> {
        self.deref().push_back(req).await
    }

    async fn pop_front(
        &self,
        req: Request<PopFrontRequest>,
    ) -> Result<Response<PopFrontResponse>, Status> {
        self.deref().pop_front(req).await
    }

    async fn count(&self, req: Request<CountRequest>) -> Result<Response<CountResponse>, Status> {
        self.deref().count(req).await
    }

    async fn next(&self, req: Request<NextRequest>) -> Result<Response<NextResponse>, Status> {
        self.deref().next(req).await
    }

    async fn wait_next(
        &self,
        req: Request<WaitNextRequest>,
    ) -> Result<Response<Self::WaitNextStream>, Status> {
        self.deref().wait_next(req).await
    }

    async fn keys(&self, req: Request<KeysRequest>) -> Result<Response<Self::KeysStream>, Status> {
        self.deref().keys(req).await
    }
}
