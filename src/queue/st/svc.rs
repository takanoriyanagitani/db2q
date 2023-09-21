use std::sync::Arc;
use tokio::sync::Mutex;

use tonic::{Request, Response, Status};

use crate::db2q::proto::queue::v1::queue_service_server::QueueService;
use crate::db2q::proto::queue::v1::topic_service_server::TopicService;

use crate::db2q::proto::queue::v1::q_svc::KeysRequest;
use crate::db2q::proto::queue::v1::q_svc::WaitNextRequest;
use crate::db2q::proto::queue::v1::q_svc::{CountRequest, CountResponse};
use crate::db2q::proto::queue::v1::q_svc::{NextRequest, NextResponse};
use crate::db2q::proto::queue::v1::q_svc::{PopFrontRequest, PopFrontResponse};
use crate::db2q::proto::queue::v1::q_svc::{PushBackRequest, PushBackResponse};

use crate::db2q::proto::queue::v1::topic_svc::{CreateRequest, CreateResponse};
use crate::db2q::proto::queue::v1::topic_svc::{DropRequest, DropResponse};
use crate::db2q::proto::queue::v1::topic_svc::{ListRequest, ListResponse};

struct Svc<Q, T> {
    q_svc: Arc<Q>,
    t_svc: Arc<T>,
}

pub struct Locked<Q, T> {
    locked: Mutex<Svc<Q, T>>,
}

#[tonic::async_trait]
impl<Q, T> QueueService for Locked<Q, T>
where
    Q: Sync + Send + 'static + QueueService,
    T: Sync + Send + 'static,
{
    type KeysStream = <Q as QueueService>::KeysStream;
    type WaitNextStream = <Q as QueueService>::WaitNextStream;

    async fn push_back(
        &self,
        req: Request<PushBackRequest>,
    ) -> Result<Response<PushBackResponse>, Status> {
        let guard = self.locked.lock().await;
        let s: &Svc<_, _> = &guard;
        let q: &Q = &s.q_svc;
        q.push_back(req).await
    }

    async fn pop_front(
        &self,
        req: Request<PopFrontRequest>,
    ) -> Result<Response<PopFrontResponse>, Status> {
        let guard = self.locked.lock().await;
        let s: &Svc<_, _> = &guard;
        let q: &Q = &s.q_svc;
        q.pop_front(req).await
    }

    async fn count(&self, req: Request<CountRequest>) -> Result<Response<CountResponse>, Status> {
        let guard = self.locked.lock().await;
        let s: &Svc<_, _> = &guard;
        let q: &Q = &s.q_svc;
        q.count(req).await
    }

    async fn next(&self, req: Request<NextRequest>) -> Result<Response<NextResponse>, Status> {
        let guard = self.locked.lock().await;
        let s: &Svc<_, _> = &guard;
        let q: &Q = &s.q_svc;
        q.next(req).await
    }

    async fn wait_next(
        &self,
        req: Request<WaitNextRequest>,
    ) -> Result<Response<Self::WaitNextStream>, Status> {
        let guard = self.locked.lock().await;
        let s: &Svc<_, _> = &guard;
        let q: &Q = &s.q_svc;
        q.wait_next(req).await
    }

    async fn keys(&self, req: Request<KeysRequest>) -> Result<Response<Self::KeysStream>, Status> {
        let guard = self.locked.lock().await;
        let s: &Svc<_, _> = &guard;
        let q: &Q = &s.q_svc;
        q.keys(req).await
    }
}

#[tonic::async_trait]
impl<Q, T> TopicService for Locked<Q, T>
where
    Q: Sync + Send + 'static,
    T: Sync + Send + 'static + TopicService,
{
    async fn create(
        &self,
        req: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, Status> {
        let guard = self.locked.lock().await;
        let s: &Svc<_, _> = &guard;
        let t: &T = &s.t_svc;
        t.create(req).await
    }
    async fn drop(&self, req: Request<DropRequest>) -> Result<Response<DropResponse>, Status> {
        let guard = self.locked.lock().await;
        let s: &Svc<_, _> = &guard;
        let t: &T = &s.t_svc;
        t.drop(req).await
    }
    async fn list(&self, req: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        let guard = self.locked.lock().await;
        let s: &Svc<_, _> = &guard;
        let t: &T = &s.t_svc;
        t.list(req).await
    }
}

pub fn locked_q_topic_svc_new<Q, T>(q: &Arc<Q>, t: &Arc<T>) -> impl QueueService + TopicService
where
    Q: Sync + Send + 'static + QueueService,
    T: Sync + Send + 'static + TopicService,
{
    Locked {
        locked: Mutex::new(Svc {
            q_svc: q.clone(),
            t_svc: t.clone(),
        }),
    }
}
