use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

use tonic::{Request, Response, Status};

use crate::db2q::proto::queue::v1::q_svc::{CountRequest, CountResponse};
use crate::db2q::proto::queue::v1::q_svc::{NextRequest, NextResponse};
use crate::db2q::proto::queue::v1::q_svc::{PopFrontRequest, PopFrontResponse};
use crate::db2q::proto::queue::v1::q_svc::{PushBackRequest, PushBackResponse};
use crate::db2q::proto::queue::v1::queue_service_server::QueueService;

pub struct RwRequest {
    pub writable: bool,
    pub reply: Sender<()>,
}

pub struct RwStateRequest {
    pub reply: Sender<bool>,
}

struct RwSvc {
    req: Receiver<RwRequest>,
    state: Receiver<RwStateRequest>,
}

pub struct RwQueueSvc<I> {
    rw_req: Sender<RwRequest>,
    state_req: Sender<RwStateRequest>,
    internal: Arc<I>,
}

impl<I> Clone for RwQueueSvc<I> {
    fn clone(&self) -> Self {
        Self {
            rw_req: self.rw_req.clone(),
            state_req: self.state_req.clone(),
            internal: self.internal.clone(),
        }
    }
}

pub fn rw_q_svc_new<I>(internal: &Arc<I>) -> RwQueueSvc<I>
where
    I: Send + Sync + 'static + QueueService,
{
    let (rw_req, req) = mpsc::channel(1);
    let (state_req, state) = mpsc::channel(1);
    let mut rwsvc = RwSvc { req, state };
    tokio::spawn(async move {
        match rwsvc.start().await {
            Ok(_) => {}
            Err(e) => log::error!("Unexpected error: {e}"),
        }
    });
    RwQueueSvc {
        rw_req,
        state_req,
        internal: internal.clone(),
    }
}

impl<I> RwQueueSvc<I> {
    pub async fn is_writable(&self) -> Result<bool, Status> {
        let (send, mut recv) = mpsc::channel(1);
        let req = RwStateRequest { reply: send };
        self.state_req
            .send(req)
            .await
            .map_err(|e| Status::internal(format!("Unable to try to get writable state: {e}")))?;
        let writable: bool = recv
            .recv()
            .await
            .ok_or_else(|| Status::internal("Unable to get writable state"))?;
        Ok(writable)
    }
    async fn change_mode(&self, writable: bool) -> Result<(), Status> {
        let (send, mut recv) = mpsc::channel(1);
        let req = RwRequest {
            reply: send,
            writable,
        };
        self.rw_req
            .send(req)
            .await
            .map_err(|e| Status::internal(format!("Unable to try to change mode: {e}")))?;
        recv.recv()
            .await
            .ok_or_else(|| Status::internal("Unable to change mode"))?;
        Ok(())
    }
    pub async fn make_writable(&self) -> Result<(), Status> {
        self.change_mode(true).await
    }
    pub async fn make_readable(&self) -> Result<(), Status> {
        self.change_mode(false).await
    }
}

#[tonic::async_trait]
impl<I> QueueService for RwQueueSvc<I>
where
    I: Send + Sync + 'static + QueueService,
{
    async fn push_back(
        &self,
        req: Request<PushBackRequest>,
    ) -> Result<Response<PushBackResponse>, Status> {
        let writable: bool = self.is_writable().await?;
        let q: Request<_> = writable
            .then_some(req)
            .ok_or_else(|| Status::failed_precondition("read only queue"))?;
        self.internal.push_back(q).await
    }

    async fn pop_front(
        &self,
        req: Request<PopFrontRequest>,
    ) -> Result<Response<PopFrontResponse>, Status> {
        let writable: bool = self.is_writable().await?;
        let q: Request<_> = writable
            .then_some(req)
            .ok_or_else(|| Status::failed_precondition("read only queue"))?;
        self.internal.pop_front(q).await
    }

    async fn count(&self, req: Request<CountRequest>) -> Result<Response<CountResponse>, Status> {
        self.internal.count(req).await
    }

    async fn next(&self, req: Request<NextRequest>) -> Result<Response<NextResponse>, Status> {
        self.internal.next(req).await
    }
}

impl RwSvc {
    pub async fn start(&mut self) -> Result<(), Status> {
        let mut writable: bool = false;
        loop {
            tokio::select! {
                oreq = self.req.recv() => {
                    match oreq {
                        None => { return Ok(()) },
                        Some(q) => {
                            writable = q.writable;
                            q.reply.send(()).await
                                .map_err(|e| Status::internal(format!("Unexpected error: {e}")))?;
                            continue
                        },
                    }
                },
                ostate = self.state.recv() => {
                    match ostate {
                        None => { continue },
                        Some(q) => {
                            q.reply.send(writable).await
                                .map_err(|e| Status::internal(format!("Unexpected error: {e}")))?;
                            continue
                        },
                    }
                },
            };
        }
    }
}
