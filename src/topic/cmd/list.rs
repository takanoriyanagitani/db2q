use tonic::Status;

use crate::uuid::Uuid;

use crate::db2q::proto::queue::v1::topic_svc::ListRequest;

pub struct ListReq {
    request_id: Uuid,
}

impl ListReq {
    pub fn as_request_id(&self) -> Uuid {
        self.request_id
    }
}

impl TryFrom<&ListRequest> for ListReq {
    type Error = Status;
    fn try_from(g: &ListRequest) -> Result<Self, Self::Error> {
        let request_id: Uuid = g
            .request_id
            .as_ref()
            .map(Uuid::from)
            .ok_or_else(|| Status::invalid_argument("request id missing"))?;
        Ok(Self { request_id })
    }
}
