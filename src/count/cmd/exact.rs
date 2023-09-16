use tonic::Status;

use crate::uuid::Uuid;

use crate::db2q::proto::queue::v1::cnt_svc::ExactRequest;

pub struct ExactReq {
    request_id: Uuid,
    topic_id: Uuid,
}

impl ExactReq {
    pub fn as_request(&self) -> Uuid {
        self.request_id
    }
    pub fn as_topic(&self) -> Uuid {
        self.topic_id
    }
}

impl TryFrom<&ExactRequest> for ExactReq {
    type Error = Status;

    fn try_from(r: &ExactRequest) -> Result<Self, Self::Error> {
        let request_id: Uuid = r
            .request_id
            .as_ref()
            .try_into()
            .map_err(|_| Status::invalid_argument("request id missing"))?;
        let topic_id: Uuid = r.topic_id.as_ref().try_into().map_err(|_| {
            Status::invalid_argument(format!("topic id missing. request id: {request_id}"))
        })?;
        Ok(Self {
            request_id,
            topic_id,
        })
    }
}
