use tonic::Status;

use crate::uuid::Uuid;

use crate::db2q::proto::queue::v1::q_svc::CountRequest;

pub struct CountReq {
    request_id: Uuid,
    topic_id: Uuid,
}

impl CountReq {
    pub fn as_request_id(&self) -> Uuid {
        self.request_id
    }

    pub fn as_topic_id(&self) -> Uuid {
        self.topic_id
    }
}

impl TryFrom<CountRequest> for CountReq {
    type Error = Status;
    fn try_from(g: CountRequest) -> Result<Self, Self::Error> {
        let request_id: Uuid = g
            .request_id
            .as_ref()
            .map(Uuid::from)
            .ok_or_else(|| Status::invalid_argument("request id missing"))?;
        let topic_id: Uuid = g.topic_id.as_ref().map(Uuid::from).ok_or_else(|| {
            Status::invalid_argument(format!("topic id missing. request id: {request_id}"))
        })?;
        Ok(Self {
            request_id,
            topic_id,
        })
    }
}
