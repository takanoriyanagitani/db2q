use tonic::Status;

use crate::uuid::Uuid;

use crate::db2q::proto::queue::v1::q_svc::PushBackRequest;

pub struct PushBackReq {
    request_id: Uuid,
    topic_id: Uuid,
    value: Vec<u8>,
}

impl PushBackReq {
    pub fn as_request_id(&self) -> Uuid {
        self.request_id
    }

    pub fn as_topic_id(&self) -> Uuid {
        self.topic_id
    }

    pub fn as_value(&self) -> &[u8] {
        &self.value
    }

    pub fn into_value(self) -> Vec<u8> {
        self.value
    }
}

impl TryFrom<PushBackRequest> for PushBackReq {
    type Error = Status;
    fn try_from(g: PushBackRequest) -> Result<Self, Self::Error> {
        let request_id: Uuid = g
            .request_id
            .as_ref()
            .map(Uuid::from)
            .ok_or_else(|| Status::invalid_argument("request id missing"))?;
        let topic_id: Uuid = g.topic_id.as_ref().map(Uuid::from).ok_or_else(|| {
            Status::invalid_argument(format!("topic id missing. request id: {request_id}"))
        })?;
        let value: Vec<u8> = g.value;
        Ok(Self {
            request_id,
            topic_id,
            value,
        })
    }
}
