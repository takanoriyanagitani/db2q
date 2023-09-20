use tonic::Status;

use crate::uuid::Uuid;

use crate::db2q::proto::queue::v1::q_svc::KeysRequest;

pub struct KeysReq {
    request_id: Uuid,
    topic_id: Uuid,
    max_keys: u64,
}

impl KeysReq {
    pub fn as_request_id(&self) -> Uuid {
        self.request_id
    }

    pub fn as_topic_id(&self) -> Uuid {
        self.topic_id
    }

    pub fn as_max_keys(&self) -> u64 {
        self.max_keys
    }
}

impl TryFrom<&KeysRequest> for KeysReq {
    type Error = Status;
    fn try_from(g: &KeysRequest) -> Result<Self, Self::Error> {
        let request_id: Uuid = g
            .request_id
            .as_ref()
            .map(Uuid::from)
            .ok_or_else(|| Status::invalid_argument("request id missing"))?;
        let topic_id: Uuid = g.topic_id.as_ref().map(Uuid::from).ok_or_else(|| {
            Status::invalid_argument(format!("topic id missing. request id: {request_id}"))
        })?;
        let max_keys: u64 = g.max_keys;
        Ok(Self {
            request_id,
            topic_id,
            max_keys,
        })
    }
}
