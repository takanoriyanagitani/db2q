use tonic::Status;

use crate::uuid::Uuid;

use crate::db2q::proto::queue::v1::q_svc::NextRequest;

pub struct NextReq {
    request_id: Uuid,
    topic_id: Uuid,
    previous: Option<u64>,
}

impl NextReq {
    pub fn as_request_id(&self) -> Uuid {
        self.request_id
    }

    pub fn as_topic_id(&self) -> Uuid {
        self.topic_id
    }

    pub fn as_previous_key(&self) -> Option<u64> {
        self.previous
    }
}

impl TryFrom<&NextRequest> for NextReq {
    type Error = Status;
    fn try_from(g: &NextRequest) -> Result<Self, Self::Error> {
        let request_id: Uuid = g
            .request_id
            .as_ref()
            .map(Uuid::from)
            .ok_or_else(|| Status::invalid_argument("request id missing"))?;
        let topic_id: Uuid = g.topic_id.as_ref().map(Uuid::from).ok_or_else(|| {
            Status::invalid_argument(format!("topic id missing. request id: {request_id}"))
        })?;
        let previous: Option<u64> = match g.previous {
            0.. => Some(g.previous.try_into().map_err(|e| {
                Status::invalid_argument(format!("the key out of range({}): {e}", g.previous))
            })?),
            ..=-1 => None,
        };
        Ok(Self {
            request_id,
            topic_id,
            previous,
        })
    }
}
