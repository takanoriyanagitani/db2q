use core::time::Duration;

use tonic::Status;

use crate::uuid::Uuid;

use crate::db2q::proto::queue::v1::q_svc::WaitNextRequest;

pub const INTERVAL_DEFAULT: Duration = Duration::from_millis(1000);
pub const TIMEOUT_DEFAULT: Duration = Duration::from_millis(2000);

pub struct WaitNextReq {
    request_id: Uuid,
    topic_id: Uuid,
    previous: Option<u64>,
    interval: Duration,
    timeout: Duration,
}

impl WaitNextReq {
    pub fn as_request_id(&self) -> Uuid {
        self.request_id
    }

    pub fn as_topic_id(&self) -> Uuid {
        self.topic_id
    }

    pub fn as_previous_key(&self) -> Option<u64> {
        self.previous
    }

    pub fn as_interval(&self) -> Duration {
        self.interval
    }

    pub fn as_timeout(&self) -> Duration {
        self.timeout
    }
}

impl TryFrom<&WaitNextRequest> for WaitNextReq {
    type Error = Status;
    fn try_from(g: &WaitNextRequest) -> Result<Self, Self::Error> {
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
        // TODO: limit minimum duration(e.g, no less than 1 ms)
        let interval: Duration = match g.interval.clone() {
            None => INTERVAL_DEFAULT,
            Some(i) => Duration::try_from(i).ok().unwrap_or(INTERVAL_DEFAULT),
        };
        let timeout: Duration = match g.timeout.clone() {
            None => TIMEOUT_DEFAULT,
            Some(i) => Duration::try_from(i).ok().unwrap_or(INTERVAL_DEFAULT),
        };
        Ok(Self {
            request_id,
            topic_id,
            previous,
            interval,
            timeout,
        })
    }
}
