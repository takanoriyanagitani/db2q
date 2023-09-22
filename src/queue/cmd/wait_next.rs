use core::time::Duration;
use std::env;
use std::sync::RwLock;

use tonic::Status;

use crate::uuid::Uuid;

use crate::db2q::proto::queue::v1::q_svc::WaitNextRequest;

pub const INTERVAL_DEFAULT: Duration = Duration::from_millis(1000);
pub const TIMEOUT_DEFAULT: Duration = Duration::from_millis(2000);

pub const INTERVAL_MINIMUM_DEFAULT: Duration = Duration::from_millis(1);
pub const INTERVAL_MINIMUM_KEY: &str = "ENV_INTERVAL_NS_MINIMUM";

static INTERVAL_MINIMUM: RwLock<Option<Duration>> = RwLock::new(None);

// TODO: rewrite using Once & unsafe if too slow
fn get_interval_minimum() -> Duration {
    let od: Option<Duration> = match INTERVAL_MINIMUM.read() {
        Err(e) => {
            log::warn!("Unable to lock: {e}");
            None
        }
        Ok(guard) => *guard,
    };
    od.unwrap_or_else(|| match INTERVAL_MINIMUM.write() {
        Err(e) => {
            log::warn!("Unable to lock: {e}");
            INTERVAL_MINIMUM_DEFAULT
        }
        Ok(mut guard) => {
            let mo: &mut Option<Duration> = &mut guard;
            match mo {
                Some(d) => *d,
                None => {
                    let ds: Option<String> = env::var(INTERVAL_MINIMUM_KEY).ok();
                    let di: Option<u64> = ds.and_then(|os| str::parse(os.as_str()).ok());
                    let od: Option<Duration> = di.map(Duration::from_nanos);
                    let d: Duration = od.unwrap_or(INTERVAL_MINIMUM_DEFAULT);
                    mo.replace(d);
                    d
                }
            }
        }
    })
}

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
        let imin: Duration = get_interval_minimum();
        let interval: Duration = match g.interval.clone() {
            None => INTERVAL_DEFAULT,
            Some(i) => Duration::try_from(i).ok().unwrap_or(INTERVAL_DEFAULT),
        }
        .max(imin);
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
