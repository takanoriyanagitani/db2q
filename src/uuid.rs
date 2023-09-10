use core::fmt;

use crate::db2q::proto::queue;

#[derive(Clone, Copy)]
pub struct Uuid {
    raw: u128,
}

impl Uuid {
    #[cfg(feature = "uv4")]
    pub fn new_v4() -> Self {
        let raw: u128 = uuid::Uuid::new_v4().as_u128();
        Self { raw }
    }

    pub fn as_u128(&self) -> u128 {
        self.raw
    }

    pub fn new(hi: u64, lo: u64) -> Self {
        let h: u128 = hi.into();
        let l: u128 = lo.into();
        let raw: u128 = (h << 64) | l;
        Self { raw }
    }
}

impl fmt::Display for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{:032x}", self.raw)
    }
}

pub trait UuidLike {
    fn as_hi(&self) -> u64;
    fn as_lo(&self) -> u64;
}

impl<U> From<&U> for Uuid
where
    U: UuidLike,
{
    fn from(u: &U) -> Self {
        let hi: u64 = u.as_hi();
        let lo: u64 = u.as_lo();
        Self::new(hi, lo)
    }
}

impl UuidLike for queue::v1::Uuid {
    fn as_hi(&self) -> u64 {
        self.hi
    }
    fn as_lo(&self) -> u64 {
        self.lo
    }
}
