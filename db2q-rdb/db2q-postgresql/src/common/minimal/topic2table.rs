use tonic::Status;

use db2q::uuid::Uuid;

pub trait Topic2Table {
    fn id2name(&self, topic_id: Uuid) -> String;
}

pub trait Table2Topic {
    fn name2id(&self, name: &str) -> Result<Uuid, Status>;
}

pub trait TopicConv: Topic2Table + Table2Topic {}

pub struct Prefix {
    prefix: String,
}

impl Default for Prefix {
    fn default() -> Self {
        Self { prefix: "t".into() }
    }
}

impl Topic2Table for Prefix {
    fn id2name(&self, topic_id: Uuid) -> String {
        format!("{}{topic_id}", self.prefix)
    }
}

impl Table2Topic for Prefix {
    fn name2id(&self, name: &str) -> Result<Uuid, Status> {
        let no_prefix: &str = name
            .strip_prefix(self.prefix.as_str())
            .ok_or_else(|| Status::internal(format!("Wrong table name: {name}")))?;
        let u: u128 = u128::from_str_radix(no_prefix, 16)
            .map_err(|e| Status::internal(format!("Invalid table name: {e}")))?;
        Ok(Uuid::from(u))
    }
}

impl<T> TopicConv for T where T: Topic2Table + Table2Topic {}

pub fn topic2table_prefix_default() -> impl TopicConv {
    Prefix::default()
}
