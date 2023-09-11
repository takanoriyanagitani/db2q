use db2q::uuid::Uuid;

pub trait Topic2Table {
    fn id2name(&self, topic_id: Uuid) -> String;
}

pub struct Prefix {
    prefix: String,
}

impl Default for Prefix {
    fn default() -> Self {
        Self { prefix: "T".into() }
    }
}

impl Topic2Table for Prefix {
    fn id2name(&self, topic_id: Uuid) -> String {
        format!("{}{topic_id}", self.prefix)
    }
}

pub fn topic2table_prefix_default() -> impl Topic2Table {
    Prefix::default()
}
