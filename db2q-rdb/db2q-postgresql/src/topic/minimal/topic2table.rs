use db2q::uuid::Uuid;

pub trait Topic2Table {
    fn id2name(&self, topic_id: Uuid) -> String;
}
