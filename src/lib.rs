pub mod db2q {
    pub mod proto {
        pub mod queue {
            pub mod v1 {
                tonic::include_proto!("db2q.proto.queue.v1");
            }
        }
    }
}

pub mod uuid;

pub mod queue;

pub mod count;
pub mod topic;
