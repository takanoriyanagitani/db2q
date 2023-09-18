use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use db2q_postgresql::db2q::queue::st::svc::locked_q_topic_svc_new;

use db2q_postgresql::deadpool_postgres;
use db2q_postgresql::tonic;

use db2q_postgresql::db2q::queue::rw::svc::rw_q_svc_new;
use db2q_postgresql::db2q::queue::rw::svc::RwQueueSvc;

use tonic::transport::{server::Router, Server};

use deadpool_postgres::tokio_postgres;
use tokio_postgres::{Config, NoTls};

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};

use db2q_postgresql::count_service_server::CountServiceServer;
use db2q_postgresql::queue_service_server::QueueServiceServer;
use db2q_postgresql::topic_service_server::TopicServiceServer;

#[tokio::main]
async fn main() -> Result<(), String> {
    let listen_addr: String =
        env::var("ENV_LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".into());
    let listen: SocketAddr = str::parse(&listen_addr).map_err(|e| format!("Invalid addr: {e}"))?;

    let pghost: String = env::var("PGHOST").unwrap_or_else(|_| "/var/run/postgresql".into());
    let pguser: String = env::var("PGUSER").unwrap_or_else(|_| "postgres".into());
    let pgpass: String = env::var("PGPASSWORD").unwrap_or_else(|_| "postgres".into());
    let pgdb: String = env::var("PGDATABASE").unwrap_or_else(|_| "postgres".into());

    let mut pgcfg: Config = Config::new();
    pgcfg
        .user(&pguser)
        .dbname(&pgdb)
        .password(&pgpass)
        .host(&pghost);

    let mgcfg: ManagerConfig = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };
    let mg: Manager = Manager::from_config(pgcfg, NoTls, mgcfg);

    let pool: Pool = Pool::builder(mg)
        .max_size(16)
        .build()
        .map_err(|e| format!("Unable to build pool: {e}"))?;

    let t2t = db2q_postgresql::topic::minimal::topic2table::topic2table_prefix_default();
    let topic_svc = db2q_postgresql::topic::minimal::svc::topic_svc_new(&pool, t2t);
    let topic_svc_shared: Arc<_> = Arc::new(topic_svc);

    let t2t = db2q_postgresql::topic::minimal::topic2table::topic2table_prefix_default();
    let count_svc = db2q_postgresql::count::minimal::svc::count_svc_new(&pool, t2t);
    let count_svr: CountServiceServer<_> = CountServiceServer::new(count_svc);

    let t2t = db2q_postgresql::topic::minimal::topic2table::topic2table_prefix_default();
    let queue_svc = db2q_postgresql::queue::minimal::svc::queue_svc_new(&pool, t2t);
    let queue_svc_shared: Arc<_> = Arc::new(queue_svc);

    let locked_q_topic_svc = locked_q_topic_svc_new(&queue_svc_shared, &topic_svc_shared);
    let lqts_shared: Arc<_> = Arc::new(locked_q_topic_svc);

    let topic_svr: TopicServiceServer<_> = TopicServiceServer::new(lqts_shared.clone());

    let rw_q_svc: RwQueueSvc<_> = rw_q_svc_new(&lqts_shared);
    let queue_svr: QueueServiceServer<_> = QueueServiceServer::new(rw_q_svc.clone());

    rw_q_svc
        .make_writable()
        .await
        .map_err(|e| format!("Unable to make writable queue: {e}"))?;

    let mut sv: Server = Server::builder();
    let router: Router<_> = sv
        .add_service(topic_svr)
        .add_service(queue_svr)
        .add_service(count_svr);

    router
        .serve(listen)
        .await
        .map_err(|e| format!("Unable to listen: {e}"))?;
    Ok(())
}
