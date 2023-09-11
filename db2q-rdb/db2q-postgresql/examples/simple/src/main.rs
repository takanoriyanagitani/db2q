use std::env;
use std::net::SocketAddr;

use db2q_postgresql::deadpool_postgres;
use db2q_postgresql::tonic;

use tonic::transport::{server::Router, Server};

use deadpool_postgres::tokio_postgres;
use tokio_postgres::{Config, NoTls};

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};

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
    let topic_svr: TopicServiceServer<_> = TopicServiceServer::new(topic_svc);

    let mut sv: Server = Server::builder();
    let router: Router<_> = sv.add_service(topic_svr);

    router
        .serve(listen)
        .await
        .map_err(|e| format!("Unable to listen: {e}"))?;
    Ok(())
}
