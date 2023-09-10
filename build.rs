use std::io;

fn main() -> Result<(), io::Error> {
    tonic_build::configure().build_server(true).compile(
        &["db2q-proto/db2q/proto/queue/v1/q.proto"],
        &["db2q-proto/db2q"],
    )?;
    Ok(())
}
