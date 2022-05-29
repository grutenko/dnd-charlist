use std::{
    cmp::Ordering,
    net::SocketAddr,
};

use flexi_logger::FileSpec;
use tonic::transport::Server;

use crate::{
    error::Error,
    object_storage::Storage,
    rpc::{
        pb,
        StorageService,
    },
};

pub async fn handler(
    storage_path: String,
    host: String,
    port: u16,
    log: String,
) -> Result<(), Error>
{
    let _ = flexi_logger::Logger::try_with_str("info")
        .and_then(|logger| {
            logger
                .log_to_file(
                    FileSpec::default()
                        .directory(log)
                        .basename("server")
                        .suffix("trc"),
                )
                .print_message()
                .start()
        })
        .map_err(|e| {
            eprintln!("Could not start logger: {} continue with stderr.", e)
        });

    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;

    let storage = Storage::new(storage_path.as_str())?;
    match storage.schema_status()? {
        Ordering::Less => {
            storage.apply_migrations()?;
        }
        Ordering::Equal => {}
        Ordering::Greater => {
            panic!("Storage schema migrated to version greater than available in current server version.");
        }
    }

    Server::builder()
        .add_service(pb::storage_server::StorageServer::new(
            StorageService::new(storage),
        ))
        .serve(addr)
        .await?;

    Ok(())
}
