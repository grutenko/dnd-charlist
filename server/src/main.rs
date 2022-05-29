#![feature(test)]

use std::process::exit;

use clap::{
    Parser,
    Subcommand,
};
use commands::{
    create_storage,
    property,
    start,
};

mod commands;
mod error;
mod object_storage;
mod rpc;

#[derive(Parser, Debug)]
#[clap(author, version, about)]
#[clap(propagate_version = true)]
pub struct Cli
{
    #[clap(subcommand)]
    subcommand: Command,
}

#[derive(Subcommand, Debug)]
enum Command
{
    #[clap(name = "storage:create")]
    CreateStorage
    {
        storage_path: String,

        #[clap(long)]
        name: String,
    },
    #[clap(name = "storage:get-property")]
    GetProperty
    {
        storage_path: String,

        #[clap(long, short)]
        prop: String,
    },
    #[clap(name = "start")]
    Start
    {
        storage_path: String,

        /// Server hostname
        #[clap(long)]
        host: String,

        /// Server port
        #[clap(long)]
        port: u16,

        /// Path to log file
        #[clap(long)]
        log: String,
    },
}

#[tokio::main]
async fn main()
{
    let result = match Cli::parse().subcommand {
        Command::CreateStorage { storage_path, name } => {
            create_storage::handler(storage_path, name)
        }
        Command::GetProperty { storage_path, prop } => {
            property::handler(storage_path, prop)
        }
        Command::Start {
            storage_path,
            host,
            port,
            log,
        } => start::handler(storage_path, host, port, log).await,
    };

    if let Err(e) = result {
        eprintln!("{}", e.to_string());
        exit(e.exit_code().into_raw());
    }
}
