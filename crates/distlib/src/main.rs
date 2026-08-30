//! The `distlib` binary: wires the crates together and provides the CLI.

mod cli;
mod commands;

use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use distlib_core::DataDir;
use tracing_subscriber::EnvFilter;

use crate::{
    cli::{Cli, Command},
    commands::Paths,
};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.verbose);

    let data_dir = DataDir::resolve(cli.data_dir.clone())?;
    let paths = Paths {
        config_file: cli.config.clone().unwrap_or_else(|| data_dir.config_file()),
        data_dir,
    };

    match cli.command {
        Command::Init { force } => commands::init(&paths, force),
        Command::Run { found_group } => commands::run(&paths, found_group).await,
        Command::Members => commands::members(&paths),
        Command::Status { online } => commands::status(&paths, online).await,
        Command::Ping {
            member,
            addrs,
            relay,
            payload,
            timeout,
        } => {
            commands::ping(
                &paths,
                member,
                addrs,
                relay,
                &payload,
                Duration::from_secs(timeout),
            )
            .await
        }
    }
}

/// Sets up logging.
///
/// `-v` wins when given, because someone who passes it is asking for output
/// now and should not have to notice that `RUST_LOG` is set in their shell.
/// Otherwise `DISTLIB_LOG` is consulted, then `RUST_LOG`, then a quiet default.
///
/// openraft is turned down a level in every case. It logs each election, vote
/// and state transition at `info`, including a debug-formatted dump of the
/// whole `RaftState` at startup — detail for someone debugging consensus, and
/// noise thick enough at the default level to bury this node's own output.
fn init_tracing(verbose: u8) {
    let filter = match verbose {
        0 => EnvFilter::try_from_env("DISTLIB_LOG")
            .or_else(|_| EnvFilter::try_from_default_env())
            .unwrap_or_else(|_| EnvFilter::new("info,openraft=warn")),
        1 => EnvFilter::new(
            "info,distlib=debug,distlib_net=debug,distlib_core=debug,\
             distlib_consensus=debug,openraft=info",
        ),
        _ => EnvFilter::new(
            "debug,distlib=trace,distlib_net=trace,distlib_core=trace,\
             distlib_consensus=trace,openraft=debug",
        ),
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}
