//! The `distlib` binary: wires the crates together and provides the CLI.

mod cli;
mod commands;

use std::{io::IsTerminal as _, time::Duration};

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
        Command::Whoami => commands::whoami(&paths).await,
        Command::Admit { member, name } => commands::admit(&paths, member, name).await,
        Command::Expel { member, reason } => commands::expel(&paths, member, reason).await,
        Command::Pledge { bytes } => commands::pledge(&paths, bytes).await,
        Command::Members => commands::members(&paths).await,
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
/// openraft is turned down unless an explicit filter is set. It logs each
/// election, vote and state transition at `info`, including a debug-formatted
/// dump of the whole `RaftState` at startup — detail for someone debugging
/// consensus, and noise thick enough at the default level to bury this node's
/// own output. `DISTLIB_LOG` and `RUST_LOG` are left exactly as written:
/// someone who sets one has said what they want to see.
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
        // Colour only for a terminal. `distlib run > node.log` is the ordinary
        // way to keep a node's output, and escape codes in that file make it
        // unreadable and ungreppable — `members=3` is not even a substring of a
        // coloured line, because the `=` is wrapped in them.
        .with_ansi(std::io::stdout().is_terminal())
        .init();
}
