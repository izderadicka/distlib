//! Command line surface.

use std::{net::SocketAddr, path::PathBuf};

use clap::{ArgAction, Parser, Subcommand};
use distlib_core::MemberId;

/// Distributed community media library for closed, trusted groups.
#[derive(Debug, Parser)]
#[command(name = "distlib", version, about, long_about = None)]
pub struct Cli {
    /// Directory holding this node's key, configuration and data.
    ///
    /// The config file lives inside it, so this cannot be set in the config
    /// file itself — only here or in the environment.
    #[arg(
        long,
        short = 'd',
        global = true,
        env = "DISTLIB_DATA_DIR",
        value_name = "DIR"
    )]
    pub data_dir: Option<PathBuf>,

    /// Configuration file. Defaults to `<data-dir>/config.toml`.
    #[arg(long, short = 'c', global = true, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Log more: `-v` for debug, `-vv` for trace.
    ///
    /// Overrides `DISTLIB_LOG` and `RUST_LOG` when given.
    #[arg(long, short = 'v', global = true, action = ArgAction::Count)]
    pub verbose: u8,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Create the data directory, generate this node's identity and write a
    /// starter configuration.
    ///
    /// Safe to run twice: an existing identity is reported, not replaced.
    Init {
        /// Replace an existing identity with a freshly generated one.
        ///
        /// This is how a node loses its group membership — the old key is the
        /// only proof of who this node is.
        #[arg(long)]
        force: bool,
    },

    /// Run the node until interrupted.
    Run,

    /// Print this node's identity and where its data lives.
    Status {
        /// Also bind the endpoint and print the full dialable address.
        #[arg(long)]
        online: bool,
    },

    /// Send a ping to another member and wait for the echo.
    Ping {
        /// The member to ping.
        member: MemberId,

        /// A socket address to try directly. May be repeated.
        ///
        /// Needed when address lookup is unavailable — for example with
        /// `relay_mode = "disabled"`, or on a LAN with no relay.
        #[arg(long = "addr", value_name = "HOST:PORT")]
        addrs: Vec<SocketAddr>,

        /// A relay to reach the member through.
        #[arg(long, value_name = "URL")]
        relay: Option<String>,

        /// Payload to send.
        #[arg(long, default_value = "ping")]
        payload: String,

        /// Give up after this many seconds.
        #[arg(long, default_value_t = 10)]
        timeout: u64,
    },
}
