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
    Run {
        /// Found the group described by `[consensus] core` on startup.
        ///
        /// Run this on exactly one founder, once. The other founders just
        /// `run`; they receive the founding entry by replication.
        ///
        /// It is a flag on `run` rather than a command of its own because the
        /// founder has to stay up afterwards to replicate what it wrote, and
        /// because the node's database is held exclusively by one process — a
        /// separate command could not open it while the node was running.
        #[arg(long)]
        found_group: bool,
    },

    /// Print this node's identity as a line for a founder's `[consensus] core`.
    ///
    /// Creates the identity if there is not one. Run this on every founder but
    /// the one doing the founding, and send them the output — they cannot found
    /// a group without knowing who is in it.
    Whoami,

    /// Admit a member.
    ///
    /// Any member may propose one; the group's rules decide whether it takes
    /// effect. Needs the node running — it holds the log.
    Admit {
        /// The member to admit. They print theirs with `distlib whoami`.
        member: MemberId,

        /// What to call them. Metadata, not identity.
        #[arg(long)]
        name: Option<String>,
    },

    /// Expel a member.
    ///
    /// The reason is recorded in the log alongside who proposed it.
    Expel {
        /// The member to remove.
        member: MemberId,

        /// Why. Kept in the log as the record of the decision.
        #[arg(long)]
        reason: String,
    },

    /// Set this node's storage pledge.
    ///
    /// Only ever this node's: a pledge is a promise about the proposer's own
    /// storage, so there is nobody else to set it for.
    Pledge {
        /// Bytes this node commits to providing.
        bytes: u64,
    },

    /// List the members of this node's group.
    ///
    /// Reads the local database, so it needs the node stopped: a running node
    /// holds that file exclusively. While it runs, its log reports every
    /// membership change instead.
    Members,

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
