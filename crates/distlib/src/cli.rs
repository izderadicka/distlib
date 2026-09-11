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
    /// Any member may propose one; a core member has to agree (§4.4). Proposed
    /// by a core member it takes effect at once, since their own proposal is
    /// their approval. Needs the node running — it holds the log.
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
    ///
    /// Removing a *core* member takes a majority of the core group, and the
    /// member concerned gets no say — so it waits, and `distlib pending` is
    /// where the others find it. Removing anybody else takes one core member.
    Expel {
        /// The member to remove.
        member: MemberId,

        /// Why. Kept in the log as the record of the decision.
        #[arg(long)]
        reason: String,
    },

    /// List the changes waiting for approvals.
    ///
    /// What the group is deciding but has not decided. Each line carries the
    /// log index that names it, which is what `distlib approve` takes.
    Pending,

    /// Approve a pending change.
    ///
    /// Core members only: §4.4 opens *submitting* a change to every member and
    /// gives the decision to a quorum of core nodes.
    Approve {
        /// The proposal, by the log index `distlib pending` lists it under.
        proposal: u64,
    },

    /// Take back a change you proposed.
    ///
    /// Yours alone: a core member able to withdraw anybody's proposal would
    /// hold a veto over a decision the rest of the core group was reaching.
    Withdraw {
        /// The proposal, by the log index `distlib pending` lists it under.
        proposal: u64,
    },

    /// Change the core group: who votes, and where they are.
    ///
    /// The core group is the set of Raft voters (§4.2). It is also the only
    /// addressing the log records, so this is how a core node that changed IP
    /// or port tells the group where it went — without which, in a group with
    /// no relay, it is out of its own group for good.
    Core {
        #[command(subcommand)]
        command: CoreCommand,
    },

    /// Set this node's storage pledge.
    ///
    /// Only ever this node's: a pledge is a promise about the proposer's own
    /// storage, so there is nobody else to set it for.
    Pledge {
        /// Bytes this node commits to providing.
        bytes: u64,
    },

    /// Print a join ticket for somebody who has been admitted.
    ///
    /// Directions, not a credential: it says which group and how to reach its
    /// core nodes. Whoever holds it still has to have been admitted — see
    /// `distlib admit` — before anything will talk to them.
    Ticket,

    /// Join a group from a ticket somebody sent you.
    ///
    /// Writes the group's core nodes and relay settings into this node's
    /// configuration. Start the node afterwards and it fetches the log.
    Join {
        /// The ticket, as printed by `distlib ticket` on a member's node.
        ticket: String,
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

/// `distlib core <...>`
///
/// Two verbs rather than one that takes a whole list, because an operator knows
/// what they want to change and not necessarily where every other core node is.
/// The API assembles the rest from the log — see `group.propose_core`.
#[derive(Debug, Subcommand)]
pub enum CoreCommand {
    /// Record where a core node is, adding them to the core group if they are
    /// not in it.
    ///
    /// **Moving an existing core node takes one core approval; adding a voter
    /// takes a majority of them.** That is the standing rule — changing *who*
    /// votes needs a majority of the voters, everything else needs one — and it
    /// is why a node that has merely moved is back in touch quickly.
    ///
    /// The addresses given replace whatever the log holds for that member
    /// rather than adding to them: a node that moved is not at both, and an old
    /// address left behind is a path every peer goes on trying.
    ///
    /// **Adding somebody who does not vote yet does not work yet.** It will be
    /// proposed, and refused when the last approval lands: a node only serves
    /// consensus from startup, so promoting one would add a voter that counts
    /// toward quorum and can never answer. Moving a node that already votes is
    /// what this command does today.
    Set {
        /// The member whose address this is.
        member: MemberId,

        /// A socket address they can be reached at. May be repeated.
        ///
        /// Required, along with `--relay`, because this command exists to say
        /// where a node is: recording nothing would leave them reachable only
        /// by address lookup, which is exactly what a group with
        /// `relay_mode = "disabled"` does not have.
        #[arg(long = "addr", value_name = "HOST:PORT")]
        addrs: Vec<SocketAddr>,

        /// A relay they can be reached through.
        #[arg(long, value_name = "URL")]
        relay: Option<String>,
    },

    /// Drop a member from the core group.
    ///
    /// A demotion, not an expulsion: they stay a member and keep following the
    /// log, they just stop voting on it. Takes a majority of the core group,
    /// since it changes who votes.
    Remove {
        /// The member to stop counting as a voter.
        member: MemberId,
    },
}
