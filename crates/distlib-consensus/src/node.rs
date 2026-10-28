//! A node that runs consensus: Raft, its stores, and the membership it derives.
//!
//! This is where the phase 1 claim becomes true — the connection allowlist stops
//! being configuration and starts being a projection of the committed log. Two
//! background tasks make that so: one carries every membership change from the
//! state machine to the allowlist, and one closes connections the change
//! invalidates (§4.4).

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use distlib_core::{GroupSettings, MemberId, NodeAddr, RawMemberId, SignedAddress};
use distlib_net::{
    AddressBook, AllowlistHooks, AllowlistWriter, Connections, Directory, Protocols, Transport,
    alpn, ping::PingProtocol,
};
use iroh::{Endpoint, SecretKey};
use iroh_gossip::{net::GOSSIP_ALPN, net::Gossip};
use openraft::{
    Config, Raft, RaftNetworkFactory, ServerState,
    error::{ClientWriteError, RaftError},
};
use redb::Database;
use tokio::{sync::watch, task::JoinHandle};

use crate::{
    error::ConsensusError,
    event::{MemberProfile, MembershipEvent, Timestamp},
    gossip,
    raft::{
        LogStore, MemberlogClient, MemberlogProtocol, ProposeError, RaftNetworkFactoryImpl,
        RaftProtocol, Seat, StateMachineStore, TypeConfig, core_group,
        follower::{self, SharedSources, Sources},
        network::TrustedCore,
    },
    signed::SignedEvent,
    state::MembershipState,
};

/// The file holding a node's whole Raft state, under its data directory.
pub const RAFT_DB: &str = "raft.redb";

/// The ALPNs a [`MembershipNode`] serves.
///
/// What the endpoint must advertise, since an endpoint offering a protocol its
/// router cannot handle negotiates it and then refuses every stream. Wider than
/// [`alpn::registered`], which covers only what `distlib-net` can serve alone.
/// Belt and braces rather than the mechanism: iroh's `Router` calls
/// `set_alpns` with whatever it accepts, so what a node *serves* is what it
/// ends up advertising either way. This keeps the endpoint honest in the window
/// before the router is up, and says in one place what a node answers.
///
/// **The same set on every node, which it did not used to be.** Until 2.3-2
/// this took a `core: bool` and left the consensus protocols off a follower,
/// on the argument that advertising a protocol you cannot answer means
/// negotiating it and then refusing every stream. That argument was right and
/// is now outweighed: a `Router`'s protocols are fixed when it spawns, and
/// `Router::shutdown` closes the *endpoint*, so a node cannot start serving a
/// protocol later. A follower that may be promoted therefore has to be
/// listening before it has anything to answer with.
///
/// Handlers check [`crate::raft::Seat`] for each operation. Consensus mutations,
/// proposals and directory requests require a seat; a seatless member can still
/// serve retained memberlog history, refer a follower to its current core, or
/// acknowledge retirement. Those read-only operations grant no voter authority.
pub fn alpns() -> Vec<Vec<u8>> {
    vec![
        alpn::PING.to_vec(),
        // Gossip on every node: a core node announces on the topic and a
        // follower listens on it, and you have to be joined to a topic to
        // broadcast on it at all.
        GOSSIP_ALPN.to_vec(),
        // Consensus on every node too, answered only by one that votes.
        alpn::RAFT.to_vec(),
        alpn::MEMBERLOG.to_vec(),
    ]
}

/// How many times a proposal re-checks who the leader is before giving up.
///
/// More than one because the answer can change underneath: the leader named in
/// a `ForwardToLeader` may have lost the term by the time we dial it.
const PROPOSE_ATTEMPTS: usize = 3;

/// The longest Raft waits without hearing from a leader before it stands for
/// election itself, in milliseconds. See [`raft_config`].
const ELECTION_TIMEOUT_MAX: u64 = 2_000;

/// How long a proposal waits, after a forward failed, for this node to stop
/// naming the leader that did not answer.
///
/// **Long enough to tell a dead leader from a live one, and no longer.** This
/// node is a voter, so if the leader is dead its own election timer fires at
/// most [`ELECTION_TIMEOUT_MAX`] after the last heartbeat, and from then on it
/// names nobody or somebody new — which ends the wait at once. Still naming
/// the same leader after that, plus a second's slack for a busy machine,
/// means the leader is alive and was only out of reach for a moment, so the
/// right thing is to ask it again rather than wait any longer.
///
/// The wait never slows down a failover: it only watches for one.
const LEADER_CHANGE_WAIT: Duration = Duration::from_millis(ELECTION_TIMEOUT_MAX + 1_000);

/// Raft's timing, in milliseconds: a heartbeat every quarter second, and an
/// election after one to two seconds without one.
///
/// openraft's defaults are 50ms and 150–300ms, which suit voters on one LAN.
/// A group of friends' machines is not that — voters may reach each other
/// through a relay, over a home uplink, on a machine that is busy with other
/// things — and one stall past the election timeout costs a term: the leader
/// is deposed, proposals in flight fail with nobody to forward to, and the
/// group spends the next election unable to commit. Found by Windows CI: under
/// load, 22 of 24 freshly founded test groups re-elected within three seconds
/// on the defaults, some of them four times; on these values none did (P3-4).
///
/// The price is detection: a leader that really has died is replaced after one
/// to two seconds rather than a fraction of one, which nothing in a membership
/// log notices.
///
/// **Nothing is ever purged** (C43): openraft still snapshots, but keeps every
/// entry behind the snapshot, so the signed log is the group's whole history
/// and a follower can always read it from the founding entry. With nothing
/// purged, openraft never sends a snapshot to a voter either, so every core
/// node holds the whole log too.
fn raft_config() -> Config {
    Config {
        heartbeat_interval: 250,
        election_timeout_min: 1_000,
        election_timeout_max: ELECTION_TIMEOUT_MAX,
        max_in_snapshot_log_to_keep: u64::MAX,
        ..Config::default()
    }
}

/// How long founding waits for the first election to settle.
///
/// Generous: it covers a real election among founders who have to reach each
/// other over the network, and it only ever runs once per group.
const LEADERSHIP_TIMEOUT: Duration = Duration::from_secs(30);

/// Why a node could not start, or could not commit what it was asked to.
///
/// Separate from [`ConsensusError`], which is about the rules an event has to
/// satisfy and stays comparable so tests can assert on it. These are failures
/// of the machinery underneath — disk, or Raft itself — and openraft's errors
/// are large and not comparable, so they are boxed behind `Display`.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    /// The Raft database could not be opened.
    #[error("could not open the raft database at {path}")]
    Database {
        path: std::path::PathBuf,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Raft failed: starting it, initialising the group, or committing.
    #[error("consensus failed")]
    Raft(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// The event itself was not valid.
    #[error(transparent)]
    Event(#[from] ConsensusError),

    /// A leader was known but could not be got to commit, on every attempt.
    #[error("could not get {leader} to commit the proposal")]
    Forward {
        leader: RawMemberId,
        #[source]
        source: ProposeError,
    },

    /// No leader emerged in time, so there was nobody able to commit.
    #[error("no leader is available to commit the proposal")]
    NoLeader,

    /// A voters-only operation was asked of a node that does not vote.
    #[error("this node is not part of the core group")]
    NotCore,

    /// The endpoint was closed before the node could be set up.
    #[error("the endpoint is closed")]
    EndpointClosed,

    /// This node could not catch up to the membership the group is at.
    ///
    /// Distinct from [`NodeError::NoLeader`]: a leader exists and answered —
    /// it refused the proposal as being made against a superseded membership —
    /// and this node then failed to reach that membership itself.
    #[error("this node did not catch up to the membership at index {changed_at}")]
    BehindTheGroup { changed_at: u64 },

    /// A proposal committed, but this node did not apply it in time to say
    /// what became of it.
    ///
    /// Not a failure of the proposal — the group has it — so it is not
    /// [`NodeError::BehindTheGroup`], which is about a proposal that never got
    /// that far.
    #[error("committed at index {index}, but this node has not caught up with it yet")]
    NotYetApplied { index: u64 },
}

/// The result of starting or driving a node.
pub type Result<T> = std::result::Result<T, NodeError>;

fn raft_failed(source: impl std::error::Error + Send + Sync + 'static) -> NodeError {
    NodeError::Raft(Box::new(source))
}

/// The three handles on this node's Raft database.
struct Stores {
    /// What openraft writes, through a voter's Raft.
    log: LogStore,
    /// A second handle on the same log, for serving followers. redb readers
    /// see a consistent snapshot without blocking the writer, so serving
    /// cannot slow consensus.
    served_log: LogStore,
    state_machine: StateMachineStore,
}

fn open_stores(data_dir: &Path) -> Result<Stores> {
    let path = data_dir.join(RAFT_DB);
    let db = Arc::new(
        Database::create(&path).map_err(|source| NodeError::Database {
            path,
            source: Box::new(source),
        })?,
    );
    Ok(Stores {
        log: LogStore::from_database(Arc::clone(&db)).map_err(raft_failed)?,
        served_log: LogStore::from_database(Arc::clone(&db)).map_err(raft_failed)?,
        state_machine: StateMachineStore::from_database(db).map_err(raft_failed)?,
    })
}

/// Whether this node votes from the start.
///
/// The log decides, once there is one. Before that, configuration does — the
/// same rule the allowlist follows, and for the same reason: a node cannot
/// read a log it has no way to reach.
fn starts_as_core(
    state_machine: &StateMachineStore,
    me: MemberId,
    core: &[(MemberId, NodeAddr)],
) -> bool {
    if state_machine.promotion().is_some() {
        return true;
    }
    let membership = state_machine.membership();
    if membership.group_id().is_some() {
        membership.is_core(&me)
            || state_machine
                .raft_membership()
                .borrow()
                .voter_ids()
                .any(|id| id == RawMemberId::from(me))
    } else {
        core.iter().any(|(member, _)| *member == me)
    }
}

/// Prefer persisted core addresses on restart, retaining applied voters while
/// a transition is unfinished. Configuration is only the pre-founding seed.
fn initial_sources(
    state_machine: &StateMachineStore,
    core: Vec<(MemberId, NodeAddr)>,
) -> Vec<(MemberId, NodeAddr)> {
    if state_machine.membership().group_id().is_some() {
        let mut known = state_machine.membership().core().clone();
        let applied = state_machine.raft_membership().borrow().clone();
        for voter in applied.voter_ids() {
            if let (Ok(id), Some(addr)) = (
                MemberId::try_from(voter),
                applied.membership().get_node(&voter),
            ) {
                known.entry(id).or_insert_with(|| addr.clone());
            }
        }
        known.into_iter().collect()
    } else {
        core
    }
}

/// Starts what the node's role at startup runs.
///
/// A voter reconciles the log's core group into Raft, which is what makes
/// `CoreGroupChanged` mean anything; the task works out for itself whether it
/// is the leader that may. A follower fetches the log instead.
fn start_first_role(role: &Role) {
    let Some(raft) = role.seat.raft() else {
        if retired_and_expelled(role) {
            return;
        }
        start(
            &role.follow,
            tokio::spawn(follower::follow(role.following(), role.listens.clone())),
        );
        return;
    };
    let asking =
        ask_once_for_the_directory(role.me, role.client.clone(), Arc::clone(&role.sources));
    let enacting = core_group::enact(raft, role.state_machine.clone(), role.network.clone());
    start(
        &role.core_group,
        // Joined rather than spawned beside it, so the one-shot ask lives and
        // dies with the task that is already tracked here. `enact` never
        // returns, so the join's only effect is to hold both under one handle.
        tokio::spawn(async move {
            tokio::join!(enacting, asking);
        }),
    );
}

/// Where the group is, for anything that dials by id alone, and the client
/// that dials the core group with it.
///
/// Our own protocols carry a `NodeAddr` beside every member and never need the
/// book; iroh-gossip subscribes with bare ids and cannot. Seeded from the
/// configured core group — which is also a ticket's core group — and added to
/// as the log and the core nodes say more.
fn install_address_book_and_client(
    endpoint: &Endpoint,
    connections: &Connections,
    directory: &Directory,
    core: &[(MemberId, NodeAddr)],
) -> Result<(AddressBook, MemberlogClient)> {
    let addresses = AddressBook::install(endpoint).map_err(|_| NodeError::EndpointClosed)?;
    addresses.learn_all(core.iter().map(|(member, addr)| (*member, addr)));
    let client = MemberlogClient::new(
        endpoint.clone(),
        connections.clone(),
        addresses.clone(),
        directory.clone(),
    );
    Ok((addresses, client))
}

/// What answers this node's peers.
///
/// Built once, at the same moment as the seat they read, rather than in
/// [`MembershipNode::protocols`].
fn protocols_for(
    role: &Role,
    served_log: LogStore,
    directory: &Directory,
    memberlog: &MemberlogClient,
) -> (RaftProtocol, MemberlogProtocol) {
    let raft = RaftProtocol::new(
        role.seat.clone(),
        role.state_machine.clone(),
        role.trusted.clone(),
    );
    let memberlog = MemberlogProtocol::new(
        role.seat.clone(),
        served_log,
        role.state_machine.clone(),
        directory.clone(),
        memberlog.clone(),
    );
    (raft, memberlog)
}

/// Joins the group's gossip topic, and answers with the task and where this
/// node's own address statements will appear.
///
/// The topic is the group's id, so this cannot join until there is a group. A
/// founder has none for the moment before it founds one, and a new follower
/// has none until its first fetch — so the task waits for one rather than the
/// caller having to order any of that.
fn spawn_join_topic(
    role: &Role,
    swarm: Gossip,
    endpoint: &Endpoint,
    directory: &Directory,
    hints: &watch::Sender<gossip::Hint>,
) -> (JoinHandle<()>, watch::Receiver<Option<SignedAddress>>) {
    let (said, own_address) = watch::channel(None);
    let joining = join_topic(
        swarm,
        role.state_machine.clone(),
        role.me,
        role.seat.raft().is_some(),
        hints.clone(),
        TopicParts {
            endpoint: endpoint.clone(),
            // The endpoint's key *is* this member's key — it is what the
            // allowlist checks on every connection — so an announcement is
            // signed by the same identity the group already authenticates.
            secret: endpoint.secret_key().clone(),
            directory: directory.clone(),
            said,
        },
    );
    (tokio::spawn(joining), own_address)
}

/// A task this node runs only while it has a particular role.
///
/// Shared and replaceable, because a role now changes while the node runs: a
/// promoted follower stops following and starts reconciling the core group,
/// and both handles have to be reachable from the task that does the promoting
/// *and* from [`MembershipNode::shutdown`].
type RoleTask = Arc<Mutex<Option<JoinHandle<()>>>>;

/// Stops and forgets a role's task, if it is running.
fn stop(task: &RoleTask) {
    if let Some(handle) = task.lock().unwrap_or_else(PoisonError::into_inner).take() {
        handle.abort();
    }
}

/// A core node asks another core node where the group is, once, at startup.
///
/// **The half of 2a-5 that was missing** (carried item C4). A follower asks for
/// the directory in its follow loop; a core node runs `core_group::enact`
/// instead and never that loop, so a core node that restarted came back with an
/// empty directory and answered `Request::Directory` with an empty list — for
/// as long as it ran, because announcements are events and a settled group
/// makes none. The other core nodes still answered, so this was a degradation
/// rather than a break, which is why it was recorded rather than rushed.
///
/// **At startup and nowhere else**, which is the decision the item was waiting
/// on. The alternative on the table — also ask when this node notices it is
/// answering with fewer addresses than the group has members — is a second
/// trigger wanting a rule about how often, and it treats a symptom this one
/// removes at the source. A core node that has just started is the only core
/// node whose directory is empty for a reason nothing else will fix.
///
/// Best effort and quiet about failing, like every other use of this ask: a
/// group of one core node has nobody to ask and says so at `debug`, and a node
/// whose peers are all down is no worse off than before it tried.
async fn ask_once_for_the_directory(me: MemberId, client: MemberlogClient, sources: SharedSources) {
    if !follower::ask_for_the_directory(me, &client, &sources).await {
        tracing::debug!("no other core node said where the group is");
    }
}

/// Hands a role's task over to be held for the life of the node.
fn start(task: &RoleTask, handle: JoinHandle<()>) {
    *task.lock().unwrap_or_else(PoisonError::into_inner) = Some(handle);
}

/// A running consensus node.
///
/// Owns the router serving its peers, the tasks that keep the allowlist in step
/// with the log, and — depending on whether it votes — either a Raft or the
/// task that follows one. Dropping it does not stop those; call
/// [`Self::shutdown`].
pub struct MembershipNode {
    id: MemberId,
    /// The Raft this node votes with, if it votes.
    ///
    /// The single answer to "is this node a voter", and the reason it is a
    /// cell rather than a field: promotion fills it while the node runs, and
    /// the protocols the router was built with have to see that happen.
    seat: Seat,
    /// Where to fetch the log from, and who last claimed to be leader.
    ///
    /// Kept on every node, not only a follower. A node that starts as a voter
    /// never reads it — but it costs a `Vec` of the core group, and the
    /// alternative is an `Option` whose `None` arm is unreachable.
    sources: SharedSources,
    state_machine: StateMachineStore,
    /// The membership as this node enforces it: published by the allowlist
    /// task once the allowlist admits what it says.
    enforced: watch::Receiver<MembershipState>,
    endpoint: Endpoint,
    /// Where members have said they can be reached.
    ///
    /// Held so the node can report it. Nothing here reads it to dial with —
    /// it is installed on the endpoint and iroh consults it — but "who can
    /// this node actually reach" is a question worth being able to answer,
    /// and the only way to answer it is to ask.
    known_addresses: Directory,
    /// The address statement this node last announced — see
    /// [`Self::own_address`].
    own_address: watch::Receiver<Option<SignedAddress>>,
    /// The handlers this node needs served, kept so [`Self::protocols`] can
    /// hand them out after the node exists. All three are cheap clones over
    /// the state they answer from.
    raft_protocol: RaftProtocol,
    memberlog_protocol: MemberlogProtocol,
    /// The gossip this node was given, for [`Self::protocols`] and for the
    /// task that joins the group's topic.
    swarm: Gossip,
    /// Held, not detached: it is the only thing keeping the allowlist writable,
    /// and dropping it would freeze membership at whatever was last applied.
    allowlist_updates: JoinHandle<()>,
    evictions: JoinHandle<()>,
    /// Speaks `distlib/memberlog/0`, for handing a proposal to the leader.
    ///
    /// Kept rather than built per forward so a forwarded proposal travels over
    /// the connection this node already has to that peer, rather than opening a
    /// second one for the purpose.
    memberlog: MemberlogClient,
    /// Every connection this node holds, across protocols.
    connections: Connections,
    /// Announcing the log, or listening for announcements of it.
    ///
    /// Started only once a group exists, since the topic is the group's id.
    /// Held so shutdown can stop it.
    gossip: JoinHandle<()>,

    /// Pulling the log over `distlib/memberlog/0`. Held, not detached: dropping
    /// it would leave the node frozen at whatever membership it last fetched,
    /// still enforcing it. Empty on a voter, and emptied when one is promoted.
    follow: RoleTask,

    /// Putting the log's core group into effect in Raft. Empty on a follower,
    /// which has no Raft to change, and filled when one is promoted.
    core_group: RoleTask,

    /// Watching the log for a change of role, and making it true when one
    /// comes. Every node has one: a follower's is waiting to be promoted, a
    /// voter's to be demoted, and a promoted follower's goes on to the second
    /// once it has done the first.
    role: JoinHandle<()>,

    /// Whether this node has been expelled from its group.
    ///
    /// Only a follower can find this out — it is told by being refused — and it
    /// is a fact about the group rather than about this task, so it lives on
    /// the node rather than inside the follow loop.
    expelled: watch::Sender<bool>,

    /// The follow loop's wake-up, shared with the gossip listener.
    ///
    /// Held so a follower's own proposal can wake it: the core node that
    /// committed the entry has just said how far the log reaches, and waiting
    /// for gossip to say it again would put [`Self::propose`]'s answer behind
    /// the idle poll whenever gossip is not getting through.
    hints: watch::Sender<gossip::Hint>,
}

// `openraft::Raft` does not implement `Debug`, and there is nothing useful to
// print here anyway: what a reader wants is the membership or Raft's metrics,
// both of which have their own accessors.
impl std::fmt::Debug for MembershipNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MembershipNode")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl MembershipNode {
    /// Starts consensus on `endpoint`, storing state under `data_dir`.
    ///
    /// The caller owns the [`Transport`] and the router: from phase 2 on,
    /// iroh-docs needs the same `Gossip` this node announces on and all of it
    /// accepted on one router, so there cannot be a second of either. What
    /// this node needs served it declares through [`Self::protocols`], which
    /// the caller hands to [`distlib_net::serve`] once every subsystem has
    /// said the same. The node is therefore not answering anything until the
    /// caller has done that.
    ///
    /// `hooks` must be the same instance installed on the endpoint: the
    /// eviction task reaches connections through it, and a different instance
    /// would have an empty table.
    ///
    /// `allowlist` must be the write half of the pair `hooks` reads. The caller
    /// creates it seeded with the bootstrap set, because core nodes cannot
    /// replicate the log without connecting and cannot connect without an
    /// allowlist — the founding set has to come from outside the log exactly
    /// once. From the moment a `GroupFounded` is applied, the log wins and the
    /// seed is never consulted again.
    ///
    /// `core` is the configured core group — `[consensus] core` — with an
    /// address for each. It decides two things, and only until the log can
    /// decide them instead: whether this node is a voter, and who it will
    /// speak to before it has a log of its own. A node not listed there is a
    /// follower, and fetches the log from the ones that are.
    ///
    /// Once a `GroupFounded` has been applied the log wins, here as everywhere:
    /// a node the group was founded without is a follower whatever its config
    /// says.
    pub async fn start(
        transport: Transport,
        hooks: AllowlistHooks,
        allowlist: AllowlistWriter,
        data_dir: &Path,
        core: Vec<(MemberId, NodeAddr)>,
    ) -> Result<Self> {
        let Transport {
            endpoint,
            gossip: swarm,
            directory,
        } = transport;
        let id = MemberId::from(endpoint.id());
        let Stores {
            log,
            served_log,
            state_machine,
        } = open_stores(data_dir)?;

        // The node's connections, shared by everything it speaks: openraft's
        // replication, forwarded proposals, following the log, and whatever
        // protocols come later. All of them draw on the same set.
        let connections = Connections::new();

        let (addresses, memberlog) =
            install_address_book_and_client(&endpoint, &connections, &directory, &core)?;

        let is_core = starts_as_core(&state_machine, id, &core);

        // The gossip listener sends on this and the follow loop waits on it.
        // Seeded with `MayHaveMissed` because that is true at startup: this
        // node has heard nothing yet and has no idea how far the log reaches.
        let (hints, listens) = watch::channel(gossip::Hint::MayHaveMissed);

        // Set only by a follower that finds every core node refusing it. Held
        // here for the life of the node so that `expelled` never observes a
        // closed channel and mistakes it for an answer.
        let (expelled, _) = watch::channel(false);

        // Configuration, until the log can say better. A follower being
        // promoted replaces this with what the log said, in the moment before
        // it blanks itself — see `TrustedCore`.
        let trusted = TrustedCore::new(core.iter().map(|(member, _)| *member).collect());
        let core = initial_sources(&state_machine, core);
        let sources: SharedSources = Arc::new(Mutex::new(Sources { core, leader: None }));

        // Watching the log for a change of role, and making it true when one
        // comes. Built first because a voter's Raft is built from it, exactly
        // as a promoted node's is later.
        let role = Role {
            me: id,
            config: Arc::new(raft_config().validate().map_err(raft_failed)?),
            // Every node serves the same protocols, and the seat decides which
            // of them can actually answer. Not a simplification: a `Router`'s
            // protocols are fixed when it spawns, and `Router::shutdown` closes
            // the endpoint — so a follower that may be promoted has to be
            // listening on `distlib/raft/0` before it has a Raft to listen
            // with. See [`alpns`].
            //
            // Forwarding does not go through this factory: a proposal travels
            // over `distlib/memberlog/0`, which every member may speak, while
            // the factory serves Raft's own replication between voters.
            network: RaftNetworkFactoryImpl::new(endpoint.clone(), connections.clone()),
            log,
            state_machine: state_machine.clone(),
            client: memberlog.clone(),
            sources: Arc::clone(&sources),
            expelled: expelled.clone(),
            listens,
            seat: Seat::empty(),
            trusted,
            follow: Arc::new(Mutex::new(None)),
            core_group: Arc::new(Mutex::new(None)),
        };
        if is_core {
            role.seat.take(new_raft(&role).await.map_err(raft_failed)?);
        }

        let (raft_protocol, memberlog_protocol) =
            protocols_for(&role, served_log, &directory, &memberlog);
        let (gossip, own_address) =
            spawn_join_topic(&role, swarm.clone(), &endpoint, &directory, &hints);

        let (allowlist_updates, enforced) =
            enforce_membership(&state_machine, allowlist, addresses, hooks.clone());
        let evictions = tokio::spawn(hooks.evict_expelled());

        start_first_role(&role);
        let (seat, follow, core_group) = (
            role.seat.clone(),
            Arc::clone(&role.follow),
            Arc::clone(&role.core_group),
        );
        let role = tokio::spawn(serve_role(role));

        Ok(Self {
            id,
            seat,
            sources,
            state_machine,
            enforced,
            endpoint,
            // Where members have said they can be reached; see
            // `distlib_net::Directory`.
            known_addresses: directory,
            own_address,
            raft_protocol,
            memberlog_protocol,
            swarm,
            allowlist_updates,
            evictions,
            memberlog,
            connections,
            gossip,
            follow,
            core_group,
            role,
            expelled,
            hints,
        })
    }

    /// This node's identity.
    pub fn id(&self) -> MemberId {
        self.id
    }

    /// The endpoint this node serves and dials on.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// What this node needs the caller's router to serve, and with what.
    ///
    /// Exactly the ALPNs [`alpns`] names, which is the one thing a test can
    /// check: the endpoint is built from that list before any handler exists,
    /// so the two are declared separately and can only be kept in step by
    /// being compared. An endpoint offering a protocol nothing handles
    /// negotiates it and then refuses every stream (P1-11).
    ///
    /// Gossip is in here although this crate did not author the handler. What
    /// must be served and what we wrote are different questions, and it is the
    /// first that the router and the endpoint both need answered — so the
    /// node, which is the thing that gossips, is what says so.
    pub fn protocols(&self) -> Protocols {
        vec![
            (alpn::PING.to_vec(), Box::new(PingProtocol) as Box<_>),
            (GOSSIP_ALPN.to_vec(), Box::new(self.swarm.clone())),
            (alpn::RAFT.to_vec(), Box::new(self.raft_protocol.clone())),
            (
                alpn::MEMBERLOG.to_vec(),
                Box::new(self.memberlog_protocol.clone()),
            ),
        ]
    }

    /// The connections this node holds, for any protocol added alongside Raft.
    pub fn connections(&self) -> &Connections {
        &self.connections
    }

    /// The core group with an address for each.
    ///
    /// What a join ticket is made of, and what a follower rotates around.
    pub fn core_addresses(&self) -> Vec<(MemberId, NodeAddr)> {
        self.state_machine.core_addresses()
    }

    /// The membership derived from the log so far, as this node enforces it.
    ///
    /// Behind the log by the moment it takes to rewrite the allowlist, so a
    /// member this reports is one this node already admits.
    pub fn membership(&self) -> MembershipState {
        self.enforced.borrow().clone()
    }

    /// Watches [`Self::membership`], for whoever has to react to it changing.
    ///
    /// A running node holds the database exclusively, so this is the only way
    /// anything outside the process can see the group: `distlib run` logs each
    /// change rather than leaving observers to poll a file they cannot open.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<MembershipState> {
        self.enforced.clone()
    }

    /// The Raft, for callers that need to inspect it.
    ///
    /// `None` on a follower, which has none: it holds the same log but takes no
    /// part in deciding it. **Owned rather than borrowed**, because the answer
    /// can change while the node runs — a promoted follower takes its seat —
    /// so there is nothing stable here to lend a reference to.
    pub fn raft(&self) -> Option<Raft<TypeConfig>> {
        self.seat.raft()
    }

    /// Whether this node votes on the log it holds.
    ///
    /// Read from the seat rather than remembered, for the same reason: a
    /// follower promoted a moment ago votes, and nothing restarted.
    /// Where members have announced they can be reached.
    ///
    /// Distinct from the core group's addresses, which come from the log: this
    /// is what members said about *themselves*, and it is the only source for
    /// a follower's address — see [`distlib_net::Directory`] and P2-14.
    pub fn known_addresses(&self) -> &Directory {
        &self.known_addresses
    }

    /// Where this node last told the group it is, as it signed it — `None`
    /// until it has said so once.
    ///
    /// For the availability heartbeat, which carries the same statement
    /// (phase 4's D1): signed at this node's log position, so a receiver's
    /// directory weighs it exactly as it weighs the announcement.
    pub fn own_address(&self) -> watch::Receiver<Option<SignedAddress>> {
        self.own_address.clone()
    }

    /// Asks a core node, once, where the group's members are — and answers
    /// whether one replied.
    ///
    /// The other half of 2a-5, which built only the startup ask. The phase
    /// plan carried the rest out of phase 2 with the reason it was left:
    /// *"There is no single place that detects it"* — a dial failure surfaces
    /// inside iroh, inside iroh-docs' downloader and at each protocol client
    /// separately, and the three do not agree on what "cannot be resolved"
    /// even means. So what was wanted first was *a single point where "we had
    /// no address for this member" is observable*, and `library.download` is
    /// one: it picks its own provider list, can see which of them
    /// [`Self::known_addresses`] cannot place, and is about to fail visibly if
    /// it goes ahead anyway.
    ///
    /// **A refresh, not a retry loop.** One ask of one core node, made by a
    /// caller that has already decided it needs one — which is what keeps this
    /// from becoming the thing that note warned against, a node answering
    /// every transient failure with an RPC. The follow loop's own startup ask
    /// is this same function; see `ask_for_the_directory` for why one answer
    /// is enough.
    pub async fn refresh_addresses(&self) -> bool {
        follower::ask_for_the_directory(self.id, &self.memberlog, &self.sources).await
    }

    pub fn is_core(&self) -> bool {
        self.seat.is_taken()
    }

    /// Resolves if this node discovers it has been expelled from its group.
    ///
    /// Never, on a core node: it holds the log itself, so there is nobody to
    /// refuse it. A follower learns it the only way it can — every core node
    /// that answers turns it away — and stops following at that point, because
    /// a node outside the group has nothing to fetch and no business asking.
    ///
    /// Callers are expected to shut down. That is a decision for whoever
    /// started the node rather than for the node, which is why this reports
    /// rather than exits.
    pub async fn expelled(&self) {
        let mut watcher = self.expelled.subscribe();
        loop {
            if *watcher.borrow_and_update() {
                return;
            }
            if watcher.changed().await.is_err() {
                // Unreachable while this node holds the sender, and a closed
                // channel is not an expulsion — so wait forever rather than
                // reporting one.
                std::future::pending::<()>().await;
            }
        }
    }

    /// How far this node has followed a log it does not vote on.
    ///
    /// Zero on a node that has only ever voted: it has the log pushed to it
    /// and tracks its position through Raft instead. A node the log *demotes*
    /// is pointed at what it had applied, so its follow loop picks up where
    /// its Raft left off — see [`StateMachineStore::resume_following`].
    pub fn followed_upto(&self) -> u64 {
        self.state_machine.followed_upto()
    }

    /// The Raft, or an error naming what this node is instead.
    fn as_core(&self) -> Result<Raft<TypeConfig>> {
        self.raft().ok_or(NodeError::NotCore)
    }

    /// Founds a group with `founders`, of whom this node must be one.
    ///
    /// Initialises Raft with the founding voters and commits the `GroupFounded`
    /// event, so the first thing in the log is the thing every later entry is
    /// checked against.
    pub async fn init_group(
        &self,
        founders: Vec<(MemberProfile, NodeAddr)>,
        settings: GroupSettings,
        secret_key: &SecretKey,
    ) -> Result<()> {
        let voters: BTreeMap<_, _> = founders
            .iter()
            .map(|(record, addr)| (RawMemberId::from(record.member_id), addr.clone()))
            .collect();

        // Raft's voters first, then the event: `client_write` needs a leader,
        // and there is none until the cluster is initialised.
        let raft = self.as_core()?;
        raft.initialize(voters).await.map_err(raft_failed)?;

        // `initialize` returns before an election has happened. With a single
        // founder that is instant, but with several the write below would be
        // refused with `ForwardToLeader { leader_id: None }` — there is simply
        // nobody to forward to yet. Wait for this node to become leader, since
        // it is the one holding the event to propose.
        raft.wait(Some(LEADERSHIP_TIMEOUT))
            .state(ServerState::Leader, "founding node becomes leader")
            .await
            .map_err(raft_failed)?;

        // The same pairs Raft was initialised with, so the log records the
        // addressing rather than leaving `raft.initialize` as its only home.
        self.propose(
            MembershipEvent::found(founders, settings, Timestamp::now())?,
            secret_key,
        )
        .await
        // Founding is entry one and takes no approvals; the index says nothing
        // the caller does not already know.
        .map(|_| ())
    }

    /// Commits a signed event through Raft, forwarding to the leader if needed.
    ///
    /// Only the leader can commit, and most nodes are not it: a member admitted
    /// to the core group later will be a follower, and so will a node that
    /// restarted while somebody else held the term. §4.3 has a member submit a
    /// proposal to *any* core node, so this hands it on rather than failing —
    /// openraft returns `ForwardToLeader` and expects the application to do the
    /// forwarding itself.
    ///
    /// Returns once the event is committed **and applied here**, so whatever
    /// the caller reads from [`Self::membership`] next already includes it.
    /// Committed alone was the leader's view: anywhere else the entry arrived
    /// with the next replication, and a caller asking at once whether its
    /// proposal was still pending found nothing under its index and reported a
    /// waiting expulsion as done (found by hand after phase 3).
    ///
    /// Signing happens here rather than in the caller because a proposal is
    /// made *against a particular membership* — see
    /// [`MembershipState::changed_at`] — and this node is the one that knows
    /// which membership it has seen. A caller handing in a pre-signed event
    /// would be stating a view it had no way to be sure of.
    /// Answers with the log index the entry was applied at.
    ///
    /// Which is what the proposer needs to say anything further about it. A
    /// change that took effect and one now waiting for approvals are both `Ok`
    /// — the entry committed and the fold accepted it either way — and the
    /// difference is a question about [`MembershipState::pending`], keyed by
    /// exactly this index.
    pub async fn propose(&self, event: MembershipEvent, secret_key: &SecretKey) -> Result<u64> {
        let index = self.propose_and_retry(&event, secret_key).await?;
        // `changed_at` moves on every applied entry the fold accepts, and a
        // committed proposal is one it accepted, so this cannot wait on an
        // index that will never be reached here.
        self.catch_up_to(index)
            .await
            .map_err(|_| NodeError::NotYetApplied { index })?;
        Ok(index)
    }

    /// [`Self::propose_once`], once more after catching up if it was stale.
    async fn propose_and_retry(
        &self,
        event: &MembershipEvent,
        secret_key: &SecretKey,
    ) -> Result<u64> {
        match self.propose_once(event, secret_key).await {
            // Somebody else changed the membership between reading it and
            // committing this. Catch up to what the group actually is, then
            // propose against that.
            //
            // Deliberately not re-signing with the `current` from the error:
            // that would assert a view of the group this node has not seen,
            // which is the exact thing the check exists to prevent. Waiting
            // until we really hold it costs a replication round and keeps the
            // statement honest.
            //
            // Not a core map, though: it is the whole core group as the caller
            // read it before the change that made it stale, so sending it again
            // would undo that change (#154). The caller reads the group again.
            Err(NodeError::Event(ConsensusError::StaleProposal { current, .. }))
                if !matches!(event, MembershipEvent::CoreGroupChanged { .. }) =>
            {
                self.catch_up_to(current).await?;
                self.propose_once(event, secret_key).await
            }
            other => other,
        }
    }

    /// Waits until this node's membership has reached `changed_at`.
    ///
    /// Failing here is not `NoLeader`: a leader demonstrably exists, since one
    /// just refused the proposal for being stale. What failed is this node
    /// keeping up with it.
    async fn catch_up_to(&self, changed_at: u64) -> Result<()> {
        let mut memberships = self.subscribe();
        let behind = || NodeError::BehindTheGroup { changed_at };

        tokio::time::timeout(LEADERSHIP_TIMEOUT, async {
            while memberships.borrow_and_update().changed_at() < changed_at {
                // The state machine is gone — shutdown. Returning `Ok` here
                // would send `propose` into a second attempt that is certain to
                // be stale for exactly the same reason as the first.
                memberships.changed().await.map_err(|_| behind())?;
            }
            Ok(())
        })
        .await
        .map_err(|_| behind())?
    }

    /// One attempt: sign against the membership this node holds, and commit it.
    async fn propose_once(&self, event: &MembershipEvent, secret_key: &SecretKey) -> Result<u64> {
        // Before signing, so an oversized proposal is refused to the caller
        // who can still fix it rather than replicated to everyone who cannot.
        // See [`MembershipEvent::within_limits`].
        event.within_limits()?;

        let event = SignedEvent::sign(
            secret_key,
            event.clone(),
            Timestamp::now(),
            self.membership().changed_at(),
        )?;

        // Read once, then acted on: a node promoted between these two lines
        // simply commits the next proposal the other way. Cloning the handle
        // out is also what keeps the seat's lock off an await.
        match self.seat.raft() {
            Some(raft) => self.commit_as_core(&raft, event).await,
            None => self.commit_as_follower(&self.sources, event).await,
        }
    }

    /// A follower's way to commit: hand it to a core node and let them.
    ///
    /// No leader hint to work from — a follower does not run Raft and so has no
    /// `ForwardToLeader` to be told — so it asks the core nodes it knows of,
    /// leader first, until one commits it. A core node that is not the leader
    /// hands it on to the leader it knows of, so any of them will do.
    async fn commit_as_follower(&self, sources: &SharedSources, event: SignedEvent) -> Result<u64> {
        let candidates = follower::read(sources).candidates(self.id);
        if candidates.is_empty() {
            return Err(NodeError::NoLeader);
        }

        let mut unreached = None;
        for (member, addr) in candidates {
            match self.memberlog.propose(member, &addr, event.clone()).await {
                Ok(index) => {
                    // The node that committed it holds it, so the follow loop
                    // has somewhere to fetch it from right now.
                    self.hints.send_replace(gossip::Hint::Reaches(index));
                    return Ok(index);
                }
                // The rules refused it. Every node reaches that verdict
                // identically, so asking somebody else cannot change it.
                Err(ProposeError::Rejected(error)) => return Err(NodeError::Event(error)),
                Err(error) => {
                    tracing::debug!(%member, %error, "a core node did not commit; trying another");
                    unreached = Some((member, error));
                }
            }
        }

        match unreached {
            Some((member, source)) => Err(NodeError::Forward {
                leader: member.into(),
                source,
            }),
            None => Err(NodeError::NoLeader),
        }
    }

    /// A voter's way to commit: through its own Raft, forwarding if it is not
    /// the leader.
    async fn commit_as_core(&self, raft: &Raft<TypeConfig>, event: SignedEvent) -> Result<u64> {
        let mut unreached = None;

        for _ in 0..PROPOSE_ATTEMPTS {
            let forward = match raft.client_write(event.clone()).await {
                // The write reached the log; `data` carries the state machine's
                // verdict on whether the event then took effect. A committed
                // event whose rules do not hold is skipped rather than fatal
                // (P1-8), so reporting `Ok` on the strength of the commit alone
                // would tell the caller something happened when it did not.
                Ok(written) => {
                    let index = written.log_id.index;
                    return written.data.map(|()| index).map_err(NodeError::Event);
                }
                Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward))) => forward,
                Err(other) => return Err(raft_failed(other)),
            };

            match (forward.leader_id, forward.leader_node) {
                (Some(leader), Some(addr)) => {
                    match self
                        .memberlog
                        .propose_to_leader(leader, &addr, event.clone())
                        .await
                    {
                        Ok(index) => return Ok(index),

                        // The rules refused it. Every node reaches that verdict
                        // identically, so asking somebody else cannot change it.
                        Err(ProposeError::Rejected(error)) => {
                            return Err(NodeError::Event(error));
                        }

                        // The hint was stale or the leader unreachable: the
                        // named node may have lost the term before we dialled
                        // it, or be dead. Our own Raft learns the new leader
                        // from the election, so ask it again rather than
                        // giving up — this is the case PROPOSE_ATTEMPTS exists
                        // for, and what the previous version got wrong by
                        // returning here.
                        //
                        // **Waiting for somebody else to lead, not for a fixed
                        // delay.** Asked again at once, Raft names the same
                        // leader until the election that replaces it has
                        // reached this node, and each attempt against a dead
                        // one costs a whole dial timeout — so a fixed delay
                        // spent the attempts racing the election, and lost it
                        // on a slow runner with P3-4's longer timeouts. The
                        // wait ends the moment the leadership moves, and its
                        // bound is what tells a leader that died from one that
                        // was briefly out of reach — see `LEADER_CHANGE_WAIT`.
                        Err(error) => {
                            tracing::debug!(%error, "forwarding failed; waiting for the leadership to move");
                            let _ = raft
                                .wait(Some(LEADER_CHANGE_WAIT))
                                .metrics(
                                    |metrics| metrics.current_leader != Some(leader),
                                    "a leader other than the one that did not answer",
                                )
                                .await;
                            unreached = Some((leader, error));
                        }
                    }
                }
                // An election is in progress, or this node has not heard from
                // one yet. There is nobody to forward to, so wait for a leader
                // and try again rather than failing a proposal that is only
                // momentarily unroutable.
                _ => {
                    raft.wait(Some(LEADERSHIP_TIMEOUT))
                        .metrics(
                            |metrics| metrics.current_leader.is_some(),
                            "a leader to emerge",
                        )
                        .await
                        .map_err(raft_failed)?;
                }
            }
        }
        // Out of attempts. Say which of the two happened: a leader we could not
        // get to commit is a different problem from never finding one.
        match unreached {
            Some((leader, source)) => Err(NodeError::Forward { leader, source }),
            None => Err(NodeError::NoLeader),
        }
    }

    /// Stops consensus and this node's background tasks.
    ///
    /// Not the router or the endpoint: the caller owns both, serves other
    /// subsystems on them, and shuts them down after this returns — see
    /// `distlib::Runtime::shutdown`, which keeps the order this used to have.
    /// Takes `&self` rather than `self` so the node can be shared.
    ///
    /// The local API serves from the same node this returns to, and an owning
    /// shutdown would mean prising it back out of the `Arc` they share.
    pub async fn shutdown(&self) {
        self.allowlist_updates.abort();
        self.evictions.abort();
        // Before anything can change role out from under the rest of this.
        self.role.abort();
        // Before Raft stops, so a change in flight is abandoned rather than
        // outliving the thing it was changing.
        stop(&self.core_group);
        // Abort rather than a graceful stop, and safe to be: the only durable
        // thing the follow loop does is `apply_followed`, whose commit runs
        // inside `spawn_blocking` — which tokio does *not* cancel, so a redb
        // transaction either never starts or runs to completion. Aborting
        // between the in-memory fold and that commit leaves the projection
        // ahead of the database, and the node is going away, so the database is
        // what the next start reads. There is nothing here a shutdown signal
        // would let it finish more tidily.
        stop(&self.follow);

        if let Some(raft) = self.seat.raft()
            && let Err(error) = raft.shutdown().await
        {
            tracing::warn!(%error, "raft did not shut down cleanly");
        }
        self.gossip.abort();
    }
}

/// Everything a node needs in order to change role without restarting.
///
/// Held by the task rather than by the node: nothing here is any use to a node
/// whose role is not changing.
struct Role {
    me: MemberId,
    config: Arc<Config>,
    network: RaftNetworkFactoryImpl,
    /// The Raft log store, for each Raft this node builds on being promoted.
    ///
    /// A clone of the one a voter's openraft writes. Promotion preserves it:
    /// the leader may still remember entries this node acknowledged as a
    /// retained learner before its previous seat was vacated.
    log: LogStore,
    state_machine: StateMachineStore,
    client: MemberlogClient,
    sources: SharedSources,
    expelled: watch::Sender<bool>,
    listens: gossip::Hints,
    seat: Seat,
    trusted: TrustedCore,
    follow: RoleTask,
    core_group: RoleTask,
}

impl Role {
    /// What the follow loop is started with, built fresh each time: a node can
    /// be started as a follower, promoted, and demoted back again, and the
    /// loop is a new task on each pass.
    fn following(&self) -> follower::Following {
        follower::Following {
            me: self.me,
            state_machine: self.state_machine.clone(),
            client: self.client.clone(),
            sources: Arc::clone(&self.sources),
            expelled: self.expelled.clone(),
        }
    }
}

/// Follows the log's answer to "does this node vote", for the life of the node.
///
/// One task rather than two because the two directions are mutually exclusive
/// and strictly alternating: a node is never waiting to be promoted and to be
/// demoted at the same time, and each transition is what makes the other one
/// reachable. Writing it as a sequence is also what keeps the ordering
/// guarantees each half needs — nothing can promote a node midway through
/// standing it down.
///
/// **As many times as the log says**, which was C12: this used to promote at
/// most once and demote at most once, and a node demoted and promoted again
/// stayed a follower while the log called it a voter. Going round again is
/// sound because promotion puts the node back exactly where a follower that
/// never voted starts for projection replay. Its existing Raft log is kept,
/// and the seating boundary protects the new seat during historical replay.
///
/// **Either half giving up ends the task**, so the loop never goes round on a
/// transition that did not happen. Today neither half can see its channel
/// close — the sender lives in the store this task holds a clone of, and
/// shutdown aborts the task rather than closing anything — but a `stand_down`
/// that returned without emptying the seat would otherwise be called again at
/// once, for ever.
async fn serve_role(role: Role) {
    if role.seat.raft().is_none() && retired_and_expelled(&role) {
        finish_expulsion(&role).await;
        return;
    }
    let mut seated_at = role.state_machine.promotion().map_or_else(
        || role.state_machine.membership().changed_at(),
        |promotion| promotion.at,
    );
    loop {
        if role.seat.raft().is_none() {
            let Some(at) = take_the_seat(&role).await else {
                return;
            };
            seated_at = at;
        }
        if !stand_down(&role, seated_at).await {
            return;
        }
    }
}

/// Waits for both application intent and a durable, uniform Raft removal
/// before standing down. Until then the old quorum still needs this node.
///
/// **Ordered, like its mirror, and for the same kind of reason:**
///
/// 1. **Stop reconciling**, before the Raft it reconciles goes away.
/// 2. **Empty the seat, then shut the Raft down.** In that order, so that the
///    moment a peer can no longer be answered is the moment this node stops
///    being a voter, rather than a window in which it answers with errors.
///    Shutting down matters on its own: a Raft left running campaigns for a
///    group that no longer counts its vote. **Not mutation-checked, and it
///    cannot be**: the handle is gone from the seat either way, so a Raft
///    that was shut down and one that was merely dropped look identical from
///    anywhere a test can stand. Deleting the call breaks nothing visible,
///    which is the reason to say so here rather than to delete it.
/// 3. **Point the follow cursor at what is already applied**, so the loop
///    picks up where the Raft left off instead of re-fetching the whole log.
/// 4. **Start following**, which is the part that matters — a demoted node
///    that does not follow freezes at the membership it held and goes on
///    enforcing that allowlist indefinitely.
///
/// The projection is *not* blanked, which is the asymmetry with promotion. It
/// is right already, having been built from the same log, and there is no
/// P1-22 window to climb back out of: a follower needs no consensus served to
/// it.
///
/// Expect this to take up to `follower::POLL` to have visible effect, because
/// gossip does not change sides — a demoted node keeps the announcing half it
/// had as a voter, so nothing pokes its new follow loop and the timer is what
/// it waits on.
///
/// Only a membership at least as new as `seated_at`, the one that seated this node,
/// can unseat it. A node catching up after promotion passes through older
/// ones — from a snapshot, or entries applied in more than one batch — and
/// those do not name it as core. Acting on one would stand it down, follow,
/// be promoted again and loop. It also covers a founder, which is not core
/// until it has founded.
///
/// Returns `true` once it has done it; `false` if the state machine went away,
/// as its mirror does.
async fn stand_down(role: &Role, seated_at: u64) -> bool {
    let mut memberships = role.state_machine.subscribe();
    let mut applied = role.state_machine.raft_membership();
    loop {
        {
            let seen = memberships.borrow_and_update();
            if may_retire(&seen, &applied.borrow_and_update(), role.me, seated_at) {
                break;
            }
        }
        tokio::select! {
            changed = memberships.changed() => { if changed.is_err() { return false; } }
            changed = applied.changed() => { if changed.is_err() { return false; } }
        }
    }
    tracing::info!("the log says this node no longer votes; standing down");

    stop(&role.core_group);
    if let Some(raft) = role.seat.vacate()
        && let Err(error) = raft.shutdown().await
    {
        // Kept going deliberately. The seat is already empty, so this node has
        // stopped being reachable as a voter whatever openraft's own task did
        // on the way out; refusing to start following on top of that would
        // leave it frozen, which is the failure being fixed.
        tracing::warn!(%error, "consensus did not shut down cleanly after demotion");
    }
    if let Err(error) = role.state_machine.resume_following().await {
        // Also not fatal: the cursor stays at zero and the first poll re-fetches
        // the log, which the fold refuses entry by entry and converges anyway.
        tracing::warn!(%error, "could not point the follow cursor at what is applied");
    }

    // The new group may be disjoint from startup configuration. Follow the
    // voters whose committed configuration authorized this retirement.
    let membership = role.state_machine.raft_membership().borrow().clone();
    let core = membership
        .voter_ids()
        .filter_map(|id| {
            Some((
                MemberId::try_from(id).ok()?,
                membership.membership().get_node(&id)?.clone(),
            ))
        })
        .collect();
    *role.sources.lock().unwrap_or_else(PoisonError::into_inner) = Sources { core, leader: None };

    if !role.state_machine.membership().is_member(&role.me) {
        finish_expulsion(role).await;
        return false;
    }

    let listens = role.listens.clone();
    start(
        &role.follow,
        tokio::spawn(follower::follow(role.following(), listens)),
    );
    tracing::info!("this node is now a follower");
    true
}

/// Keep the retirement responder alive until a voter confirms cleanup. In
/// particular, do not start the ordinary follower loop: its application-level
/// refusal would tell the CLI to exit before the leader could acknowledge it.
async fn finish_expulsion(role: &Role) {
    let mut network = role.network.clone();
    tracing::warn!(member = %role.me, "this node was expelled; waiting for a voter to confirm Raft learner cleanup before exiting");
    loop {
        for (id, addr) in follower::read(&role.sources).candidates(role.me) {
            let client = network.new_client(RawMemberId::from(id), &addr).await;
            if tokio::time::timeout(core_group::RETRY, client.forget_me()).await == Ok(true) {
                role.expelled.send_replace(true);
                return;
            }
        }
        tracing::debug!(member = %role.me, "no voter confirmed Raft learner cleanup; retrying");
        tokio::time::sleep(core_group::RETRY).await;
    }
}

fn retired_and_expelled(role: &Role) -> bool {
    let membership = role.state_machine.membership();
    membership.group_id().is_some()
        && !membership.is_member(&role.me)
        && role
            .state_machine
            .raft_membership()
            .borrow()
            .log_id()
            .is_some()
}

/// Application intent alone is insufficient. Both views must have reached
/// the seating boundary, and the durable Raft configuration must be uniform
/// and exclude this node. Joint configurations still require the old quorum.
fn may_retire(
    wanted: &MembershipState,
    applied: &openraft::StoredMembership<RawMemberId, NodeAddr>,
    me: MemberId,
    seated_at: u64,
) -> bool {
    wanted.group_id().is_some()
        && wanted.changed_at() >= seated_at
        && !wanted.is_core(&me)
        && applied.log_id().is_some_and(|id| id.index >= seated_at)
        && applied.membership().get_joint_config().len() == 1
        && !applied.voter_ids().any(|id| id == RawMemberId::from(me))
}

/// A Raft for this node, over its stores, as a voter starts one or a promoted
/// node does.
async fn new_raft(
    role: &Role,
) -> std::result::Result<Raft<TypeConfig>, Box<openraft::error::Fatal<RawMemberId>>> {
    Raft::new(
        RawMemberId::from(role.me),
        Arc::clone(&role.config),
        role.network.clone(),
        role.log.clone(),
        role.state_machine.clone(),
    )
    .await
    .map_err(Box::new)
}

/// Waits for the log to say this node votes, and then makes that true.
///
/// The other half of promotion; the first is [`crate::raft::core_group`] on
/// the leader, which adds this node as a learner and then as a voter. Neither
/// half can go first: openraft will not make a node a voter until it is a
/// learner it can replicate to, and this node cannot be replicated to until it
/// has a Raft. So the *log* is the rendezvous — the leader enacts what the
/// committed `CoreGroupChanged` says, and this node reads the same entry and
/// sits down.
///
/// **Ordered, and the order is the whole of it:**
///
/// 1. **Stop following.** The follow loop and a Raft both write the state
///    machine; leaving both running would interleave two pictures of the log.
/// 2. **Persist who to trust during replay**, from the signed desired core and
///    the core immediately before this node was promoted — step 3 resets that
///    projection, and step 4
///    leaves a node with no voters of its own for as long as it takes to be
///    caught up. See [`TrustedCore`].
/// 3. **Blank the state machine, preserving the Raft log.** The leader may
///    still track this node as a retained learner, so acknowledged entries
///    cannot disappear. The projection is replayed from the preserved log and
///    subsequent replication. `seated_at` prevents historical demotions from
///    unseating this promotion. See [`StateMachineStore::reset_for_promotion`].
/// 4. **Build the Raft and sit down.** `Raft::new` reads the applied state
///    once, at construction, which is why step 3 cannot come after it.
/// 5. **Start reconciling**, so this node does its share of putting later
///    core-group changes into effect once it is leader material.
///
/// Returns the `changed_at` of the membership that seated it; `None` if it
/// could not, or if the state machine went away.
async fn take_the_seat(role: &Role) -> Option<u64> {
    let Role {
        me,
        state_machine,
        seat,
        trusted,
        follow,
        core_group,
        ..
    } = role;

    let mut memberships = state_machine.subscribe();
    let seated_at = loop {
        let core_at = {
            let seen = memberships.borrow_and_update();
            seen.is_core(me).then(|| seen.changed_at())
        };
        if let Some(at) = core_at {
            break at;
        }
        if memberships.changed().await.is_err() {
            tracing::error!("membership channel closed");
            return None;
        }
    };
    tracing::info!("the log says this node votes now; taking a seat in consensus");

    stop(follow);
    // A disjoint replacement still needs its old leader, but referrals only
    // locate log sources. Authority comes exclusively from the signed fold:
    // the desired core and the core immediately before this node's promotion.
    let promotion = match state_machine.prepare_promotion(*me).await {
        Ok(promotion) => promotion,
        Err(error) => {
            // Left a follower, and a working one: the seat stays empty, so the
            // protocols go on refusing and the projection is whatever was last
            // written. What is lost is the follow loop, which has already been
            // stopped — so this node is frozen at the membership it holds until it
            // is restarted, which is the honest outcome of a disk that will not
            // take a write.
            tracing::error!(%error, "could not clear this node's state to be promoted");
            return None;
        }
    };
    trusted.replace(promotion.voters);

    let raft = match new_raft(role).await {
        Ok(raft) => raft,
        Err(error) => {
            tracing::error!(%error, "could not start consensus after being promoted");
            return None;
        }
    };

    seat.take(raft.clone());
    start(
        core_group,
        tokio::spawn(core_group::enact(
            raft,
            state_machine.clone(),
            role.network.clone(),
        )),
    );
    tracing::info!("this node is now a voter");
    Some(seated_at)
}

/// Joins the group's gossip topic once there is a group, and stays on it.
///
/// A core node announces its applied index on every membership change; a
/// follower listens and pokes its follow loop. Both have to *join* — you
/// broadcast on a topic you are subscribed to — so the two differ only in which
/// half of the conversation they take part in.
///
/// Bootstrapping is the interesting part: gossip needs somebody to talk to
/// before it can find anybody else. The members this node already knows of come
/// from the log it has just folded, which is why this waits for one.
/// What [`join_topic`] needs beyond the log, bundled because the parameter list
/// would otherwise run past what CLAUDE.md allows — and because these three
/// travel together: they are what it takes to say where this node is and to
/// write down where everyone else is.
struct TopicParts {
    endpoint: Endpoint,
    secret: SecretKey,
    directory: Directory,
    /// Where each address statement is kept once signed — see
    /// [`MembershipNode::own_address`].
    said: watch::Sender<Option<SignedAddress>>,
}

async fn join_topic(
    swarm: Gossip,
    state_machine: StateMachineStore,
    me: MemberId,
    is_core: bool,
    hints: watch::Sender<gossip::Hint>,
    joining: TopicParts,
) {
    let TopicParts {
        endpoint,
        secret,
        directory,
        said,
    } = joining;
    let mut memberships = state_machine.subscribe();
    let group = loop {
        if let Some(group) = memberships.borrow_and_update().group_id() {
            break group;
        }
        if memberships.changed().await.is_err() {
            tracing::error!("membership channel closed");
            return;
        }
    };

    // Everyone else in the group *that this node can reach* — for a follower
    // that has just started, the core group and nobody else. A member named
    // here with no address is not merely skipped by gossip: the dial fails, and
    // that strands them on every topic until they dial us (C25). The rest need
    // no introduction from here: gossip finds its own way once nobody is
    // stranded, and the catalogue offers each member as its address is heard.
    let bootstrap = gossip::reachable(&memberships.borrow_and_update(), me, &directory)
        .map(|(member, _)| member.endpoint_id())
        .collect();

    let topic = match swarm.subscribe(gossip::topic_for(group), bootstrap).await {
        Ok(topic) => topic,
        Err(error) => {
            // The poll still stands, so this is a lost optimisation rather
            // than a lost node.
            tracing::warn!(%error, "could not join the group's gossip topic");
            return;
        }
    };
    tracing::debug!(%group, "joined the group's gossip topic");

    // **Both halves, on both roles**, which is a change. A core node used to
    // drop the receiver and a follower the sender, because the only thing on
    // this topic was the log's index: a core node had nothing to learn from it
    // and a follower nothing to say. Addresses are the other way round —
    // everyone has something to say, because a follower's address is written
    // down nowhere else, and everyone has something to learn, core nodes
    // included, since they are the ones who will answer for the group.
    let (sender, receiver) = topic.split();

    // None of these three returns while the node is running; the task is
    // stopped by being aborted, like the rest of the node's tasks.
    tokio::join!(
        gossip::announce_address(
            &endpoint,
            &secret,
            &state_machine,
            &sender,
            directory.learned(),
            &said,
        ),
        async {
            if is_core {
                gossip::announce_log(state_machine.clone(), &sender).await;
            }
        },
        gossip::listen(receiver, hints, directory, is_core),
    );
}

/// Starts [`follow_membership`], and answers with the membership it enforces.
fn enforce_membership(
    state_machine: &StateMachineStore,
    writer: AllowlistWriter,
    addresses: AddressBook,
    hooks: AllowlistHooks,
) -> (JoinHandle<()>, watch::Receiver<MembershipState>) {
    // Starts from what the log holds: until the router serves, nobody can
    // connect to be refused.
    let (enforced_tx, enforced) = watch::channel(state_machine.membership());
    let task = tokio::spawn(follow_membership(
        state_machine.clone(),
        writer,
        addresses,
        hooks,
        enforced_tx,
    ));
    (task, enforced)
}

/// Carries every membership change from the log to the allowlist.
///
/// The one place where consensus and the transport meet. It runs until the
/// state machine is dropped, which only happens when the node shuts down.
async fn follow_membership(
    state_machine: StateMachineStore,
    writer: AllowlistWriter,
    addresses: AddressBook,
    hooks: AllowlistHooks,
    enforced: watch::Sender<MembershipState>,
) {
    let mut memberships = state_machine.subscribe();
    let mut raft_memberships = state_machine.raft_membership();
    // What this task last published, so it can tell a core node that has
    // *moved* from one it is merely seeing again. Empty at startup, which is
    // right: there are no connections yet to be pointing anywhere stale.
    let mut published: BTreeMap<MemberId, NodeAddr> = BTreeMap::new();

    loop {
        // Read before waiting, so a node that applied entries before this task
        // started enforces them immediately rather than at the next change —
        // which for a quiet group could be a very long time.
        let membership = memberships.borrow_and_update().clone();
        // Install the Raft-only exception before revoking application access.
        // Both watches publish after storage commits, including Raft-only
        // entries, so cleanup also closes obsolete Raft connections.
        let raft_membership = raft_memberships.borrow_and_update().clone();
        hooks.set_raft_peers(
            raft_membership
                .nodes()
                .filter_map(|(id, _)| MemberId::try_from(*id).ok())
                .chain(
                    state_machine
                        .promotion()
                        .into_iter()
                        .flat_map(|promotion| promotion.voters),
                ),
        );

        let replaying = state_machine
            .promotion()
            .is_some_and(|promotion| membership.changed_at() < promotion.at);
        if membership.group_id().is_some() && !replaying {
            // Where the voters are, from the same source that decides who they
            // are. Empty on a follower, which holds no `StoredMembership` of
            // its own — it learns the same addresses from the core nodes that
            // serve it the log.
            //
            // Before the allowlist, deliberately: publishing *who* this node
            // will talk to while it still cannot say *where* they are leaves a
            // window in which anything reacting to the allowlist dials an id
            // that resolves to nothing. Doing it in this order means one
            // observable event — the allowlist changing — implies both.
            let core: BTreeMap<MemberId, NodeAddr> =
                state_machine.core_addresses().into_iter().collect();
            addresses.learn_all(core.iter().map(|(member, addr)| (*member, addr)));
            let_go_of_the_moved(&published, &core, &hooks);
            published = core;

            let members: Vec<MemberId> = membership.allowlist().collect();
            tracing::debug!(count = members.len(), "allowlist derived from the log");
            writer.replace(members);
        } else {
            // A blank or historically replayed projection must not replace
            // either bootstrap membership or the newer pre-promotion allowlist.
            tracing::debug!(
                replaying,
                "holding the allowlist until the membership projection is current"
            );
        }
        // Only now: whoever reads the membership can rely on it being enforced.
        if !replaying {
            enforced.send_replace(membership);
        }

        tokio::select! {
            changed = memberships.changed() => { if changed.is_err() { return; } }
            changed = raft_memberships.changed() => { if changed.is_err() { return; } }
        }
    }
}

/// Closes every connection to a core node the log has just moved.
///
/// Knowing the new address is not enough, and this is the half of P1-23 that
/// only an end-to-end test could have found. A node that is killed — which is
/// what a machine being renumbered looks like from outside — sends no close
/// frame, so its peers keep a connection that still reads as open to a socket
/// with nothing behind it. While they do, iroh will not reach that endpoint id
/// anywhere else: the dial goes to the pinned path and times out, forever,
/// however often it is retried and whatever address is handed to it. Measured
/// on the real thing: the leader dialled the correct new address every 52
/// milliseconds for thirty seconds and never once connected. With this, the
/// same test converges in about a second.
///
/// So the log moving a member is the signal to let go of what we hold — and it
/// goes through [`AllowlistHooks`] rather than [`Connections`] because that map
/// holds only the connections this codebase opened. The one that pins the path
/// is usually iroh-gossip's, which we have no handle on at all; every
/// connection the endpoint makes or accepts passes through the hooks, so they
/// are the only place that can reach it. `distlib-net`'s `tests/moved.rs` is
/// the demonstration, down at the level where the behaviour lives.
///
/// Nothing tells [`Connections`] separately. Closing a connection is enough:
/// its cache drops one that is no longer live and dials a fresh one, which is
/// what that check is for. Removing the entry as well passed every test with
/// the line deleted, which is the definition of code nothing is holding up.
///
/// On every node, not only the leader. A follower fetching the log over
/// `distlib/memberlog/0` holds exactly the same stale handle, and it has no
/// [`crate::raft::core_group`] loop to notice.
fn let_go_of_the_moved(
    published: &BTreeMap<MemberId, NodeAddr>,
    core: &BTreeMap<MemberId, NodeAddr>,
    hooks: &AllowlistHooks,
) {
    for (member, addr) in core {
        // `is_some_and`, so a member seen for the first time is not "moved".
        if published.get(member).is_some_and(|was| was != addr) {
            let closed = hooks.close_connections_to(*member);
            tracing::info!(%member, closed, "the log moves a core node; letting go of it");
        }
    }
}

#[cfg(test)]
mod retirement_tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use openraft::{
        CommittedLeaderId, Entry, EntryPayload, LogId, Membership, StoredMembership,
        storage::RaftStateMachine,
    };
    use std::collections::BTreeSet;

    fn promotion_history() -> (SecretKey, MemberId, MemberId, Vec<(u64, SignedEvent)>) {
        let old = SecretKey::from_bytes(&[21; 32]);
        let promoted = SecretKey::from_bytes(&[22; 32]);
        let teacher = MemberId::from(old.public());
        let me = MemberId::from(promoted.public());
        let expelled = MemberId::from(SecretKey::from_bytes(&[23; 32]).public());
        let profile = |member_id| MemberProfile {
            member_id,
            display_name: "member".into(),
        };
        let founding = MembershipEvent::found(
            vec![(profile(teacher), NodeAddr::default())],
            Default::default(),
            Timestamp::from_millis(1),
        )
        .unwrap();
        let events = vec![
            (&old, founding),
            (
                &old,
                MembershipEvent::MemberAdded {
                    member: profile(me),
                },
            ),
            (
                &old,
                MembershipEvent::MemberAdded {
                    member: profile(expelled),
                },
            ),
            (
                &old,
                MembershipEvent::MemberExpelled {
                    member: expelled,
                    reason: "removed".into(),
                },
            ),
            (
                &old,
                MembershipEvent::CoreGroupChanged {
                    core: vec![(me, NodeAddr::default())],
                },
            ),
            // A later address change must not replace the predecessor of this
            // promotion with the newly promoted node itself.
            (
                &promoted,
                MembershipEvent::CoreGroupChanged {
                    core: vec![(
                        me,
                        NodeAddr::default().with_direct(([127, 0, 0, 1], 1234).into()),
                    )],
                },
            ),
        ]
        .into_iter()
        .enumerate()
        .map(|(i, (key, event))| {
            let at = i as u64 + 1;
            (
                at,
                SignedEvent::sign(key, event, Timestamp::from_millis(at), at - 1).unwrap(),
            )
        })
        .collect();
        (promoted, teacher, expelled, events)
    }

    #[tokio::test]
    async fn promotion_does_not_trust_an_expelled_predecessor() {
        let (key, teacher, _, mut events) = promotion_history();
        let me = MemberId::from(key.public());
        // The teacher was recorded when it promoted this node. Expelling it
        // afterward must remove its authority even though the predecessor is
        // still remembered, including after a restart before seating.
        events.push((
            7,
            SignedEvent::sign(
                &key,
                MembershipEvent::MemberExpelled {
                    member: teacher,
                    reason: "former voter expelled".into(),
                },
                Timestamp::from_millis(7),
                6,
            )
            .unwrap(),
        ));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("expelled-predecessor.redb");
        let state = StateMachineStore::open(&path).unwrap();
        for verdict in state.apply_followed(7, &events).await.unwrap() {
            verdict.unwrap();
        }
        assert!(!state.membership().is_member(&teacher));
        drop(state);
        let state = StateMachineStore::open(&path).unwrap();
        let promotion = state.prepare_promotion(me).await.unwrap();
        assert_eq!(promotion.voters, BTreeSet::from([me]));
        assert_eq!(state.promotion().unwrap().voters, promotion.voters);
    }

    #[tokio::test]
    async fn promotion_trust_ignores_referrals_and_keeps_the_signed_predecessor_after_restart() {
        let (key, teacher, expelled, events) = promotion_history();
        let me = MemberId::from(key.public());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("authority.redb");
        let state = StateMachineStore::open(&path).unwrap();
        assert!(
            state
                .apply_followed(6, &events)
                .await
                .unwrap()
                .iter()
                .all(|v| v.is_ok())
        );
        drop(state);
        let db = Arc::new(Database::create(&path).unwrap());
        let state = StateMachineStore::from_database(Arc::clone(&db)).unwrap();
        let endpoint = Endpoint::builder(iroh::endpoint::presets::Minimal)
            .secret_key(key)
            .relay_mode(iroh::endpoint::RelayMode::Disabled)
            .bind_addr(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
            .unwrap()
            .bind()
            .await
            .unwrap();
        let connections = Connections::new();
        let role = Role {
            me,
            config: Arc::new(
                Config {
                    enable_tick: false,
                    ..Default::default()
                }
                .validate()
                .unwrap(),
            ),
            network: RaftNetworkFactoryImpl::new(endpoint.clone(), connections.clone()),
            log: LogStore::from_database(db).unwrap(),
            state_machine: state.clone(),
            client: MemberlogClient::new(
                endpoint.clone(),
                connections,
                AddressBook::default(),
                Directory::default(),
            ),
            // A stale seatless source referred us to an expelled peer, and
            // omitted the legitimate old leader. Neither fact is authority.
            sources: Arc::new(Mutex::new(Sources {
                core: vec![(expelled, NodeAddr::default())],
                leader: Some(expelled),
            })),
            expelled: watch::channel(false).0,
            listens: watch::channel(gossip::Hint::MayHaveMissed).1,
            seat: Seat::default(),
            trusted: TrustedCore::default(),
            follow: RoleTask::default(),
            core_group: RoleTask::default(),
        };
        assert_eq!(take_the_seat(&role).await, Some(6));
        assert_eq!(
            state.promotion().unwrap().voters,
            BTreeSet::from([me, teacher])
        );
        assert!(role.trusted.contains(&teacher));
        assert!(
            !role.trusted.contains(&expelled),
            "referrals cannot grant bootstrap consensus authority"
        );
        stop(&role.core_group);
        role.seat.vacate().unwrap().shutdown().await.unwrap();
        endpoint.close().await;
    }

    #[tokio::test]
    async fn promotion_replay_preserves_the_enforced_allowlist_until_the_boundary() {
        let (key, _, expelled, events) = promotion_history();
        let me = MemberId::from(key.public());
        let dir = tempfile::tempdir().unwrap();
        let mut state = StateMachineStore::open(dir.path().join("replay.redb")).unwrap();
        assert!(
            state
                .apply_followed(6, &events)
                .await
                .unwrap()
                .iter()
                .all(|v| v.is_ok())
        );
        let current = state.membership();
        state.prepare_promotion(me).await.unwrap();
        let entries: Vec<Entry<TypeConfig>> = events
            .into_iter()
            .map(|(at, event)| Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, RawMemberId::from(me)), at),
                payload: EntryPayload::Normal(event),
            })
            .collect();
        // The replayed historical projection still admits the expelled key.
        state.apply(entries[..3].to_vec()).await.unwrap();
        let (writer, mut allowlist) = distlib_net::allowlist(me, current.allowlist());
        let hooks = AllowlistHooks::new(allowlist.clone());
        let (published, mut enforced) = watch::channel(current.clone());
        let observer = tokio::spawn(follow_membership(
            state.clone(),
            writer,
            AddressBook::default(),
            hooks,
            published,
        ));
        // Either publication is observable, including an unchanged allowlist
        // replacement. This also catches the two suppression mutations separately.
        assert!(
            tokio::time::timeout(Duration::from_millis(50), async {
                tokio::select! { _ = allowlist.changed() => {}, _ = enforced.changed() => {} }
            })
            .await
            .is_err(),
            "historical replay must not publish membership or its allowlist"
        );
        assert!(!allowlist.is_allowed(&expelled));
        assert_eq!(*enforced.borrow(), current);
        state.apply(entries[3..].to_vec()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), enforced.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*enforced.borrow(), current);
        assert!(!allowlist.is_allowed(&expelled));
        observer.abort();
    }

    #[tokio::test]
    async fn promotion_authority_survives_restart_and_expires_after_raft_catches_up() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("promotion.redb");
        let mut store = StateMachineStore::open(&path).unwrap();
        let key = SecretKey::from_bytes(&[1; 32]);
        let me = MemberId::from(key.public());
        let teacher = MemberId::from(SecretKey::from_bytes(&[2; 32]).public());
        let event = MembershipEvent::found(
            vec![(
                crate::MemberProfile {
                    member_id: me,
                    display_name: "me".into(),
                },
                NodeAddr::default(),
            )],
            Default::default(),
            Timestamp::from_millis(1),
        )
        .unwrap();
        let signed = SignedEvent::sign(&key, event, Timestamp::from_millis(1), 0).unwrap();
        let founded = Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, RawMemberId::from(me)), 10),
            payload: EntryPayload::Normal(signed),
        };
        store.apply([founded.clone()]).await.unwrap();
        store.prepare_promotion(me).await.unwrap();
        drop(store);
        let mut store = StateMachineStore::open(&path).unwrap();
        assert!(
            starts_as_core(&store, me, &[]),
            "resume even in the blank window"
        );
        let promotion = store.promotion().unwrap();
        assert_eq!(promotion.at, 10);
        assert_eq!(promotion.voters, BTreeSet::from([me]));
        store.apply([founded]).await.unwrap();
        assert!(store.membership().group_id().is_some());
        assert!(
            store.promotion().is_some(),
            "old founded state does not close replay trust"
        );
        let membership = configuration(11, vec![vec![teacher]]);
        store
            .apply([Entry {
                log_id: *membership.log_id().as_ref().unwrap(),
                payload: EntryPayload::Membership(membership.membership().clone()),
            }])
            .await
            .unwrap();
        assert!(store.promotion().is_none());
        drop(store);
        assert!(
            StateMachineStore::open(&path)
                .unwrap()
                .promotion()
                .is_none()
        );
    }

    fn configuration(
        index: u64,
        voters: Vec<Vec<MemberId>>,
    ) -> StoredMembership<RawMemberId, NodeAddr> {
        let configs: Vec<BTreeSet<_>> = voters
            .into_iter()
            .map(|ids| ids.into_iter().map(RawMemberId::from).collect())
            .collect();
        let nodes: BTreeMap<_, _> = configs
            .iter()
            .flatten()
            .map(|id| (*id, NodeAddr::default()))
            .collect();
        StoredMembership::new(
            Some(LogId::new(
                CommittedLeaderId::new(1, RawMemberId::default()),
                index,
            )),
            Membership::new(configs, nodes),
        )
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // One restart scenario, kept in execution order.
    async fn retirement_waits_for_durable_uniform_removal_and_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("retirement.redb");
        let mut store = StateMachineStore::open(&path).unwrap();
        let key = SecretKey::from_bytes(&[1; 32]);
        let old = MemberId::from(key.public());
        let new = MemberId::from(SecretKey::from_bytes(&[2; 32]).public());
        let profile = |id| crate::MemberProfile {
            member_id: id,
            display_name: "member".into(),
        };
        let founded = MembershipEvent::found(
            vec![(profile(old), NodeAddr::default())],
            Default::default(),
            Timestamp::from_millis(1),
        )
        .unwrap();
        let events = [
            founded,
            MembershipEvent::MemberAdded {
                member: profile(new),
            },
            MembershipEvent::CoreGroupChanged {
                core: vec![(new, NodeAddr::default())],
            },
        ];
        for (index, event) in events.into_iter().enumerate() {
            let signed = SignedEvent::sign(
                &key,
                event,
                Timestamp::from_millis(1),
                store.membership().changed_at(),
            )
            .unwrap();
            let results = store
                .apply([Entry {
                    log_id: LogId::new(
                        CommittedLeaderId::new(1, RawMemberId::from(old)),
                        index as u64 + 1,
                    ),
                    payload: EntryPayload::Normal(signed),
                }])
                .await
                .unwrap();
            assert!(results.iter().all(|result| result.is_ok()));
        }
        let wanted = store.membership();
        assert!(!wanted.is_core(&old));
        let at = wanted.changed_at();
        let old_config = configuration(at + 1, vec![vec![old, new]]);
        let joint = configuration(at + 2, vec![vec![old, new], vec![new]]);
        let final_config = configuration(at + 3, vec![vec![new]]);
        assert!(!may_retire(&wanted, &StoredMembership::default(), old, at));
        assert!(!may_retire(&wanted, &old_config, old, at));
        assert!(!may_retire(&wanted, &joint, old, at));
        assert!(may_retire(&wanted, &final_config, old, at));
        assert!(
            !may_retire(&wanted, &final_config, old, at + 10),
            "historical replay must not retire a new seat"
        );

        let mut changes = store.raft_membership();
        store
            .apply([Entry {
                log_id: *old_config.log_id().as_ref().unwrap(),
                payload: EntryPayload::Membership(old_config.membership().clone()),
            }])
            .await
            .unwrap();
        assert!(
            changes.has_changed().unwrap(),
            "Raft-only entries must wake retirement"
        );
        assert_eq!(*changes.borrow_and_update(), old_config);
        drop(changes);
        drop(store);
        let mut store = StateMachineStore::open(&path).unwrap();
        assert!(
            starts_as_core(&store, old, &[]),
            "pending removal must resume voting after restart"
        );
        assert_eq!(*store.raft_membership().borrow(), old_config);
        store
            .apply([Entry {
                log_id: *final_config.log_id().as_ref().unwrap(),
                payload: EntryPayload::Membership(final_config.membership().clone()),
            }])
            .await
            .unwrap();
        assert!(
            !starts_as_core(&store, old, &[(old, NodeAddr::default())]),
            "a completed removal overrides stale bootstrap config"
        );
        assert!(may_retire(
            &store.membership(),
            &store.raft_membership().borrow(),
            old,
            at
        ));
    }
}
