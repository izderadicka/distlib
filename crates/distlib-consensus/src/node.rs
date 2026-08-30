//! A node that runs consensus: Raft, its stores, and the membership it derives.
//!
//! This is where the phase 1 claim becomes true — the connection allowlist stops
//! being configuration and starts being a projection of the committed log. Two
//! background tasks make that so: one carries every membership change from the
//! state machine to the allowlist, and one closes connections the change
//! invalidates (§4.4).

use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use distlib_core::{MemberId, RawMemberId};
use distlib_net::{AllowlistHooks, AllowlistWriter, alpn, ping::PingProtocol};
use iroh::{Endpoint, protocol::Router};
use openraft::{
    Config, Raft, RaftNetworkFactory, ServerState,
    error::{ClientWriteError, RaftError},
};
use redb::Database;
use tokio::task::JoinHandle;

use crate::{
    error::ConsensusError,
    event::{MemberRecord, MembershipEvent, Timestamp},
    raft::{
        LogStore, NodeAddr, ProposeError, RaftClient, RaftNetworkFactoryImpl, RaftProtocol,
        StateMachineStore, TypeConfig,
    },
    signed::SignedEvent,
    state::MembershipState,
};

/// The file holding a node's whole Raft state, under its data directory.
pub const RAFT_DB: &str = "raft.redb";

/// How many times a proposal re-checks who the leader is before giving up.
///
/// More than one because the answer can change underneath: the leader named in
/// a `ForwardToLeader` may have lost the term by the time we dial it.
const PROPOSE_ATTEMPTS: usize = 3;

/// How long to wait before asking again who the leader is.
///
/// Long enough for replication to tell this node about a term it missed, short
/// enough not to stall a proposal that would otherwise succeed immediately.
const FORWARD_RETRY_DELAY: Duration = Duration::from_millis(250);

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
}

/// The result of starting or driving a node.
pub type Result<T> = std::result::Result<T, NodeError>;

fn raft_failed(source: impl std::error::Error + Send + Sync + 'static) -> NodeError {
    NodeError::Raft(Box::new(source))
}

/// A running consensus node.
///
/// Owns the Raft, the router serving its peers, and the tasks that keep the
/// allowlist in step with the log. Dropping it does not stop those; call
/// [`Self::shutdown`].
pub struct MembershipNode {
    id: MemberId,
    raft: Raft<TypeConfig>,
    state_machine: StateMachineStore,
    router: Router,
    /// Held, not detached: it is the only thing keeping the allowlist writable,
    /// and dropping it would freeze membership at whatever was last applied.
    allowlist_updates: JoinHandle<()>,
    evictions: JoinHandle<()>,
    /// Clients for peers this node has forwarded a proposal to.
    ///
    /// Keyed by leader, because leadership moves and the previous leader's
    /// client stays useful when it comes back.
    forward_clients: Mutex<HashMap<RawMemberId, RaftClient>>,
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
    pub async fn start(
        endpoint: Endpoint,
        hooks: AllowlistHooks,
        allowlist: AllowlistWriter,
        data_dir: &Path,
    ) -> Result<Self> {
        let id = MemberId::from(endpoint.id());
        let path = data_dir.join(RAFT_DB);
        let db = Arc::new(
            Database::create(&path).map_err(|source| NodeError::Database {
                path,
                source: Box::new(source),
            })?,
        );

        let log = LogStore::from_database(Arc::clone(&db)).map_err(raft_failed)?;
        let state_machine = StateMachineStore::from_database(db).map_err(raft_failed)?;

        let raft = Raft::new(
            RawMemberId::from(id),
            Arc::new(Config::default().validate().map_err(raft_failed)?),
            RaftNetworkFactoryImpl::new(endpoint.clone()),
            log,
            state_machine.clone(),
        )
        .await
        .map_err(raft_failed)?;

        // Ping and Raft: exactly what this router serves, which is what the
        // endpoint must have been told to advertise.
        let router = Router::builder(endpoint)
            .accept(alpn::PING, PingProtocol)
            .accept(alpn::RAFT, RaftProtocol::new(raft.clone()))
            .spawn();

        let allowlist_updates = tokio::spawn(follow_membership(state_machine.clone(), allowlist));
        let evictions = tokio::spawn(hooks.evict_expelled());

        Ok(Self {
            id,
            raft,
            state_machine,
            router,
            allowlist_updates,
            evictions,
            forward_clients: Mutex::new(HashMap::new()),
        })
    }

    /// This node's identity.
    pub fn id(&self) -> MemberId {
        self.id
    }

    /// The membership derived from the log so far.
    pub fn membership(&self) -> MembershipState {
        self.state_machine.membership()
    }

    /// The Raft, for callers that need to propose or inspect it.
    pub fn raft(&self) -> &Raft<TypeConfig> {
        &self.raft
    }

    /// Founds a group with `founders`, of whom this node must be one.
    ///
    /// Initialises Raft with the founding voters and commits the `GroupFounded`
    /// event, so the first thing in the log is the thing every later entry is
    /// checked against.
    pub async fn init_group(
        &self,
        founders: Vec<(MemberRecord, NodeAddr)>,
        secret_key: &iroh::SecretKey,
    ) -> Result<()> {
        let voters: BTreeMap<_, _> = founders
            .iter()
            .map(|(record, addr)| (RawMemberId::from(record.member_id), addr.clone()))
            .collect();
        let records: Vec<MemberRecord> = founders.into_iter().map(|(record, _)| record).collect();

        // Raft's voters first, then the event: `client_write` needs a leader,
        // and there is none until the cluster is initialised.
        self.raft.initialize(voters).await.map_err(raft_failed)?;

        // `initialize` returns before an election has happened. With a single
        // founder that is instant, but with several the write below would be
        // refused with `ForwardToLeader { leader_id: None }` — there is simply
        // nobody to forward to yet. Wait for this node to become leader, since
        // it is the one holding the event to propose.
        self.raft
            .wait(Some(LEADERSHIP_TIMEOUT))
            .state(ServerState::Leader, "founding node becomes leader")
            .await
            .map_err(raft_failed)?;

        let at = Timestamp::now();
        let event = MembershipEvent::found(records, at)?;
        self.propose(SignedEvent::sign(secret_key, event, at)?)
            .await
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
    /// Returns once the event is committed. If this node is the leader that is
    /// also when it has been applied here; otherwise it arrives with the next
    /// replication.
    pub async fn propose(&self, event: SignedEvent) -> Result<()> {
        let mut unreached = None;

        for _ in 0..PROPOSE_ATTEMPTS {
            let forward = match self.raft.client_write(event.clone()).await {
                // The write reached the log; `data` carries the state machine's
                // verdict on whether the event then took effect. A committed
                // event whose rules do not hold is skipped rather than fatal
                // (P1-8), so reporting `Ok` on the strength of the commit alone
                // would tell the caller something happened when it did not.
                Ok(written) => return written.data.map_err(NodeError::Event),
                Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward))) => forward,
                Err(other) => return Err(raft_failed(other)),
            };

            match (forward.leader_id, forward.leader_node) {
                (Some(leader), Some(addr)) => {
                    match self.forward(leader, addr, event.clone()).await {
                        Ok(()) => return Ok(()),

                        // The rules refused it. Every node reaches that verdict
                        // identically, so asking somebody else cannot change it.
                        Err(ProposeError::Rejected(error)) => {
                            return Err(NodeError::Event(error));
                        }

                        // The hint was stale or the leader unreachable: the
                        // named node may have lost the term before we dialled
                        // it. Our own Raft learns the new leader from
                        // replication, so ask it again rather than giving up —
                        // this is the case PROPOSE_ATTEMPTS exists for, and
                        // what the previous version got wrong by returning here.
                        Err(error) => {
                            tracing::debug!(%error, "forwarding failed; asking again");
                            unreached = Some((leader, error));
                            tokio::time::sleep(FORWARD_RETRY_DELAY).await;
                        }
                    }
                }
                // An election is in progress, or this node has not heard from
                // one yet. There is nobody to forward to, so wait for a leader
                // and try again rather than failing a proposal that is only
                // momentarily unroutable.
                _ => {
                    self.raft
                        .wait(Some(LEADERSHIP_TIMEOUT))
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

    /// Hands a proposal to the leader over the raft ALPN.
    ///
    /// Returns the leader's outcome unwrapped, so the caller can tell a verdict
    /// — which no amount of retrying will change, since every node reaches it
    /// identically — from a failure to reach the leader, which retrying might.
    async fn forward(
        &self,
        leader: RawMemberId,
        addr: NodeAddr,
        event: SignedEvent,
    ) -> std::result::Result<(), ProposeError> {
        tracing::debug!(leader = %leader, "forwarding a proposal to the leader");
        self.client_for(leader, addr).await.propose(event).await
    }

    /// A client for `leader`, reused across forwards.
    ///
    /// Building one per proposal would pay a fresh dial and TLS handshake to a
    /// peer this node almost certainly already has a live raft connection to —
    /// and now that a failed forward is retried, it would pay it again on every
    /// attempt. `RaftClient` caches its own connection, so keeping the client
    /// keeps the connection.
    async fn client_for(&self, leader: RawMemberId, addr: NodeAddr) -> RaftClient {
        // Scoped so the guard is gone before the await below. Holding a
        // blocking lock across a suspension point is how an executor gets
        // wedged, and clippy is right to refuse it.
        {
            let clients = self.lock_clients();
            if let Some(client) = clients.get(&leader) {
                return client.clone();
            }
        }

        let client = RaftNetworkFactoryImpl::new(self.router.endpoint().clone())
            .new_client(leader, &addr)
            .await;

        // Two concurrent proposals to a new leader can both get here and one
        // client is discarded. That costs nothing — `new_client` does not dial,
        // it only records where to — and is cheaper than holding the lock.
        self.lock_clients().insert(leader, client.clone());
        client
    }

    fn lock_clients(&self) -> std::sync::MutexGuard<'_, HashMap<RawMemberId, RaftClient>> {
        self.forward_clients
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Stops consensus, the router and the background tasks.
    pub async fn shutdown(self) {
        self.allowlist_updates.abort();
        self.evictions.abort();
        if let Err(error) = self.raft.shutdown().await {
            tracing::warn!(%error, "raft did not shut down cleanly");
        }
        if let Err(error) = self.router.shutdown().await {
            tracing::warn!(%error, "router did not shut down cleanly");
        }
    }
}

/// Carries every membership change from the log to the allowlist.
///
/// The one place where consensus and the transport meet. It runs until the
/// state machine is dropped, which only happens when the node shuts down.
async fn follow_membership(state_machine: StateMachineStore, writer: AllowlistWriter) {
    let mut memberships = state_machine.subscribe();
    loop {
        // Read before waiting, so a node that applied entries before this task
        // started enforces them immediately rather than at the next change —
        // which for a quiet group could be a very long time.
        let membership = memberships.borrow_and_update().clone();

        if membership.group_id().is_some() {
            let members: Vec<MemberId> = membership.allowlist().collect();
            tracing::debug!(count = members.len(), "allowlist derived from the log");
            writer.replace(members);
        } else {
            // No `GroupFounded` yet, so the log has nothing to say about who
            // belongs. Publishing its empty membership here would wipe the
            // bootstrap seed and leave core nodes unable to reach each other —
            // which is precisely how they would replicate the founding entry.
            tracing::debug!("no group founded yet; leaving the bootstrap allowlist in place");
        }

        if memberships.changed().await.is_err() {
            return;
        }
    }
}
