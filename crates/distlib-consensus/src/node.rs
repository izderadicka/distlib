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
    sync::{Arc, Mutex},
    time::Duration,
};

use distlib_core::{MemberId, NodeAddr, RawMemberId};
use distlib_net::{AllowlistHooks, AllowlistWriter, Connections, alpn, ping::PingProtocol};
use iroh::{Endpoint, SecretKey, protocol::Router};
use openraft::{
    Config, Raft, ServerState,
    error::{ClientWriteError, RaftError},
};
use redb::Database;
use tokio::task::JoinHandle;

use crate::{
    error::ConsensusError,
    event::{MemberRecord, MembershipEvent, Timestamp},
    raft::{
        LogStore, MemberlogClient, MemberlogProtocol, ProposeError, RaftNetworkFactoryImpl,
        RaftProtocol, StateMachineStore, TypeConfig,
        follower::{self, SharedSources, Sources},
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
/// before the router is up, and says in one place what each kind of node
/// answers.
pub fn alpns(core: bool) -> Vec<Vec<u8>> {
    let mut alpns = vec![alpn::PING.to_vec()];
    if core {
        // Only a core node can answer either: consensus is between voters, and
        // answering a proposal or serving the log needs a Raft to do it with.
        alpns.push(alpn::RAFT.to_vec());
        alpns.push(alpn::MEMBERLOG.to_vec());
    }
    alpns
}

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

    /// A voters-only operation was asked of a node that does not vote.
    #[error("this node is not part of the core group")]
    NotCore,

    /// This node could not catch up to the membership the group is at.
    ///
    /// Distinct from [`NodeError::NoLeader`]: a leader exists and answered —
    /// it refused the proposal as being made against a superseded membership —
    /// and this node then failed to reach that membership itself.
    #[error("this node did not catch up to the membership at index {changed_at}")]
    BehindTheGroup { changed_at: u64 },
}

/// The result of starting or driving a node.
pub type Result<T> = std::result::Result<T, NodeError>;

fn raft_failed(source: impl std::error::Error + Send + Sync + 'static) -> NodeError {
    NodeError::Raft(Box::new(source))
}

/// How this node takes part in the group.
///
/// Not a configuration switch but a reading of it: a node in the core group
/// votes, and everyone else follows. Everything either kind does with the log —
/// the projection, the allowlist it derives, the eviction task, the connections
/// — is identical, so this is a difference in how the log *arrives*, and
/// nothing else.
enum Role {
    /// A voter. Receives the log through Raft and can commit to it.
    Core { raft: Raft<TypeConfig> },

    /// Everyone else. Pulls the log over `distlib/memberlog/0` (§4.2).
    Follower {
        /// Held, not detached: dropping it would leave the node frozen at
        /// whatever membership it last fetched, still enforcing it.
        follow: JoinHandle<()>,
        /// Where to fetch from, kept current by the task above.
        sources: SharedSources,
    },
}

/// A running consensus node.
///
/// Owns the router serving its peers, the tasks that keep the allowlist in step
/// with the log, and — depending on whether it votes — either a Raft or the
/// task that follows one. Dropping it does not stop those; call
/// [`Self::shutdown`].
pub struct MembershipNode {
    id: MemberId,
    role: Role,
    state_machine: StateMachineStore,
    router: Router,
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
        endpoint: Endpoint,
        hooks: AllowlistHooks,
        allowlist: AllowlistWriter,
        data_dir: &Path,
        core: Vec<(MemberId, NodeAddr)>,
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
        // A second handle on the same database, for serving followers. openraft
        // takes ownership of the one above, and redb readers see a consistent
        // snapshot without blocking the writer — so this cannot slow consensus.
        let served_log = LogStore::from_database(Arc::clone(&db)).map_err(raft_failed)?;
        let state_machine = StateMachineStore::from_database(db).map_err(raft_failed)?;

        // The node's connections, shared by everything it speaks: openraft's
        // replication, forwarded proposals, following the log, and whatever
        // protocols come later. All of them draw on the same set.
        let connections = Connections::new();
        let memberlog = MemberlogClient::new(endpoint.clone(), connections.clone());

        // The log decides, once there is one. Before that, configuration does —
        // the same rule the allowlist follows, and for the same reason: a node
        // cannot read a log it has no way to reach.
        let membership = state_machine.membership();
        let is_core = if membership.group_id().is_some() {
            membership.core().contains(&id)
        } else {
            core.iter().any(|(member, _)| *member == id)
        };

        let (role, router) = if is_core {
            // Forwarding does not go through this factory: a proposal travels
            // over `distlib/memberlog/0`, which every member may speak, while
            // the factory serves Raft's own replication between voters.
            let network = RaftNetworkFactoryImpl::new(endpoint.clone(), connections.clone());
            let raft = Raft::new(
                RawMemberId::from(id),
                Arc::new(Config::default().validate().map_err(raft_failed)?),
                network,
                log,
                state_machine.clone(),
            )
            .await
            .map_err(raft_failed)?;

            let founding_core = core.iter().map(|(member, _)| *member).collect();
            let router = Router::builder(endpoint)
                .accept(alpn::PING, PingProtocol)
                .accept(
                    alpn::RAFT,
                    RaftProtocol::new(raft.clone(), state_machine.clone(), founding_core),
                )
                .accept(
                    alpn::MEMBERLOG,
                    MemberlogProtocol::new(raft.clone(), served_log, state_machine.clone()),
                )
                .spawn();

            (Role::Core { raft }, router)
        } else {
            // Ping and nothing else. A follower has no Raft to answer
            // consensus with and none to commit a proposal into, so
            // advertising either would mean negotiating a protocol and then
            // refusing every stream on it.
            let router = Router::builder(endpoint)
                .accept(alpn::PING, PingProtocol)
                .spawn();

            let sources: SharedSources = Arc::new(Mutex::new(Sources { core, leader: None }));
            let follow = tokio::spawn(follower::follow(
                id,
                state_machine.clone(),
                memberlog.clone(),
                Arc::clone(&sources),
            ));

            (Role::Follower { follow, sources }, router)
        };

        let allowlist_updates = tokio::spawn(follow_membership(state_machine.clone(), allowlist));
        let evictions = tokio::spawn(hooks.evict_expelled());

        Ok(Self {
            id,
            role,
            state_machine,
            router,
            allowlist_updates,
            evictions,
            memberlog,
            connections,
        })
    }

    /// This node's identity.
    pub fn id(&self) -> MemberId {
        self.id
    }

    /// The endpoint this node serves and dials on.
    pub fn endpoint(&self) -> &Endpoint {
        self.router.endpoint()
    }

    /// The connections this node holds, for any protocol added alongside Raft.
    pub fn connections(&self) -> &Connections {
        &self.connections
    }

    /// The membership derived from the log so far.
    pub fn membership(&self) -> MembershipState {
        self.state_machine.membership()
    }

    /// Watches the membership, for whoever has to react to it changing.
    ///
    /// A running node holds the database exclusively, so this is the only way
    /// anything outside the process can see the group: `distlib run` logs each
    /// change rather than leaving observers to poll a file they cannot open.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<MembershipState> {
        self.state_machine.subscribe()
    }

    /// The Raft, for callers that need to inspect it.
    ///
    /// `None` on a follower, which has none: it holds the same log but takes no
    /// part in deciding it.
    pub fn raft(&self) -> Option<&Raft<TypeConfig>> {
        match &self.role {
            Role::Core { raft } => Some(raft),
            Role::Follower { .. } => None,
        }
    }

    /// Whether this node votes on the log it holds.
    pub fn is_core(&self) -> bool {
        matches!(self.role, Role::Core { .. })
    }

    /// How far this node has followed a log it does not vote on.
    ///
    /// Zero on a voter, which has the log pushed to it and tracks its position
    /// through Raft instead.
    pub fn followed_upto(&self) -> u64 {
        self.state_machine.followed_upto()
    }

    /// The Raft, or an error naming what this node is instead.
    fn as_core(&self) -> Result<&Raft<TypeConfig>> {
        self.raft().ok_or(NodeError::NotCore)
    }

    /// Founds a group with `founders`, of whom this node must be one.
    ///
    /// Initialises Raft with the founding voters and commits the `GroupFounded`
    /// event, so the first thing in the log is the thing every later entry is
    /// checked against.
    pub async fn init_group(
        &self,
        founders: Vec<(MemberRecord, NodeAddr)>,
        secret_key: &SecretKey,
    ) -> Result<()> {
        let voters: BTreeMap<_, _> = founders
            .iter()
            .map(|(record, addr)| (RawMemberId::from(record.member_id), addr.clone()))
            .collect();
        let records: Vec<MemberRecord> = founders.into_iter().map(|(record, _)| record).collect();

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

        self.propose(
            MembershipEvent::found(records, Timestamp::now())?,
            secret_key,
        )
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
    ///
    /// Signing happens here rather than in the caller because a proposal is
    /// made *against a particular membership* — see
    /// [`MembershipState::changed_at`] — and this node is the one that knows
    /// which membership it has seen. A caller handing in a pre-signed event
    /// would be stating a view it had no way to be sure of.
    pub async fn propose(&self, event: MembershipEvent, secret_key: &SecretKey) -> Result<()> {
        match self.propose_once(&event, secret_key).await {
            // Somebody else changed the membership between reading it and
            // committing this. Catch up to what the group actually is, then
            // propose against that.
            //
            // Deliberately not re-signing with the `current` from the error:
            // that would assert a view of the group this node has not seen,
            // which is the exact thing the check exists to prevent. Waiting
            // until we really hold it costs a replication round and keeps the
            // statement honest.
            Err(NodeError::Event(ConsensusError::StaleProposal { current, .. })) => {
                self.catch_up_to(current).await?;
                self.propose_once(&event, secret_key).await
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
        let mut memberships = self.state_machine.subscribe();
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
    async fn propose_once(&self, event: &MembershipEvent, secret_key: &SecretKey) -> Result<()> {
        let event = SignedEvent::sign(
            secret_key,
            event.clone(),
            Timestamp::now(),
            self.membership().changed_at(),
        )?;

        match &self.role {
            Role::Core { raft } => self.commit_as_core(raft, event).await,
            Role::Follower { sources, .. } => self.commit_as_follower(sources, event).await,
        }
    }

    /// A follower's way to commit: hand it to a core node and let them.
    ///
    /// No leader hint to work from — a follower does not run Raft and so has no
    /// `ForwardToLeader` to be told — so it asks the core nodes it knows of,
    /// leader first, until one commits it. A node that is not the leader
    /// forwards through its own Raft, so any of them will do.
    async fn commit_as_follower(&self, sources: &SharedSources, event: SignedEvent) -> Result<()> {
        let candidates = follower::read(sources).candidates(self.id);
        if candidates.is_empty() {
            return Err(NodeError::NoLeader);
        }

        let mut unreached = None;
        for (member, addr) in candidates {
            match self.memberlog.propose(member, &addr, event.clone()).await {
                Ok(()) => return Ok(()),
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
    async fn commit_as_core(&self, raft: &Raft<TypeConfig>, event: SignedEvent) -> Result<()> {
        let mut unreached = None;

        for _ in 0..PROPOSE_ATTEMPTS {
            let forward = match raft.client_write(event.clone()).await {
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
        // Not `Unreachable`: that names a member who could not be reached, and
        // the only id we have here is one that is not a member id at all.
        // Reporting it against `self.id` would blame the one node that is
        // certainly reachable.
        let leader = MemberId::try_from(leader).map_err(|error| {
            ProposeError::NotCommitted(format!(
                "the leader's id {leader} is not a member id: {error}"
            ))
        })?;
        self.memberlog.propose(leader, &addr, event).await
    }

    /// Stops consensus, the router and the background tasks.
    /// Takes `&self` rather than `self` so the node can be shared.
    ///
    /// The local API serves from the same node this returns to, and an owning
    /// shutdown would mean prising it back out of the `Arc` they share.
    pub async fn shutdown(&self) {
        self.allowlist_updates.abort();
        self.evictions.abort();
        match &self.role {
            Role::Core { raft } => {
                if let Err(error) = raft.shutdown().await {
                    tracing::warn!(%error, "raft did not shut down cleanly");
                }
            }
            Role::Follower { follow, .. } => follow.abort(),
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
