//! A node that runs consensus: Raft, its stores, and the membership it derives.
//!
//! This is where the phase 1 claim becomes true — the connection allowlist stops
//! being configuration and starts being a projection of the committed log. Two
//! background tasks make that so: one carries every membership change from the
//! state machine to the allowlist, and one closes connections the change
//! invalidates (§4.4).

use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};

use distlib_core::{MemberId, RawMemberId};
use distlib_net::{AllowlistHooks, AllowlistWriter, alpn, ping::PingProtocol};
use iroh::{Endpoint, protocol::Router};
use openraft::{Config, Raft, ServerState};
use redb::Database;
use tokio::task::JoinHandle;

use crate::{
    error::ConsensusError,
    event::{MemberRecord, MembershipEvent, Timestamp},
    raft::{
        LogStore, NodeAddr, RaftNetworkFactoryImpl, RaftProtocol, StateMachineStore, TypeConfig,
    },
    signed::SignedEvent,
    state::MembershipState,
};

/// The file holding a node's whole Raft state, under its data directory.
pub const RAFT_DB: &str = "raft.redb";

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

    /// Commits a signed event through Raft.
    ///
    /// Returns once it is committed and applied here; other members apply it as
    /// replication reaches them.
    pub async fn propose(&self, event: SignedEvent) -> Result<()> {
        self.raft
            .client_write(event)
            .await
            .map(|_| ())
            .map_err(raft_failed)
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
