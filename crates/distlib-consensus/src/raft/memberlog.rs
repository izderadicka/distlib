//! `distlib/memberlog/0` — what a member says to a core node about the log.
//!
//! Split from `distlib/raft/0` deliberately, and the split is a security
//! boundary rather than tidiness. Raft's own RPCs — `AppendEntries`, `Vote`,
//! `InstallSnapshot` — are a conversation *between voters*, and a node that
//! processes them from a non-voter can have its consensus disrupted by any
//! member who happens to be in the allowlist. Submitting a proposal is a
//! different conversation, held between *any* member and a core node: §4.3 has
//! a member submit a proposal to any core node, and §4.4 the same for
//! expulsion.
//!
//! Keeping both on one ALPN meant a node serving proposals was also serving
//! Raft to whoever asked. Now `distlib/raft/0` is voters-only and refuses
//! everyone else, while this protocol is open to every member and can do
//! nothing but ask.
//!
//! Phase 1b adds the other half — fetching the log — on this same ALPN, which
//! is where §4.2's non-core followers get their copy.

use distlib_core::{MemberId, NodeAddr};
use distlib_net::{Connections, alpn};
use iroh::{
    Endpoint,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
};
use openraft::Raft;
use serde::{Deserialize, Serialize};

use std::time::Duration;

use crate::{
    error::ConsensusError,
    raft::{
        log_store::LogStore, network::MAX_RPC_BYTES, state_machine::StateMachineStore,
        types::TypeConfig,
    },
    signed::SignedEvent,
};

/// How long a core node will wait for a proposal to commit before answering.
///
/// `client_write` waits for the entry to be committed, and a leader that has
/// lost quorum waits for one that is not coming. Without a bound that parks
/// this node's accept-loop task — it cannot even notice the peer hanging up —
/// and every later proposal on the same connection queues behind it.
const COMMIT_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a proposer waits for the whole exchange.
///
/// Independent of [`COMMIT_TIMEOUT`] rather than derived from it: this bounds a
/// peer that stops answering at all, which is the case the server-side bound
/// cannot help with. Longer, so a peer that is merely slow gets to answer
/// before this fires and reports it as unreachable.
const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(45);

/// What a member asks a core node.
#[derive(Debug, Serialize, Deserialize)]
enum Request {
    /// A membership event the sender wants committed.
    ///
    /// Only the leader can commit one, so a core node that is not the leader
    /// still has to hand it on — which it does through its own Raft, exactly as
    /// if the proposal had originated there.
    Propose(SignedEvent),

    /// Everything committed since `cursor`.
    ///
    /// §4.2's non-core members hold the whole log and derive from it; this is
    /// how they get it. A cursor of zero asks for the group from its founding.
    From { cursor: u64 },
}

/// What the core node answers.
#[derive(Debug, Serialize, Deserialize)]
enum Response {
    Proposed(ProposeOutcome),
    Fetched(Fetched),
}

/// What came back from asking for the log.
#[derive(Debug, Serialize, Deserialize)]
pub enum Fetched {
    /// Committed membership events, and how far they reach.
    Entries {
        /// The serving node's applied index — the caller's next cursor.
        ///
        /// Not the last event's index, because most entries carry no event: a
        /// cursor taken from the last event would re-request the gap forever.
        up_to: u64,
        /// The events in `(cursor, up_to]`, each with its log index.
        events: Vec<(u64, SignedEvent)>,
        /// Where to ask next time.
        source: Source,
    },

    /// The serving node has no group yet, so there is nothing to hand over.
    NoGroup,

    /// The cursor is below what this node's log still holds.
    ///
    /// Everything before `first_available` was purged after a snapshot. A
    /// follower this far behind cannot be caught up from entries alone and
    /// needs the state itself — which is phase 1b's next problem, not this
    /// one, since the log is tiny and openraft snapshots at 5000 entries.
    TooFarBehind { first_available: u64 },
}

/// Where a follower should ask next, and who is worth asking first.
///
/// Carried on every answer rather than configured: §4.5 has `CoreGroupChanged`
/// tell followers where to fetch from, but the event carries member ids and no
/// addresses, so the addresses have to travel with the log itself.
#[derive(Debug, Serialize, Deserialize)]
pub struct Source {
    /// The current core group, with somewhere to reach each of them.
    pub core: Vec<(MemberId, NodeAddr)>,

    /// The leader, if this node knows of one.
    ///
    /// Worth preferring: Raft guarantees the leader holds every committed
    /// entry, so reading from it is as current as a follower can get.
    pub leader: Option<MemberId>,
}

/// What became of a forwarded proposal.
#[derive(Debug, Serialize, Deserialize)]
pub enum ProposeOutcome {
    /// Committed and applied.
    Applied,
    /// Committed, then refused by the rules — the log has it, the state does not.
    Rejected(ConsensusError),
    /// Never made it into the log.
    NotCommitted(String),
}

/// Why asking for the log produced nothing.
///
/// One case, because the outcomes that are not failures — no group yet, purged
/// past the cursor — are [`Fetched`] variants rather than errors.
#[derive(Debug, thiserror::Error)]
#[error("could not fetch the log from {member}: {message}")]
pub struct FetchFailed {
    pub member: MemberId,
    pub message: String,
}

/// Why a forwarded proposal did not take effect.
#[derive(Debug, thiserror::Error)]
pub enum ProposeError {
    /// The peer could not be reached, or answered nonsense.
    #[error("could not reach {member}: {message}")]
    Unreachable { member: MemberId, message: String },

    /// The peer was reached but did not commit — it may have lost the term.
    #[error("the leader did not commit the proposal: {0}")]
    NotCommitted(String),

    /// The event committed and was then refused by the rules.
    ///
    /// Distinct from the others because retrying cannot help: every node
    /// reaches the same verdict.
    #[error(transparent)]
    Rejected(ConsensusError),
}

/// The **answering** side of `distlib/memberlog/0`.
///
/// Only this half needs a Raft, and that is the whole shape of the protocol:
/// any member may *ask* — that is [`MemberlogClient`], which needs nothing but
/// a connection — and only a core node can answer, because answering means
/// committing through consensus.
///
/// It belongs on core nodes alone, and phase 1b's roles are what will put it
/// there: until then [`crate::MembershipNode`] installs it on every node, and a
/// node with no group of its own accepts a proposal only to fail it with
/// [`ProposeOutcome::NotCommitted`] — harmless, but not the intended shape.
#[derive(Clone)]
pub struct MemberlogProtocol {
    raft: Raft<TypeConfig>,
    /// A second handle on the same database openraft writes through.
    ///
    /// Reading it here rather than going through openraft is deliberate: redb
    /// gives readers a consistent snapshot without blocking the writer, so
    /// serving a follower cannot slow consensus down.
    log: LogStore,
    state_machine: StateMachineStore,
}

// `ProtocolHandler` requires `Debug`, and `Raft` does not implement it.
impl std::fmt::Debug for MemberlogProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemberlogProtocol").finish_non_exhaustive()
    }
}

impl MemberlogProtocol {
    /// Serves the log backed by `raft`.
    pub fn new(raft: Raft<TypeConfig>, log: LogStore, state_machine: StateMachineStore) -> Self {
        Self {
            raft,
            log,
            state_machine,
        }
    }

    async fn answer(&self, request: Request) -> Response {
        match request {
            Request::Propose(event) => Response::Proposed(self.propose(event).await),
            Request::From { cursor } => Response::Fetched(self.fetch(cursor)),
        }
    }

    /// Everything committed since `cursor`.
    fn fetch(&self, cursor: u64) -> Fetched {
        if self.state_machine.membership().group_id().is_none() {
            return Fetched::NoGroup;
        }

        // Only what has been *applied*. Entries beyond it are committed but not
        // yet part of anybody's membership, and serving them would hand over a
        // decision the group has not finished making.
        let up_to = self.state_machine.last_applied_index();

        match self.log.first_available() {
            Ok(first) if cursor.saturating_add(1) < first => {
                return Fetched::TooFarBehind {
                    first_available: first,
                };
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(%error, "could not read the purge watermark");
                return Fetched::NoGroup;
            }
        }

        match self.log.events_after(cursor, up_to) {
            Ok(events) => Fetched::Entries {
                up_to,
                events,
                source: self.source(),
            },
            Err(error) => {
                // Nothing to hand over rather than a protocol error: the caller
                // will ask another core node, which is the right response to
                // one node's storage misbehaving.
                tracing::warn!(%error, "could not read the log for a follower");
                Fetched::NoGroup
            }
        }
    }

    /// Who to ask next time, from Raft's own view of the group.
    fn source(&self) -> Source {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow();

        Source {
            core: metrics
                .membership_config
                .nodes()
                .filter_map(|(id, addr)| Some((MemberId::try_from(*id).ok()?, addr.clone())))
                .collect(),
            leader: metrics
                .current_leader
                .and_then(|id| MemberId::try_from(id).ok()),
        }
    }

    async fn propose(&self, event: SignedEvent) -> ProposeOutcome {
        let written =
            match tokio::time::timeout(COMMIT_TIMEOUT, self.raft.client_write(event)).await {
                Ok(written) => written,
                Err(_) => {
                    return ProposeOutcome::NotCommitted(format!(
                        "the entry did not commit within {COMMIT_TIMEOUT:?}"
                    ));
                }
            };

        match written {
            // The write reached the log; `data` is the state machine's verdict
            // on whether it then took effect.
            Ok(written) => match written.data {
                Ok(()) => ProposeOutcome::Applied,
                Err(rejected) => ProposeOutcome::Rejected(rejected),
            },
            // Includes this node having lost leadership since the sender's hint
            // was issued. Its own `ForwardToLeader` is no use to them — their
            // Raft learns the new leader from replication — so it travels as a
            // message rather than as routing advice.
            Err(error) => ProposeOutcome::NotCommitted(error.to_string()),
        }
    }
}

impl ProtocolHandler for MemberlogProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        // No authorisation here on purpose. Every member may propose — §4.3 and
        // §4.4 both say so — and *what* they may propose is decided by
        // `MembershipState::apply`, which every node runs identically. A check
        // here would be a second opinion that only this node holds.
        loop {
            let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                // The peer closed the connection: the normal way this ends.
                return Ok(());
            };

            let encoded = recv
                .read_to_end(MAX_RPC_BYTES)
                .await
                .map_err(AcceptError::from_err)?;
            let request: Request = postcard::from_bytes(&encoded).map_err(AcceptError::from_err)?;

            let encoded =
                postcard::to_stdvec(&self.answer(request).await).map_err(AcceptError::from_err)?;

            send.write_all(&encoded)
                .await
                .map_err(AcceptError::from_err)?;
            send.finish().map_err(AcceptError::from_err)?;
        }
    }
}

/// Talks `distlib/memberlog/0` to one core node.
///
/// Deliberately free of openraft's error types, unlike
/// [`crate::raft::RaftClient`]: those exist because openraft's traits dictate
/// them, and nothing here implements one.
#[derive(Debug, Clone)]
pub struct MemberlogClient {
    endpoint: Endpoint,
    connections: Connections,
}

impl MemberlogClient {
    /// Dials core nodes from `endpoint`, reusing the node's connections.
    pub fn new(endpoint: Endpoint, connections: Connections) -> Self {
        Self {
            endpoint,
            connections,
        }
    }

    /// Asks `member`, which should be a core node, to commit `event`.
    pub async fn propose(
        &self,
        member: MemberId,
        addr: &NodeAddr,
        event: SignedEvent,
    ) -> Result<(), ProposeError> {
        let unreachable = |message: String| ProposeError::Unreachable { member, message };

        let exchange = self.exchange(member, addr, Request::Propose(event));
        let answer = tokio::time::timeout(EXCHANGE_TIMEOUT, exchange)
            .await
            .map_err(|_| unreachable(format!("no answer within {EXCHANGE_TIMEOUT:?}")))?
            .map_err(unreachable)?;

        match answer {
            Response::Fetched(_) => Err(ProposeError::NotCommitted(
                "the peer answered a fetch to a proposal".to_owned(),
            )),
            Response::Proposed(ProposeOutcome::Applied) => Ok(()),
            Response::Proposed(ProposeOutcome::Rejected(error)) => {
                Err(ProposeError::Rejected(error))
            }
            Response::Proposed(ProposeOutcome::NotCommitted(error)) => {
                Err(ProposeError::NotCommitted(error))
            }
        }
    }

    /// Asks `member` for everything committed since `cursor`.
    ///
    /// The answer is an outcome, not a success or a failure: a node with no
    /// group and one that has purged past the cursor have both answered
    /// correctly, and a follower does something different with each.
    pub async fn fetch(
        &self,
        member: MemberId,
        addr: &NodeAddr,
        cursor: u64,
    ) -> Result<Fetched, FetchFailed> {
        let failed = |message: String| FetchFailed { member, message };
        let exchange = self.exchange(member, addr, Request::From { cursor });

        match tokio::time::timeout(EXCHANGE_TIMEOUT, exchange)
            .await
            .map_err(|_| failed(format!("no answer within {EXCHANGE_TIMEOUT:?}")))?
            .map_err(failed)?
        {
            Response::Fetched(fetched) => Ok(fetched),
            Response::Proposed(_) => {
                Err(failed("the peer answered a proposal to a fetch".to_owned()))
            }
        }
    }

    /// One request, one answer.
    ///
    /// A failure drops the cached connection: a half-open one that is never
    /// replaced would fail every later call to this peer.
    async fn exchange(
        &self,
        member: MemberId,
        addr: &NodeAddr,
        request: Request,
    ) -> Result<Response, String> {
        match self.try_exchange(member, addr, request).await {
            Ok(response) => Ok(response),
            Err(error) => {
                // Only this peer's memberlog connection. A failure here says
                // nothing about other peers, or about Raft's connection to this
                // one.
                self.connections.forget(member, alpn::MEMBERLOG);
                Err(error)
            }
        }
    }

    async fn try_exchange(
        &self,
        member: MemberId,
        addr: &NodeAddr,
        request: Request,
    ) -> Result<Response, String> {
        let encoded = postcard::to_stdvec(&request).map_err(failed("encoding the request"))?;
        let addr = addr
            .to_endpoint_addr(member)
            .map_err(|error| error.to_string())?;
        let connection = self
            .connections
            .get_or_connect(&self.endpoint, member, addr, alpn::MEMBERLOG)
            .await
            .map_err(failed("connecting"))?;

        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .map_err(failed("opening a stream"))?;
        send.write_all(&encoded).await.map_err(failed("sending"))?;
        send.finish().map_err(failed("sending"))?;

        let encoded = recv
            .read_to_end(MAX_RPC_BYTES)
            .await
            .map_err(failed("reading the answer"))?;
        postcard::from_bytes(&encoded).map_err(failed("decoding the answer"))
    }
}

/// Labels a failure with the step it happened at.
///
/// The steps are worth telling apart when a proposal does not land: failing to
/// connect means the peer is down, while failing to decode its answer means it
/// is running something else.
fn failed<E: std::fmt::Display>(what: &'static str) -> impl FnOnce(E) -> String {
    move |error| format!("{what}: {error}")
}
