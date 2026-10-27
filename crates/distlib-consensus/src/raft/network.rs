//! Raft RPC over iroh connections.
//!
//! One request per bidirectional stream, postcard on the wire — the same shape
//! as `distlib/ping/0`, on the `distlib/raft/0` ALPN reserved for it in Phase 0.
//!
//! **Voters only.** `AllowlistHooks` gets a connection this far only if the
//! peer is a member, but membership is not enough here: these are Raft's own
//! RPCs, and a node that processes a `Vote` or an `AppendEntries` from a
//! non-voter can have its consensus disrupted by any member in the allowlist.
//! §4.2 makes the core group the only voters, so this handler checks that the
//! peer is one before answering anything.
//!
//! A member who is not a voter has [`crate::raft::memberlog`] instead: that is
//! where proposals go, and it is open to everyone.

// `RPCError` is openraft's type and appears in signatures openraft dictates, so
// it cannot be boxed without breaking the trait impls; the helpers below carry
// the same type through. Not ours to fix — the same call log_store.rs and
// state_machine.rs make.
#![allow(clippy::result_large_err)]

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex, PoisonError},
};

use distlib_core::{MemberId, NodeAddr, RawMemberId};
use distlib_net::{Connections, IsRejection, NOT_A_VOTER_REASON, NetError, alpn, close_code};
use iroh::{
    Endpoint, EndpointAddr, RelayUrl,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
};
use openraft::{
    Raft, RaftNetworkFactory,
    error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable},
    network::{RPCOption, RaftNetwork},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use serde::{Deserialize, Serialize};

use crate::raft::{seat::Seat, state_machine::StateMachineStore, types::TypeConfig};

/// Largest encoded RPC accepted in either direction.
///
/// A cap is required rather than decorative: `read_to_end` without one lets any
/// member allocate arbitrarily on its peers. 16 MiB is far above anything this
/// protocol produces — the membership snapshot is a small table, and openraft
/// chunks snapshots below this by default — while still being bounded.
pub const MAX_RPC_BYTES: usize = 16 * 1024 * 1024;

type NodeId = RawMemberId;

/// What one node asks another.
#[derive(Debug, Serialize, Deserialize)]
enum Request {
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<NodeId>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
    /// Read-only acknowledgment used before forgetting a retired learner.
    Retired,
    /// An expelled, durably retired node asks the leader to commit its cleanup.
    ForgetMe,
}

/// What it answers.
///
/// Each variant carries the `Result` the local Raft produced, so a rejection
/// decided by the remote — a stale term, say — arrives as a remote error rather
/// than being flattened into a transport failure. Those mean different things
/// to openraft: one is Raft working correctly, the other is a broken link.
#[derive(Debug, Serialize, Deserialize)]
enum Response {
    AppendEntries(Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>),
    Vote(Result<VoteResponse<NodeId>, RaftError<NodeId>>),
    InstallSnapshot(
        Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
    ),
    Retired(bool),
    Forgotten(bool),
}

/// Who this node will speak consensus with while it holds no voter set of its
/// own.
///
/// The stand-in [`RaftProtocol::may_speak_raft`] falls back to, and shared
/// rather than fixed because there are now two windows in which a node has no
/// voter set, with different right answers:
///
/// * **Founding.** Raft has not been initialised, and the only thing that can
///   say who the voters are is `[consensus] core` — configuration, because
///   there is no log yet.
/// * **Promotion.** A follower taking its seat empties its state machine so
///   the log folds exactly once
///   ([`crate::StateMachineStore::reset_for_promotion`]), which puts it back
///   into "no group, no voters" for as long as it takes the leader to catch it
///   up. Configuration is the *wrong* answer there: a node that joined by
///   ticket has whatever core group that ticket named, which may be years out
///   of date. The right answer is the signed desired core plus the core just
///   before this node was promoted, excluding members since expelled. Routing
///   referrals never contribute. Persisted promotion authority keeps this
///   trust available through historical replay until Raft reaches its boundary.
///
/// Getting this wrong is not cosmetic. The window is where a node can be
/// talked into applying a `GroupFounded` naming somebody else and rebuilding
/// its allowlist from it — which is the attack the narrow founding window was
/// made narrow for.
#[derive(Debug, Clone, Default)]
pub(crate) struct TrustedCore(Arc<Mutex<BTreeSet<MemberId>>>);

impl TrustedCore {
    pub(crate) fn new(members: BTreeSet<MemberId>) -> Self {
        Self(Arc::new(Mutex::new(members)))
    }

    /// Whether `peer` is one of them.
    pub(crate) fn contains(&self, peer: &MemberId) -> bool {
        self.lock().contains(peer)
    }

    /// Replaces them — for a node about to blank its state machine, saying who
    /// the log said the voters were while it still knew.
    pub(crate) fn replace(&self, members: BTreeSet<MemberId>) {
        *self.lock() = members;
    }

    /// A poison-tolerant lock; the guard covers a set lookup and nothing else.
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeSet<MemberId>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Serves `distlib/raft/0` by handing requests to the local Raft.
#[derive(Clone)]
pub struct RaftProtocol {
    /// The Raft to hand requests to — if this node has one.
    ///
    /// Empty on a node that does not vote, which every node's router now serves
    /// this protocol on regardless. See [`Seat`]: a router's protocols are
    /// fixed when it spawns and cannot be added to, so a node that may be
    /// promoted has to be serving consensus before it can answer any of it.
    seat: Seat,
    /// The log-derived membership, for deciding when founding is over.
    ///
    /// Raft's own config cannot answer that: it is empty both for a node that
    /// has not been initialised *yet* and for one that never will be.
    state_machine: StateMachineStore,
    /// The only voters this node will accept before Raft knows its own.
    trusted: TrustedCore,
}

// `ProtocolHandler` requires `Debug`, and `Raft` does not implement it. There is
// nothing useful to print here anyway: the interesting state is inside Raft and
// is reached through its own metrics.
impl std::fmt::Debug for RaftProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftProtocol").finish_non_exhaustive()
    }
}

impl RaftProtocol {
    /// Serves `raft` to voters.
    ///
    /// `trusted` is who to accept from while Raft knows no voters of its own —
    /// see [`TrustedCore`]. Empty for a node that is neither founding nor being
    /// promoted, which is exactly right: it should serve consensus to nobody.
    pub(crate) fn new(seat: Seat, state_machine: StateMachineStore, trusted: TrustedCore) -> Self {
        Self {
            seat,
            state_machine,
            trusted,
        }
    }

    /// Serves a Raft that already exists, for a caller assembling a node by
    /// hand.
    ///
    /// The [`Seat`] is an affair between a node and its own promotion, so it
    /// is not part of this crate's surface; a caller outside it has a `Raft`
    /// and wants it served. `founding_core` is who to accept from until Raft
    /// knows its own voters — see [`Self::may_speak_raft`].
    pub fn serving(
        raft: Raft<TypeConfig>,
        state_machine: StateMachineStore,
        founding_core: BTreeSet<MemberId>,
    ) -> Self {
        Self::new(
            Seat::holding(raft),
            state_machine,
            TrustedCore::new(founding_core),
        )
    }

    async fn answer(&self, raft: &Raft<TypeConfig>, request: Request) -> Response {
        match request {
            Request::Retired => Response::Retired(self.seat.raft().is_none()),
            Request::ForgetMe => Response::Forgotten(false),
            Request::AppendEntries(rpc) => Response::AppendEntries(raft.append_entries(rpc).await),
            Request::Vote(rpc) => Response::Vote(raft.vote(rpc).await),
            Request::InstallSnapshot(rpc) => {
                Response::InstallSnapshot(raft.install_snapshot(rpc).await)
            }
        }
    }

    /// Whether `peer` may take part in this node's consensus.
    ///
    /// During promotion, persisted replay authority also admits the voters
    /// captured from the followed log, until an applied Raft configuration
    /// reaches that promotion's index. Old log replay cannot close it early.
    /// Outside that window, Raft's applied and effective voters decide. During
    /// founding, the configured core stands in for the missing voter set: a
    /// founder sends `Vote` to peers whose Raft membership is still empty and
    /// founding could never happen if they refused it.
    ///
    /// The stand-in is [`TrustedCore`] — the configured founding core group
    /// while founding, and the core group the log last gave while a promotion
    /// is being caught up — and it is narrow on purpose. "Voter set is empty" was the first answer and it was wrong:
    /// a node that is never initialised has an empty voter set for its whole
    /// life, so it would serve consensus to every member in its allowlist
    /// forever. That is not hypothetical — a member could send it a `Vote`, an
    /// `AppendEntries` carrying a `GroupFounded` naming only themselves, and
    /// the victim would apply it, rebuild its allowlist from that log and evict
    /// the real group. Phase 1b makes it worse: every follower runs an
    /// uninitialised Raft with the full membership in its allowlist.
    ///
    /// So the window is bounded twice over — by who ([`TrustedCore`], not
    /// "anyone we would talk to") and by when (only until the log says a group
    /// exists, after which an empty voter set means this node is not a voter at
    /// all and must not be talked into behaving like one).
    ///
    /// The second bound bites in 1b, where a member listed in a stale
    /// `[consensus] core` that the group was founded without holds the log
    /// through follower mode while never being a voter — and would otherwise go
    /// on serving consensus to whoever that stale config names.
    fn may_speak_raft(&self, raft: &Raft<TypeConfig>, peer: MemberId) -> bool {
        if self
            .state_machine
            .promotion()
            .is_some_and(|promotion| promotion.voters.contains(&peer))
        {
            return true;
        }
        if self.applied_voter(peer) {
            return true;
        }
        {
            let metrics = raft.metrics();
            let membership = &metrics.borrow().membership_config;
            let mut voters = membership.voter_ids().peekable();
            if voters.peek().is_some() {
                return voters
                    .any(|voter| MemberId::try_from(voter).is_ok_and(|voter| voter == peer));
            }
        }

        self.state_machine.membership().group_id().is_none() && self.trusted.contains(&peer)
    }
    /// Learners may request their own cleanup. A retired node also answers a
    /// current core learned through the memberlog, even after voter turnover.
    /// This does not authorize consensus mutations from those extra peers.
    fn may_query_retirement(&self, peer: MemberId) -> bool {
        self.seat
            .raft()
            .as_ref()
            .is_some_and(|raft| self.may_speak_raft(raft, peer))
            || self.state_machine.membership().is_core(&peer)
            || self
                .state_machine
                .raft_membership()
                .borrow()
                .nodes()
                .any(|(id, _)| *id == RawMemberId::from(peer))
    }

    /// The caller may remove only itself, only after expulsion and committed
    /// uniform voter removal. The reply follows the cleanup commit, so an
    /// orderly exiting process cannot leave an unreachable retained learner.
    async fn forget_retired(&self, peer: MemberId) -> bool {
        let Some(raft) = self.seat.raft() else {
            return false;
        };
        let applied = self.state_machine.raft_membership().borrow().clone();
        let id = RawMemberId::from(peer);
        if self.state_machine.membership().is_member(&peer)
            || applied.membership().get_joint_config().len() != 1
            || applied.voter_ids().any(|voter| voter == id)
        {
            return false;
        }
        matches!(
            tokio::time::timeout(
                crate::raft::core_group::RETRY,
                raft.change_membership(
                    openraft::ChangeMembers::RemoveNodes(BTreeSet::from([id])),
                    true
                )
            )
            .await,
            Ok(Ok(_))
        )
    }

    fn applied_voter(&self, peer: MemberId) -> bool {
        self.state_machine
            .raft_membership()
            .borrow()
            .voter_ids()
            .any(|id| id == RawMemberId::from(peer))
    }
}

impl ProtocolHandler for RaftProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        // Voters only. The allowlist got them this far, which proves they are
        // a member; being a member is not licence to take part in consensus.
        let peer = MemberId::from(connection.remote_id());

        // One stream per RPC, and a connection carries many: a follower is
        // answering heartbeats continuously, so tearing the connection down
        // after one exchange would mean a handshake per heartbeat.
        loop {
            if !self.may_query_retirement(peer) {
                connection.close(close_code::NOT_A_VOTER, NOT_A_VOTER_REASON);
                return Ok(());
            }
            let (mut send, mut recv) = match connection.accept_bi().await {
                Ok(streams) => streams,
                // The peer closed the connection: the normal way this ends.
                Err(_) => return Ok(()),
            };

            let encoded = recv.read_to_end(MAX_RPC_BYTES).await.accepting()?;
            let request: Request = postcard::from_bytes(&encoded).accepting()?;

            // Authorize after receiving each request: the seat and membership
            // may change while accept_bi() waits. A retired node still answers
            // the read-only acknowledgment, but never a consensus mutation.
            let raft = self.seat.raft();
            let authorized = raft.as_ref().map_or_else(
                || self.applied_voter(peer),
                |raft| self.may_speak_raft(raft, peer),
            );
            let may_query = self.may_query_retirement(peer);
            let response = match request {
                Request::Retired if may_query => Response::Retired(self.seat.raft().is_none()),
                Request::ForgetMe if may_query => {
                    Response::Forgotten(self.forget_retired(peer).await)
                }
                request => {
                    let Some(raft) = raft.filter(|_| authorized) else {
                        connection.close(close_code::NOT_A_VOTER, NOT_A_VOTER_REASON);
                        return Ok(());
                    };
                    self.answer(&raft, request).await
                }
            };
            let encoded = postcard::to_stdvec(&response).accepting()?;

            send.write_all(&encoded).await.accepting()?;
            send.finish().accepting()?;
        }
    }
}

/// Creates a client per peer, as openraft asks for.
#[derive(Debug, Clone)]
pub struct RaftNetworkFactoryImpl {
    endpoint: Endpoint,
    /// The node's connections, not this factory's.
    ///
    /// Passed in rather than created here so every protocol on the node shares
    /// one set: openraft keeps a client per peer for replication, a forwarded
    /// proposal needs one too, and phase 1b's log replication will as well.
    connections: Connections,
}

impl RaftNetworkFactoryImpl {
    /// Dials peers from `endpoint`.
    ///
    /// The endpoint must offer [`alpn::RAFT`] and have the allowlist hooks
    /// installed; both come from `distlib_net::endpoint::configure`.
    pub fn new(endpoint: Endpoint, connections: Connections) -> Self {
        Self {
            endpoint,
            connections,
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for RaftNetworkFactoryImpl {
    type Network = RaftClient;

    async fn new_client(&mut self, target: NodeId, node: &NodeAddr) -> RaftClient {
        // Deliberately does not connect: openraft documents this as building a
        // client, and a node that is currently unreachable must still get one.
        RaftClient {
            endpoint: self.endpoint.clone(),
            target,
            addr: node.clone(),
            connections: self.connections.clone(),
        }
    }
}

/// Labels a failure as the kind of RPC failure it is.
///
/// A `From` impl would be the idiomatic way to get bare `?` here, but the
/// orphan rule forbids it: `RPCError` is openraft's and the I/O errors are
/// quinn's, so neither is ours to implement across. This is the `.context()`
/// shape instead — and it keeps the choice visible at each call site, which
/// matters because the two are not interchangeable: openraft backs off before
/// retrying an unreachable peer and retries a network failure immediately.
trait FailedRpc<T> {
    /// The peer could not be dialled at all.
    fn unreachable<E: std::error::Error>(self) -> Result<T, RPCError<NodeId, NodeAddr, E>>;

    /// The exchange failed once there was a connection.
    fn network<E: std::error::Error>(self) -> Result<T, RPCError<NodeId, NodeAddr, E>>;

    /// A failure while serving a request, for the accept side.
    fn accepting(self) -> Result<T, AcceptError>;
}

impl<T, F> FailedRpc<T> for Result<T, F>
where
    // `Send + Sync` is `AcceptError::from_err`'s requirement, not ours; every
    // error this file handles — quinn's, postcard's, io — satisfies it.
    F: std::error::Error + Send + Sync + 'static,
{
    fn unreachable<E: std::error::Error>(self) -> Result<T, RPCError<NodeId, NodeAddr, E>> {
        self.map_err(|error| RPCError::Unreachable(Unreachable::new(&error)))
    }

    fn network<E: std::error::Error>(self) -> Result<T, RPCError<NodeId, NodeAddr, E>> {
        self.map_err(|error| RPCError::Network(NetworkError::new(&error)))
    }

    fn accepting(self) -> Result<T, AcceptError> {
        self.map_err(AcceptError::from_err)
    }
}

/// Sends Raft RPCs to one peer.
#[derive(Debug, Clone)]
pub struct RaftClient {
    endpoint: Endpoint,
    target: NodeId,
    addr: NodeAddr,
    /// The node's connections. Shared with every other protocol, so a peer is
    /// dialled once per protocol however many callers want it.
    connections: Connections,
}

impl RaftClient {
    /// True only after the remote has vacated its Raft seat. Silence or an
    /// older peer that does not recognize this query is never an acknowledgment.
    pub(crate) async fn retired(
        &self,
    ) -> Result<bool, RPCError<NodeId, NodeAddr, RaftError<NodeId>>> {
        match self.call(Request::Retired).await? {
            Response::Retired(retired) => Ok(retired),
            other => Err(mismatched(self.target, &other)),
        }
    }

    /// A committed cleanup response, or revocation of the temporary Raft
    /// transport exception, lets an expelled node exit. Silence never does.
    pub(crate) async fn forget_me(&self) -> bool {
        let Ok(peer) = MemberId::try_from(self.target) else {
            return false;
        };
        let Ok(addr) = self.endpoint_addr::<RaftError<NodeId>>() else {
            return false;
        };
        let connection = match self
            .connections
            .get_or_connect(&self.endpoint, peer, addr, alpn::RAFT)
            .await
        {
            Ok(connection) => connection,
            Err(NetError::Rejected {
                by_remote: true, ..
            }) => return true,
            Err(_) => return false,
        };
        matches!(
            self.call::<RaftError<NodeId>>(Request::ForgetMe).await,
            Ok(Response::Forgotten(true))
        ) || connection
            .close_reason()
            .is_some_and(|error| error.is_rejection())
    }

    /// Where to dial, from the addressing Raft carries in its membership.
    ///
    /// An empty [`NodeAddr`] is meaningful rather than broken: it means "find
    /// them by member id", which works whenever address lookup is configured.
    fn endpoint_addr<E: std::error::Error>(
        &self,
    ) -> Result<EndpointAddr, RPCError<NodeId, NodeAddr, E>> {
        let member = MemberId::try_from(self.target).unreachable()?;
        let mut addr = EndpointAddr::new(member.endpoint_id());

        for socket in &self.addr.direct {
            addr = addr.with_ip_addr(*socket);
        }
        if let Some(url) = &self.addr.relay {
            let relay: RelayUrl = url
                .parse()
                .map_err(|_| {
                    std::io::Error::other(format!(
                        "member {member} lists an unparseable relay url: {url}"
                    ))
                })
                .unreachable()?;
            addr = addr.with_relay_url(relay);
        }
        Ok(addr)
    }

    /// Sends one request and waits for its answer.
    ///
    /// Failures drop the cached connection. A half-open connection that is
    /// never replaced would fail every future RPC to this peer, which for a
    /// follower means never hearing another heartbeat.
    async fn call<E: std::error::Error>(
        &self,
        request: Request,
    ) -> Result<Response, RPCError<NodeId, NodeAddr, E>> {
        match self.exchange(request).await {
            Ok(response) => Ok(response),
            Err(error) => {
                // Only this peer's raft connection: a failure here says nothing
                // about other peers, or about other protocols to this one.
                if let Ok(peer) = MemberId::try_from(self.target) {
                    self.connections.forget(peer, alpn::RAFT);
                }
                Err(error)
            }
        }
    }

    async fn exchange<E: std::error::Error>(
        &self,
        request: Request,
    ) -> Result<Response, RPCError<NodeId, NodeAddr, E>> {
        let encoded = postcard::to_stdvec(&request).network()?;

        let connection = self.connection().await?;
        let (mut send, mut recv) = connection.open_bi().await.network()?;

        send.write_all(&encoded).await.network()?;
        send.finish().network()?;

        let encoded = recv.read_to_end(MAX_RPC_BYTES).await.network()?;

        postcard::from_bytes(&encoded).network()
    }

    /// The cached connection, dialling if there is not one.
    async fn connection<E: std::error::Error>(
        &self,
    ) -> Result<Connection, RPCError<NodeId, NodeAddr, E>> {
        let peer = MemberId::try_from(self.target).unreachable()?;
        let addr = self.endpoint_addr()?;

        self.connections
            .get_or_connect(&self.endpoint, peer, addr, alpn::RAFT)
            .await
            // Unreachable rather than Network: openraft backs off before
            // retrying an unreachable peer, which is the right response to a
            // member who is simply offline, and members here are often offline.
            .unreachable()
    }
}

impl RaftNetwork<TypeConfig> for RaftClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, NodeAddr, RaftError<NodeId>>> {
        match self.call(Request::AppendEntries(rpc)).await? {
            Response::AppendEntries(result) => result.map_err(|error| remote(self.target, error)),
            other => Err(mismatched(self.target, &other)),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, NodeAddr, RaftError<NodeId>>> {
        match self.call(Request::Vote(rpc)).await? {
            Response::Vote(result) => result.map_err(|error| remote(self.target, error)),
            other => Err(mismatched(self.target, &other)),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, NodeAddr, RaftError<NodeId, InstallSnapshotError>>,
    > {
        match self.call(Request::InstallSnapshot(rpc)).await? {
            Response::InstallSnapshot(result) => result.map_err(|error| remote(self.target, error)),
            other => Err(mismatched(self.target, &other)),
        }
    }
}

fn remote<E: std::error::Error>(target: NodeId, error: E) -> RPCError<NodeId, NodeAddr, E> {
    RPCError::RemoteError(openraft::error::RemoteError::new(target, error))
}

/// A peer answered a different RPC than it was asked.
///
/// Only reachable if the two ends disagree about the protocol, which the ALPN
/// version exists to prevent. Treated as a transport fault so openraft retries
/// rather than concluding anything about Raft state.
fn mismatched<E: std::error::Error>(
    target: NodeId,
    response: &Response,
) -> RPCError<NodeId, NodeAddr, E> {
    RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
        "member {target} answered a different request than it was asked: {response:?}"
    ))))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::{MemberRecord, MembershipEvent, SignedEvent, Timestamp};
    use openraft::{
        CommittedLeaderId, Entry, EntryPayload, LogId, Membership, Vote,
        storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine},
    };
    use std::{collections::BTreeMap, time::Duration};

    #[test]
    fn retirement_rpc_encodings_are_stable() {
        assert_eq!(postcard::to_stdvec(&Request::Retired).unwrap(), [3]);
        assert_eq!(postcard::to_stdvec(&Request::ForgetMe).unwrap(), [4]);
        assert!(matches!(
            postcard::from_bytes::<Request>(&[4]).unwrap(),
            Request::ForgetMe
        ));
        for value in [false, true] {
            assert_eq!(
                postcard::to_stdvec(&Response::Retired(value)).unwrap(),
                [3, u8::from(value)]
            );
            assert_eq!(
                postcard::to_stdvec(&Response::Forgotten(value)).unwrap(),
                [4, u8::from(value)]
            );
            assert!(
                matches!(postcard::from_bytes::<Response>(&[4, u8::from(value)]).unwrap(), Response::Forgotten(decoded) if decoded == value)
            );
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn a_current_member_cannot_request_learner_cleanup() {
        let key = iroh::SecretKey::from_bytes(&[31; 32]);
        let me = MemberId::from(key.public());
        let learner = MemberId::from(iroh::SecretKey::from_bytes(&[32; 32]).public());
        let raw = RawMemberId::from(me);
        let record = |member_id| MemberRecord {
            member_id,
            display_name: "member".into(),
            pledge_bytes: 0,
        };
        let found = MembershipEvent::found(
            vec![(record(me), NodeAddr::default())],
            Default::default(),
            Timestamp::from_millis(1),
        )
        .unwrap();
        let sign = |event, nonce| {
            SignedEvent::sign(&key, event, Timestamp::from_millis(nonce), nonce - 1).unwrap()
        };
        let membership = Membership::new(
            vec![BTreeSet::from([raw])],
            BTreeMap::from([
                (raw, NodeAddr::default()),
                (RawMemberId::from(learner), NodeAddr::default()),
            ]),
        );
        let entries: Vec<Entry<TypeConfig>> = [
            EntryPayload::Membership(membership),
            EntryPayload::Normal(sign(found, 1)),
            EntryPayload::Normal(sign(
                MembershipEvent::MemberAdded {
                    member: record(learner),
                },
                2,
            )),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, payload)| Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, raw), index as u64),
            payload,
        })
        .collect();
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(redb::Database::create(dir.path().join("cleanup.redb")).unwrap());
        let mut state = StateMachineStore::from_database(Arc::clone(&db)).unwrap();
        let mut log = crate::LogStore::from_database(db).unwrap();
        log.save_vote(&Vote::new_committed(1, raw)).await.unwrap();
        log.blocking_append(entries.clone()).await.unwrap();
        log.save_committed(Some(entries[2].log_id)).await.unwrap();
        assert!(
            state
                .apply(entries)
                .await
                .unwrap()
                .iter()
                .all(Result::is_ok)
        );
        let endpoint = Endpoint::builder(iroh::endpoint::presets::Minimal)
            .secret_key(key.clone())
            .relay_mode(iroh::endpoint::RelayMode::Disabled)
            .bind_addr(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
            .unwrap()
            .bind()
            .await
            .unwrap();
        let config = Arc::new(
            openraft::Config {
                enable_tick: false,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );
        let raft = Raft::new(
            raw,
            config,
            RaftNetworkFactoryImpl::new(endpoint.clone(), Connections::new()),
            log,
            state.clone(),
        )
        .await
        .unwrap();
        raft.wait(Some(Duration::from_secs(5)))
            .state(openraft::ServerState::Leader, "restored singleton")
            .await
            .unwrap();
        let protocol = RaftProtocol::serving(raft.clone(), state, BTreeSet::from([me]));
        assert!(
            !protocol.forget_retired(learner).await,
            "membership still admits this learner"
        );
        assert!(
            raft.metrics()
                .borrow()
                .membership_config
                .nodes()
                .any(|(id, _)| *id == RawMemberId::from(learner))
        );
        raft.client_write(sign(
            MembershipEvent::MemberExpelled {
                member: learner,
                reason: "expelled".into(),
            },
            3,
        ))
        .await
        .unwrap()
        .data
        .unwrap();
        assert!(
            protocol.forget_retired(learner).await,
            "cleanup becomes valid after expulsion"
        );
        assert!(
            !raft
                .metrics()
                .borrow()
                .membership_config
                .nodes()
                .any(|(id, _)| *id == RawMemberId::from(learner))
        );
        raft.shutdown().await.unwrap();
        endpoint.close().await;
    }
}
