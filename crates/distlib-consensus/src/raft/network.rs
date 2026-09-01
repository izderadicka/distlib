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

use std::collections::BTreeSet;

use distlib_core::{MemberId, NodeAddr, RawMemberId};
use distlib_net::{Connections, NOT_A_VOTER_REASON, alpn, close_code};
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

use crate::raft::{state_machine::StateMachineStore, types::TypeConfig};

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
}

/// Serves `distlib/raft/0` by handing requests to the local Raft.
#[derive(Clone)]
pub struct RaftProtocol {
    raft: Raft<TypeConfig>,
    /// The log-derived membership, for deciding when founding is over.
    ///
    /// Raft's own config cannot answer that: it is empty both for a node that
    /// has not been initialised *yet* and for one that never will be.
    state_machine: StateMachineStore,
    /// The members this node was configured to found a group with.
    ///
    /// The only voters it will accept before Raft knows its own.
    founding_core: BTreeSet<MemberId>,
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
    /// `founding_core` is the configured founding core group — `[consensus]
    /// core` — which is the only thing that can say who the voters are before
    /// Raft has been initialised. Empty for a node that is not founding
    /// anything, which is exactly right: it should serve consensus to nobody.
    pub fn new(
        raft: Raft<TypeConfig>,
        state_machine: StateMachineStore,
        founding_core: BTreeSet<MemberId>,
    ) -> Self {
        Self {
            raft,
            state_machine,
            founding_core,
        }
    }

    async fn answer(&self, request: Request) -> Response {
        match request {
            Request::AppendEntries(rpc) => {
                Response::AppendEntries(self.raft.append_entries(rpc).await)
            }
            Request::Vote(rpc) => Response::Vote(self.raft.vote(rpc).await),
            Request::InstallSnapshot(rpc) => {
                Response::InstallSnapshot(self.raft.install_snapshot(rpc).await)
            }
        }
    }

    /// Whether `peer` may take part in this node's consensus.
    ///
    /// Once Raft knows its voters they are the whole answer. Before that — the
    /// founding window — it does not, and something has to stand in, because a
    /// founder sends `Vote` to peers whose Raft membership is still empty and
    /// founding could never happen if they refused it.
    ///
    /// The stand-in is the *configured* founding core group, and it is narrow
    /// on purpose. "Voter set is empty" was the first answer and it was wrong:
    /// a node that is never initialised has an empty voter set for its whole
    /// life, so it would serve consensus to every member in its allowlist
    /// forever. That is not hypothetical — a member could send it a `Vote`, an
    /// `AppendEntries` carrying a `GroupFounded` naming only themselves, and
    /// the victim would apply it, rebuild its allowlist from that log and evict
    /// the real group. Phase 1b makes it worse: every follower runs an
    /// uninitialised Raft with the full membership in its allowlist.
    ///
    /// So the window is bounded twice over — by who (`founding_core`, not
    /// "anyone we would talk to") and by when (only until the log says a group
    /// exists, after which an empty voter set means this node is not a voter at
    /// all and must not be talked into behaving like one).
    ///
    /// The second bound is deliberately ahead of its use, and unreachable
    /// today: nothing in phase 1a can give a node a founded log *and* an empty
    /// Raft config, so mutating that half of the condition away breaks no test.
    /// It bites in 1b, where a member listed in a stale `[consensus] core` that
    /// the group was founded without will hold the log through follower mode
    /// while never being a voter — and would otherwise go on serving consensus
    /// to whoever that stale config names.
    fn may_speak_raft(&self, peer: MemberId) -> bool {
        {
            let metrics = self.raft.metrics();
            let membership = &metrics.borrow().membership_config;
            let mut voters = membership.voter_ids().peekable();
            if voters.peek().is_some() {
                return voters
                    .any(|voter| MemberId::try_from(voter).is_ok_and(|voter| voter == peer));
            }
        }

        self.state_machine.membership().group_id().is_none() && self.founding_core.contains(&peer)
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
            // Re-checked every time rather than once at accept. The voter set
            // changes under a long-lived connection — founding ends, and later
            // a `CoreGroupChanged` demotes somebody — and a check made only at
            // accept would keep serving whoever got in before it moved. §4.4
            // makes the same argument for the allowlist: refusing the *next*
            // connection is not enough when the current one is still open. One
            // watch borrow per RPC is not worth optimising away.
            if !self.may_speak_raft(peer) {
                tracing::info!(%peer, "refused raft rpc from a non-voter");
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

            let response = self.answer(request).await;
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
