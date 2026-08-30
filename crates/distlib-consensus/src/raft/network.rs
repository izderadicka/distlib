//! Raft RPC over iroh connections.
//!
//! One request per bidirectional stream, postcard on the wire — the same shape
//! as `distlib/ping/0`, on the `distlib/raft/0` ALPN reserved for it in Phase 0.
//!
//! There is no authentication here and none is needed: `AllowlistHooks` refuses
//! a connection from a non-member after the TLS handshake, so by the time a
//! stream reaches this handler the peer is a member of the group whose key the
//! connection is authenticated by. That is the whole reason the allowlist lives
//! on the endpoint rather than inside any one protocol.

// `RPCError` is openraft's type and appears in signatures openraft dictates, so
// it cannot be boxed without breaking the trait impls; the helpers below carry
// the same type through. Not ours to fix — the same call log_store.rs and
// state_machine.rs make.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use distlib_core::{MemberId, RawMemberId};
use distlib_net::alpn;
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
use tokio::sync::Mutex;

use crate::raft::types::{NodeAddr, TypeConfig};

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
    /// Serves `raft` to other members.
    pub fn new(raft: Raft<TypeConfig>) -> Self {
        Self { raft }
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
}

impl ProtocolHandler for RaftProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        // One stream per RPC, and a connection carries many: a follower is
        // answering heartbeats continuously, so tearing the connection down
        // after one exchange would mean a handshake per heartbeat.
        loop {
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
}

impl RaftNetworkFactoryImpl {
    /// Dials peers from `endpoint`.
    ///
    /// The endpoint must offer [`alpn::RAFT`] and have the allowlist hooks
    /// installed; both come from `distlib_net::endpoint::configure`.
    pub fn new(endpoint: Endpoint) -> Self {
        Self { endpoint }
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
            connection: Arc::new(Mutex::new(None)),
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
    /// Reused across RPCs, and dropped on any failure so the next call redials.
    connection: Arc<Mutex<Option<Connection>>>,
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
                *self.connection.lock().await = None;
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
        let mut cached = self.connection.lock().await;
        if let Some(connection) = cached.as_ref() {
            return Ok(connection.clone());
        }

        let addr = self.endpoint_addr()?;
        let connection = self
            .endpoint
            .connect(addr, alpn::RAFT)
            .await
            // Unreachable rather than Network: openraft backs off before
            // retrying an unreachable peer, which is the right response to a
            // member who is simply offline, and members here are often offline.
            .unreachable()?;

        *cached = Some(connection.clone());
        Ok(connection)
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
