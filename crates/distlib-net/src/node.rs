//! A running node: an endpoint plus the router that serves its protocols.

use distlib_core::MemberId;
use iroh::{Endpoint, EndpointAddr, protocol::Router};

use crate::{alpn, ping::PingProtocol};

/// An endpoint with every distlib protocol handler attached.
///
/// Handlers are registered on the router, not the endpoint. The endpoint
/// advertises the ALPNs; the router dispatches accepted connections to the
/// handler for the negotiated one.
#[derive(Debug, Clone)]
pub struct Node {
    router: Router,
}

impl Node {
    /// Attaches the protocol handlers to `endpoint` and starts accepting.
    ///
    /// The endpoint must have been built with [`crate::alpn::registered`], or it
    /// will advertise protocols the router cannot serve.
    pub fn spawn(endpoint: Endpoint) -> Self {
        let router = Router::builder(endpoint)
            .accept(alpn::PING, PingProtocol)
            .spawn();
        Self { router }
    }

    /// The underlying endpoint, for dialling and for status.
    pub fn endpoint(&self) -> &Endpoint {
        self.router.endpoint()
    }

    /// This node's member identity.
    pub fn id(&self) -> MemberId {
        MemberId::from(self.endpoint().id())
    }

    /// This node's dialable address: its id plus whatever paths it currently
    /// knows it is reachable on.
    pub fn addr(&self) -> EndpointAddr {
        self.endpoint().addr()
    }

    /// Stops accepting, lets handlers finish, then closes the endpoint.
    pub async fn shutdown(&self) {
        if let Err(error) = self.router.shutdown().await {
            tracing::warn!(%error, "router shutdown did not complete cleanly");
        }
    }
}
