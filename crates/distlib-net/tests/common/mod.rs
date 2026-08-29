//! Shared setup for the transport tests.
//!
//! No test may reach n0's public relay or DNS infrastructure. Every endpoint
//! here is built from `presets::Minimal`, which configures no address lookup,
//! and is given either explicit socket addresses or an in-process relay. The
//! requirement is determinism: a suite depending on third-party infrastructure
//! is flaky by construction.

#![allow(dead_code)] // each test binary uses a different subset of these

use std::net::{Ipv4Addr, SocketAddr};

use distlib_core::MemberId;
use distlib_net::{Allowlist, AllowlistWriter, allowlist, endpoint::configure};
use iroh::{
    Endpoint, EndpointAddr, RelayUrl, SecretKey, TransportAddr,
    endpoint::{RelayMode, presets},
};
use iroh_relay::tls::CaTlsConfig;

/// A keypair plus the member identity it implies, so a test can decide who is
/// on whose allowlist before any endpoint exists.
pub struct Member {
    pub secret: SecretKey,
    pub id: MemberId,
}

impl Member {
    pub fn generate() -> Self {
        let secret = SecretKey::generate();
        let id = MemberId::from(secret.public());
        Self { secret, id }
    }

    /// An allowlist for this member admitting exactly `peers`.
    pub fn admitting(
        &self,
        peers: impl IntoIterator<Item = MemberId>,
    ) -> (AllowlistWriter, Allowlist) {
        allowlist(self.id, peers)
    }
}

/// An endpoint with no relays and no address lookup: reachable only at the
/// socket addresses it is bound to.
pub async fn direct_endpoint(member: &Member, allowlist: Allowlist) -> Endpoint {
    let builder = Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled);
    configure(
        builder,
        member.secret.clone(),
        allowlist,
        distlib_net::alpn::registered(),
    )
    .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
    .expect("loopback is a valid bind address")
    .bind()
    .await
    .expect("endpoint failed to bind")
}

/// An endpoint that can *only* use the given relay: `clear_ip_transports`
/// removes the IP transports entirely, so no direct path can exist.
///
/// Certificate verification is disabled because the in-process relay from
/// `iroh::test_utils` serves self-signed certificates. It is scoped to this
/// helper and never reaches production code, which uses real relays.
pub async fn relay_endpoint(member: &Member, allowlist: Allowlist, relays: RelayMode) -> Endpoint {
    let builder = Endpoint::builder(presets::Minimal)
        .relay_mode(relays)
        .clear_ip_transports()
        .ca_tls_config(CaTlsConfig::insecure_skip_verify());
    configure(
        builder,
        member.secret.clone(),
        allowlist,
        distlib_net::alpn::registered(),
    )
    .bind()
    .await
    .expect("endpoint failed to bind")
}

/// The address of an endpoint reachable only over IP.
pub fn direct_addr(endpoint: &Endpoint) -> EndpointAddr {
    let addr = EndpointAddr::new(endpoint.id())
        .with_addrs(endpoint.bound_sockets().into_iter().map(TransportAddr::Ip));
    assert!(!addr.is_empty(), "endpoint reported no bound sockets");
    addr
}

/// The address of an endpoint reachable only through `relay`.
///
/// `online()` waits until the endpoint has registered with the relay; dialling
/// before the home relay is established is a race, not a failure.
pub async fn relay_addr(endpoint: &Endpoint, relay: &RelayUrl) -> EndpointAddr {
    endpoint.online().await;
    EndpointAddr::new(endpoint.id()).with_relay_url(relay.clone())
}
