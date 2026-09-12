//! Assembling the router from the handlers a node serves.
//!
//! Kept here rather than in whichever crate owns the handlers, because from
//! phase 2 on no one crate owns them all: consensus authors three, iroh-gossip
//! one, and iroh-docs and iroh-blobs more still. Each owner says what it
//! serves; this puts the answers together in the one place that knows the
//! endpoint.

use iroh::{
    Endpoint,
    protocol::{DynProtocolHandler, Router},
};

/// What a subsystem serves: each of its ALPNs, and the handler for it.
///
/// Boxed because the handlers have no common type — [`iroh`] boxes them into
/// the router anyway, so this is the shape they end up in regardless.
pub type Protocols = Vec<(Vec<u8>, Box<dyn DynProtocolHandler>)>;

/// Starts accepting `protocols` on `endpoint`.
///
/// The endpoint must already advertise every ALPN given here. It does not have
/// to be told again: [`Router::spawn`] calls `set_alpns` with exactly what it
/// accepts. What the endpoint was built with matters for the window before
/// this is called, which is why the two lists are declared separately and
/// tested against each other.
///
/// An ALPN given twice keeps the last handler, silently — iroh's protocol map
/// is a `BTreeMap`. So each protocol must have exactly one owner among the
/// callers whose lists are joined here.
pub fn serve(endpoint: Endpoint, protocols: Protocols) -> Router {
    protocols
        .into_iter()
        .fold(Router::builder(endpoint), |builder, (alpn, handler)| {
            builder.accept(alpn, handler)
        })
        .spawn()
}
