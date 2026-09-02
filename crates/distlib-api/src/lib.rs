//! The node's local control API: JSON-RPC 2.0 over HTTP.
//!
//! §7.1's API, arriving early. Phase 1b needs *some* way to commit a
//! `MemberAdded` from outside a test — the running node holds the redb lock and
//! the Raft, so nothing else in the process tree can — and building a bespoke
//! control channel for that would be a second thing to throw away when the
//! specified one arrives. This is the specified one, with the membership
//! methods implemented and `library.*` and the SSE stream still to come.
//!
//! **A bearer token always; loopback by default.** Whoever can call this can
//! make the node propose membership changes as itself. That is a narrower power
//! than the node's key — it cannot sign anything the group's rules refuse, and
//! every proposal is attributed — but it is not nothing, so every request must
//! carry the token from `<data-dir>/api.token`.
//!
//! The default listener is `127.0.0.1`, which is a default rather than a
//! promise: a node on a server or in a container has to be reachable from
//! somewhere else. Nothing here refuses to bind elsewhere, and nothing here
//! offers TLS either, so a non-loopback address wants a reverse proxy in front
//! of it. TLS and whatever authentication belongs beside it are phase 3's, with
//! the UI that needs them.

pub mod methods;
pub mod rpc;

use std::{net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::IntoResponse,
    routing::post,
};
use secrecy::{ExposeSecret as _, SecretString};
use serde_json::Value;
use tokio::net::TcpListener;

pub use methods::Api;
use rpc::{Error, Request, Response};

/// A running API server.
pub struct Server {
    addr: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    /// Where it is listening.
    ///
    /// Worth asking for rather than assuming: a caller may bind port 0, and the
    /// tests do.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stops serving.
    pub fn shutdown(&self) {
        self.task.abort();
    }
}

/// State shared by every request.
struct Shared {
    api: Api,
    token: SecretString,
}

/// Binds `addr` and serves the API until the returned [`Server`] is shut down.
///
/// Returns once the listener is bound, so a caller that immediately connects
/// will not race the server into existence.
pub async fn serve(addr: SocketAddr, api: Api, token: SecretString) -> std::io::Result<Server> {
    let listener = TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;

    let shared = Arc::new(Shared { api, token });
    let router = Router::new().route("/rpc", post(handle)).with_state(shared);

    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router).await {
            tracing::error!(%error, "the api server stopped");
        }
    });

    Ok(Server { addr, task })
}

/// One JSON-RPC call.
async fn handle(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    body: String,
) -> axum::response::Response {
    if !authorised(&shared.token, &headers) {
        // 401 rather than a JSON-RPC error: the request never got as far as
        // being a call, and a caller with the wrong token needs to fix their
        // transport, not read a result object.
        return (StatusCode::UNAUTHORIZED, "invalid or missing bearer token").into_response();
    }

    let request: Request = match serde_json::from_str(&body) {
        Ok(request) => request,
        Err(error) => {
            return Json(Response::failed(
                Value::Null,
                Error::invalid_request(error.to_string()),
            ))
            .into_response();
        }
    };

    // The id is echoed even when the call is refused, so a caller can match an
    // error to what caused it.
    let id = request.id.clone().unwrap_or(Value::Null);

    if request.jsonrpc != "2.0" {
        return Json(Response::failed(
            id,
            Error::invalid_request(format!("unsupported jsonrpc version: {}", request.jsonrpc)),
        ))
        .into_response();
    }

    match shared.api.call(&request.method, request.params).await {
        Ok(result) => Json(Response::ok(id, result)).into_response(),
        Err(error) => Json(Response::failed(id, error)).into_response(),
    }
}

/// Whether the request carries this node's token.
///
/// A plain comparison. It stops at the first differing byte, which in principle
/// leaks how much of a guess was right — but that difference is a nanosecond or
/// two, under an HTTP round trip whose jitter is tens of microseconds, so it is
/// not a signal anyone is pulling out of the noise. If guessing tokens ever
/// becomes a concern, the answer is rate limiting here, not a slower compare.
fn authorised(expected: &SecretString, headers: &HeaderMap) -> bool {
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|offered| offered.trim() == expected.expose_secret())
}
