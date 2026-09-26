//! The node's local control API: JSON-RPC 2.0 over HTTP.
//!
//! §7.1's API, arriving early. Phase 1b needs *some* way to commit a
//! `MemberAdded` from outside a test — the running node holds the redb lock and
//! the Raft, so nothing else in the process tree can — and building a bespoke
//! control channel for that would be a second thing to throw away when the
//! specified one arrives. This is the specified one: `POST /rpc` for calls,
//! `GET /events` for §7.2's stream.
//!
//! **Every answer is JSON, on every path** (phase 3's D9). A refused token, an
//! unknown route and a body that is not text all answer with a JSON-RPC error
//! object, so a caller has one parser. The HTTP status is kept as it would
//! have been — 401 is still 401 — because browsers and proxies read it, and
//! the JSON-RPC code beside it is for the caller.
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

pub mod client;
pub mod events;
pub mod methods;
pub mod rpc;

use std::{net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    extract::{Request as HttpRequest, State, rejection::StringRejection},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION, header::WWW_AUTHENTICATE},
    middleware::{self, Next},
    response::{
        IntoResponse, Response as HttpResponse,
        sse::{KeepAlive, Sse},
    },
    routing::{get, post},
};
use distlib_core::Event;
use secrecy::{ExposeSecret as _, SecretString};
use serde_json::Value;
use tokio::{net::TcpListener, sync::broadcast};

pub use client::{Client, ClientError};
pub use methods::Api;
use rpc::{Error, Request, Response};

/// A running API server.
pub struct Server {
    addr: SocketAddr,
    task: tokio::task::JoinHandle<()>,
    /// Turns membership changes into events. Owned here so that it stops with
    /// the server rather than outliving it.
    membership: tokio::task::JoinHandle<()>,
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
        self.membership.abort();
    }
}

/// State shared by every request.
struct Shared {
    api: Api,
    token: SecretString,
    events: broadcast::Sender<Event>,
}

/// Binds `addr` and serves the API until the returned [`Server`] is shut down.
///
/// `events` is the node's bus — see [`events::bus`]. The server subscribes a
/// receiver per watcher, and publishes the membership's changes into it
/// itself, since the membership node is already in `api`.
///
/// Returns once the listener is bound, so a caller that immediately connects
/// will not race the server into existence.
pub async fn serve(
    addr: SocketAddr,
    api: Api,
    token: SecretString,
    events: broadcast::Sender<Event>,
) -> std::io::Result<Server> {
    let listener = TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;

    let membership = tokio::spawn(events::publish_membership(
        api.node.subscribe(),
        events.clone(),
    ));
    let shared = Arc::new(Shared { api, token, events });
    // The token guards these two routes rather than the whole router, so that
    // the UI's static assets can be added beside them unguarded (D3) and an
    // unknown path is answered as unknown rather than as unauthorised.
    let router = Router::new()
        .route("/rpc", post(handle))
        .route("/events", get(watch))
        .route_layer(middleware::from_fn_with_state(
            Arc::clone(&shared),
            require_token,
        ))
        // 3b-1 replaces this with the page's own fallback, which is how a
        // single-page app answers a path it routes itself.
        .fallback(|| async {
            refused(
                StatusCode::NOT_FOUND,
                Error::invalid_request("no such route"),
            )
        })
        .method_not_allowed_fallback(|| async {
            refused(
                StatusCode::METHOD_NOT_ALLOWED,
                Error::invalid_request("this route does not answer that method"),
            )
        })
        .with_state(shared);

    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router).await {
            tracing::error!(%error, "the api server stopped");
        }
    });

    Ok(Server {
        addr,
        task,
        membership,
    })
}

/// Lets a request through only if it carries this node's token.
async fn require_token(
    State(shared): State<Arc<Shared>>,
    request: HttpRequest,
    next: Next,
) -> HttpResponse {
    if authorised(&shared.token, request.headers()) {
        return next.run(request).await;
    }
    let mut response = refused(StatusCode::UNAUTHORIZED, Error::unauthorised());
    // What RFC 6750 asks a 401 to say, and what tells a generic HTTP client
    // which scheme to try.
    response.headers_mut().insert(
        WWW_AUTHENTICATE,
        axum::http::HeaderValue::from_static("Bearer"),
    );
    response
}

/// A JSON-RPC error with an HTTP status other than 200.
///
/// For the refusals that happen before there is a call to answer — which is
/// why the id is null: there is no request id to echo.
fn refused(status: StatusCode, error: Error) -> HttpResponse {
    (status, Json(Response::failed(Value::Null, error))).into_response()
}

/// `GET /events`: one watcher's stream.
///
/// Subscribed before the response starts, so a caller that has its headers
/// back is already receiving — nothing published after that point is missed.
/// The keep-alive is a comment line every fifteen seconds, which keeps an
/// idle connection from being closed by whatever sits in between.
async fn watch(State(shared): State<Arc<Shared>>) -> impl IntoResponse {
    Sse::new(events::frames(shared.events.subscribe())).keep_alive(KeepAlive::default())
}

/// One JSON-RPC call.
async fn handle(
    State(shared): State<Arc<Shared>>,
    body: Result<String, StringRejection>,
) -> HttpResponse {
    // Too large, or not UTF-8: refused with the status axum chose, but in the
    // same envelope as everything else.
    let body = match body {
        Ok(body) => body,
        Err(rejection) => {
            return refused(
                rejection.status(),
                Error::invalid_request(rejection.body_text()),
            );
        }
    };

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
