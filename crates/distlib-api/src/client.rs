//! Talking to a node's local API.
//!
//! Lives beside the server for the same reason `RaftClient` lives beside
//! `RaftProtocol`: one crate owns the protocol, so the two halves cannot drift.
//! The CLI uses this, and so do the tests.
//!
//! hyper rather than reqwest, deliberately. This only ever speaks HTTP to
//! `127.0.0.1` — the API refuses to be anything but loopback — and reqwest's
//! `rustls` feature would pull `aws-lc-rs`, a C toolchain, to satisfy a TLS
//! stack nothing here uses.

use std::net::SocketAddr;

use http_body_util::{BodyExt as _, Full};
use hyper::{Request, StatusCode, body::Bytes, header::AUTHORIZATION};
use hyper_util::{client::legacy::Client as Hyper, rt::TokioExecutor};
use secrecy::{ExposeSecret as _, SecretString};
use serde_json::{Value, json};

use crate::rpc;

/// A client for one node's local API.
pub struct Client {
    addr: SocketAddr,
    token: SecretString,
    http: Hyper<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>,
}

/// Why a call did not produce a result.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The node could not be reached.
    ///
    /// Its own variant because it is the common case and means something
    /// specific: the node is not running, or is not serving its API.
    #[error("could not reach the node's api at {addr}: {message}")]
    Unreachable { addr: SocketAddr, message: String },

    /// The token was missing or wrong.
    #[error("the api rejected the token; check {0}")]
    Unauthorised(String),

    /// The node answered, and the answer was a JSON-RPC error.
    #[error("{0}")]
    Failed(rpc::Error),

    /// The node answered with something that is not a JSON-RPC response.
    #[error("the api answered with something unexpected: {0}")]
    Malformed(String),
}

impl Client {
    /// A client for the API at `addr`, authenticating with `token`.
    pub fn new(addr: SocketAddr, token: SecretString) -> Self {
        Self {
            addr,
            token,
            http: Hyper::builder(TokioExecutor::new()).build_http(),
        }
    }

    /// Calls `method` and returns its result.
    pub async fn call(&self, method: &str, params: Value) -> Result<Value, ClientError> {
        let body = json!({
            "jsonrpc": "2.0",
            // Nothing here issues concurrent calls on one client, so there is
            // no id to match against; the server echoes it and we ignore it.
            "id": 1,
            "method": method,
            "params": params,
        });
        let unreachable = |message: String| ClientError::Unreachable {
            addr: self.addr,
            message,
        };

        let request = Request::post(format!("http://{}/rpc", self.addr))
            .header(
                AUTHORIZATION,
                format!("Bearer {}", self.token.expose_secret()),
            )
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body.to_string())))
            .map_err(|error| unreachable(error.to_string()))?;

        let response = self
            .http
            .request(request)
            .await
            .map_err(|error| unreachable(error.to_string()))?;

        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|error| unreachable(error.to_string()))?
            .to_bytes();

        if status == StatusCode::UNAUTHORIZED {
            return Err(ClientError::Unauthorised(
                String::from_utf8_lossy(&body).into_owned(),
            ));
        }

        let answer: Value = serde_json::from_slice(&body)
            .map_err(|error| ClientError::Malformed(error.to_string()))?;

        if let Some(error) = answer.get("error") {
            return Err(serde_json::from_value(error.clone())
                .map(ClientError::Failed)
                .unwrap_or_else(|_| ClientError::Malformed(error.to_string())));
        }

        answer
            .get("result")
            .cloned()
            .ok_or_else(|| ClientError::Malformed(format!("no result and no error in {answer}")))
    }
}
