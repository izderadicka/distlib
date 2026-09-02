//! Calling a node's local API.
//!
//! Lives beside the server so the two cannot drift, now that the CLI needs one.
//!
//! hyper rather than reqwest: this only ever speaks HTTP to the address in
//! `[api] bind_addr`, and reqwest's `rustls` feature pulls `aws-lc-rs` — a C
//! toolchain — for a TLS stack this does not use. hyper is already in the tree
//! via axum.

use std::net::SocketAddr;

use http_body_util::{BodyExt as _, Full};
use hyper::{Request, StatusCode, body::Bytes, header::AUTHORIZATION};
use hyper_util::{client::legacy::Client as Hyper, rt::TokioExecutor};
use secrecy::{ExposeSecret as _, SecretString};
use serde_json::{Value, json};

/// A client for one node's local API.
pub struct Client {
    addr: SocketAddr,
    token: SecretString,
}

/// Why a call produced no result.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Nothing answered.
    ///
    /// Its own variant because it is the common case and means something a
    /// caller can act on: the node is not running.
    #[error("could not reach the node's api at {addr}: {message}")]
    Unreachable { addr: SocketAddr, message: String },

    /// The token was wrong, or missing.
    #[error("the api refused the token")]
    Unauthorised,

    /// The node answered, and the answer was an error.
    #[error("{0}")]
    Failed(String),

    /// The node answered with something that is not a JSON-RPC response.
    #[error("the api answered with something unexpected: {0}")]
    Malformed(String),
}

impl Client {
    /// A client for the API at `addr`, authenticating with `token`.
    pub fn new(addr: SocketAddr, token: SecretString) -> Self {
        Self { addr, token }
    }

    /// Calls `method` and returns its result.
    pub async fn call(&self, method: &str, params: Value) -> Result<Value, ClientError> {
        let unreachable = |error: &dyn std::fmt::Display| ClientError::Unreachable {
            addr: self.addr,
            message: error.to_string(),
        };

        let body = json!({
            "jsonrpc": "2.0",
            // Nothing here issues concurrent calls, so there is no id to match
            // against; the server echoes it and this ignores it.
            "id": 1,
            "method": method,
            "params": params,
        });
        let request = Request::post(format!("http://{}/rpc", self.addr))
            .header(
                AUTHORIZATION,
                format!("Bearer {}", self.token.expose_secret()),
            )
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body.to_string())))
            .map_err(|error| unreachable(&error))?;

        let response = Hyper::builder(TokioExecutor::new())
            .build_http()
            .request(request)
            .await
            .map_err(|error| unreachable(&error))?;

        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|error| unreachable(&error))?
            .to_bytes();

        if status == StatusCode::UNAUTHORIZED {
            return Err(ClientError::Unauthorised);
        }

        let answer: Value = serde_json::from_slice(&body)
            .map_err(|error| ClientError::Malformed(error.to_string()))?;

        if let Some(error) = answer.get("error") {
            let message = error["message"].as_str().unwrap_or("unknown error");
            return Err(ClientError::Failed(message.to_owned()));
        }

        answer
            .get("result")
            .cloned()
            .ok_or_else(|| ClientError::Malformed(format!("no result and no error in {answer}")))
    }
}
