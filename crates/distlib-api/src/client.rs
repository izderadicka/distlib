//! Calling a node's local API.
//!
//! Lives beside the server so the two cannot drift, now that the CLI needs one.
//!
//! hyper rather than reqwest: this only ever speaks HTTP to the address in
//! `[api] bind_addr`, and reqwest's `rustls` feature pulls `aws-lc-rs` — a C
//! toolchain — for a TLS stack this does not use. hyper is already in the tree
//! via axum.

use std::{net::SocketAddr, time::Duration};

use distlib_core::{Event, TaskId};
use http_body_util::{BodyExt as _, Full};
use hyper::{
    Request, StatusCode,
    body::{Bytes, Incoming},
    header::AUTHORIZATION,
};
use hyper_util::{client::legacy::Client as Hyper, rt::TokioExecutor};
use secrecy::{ExposeSecret as _, SecretString};
use serde_json::{Value, json};

use crate::{
    events::KEEP_ALIVE,
    tasks::{Outcome, TaskState},
};

/// How many times [`Client::until_ended`] tries to reopen a broken event
/// stream before giving up, the waits between doubling from a quarter of a
/// second — about four seconds in all. Enough for a connection that broke to
/// be replaced; a node that is gone for longer has lost the download with it,
/// since tasks do not outlive a restart.
const REOPEN_ATTEMPTS: u32 = 5;

/// A client for one node's local API.
pub struct Client {
    addr: SocketAddr,
    token: SecretString,
    /// How long an event stream may say nothing at all — keep-alives
    /// included — before it is taken to be broken.
    silence: Duration,
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
        Self {
            addr,
            token,
            // Twice the keep-alive: one missed comment is a slow moment, two
            // are a connection nobody is on the other end of.
            silence: KEEP_ALIVE * 2,
        }
    }

    /// Takes an event stream that has said nothing for `silence` to be
    /// broken, in place of twice the node's keep-alive — which is right for
    /// every real caller, and too long for a test to wait out.
    pub fn silent_after(self, silence: Duration) -> Self {
        Self { silence, ..self }
    }

    fn unreachable(&self, error: &dyn std::fmt::Display) -> ClientError {
        ClientError::Unreachable {
            addr: self.addr,
            message: error.to_string(),
        }
    }

    /// Opens `GET /events`, and returns once the node has answered.
    ///
    /// The node subscribes this watcher before it answers, so everything it
    /// publishes after this returns is heard — which is what lets a caller
    /// start something and then wait for the news of it ending, without a
    /// window in which the ending could go by unheard.
    pub async fn watch(&self) -> Result<Events, ClientError> {
        let request = Request::get(format!("http://{}/events", self.addr))
            .header(
                AUTHORIZATION,
                format!("Bearer {}", self.token.expose_secret()),
            )
            .body(Full::new(Bytes::new()))
            .map_err(|error| self.unreachable(&error))?;
        let response = Hyper::builder(TokioExecutor::new())
            .build_http()
            .request(request)
            .await
            .map_err(|error| self.unreachable(&error))?;
        match response.status() {
            StatusCode::OK => Ok(Events {
                addr: self.addr,
                body: response.into_body(),
                unread: Vec::new(),
                silence: self.silence,
            }),
            StatusCode::UNAUTHORIZED => Err(ClientError::Unauthorised),
            other => Err(ClientError::Malformed(format!("/events answered {other}"))),
        }
    }

    /// `library.task`, read as the type the node writes it as.
    pub async fn task(&self, task_id: TaskId) -> Result<TaskState, ClientError> {
        let state = self
            .call("library.task", json!({ "task_id": task_id }))
            .await?;
        serde_json::from_value(state).map_err(|error| ClientError::Malformed(error.to_string()))
    }

    /// Waits for download `task_id` to end, and answers with how it ended.
    ///
    /// `events` must have been opened before the download was started, so
    /// that its ending cannot go by before anybody listens for it.
    ///
    /// **Heard, and asked about whenever hearing may have failed.** The
    /// ending normally arrives as an event. Three things mean it may have
    /// been missed, and all three are answered the same way — by asking
    /// `library.task` — with the stream reopened *first* where it broke, so
    /// that an ending after the answer is heard on the new one:
    ///
    /// - `resync`: this watcher fell behind, and the node skipped it past
    ///   what it missed;
    /// - the stream closed or failed;
    /// - the stream went silent for longer than the keep-alive allows, which
    ///   is how a connection that died without closing shows itself.
    ///
    /// A break is therefore never taken for the download's failure: the
    /// download runs on in the node whatever happens to this connection.
    pub async fn until_ended(
        &self,
        mut events: Events,
        task_id: TaskId,
    ) -> Result<TaskState, ClientError> {
        loop {
            match events.next().await {
                Some(Ok(Watched::Event(
                    Event::DownloadFinished { task_id: id, .. }
                    | Event::DownloadFailed { task_id: id, .. },
                ))) if id == task_id => return self.task(task_id).await,
                Some(Ok(Watched::Event(_))) => continue,
                Some(Ok(Watched::Resync)) => {}
                broken => {
                    tracing::debug!(?broken, "the event stream broke; reopening it");
                    events = self.reopen().await?;
                }
            }
            let state = self.task(task_id).await?;
            if state.outcome != Outcome::Running {
                return Ok(state);
            }
        }
    }

    /// [`Self::watch`], tried again a few times if the node cannot be
    /// reached — see [`REOPEN_ATTEMPTS`].
    async fn reopen(&self) -> Result<Events, ClientError> {
        let mut wait = Duration::from_millis(250);
        for _ in 1..REOPEN_ATTEMPTS {
            match self.watch().await {
                Err(ClientError::Unreachable { .. }) => {
                    tokio::time::sleep(wait).await;
                    wait *= 2;
                }
                reached => return reached,
            }
        }
        self.watch().await
    }

    /// Calls `method` and returns its result.
    pub async fn call(&self, method: &str, params: Value) -> Result<Value, ClientError> {
        let unreachable = |error: &dyn std::fmt::Display| self.unreachable(error);

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

/// One thing `GET /events` said.
#[derive(Debug, Clone, PartialEq)]
pub enum Watched {
    Event(Event),
    /// This watcher fell behind and missed events; whatever it is waiting
    /// for has to be asked about rather than heard.
    Resync,
}

/// An open `GET /events`.
pub struct Events {
    addr: SocketAddr,
    body: Incoming,
    /// Bytes read but not yet a whole event: a read can end anywhere, half an
    /// event or three.
    unread: Vec<u8>,
    silence: Duration,
}

impl Events {
    /// The next thing the node says, or `None` once it has closed the stream.
    ///
    /// **An event this build cannot read is skipped**, not an error: a newer
    /// node may publish types this client has never heard of, and events are
    /// ids a watcher refetches by (D2), so there is nothing in one it must
    /// not miss. The keep-alive comments are skipped likewise — but they are
    /// still something heard, so a stream that carries nothing, not even
    /// those, for longer than the client's silence allows answers with an
    /// error: it is broken, whether or not anything closed it.
    pub async fn next(&mut self) -> Option<Result<Watched, ClientError>> {
        loop {
            if let Some(end) = self.unread.windows(2).position(|pair| pair == b"\n\n") {
                let block: Vec<u8> = self.unread.drain(..end + 2).collect();
                if let Some(watched) = read_block(&String::from_utf8_lossy(&block)) {
                    return Some(Ok(watched));
                }
                continue;
            }
            let Ok(read) = tokio::time::timeout(self.silence, self.body.frame()).await else {
                return Some(Err(ClientError::Unreachable {
                    addr: self.addr,
                    message: format!("the event stream said nothing for {:?}", self.silence),
                }));
            };
            match read? {
                Ok(frame) => {
                    if let Ok(data) = frame.into_data() {
                        self.unread.extend_from_slice(&data);
                    }
                }
                Err(error) => {
                    return Some(Err(ClientError::Unreachable {
                        addr: self.addr,
                        message: error.to_string(),
                    }));
                }
            }
        }
    }
}

/// One server-sent event, as `crate::events` writes it: an `event:` line and
/// one `data:` line of JSON. `None` for anything else.
fn read_block(block: &str) -> Option<Watched> {
    let field = |name: &str| {
        block
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .map(str::trim_start)
    };
    match field("event:")? {
        "resync" => Some(Watched::Resync),
        _ => serde_json::from_str(field("data:")?)
            .ok()
            .map(Watched::Event),
    }
}

#[cfg(test)]
mod tests {
    use distlib_core::ItemId;

    use super::*;

    #[test]
    fn an_event_reads_back_as_the_node_wrote_it() {
        let item_id = ItemId::from_bytes([1; 32]);
        let block = format!(
            "event: catalogue.item_added\ndata: {}\n\n",
            serde_json::to_string(&Event::ItemAdded { item_id }).unwrap_or_default()
        );
        assert_eq!(
            read_block(&block),
            Some(Watched::Event(Event::ItemAdded { item_id }))
        );
        assert_eq!(
            read_block("event: resync\ndata: {\"type\":\"resync\"}\n\n"),
            Some(Watched::Resync)
        );
    }

    #[test]
    fn what_this_build_cannot_read_is_skipped() {
        assert_eq!(read_block(":\n\n"), None, "a keep-alive");
        assert_eq!(
            read_block("event: wish.changed\ndata: {\"type\":\"wish.changed\"}\n\n"),
            None,
            "an event from a newer node"
        );
    }
}
