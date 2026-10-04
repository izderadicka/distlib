//! The local API, driven over its real listener against a real group.
//!
//! Not a router-level test: `serve` binds a socket, and the token check sits in
//! front of everything, so these go over HTTP to a port the server chose. What
//! is being checked is that a caller holding the token can run a group and a
//! caller without one cannot.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use distlib_api::{
    Api, Client, Server, serve,
    tasks::{Downloaded, Outcome, Source, TaskState, Tasks},
    uploads::Uploads,
};
use distlib_consensus::{MemberRecord, MembershipNode};
use distlib_core::{ContentHash, Item, ItemId, MemberId, NodeAddr, Ticket};
use distlib_net::{AllowlistHooks, Transport, allowlist, endpoint::configure};
use distlib_store::{ReindexHandle, SearchIndex, Store, StoredItem};
use distlib_sync::Catalogue;
use http_body_util::{BodyExt as _, Full, StreamBody};
use hyper::{
    Request, StatusCode,
    body::{Bytes, Frame},
    header::{AUTHORIZATION, WWW_AUTHENTICATE},
};
use hyper_util::{client::legacy::Client as Hyper, rt::TokioExecutor};
use iroh::{
    Endpoint, SecretKey,
    endpoint::{RelayMode, presets},
    protocol::Router,
};
use iroh_gossip::net::Gossip;
use secrecy::SecretString;
use serde_json::{Value, json};
use tempfile::TempDir;

/// Logs to this test's output, filtered by `RUST_LOG`.
///
/// CI sets `RUST_LOG`, and nextest shows a test's output only when it fails,
/// so a failure that will not reproduce arrives with the log of what led to
/// it. Unset, only errors are logged. One subscriber serves every node in the
/// process: the first call installs it and the rest do nothing.
fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

/// The upload cap the harness serves with: above axum's two-megabyte default
/// body limit, so a test can show that limit is not the one in force.
const MAX_UPLOAD: u64 = 4_000_000;

/// A handle nobody answers.
///
/// These tests exercise `group.*` and `node.status` against a bare consensus
/// node — no catalogue, no read model — so `admin.reindex` has nothing behind
/// it to call. Good enough here: nothing in this file asks for one.
fn no_reindex() -> ReindexHandle {
    let (sender, _receiver) = tokio::sync::mpsc::channel(1);
    ReindexHandle::new(sender)
}

/// A founded one-node group with its API up.
struct Harness {
    node: Arc<MembershipNode>,
    server: Server,
    token: String,
    /// One per node started here, the API's own first.
    ///
    /// A node no longer owns what serves it, and a router left running holds
    /// an endpoint open — so the harness keeps them and [`Harness::shutdown`]
    /// closes every one, including the routers of nodes a test stops itself.
    routers: Vec<Router>,
    /// In memory, and empty until a `library.*` test writes into it directly
    /// — there is no catalogue or projection here to fill it the real way.
    store: Store,
    /// Same reasoning, same emptiness, for `library.search`.
    search: SearchIndex,
    /// The server's own task registry, for a test to start a download in
    /// directly — nothing here has files to fetch the real way.
    tasks: Tasks,
    /// The API's catalogue, for a test to read what `library.add` wrote.
    catalogue: Catalogue,
    /// Where the server holds uploads, for a test to see what is left there.
    uploads: std::path::PathBuf,
    _dir: TempDir,
}

impl Harness {
    /// Founds a group and serves the API on a port the OS picks.
    async fn start() -> Self {
        Self::start_with(no_reindex()).await
    }

    /// [`Harness::start`], but with a `reindex_handle` the caller supplies —
    /// for the tests that need to control what answers `admin.reindex`
    /// rather than have nothing behind it.
    async fn start_with(reindex_handle: ReindexHandle) -> Self {
        init_logging();
        let store = Store::open(None).await.unwrap();
        let search = SearchIndex::open(None).await.unwrap();
        let secret = SecretKey::generate();
        let id = MemberId::from(secret.public());
        let dir = TempDir::new().unwrap();

        let (writer, reader) = allowlist(id, []);
        let hooks = AllowlistHooks::new(reader);
        let endpoint = configure(
            Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled),
            secret.clone(),
            hooks.clone(),
            distlib_consensus::alpns(),
        )
        .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .unwrap()
        .bind()
        .await
        .unwrap();

        let addr = NodeAddr {
            relay: None,
            direct: endpoint.bound_sockets().into_iter().collect(),
        };
        let swarm = Gossip::builder().spawn(endpoint.clone());
        let transport = Transport::new(endpoint.clone(), swarm).unwrap();
        let node = Arc::new(
            MembershipNode::start(
                transport.clone(),
                hooks,
                writer,
                dir.path(),
                vec![(id, NodeAddr::default())],
            )
            .await
            .unwrap(),
        );
        // In memory: nothing here exercises `library.add` or
        // `library.download`, so these exist only to give `Api` a catalogue
        // and a `Blobs` to hold — see `Api::catalogue`'s own doc comment for
        // why neither field can be optional. One store between them, as in
        // production: what a download fetches has to land where the
        // catalogue's own handler serves from.
        let media = iroh_blobs::store::mem::MemStore::new();
        let blobs = distlib_net::Blobs::new(&media, &endpoint);
        let catalogue =
            Catalogue::start(transport, (*media).clone(), None, &secret, node.subscribe())
                .await
                .unwrap();
        let router = distlib_net::serve(
            endpoint,
            node.protocols()
                .into_iter()
                .chain(catalogue.protocols())
                .collect(),
        );

        node.init_group(
            vec![(
                MemberRecord {
                    member_id: id,
                    display_name: "founder".to_owned(),
                    pledge_bytes: 0,
                },
                addr,
            )],
            &secret,
        )
        .await
        .unwrap();

        let token = "0123456789abcdef".repeat(4);
        let tasks = Tasks::new(distlib_api::events::bus());
        let server = serve(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            Api {
                node: Arc::clone(&node),
                secret,
                net: distlib_core::NetConfig::default(),
                reindex_handle,
                catalogue: catalogue.clone(),
                blobs,
                store: store.clone(),
                search: search.clone(),
                tasks: tasks.clone(),
                downloads: dir.path().join("downloads"),
                uploads: Uploads::open(dir.path().join("uploads"), MAX_UPLOAD).unwrap(),
            },
            SecretString::from(token.clone()),
        )
        .await
        .unwrap();
        Self {
            node,
            server,
            token,
            routers: vec![router],
            store,
            search,
            tasks,
            catalogue,
            uploads: dir.path().join("uploads"),
            _dir: dir,
        }
    }

    /// Founds a group of `voters` core nodes and serves the API on the first.
    ///
    /// Needed because a threshold only becomes visible above three voters: with
    /// one or two, every proposal either applies at once or can never be
    /// decided at all. Four is the smallest group where an approval can land,
    /// count, and still leave the proposal waiting — which is the case
    /// `group.approve` has to report honestly.
    ///
    /// Only the first node serves the API; the rest propose through their own
    /// `MembershipNode`, which is what the other operators would be doing.
    #[cfg(feature = "slow-tests")]
    async fn founded_by(voters: usize) -> (Self, Vec<Arc<MembershipNode>>, Vec<SecretKey>) {
        use std::time::Duration;
        use tokio::time::timeout;

        init_logging();
        let dir = TempDir::new().unwrap();
        let secrets: Vec<SecretKey> = (0..voters).map(|_| SecretKey::generate()).collect();
        let ids: Vec<MemberId> = secrets
            .iter()
            .map(|secret| MemberId::from(secret.public()))
            .collect();

        let mut nodes = Vec::new();
        let mut routers = Vec::new();
        let mut addrs = Vec::new();
        // Only node 0 ends up serving the API, so only it gets a catalogue —
        // building one for the others would be work spent on something
        // nothing here asks of them.
        let mut catalogue = None;
        let mut blobs = None;
        for (index, secret) in secrets.iter().enumerate() {
            let others = ids
                .iter()
                .enumerate()
                .filter(|(other, _)| *other != index)
                .map(|(_, id)| *id);
            let (writer, reader) = allowlist(ids[index], others);
            let hooks = AllowlistHooks::new(reader);
            let endpoint = configure(
                Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled),
                secret.clone(),
                hooks.clone(),
                distlib_consensus::alpns(),
            )
            .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .unwrap()
            .bind()
            .await
            .unwrap();

            addrs.push(NodeAddr {
                relay: None,
                direct: endpoint.bound_sockets().into_iter().collect(),
            });
            let core = ids.iter().map(|id| (*id, NodeAddr::default())).collect();
            let swarm = Gossip::builder().spawn(endpoint.clone());
            let transport = Transport::new(endpoint.clone(), swarm).unwrap();
            let node = Arc::new(
                MembershipNode::start(
                    transport.clone(),
                    hooks,
                    writer,
                    &{
                        let path = dir.path().join(format!("node-{index}"));
                        std::fs::create_dir_all(&path).unwrap();
                        path
                    },
                    core,
                )
                .await
                .unwrap(),
            );
            let protocols = if index == 0 {
                let media = iroh_blobs::store::mem::MemStore::new();
                blobs = Some(distlib_net::Blobs::new(&media, &endpoint));
                let started =
                    Catalogue::start(transport, (*media).clone(), None, secret, node.subscribe())
                        .await
                        .unwrap();
                let protocols = node
                    .protocols()
                    .into_iter()
                    .chain(started.protocols())
                    .collect();
                catalogue = Some(started);
                protocols
            } else {
                node.protocols()
            };
            routers.push(distlib_net::serve(endpoint, protocols));
            nodes.push(node);
        }

        let founders = ids
            .iter()
            .zip(&addrs)
            .enumerate()
            .map(|(index, (id, addr))| {
                (
                    MemberRecord {
                        member_id: *id,
                        display_name: format!("founder-{index}"),
                        pledge_bytes: 0,
                    },
                    addr.clone(),
                )
            })
            .collect();
        nodes[0].init_group(founders, &secrets[0]).await.unwrap();
        for node in &nodes {
            timeout(Duration::from_secs(15), async {
                while node.membership().group_id().is_none() {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .expect("the founding entry must reach every founder");
        }

        let store = Store::open(None).await.unwrap();
        let search = SearchIndex::open(None).await.unwrap();
        let catalogue = catalogue.expect("node 0 built one above");
        let token = "0123456789abcdef".repeat(4);
        let tasks = Tasks::new(distlib_api::events::bus());
        let server = serve(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            Api {
                node: Arc::clone(&nodes[0]),
                secret: secrets[0].clone(),
                net: distlib_core::NetConfig::default(),
                reindex_handle: no_reindex(),
                catalogue: catalogue.clone(),
                blobs: blobs.expect("node 0 built one above"),
                store: store.clone(),
                search: search.clone(),
                tasks: tasks.clone(),
                downloads: dir.path().join("downloads"),
                uploads: Uploads::open(dir.path().join("uploads"), MAX_UPLOAD).unwrap(),
            },
            SecretString::from(token.clone()),
        )
        .await
        .unwrap();

        let harness = Self {
            node: Arc::clone(&nodes[0]),
            server,
            token,
            routers,
            store,
            search,
            tasks,
            catalogue,
            uploads: dir.path().join("uploads"),
            _dir: dir,
        };
        (harness, nodes, secrets)
    }

    /// A call carrying the right token. Returns its `result`.
    async fn call(&self, method: &str, params: Value) -> Value {
        let (status, answer) = self.post(Some(&self.token), rpc(method, params)).await;
        assert_eq!(status, StatusCode::OK, "{answer}");
        answer
            .get("result")
            .unwrap_or_else(|| panic!("expected a result; got {answer}"))
            .clone()
    }

    /// A call expected to be refused. Returns its `error`.
    async fn refuse(&self, method: &str, params: Value) -> Value {
        let (status, answer) = self.post(Some(&self.token), rpc(method, params)).await;
        assert_eq!(status, StatusCode::OK, "{answer}");
        answer
            .get("error")
            .unwrap_or_else(|| panic!("expected an error; got {answer}"))
            .clone()
    }

    /// Posts a body, returning the status and whatever came back.
    ///
    /// Deliberately raw rather than a typed client: these tests are the only
    /// caller until the CLI arrives, and the envelope rules — a batch, a wrong
    /// version, a missing token — are exactly the ones a typed client would
    /// make unreachable.
    async fn post(&self, token: Option<&str>, body: Value) -> (StatusCode, Value) {
        let mut request = Request::post(format!("http://{}/rpc", self.server.addr()))
            .header("content-type", "application/json");
        if let Some(token) = token {
            request = request.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        let request = request
            .body(Full::new(Bytes::from(body.to_string())))
            .unwrap();

        let response = Hyper::builder(TokioExecutor::new())
            .build_http()
            .request(request)
            .await
            .unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();

        // Every answer is JSON, refusals included (D9) — so a body that is not
        // is a failure of the server, not something to tolerate here.
        let answer = serde_json::from_slice(&body).unwrap_or_else(|_| {
            panic!(
                "a {status} answer that is not JSON: {}",
                String::from_utf8_lossy(&body)
            )
        });
        (status, answer)
    }

    /// `POST /upload` with `query`, sending `body` in one piece with its length
    /// declared, or — `chunked` — in pieces with none, the way a stream of
    /// unknown length is sent. The answer must be JSON, whatever the status.
    async fn upload(&self, query: &str, body: Vec<u8>, chunked: bool) -> (StatusCode, Value) {
        self.try_upload(query, body, chunked).await.unwrap()
    }

    /// [`Harness::upload`], but handing back a connection that failed before
    /// an answer arrived rather than panicking on it.
    async fn try_upload(
        &self,
        query: &str,
        body: Vec<u8>,
        chunked: bool,
    ) -> Result<(StatusCode, Value), hyper_util::client::legacy::Error> {
        let request = Request::post(format!("http://{}/upload?{query}", self.server.addr()))
            .header(AUTHORIZATION, format!("Bearer {}", self.token));
        let response = if chunked {
            let pieces: Vec<Result<Frame<Bytes>, std::convert::Infallible>> = body
                .chunks(64 * 1024)
                .map(|piece| Ok(Frame::data(Bytes::copy_from_slice(piece))))
                .collect();
            let body = StreamBody::new(futures_lite::stream::iter(pieces));
            Hyper::builder(TokioExecutor::new())
                .build_http()
                .request(request.body(body).unwrap())
                .await
        } else {
            Hyper::builder(TokioExecutor::new())
                .build_http()
                .request(request.body(Full::new(Bytes::from(body))).unwrap())
                .await
        };
        let response = response?;
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let answer = serde_json::from_slice(&body).unwrap_or_else(|_| {
            panic!(
                "a {status} answer that is not JSON: {}",
                String::from_utf8_lossy(&body)
            )
        });
        Ok((status, answer))
    }

    /// What is held in the upload directory: uploads received and not yet
    /// taken.
    fn held(&self) -> Vec<String> {
        std::fs::read_dir(&self.uploads)
            .map(|entries| {
                entries
                    .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Sends `method` to `path` and returns the status, one header and the
    /// body parsed as JSON — which it must be, whatever the status.
    async fn raw(
        &self,
        method: &str,
        path: &str,
        token: Option<&str>,
    ) -> (StatusCode, Option<String>, Value) {
        let mut request = Request::builder()
            .method(method)
            .uri(format!("http://{}{path}", self.server.addr()));
        if let Some(token) = token {
            request = request.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        let response = Hyper::builder(TokioExecutor::new())
            .build_http()
            .request(request.body(Full::new(Bytes::new())).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let challenge = response
            .headers()
            .get(WWW_AUTHENTICATE)
            .map(|value| value.to_str().unwrap().to_owned());
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let answer = serde_json::from_slice(&body).unwrap_or_else(|_| {
            panic!(
                "{method} {path} answered {status} with a body that is not JSON: {}",
                String::from_utf8_lossy(&body)
            )
        });
        (status, challenge, answer)
    }

    /// `GET`s a page of the UI, with no token, and reads it as text.
    async fn page(&self, path: &str) -> (StatusCode, hyper::HeaderMap, String) {
        let request = Request::get(format!("http://{}{path}", self.server.addr()))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let response = Hyper::builder(TokioExecutor::new())
            .build_http()
            .request(request)
            .await
            .unwrap();
        let (parts, body) = response.into_parts();
        let body = body.collect().await.unwrap().to_bytes();
        (
            parts.status,
            parts.headers,
            String::from_utf8_lossy(&body).into_owned(),
        )
    }

    /// Opens `GET /events` with the token and waits for its headers.
    ///
    /// Waiting for them is what makes a test that then changes something
    /// deterministic: the server subscribes before it answers, so once the 200
    /// is here nothing published afterwards can be missed.
    async fn watch(&self) -> Watcher {
        let request = Request::get(format!("http://{}/events", self.server.addr()))
            .header(AUTHORIZATION, format!("Bearer {}", self.token))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let response = Hyper::builder(TokioExecutor::new())
            .build_http()
            .request(request)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            "text/event-stream",
            "an event stream, not a document"
        );
        Watcher {
            body: response.into_body(),
            seen: String::new(),
        }
    }

    async fn shutdown(self) {
        self.server.shutdown();
        self.node.shutdown().await;
        // Last, as production does it: a router's shutdown closes the endpoint
        // under it, and the nodes a test stopped itself still have theirs here.
        for router in &self.routers {
            let _ = router.shutdown().await;
        }
    }
}

/// One open `GET /events`.
struct Watcher {
    body: hyper::body::Incoming,
    /// Everything read so far. Frames and events do not line up — one read
    /// can hold half an event or three — so what is asserted is the text.
    seen: String,
}

impl Watcher {
    /// Reads until an `event:` line naming `name` arrives, or fails the test,
    /// and answers with everything read up to and including it.
    ///
    /// "Within a bound" rather than "next": keep-alive comments and other
    /// events may arrive first, on their own schedules.
    async fn expect(&mut self, name: &str) -> String {
        let wanted = format!("event: {name}\n");
        let found = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !self.seen.contains(&wanted) {
                let frame = self.body.frame().await.expect("the stream ended").unwrap();
                if let Ok(data) = frame.into_data() {
                    self.seen.push_str(&String::from_utf8_lossy(&data));
                }
            }
        })
        .await;
        assert!(
            found.is_ok(),
            "no `{name}` event within ten seconds; the stream said:\n{}",
            self.seen
        );
        // Consumed, so that a second `expect` waits for a second event.
        std::mem::take(&mut self.seen)
    }
}

/// A well-formed single call.
fn rpc(method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
}

/// The code out of a JSON-RPC error object.
fn code(error: &Value) -> i64 {
    error["code"]
        .as_i64()
        .unwrap_or_else(|| panic!("expected an error object; got {error}"))
}

/// The code out of a whole response body.
/// Whether `error` is the connection being closed under a request still
/// being sent — aborted, reset or a broken pipe, whichever the platform says.
fn cut_off(error: &(dyn std::error::Error + 'static)) -> bool {
    std::iter::successors(Some(error), |error| error.source()).any(|error| {
        error.downcast_ref::<std::io::Error>().is_some_and(|io| {
            matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe
            )
        })
    })
}

fn raw_code(response: &Value) -> i64 {
    code(
        response
            .get("error")
            .unwrap_or_else(|| panic!("expected an error; got {response}")),
    )
}

#[tokio::test]
async fn a_call_with_the_wrong_token_is_refused() {
    // The whole security model of this listener: loopback plus the token. A
    // call that gets past this can make the node propose as itself.
    let harness = Harness::start().await;

    let (anonymous, answer) = harness.post(None, rpc("node.status", Value::Null)).await;
    assert_eq!(anonymous, StatusCode::UNAUTHORIZED, "no token at all");
    assert_eq!(raw_code(&answer), -32001, "refused in the envelope, too");

    let (stranger, _) = harness
        .post(Some(&"f".repeat(64)), rpc("node.status", Value::Null))
        .await;
    assert_eq!(
        stranger,
        StatusCode::UNAUTHORIZED,
        "a wrong token is no better than none"
    );

    // And the right token works, so this is not passing because the server is
    // simply broken.
    harness.call("node.status", Value::Null).await;

    harness.shutdown().await;
}

#[tokio::test]
async fn every_refusal_is_answered_in_json_with_its_http_status() {
    // D9: one parser for every answer. Before it, a refused token came back as
    // plain text, and a caller had to guess which kind of body it was holding
    // before it could read the reason. The statuses are kept exactly as they
    // were — browsers and proxies read them.
    let harness = Harness::start().await;

    for path in ["/rpc", "/events", "/upload"] {
        let method = if path == "/events" { "GET" } else { "POST" };
        let (status, challenge, answer) = harness.raw(method, path, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {path}");
        assert_eq!(raw_code(&answer), -32001, "{method} {path}: {answer}");
        assert_eq!(challenge.as_deref(), Some("Bearer"), "{method} {path}");
    }

    // The token guards routes, not the listener: an unknown path is answered
    // as unknown, with or without one. (`GET` of a path with no extension is
    // the UI's, which routes itself — see `the_ui_is_served_to_anybody`.)
    for (method, path) in [("GET", "/nowhere.js"), ("POST", "/nowhere")] {
        let (status, _, answer) = harness.raw(method, path, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{method} {path}");
        assert_eq!(raw_code(&answer), -32600, "{method} {path}: {answer}");
    }

    let (status, _, answer) = harness.raw("GET", "/rpc", Some(&harness.token)).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(raw_code(&answer), -32600, "{answer}");

    harness.shutdown().await;
}

#[tokio::test]
async fn the_ui_is_served_to_anybody() {
    // D3: the page is open, and everything about this node is behind the
    // token. With no `ui/dist` built — CI's case — what is served is the
    // placeholder; either way it is an HTML page.
    let harness = Harness::start().await;

    for path in ["/", "/members"] {
        let (status, headers, body) = harness.page(path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        assert!(
            headers["content-type"]
                .to_str()
                .unwrap()
                .starts_with("text/html"),
            "{path}"
        );
        assert!(body.contains("<html"), "{path}: {body}");
        // The token lives in the page's storage; script from anywhere else
        // is how it would be stolen.
        assert!(
            headers["content-security-policy"]
                .to_str()
                .unwrap()
                .contains("default-src 'self'"),
            "{path}"
        );
        assert_eq!(headers["x-content-type-options"], "nosniff", "{path}");
    }

    // A path that climbs out of the UI's folder finds nothing there.
    let (status, _, _) = harness.page("/../Cargo.toml").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    harness.shutdown().await;
}

#[tokio::test]
async fn a_watcher_hears_the_membership_change() {
    // 3a-1's acceptance: a page that is open when somebody is admitted is told
    // so, without asking.
    let harness = Harness::start().await;
    let mut watcher = harness.watch().await;

    harness
        .call(
            "group.propose_add",
            json!({ "member": MemberId::from(SecretKey::generate().public()) }),
        )
        .await;
    watcher.expect("membership.changed").await;

    harness.shutdown().await;
}

#[tokio::test]
async fn a_change_nobody_watched_does_not_silence_the_next_one() {
    // With no page open there is no receiver, and a broadcast send then fails
    // — which is most of a node's life, not an error. A producer that treated
    // it as one would stop at the first change made while nobody was looking,
    // and every page opened afterwards would wait for events that never come.
    let harness = Harness::start().await;

    harness
        .call(
            "group.propose_add",
            json!({ "member": MemberId::from(SecretKey::generate().public()) }),
        )
        .await;

    let mut watcher = harness.watch().await;
    harness
        .call(
            "group.propose_add",
            json!({ "member": MemberId::from(SecretKey::generate().public()) }),
        )
        .await;
    watcher.expect("membership.changed").await;

    harness.shutdown().await;
}

#[tokio::test]
async fn a_page_that_connects_mid_download_is_told_how_far_it_has_got() {
    // 3a-5: a page reloaded while a download runs must get its bar back at
    // once, not at the next report — which, for a download that has stalled
    // on a slow provider, may be a long time coming.
    let harness = Harness::start().await;
    let mut download = harness
        .tasks
        .start_download(ItemId::from_bytes([3; 32]), None, 2, 100);
    // Published to nobody: no page was open.
    download.progress(40);

    let mut watcher = harness.watch().await;
    let replayed = watcher.expect("download.progress").await;
    assert!(replayed.contains(r#""bytes_done":40"#), "{replayed}");

    // And then live, as for any other watcher.
    download.finish(Vec::new());
    watcher.expect("download.finished").await;

    harness.shutdown().await;
}

#[tokio::test]
async fn a_watcher_that_hears_nothing_asks_rather_than_waiting_for_ever() {
    // The ending was published before this watcher connected, so no stream it
    // opens will ever carry it — the case of a connection that died without
    // closing, or broke and missed the ending while it was down. Only
    // noticing the silence, reopening and asking can end this wait.
    let harness = Harness::start().await;
    let download = harness
        .tasks
        .start_download(ItemId::from_bytes([3; 32]), None, 1, 100);
    let task_id = download.id();
    download.finish(Vec::new());

    let client = Client::new(
        harness.server.addr(),
        SecretString::from(harness.token.clone()),
    )
    .silent_after(Duration::from_millis(200));
    let events = client.watch().await.unwrap();
    let state = tokio::time::timeout(Duration::from_secs(10), client.until_ended(events, task_id))
        .await
        .expect("a silent stream is taken for broken, not waited on")
        .expect("a broken stream is reopened, not taken for a failed download");
    assert_eq!(state.outcome, Outcome::Finished { files: Vec::new() });

    harness.shutdown().await;
}

#[tokio::test]
async fn a_download_can_be_asked_after_while_it_runs_and_once_it_has_ended() {
    let harness = Harness::start().await;
    let mut download =
        harness
            .tasks
            .start_download(ItemId::from_bytes([3; 32]), Some("Dune".to_owned()), 1, 100);
    let task_id = download.id();
    download.progress(40);

    // Read back as the typed state `distlib download` reads it as, so that
    // what the server writes and what a client parses cannot drift apart.
    let ask = || async {
        serde_json::from_value::<TaskState>(
            harness
                .call("library.task", json!({ "task_id": task_id }))
                .await,
        )
        .unwrap()
    };
    let running = ask().await;
    assert_eq!(running.outcome, Outcome::Running);
    assert_eq!(running.progress.bytes_done, 40);
    assert_eq!(running.title.as_deref(), Some("Dune"));

    let written = Downloaded {
        file: ContentHash::from_bytes([4; 32]),
        filename: "dune.epub".to_owned(),
        path: "/books/dune.epub".into(),
        from: Source::Network,
    };
    download.finish(vec![written.clone()]);
    let finished = ask().await;
    assert_eq!(
        finished.outcome,
        Outcome::Finished {
            files: vec![written]
        }
    );
    assert_eq!(finished.progress.files_done, 1);

    let unknown = harness
        .refuse("library.task", json!({ "task_id": 999 }))
        .await;
    assert!(
        unknown["message"]
            .as_str()
            .unwrap()
            .contains("no such task"),
        "{unknown}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn an_edit_must_say_what_to_write() {
    // Refused before anything is asked of the catalogue — which this harness
    // has none of worth the name. That an edit of an item nobody added is
    // refused is `library.rs`'s to show, against a real one.
    let harness = Harness::start().await;
    let item_id = ItemId::from_bytes([3; 32]);

    let no_fields = harness
        .refuse(
            "library.edit_metadata",
            json!({ "item_id": item_id, "fields": {} }),
        )
        .await;
    assert_eq!(code(&no_fields), -32602, "{no_fields}");

    let not_a_field = harness
        .refuse(
            "library.edit_metadata",
            json!({ "item_id": item_id, "fields": { "replicas": 5 } }),
        )
        .await;
    assert_eq!(
        code(&not_a_field),
        -32602,
        "replicas is custodianship, not metadata: {not_a_field}"
    );

    let no_such_field = harness
        .refuse(
            "library.edit_metadata",
            // Beside a real field, so that ignoring the unknown one would
            // leave an edit to make rather than an empty one to refuse.
            json!({ "item_id": item_id, "fields": { "title": "Dune", "colour": "blue" } }),
        )
        .await;
    assert_eq!(
        code(&no_such_field),
        -32602,
        "a name that is not a field is refused, not ignored: {no_such_field}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn node_status_reports_the_group_this_node_founded() {
    let harness = Harness::start().await;
    let status = harness.call("node.status", Value::Null).await;

    assert_eq!(status["member"], json!(harness.node.id()));
    assert!(!status["group"].is_null(), "the group was founded");
    assert_eq!(status["core"], json!(true), "a founder is a voter");
    assert_eq!(status["members"], json!(1));
    assert_eq!(status["raft"], json!("Leader"));
    assert_eq!(
        status["sync"],
        json!({"neighbours": [], "last_sync": []}),
        "a node alone has nobody to be a neighbour of (C14)"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn admitting_a_member_moves_the_membership() {
    let harness = Harness::start().await;
    let newcomer = MemberId::from(SecretKey::generate().public());

    let before = harness.call("node.status", Value::Null).await["changed_at"].clone();

    let admitted = harness
        .call(
            "group.propose_add",
            json!({ "member": newcomer, "name": "bob" }),
        )
        .await;
    let after = admitted["changed_at"].clone();
    assert_ne!(before, after, "admitting somebody changes the membership");

    let listed = harness.call("group.members", Value::Null).await;
    let members = listed["members"].as_array().unwrap();
    let bob = members
        .iter()
        .find(|member| member["member"] == json!(newcomer))
        .expect("the newcomer is a member");
    assert_eq!(bob["name"], json!("bob"));
    assert_eq!(bob["core"], json!(false), "admission is not promotion");
    assert_eq!(
        bob["pledge_bytes"],
        json!(0),
        "admitting somebody does not speak for their storage"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn a_proposal_the_rules_refuse_comes_back_as_an_error() {
    // Committing and applying are different things, and the API must not report
    // a refused event as success just because the write reached the log.
    let harness = Harness::start().await;
    let stranger = MemberId::from(SecretKey::generate().public());

    let refused = harness
        .refuse(
            "group.propose_expel",
            json!({ "member": stranger, "reason": "never joined" }),
        )
        .await;

    assert_eq!(code(&refused), -32000);
    assert!(
        refused["message"]
            .as_str()
            .unwrap()
            .contains(&stranger.to_string()),
        "the caller should learn which member was refused: {refused}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn a_proposal_that_takes_effect_says_so_and_one_that_waits_says_what_it_waits_for() {
    // The gap this closes. Before approvals every proposal took effect, so
    // "admitted" was always true; since 2.2-1 one may be waiting for core
    // members to agree, and an API that reported only "committed" would tell a
    // caller something had happened that had not — leaving them to wonder why
    // the person they admitted still cannot connect.
    //
    // The waiting case here is a node proposing its own expulsion. In a group
    // of one that is the only proposal that *can* wait — removing a voter takes
    // a majority of the core, and the one core member is the one being removed,
    // so their own proposal is not their approval. It never decides, which is
    // the point of `the_last_voter_cannot_be_expelled`; what is being checked
    // is that the API says so rather than reporting success.
    let harness = Harness::start().await;
    let me = harness.node.id();

    let applied = harness
        .call(
            "group.propose_add",
            json!({ "member": MemberId::from(SecretKey::generate().public()) }),
        )
        .await;
    assert_eq!(
        applied["applied"],
        json!(true),
        "a core member admitting somebody is one step: their proposal is their approval"
    );
    assert_eq!(applied["waiting"], Value::Null);

    let waiting = harness
        .call(
            "group.propose_expel",
            json!({ "member": me, "reason": "the only voter" }),
        )
        .await;
    assert_eq!(
        waiting["applied"],
        json!(false),
        "removing a voter takes a majority, and the target does not vote on it"
    );
    assert_eq!(waiting["waiting"]["approvals"], json!(0));
    assert_eq!(waiting["waiting"]["needed"], json!(1));

    // And the caller is told which proposal it is, by the index everything else
    // names it by.
    let proposal = waiting["proposal"].as_u64().expect("a proposal index");
    let pending = harness.call("group.pending", Value::Null).await;
    let listed = pending["pending"].as_array().unwrap();
    assert_eq!(listed.len(), 1, "{pending}");
    assert_eq!(listed[0]["proposal"], json!(proposal));
    assert_eq!(listed[0]["proposer"], json!(me));
    assert_eq!(listed[0]["needed"], json!(1));
    assert!(
        listed[0]["what"].as_str().unwrap().starts_with("expel "),
        "an operator deciding about this has to be able to read it: {pending}"
    );

    // Status carries the count, so somebody who never runs `distlib pending`
    // still finds out there is something to look at.
    let status = harness.call("node.status", Value::Null).await;
    assert_eq!(status["pending"], json!(1));

    harness.shutdown().await;
}

#[tokio::test]
async fn a_proposer_can_withdraw_their_own_proposal() {
    let harness = Harness::start().await;
    let me = harness.node.id();

    let waiting = harness
        .call(
            "group.propose_expel",
            json!({ "member": me, "reason": "changed my mind in a moment" }),
        )
        .await;
    let proposal = waiting["proposal"].as_u64().expect("a proposal index");

    harness
        .call("group.withdraw", json!({ "proposal": proposal }))
        .await;

    let pending = harness.call("group.pending", Value::Null).await;
    assert_eq!(pending["pending"].as_array().unwrap().len(), 0, "{pending}");

    // And a proposal that is not there cannot be approved into existence.
    let refused = harness
        .refuse("group.approve", json!({ "proposal": proposal }))
        .await;
    assert_eq!(code(&refused), -32000);

    harness.shutdown().await;
}

// Four in-process raft nodes: skipped by `--no-default-features`, like every
// other multi-node test in this workspace.
#[cfg(feature = "slow-tests")]
#[tokio::test]
async fn approving_answers_about_the_proposal_not_about_the_approval() {
    use distlib_consensus::MembershipEvent;
    use std::time::Duration;
    use tokio::time::timeout;

    // The trap this exists for. An approval is never itself a proposal — the
    // fold dispatches it rather than holding it — so it *always* applies. An
    // implementation that reported on the approval's own log index would answer
    // "applied" every single time, and `distlib approve` would tell an operator
    // the change had happened while it was still an approval short.
    //
    // Four voters, because a threshold is only visible above three: a majority
    // of four is three, so the proposer's own approval plus one more is two,
    // and the proposal is still waiting when this node approves it.
    let (harness, nodes, secrets) = Harness::founded_by(4).await;
    let doomed = nodes[3].id();

    // Another operator proposes removing a voter. Their own approval counts, so
    // it stands at one of three.
    let proposal = nodes[1]
        .propose(
            MembershipEvent::MemberExpelled {
                member: doomed,
                reason: "proposed by somebody else".to_owned(),
            },
            &secrets[1],
        )
        .await
        .unwrap();
    timeout(Duration::from_secs(15), async {
        while harness.node.membership().pending().count() == 0 {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the proposal must replicate to the node serving the api");
    let pending = harness.call("group.pending", Value::Null).await;
    assert_eq!(
        pending["pending"][0]["approved_by_you"],
        json!(false),
        "{pending}"
    );

    let approved = harness
        .call("group.approve", json!({ "proposal": proposal }))
        .await;

    assert_eq!(
        approved["applied"],
        json!(false),
        "two of four is not a majority, and the answer is about the proposal: {approved}"
    );
    assert_eq!(
        approved["proposal"],
        json!(proposal),
        "not the approval's own index"
    );
    assert_eq!(approved["waiting"]["approvals"], json!(2));
    assert_eq!(approved["waiting"]["needed"], json!(3));
    assert_eq!(approved["already_approved"], json!(false));
    assert!(
        harness.node.membership().is_member(&doomed),
        "and nothing has happened to them yet"
    );

    // Saying it twice is accepted — the fold allows a repeat on purpose — but
    // the answer says so, and `pending` says whose agreement is already in.
    // Found by hand after phase 3: both used to read exactly like a first.
    let again = harness
        .call("group.approve", json!({ "proposal": proposal }))
        .await;
    assert_eq!(again["already_approved"], json!(true), "{again}");
    assert_eq!(
        again["waiting"]["approvals"],
        json!(2),
        "and it counts once"
    );
    let pending = harness.call("group.pending", Value::Null).await;
    assert_eq!(
        pending["pending"][0]["approved_by_you"],
        json!(true),
        "{pending}"
    );

    // The third approval decides it, and only then does the answer change.
    let third = nodes[2]
        .propose(MembershipEvent::Approved { proposal }, &secrets[2])
        .await;
    third.unwrap();
    timeout(Duration::from_secs(15), async {
        while harness.node.membership().is_member(&doomed) {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("three of four is a majority");

    // The other nodes first: the harness holds their routers, and closing an
    // endpoint out from under a Raft that has not stopped is the wrong order.
    for node in nodes.into_iter().skip(1) {
        node.shutdown().await;
    }
    harness.shutdown().await;
}

/// The pair that pins the threshold rule for the core group, and the reason
/// they are written as one test rather than two: the claim is not "this
/// applies" or "that waits" but that the *same* event takes a different
/// threshold depending on whether it moves the voters. Split apart, either half
/// passes against an implementation that treats every `CoreGroupChanged` alike.
///
/// Three voters, because that is the smallest group where a majority is more
/// than one and so the two answers can differ at all.
///
/// **Mutation check:** delete the `CoreGroupChanged` arm of
/// `MembershipState::changes_the_voters` and the second half goes red — a
/// demotion becomes a one-approval change and applies at once — while the first
/// half stays green.
#[cfg(feature = "slow-tests")]
#[tokio::test]
async fn moving_a_core_node_applies_while_dropping_one_waits_for_a_majority() {
    let (harness, nodes, _) = Harness::founded_by(3).await;
    let moved = nodes[2].id();

    // Where the log says that node is now, plus somewhere it is not. A real
    // move would replace the address outright; adding to it keeps the cluster
    // reachable for the second half of this test while still being a genuine
    // change to what the log records.
    let mut addr = harness
        .node
        .core_addresses()
        .into_iter()
        .find_map(|(member, addr)| (member == moved).then_some(addr))
        .expect("the log records an address for every core node");
    addr.direct
        .insert(SocketAddr::from((Ipv4Addr::LOCALHOST, 1)));

    let answer = harness
        .call(
            "group.propose_core",
            json!({ "change": "set", "member": moved, "addr": addr }),
        )
        .await;
    assert_eq!(
        answer["applied"],
        json!(true),
        "the same three people vote before and after, so one core member decides it: {answer}"
    );

    // The same event kind, proposed by the same core member, about the same
    // person — and now it has to wait, because this one changes who votes.
    let answer = harness
        .call(
            "group.propose_core",
            json!({ "change": "remove", "member": moved }),
        )
        .await;
    assert_eq!(
        answer["applied"],
        json!(false),
        "dropping a voter takes a majority of the voters: {answer}"
    );
    assert_eq!(answer["waiting"]["approvals"], json!(1));
    assert_eq!(answer["waiting"]["needed"], json!(2));
    assert!(
        harness.node.membership().is_core(&moved),
        "and nothing has happened to them yet"
    );

    // And it is legible to the core members who have to decide about it. The
    // event carries the whole desired core group rather than a delta, so the
    // obvious rendering — list what is in it — tells an approver who would be
    // *left*, which for a demotion is everyone except the one fact that matters.
    let pending = harness.call("group.pending", Value::Null).await;
    let what = pending["pending"][0]["what"].as_str().unwrap_or_default();
    assert!(
        what.contains(&moved.to_string()) && what.contains("drop"),
        "a pending core change has to name who it is about: {pending}"
    );

    // The other nodes first: the harness holds their routers, and closing an
    // endpoint out from under a Raft that has not stopped is the wrong order.
    for node in nodes.into_iter().skip(1) {
        node.shutdown().await;
    }
    harness.shutdown().await;
}

#[tokio::test]
async fn propose_core_refuses_a_no_op_and_anything_it_would_have_to_guess_at() {
    let harness = Harness::start().await;
    let me = harness.node.id();

    // Committing either of these would apply — the map is valid — and applying
    // moves `changed_at`, which invalidates every proposal in flight. Asking
    // for something that is already true must not be able to do that.
    let (_, here) = harness
        .node
        .core_addresses()
        .into_iter()
        .next()
        .expect("the founder is a core node");
    let unchanged = harness
        .refuse(
            "group.propose_core",
            json!({ "change": "set", "member": me, "addr": here }),
        )
        .await;
    assert_eq!(code(&unchanged), -32602);

    let stranger = MemberId::from(SecretKey::generate().public());
    let not_core = harness
        .refuse(
            "group.propose_core",
            json!({ "change": "remove", "member": stranger }),
        )
        .await;
    assert_eq!(code(&not_core), -32602);

    // And which of the two it is has to be *said*. An address left off a
    // `set` is not a quiet demotion, and there is no shape here that means one
    // thing or the other depending on whether a field was remembered.
    for malformed in [
        json!({ "change": "set", "member": me }),
        json!({ "member": me, "addr": here }),
        json!({ "change": "move", "member": me, "addr": here }),
        json!({ "change": "remove", "member": me, "addr": here }),
    ] {
        let refused = harness
            .refuse("group.propose_core", malformed.clone())
            .await;
        assert_eq!(
            code(&refused),
            -32602,
            "{malformed} should not be a proposal at all; got {refused}"
        );
    }

    harness.shutdown().await;
}

#[tokio::test]
async fn a_pledge_is_set_for_this_node_and_no_other() {
    let harness = Harness::start().await;

    harness
        .call("group.pledge_set", json!({ "bytes": 4096 }))
        .await;

    let listed = harness.call("group.members", Value::Null).await;
    let members = listed["members"].as_array().unwrap();
    assert_eq!(members[0]["pledge_bytes"], json!(4096));

    // There is no member parameter to abuse: a pledge belongs to its owner, so
    // the method takes the node's own id and nothing else.
    let refused = harness
        .refuse(
            "group.pledge_set",
            json!({ "bytes": 1, "member": "whoever" }),
        )
        .await;
    assert_eq!(
        code(&refused),
        -32602,
        "an unexpected parameter is refused rather than quietly ignored"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn malformed_calls_are_reported_by_kind() {
    let harness = Harness::start().await;

    let unknown = harness.refuse("group.nonsense", Value::Null).await;
    assert_eq!(code(&unknown), -32601);

    let bad_params = harness
        .refuse("group.propose_add", json!({ "member": "not-an-id" }))
        .await;
    assert_eq!(code(&bad_params), -32602);

    let missing_params = harness.refuse("group.propose_expel", Value::Null).await;
    assert_eq!(code(&missing_params), -32602);

    // Batches are deliberately not served — see the rpc module docs. An array
    // where an object belongs is an invalid request, which is the honest answer
    // rather than silently running its first element.
    let (_, batch) = harness
        .post(
            Some(&harness.token),
            json!([{"jsonrpc": "2.0", "id": 1, "method": "node.status"}]),
        )
        .await;
    assert_eq!(raw_code(&batch), -32600);

    let (_, wrong_version) = harness
        .post(
            Some(&harness.token),
            json!({"jsonrpc": "1.0", "id": 1, "method": "node.status"}),
        )
        .await;
    assert_eq!(raw_code(&wrong_version), -32600);

    harness.shutdown().await;
}

/// `admin.reindex` actually calls through to the projection, over the real
/// HTTP dispatch — not just `ReindexHandle::request` on its own, which is all
/// the `distlib-store` and `distlib` test suites exercise. What is being
/// pinned here is `Api::call`'s routing: that `"admin.reindex"` reaches
/// [`distlib_api::Api::call`]'s `reindex` arm and blocks on the same handle a
/// caller behind it would answer.
#[tokio::test]
async fn admin_reindex_reaches_the_projection_through_the_rpc_dispatch() {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let harness = Harness::start_with(ReindexHandle::new(sender)).await;

    // Stands in for the projection task: answers the one request this test
    // sends, the moment it arrives, the way `Projection::run` answers one it
    // is asked for.
    tokio::spawn(async move {
        if let Some(done) = receiver.recv().await {
            let _ = done.send(());
        }
    });

    let result = harness.call("admin.reindex", Value::Null).await;
    assert_eq!(result, json!({}));

    harness.shutdown().await;
}

/// The other half: a projection that is gone — dropped, or never started —
/// is reported as a call failure rather than a request that never returns.
/// [`no_reindex`] drops its receiver immediately, which is exactly that case.
#[tokio::test]
async fn admin_reindex_reports_a_projection_that_is_gone() {
    let harness = Harness::start_with(no_reindex()).await;

    let refused = harness.refuse("admin.reindex", Value::Null).await;
    assert_eq!(code(&refused), -32000);

    harness.shutdown().await;
}

/// `library.item` reads the read model over the real RPC dispatch, not the
/// `Store` directly — pinning `Api::call`'s routing the same way the
/// `admin.reindex` pair above pins its own.
///
/// Written straight into `harness.store` rather than through a catalogue and
/// a projection, which this harness has neither of: what is being checked is
/// that `library.item` reads a row correctly, not that one arrives there.
#[tokio::test]
async fn library_item_reads_the_stored_record() {
    let harness = Harness::start().await;
    let id = ItemId::from_bytes([7; 32]);
    harness
        .store
        .upsert_item(StoredItem {
            item: Item {
                title: Some("Dune".to_owned()),
                authors: Some(vec!["Frank Herbert".to_owned()]),
                ..Item::new(id)
            },
            last_modified: 1,
        })
        .await
        .unwrap();

    let answer = harness.call("library.item", json!({ "item_id": id })).await;
    assert_eq!(answer["item_id"], json!(id));
    assert_eq!(answer["title"], json!("Dune"));
    assert_eq!(answer["authors"], json!(["Frank Herbert"]));

    harness.shutdown().await;
}

/// An id the read model has nothing for is a call failure, not a null result
/// a caller could mistake for "found, and empty".
#[tokio::test]
async fn library_item_reports_no_such_item() {
    let harness = Harness::start().await;
    let refused = harness
        .refuse(
            "library.item",
            json!({ "item_id": ItemId::from_bytes([9; 32]) }),
        )
        .await;
    assert_eq!(code(&refused), -32000);

    harness.shutdown().await;
}

/// `library.search` ranks against `harness.search` and reads the fields to
/// show back from `harness.store` — the same two-step `SearchIndex::search`'s
/// own doc comment describes, exercised here over the real RPC dispatch.
#[tokio::test]
async fn library_search_ranks_and_reads_hits_back() {
    let harness = Harness::start().await;
    let id = ItemId::from_bytes([7; 32]);
    let item = Item {
        title: Some("Dune".to_owned()),
        authors: Some(vec!["Frank Herbert".to_owned()]),
        ..Item::new(id)
    };
    harness
        .store
        .upsert_item(StoredItem {
            item: item.clone(),
            last_modified: 1,
        })
        .await
        .unwrap();
    harness.search.index_item(item).await.unwrap();
    harness.search.commit().await.unwrap();

    let answer = harness
        .call("library.search", json!({ "query": "Herbert" }))
        .await;
    let results = answer["results"].as_array().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["item_id"], json!(id));
    assert_eq!(results[0]["title"], json!("Dune"));

    harness.shutdown().await;
}

/// 3a-6's acceptance: 250 items read in three pages, with no overlap and no
/// gap, by browsing and by searching — and the same at an offset past the
/// end.
#[tokio::test]
async fn a_long_library_reads_in_pages_with_nothing_twice_and_nothing_missed() {
    let harness = Harness::start().await;
    // Written in an order that is neither the ids' nor the titles', with the
    // case of the titles alternating: a sort that minded case would put every
    // `Book` before every `book`. Item 0 has no title, and belongs last.
    let id = |n: u16| {
        let mut bytes = [0; 32];
        bytes[..2].copy_from_slice(&n.wrapping_mul(7919).to_be_bytes());
        bytes[31] = 1;
        ItemId::from_bytes(bytes)
    };
    for n in (0..250_u16).rev() {
        let item = Item {
            title: (n > 0).then(|| format!("{} {n:03}", ["book", "Book"][usize::from(n % 2)])),
            ..Item::new(id(n))
        };
        harness
            .store
            .upsert_item(StoredItem {
                item: item.clone(),
                last_modified: 1,
            })
            .await
            .unwrap();
        harness.search.index_item(item).await.unwrap();
    }
    harness.search.commit().await.unwrap();

    // Three pages of a hundred, then one far past the end: the ids read, in
    // order, and the total each page reported.
    let read_all = async |method: &str, params: Value| {
        let page = async |offset: u64| {
            let mut params = params.clone();
            params["offset"] = json!(offset);
            params["limit"] = json!(100);
            harness.call(method, params).await
        };
        let mut ids = Vec::new();
        let mut totals = Vec::new();
        for offset in [0, 100, 200] {
            let page = page(offset).await;
            ids.extend(
                page["results"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|hit| hit["item_id"].clone()),
            );
            totals.push(page["total"].clone());
        }
        // As far as there is: tantivy sizes its heap by `offset + limit`,
        // which is then an overflow, not merely an allocation Linux's
        // overcommit would quietly grant.
        let past_the_end = page(u64::MAX).await;
        assert_eq!(past_the_end["results"], json!([]), "{method}");
        (ids, totals)
    };

    let (listed, totals) = read_all("library.list", json!({})).await;
    assert_eq!(
        totals,
        vec![json!(250); 3],
        "every page says how many there are"
    );
    let in_title_order: Vec<Value> = (1..250).chain([0]).map(|n| json!(id(n))).collect();
    assert_eq!(
        listed, in_title_order,
        "by title, ignoring case, untitled last"
    );

    let (found, totals) = read_all("library.search", json!({ "query": "book" })).await;
    assert_eq!(totals, vec![json!(249); 3], "every titled item matches");
    let distinct: std::collections::BTreeSet<String> = found.iter().map(Value::to_string).collect();
    assert_eq!(found.len(), 249, "nothing missed");
    assert_eq!(distinct.len(), 249, "nothing twice");

    harness.shutdown().await;
}

/// A malformed query is the caller's mistake (`-32602`), not this method
/// failing (`-32000`) — the same distinction `an_unbalanced_query_is_refused`
/// pins one layer down, in `distlib-store` itself.
#[tokio::test]
async fn library_search_refuses_a_malformed_query() {
    let harness = Harness::start().await;
    let refused = harness
        .refuse("library.search", json!({ "query": "\"unterminated" }))
        .await;
    assert_eq!(code(&refused), -32602);

    harness.shutdown().await;
}

#[tokio::test]
async fn a_ticket_carries_directions_to_this_group() {
    // §4.3 step 4. Not a credential — anyone who can call the API can ask for
    // one, and holding it grants nothing until a `MemberAdded` is committed.
    let harness = Harness::start().await;

    let answer = harness.call("group.ticket", Value::Null).await;
    let ticket: Ticket = answer["ticket"].as_str().unwrap().parse().unwrap();

    assert_eq!(
        ticket.group,
        harness.node.membership().group_id().unwrap(),
        "the ticket names the group it came from"
    );
    assert!(
        ticket
            .core
            .iter()
            .any(|(member, addr)| *member == harness.node.id() && !addr.direct.is_empty()),
        "a joiner has to be able to reach a core node: {:?}",
        ticket.core
    );

    harness.shutdown().await;
}

/// D5: a browser's door into `library.add`. A file larger than axum's default
/// two-megabyte body limit is taken — the body is streamed, so that limit is
/// not the one in force — and becomes an item under the name it was uploaded
/// with, after which nothing of the upload is left.
#[tokio::test]
async fn an_uploaded_file_becomes_an_item_and_is_not_kept() {
    let harness = Harness::start().await;

    let (status, uploaded) = harness
        .upload(
            "filename=V%C3%A1lka%20s%20mloky.epub",
            vec![7_u8; 3_000_000],
            false,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{uploaded}");
    assert_eq!(uploaded["filename"], "Válka s mloky.epub");
    assert_eq!(uploaded["size"], 3_000_000);
    assert_eq!(harness.held(), [uploaded["upload"].as_str().unwrap()]);

    let added = harness
        .call(
            "library.add",
            json!({ "kind": "ebook", "uploads": [uploaded["upload"]], "title": "Válka s mloky" }),
        )
        .await;
    assert_eq!(added["created"], true, "{added}");
    assert!(harness.held().is_empty(), "taken, so removed");

    let item: ItemId = serde_json::from_value(added["item_id"].clone()).unwrap();
    let stored = harness.catalogue.item(item).await.unwrap().unwrap();
    let file = stored.files.values().next().unwrap();
    assert_eq!(file.filename, "Válka s mloky.epub");
    assert_eq!(file.format, "epub");
    assert_eq!(file.size, 3_000_000);

    harness.shutdown().await;
}

/// `[api] max_upload_bytes` bounds an upload whether its length is declared —
/// refused before a byte is read — or not, and nothing of a refused upload is
/// left behind.
#[tokio::test]
async fn an_upload_is_capped_whether_or_not_it_says_how_long_it_is() {
    let harness = Harness::start().await;
    let at_the_cap = vec![1_u8; usize::try_from(MAX_UPLOAD).unwrap()];
    let over_it = vec![1_u8; usize::try_from(MAX_UPLOAD).unwrap() + 1];

    for chunked in [false, true] {
        let (status, answer) = harness
            .upload("filename=big.mkv", at_the_cap.clone(), chunked)
            .await;
        assert_eq!(status, StatusCode::OK, "chunked: {chunked}: {answer}");
        assert_eq!(answer["size"], MAX_UPLOAD);

        let before = harness.held();
        match harness
            .try_upload("filename=big.mkv", over_it.clone(), chunked)
            .await
        {
            Ok((status, answer)) => {
                assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "chunked: {chunked}");
                assert_eq!(raw_code(&answer), -32602, "{answer}");
            }
            // Refused before the whole body has arrived, the node closes a
            // connection the client is still writing to. Linux lets the
            // client read the 413 first; Windows resets the connection, and
            // the answer goes with it. Either way the upload was refused.
            Err(error) => assert!(cut_off(&error), "chunked: {chunked}: {error:?}"),
        }
        assert_eq!(
            harness.held(),
            before,
            "chunked: {chunked}: nothing left of it"
        );
    }

    harness.shutdown().await;
}

/// An upload's name becomes a file's name in a shared catalogue, so it has to
/// be one plain name with an extension — never a path — and a refusal says
/// why, in JSON like everything else.
#[tokio::test]
async fn an_upload_needs_a_plain_filename() {
    let harness = Harness::start().await;

    for query in [
        "",
        "name=mloky.epub",
        "filename=",
        "filename=..%2F..%2Fescape.epub",
        "filename=a%2Fb.epub",
        "filename=a%5Cb.epub",
        "filename=..",
        "filename=.epub",
        "filename=.hidden.epub",
        "filename=mloky.",
        "filename=%20mloky.epub",
        "filename=mloky.epub%20",
        "filename=mloky",
    ] {
        let (status, answer) = harness.upload(query, b"bytes".to_vec(), false).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query:?}: {answer}");
        assert_eq!(raw_code(&answer), -32602, "{query:?}: {answer}");
    }
    assert!(harness.held().is_empty());

    harness.shutdown().await;
}

/// An upload is taken once, by the `library.add` it is named to — which
/// takes it whether it succeeds or not — and a call naming files by path and
/// by upload at once, or neither, is refused.
#[tokio::test]
async fn library_add_takes_an_upload_once() {
    let harness = Harness::start().await;
    let upload = async |name: &str| {
        let (_, uploaded) = harness
            .upload(&format!("filename={name}"), name.as_bytes().to_vec(), false)
            .await;
        uploaded["upload"].clone()
    };

    let first = upload("first.epub").await;
    harness
        .call(
            "library.add",
            json!({ "kind": "ebook", "uploads": [first] }),
        )
        .await;
    let again = harness
        .refuse(
            "library.add",
            json!({ "kind": "ebook", "uploads": [first] }),
        )
        .await;
    assert!(again.to_string().contains("no such upload"), "{again}");

    let both = upload("both.epub").await;
    let refused = harness
        .refuse(
            "library.add",
            json!({ "kind": "ebook", "uploads": [both], "files": ["/etc/hostname"] }),
        )
        .await;
    assert!(refused.to_string().contains("not both"), "{refused}");
    assert!(
        harness.held().is_empty(),
        "a refused add takes its uploads too"
    );

    let neither = harness
        .refuse("library.add", json!({ "kind": "ebook" }))
        .await;
    assert!(
        neither.to_string().contains("at least one file"),
        "{neither}"
    );

    let malformed = harness
        .refuse(
            "library.add",
            json!({ "kind": "ebook", "uploads": ["../../etc"] }),
        )
        .await;
    assert!(
        malformed.to_string().contains("not an upload id"),
        "{malformed}"
    );

    harness.shutdown().await;
}
