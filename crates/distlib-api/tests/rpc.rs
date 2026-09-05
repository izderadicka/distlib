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
};

use distlib_api::{Api, Server, serve};
use distlib_consensus::{MemberRecord, MembershipNode};
use distlib_core::{MemberId, NodeAddr, Ticket};
use distlib_net::{AllowlistHooks, allowlist, endpoint::configure};
use http_body_util::{BodyExt as _, Full};
use hyper::{Request, StatusCode, body::Bytes, header::AUTHORIZATION};
use hyper_util::{client::legacy::Client as Hyper, rt::TokioExecutor};
use iroh::{
    Endpoint, SecretKey,
    endpoint::{RelayMode, presets},
};
use secrecy::SecretString;
use serde_json::{Value, json};
use tempfile::TempDir;

/// A founded one-node group with its API up.
struct Harness {
    node: Arc<MembershipNode>,
    server: Server,
    token: String,
    _dir: TempDir,
}

impl Harness {
    /// Founds a group and serves the API on a port the OS picks.
    async fn start() -> Self {
        let secret = SecretKey::generate();
        let id = MemberId::from(secret.public());
        let dir = TempDir::new().unwrap();

        let (writer, reader) = allowlist(id, []);
        let hooks = AllowlistHooks::new(reader);
        let endpoint = configure(
            Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled),
            secret.clone(),
            hooks.clone(),
            distlib_consensus::alpns(true),
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
        let node = Arc::new(
            MembershipNode::start(
                endpoint,
                hooks,
                writer,
                dir.path(),
                vec![(id, NodeAddr::default())],
            )
            .await
            .unwrap(),
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
        let server = serve(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            Api {
                node: Arc::clone(&node),
                secret,
                net: distlib_core::NetConfig::default(),
            },
            SecretString::from(token.clone()),
        )
        .await
        .unwrap();
        Self {
            node,
            server,
            token,
            _dir: dir,
        }
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

        // A 401 carries plain text, not a JSON-RPC envelope.
        let answer = serde_json::from_slice(&body)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&body).into_owned()));
        (status, answer)
    }

    async fn shutdown(self) {
        self.server.shutdown();
        self.node.shutdown().await;
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

    let (anonymous, _) = harness.post(None, rpc("node.status", Value::Null)).await;
    assert_eq!(anonymous, StatusCode::UNAUTHORIZED, "no token at all");

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
async fn node_status_reports_the_group_this_node_founded() {
    let harness = Harness::start().await;
    let status = harness.call("node.status", Value::Null).await;

    assert_eq!(status["member"], json!(harness.node.id()));
    assert!(!status["group"].is_null(), "the group was founded");
    assert_eq!(status["core"], json!(true), "a founder is a voter");
    assert_eq!(status["members"], json!(1));
    assert_eq!(status["raft"], json!("Leader"));

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
