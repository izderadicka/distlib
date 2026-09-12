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

        let dir = TempDir::new().unwrap();
        let secrets: Vec<SecretKey> = (0..voters).map(|_| SecretKey::generate()).collect();
        let ids: Vec<MemberId> = secrets
            .iter()
            .map(|secret| MemberId::from(secret.public()))
            .collect();

        let mut nodes = Vec::new();
        let mut addrs = Vec::new();
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
                distlib_consensus::alpns(true),
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
            nodes.push(Arc::new(
                MembershipNode::start(
                    endpoint,
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
            ));
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

        let token = "0123456789abcdef".repeat(4);
        let server = serve(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            Api {
                node: Arc::clone(&nodes[0]),
                secret: secrets[0].clone(),
                net: distlib_core::NetConfig::default(),
            },
            SecretString::from(token.clone()),
        )
        .await
        .unwrap();

        let harness = Self {
            node: Arc::clone(&nodes[0]),
            server,
            token,
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
    assert!(
        harness.node.membership().is_member(&doomed),
        "and nothing has happened to them yet"
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

    harness.shutdown().await;
    for node in nodes.into_iter().skip(1) {
        node.shutdown().await;
    }
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

    harness.shutdown().await;
    for node in nodes.into_iter().skip(1) {
        node.shutdown().await;
    }
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
