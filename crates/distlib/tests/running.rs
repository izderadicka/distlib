//! What `distlib run` does at its edges: when it is in no group, when its api
//! is turned off, when founding cannot go ahead, and when the group expels it.
//! Through the binary, because each of these is what an operator sees.

// Builds the binary and runs it, so it costs seconds. Skipped by
// `--no-default-features`; see the `slow-tests` feature in Cargo.toml.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::net::TcpStream;

mod common;

use common::process::{Friend, Running, run_command};

/// A group of one, founded and running.
fn a_founder() -> (Friend, Running) {
    let founder = Friend::introduce();
    founder.agree_on(&[(founder.id.clone(), founder.port)]);
    let mut node = founder.run(true);
    node.wait_for("members=1");
    (founder, node)
}

#[test]
fn an_expelled_node_stops_and_says_why() {
    let (founder, _founder_node) = a_founder();
    let member = Friend::introduce();
    founder.admit(&member.id);
    member.join(&founder.ticket());
    let mut node = member.run(false);
    node.wait_for("members=2");

    run_command(
        founder.dir.path(),
        &["expel", &member.id, "--reason", "a test"],
    );
    let status = node
        .wait_until_gone()
        .expect("an expelled node stops by itself");

    let log = node.log_contents();
    assert!(
        !status.success(),
        "being expelled is a failure to its operator; log:\n{log}"
    );
    assert!(
        log.contains(
            "this node has been expelled from its group and is no longer a member; its data \
             directory can be deleted"
        ),
        "it says what happened and what to do, in one sentence; log:\n{log}"
    );
}

#[test]
fn a_node_with_its_api_turned_off_serves_none() {
    let friend = Friend::introduce();
    friend.agree_on(&[(friend.id.clone(), friend.port)]);
    let config = friend.dir.path().join("config.toml");
    let turned_off = std::fs::read_to_string(&config)
        .unwrap()
        .replace("[api]\n", "[api]\nenabled = false\n");
    std::fs::write(&config, turned_off).unwrap();

    let mut node = friend.run(true);
    node.wait_for("members=1");

    assert!(node.log_contents().contains("the local api is disabled"));
    assert!(
        TcpStream::connect(("127.0.0.1", friend.api_port)).is_err(),
        "nothing listens on the api's port"
    );
}

#[test]
fn a_node_in_no_group_says_how_to_get_into_one_and_keeps_running() {
    let friend = Friend::introduce();
    friend.agree_on(&[(friend.id.clone(), friend.port)]);

    let mut node = friend.run(false);
    node.wait_for("this node is in no group");

    assert!(
        node.log_contents().contains("distlib run --found-group"),
        "the warning names the way in"
    );
    assert_eq!(node.exited(), None, "it waits to be admitted");
}

#[test]
fn founding_a_second_time_is_refused_and_the_node_shut_down() {
    let (founder, node) = a_founder();
    node.stop();

    let mut again = founder.run(true);
    let status = again
        .wait_until_gone()
        .expect("a refused founding stops the node");

    let log = again.log_contents();
    assert!(!status.success(), "log:\n{log}");
    assert!(
        log.contains("already in a group; founding again would abandon it"),
        "it says why; log:\n{log}"
    );
    assert!(
        !log.contains("Endpoint dropped without calling"),
        "the node is shut down, not dropped; log:\n{log}"
    );
}
