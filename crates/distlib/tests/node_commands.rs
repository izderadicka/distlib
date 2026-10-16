//! `init`, `status` and `ping` through the binary, as an operator runs them.

// Runs the binary, and for `status` and `ping` a node, so it costs seconds.
// Skipped by `--no-default-features`; see the `slow-tests` feature in
// Cargo.toml.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::path::Path;

use tempfile::TempDir;

mod common;

use common::process::{Friend, run_command};

/// The member id `init` printed, from its `identity` line.
fn identity_in(printed: &str) -> String {
    printed
        .lines()
        .find_map(|line| line.strip_prefix("identity    "))
        .and_then(|rest| rest.split_whitespace().next())
        .unwrap_or_else(|| panic!("no identity line in:\n{printed}"))
        .to_owned()
}

fn config_of(dir: &Path) -> String {
    std::fs::read_to_string(dir.join("config.toml")).unwrap()
}

#[test]
fn init_creates_an_identity_and_a_starter_config() {
    let dir = TempDir::new().unwrap();

    let printed = run_command(dir.path(), &["init"]);

    assert!(
        printed.contains("wrote"),
        "says it wrote the config:\n{printed}"
    );
    assert!(
        printed.contains("(new)"),
        "says the identity is new:\n{printed}"
    );
    assert_eq!(identity_in(&printed).len(), 64, "a member id:\n{printed}");
    assert!(
        config_of(dir.path()).contains("[net]"),
        "the starter config is written"
    );
}

#[test]
fn init_again_keeps_the_identity_and_the_config_as_they_are() {
    let dir = TempDir::new().unwrap();
    let first = identity_in(&run_command(dir.path(), &["init"]));
    // The operator's own edit, which a second `init` must not undo.
    let edited = format!("{}\n# mine\n", config_of(dir.path()));
    std::fs::write(dir.path().join("config.toml"), &edited).unwrap();

    let printed = run_command(dir.path(), &["init"]);

    assert_eq!(identity_in(&printed), first);
    assert!(printed.contains("(existing)"), "{printed}");
    assert!(!printed.contains("wrote"), "{printed}");
    assert_eq!(config_of(dir.path()), edited);
}

#[test]
fn init_with_force_replaces_the_identity_and_says_so() {
    let dir = TempDir::new().unwrap();
    let first = identity_in(&run_command(dir.path(), &["init"]));

    let printed = run_command(dir.path(), &["init", "--force"]);

    assert_ne!(identity_in(&printed), first, "a new identity");
    assert!(printed.contains("(new)"), "{printed}");
    assert!(
        printed.contains("replacing the existing identity; this node leaves any group it was in"),
        "the consequence is said plainly:\n{printed}"
    );
}

#[test]
fn status_without_an_identity_says_how_to_make_one() {
    let dir = TempDir::new().unwrap();

    let printed = run_command(dir.path(), &["status"]);

    assert!(
        printed.contains("identity    not initialised — run `distlib init`"),
        "{printed}"
    );
}

#[test]
fn status_of_a_node_in_no_group_that_is_not_running() {
    let dir = TempDir::new().unwrap();
    let me = identity_in(&run_command(dir.path(), &["init"]));

    let printed = run_command(dir.path(), &["status"]);

    assert!(printed.contains(&format!("identity    {me}")), "{printed}");
    assert!(printed.contains("group       none yet"), "{printed}");
    assert!(printed.contains("node        not running"), "{printed}");
}

#[test]
fn status_of_a_stopped_founder_reads_its_role_from_the_database() {
    let founder = Friend::introduce();
    founder.agree_on(&[(founder.id.clone(), founder.port)]);
    let mut node = founder.run(true);
    node.wait_for("members=1");
    node.stop();

    let printed = run_command(founder.dir.path(), &["status"]);

    assert!(printed.contains("role        core member"), "{printed}");
    assert!(printed.contains("node        not running"), "{printed}");
}

/// 5a-3: a founder's `[group]` is what its group is founded with, and
/// `distlib status` shows it whether the node is running or not. A config
/// that says otherwise later is warned about, not obeyed: the log's settings
/// are fixed.
#[test]
fn a_group_founded_from_the_config_keeps_its_settings_when_the_config_changes() {
    let founder = Friend::introduce();
    founder.agree_on(&[(founder.id.clone(), founder.port)]);
    let config = founder.dir.path().join("config.toml");
    let agreed = std::fs::read_to_string(&config).unwrap();
    std::fs::write(&config, format!("{agreed}\n[group]\nreplicas = 2\n")).unwrap();

    let mut node = founder.run(true);
    node.wait_for("members=1");
    founder.wait_for_status("the node to answer", |status| {
        status.contains("node        running") && status.contains("replicas = 2")
    });
    node.stop();
    let printed = run_command(founder.dir.path(), &["status"]);
    assert!(printed.contains("node        not running"), "{printed}");
    assert!(printed.contains("replicas = 2"), "{printed}");

    std::fs::write(&config, format!("{agreed}\n[group]\nreplicas = 4\n")).unwrap();
    let mut node = founder.run(false);
    node.wait_for("differs from the settings this group was founded with");
    let printed = founder.wait_for_status("the node to answer", |status| {
        status.contains("node        running")
    });
    assert!(printed.contains("replicas = 2"), "{printed}");
    node.stop();
}

#[test]
fn ping_reaches_a_running_member_and_prints_the_echo() {
    let founder = Friend::introduce();
    founder.agree_on(&[(founder.id.clone(), founder.port)]);
    let mut node = founder.run(true);
    node.wait_for("members=1");
    // A running node answers only those it knows; admitting the pinger is
    // what lets it through.
    let pinger = Friend::introduce();
    founder.admit(&pinger.id);
    node.wait_for("members=2");

    let printed = run_command(
        pinger.dir.path(),
        &[
            "ping",
            &founder.id,
            "--addr",
            &format!("127.0.0.1:{}", founder.port),
            "--payload",
            "are you there",
            "--timeout",
            "10",
        ],
    );

    assert_eq!(printed.trim(), "are you there");
}
