//! Three friends found a group, through the actual commands.
//!
//! Every other test in this workspace drives the library. This one drives the
//! binary, because the thing being checked is the *procedure*: nobody can found
//! a group without first collecting ids and addresses from the others, and that
//! exchange happens outside the program. A library test cannot get it wrong, so
//! it cannot catch it being wrong either.

// Builds the binary and runs three of it, so it costs seconds and varies with
// the machine. Skipped by `--no-default-features`; see the `slow-tests` feature
// in Cargo.toml.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::process::Stdio;

use tempfile::TempDir;

mod common;
use common::process::{Friend, Protocol, a_free_port, distlib, wait_for_all, wait_for_exit};

#[test]
fn run_refuses_a_data_directory_with_no_identity() {
    // The failure this prevents: `distlib run` pointed at the wrong directory
    // used to mint a fresh key and start a node that was nobody, with the only
    // symptom a member id nobody recognised and a port nobody was told about.
    let dir = TempDir::new().unwrap();

    let child = distlib(dir.path())
        .arg("run")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let output = wait_for_exit(child, "`distlib run` on an empty data directory");

    assert!(
        !output.status.success(),
        "an empty data directory must fail"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no identity"),
        "the error must name the problem, got:\n{stderr}"
    );
    assert!(
        stderr.contains("distlib init"),
        "the error must name the way out, got:\n{stderr}"
    );
    assert!(
        !dir.path().join("keys").join("node.key").exists(),
        "refusing must leave no identity behind"
    );
}

#[test]
fn three_friends_found_a_group() {
    // 1. Each of them runs `whoami` and sends the founder the line it prints.
    let friends: Vec<Friend> = (0..3).map(|_| Friend::introduce()).collect();
    let everyone: Vec<(String, u16)> = friends
        .iter()
        .map(|friend| (friend.id.clone(), friend.port))
        .collect();

    // 2. The founder assembles the core group and sends it back, so all three
    //    configs are identical. A founder who is not in their own core list is
    //    refused, and a member missing from someone else's cannot be reached.
    for friend in &friends {
        friend.agree_on(&everyone);
    }

    // 3. The other two start first. With three voters the founder needs one of
    //    them to grant its vote before it can commit anything.
    let mut second = friends[1].run(false);
    let mut third = friends[2].run(false);
    let mut first = friends[0].run(true);

    // 4. All three converge on one group, and only the founder was told to
    //    found it — the others learned everything by replication.
    let converged = "members=3 core=3";
    wait_for_all(&mut [&mut first, &mut second, &mut third], converged);
    assert!(
        !second.log_contents().contains("founding the group"),
        "only the founder founds"
    );

    let group = group_id(&first.log_contents());
    for node in [&second, &third] {
        assert_eq!(
            group_id(&node.log_contents()),
            group,
            "one group, not three separate ones"
        );
    }

    // 5. A fourth member is admitted from the command line, against a running
    //    node. This is the part that cannot be done any other way: the node
    //    holds its database exclusively, so the CLI has to go through its API.
    let newcomer = Friend::introduce();
    friends[0].admit(&newcomer.id);

    wait_for_all(&mut [&mut first, &mut second, &mut third], "members=4");

    // ...and a node that was told nothing about it lists them, live, while
    //    still running.
    let listed = friends[2].members();
    assert!(
        listed.contains(&newcomer.id),
        "a node that never heard the command should still list the newcomer; got:\n{listed}"
    );

    // 6. The newcomer joins for real: it takes a ticket, writes the group into
    //    its own configuration, and follows the log without ever having been
    //    told what is in it. §4.3 end to end.
    newcomer.join(&friends[1].ticket());
    let mut joined = newcomer.run(false);
    joined.wait_for("members=4");

    let listed = newcomer.members();
    for friend in &friends {
        assert!(
            listed.contains(&friend.id),
            "a joiner must derive the whole group from the log; got:\n{listed}"
        );
    }
    let own_line = listed
        .lines()
        .find(|line| line.contains(&newcomer.id))
        .expect("the joiner lists itself");
    assert!(
        !own_line.contains("core"),
        "a joiner follows rather than votes; got: {own_line}"
    );
    assert!(
        listed.contains("(3 core)"),
        "the three founders still vote; got:\n{listed}"
    );
    joined.stop();

    // 7. Stopped, each of them can be asked who is in the group, and the answer
    //    comes from its own copy of the log rather than from its config.
    for node in [first, second, third] {
        node.stop();
    }
    for friend in &friends {
        let listed = friend.members();
        assert!(listed.contains(&group), "every node names the same group");
        for other in friends.iter().chain([&newcomer]) {
            assert!(
                listed.contains(&other.id),
                "{} should list {}; got:\n{listed}",
                friend.id,
                other.id
            );
        }
    }
}

/// The group id from a `membership` log line.
fn group_id(log: &str) -> String {
    log.split("group=")
        .nth(1)
        .expect("a membership line names the group")
        .split_whitespace()
        .next()
        .expect("the group id is one word")
        .to_owned()
}

#[test]
fn a_core_node_that_moves_is_told_to_the_group_and_comes_back() {
    // P1-23, end to end and through the commands. The failure it closes: a core
    // node in a group with no relay changes IP or port, nobody can reach it
    // again, and — because the only addressing the log ever recorded was the
    // founding one — there was no way to say so. Refounding the group was the
    // whole of the fix.
    let mut friends: Vec<Friend> = (0..3).map(|_| Friend::introduce()).collect();
    let everyone: Vec<(String, u16)> = friends
        .iter()
        .map(|friend| (friend.id.clone(), friend.port))
        .collect();
    for friend in &friends {
        friend.agree_on(&everyone);
    }

    let mut second = friends[1].run(false);
    let mut third = friends[2].run(false);
    let mut first = friends[0].run(true);
    wait_for_all(
        &mut [&mut first, &mut second, &mut third],
        "members=3 core=3",
    );

    // The third friend's machine is renumbered. It stops, comes back on a
    // different port, and the other two are still looking for it on the old
    // one — which under `relay_mode = "disabled"` is the whole of the problem:
    // there is no lookup to fall back on.
    //
    // **Crashed rather than stopped, and that is the scenario rather than a
    // shortcut.** A machine that is renumbered does not first close its
    // connections and tell its peers it is going. If it did, they would drop
    // the path they hold for it and adopt the new one the moment it dialled
    // them again — which the catalogue's own document sync does within
    // milliseconds of startup, healing the address change without anybody
    // running the command this test is named for. Measured at roughly one run
    // in five; see P2-25.
    third.crash();
    let moved_port = a_free_port(Protocol::Udp);
    friends[2].move_to(&everyone, moved_port);
    let mut third = friends[2].run(false);

    // Meanwhile the group carries on without it — two of three is still a
    // majority — so there is something for the moved node to have missed.
    let newcomer = Friend::introduce();
    friends[0].admit(&newcomer.id);
    wait_for_all(&mut [&mut first, &mut second], "members=4");

    // And it *has* missed it. Asserted rather than assumed, because without
    // this the test would go on passing the day a moved node starts finding its
    // own way back — and would then be proving nothing about the command it is
    // named for. A node the group cannot reach cannot be told anything: there
    // is no race here to lose.
    assert!(
        !third.log_contents().contains("members=4"),
        "a moved node should be out of touch until the group is told where it went; its log was:\n{}",
        third.log_contents()
    );

    // One core member says where it went. An address change does not move the
    // voters, so it takes one approval and theirs is it: this applies rather
    // than waiting, which is what makes a moved node's way back quick.
    let said = friends[0].core_set(&friends[2].id, moved_port);
    assert!(
        said.contains("core set"),
        "moving a core node takes one core approval, so it should apply at once; got:\n{said}"
    );

    // And the entry it committed is what reaches the moved node: it can only
    // learn `members=4` by being replicated to, at the address the log now
    // carries.
    wait_for_all(&mut [&mut first, &mut second, &mut third], "members=4");

    for node in [first, second, third] {
        node.stop();
    }
}

#[test]
fn a_follower_promoted_by_the_group_starts_voting_without_a_restart() {
    // 2.3-2, through the commands, and the half of P1-30 that 2.1-2 left open.
    // The claim is the last four words of the name: the node that ends up
    // voting is the same process that started as a follower, and nobody
    // restarted it or edited its configuration.
    let friends: Vec<Friend> = (0..3).map(|_| Friend::introduce()).collect();
    let everyone: Vec<(String, u16)> = friends
        .iter()
        .map(|friend| (friend.id.clone(), friend.port))
        .collect();
    for friend in &friends {
        friend.agree_on(&everyone);
    }

    let mut second = friends[1].run(false);
    let mut third = friends[2].run(false);
    let mut first = friends[0].run(true);
    wait_for_all(
        &mut [&mut first, &mut second, &mut third],
        "members=3 core=3",
    );

    // A fourth member joins the ordinary way: admitted, handed a ticket, and
    // started. It follows the log and votes on nothing.
    let newcomer = Friend::introduce();
    friends[0].admit(&newcomer.id);
    newcomer.join(&friends[1].ticket());
    let mut joined = newcomer.run(false);
    joined.wait_for("members=4");

    let before = newcomer.status();
    assert!(
        before.contains("role        member") && before.contains("follows     the log"),
        "a joiner follows rather than votes; got:\n{before}"
    );

    // Promoting somebody changes who votes, so it takes a majority of the
    // three — one core member to propose it and a second to agree.
    let said = friends[0].core_set(&newcomer.id, newcomer.port);
    assert!(
        said.contains("proposed"),
        "adding a voter is not one member's decision; got:\n{said}"
    );
    friends[1].approve(friends[1].the_pending_one());

    // It reads the same entry the leader does, and sits down.
    joined.wait_for("this node is now a voter");

    // And then it is caught up *as a voter*, which is the part that cannot be
    // faked: it blanked its own projection on the way in, so a fifth member
    // admitted now can only reach it over `distlib/raft/0`. The count is what
    // distinguishes this from the picture it had as a follower — the text of
    // the membership line is otherwise identical either side of the promotion.
    let fifth = Friend::introduce();
    friends[0].admit(&fifth.id);
    wait_for_all(
        &mut [&mut first, &mut second, &mut third, &mut joined],
        "members=5 core=4",
    );

    let after = newcomer.wait_for_status("the promoted node to start voting", |status| {
        status
            .lines()
            .any(|line| matches!(line.trim(), "Raft role   Follower" | "Raft role   Leader"))
    });
    assert!(
        after.contains("role        core member"),
        "the promoted node should know it votes; got:\n{after}"
    );
    // Openraft's own word for it, and the assertion that makes this test about
    // *voting* rather than about being replicated to. A node that had been
    // added as a learner and never promoted would satisfy everything above —
    // it would hold a Raft, be caught up, and read "core member" off the log —
    // and would report `Learner` here. A voter reports `Follower` or `Leader`.
    let role = after
        .lines()
        .find_map(|line| line.strip_prefix("Raft role   "))
        .unwrap_or_else(|| panic!("the promoted node should run a Raft; got:\n{after}"));
    assert!(
        matches!(role.trim(), "Follower" | "Leader"),
        "a promoted node must be a voter, not a learner; got Raft role {role}"
    );

    for node in [first, second, third, joined] {
        node.stop();
    }
}

#[test]
fn a_node_stopped_by_a_service_manager_shuts_down_cleanly() {
    // A node run by hand is stopped with Ctrl-C, and that was the only signal
    // `run` listened for. A node run by systemd, Docker or any other
    // supervisor is stopped with SIGTERM, which without a handler kills the
    // process where it stands — and what that costs here is specific rather
    // than general untidiness: the blob store writes its metadata when the
    // router closes it, so a node that never shut down comes back holding
    // every *downloaded* blob's bytes with no record that it holds them, and
    // fetches the lot again. (Imported blobs survive it; the asymmetry is
    // measured in P2-25.)
    //
    // A group of one is enough. What is in question is whether the signal is
    // answered rather than fatal — what the shutdown then does is the same
    // thing Ctrl-C has always run, and there is no second path to check.
    let friend = Friend::introduce();
    friend.agree_on(&[(friend.id.clone(), friend.port)]);
    let mut node = friend.run(true);
    node.wait_for("members=1");

    node.signal("TERM");
    let status = node
        .wait_until_gone()
        .expect("a node asked to stop should stop");

    // Both halves are needed, and the first one alone would be a test that
    // passes on the behaviour it is meant to close: an unhandled SIGTERM also
    // makes the process go away, rather faster. `success()` is what separates
    // running the shutdown from being killed by the signal.
    let log = node.log_contents();
    assert!(
        status.success(),
        "a node killed by the signal rather than answering it exits {status}; its log was:\n{log}"
    );
    assert!(
        log.contains("SIGTERM"),
        "the log should say which signal stopped it; got:\n{log}"
    );
    assert!(
        log.contains("shutting down"),
        "the node should reach its own shutdown; got:\n{log}"
    );
}
