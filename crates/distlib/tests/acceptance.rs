//! §9's Phase 2 acceptance, through the actual commands.
//!
//! > *Node A adds 3 ebooks; node B (fresh join) syncs the catalogue, searches
//! > by author, downloads a file, and after restart still serves it.*
//!
//! One sentence, and every clause of it is a step an operator performs. That
//! is why this drives the binary rather than the library, on `founding.rs`'s
//! reasoning: the procedure is the thing being checked. A library test
//! assembles a `Runtime` and calls `Api::call`, which cannot get the
//! *sequence* wrong — cannot forget that a joiner needs a ticket, that the
//! ticket needs an admission first, that `search` is answered by a node that
//! has to be running. This can, and therefore can catch it.
//!
//! **What the in-process version already pins**, so that this one is not
//! written twice: `download.rs`'s
//! `a_node_that_downloads_a_file_serves_it_after_a_restart` is the same
//! serving claim with the joining procedure left out, where the fetch can be
//! watched rather than inferred from a printed line. What is new here is
//! everything around it — the ticket, the join, the CLI's own output being
//! the only thing the test is allowed to read.
//!
//! **"Still serves it" needs somebody to serve it to.** A restarted node
//! re-downloading its own file proves nothing: it already holds the bytes and
//! never reaches the network. So there is a third member, and the node that
//! added the books is **stopped** before she asks — with node A gone, bytes
//! that arrive have nowhere else in the group to have come from.

// Builds the binary and runs three of it, with blob transfers and a restart.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{
    path::Path,
    time::{Duration, Instant},
};

mod common;
use common::process::{CONVERGE_TIMEOUT, Friend, distlib, wait_for_all};

/// The three ebooks node A adds, as §9 asks for. One author across all three,
/// because the criterion's next clause is searching by it.
const LIBRARY: [(&str, &str); 3] = [
    ("dune.epub", "Dune"),
    ("dune-messiah.epub", "Dune Messiah"),
    ("children-of-dune.epub", "Children of Dune"),
];

/// Runs a command that is expected to succeed, and answers with its stdout.
fn run(at: &Path, args: &[&str]) -> String {
    let output = distlib(at).args(args).output().unwrap();
    assert!(
        output.status.success(),
        "`distlib {}` failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

/// Polls a command until its output satisfies `settled`.
///
/// The catalogue is eventually consistent and every read here is of a
/// *projection* of it, so a single call is asserting the absence of a
/// replication delay rather than the thing the test is about — the same
/// reasoning `Friend::wait_for_status` gives for polling a promotion.
fn until(at: &Path, args: &[&str], what: &str, settled: impl Fn(&str) -> bool) -> String {
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let out = run(at, args);
        if settled(&out) {
            return out;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}; `distlib {}` last said:\n{out}",
            args.join(" ")
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// The `(item id, title)` of every hit `distlib search` printed.
///
/// **Parsed as hits rather than searched for as text**, which is not
/// fussiness: `distlib search` answers a query that found nothing with `no
/// matches for "Dune"` — a line containing the very word a `contains` check
/// would be looking for. A wait written that way returns immediately and the
/// step after it fails instead, somewhere less informative. A hit is a line
/// starting with a 64-character id, and nothing else here is.
fn hits(listing: &str) -> impl Iterator<Item = (&str, &str)> {
    listing.lines().filter_map(|line| {
        let (id, title) = line.split_once("  ")?;
        (id.len() == 64 && id.chars().all(|c| c.is_ascii_hexdigit())).then(|| (id, title.trim()))
    })
}

/// The item id `distlib search` printed for `title`.
///
/// Read out of the CLI's own output rather than computed here. Recomputing
/// the fingerprint would test this file's arithmetic against `distlib-core`'s
/// and say nothing about whether the two nodes are looking at one item.
fn id_of(listing: &str, title: &str) -> String {
    hits(listing)
        .find(|(_, hit)| *hit == title)
        .unwrap_or_else(|| panic!("no hit titled {title:?} in:\n{listing}"))
        .0
        .to_owned()
}

#[test]
fn a_fresh_member_syncs_searches_downloads_and_still_serves_after_a_restart() {
    let alice = Friend::introduce();
    let bob = Friend::introduce();
    let carol = Friend::introduce();

    // Alice founds alone, so that stopping her later is the group losing its
    // only core node rather than losing quorum as well — and so that bob and
    // carol arrive the way §9 says, as fresh joins rather than as founders.
    alice.agree_on(&[(alice.id.clone(), alice.port)]);
    let mut alice_node = alice.run(true);
    alice_node.wait_for("members=1");

    // The admission comes before the ticket, and the ticket before the join:
    // a ticket says where the group is, not who may join it. Getting this
    // order wrong is the kind of thing only a test of the procedure notices.
    alice.admit(&bob.id);
    alice.admit(&carol.id);
    let ticket = alice.ticket();
    bob.join(&ticket);
    carol.join(&ticket);

    // **Carol starts before bob, and the order is load bearing — but it is
    // not sufficient, which is why this test does not pass yet.**
    //
    // Carol has to reach bob at the end of this run, and today the only way
    // she comes to hold his address is by already being in the gossip swarm
    // when he announces himself. Established by hand, not by argument: with
    // bob started first, bob learns carol — she announces and he is
    // listening — and carol never learns bob, because his announcement was
    // made before she arrived and nothing repeats it for a newcomer. It does
    // not heal: sixty seconds of retries, no change. Both mechanisms that
    // should cover it are recorded open in the phase doc's carried-out
    // table — a follower asks the core group for the directory *once* at
    // startup and latches on whatever came back, and nothing asks again when
    // a member cannot be resolved. A restarted core node makes it worse
    // still: its directory comes back empty, so the one ask returns nothing.
    //
    // In this order the whole run passes **by hand**. It does not pass here,
    // because `members=3` says carol has fetched the log, not that she has
    // joined the gossip topic — so bob can still announce into a swarm she
    // has not reached. Waiting for the thing that actually matters needs
    // something that can observe "carol holds an address for bob", and no
    // command reports that.
    //
    // So the ordering is a way around a real limitation rather than a
    // property of the design, and it is left visible instead of being tuned
    // until it passes. See delta P2-25.
    let mut carol_node = carol.run(false);
    carol_node.wait_for("members=3");
    let mut bob_node = bob.run(false);
    wait_for_all(
        &mut [&mut alice_node, &mut bob_node, &mut carol_node],
        "members=3",
    );

    // *Node A adds 3 ebooks.*
    let books = alice.dir.path().join("books");
    std::fs::create_dir_all(&books).unwrap();
    for (filename, title) in LIBRARY {
        let path = books.join(filename);
        // Big enough that the blob is a file in the store rather than a row
        // inlined in its database — which is the shape `download.rs` found
        // matters, and the shape a real book has.
        std::fs::write(&path, format!("{title}, in full\n").repeat(20_000)).unwrap();
        let added = run(
            alice.dir.path(),
            &[
                "add",
                path.to_str().unwrap(),
                "--kind",
                "ebook",
                "--title",
                title,
                "--author",
                "Frank Herbert",
            ],
        );
        assert!(added.starts_with("added"), "{added}");
    }

    // *Node B (fresh join) syncs the catalogue, searches by author.*
    //
    // By author, as the criterion says, and not by title — a title match
    // would be satisfied by the one field the projection writes first, which
    // is exactly the confusion that made `read_model.rs`'s own test flaky.
    let found = until(
        bob.dir.path(),
        &["search", "authors:herbert"],
        "bob to sync all three books and find them by author",
        |out| {
            LIBRARY
                .iter()
                .all(|(_, want)| hits(out).any(|(_, title)| title == *want))
        },
    );
    let dune = id_of(&found, "Dune");

    // *Downloads a file.*
    let bobs_books = bob.dir.path().join("downloads");
    std::fs::create_dir_all(&bobs_books).unwrap();
    let downloaded = run(
        bob.dir.path(),
        &["download", &dune, "--dest", bobs_books.to_str().unwrap()],
    );
    assert!(downloaded.contains("fetched"), "{downloaded}");
    let bobs_copy = bobs_books.join("dune.epub");
    assert_eq!(
        std::fs::read(&bobs_copy).unwrap(),
        std::fs::read(books.join("dune.epub")).unwrap(),
        "what bob downloaded has to be what alice added"
    );

    // Bob reads the book and tidies up. Load bearing: with the exported file
    // still on disk, bob would go on serving the blob even if `export` had
    // moved it out of his store rather than copied it — `download.rs`'s own
    // mutation check found that, and it is no less true here.
    std::fs::remove_file(&bobs_copy).unwrap();

    // *And after restart still serves it.*
    bob_node.stop();
    let mut bob_node = bob.run(false);
    bob_node.wait_for("members=3");

    // Carol syncs the catalogue while the node that has the books is still
    // up: an item she had never heard of would make her download a test of
    // the catalogue rather than of who served the bytes.
    until(
        carol.dir.path(),
        &["search", "authors:herbert"],
        "carol to sync the catalogue",
        |out| hits(out).any(|(_, title)| title == "Dune"),
    );

    // The only other copy in the group leaves. Whatever carol gets now came
    // out of the store bob restarted onto.
    alice_node.stop();

    let carols_books = carol.dir.path().join("downloads");
    std::fs::create_dir_all(&carols_books).unwrap();
    let carols_copy = carols_books.join("dune.epub");
    // Retried rather than asserted first time: carol may reach for bob before
    // her endpoint has noticed the old connection died, and a fetch that
    // finds nobody fails rather than waiting. What is pinned is that it comes
    // through, not how soon.
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let attempt = distlib(carol.dir.path())
            .args(["download", &dune, "--dest", carols_books.to_str().unwrap()])
            .output()
            .unwrap();
        if attempt.status.success() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "carol never got the file from the restarted bob, with alice gone: {}\n\n--- BOB\n{}\n--- CAROL\n{}",
            String::from_utf8_lossy(&attempt.stderr),
            bob_node.log_contents(),
            carol_node.log_contents()
        );
        std::thread::sleep(Duration::from_millis(500));
    }
    assert_eq!(
        std::fs::read(&carols_copy).unwrap(),
        std::fs::read(books.join("dune.epub")).unwrap(),
        "bob served what bob downloaded"
    );

    carol_node.stop();
    bob_node.stop();
}

/// Kept separate from the run above, because it is a claim about `distlib
/// download`'s own surface rather than about §9's procedure, and folding it
/// in would mean a failure in either one being reported as the other.
///
/// Both refusals are the ones an operator meets first: a second download over
/// a file that is already there, and an item this node has never heard of.
#[test]
fn download_refuses_what_it_cannot_do_without_losing_anything() {
    let alice = Friend::introduce();
    alice.agree_on(&[(alice.id.clone(), alice.port)]);
    let mut alice_node = alice.run(true);
    alice_node.wait_for("members=1");

    let book = alice.dir.path().join("dune.epub");
    std::fs::write(&book, b"Dune, in full").unwrap();
    run(
        alice.dir.path(),
        &[
            "add",
            book.to_str().unwrap(),
            "--kind",
            "ebook",
            "--title",
            "Dune",
        ],
    );
    let found = until(
        alice.dir.path(),
        &["search", "Dune"],
        "the book alice just added to be searchable",
        |out| hits(out).any(|(_, title)| title == "Dune"),
    );
    let dune = id_of(&found, "Dune");

    let dest = alice.dir.path().join("downloads");
    std::fs::create_dir_all(&dest).unwrap();
    let first = run(
        alice.dir.path(),
        &["download", &dune, "--dest", dest.to_str().unwrap()],
    );
    // Alice is the one who added it, so there is nobody to ask and nothing to
    // ask for — this is the local-store path, and it has to work in a group
    // of one where a fetch could not.
    assert!(first.contains("had it"), "{first}");

    // An edit the operator made to their own copy, which a second download
    // must not silently undo.
    let downloaded = dest.join("dune.epub");
    std::fs::write(&downloaded, b"my own notes in the margin").unwrap();
    let again = distlib(alice.dir.path())
        .args(["download", &dune, "--dest", dest.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!again.status.success());
    let complaint = String::from_utf8_lossy(&again.stderr);
    assert!(complaint.contains("already exists"), "{complaint}");
    assert_eq!(
        std::fs::read(&downloaded).unwrap(),
        b"my own notes in the margin",
        "the refusal has to leave the file alone"
    );

    let unknown = distlib(alice.dir.path())
        .args([
            "download",
            &"ab".repeat(32),
            "--dest",
            dest.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!unknown.status.success());
    assert!(
        String::from_utf8_lossy(&unknown.stderr).contains("no such item"),
        "{}",
        String::from_utf8_lossy(&unknown.stderr)
    );

    alice_node.stop();
}
