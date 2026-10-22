//! The community commands through the binary: `rate`, `review`, `bookmark`,
//! `bookmarks` and `wish`, and what `item` shows of them. One node;
//! `catalogue.rs` covers a group.

// Builds the binary and runs a node of it.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

mod common;
use common::process::{Friend, Running, run_command, until_output};

/// Founds a group of one, adds a book to it, and answers with the node and
/// the book's item id.
fn a_node_with_a_book(alice: &Friend) -> (Running, String) {
    alice.agree_on(&[(alice.id.clone(), alice.port)]);
    let mut node = alice.run(true);
    node.wait_for("members=1");
    alice.pledge();

    let book = alice.dir.path().join("hordubal.epub");
    std::fs::write(&book, "Hordubal, in full\n").unwrap();
    let added = run_command(
        alice.dir.path(),
        &[
            "add",
            book.to_str().unwrap(),
            "--kind",
            "ebook",
            "--title",
            "Hordubal",
        ],
    );
    // `added       {item}  {title}`
    let item = added.split_whitespace().nth(1).unwrap().to_owned();
    (node, item)
}

#[test]
fn a_member_rates_and_reviews_an_item_then_clears_both() {
    let alice = Friend::introduce();
    let (node, item) = a_node_with_a_book(&alice);
    let at = alice.dir.path();

    run_command(at, &["rate", &item, "4"]);
    run_command(at, &["review", &item, "Capek at his quietest."]);
    until_output(at, &["item", &item], "the rating and review", |out| {
        out.contains("rating      4.0 from 1 (yours 4)")
            && out.contains("reviews     1")
            && out.contains("Capek at his quietest.")
    });

    run_command(at, &["rate", &item, "--clear"]);
    run_command(at, &["review", &item, "--clear"]);
    until_output(at, &["item", &item], "both cleared", |out| {
        out.contains("rating      none yet") && out.contains("reviews     0")
    });

    node.stop();
}

#[test]
fn a_member_bookmarks_an_item_twice_finds_one_edits_it_and_deletes_the_other() {
    let alice = Friend::introduce();
    let (node, item) = a_node_with_a_book(&alice);
    let at = alice.dir.path();

    // `bookmarked {item} as {bookmark}`
    let bookmark = |position: &str, note: &str| {
        let made = run_command(at, &["bookmark", &item, position, "--note", note]);
        made.split_whitespace().last().unwrap().to_owned()
    };
    let first = bookmark("p. 12", "the farm");
    let second = bookmark("p. 40", "the ending");
    until_output(at, &["item", &item], "both bookmarks", |out| {
        out.contains("bookmarks   2")
    });

    let found = run_command(at, &["bookmarks", "farm", "--mine"]);
    assert!(
        found.contains(&first) && !found.contains(&second),
        "{found}"
    );
    assert!(found.contains("Hordubal"), "{found}");

    run_command(at, &["bookmark", &item, "p. 13", "--edit", &first]);
    run_command(at, &["bookmark", &item, "--delete", &second]);
    until_output(at, &["bookmarks"], "one bookmark, edited", |out| {
        out.contains("at p. 13") && !out.contains("at p. 12") && !out.contains(&second)
    });

    node.stop();
}

#[test]
fn a_member_wishes_comments_and_fulfils_the_wish_with_an_item() {
    let alice = Friend::introduce();
    let (node, item) = a_node_with_a_book(&alice);
    let at = alice.dir.path();

    // `wished      {wish}  {title}`
    let made = run_command(
        at,
        &["wish", "create", "Hordubal", "--author", "Karel Capek"],
    );
    let wish = made.split_whitespace().nth(1).unwrap().to_owned();
    until_output(at, &["wish", "list", "--open"], "the wish, open", |out| {
        out.contains(&format!("{wish}  Hordubal  [open]"))
    });

    run_command(at, &["wish", "comment", &wish, "On paper, please"]);
    run_command(at, &["wish", "fulfill", &wish, &item]);
    let fulfilled = until_output(
        at,
        &["wish", "list", "--fulfilled"],
        "the wish, fulfilled",
        |out| out.contains(&format!("with {item}")) && out.contains("On paper, please"),
    );
    assert!(fulfilled.contains("[fulfilled]"), "{fulfilled}");
    assert!(fulfilled.contains("Karel Capek"), "{fulfilled}");

    node.stop();
}
