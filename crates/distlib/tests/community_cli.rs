//! The community commands through the binary: `rate`, `review`, and what
//! `item` shows of them. One node; `catalogue.rs` covers a group.

// Builds the binary and runs a node of it.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

mod common;
use common::process::{Friend, run_command, until_output};

#[test]
fn a_member_rates_and_reviews_an_item_then_clears_both() {
    let alice = Friend::introduce();
    alice.agree_on(&[(alice.id.clone(), alice.port)]);
    let mut node = alice.run(true);
    node.wait_for("members=1");
    let at = alice.dir.path();

    let book = at.join("hordubal.epub");
    std::fs::write(&book, "Hordubal, in full\n").unwrap();
    let added = run_command(
        at,
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
