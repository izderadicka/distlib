//! How a member's address compares and encodes.
//!
//! `NodeAddr` has to implement `Eq` — openraft's `Node` bound requires it, since
//! this is what Raft membership carries — so the question is not whether it
//! compares, but whether comparing means the right thing. These values are
//! persisted in the log and compared across nodes, so equality that depended on
//! the order addresses happened to be listed in would report membership changes
//! where nothing changed.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::net::SocketAddr;

use distlib_core::NodeAddr;

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

#[test]
fn address_order_does_not_affect_equality() {
    let one = NodeAddr::default()
        .with_direct(addr(1))
        .with_direct(addr(2));
    let other = NodeAddr::default()
        .with_direct(addr(2))
        .with_direct(addr(1));

    assert_eq!(
        one, other,
        "the same addresses in a different order are the same node"
    );
}

#[test]
fn address_order_does_not_affect_the_encoding() {
    // Stronger than equality, and the reason it matters: these bytes go into
    // the replicated log, so two nodes describing the same address set must
    // produce identical entries.
    let one = NodeAddr::default()
        .with_direct(addr(1))
        .with_direct(addr(2));
    let other = NodeAddr::default()
        .with_direct(addr(2))
        .with_direct(addr(1));

    assert_eq!(
        postcard::to_stdvec(&one).unwrap(),
        postcard::to_stdvec(&other).unwrap()
    );
}

#[test]
fn a_repeated_address_is_stored_once() {
    let addrs = NodeAddr::default()
        .with_direct(addr(1))
        .with_direct(addr(1));

    assert_eq!(addrs.direct.len(), 1);
}

#[test]
fn the_default_is_lookup_only_and_reads_as_empty() {
    // A meaningful value rather than a placeholder: no addresses means "dial by
    // member id and let address lookup find them".
    let addrs = NodeAddr::lookup_only();

    assert!(addrs.is_empty());
    assert_eq!(addrs, NodeAddr::default());
}

#[test]
fn a_relay_alone_is_not_empty() {
    let addrs = NodeAddr::default().with_relay("https://relay.example/");

    assert!(!addrs.is_empty());
}
