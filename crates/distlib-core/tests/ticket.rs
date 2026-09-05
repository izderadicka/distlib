//! The join ticket: what a new member is handed, and what it is not.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{net::SocketAddr, str::FromStr as _};

use distlib_core::{CoreError, GroupId, MemberId, NodeAddr, RelayMode, Ticket};
use iroh::SecretKey;

fn a_ticket() -> Ticket {
    let member = MemberId::from(SecretKey::generate().public());
    Ticket {
        group: GroupId::from_str(&"ab".repeat(32)).unwrap(),
        core: vec![(
            member,
            NodeAddr {
                relay: Some("https://relay.example/".to_owned()),
                direct: ["192.0.2.1:11204".parse::<SocketAddr>().unwrap()]
                    .into_iter()
                    .collect(),
            },
        )],
        relay_mode: RelayMode::Custom,
        relay_urls: vec!["https://relay.example/".to_owned()],
    }
}

#[test]
fn a_ticket_survives_the_round_trip() {
    let ticket = a_ticket();
    let printed = ticket.to_string();

    assert_eq!(Ticket::from_str(&printed).unwrap(), ticket);
}

#[test]
fn a_ticket_is_one_line_and_says_what_it_is() {
    // It gets pasted into a chat window, so it has to survive being wrapped and
    // quoted, and somebody looking at it should be able to tell what it is.
    let printed = a_ticket().to_string();

    assert!(printed.starts_with("distlib1"), "got {printed}");
    assert!(!printed.contains(char::is_whitespace), "got {printed}");
}

#[test]
fn surrounding_whitespace_is_forgiven() {
    // Copying out of a chat window brings a newline more often than not.
    let ticket = a_ticket();
    let printed = format!("  {}\n", ticket);

    assert_eq!(Ticket::from_str(&printed).unwrap(), ticket);
}

#[test]
fn something_that_is_not_a_ticket_says_so() {
    for (text, expected) in [
        ("hello", "does not start with"),
        ("distlib1!!!!", "not valid base32"),
        ("distlib1AAAA", "did not decode"),
    ] {
        let error = Ticket::from_str(text).unwrap_err();
        assert!(
            matches!(&error, CoreError::MalformedTicket { reason } if reason.contains(expected)),
            "{text:?} gave {error}"
        );
    }
}
