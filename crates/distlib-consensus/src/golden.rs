//! Golden encodings: the exact bytes of what this crate puts on the wire or on
//! disk, so that a change to an encoding fails here rather than in a live
//! group (5d-4, `docs/plan-upgrades.md`). A failure is a protocol or storage
//! change: append a variant instead of inserting one, or make the change behind
//! a protocol raise.
//!
//! The private envelopes are pinned beside their types, with the helpers and
//! fixtures from here.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::{Ipv4Addr, SocketAddr},
};

use data_encoding::HEXLOWER;
use distlib_core::{GroupId, MemberId, NodeAddr, RawMemberId, SignedAddress};
use iroh::SecretKey;
use openraft::{
    CommittedLeaderId, Entry, EntryPayload, LogId, Membership, SnapshotMeta, StoredMembership, Vote,
};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    ConsensusError, Fetched, MemberProfile, MembershipEvent, ProposeOutcome, SignedEvent, Source,
    Timestamp, TypeConfig,
};

/// `value` encodes to exactly `hex`, and `hex` decodes to a value that encodes
/// back to it.
pub(crate) fn assert_golden<T: Serialize + DeserializeOwned>(value: &T, hex: &str) {
    let encoded = postcard::to_stdvec(value).expect("a golden value encodes");
    assert_eq!(HEXLOWER.encode(&encoded), hex, "the encoding changed");
    let decoded: T = postcard::from_bytes(&encoded).expect("the golden bytes decode");
    assert_eq!(
        postcard::to_stdvec(&decoded).expect("a decoded value encodes"),
        encoded,
        "the decoding changed"
    );
}

pub(crate) fn key(seed: u8) -> SecretKey {
    SecretKey::from_bytes(&[seed; 32])
}

pub(crate) fn member(seed: u8) -> MemberId {
    MemberId::from(key(seed).public())
}

pub(crate) fn raw(seed: u8) -> RawMemberId {
    RawMemberId::from(member(seed))
}

/// An address with both a relay and a direct socket, so both are pinned.
pub(crate) fn addr() -> NodeAddr {
    NodeAddr::lookup_only()
        .with_relay("https://relay.example/")
        .with_direct(SocketAddr::from((Ipv4Addr::new(10, 0, 0, 1), 4433)))
}

pub(crate) fn profile(seed: u8) -> MemberProfile {
    MemberProfile {
        member_id: member(seed),
        display_name: "ann".into(),
    }
}

/// An approval by member 1, signed: ed25519 is deterministic, so its bytes are
/// fixed, signature included.
pub(crate) fn signed() -> SignedEvent {
    SignedEvent::sign(
        &key(1),
        MembershipEvent::Approved { proposal: 7 },
        Timestamp::from_millis(1),
        3,
    )
    .expect("an approval signs")
}

/// Member 1 saying it is at [`addr`], as of index 5.
pub(crate) fn signed_address() -> SignedAddress {
    SignedAddress::sign(&key(1), addr(), 5).expect("an address signs")
}

/// Index 5, in term 2, under member 1.
pub(crate) fn log_id() -> LogId<RawMemberId> {
    LogId::new(CommittedLeaderId::new(2, raw(1)), 5)
}

pub(crate) fn raft_membership() -> Membership<RawMemberId, NodeAddr> {
    Membership::new(
        vec![BTreeSet::from([raw(1)])],
        BTreeMap::from([(raw(1), addr())]),
    )
}

/// Member 1's id: its key in hex, as a string.
pub(crate) const MEMBER_1: &str = "4038613838653364643734303966313935666435326462326433636261356437326361363730396266316439343132316266333734383830316234306636663563";
pub(crate) const MEMBER_2: &str = "4038313339373730656138376431373566353661333534363663333463376563636362386438613931623465653337613235646636306635623866633962333934";
/// `GroupId::from_bytes([9; 32])`, as a hex string.
pub(crate) const GROUP_9: &str = "4030393039303930393039303930393039303930393039303930393039303930393039303930393039303930393039303930393039303930393039303930393039";
/// `GroupSettings::default()`, field by field: 3 replicas, a 2 TB cap, 10 GB
/// steps, a 100 GB minimum, 128 entries' expiry, 3600 s settle, a day's most
/// settle, a day to emergency, 1 MB/s, probes on, every 300 s, a week's
/// failure window.
pub(crate) const DEFAULT_SETTINGS: &str =
    "0380c0a8ca9a3a80c8afa02580d0dbc3f4028001901c80a30580a305c0843d01ac0280f524";
/// Member 1 as openraft's node id: the key's own 32 bytes.
pub(crate) const RAW_1: &str = "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c";
/// [`addr`]: the relay url as a string, then a set of one IPv4 socket.
pub(crate) const ADDR: &str = "011668747470733a2f2f72656c61792e6578616d706c652f01000a000001d122";

/// [`signed`]: the event, the proposer, the time, `changed_at`, the signature.
pub(crate) fn signed_hex() -> String {
    format!(
        "0507{MEMBER_1}0103\
         873d3d971ea480881c76f1e7317c257d5f2145ab489e9c39f232835e4cd052bf\
         51c30c17ea594b343854f811086bf7bb08bb2803d2287b91fb2317b816a42200"
    )
}

/// [`signed_address`]: the member, the address, the index, the signature.
pub(crate) fn signed_address_hex() -> String {
    format!(
        "{MEMBER_1}{ADDR}05\
         ad7ead7485c8f9caea8fe8b3d890c2f6c578e6e1ab13c70171d6536d86bf4848\
         b91b8fd1989c8eb7e87c7fb1bc527a878efc798ca7f1c31ba723374a7d846000"
    )
}

/// [`log_id`]: the term, the leader, the index.
pub(crate) fn log_id_hex() -> String {
    format!("02{RAW_1}05")
}

/// [`raft_membership`]: one voter set of one, and its node.
pub(crate) fn raft_membership_hex() -> String {
    format!("0101{RAW_1}01{RAW_1}{ADDR}")
}

#[test]
fn ids_addresses_and_a_signed_event() {
    assert_golden(&member(1), MEMBER_1);
    assert_golden(&member(2), MEMBER_2);
    assert_golden(&raw(1), RAW_1);
    assert_golden(&addr(), ADDR);
    assert_golden(&NodeAddr::lookup_only(), "0000");
    assert_golden(&signed(), &signed_hex());
    assert_golden(&signed_address(), &signed_address_hex());
}

#[test]
fn every_membership_event() {
    let events = [
        (
            MembershipEvent::GroupFounded {
                group_id: GroupId::from_bytes([9; 32]),
                founders: vec![(profile(1), addr())],
                settings: Box::default(),
            },
            format!("00{GROUP_9}01{MEMBER_1}03616e6e{ADDR}{DEFAULT_SETTINGS}"),
        ),
        (
            MembershipEvent::MemberAdded { member: profile(2) },
            format!("01{MEMBER_2}03616e6e"),
        ),
        (
            MembershipEvent::MemberExpelled {
                member: member(2),
                reason: "gone".into(),
            },
            format!("02{MEMBER_2}04676f6e65"),
        ),
        (
            MembershipEvent::PledgeChanged {
                member: member(2),
                pledge_bytes: 1_000,
            },
            format!("03{MEMBER_2}e807"),
        ),
        (
            MembershipEvent::CoreGroupChanged {
                core: vec![(member(1), addr())],
            },
            format!("0401{MEMBER_1}{ADDR}"),
        ),
        (MembershipEvent::Approved { proposal: 7 }, "0507".to_owned()),
        (
            MembershipEvent::Withdrawn { proposal: 7 },
            "0607".to_owned(),
        ),
    ];
    for (event, hex) in &events {
        assert_golden(event, hex);
    }
}

#[test]
fn what_a_follower_fetches() {
    let source = || Source {
        core: vec![(member(1), addr())],
        leader: Some(member(1)),
    };
    // One core member with its address, and the leader.
    let source_hex = format!("01{MEMBER_1}{ADDR}01{MEMBER_1}");
    assert_golden(
        &Fetched::Entries {
            up_to: 5,
            events: vec![(5, signed())],
            more: true,
            source: source(),
        },
        &format!("00050105{}01{source_hex}", signed_hex()),
    );
    assert_golden(&Fetched::NoGroup, "01");
    assert_golden(&Fetched::Redirect(source()), &format!("02{source_hex}"));
}

#[test]
fn what_became_of_a_proposal() {
    assert_golden(&ProposeOutcome::Applied { index: 5 }, "0005");
    assert_golden(
        &ProposeOutcome::Rejected(ConsensusError::NotFounded),
        "0101",
    );
    assert_golden(
        &ProposeOutcome::NotCommitted("no leader".into()),
        "02096e6f206c6561646572",
    );
}

/// A refusal travels in [`ProposeOutcome::Rejected`], so the order of the
/// variants is wire too. Pinned by discriminant: the fields are plain.
#[test]
fn every_consensus_error_keeps_its_discriminant() {
    let (one, two) = (member(1), member(2));
    let errors = [
        ConsensusError::BadSignature { proposer: one },
        ConsensusError::NotFounded,
        ConsensusError::AlreadyFounded,
        ConsensusError::FounderNotIncluded { proposer: one },
        ConsensusError::NoFounders,
        ConsensusError::DuplicateFounder { member: one },
        ConsensusError::ProposerNotAMember { proposer: one },
        ConsensusError::UnknownMember { member: one },
        ConsensusError::InvalidCoreGroup,
        ConsensusError::PledgeNotOwn {
            proposer: one,
            member: two,
        },
        ConsensusError::PledgeBelowMinimum {
            pledge_bytes: 1,
            min_pledge_bytes: 2,
        },
        ConsensusError::NotCoreMember { proposer: one },
        ConsensusError::AlreadyPending { proposal: 1 },
        ConsensusError::UnknownProposal { proposal: 1 },
        ConsensusError::ProposalExpired {
            proposal: 1,
            index: 2,
        },
        ConsensusError::ApproverNotCore { approver: one },
        ConsensusError::SelfApproval { member: one },
        ConsensusError::NotTheProposer {
            member: one,
            proposal: 1,
        },
        ConsensusError::NotAMembershipChange,
        ConsensusError::StaleProposal {
            seen: 1,
            current: 2,
        },
        ConsensusError::TooLong {
            field: "reason".into(),
            length: 2,
            limit: 1,
        },
        ConsensusError::Encode {
            message: "bad".into(),
        },
        ConsensusError::InvalidSettings {
            reason: "bad".into(),
        },
    ];
    for (discriminant, error) in errors.iter().enumerate() {
        let encoded = postcard::to_stdvec(error).expect("an error encodes");
        assert_eq!(usize::from(encoded[0]), discriminant, "{error:?}");
        assert_eq!(
            &postcard::from_bytes::<ConsensusError>(&encoded).expect("an error decodes"),
            error
        );
    }
}

/// openraft's types as the log store keeps them, and as they travel inside
/// this crate's own: a dependency upgrade that changes them is a protocol
/// change too.
#[test]
fn openraft_log_entries_votes_and_membership() {
    let (log_id_hex, membership_hex) = (log_id_hex(), raft_membership_hex());
    assert_golden(&log_id(), &log_id_hex);
    assert_golden(&Some(log_id()), &format!("01{log_id_hex}"));
    // The term and the voted-for, then whether the vote is committed.
    assert_golden(&Vote::new_committed(2, raw(1)), &format!("02{RAW_1}01"));
    assert_golden(&raft_membership(), &membership_hex);
    let entries: [(EntryPayload<TypeConfig>, String); 3] = [
        (EntryPayload::Blank, "00".into()),
        (
            EntryPayload::Normal(signed()),
            format!("01{}", signed_hex()),
        ),
        (
            EntryPayload::Membership(raft_membership()),
            format!("02{membership_hex}"),
        ),
    ];
    for (payload, payload_hex) in entries {
        assert_golden(
            &Entry::<TypeConfig> {
                log_id: log_id(),
                payload,
            },
            &format!("{log_id_hex}{payload_hex}"),
        );
    }
    let stored = || StoredMembership::new(Some(log_id()), raft_membership());
    let stored_hex = format!("01{log_id_hex}{membership_hex}");
    assert_golden(&stored(), &stored_hex);
    assert_golden(
        &SnapshotMeta {
            last_log_id: Some(log_id()),
            last_membership: stored(),
            snapshot_id: "2-5-0".into(),
        },
        &format!("01{log_id_hex}{stored_hex}05322d352d30"),
    );
}
