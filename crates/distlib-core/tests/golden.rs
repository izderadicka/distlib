//! Golden encodings: the exact bytes of what core types put on the wire, in the
//! catalogue document or in the config file, so that a change to an encoding
//! fails here rather than in a live group (5d-4b, `docs/plan-upgrades.md`).
//!
//! Each check runs both ways: the value encodes to the golden, and the golden
//! reads back to the value. The signed address inside the heartbeat is the one
//! `distlib-consensus` pins too, from the same seed.

#![allow(clippy::unwrap_used)]
// test code: a panic on a broken invariant is the point
// `Jail::expect_with` closures return figment's own large error type.
#![allow(clippy::result_large_err)]

use std::{
    net::{Ipv4Addr, SocketAddr},
    path::Path,
    str::FromStr as _,
};

use data_encoding::HEXLOWER;
use distlib_core::{
    ApiConfig, AvailabilityConfig, Bookmark, BookmarkId, Comment, CommunityKey, Config,
    ConsensusConfig, ContentHash, CoreMember, FileRecord, FileRole, GroupId, GroupSettings,
    Heartbeat, Holdings, Item, ItemId, ItemKind, LibraryConfig, MemberId, NetConfig, NodeAddr,
    ProbeFailure, Rating, RelayMode, Review, Series, SignedAddress, SignedHeartbeat, StorageConfig,
    Ticket, TransferConfig, Wish, WishFields, WishId, WishStatus,
    availability::{decode_base, encode_base},
};
use figment::Jail;
use iroh::SecretKey;
use serde::{Serialize, de::DeserializeOwned};

/// `value` encodes to exactly `hex` in postcard, and `hex` decodes to a value
/// that encodes back to it.
fn assert_postcard<T: Serialize + DeserializeOwned>(value: &T, hex: &str) {
    let encoded = postcard::to_stdvec(value).unwrap();
    assert_eq!(HEXLOWER.encode(&encoded), hex, "the encoding changed");
    let decoded: T = postcard::from_bytes(&encoded).unwrap();
    assert_eq!(
        postcard::to_stdvec(&decoded).unwrap(),
        encoded,
        "the decoding changed"
    );
}

/// `text` as postcard writes a string of 64 bytes: the length, then the text.
fn postcard_text_64(text: &str) -> String {
    assert_eq!(text.len(), 64);
    format!("40{}", HEXLOWER.encode(text.as_bytes()))
}

fn key(seed: u8) -> SecretKey {
    SecretKey::from_bytes(&[seed; 32])
}

fn member(seed: u8) -> MemberId {
    MemberId::from(key(seed).public())
}

/// An address with both a relay and a direct socket, so both are pinned.
fn addr() -> NodeAddr {
    NodeAddr::lookup_only()
        .with_relay("https://relay.example/")
        .with_direct(SocketAddr::from((Ipv4Addr::new(10, 0, 0, 1), 4433)))
}

const ITEM: ItemId = ItemId::from_bytes([1; 32]);
const BLOB: ContentHash = ContentHash::from_bytes([3; 32]);
const WISH: WishId = WishId::from_bytes([4; 32]);
const BOOKMARK: BookmarkId = BookmarkId::from_bytes([5; 16]);
const GROUP: GroupId = GroupId::from_bytes([9; 32]);

const ITEM_HEX: &str = "0101010101010101010101010101010101010101010101010101010101010101";
const BLOB_HEX: &str = "0303030303030303030303030303030303030303030303030303030303030303";
const WISH_HEX: &str = "0404040404040404040404040404040404040404040404040404040404040404";
const BOOKMARK_HEX: &str = "05050505050505050505050505050505";
const GROUP_HEX: &str = "0909090909090909090909090909090909090909090909090909090909090909";
/// Member 1's id: its public key in hex.
const MEMBER_1_HEX: &str = "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c";
const MEMBER_2_HEX: &str = "8139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394";
/// [`addr`]: the relay url as a string, then a set of one IPv4 socket.
const ADDR: &str = "011668747470733a2f2f72656c61792e6578616d706c652f01000a000001d122";

#[test]
fn every_item_field_and_file_has_a_fixed_key_and_json_value() {
    let mut item = Item::new(ITEM);
    item.kind = Some(ItemKind::Audiobook);
    item.title = Some("Dune".into());
    item.authors = Some(vec!["Frank Herbert".into()]);
    item.genres = Some(vec!["sf".into()]);
    item.series = Some(Series {
        name: "Dune".into(),
        index: Some(1.5),
    });
    item.year = Some(1965);
    item.lang = Some("en".into());
    item.description = Some("Spice.".into());
    item.files.insert(
        BLOB,
        FileRecord {
            role: FileRole::Content,
            format: "mp3".into(),
            size: 1000,
            filename: "01.mp3".into(),
            seq: Some(1),
            disc: Some(2),
            title: Some("One".into()),
            duration: Some(60),
        },
    );
    let golden = [
        ("type", r#""audiobook""#),
        ("title", r#""Dune""#),
        ("authors", r#"["Frank Herbert"]"#),
        ("genres", r#"["sf"]"#),
        ("series", r#"{"name":"Dune","index":1.5}"#),
        ("year", "1965"),
        ("lang", r#""en""#),
        ("description", r#""Spice.""#),
        (
            &format!("file/{BLOB_HEX}"),
            r#"{"role":"content","format":"mp3","size":1000,"filename":"01.mp3","seq":1,"disc":2,"title":"One","duration":60}"#,
        ),
    ]
    .map(|(field, value)| (format!("item/{ITEM_HEX}/{field}"), value.to_owned()));

    let entries: Vec<_> = item
        .entries()
        .into_iter()
        .map(|(key, value)| (key, String::from_utf8(value).unwrap()))
        .collect();
    assert_eq!(entries, golden);

    let mut read_back = Item::new(ITEM);
    for (key, value) in &golden {
        read_back.absorb(key.as_bytes(), value.as_bytes());
    }
    assert_eq!(read_back, item);
}

#[test]
fn every_item_kind_and_file_role_has_a_fixed_name() {
    for (kind, name) in [
        (ItemKind::Ebook, r#""ebook""#),
        (ItemKind::Audiobook, r#""audiobook""#),
        (ItemKind::Video, r#""video""#),
        (ItemKind::Other, r#""other""#),
    ] {
        assert_eq!(serde_json::to_string(&kind).unwrap(), name);
        assert_eq!(serde_json::from_str::<ItemKind>(name).unwrap(), kind);
    }
    for (role, name) in [
        (FileRole::Content, r#""content""#),
        (FileRole::Cover, r#""cover""#),
        (FileRole::Subtitle, r#""subtitle""#),
        (FileRole::Metadata, r#""metadata""#),
        (FileRole::Other, r#""other""#),
    ] {
        assert_eq!(serde_json::to_string(&role).unwrap(), name);
        assert_eq!(serde_json::from_str::<FileRole>(name).unwrap(), role);
    }
}

#[test]
fn an_items_fingerprint_is_fixed() {
    assert_eq!(
        ItemId::from_content_hashes(&[[3; 32], [6; 32]]).to_string(),
        "4e873bb9adf1fe0a4f613cb3ec69d48705fa77fba785e907387150b992657f8b"
    );
}

#[test]
fn every_community_key_is_fixed() {
    let (member, custodian) = (member(1), member(2));
    for (key, golden) in [
        (
            CommunityKey::Rating { item: ITEM, member },
            format!("rating/{ITEM_HEX}/{MEMBER_1_HEX}"),
        ),
        (
            CommunityKey::Review { item: ITEM, member },
            format!("review/{ITEM_HEX}/{MEMBER_1_HEX}"),
        ),
        (
            CommunityKey::Bookmark {
                item: ITEM,
                member,
                bookmark: BOOKMARK,
            },
            format!("bookmark/{ITEM_HEX}/{MEMBER_1_HEX}/{BOOKMARK_HEX}"),
        ),
        (
            CommunityKey::Wish { wish: WISH, member },
            format!("wish/{WISH_HEX}/{MEMBER_1_HEX}"),
        ),
        (
            CommunityKey::WishComment { wish: WISH, member },
            format!("wish_comment/{WISH_HEX}/{MEMBER_1_HEX}"),
        ),
        (
            CommunityKey::ProbeFailure {
                custodian,
                prober: member,
            },
            format!("probe/{MEMBER_2_HEX}/{MEMBER_1_HEX}"),
        ),
    ] {
        assert_eq!(key.encode(), golden);
        assert_eq!(CommunityKey::parse(golden.as_bytes()), Some(key));
    }
}

#[test]
fn every_community_value_is_fixed_json() {
    fn check<T: PartialEq + std::fmt::Debug>(
        value: T,
        encode: impl Fn(&T) -> Vec<u8>,
        decode: impl Fn(&[u8]) -> Option<T>,
        golden: &str,
    ) {
        assert_eq!(String::from_utf8(encode(&value)).unwrap(), golden);
        assert_eq!(decode(golden.as_bytes()), Some(value));
    }

    check(
        Rating::try_from(4).unwrap(),
        |rating| rating.encode(),
        Rating::decode,
        "4",
    );
    check(
        Review::try_from("Good.".to_owned()).unwrap(),
        Review::encode,
        Review::decode,
        r#""Good.""#,
    );
    check(
        Comment::try_from("Yes.".to_owned()).unwrap(),
        Comment::encode,
        Comment::decode,
        r#""Yes.""#,
    );
    check(
        Bookmark::new("p. 12".into(), "Here.".into(), 7).unwrap(),
        Bookmark::encode,
        Bookmark::decode,
        r#"{"position":"p. 12","note":"Here.","created_at":7}"#,
    );
    check(
        Wish::new(WishFields {
            kind: Some(ItemKind::Ebook),
            title: Some("Dune".into()),
            authors: Some(vec!["Frank Herbert".into()]),
            description: Some("Any.".into()),
            created_at: Some(7),
            status: WishStatus::Open,
        })
        .unwrap(),
        Wish::encode,
        Wish::decode,
        r#"{"kind":"ebook","title":"Dune","authors":["Frank Herbert"],"description":"Any.","created_at":7,"status":"open"}"#,
    );
    check(
        Wish::new(WishFields {
            kind: None,
            title: None,
            authors: None,
            description: None,
            created_at: None,
            status: WishStatus::Fulfilled { item_id: ITEM },
        })
        .unwrap(),
        Wish::encode,
        Wish::decode,
        &format!(r#"{{"status":"fulfilled","item_id":"{ITEM_HEX}"}}"#),
    );
    check(
        ProbeFailure {
            item: ITEM,
            blob: BLOB,
            chunk: 9,
            in_a_row: 2,
        },
        ProbeFailure::encode,
        ProbeFailure::decode,
        &format!(r#"{{"item":"{ITEM_HEX}","blob":"{BLOB_HEX}","chunk":9,"in_a_row":2}}"#),
    );
}

#[test]
fn a_signed_heartbeat_is_fixed_signature_included() {
    let heartbeat = Heartbeat {
        address: SignedAddress::sign(&key(1), addr(), 5).unwrap(),
        epoch: 7,
        seq: 2,
        interval_secs: 60,
        holdings: Holdings {
            count: 2,
            base: Some(BLOB),
            added: vec![ITEM],
            removed: vec![ItemId::from_bytes([2; 32])],
        },
        leaving: false,
    };
    let signed = SignedHeartbeat::sign(&key(1), &GROUP, heartbeat).unwrap();
    // The signed address: the member, the address, the index, the signature.
    let signed_address = format!(
        "{}{ADDR}05\
         ad7ead7485c8f9caea8fe8b3d890c2f6c578e6e1ab13c70171d6536d86bf4848\
         b91b8fd1989c8eb7e87c7fb1bc527a878efc798ca7f1c31ba723374a7d846000",
        postcard_text_64(MEMBER_1_HEX)
    );
    // The epoch, the sequence, the interval; then the count, the base as hex
    // text, one added and one removed id as raw bytes; then not leaving, and
    // the signature over the group and all of it.
    let golden = format!(
        "{signed_address}07023c\
         0201{}\
         01{ITEM_HEX}\
         010202020202020202020202020202020202020202020202020202020202020202\
         00\
         0c9a52957c0b412cb69d0b9c9474bf0144a28e436d2205c36d0a0bde31d88502\
         251e84f7a4eab3a540b5971f7fa302602b4cae25bb411a4995144b0e568d0c0f",
        postcard_text_64(BLOB_HEX)
    );

    assert_eq!(HEXLOWER.encode(&signed.encode().unwrap()), golden);
    let decoded = SignedHeartbeat::decode(&HEXLOWER.decode(golden.as_bytes()).unwrap()).unwrap();
    assert_eq!(decoded, signed);
    decoded.open(&GROUP).unwrap();
}

#[test]
fn a_base_list_is_fixed() {
    let golden = format!("01{ITEM_HEX}{}", "02".repeat(32));
    let bytes = encode_base([ItemId::from_bytes([2; 32]), ITEM, ITEM]);

    assert_eq!(HEXLOWER.encode(&bytes), golden);
    assert_eq!(
        decode_base(&bytes).unwrap().collect::<Vec<_>>(),
        [ITEM, ItemId::from_bytes([2; 32])]
    );
}

#[test]
fn a_ticket_is_fixed() {
    let ticket = Ticket {
        group: GROUP,
        core: vec![(member(1), addr())],
        relay_mode: RelayMode::Custom,
        relay_urls: vec!["https://relay.example/".into()],
    };
    // The group, one core member with its address, `custom`, one relay url.
    assert_postcard(
        &ticket,
        &format!(
            "{}01{}{ADDR}02011668747470733a2f2f72656c61792e6578616d706c652f",
            postcard_text_64(GROUP_HEX),
            postcard_text_64(MEMBER_1_HEX)
        ),
    );
    // The same bytes in base32, after the version prefix.
    let printed = "distlib1IAYDSMBZGA4TAOJQHEYDSMBZGA4TAOJQHEYDSMBZGA4TAOJQHEYDSMBZGA4TAOJQHEYDSMBZGA4TAOJQHEYDSMBZGA4TAOJQHEYDSMBZAFADQYJYHBSTGZDEG42DAOLGGE4TKZTEGUZGIYRSMQZWGYTBGVSDOMTDME3DOMBZMJTDCZBZGQYTEMLCMYZTONBYHAYDCYRUGBTDMZRVMMARM2DUORYHGORPF5ZGK3DBPEXGK6DBNVYGYZJPAEAAUAAAAHISEAQBCZUHI5DQOM5C6L3SMVWGC6JOMV4GC3LQNRSS6";
    assert_eq!(ticket.to_string(), printed);
    assert_eq!(Ticket::from_str(printed).unwrap(), ticket);
}

/// Every key the config file has, each set to something other than its
/// default, so a renamed key fails to load and a changed meaning fails to
/// compare.
const CONFIG_SAMPLE: &str = r#"
[net]
bind_addr_v4 = "127.0.0.1:11204"
relay_mode = "custom"
relay_urls = ["https://relay.example/"]

[[consensus.core]]
member = "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c"
name = "ann"
addrs = ["10.0.0.1:4433"]
relay = "https://relay.example/"

[api]
enabled = false
bind_addr = "127.0.0.1:11999"
max_upload_bytes = 1234567

[library]
download_dir = "/srv/books"

[availability]
beat_interval_secs = 17

[storage]
gc_interval_secs = 19
limit_bytes = 50000000000

[transfer]
max_fetches = 3
max_download_rate = 2000000
max_upload_rate = 3000000

[group]
replicas = 2
cap_bytes = 1000
weight_step_bytes = 100
min_pledge_bytes = 10
pending_expiry = 64
settle_secs = 60
max_settle_secs = 600
emergency_after_secs = 120
min_transfer_rate = 5000
probes = false
probe_every_secs = 30
failure_window_secs = 900
"#;

#[test]
fn a_config_sample_with_every_key_keeps_loading() {
    let expected = Config {
        net: NetConfig {
            bind_addr_v4: "127.0.0.1:11204".parse().unwrap(),
            relay_mode: RelayMode::Custom,
            relay_urls: vec!["https://relay.example/".into()],
        },
        consensus: ConsensusConfig {
            core: vec![CoreMember {
                member: member(1),
                name: "ann".into(),
                addrs: vec!["10.0.0.1:4433".parse().unwrap()],
                relay: Some("https://relay.example/".into()),
            }],
        },
        api: ApiConfig {
            enabled: false,
            bind_addr: "127.0.0.1:11999".parse().unwrap(),
            max_upload_bytes: 1_234_567,
        },
        library: LibraryConfig {
            download_dir: "/srv/books".into(),
        },
        availability: AvailabilityConfig {
            beat_interval_secs: 17.try_into().unwrap(),
        },
        storage: StorageConfig {
            gc_interval_secs: 19.try_into().unwrap(),
            limit_bytes: Some(50_000_000_000.try_into().unwrap()),
        },
        transfer: TransferConfig {
            max_fetches: 3.try_into().unwrap(),
            max_download_rate: Some(2_000_000.try_into().unwrap()),
            max_upload_rate: Some(3_000_000.try_into().unwrap()),
        },
        group: Some(GroupSettings {
            replicas: 2.try_into().unwrap(),
            cap_bytes: 1000,
            weight_step_bytes: 100.try_into().unwrap(),
            min_pledge_bytes: 10,
            pending_expiry: 64.try_into().unwrap(),
            settle_secs: 60,
            max_settle_secs: 600,
            emergency_after_secs: 120,
            min_transfer_rate: 5000,
            probes: false,
            probe_every_secs: 30.try_into().unwrap(),
            failure_window_secs: 900,
        }),
    };

    Jail::expect_with(move |jail| {
        jail.create_file("config.toml", CONFIG_SAMPLE)?;

        assert_eq!(Config::load(Path::new("config.toml")).unwrap(), expected);
        Ok(())
    });
}
