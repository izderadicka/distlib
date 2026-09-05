//! Membership enforcement at the endpoint.
//!
//! This is the single choke point. It is installed on the *endpoint*, not on a
//! protocol handler, and that placement is the whole point: phases 2–5 register
//! handlers this crate does not author (iroh-blobs, iroh-docs, iroh-gossip), so
//! a check inside `PingProtocol` would protect `distlib/ping/0` and nothing
//! else. **Do not move this into a protocol handler.**

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};

use distlib_core::MemberId;
use iroh::{
    EndpointAddr,
    endpoint::{
        AfterHandshakeOutcome, BeforeConnectOutcome, Connection, EndpointHooks,
        WeakConnectionHandle,
    },
};

use crate::allowlist::Allowlist;

/// QUIC application close codes distlib sends.
pub mod close_code {
    use iroh::endpoint::VarInt;

    /// The peer is not a member of this group.
    ///
    /// A distinct code so the far side can tell "refused by policy" from a
    /// network failure and stop retrying.
    pub const NOT_A_MEMBER: VarInt = VarInt::from_u32(0x1000);

    /// The peer is a member, but not a Raft voter.
    ///
    /// Distinct from [`NOT_A_MEMBER`] because they mean different things to
    /// whoever reads a packet capture: one is "you are not in this group", the
    /// other "you are, but consensus is not yours to take part in".
    pub const NOT_A_VOTER: VarInt = VarInt::from_u32(0x1001);
}

/// Human-readable reason sent alongside [`close_code::NOT_A_MEMBER`].
pub const NOT_A_MEMBER_REASON: &[u8] = b"not a member";

/// Human-readable reason sent alongside [`close_code::NOT_A_VOTER`].
pub const NOT_A_VOTER_REASON: &[u8] = b"not a voter";

/// How long the same peer's refusals are summarised rather than logged.
///
/// A refused peer usually retries, and some retry hard: a follower that has
/// been expelled asks its sources about once a second, and anybody at all can
/// dial this node as fast as they like. One line per attempt turns a node's log
/// into whatever a stranger wants it to be, which is a denial of service
/// against the operator and against the disk.
const REFUSAL_QUIET: Duration = Duration::from_secs(60);

/// How many peers may be remembered for that purpose.
///
/// Bounded because the keys come from strangers. Past this the oldest are
/// dropped, which costs at worst one extra log line each.
const REFUSALS_REMEMBERED: usize = 64;

/// One peer's refusals since the last time they were logged.
#[derive(Debug)]
struct Refusals {
    last_logged: Instant,
    since: u64,
}

/// Refuses every connection to or from a non-member, and closes the ones an
/// expulsion invalidates.
///
/// Clone to hold one copy while the endpoint owns another: every clone shares
/// the same connection table, which is what lets [`Self::evict_expelled`] reach
/// connections the hook recorded.
#[derive(Debug, Clone)]
pub struct AllowlistHooks {
    allowlist: Allowlist,
    /// Connections seen since start-up, by peer.
    ///
    /// Weak handles, because [`EndpointHooks`] is explicit that holding a
    /// strong [`Connection`] keeps it alive and disables close-on-drop. A dead
    /// entry simply fails to upgrade, and is pruned when noticed.
    connections: Arc<Mutex<HashMap<MemberId, Vec<WeakConnectionHandle>>>>,

    /// When each refused peer was last logged, and how often since.
    refusals: Arc<Mutex<HashMap<MemberId, Refusals>>>,
}

impl AllowlistHooks {
    /// Enforces `allowlist` on the endpoint it is installed on.
    pub fn new(allowlist: Allowlist) -> Self {
        Self {
            allowlist,
            connections: Arc::new(Mutex::new(HashMap::new())),
            refusals: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// The allowlist being enforced.
    pub fn allowlist(&self) -> &Allowlist {
        &self.allowlist
    }

    /// Closes open connections whenever the allowlist stops admitting a peer.
    ///
    /// §4.4 requires this: refusing a removed member's *next* attempt is not
    /// enough, because a member expelled mid-transfer would otherwise carry on
    /// with the connection they already had. Run it as a task alongside the
    /// endpoint.
    ///
    /// Returns when the allowlist can no longer change — the writer has been
    /// dropped — since there is nothing further to react to.
    pub async fn evict_expelled(mut self) {
        while self.allowlist.changed().await.is_ok() {
            self.evict_once();
        }
    }

    /// Whether a recorded connection is still worth keeping.
    ///
    /// `upgrade` alone is not the test. The handle holds a `Weak` on iroh's
    /// internal connection state, which outlives the `Connection` the protocol
    /// handler dropped, so a finished connection still upgrades — for the
    /// lifetime of the endpoint. `close_reason` is what actually says it is
    /// over.
    fn is_live(handle: &WeakConnectionHandle) -> bool {
        handle
            .upgrade()
            .is_some_and(|connection| crate::connections::is_live(&connection))
    }

    /// One pass: close what is no longer admitted, forget what has died.
    fn evict_once(&self) {
        let mut connections = self.lock();
        connections.retain(|peer, handles| {
            if self.allowlist.is_allowed(peer) {
                // Still a member; keep only the connections still open.
                handles.retain(Self::is_live);
                return !handles.is_empty();
            }

            for connection in handles.iter().filter_map(WeakConnectionHandle::upgrade) {
                // The same code a refused handshake gets, so an evicted peer
                // and a rejected one cannot tell the two apart — and both learn
                // to stop retrying.
                connection.close(close_code::NOT_A_MEMBER, NOT_A_MEMBER_REASON);
            }
            tracing::info!(peer = %peer, "closed connections to a former member");
            false
        });
    }

    /// Records a connection so an expulsion can find it later.
    ///
    /// Prunes this peer's finished connections on the way in. A reconnect is
    /// the natural moment to notice the previous one has ended, and doing it
    /// here keeps the table bounded without a timer or a task per connection: a
    /// peer that reconnects repeatedly cleans up after itself, and one that
    /// never reconnects leaves at most a single closed handle behind. The total
    /// is bounded by the size of the group rather than by uptime.
    fn remember(&self, peer: MemberId, connection: &Connection) {
        let mut connections = self.lock();
        let handles = connections.entry(peer).or_default();
        handles.retain(Self::is_live);
        handles.push(connection.weak_handle());
    }

    /// How many connections are currently tracked.
    ///
    /// The number [`Self::evict_expelled`] would have to walk. Worth being able
    /// to see: it is the quantity that would reveal a leak here. It stays small
    /// because a peer's finished connections are dropped when it reconnects, so
    /// the total is bounded by the size of the group rather than by uptime.
    pub fn tracked_connections(&self) -> usize {
        self.lock().values().map(Vec::len).sum()
    }

    /// Records that `peer` was refused, and says whether to log it.
    ///
    /// `Some(n)` means log, where `n` is how many refusals of this peer went
    /// unlogged since the last line — so the audit trail keeps every peer that
    /// tried, and the count of how hard, without the attempts themselves
    /// deciding how much this node writes.
    fn note_refusal(&self, peer: MemberId, now: Instant) -> Option<u64> {
        let mut refusals = self.refusals.lock().unwrap_or_else(PoisonError::into_inner);

        match refusals.get_mut(&peer) {
            Some(seen) if now.duration_since(seen.last_logged) < REFUSAL_QUIET => {
                seen.since += 1;
                None
            }
            Some(seen) => {
                let since = std::mem::replace(&mut seen.since, 0);
                seen.last_logged = now;
                Some(since)
            }
            None => {
                // Room is made before inserting, and by dropping whoever has
                // been quiet longest: a peer still hammering this node is the
                // one worth keeping, since it is the one being suppressed.
                if refusals.len() >= REFUSALS_REMEMBERED
                    && let Some(stalest) = refusals
                        .iter()
                        .min_by_key(|(_, seen)| seen.last_logged)
                        .map(|(peer, _)| *peer)
                {
                    refusals.remove(&stalest);
                }
                refusals.insert(
                    peer,
                    Refusals {
                        last_logged: now,
                        since: 0,
                    },
                );
                Some(0)
            }
        }
    }

    /// A poison-tolerant lock.
    ///
    /// Nothing here panics while holding it, so a poisoned lock means an
    /// unrelated panic elsewhere; refusing to enforce membership afterwards
    /// would turn that into a security failure rather than a second bug.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<MemberId, Vec<WeakConnectionHandle>>> {
        self.connections
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl EndpointHooks for AllowlistHooks {
    /// Refuses to dial a non-member.
    ///
    /// This fires before any packet is sent, so an expelled member learns
    /// nothing — not even that this node is running. The identity here is the
    /// one we intend to dial rather than a verified one, which is exactly
    /// right: this is a policy decision about who we choose to contact.
    /// Verification is [`Self::after_handshake`]'s job.
    async fn before_connect(&self, addr: &EndpointAddr, alpn: &[u8]) -> BeforeConnectOutcome {
        let peer = MemberId::from(addr.id);
        if self.allowlist.is_allowed(&peer) {
            return BeforeConnectOutcome::Accept;
        }
        tracing::info!(
            peer = %peer,
            alpn = %String::from_utf8_lossy(alpn),
            "refused to dial a non-member",
        );
        BeforeConnectOutcome::Reject
    }

    /// Refuses a connection whose verified peer is not a member.
    ///
    /// Runs on incoming *and* outgoing connections. This is the authoritative
    /// check: only after the TLS handshake is the remote's identity proven
    /// rather than claimed.
    async fn after_handshake(&self, conn: &Connection) -> AfterHandshakeOutcome {
        let peer = MemberId::from(conn.remote_id());
        if self.allowlist.is_allowed(&peer) {
            // Recorded only once admitted: a rejected connection is closed
            // here and there is nothing left to evict later.
            self.remember(peer, conn);
            return AfterHandshakeOutcome::Accept;
        }
        // At info, because this is the audit trail for who tried to reach a
        // closed group — but summarised, because a peer that retries decides
        // how often this line would otherwise be written. See
        // [`Self::note_refusal`].
        match self.note_refusal(peer, Instant::now()) {
            Some(0) => tracing::info!(peer = %peer, "rejected a connection from a non-member"),
            Some(since) => tracing::info!(
                peer = %peer,
                since,
                "rejected a connection from a non-member, and {since} more since the last line",
            ),
            None => tracing::debug!(peer = %peer, "rejected a connection from a non-member"),
        }
        AfterHandshakeOutcome::Reject {
            error_code: close_code::NOT_A_MEMBER,
            reason: NOT_A_MEMBER_REASON.to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

    use super::*;
    use crate::allowlist::{AllowlistWriter, allowlist};
    use iroh::SecretKey;

    fn an_id(byte: u8) -> MemberId {
        MemberId::from(SecretKey::from_bytes(&[byte; 32]).public())
    }

    /// Hooks admitting nobody, which is all these tests need: they are about
    /// what happens *after* a peer has been refused.
    fn hooks() -> (AllowlistWriter, AllowlistHooks) {
        let (writer, list) = allowlist(an_id(1), []);
        (writer, AllowlistHooks::new(list))
    }

    #[test]
    fn a_peer_hammering_this_node_gets_one_line_a_minute() {
        // The point of the whole thing: how often this is written must be
        // decided here and not by whoever is knocking.
        let (_writer, hooks) = hooks();
        let peer = an_id(2);
        let start = Instant::now();

        assert_eq!(hooks.note_refusal(peer, start), Some(0), "the first one");
        assert_eq!(hooks.note_refusal(peer, start + REFUSAL_QUIET / 2), None);
        assert_eq!(hooks.note_refusal(peer, start + REFUSAL_QUIET / 2), None);
        assert_eq!(
            hooks.note_refusal(peer, start + REFUSAL_QUIET),
            Some(2),
            "and the next line says how many went unwritten"
        );
        assert_eq!(
            hooks.note_refusal(peer, start + REFUSAL_QUIET + REFUSAL_QUIET / 2),
            None,
            "the count starts again from the line just written"
        );
    }

    #[test]
    fn every_peer_is_logged_the_first_time_it_is_refused() {
        // The audit trail is who tried, so a new one is never summarised away —
        // however busy somebody else is being.
        let (_writer, hooks) = hooks();
        let now = Instant::now();

        assert_eq!(hooks.note_refusal(an_id(2), now), Some(0));
        assert_eq!(hooks.note_refusal(an_id(2), now), None);
        assert_eq!(hooks.note_refusal(an_id(3), now), Some(0));
        assert_eq!(hooks.note_refusal(an_id(4), now), Some(0));
    }

    #[test]
    fn the_table_of_refused_peers_cannot_grow_without_bound() {
        // The keys come from strangers, so this is the other half of the same
        // problem: a peer that cannot flood the log must not be able to flood
        // memory by using a fresh identity each time.
        let (_writer, hooks) = hooks();
        let now = Instant::now();

        for byte in 0..=255u8 {
            hooks.note_refusal(an_id(byte), now + Duration::from_millis(u64::from(byte)));
        }

        let remembered = hooks
            .refusals
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        assert!(
            remembered <= REFUSALS_REMEMBERED,
            "kept {remembered} peers, which is more than the cap"
        );
    }
}
