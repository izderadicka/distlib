//! Who keeps which item for the group: weighted rendezvous hashing, computed
//! from the membership log alone (phase 5's D1 and D3).
//!
//! Every member scores every item; an item's custodians are the members with
//! the highest scores. Nothing about custody is stored anywhere: every node
//! computes the same answer from the same pledges and the same items.

use rayon::prelude::*;

use crate::{ItemId, MemberId};

/// Separates a custody score's hash from every other hash of the same ids:
/// the key it is hashed with is derived from this.
const CUSTODY_CONTEXT: &str = "distlib.custody.v1";

/// From this many items on, [`CustodyGroup::custody_of`] spreads the work over
/// every core. Measured at 1,000 members on 12 cores: even at 256 items, twice
/// as fast at 1,024, eight times at a million (0.25 s against 2 s).
const PARALLEL_FROM: usize = 1024;

/// How a pledge becomes a weight (D3): capped, so no one member attracts most
/// of the items, and rounded down to a step, so a small edit moves nothing.
/// Nothing below the minimum, so no custodian is refused its content (5b-5b).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WeightRule {
    pub cap_bytes: u64,
    pub step_bytes: u64,
    pub min_bytes: u64,
}

impl WeightRule {
    /// The weight of a member pledging `pledge_bytes`.
    pub fn weight(&self, pledge_bytes: u64) -> u64 {
        if pledge_bytes < self.min_bytes {
            return 0;
        }
        let capped = pledge_bytes.min(self.cap_bytes);
        match self.step_bytes {
            0 => capped,
            step => capped - capped % step,
        }
    }
}

/// An item as custody sees it: who added it, whose copies are their own and
/// so are never ranked as its custodians, and how much there is to keep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustodyItem {
    pub item: ItemId,
    /// Usually one; two members adding the same files at once both count.
    pub creators: Vec<MemberId>,
    /// All of its files.
    pub size_bytes: u64,
}

/// The members custody is shared among, with their weights, and how many
/// copies the group keeps of each item.
#[derive(Debug, Clone, PartialEq)]
pub struct CustodyGroup {
    replicas: u8,
    /// Derived from [`CUSTODY_CONTEXT`] once, so that a score is one keyed
    /// hash of exactly one 64-byte block: the item and the member.
    key: [u8; 32],
    /// Every member, sorted: a creator still in here keeps its own copy.
    members: Vec<MemberId>,
    /// The members with a weight, sorted by id: the only ones ranked.
    weighted: Vec<(MemberId, f64)>,
}

impl CustodyGroup {
    /// The group of `pledges`, each member's pledge in bytes, weighed by
    /// `rule`. A member weighing nothing is custodian of nothing.
    pub fn new(
        replicas: u8,
        rule: WeightRule,
        pledges: impl IntoIterator<Item = (MemberId, u64)>,
    ) -> Self {
        let mut pledges: Vec<(MemberId, u64)> = pledges.into_iter().collect();
        pledges.sort_unstable_by_key(|(member, _)| *member);
        pledges.dedup_by_key(|(member, _)| *member);
        let weighted = pledges
            .iter()
            .map(|(member, pledge)| (*member, rule.weight(*pledge)))
            .filter(|(_, weight)| *weight > 0)
            .map(|(member, weight)| (member, weight as f64))
            .collect();
        Self {
            replicas,
            key: blake3::derive_key(CUSTODY_CONTEXT, &[]),
            members: pledges.into_iter().map(|(member, _)| member).collect(),
            weighted,
        }
    }

    /// The custodians of `item`, highest score first.
    ///
    /// `replicas − 1` of them while a creator is a member, since its own copy
    /// is the first replica; `replicas` once none is. Fewer when fewer members
    /// have a weight.
    pub fn custodians(&self, item: &CustodyItem) -> Vec<MemberId> {
        let slots = self.slots(item);
        let mut ranked: Vec<Ranked> = self
            .candidates(item)
            .map(|(member, weight)| Ranked::new(&self.key, item.item, member, weight))
            .collect();
        let highest_first = |a: &Ranked, b: &Ranked| b.cmp(a);
        if ranked.len() > slots {
            ranked.select_nth_unstable_by(slots, highest_first);
            ranked.truncate(slots);
        }
        ranked.sort_unstable_by(highest_first);
        ranked.into_iter().map(|ranked| ranked.member).collect()
    }

    /// Whether `me` is one of `item`'s custodians: what
    /// [`Self::custodians`] says, without scoring every member.
    ///
    /// Stops at the first `slots` members found to outrank `me`, which for an
    /// item `me` is not custodian of comes after a handful of scores, however
    /// large the group.
    pub fn is_custodian(&self, item: &CustodyItem, me: MemberId) -> bool {
        let slots = self.slots(item);
        let Some(weight) = self.weight_of(me) else {
            return false;
        };
        if slots == 0 || item.creators.contains(&me) {
            return false;
        }
        let mine = Ranked::new(&self.key, item.item, me, weight);
        self.candidates(item)
            .filter(|(member, _)| *member != me)
            .filter(|(member, weight)| Ranked::new(&self.key, item.item, *member, *weight) > mine)
            .nth(slots - 1)
            .is_none()
    }

    /// The items of `items` that `me` is custodian of, in the order given.
    pub fn custody_of(&self, me: MemberId, items: &[CustodyItem]) -> Vec<ItemId> {
        if self.weight_of(me).is_none() {
            return Vec::new();
        }
        let kept = |item: &CustodyItem| self.is_custodian(item, me).then_some(item.item);
        if items.len() < PARALLEL_FROM {
            items.iter().filter_map(kept).collect()
        } else {
            items.par_iter().filter_map(kept).collect()
        }
    }

    /// The first member after `item`'s custodians, in score order, that
    /// `eligible` admits: who keeps the emergency copy of an item down to its
    /// last copy (D6). Only a member with a weight, never a creator.
    pub fn first_after_custodians(
        &self,
        item: &CustodyItem,
        eligible: impl Fn(MemberId) -> bool,
    ) -> Option<MemberId> {
        let mut ranked: Vec<Ranked> = self
            .candidates(item)
            .map(|(member, weight)| Ranked::new(&self.key, item.item, member, weight))
            .collect();
        ranked.sort_unstable_by(|a, b| b.cmp(a));
        ranked
            .into_iter()
            .skip(self.slots(item))
            .map(|ranked| ranked.member)
            .find(|member| eligible(*member))
    }

    /// What keeping `item` asks of the group's pledges: its size once per
    /// custodian slot (D9).
    pub fn demand_bytes(&self, item: &CustodyItem) -> u64 {
        item.size_bytes.saturating_mul(self.slots(item) as u64)
    }

    /// `demand_bytes` over the total weight (D9): above 1, some items are a
    /// copy short. `None` when nobody has a weight.
    pub fn load_factor(&self, demand_bytes: u128) -> Option<f64> {
        let total_weight: f64 = self.weighted.iter().map(|(_, weight)| weight).sum();
        (total_weight > 0.0).then(|| demand_bytes as f64 / total_weight)
    }

    /// How many custodians `item` has: one fewer while a creator is a member.
    fn slots(&self, item: &CustodyItem) -> usize {
        let created_here = item
            .creators
            .iter()
            .any(|creator| self.members.binary_search(creator).is_ok());
        usize::from(self.replicas).saturating_sub(usize::from(created_here))
    }

    /// The members ranked for `item`: everyone with a weight but its creators.
    fn candidates<'a>(
        &'a self,
        item: &'a CustodyItem,
    ) -> impl Iterator<Item = (MemberId, f64)> + 'a {
        self.weighted
            .iter()
            .copied()
            .filter(|(member, _)| !item.creators.contains(member))
    }

    fn weight_of(&self, member: MemberId) -> Option<f64> {
        self.weighted
            .binary_search_by_key(&member, |(member, _)| *member)
            .ok()
            .map(|index| self.weighted[index].1)
    }
}

/// A member's score for one item. Higher ranks first; equal scores, which
/// take a hash collision, rank the lower member id first.
#[derive(Debug, Clone, Copy)]
struct Ranked {
    score: f64,
    member: MemberId,
}

impl Ranked {
    /// `−weight / ln(h)`, with `h` in (0, 1) from the hash of the item and the
    /// member: a member twice as heavy wins twice as many items.
    fn new(key: &[u8; 32], item: ItemId, member: MemberId, weight: f64) -> Self {
        let mut ids = [0; 64];
        ids[..32].copy_from_slice(item.as_bytes());
        ids[32..].copy_from_slice(member.as_bytes());
        let mut first = [0; 8];
        first.copy_from_slice(&blake3::keyed_hash(key, &ids).as_bytes()[..8]);
        // The top 53 bits, which an f64 holds exactly, moved off zero by half
        // a step so that neither end of the interval is reached.
        let bits = u64::from_le_bytes(first) >> 11;
        let h = (bits as f64 + 0.5) / (1_u64 << 53) as f64;
        // `ln` is the platform's: glibc and Windows' runtime may differ in the
        // last bit, so two nodes can rank a near-tie differently (P5-4).
        // `libm::log` would make it the same everywhere.
        Self {
            score: -weight / h.ln(),
            member,
        }
    }
}

impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for Ranked {}

impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.member.cmp(&self.member))
    }
}
