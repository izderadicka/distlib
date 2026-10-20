//! What each JSON-RPC method does.
//!
//! Method names come from §7.1 verbatim, so phase 3 extends this set rather
//! than renaming it: `library.*` and the SSE stream land beside these, and a
//! caller written against `group.members` today keeps working.

use std::{
    cmp::Reverse,
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use distlib_consensus::{MemberRecord, MembershipEvent, MembershipNode, MembershipState};
use distlib_core::{
    Bookmark, BookmarkId, Comment, ContentHash, FileRecord, FileRole, Item, ItemFields, ItemId,
    ItemKind, KeepReason, MemberId, NetConfig, NodeAddr, Rating, ResolvedWish, Review, Series,
    TaskId, Ticket, Wish, WishFields, WishId, WishStatus,
};
use distlib_custody::{Custody, PinCounts, Room, Storage, StorageError};
use distlib_net::{Blobs, NetError};
use distlib_store::{
    BookmarkFilter, ReindexHandle, SearchIndex, Store, StoreError, StoredBookmark, StoredItem,
};
use distlib_sync::{Availability, Catalogue, ImportedFile, SyncError, SyncState};
use iroh::SecretKey;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::watch;

use crate::{
    availability::Seen,
    rpc::Error,
    tasks::{self, Downloaded, Source, Tasks},
    uploads::{Staged, UploadId, Uploads},
};

/// A ceiling on a page's `limit` — `library.search`'s and `library.list`'s —
/// whatever a caller asks for.
///
/// This listener is loopback and token-gated (§7.1, P1-25), so this is a
/// guard against a mistake rather than an attacker — but tantivy's
/// `TopDocs::with_limit` allocates a heap sized to it, and no legitimate
/// caller of a personal library needs a single page bigger than this.
const MAX_PAGE: usize = 500;

/// Everything the methods need: the running node and the key it signs with.
///
/// The node is shared rather than owned: whoever started it keeps serving the
/// group with it while this answers questions about it.
#[derive(Clone)]
pub struct Api {
    pub node: Arc<MembershipNode>,
    pub secret: SecretKey,
    /// How this node reaches the network.
    ///
    /// Needed for `group.ticket`: a joiner has to reach the group the way this
    /// node does, so the directions have to carry it.
    pub net: NetConfig,
    /// `admin.reindex`'s way of asking the projection task to run, without
    /// this struct owning the task itself.
    pub reindex_handle: ReindexHandle,
    /// What `library.add` writes into. Every other `library.*` method reads
    /// `store`/`search` instead — the read model a projection keeps in step —
    /// because a write has to reach the document itself, and a read model is
    /// downstream of it rather than a second place to put one.
    pub catalogue: Catalogue,
    /// What `library.download` fetches an item's files with, and writes them
    /// out of. Against the same blob store `catalogue` hands iroh-docs and
    /// serves `iroh_blobs::ALPN` from, which is what makes a node that
    /// downloads a file a provider of it.
    pub blobs: Blobs,
    /// What `library.item` and `library.search` read the record fields from.
    pub store: Store,
    /// What `library.search` ranks against. Only a ranking — see its own doc
    /// comment — so a hit's fields still come from `store`.
    pub search: SearchIndex,
    /// Who is online and what they hold, for a hit's `providers` and
    /// `library.item`'s `availability`.
    pub availability: Availability,
    /// The downloads this node is running or has recently run, and the event
    /// bus they — and everything else — publish to.
    pub tasks: Tasks,
    /// Where `library.download` writes when it is given no `dest` — `[library]
    /// download_dir`, resolved against the data directory. Created when first
    /// needed, so a node nobody downloads through never has one.
    pub downloads: PathBuf,
    /// Files `POST /upload` has received, for `library.add`'s `uploads`.
    pub uploads: Uploads,
    /// Which items this node keeps for the group, and each item's custodians.
    pub custody: Custody,
    /// Where the items this node keeps stand: queued, fetched, kept.
    pub pins: watch::Receiver<PinCounts>,
    /// `[storage] limit_bytes`, and the cache's eviction to stay within it.
    pub storage: Storage,
    /// When the last garbage collection pass began; `None` before the first.
    pub collections: watch::Receiver<Option<SystemTime>>,
}

impl Api {
    /// Dispatches one call.
    pub async fn call(&self, method: &str, params: Option<Value>) -> Result<Value, Error> {
        match method {
            "node.status" => self.status().await,
            "group.members" => self.members(),
            "group.propose_add" => self.propose_add(parse(params)?).await,
            "group.propose_expel" => self.propose_expel(parse(params)?).await,
            "group.propose_core" => self.propose_core(parse(params)?).await,
            "group.pending" => self.pending(),
            "group.approve" => self.approve(parse(params)?).await,
            "group.withdraw" => self.withdraw(parse(params)?).await,
            "group.pledge_set" => self.pledge_set(parse(params)?).await,
            "group.ticket" => self.ticket(),
            "admin.reindex" => self.reindex().await,
            "library.search" => self.search(parse(params)?).await,
            "library.list" => self.list(parse(params)?).await,
            "library.item" => self.item(parse(params)?).await,
            "library.add" => self.add(parse(params)?).await,
            "library.download" => self.download(parse(params)?).await,
            "library.task" => self.task(parse(params)?),
            "library.edit_metadata" => self.edit_metadata(parse(params)?).await,
            "community.rate" => self.rate(parse(params)?).await,
            "community.review" => self.review(parse(params)?).await,
            "community.bookmark" => self.bookmark(parse(params)?).await,
            "community.bookmark_delete" => self.bookmark_delete(parse(params)?).await,
            "community.bookmarks" => self.bookmarks(parse(params)?).await,
            "community.wish_create" => self.wish_create(parse(params)?).await,
            "community.wish_list" => self.wish_list(parse(params)?).await,
            "community.wish_comment" => self.wish_comment(parse(params)?).await,
            "community.wish_fulfill" => self.wish_fulfill(parse(params)?).await,
            other => Err(Error::method_not_found(other)),
        }
    }

    /// `node.status` — who this node is and where it stands in its group.
    async fn status(&self) -> Result<Value, Error> {
        let storage = self.storage().await?;
        let membership = self.node.membership();
        let me = self.node.id();

        // Only a voter has a Raft to report on. A follower answers `null` for
        // both rather than inventing a state, since "this node is following"
        // is a different thing from "this node is a follower of a term".
        let (raft, leader) = match self.node.raft() {
            Some(raft) => {
                let metrics = raft.metrics();
                let metrics = metrics.borrow();
                (
                    Some(format!("{:?}", metrics.state)),
                    metrics
                        .current_leader
                        .and_then(|id| MemberId::try_from(id).ok()),
                )
            }
            None => (None, None),
        };

        Ok(json!({
            "member": me,
            "group": membership.group_id(),
            // Derived, not configured — the log decides who votes.
            "core": membership.is_core(&me),
            "members": membership.len(),
            "core_group": membership.core().keys().collect::<Vec<_>>(),
            // The log index this membership last changed at: what a proposal is
            // checked against, so a caller can see whether it is looking at a
            // current view.
            "changed_at": membership.changed_at(),
            "raft": raft,
            "leader": leader,
            // How far a follower has read the log. Null on a voter, which gets
            // the log pushed to it rather than fetching it.
            "followed_upto": (!self.node.is_core()).then(|| self.node.followed_upto()),
            // A count, not a listing. Status is a summary and every other field
            // in it is one line; `group.pending` is where the detail lives.
            "pending": membership.pending().count(),
            // Fixed at founding, so null until there is a group.
            "settings": membership.group_id().map(|_| membership.settings()),
            // Null until custody has been computed.
            "custody": self.custody.status(),
            "pins": *self.pins.borrow(),
            "storage": storage,
            "sync": sync_block(&self.catalogue.sync_status().borrow()),
        }))
    }

    /// The bytes this node keeps, by reason, against its storage limit.
    async fn storage(&self) -> Result<Value, Error> {
        let usage = self.blobs.kept_blobs().await.map_err(net_error)?.usage();
        Ok(json!({
            "limit_bytes": self.storage.limit(),
            "own_bytes": usage.own_bytes,
            "custody_bytes": usage.custody_bytes,
            "cache_bytes": usage.cache_bytes,
            // Own and custody alone over the limit: nothing left to evict.
            "mandatory_over_limit": self
                .storage
                .limit()
                .is_some_and(|limit| usage.mandatory_bytes() > limit.get()),
            "last_collection_at": self.collections.borrow().map(micros_since_epoch),
        }))
    }

    /// `group.ticket` — directions for somebody who has been admitted (§4.3).
    ///
    /// Built here rather than by the caller because the addresses come from
    /// Raft's own membership, which only a running node holds. The relay
    /// settings come from this node's configuration: a joiner has to reach the
    /// group the same way this node does.
    ///
    /// Not a credential. Anyone may ask for one, and holding it grants nothing
    /// — admission is a committed `MemberAdded`, and until that exists a
    /// ticket-holder is refused at the allowlist like anybody else.
    fn ticket(&self) -> Result<Value, Error> {
        let membership = self.node.membership();
        let group = membership
            .group_id()
            .ok_or_else(|| Error::failed("this node is in no group yet"))?;

        let ticket = Ticket {
            group,
            core: self.node.core_addresses(),
            relay_mode: self.net.relay_mode,
            relay_urls: self.net.relay_urls.clone(),
        };

        Ok(json!({ "ticket": ticket.to_string(), "group": group }))
    }

    /// `group.members` — the membership as this node has it.
    fn members(&self) -> Result<Value, Error> {
        let membership = self.node.membership();
        let members: Vec<Value> = membership
            .members()
            .map(|record| {
                json!({
                    "member": record.member_id,
                    "name": record.display_name,
                    "pledge_bytes": record.pledge_bytes,
                    "core": membership.is_core(&record.member_id),
                })
            })
            .collect();

        Ok(json!({
            "group": membership.group_id(),
            "changed_at": membership.changed_at(),
            "members": members,
        }))
    }

    /// `group.pending` — the changes waiting for approvals (§4.4 step 2).
    ///
    /// `needed` and `approvals` are both measured against the core group as it
    /// stands now — `approvals` counts only those from members who are still
    /// voters — which is what the fold will do when the next approval lands.
    /// So a proposal can be one approval away today and two away tomorrow, and
    /// this reports what is true when asked rather than what was true when it
    /// was proposed.
    ///
    /// `approved_by_you` because the list is the same for everyone who asks,
    /// and the one thing that differs — whether *this* member has already
    /// agreed — is what decides whether there is anything left for them to do.
    fn pending(&self) -> Result<Value, Error> {
        let membership = self.node.membership();
        let me = self.node.id();
        let pending: Vec<Value> = membership
            .pending()
            .map(|(proposal, entry)| {
                let approvals: Vec<_> = membership.approvals_counting(entry).collect();
                json!({
                    "proposal": proposal,
                    "proposer": entry.proposer(),
                    "what": describe(entry.event(), &membership),
                    "approved_by_you": approvals.contains(&me),
                    "approvals": approvals,
                    "needed": membership.approvals_needed(entry.event()),
                    // A count of further *changes*, not a duration: it only
                    // moves when the group commits something. Named so a
                    // caller cannot read it as time.
                    "expires_after_changes": membership.expires_after(proposal),
                })
            })
            .collect();

        Ok(json!({
            "changed_at": membership.changed_at(),
            "pending": pending,
        }))
    }

    /// `group.approve` — agree to a pending proposal (§4.4 step 2).
    ///
    /// **Answers about the proposal, not about the approval.** An approval is
    /// never itself a proposal — the fold dispatches it rather than holding it —
    /// so asking after the approval's own log index would always answer
    /// "applied", which is true of the approval and says nothing about the
    /// thing it was cast on. The caller wants to know whether the change has
    /// happened yet, and that is a question about the proposal's index.
    ///
    /// A repeat is committed, not refused — the fold accepts it on purpose, see
    /// `MembershipState::approve` — but answered with `already_approved`, read
    /// before committing since afterwards the two cases look the same.
    async fn approve(&self, params: Proposal) -> Result<Value, Error> {
        let me = self.node.id();
        let membership = self.node.membership();
        let already = membership
            .pending()
            .find(|(index, _)| *index == params.proposal)
            .is_some_and(|(_, entry)| membership.approvals_counting(entry).any(|a| a == me));

        self.commit(MembershipEvent::Approved {
            proposal: params.proposal,
        })
        .await?;
        let mut answer = self.outcome(params.proposal);
        answer["already_approved"] = json!(already);
        Ok(answer)
    }

    /// `group.withdraw` — take back a proposal of your own.
    ///
    /// No member parameter, for the same reason `group.pledge_set` has none:
    /// only the proposer may withdraw, so accepting one would only produce
    /// proposals the group refuses.
    async fn withdraw(&self, params: Proposal) -> Result<Value, Error> {
        self.commit(MembershipEvent::Withdrawn {
            proposal: params.proposal,
        })
        .await?;
        // Not `outcome`: a withdrawn proposal is not pending, and reporting
        // that as "applied" would say the change had taken effect when the
        // point of withdrawing was that it will not.
        Ok(json!({
            "changed_at": self.node.membership().changed_at(),
            "proposal": params.proposal,
            "withdrawn": true,
        }))
    }

    /// `group.propose_add` — admit a member (§4.3).
    async fn propose_add(&self, params: ProposeAdd) -> Result<Value, Error> {
        self.propose(MembershipEvent::MemberAdded {
            member: MemberRecord {
                member_id: params.member,
                display_name: params.name.unwrap_or_default(),
                // Theirs to set, not ours: §5.5 makes custodian assignment
                // depend on it, and `PledgeChanged` is self-only for that
                // reason. Admitting somebody does not speak for their storage.
                pledge_bytes: 0,
            },
        })
        .await
    }

    /// `group.propose_expel` — remove a member (§4.4).
    async fn propose_expel(&self, params: ProposeExpel) -> Result<Value, Error> {
        self.propose(MembershipEvent::MemberExpelled {
            member: params.member,
            reason: params.reason,
        })
        .await
    }

    /// `group.propose_core` — move, add or drop one core node (§4.2).
    ///
    /// **A delta, though the event is not.** [`MembershipEvent::CoreGroupChanged`]
    /// carries the whole desired core group, because a node map is what openraft
    /// is handed; but building that map is a read-modify-write, and where it
    /// happens decides whether it is safe. Done here, the read and the signature
    /// are the same node's, one await apart, and [`ConsensusError::StaleProposal`]
    /// covers the gap — the event is signed against the `changed_at` the map was
    /// read at, so a change that landed in between refuses this one rather than
    /// silently reverting it. A caller that assembled the map itself would sign
    /// against a `changed_at` newer than its own read, and that guard would pass
    /// while the map put back an address, a voter, or a demotion the group had
    /// just decided.
    ///
    /// So the caller names one member and says which of two things should
    /// become of them — `"change": "set"` with an address, or
    /// `"change": "remove"` — and never has to know where the others are.
    ///
    /// An address given replaces whatever the log holds rather than adding to
    /// it: a node that moved is not at both addresses, and a stale one left
    /// behind is a path every peer keeps trying. Removal is a demotion, not an
    /// expulsion; they stay a member.
    ///
    /// [`ConsensusError::StaleProposal`]: distlib_consensus::ConsensusError::StaleProposal
    async fn propose_core(&self, params: ProposeCore) -> Result<Value, Error> {
        let membership = self.node.membership();
        let mut core = membership.core().clone();

        // Either arm refuses a request that would change nothing, rather than
        // committing it as a no-op. The event would apply — the map is valid —
        // and applying it moves `changed_at`, which invalidates every proposal
        // in flight. Nothing should be able to do that by asking for something
        // that was already true.
        match params {
            ProposeCore::Set { member, addr } => {
                if core.get(&member) == Some(&addr) {
                    return Err(Error::invalid_params(format!(
                        "{member} is already a core node at that address"
                    )));
                }
                core.insert(member, addr);
            }
            ProposeCore::Remove { member } => {
                if core.remove(&member).is_none() {
                    return Err(Error::invalid_params(format!(
                        "{member} is not a core node"
                    )));
                }
            }
        }

        self.propose(MembershipEvent::CoreGroupChanged {
            core: core.into_iter().collect(),
        })
        .await
    }

    /// `group.pledge_set` — set *this* node's storage pledge.
    ///
    /// No member parameter, and that is the rule rather than a simplification:
    /// a pledge may only be set by the member it belongs to, so accepting one
    /// would only produce proposals the group refuses.
    async fn pledge_set(&self, params: PledgeSet) -> Result<Value, Error> {
        // The space for own items and the cache is what the limit leaves.
        if let Some(limit) = self.storage.limit()
            && params.bytes >= limit.get()
        {
            return Err(Error::invalid_params(format!(
                "a pledge of {} bytes is not below this node's storage limit of {limit} bytes",
                params.bytes
            )));
        }
        self.propose(MembershipEvent::PledgeChanged {
            member: self.node.id(),
            pledge_bytes: params.bytes,
        })
        .await
    }

    /// `admin.reindex` — rebuilds the read model from the document, the same
    /// replay a cold start runs (§5.4, P2-19). Blocks until it has finished,
    /// which for a settled catalogue is the point: a caller asking to reindex
    /// wants to know it is done, not that it was queued.
    async fn reindex(&self) -> Result<Value, Error> {
        self.reindex_handle
            .request()
            .await
            .map_err(|error| Error::failed(error.to_string()))?;
        Ok(json!({}))
    }

    /// `library.search` — ranks items in the search index for `query`, then
    /// reads each hit's fields back from the read model to show for it.
    ///
    /// **Two reads, not one, and deliberately.** `SearchIndex::search`'s own
    /// doc comment says why: it stores only enough to rank and identify a hit,
    /// not a second copy of the row to go stale in one of the two places.
    /// Hits are read in the order the index returned them, one at a time
    /// rather than a single `IN (...)`, because that order *is* the ranking —
    /// a batched query would have to be reassembled into it afterwards for no
    /// saving at the sizes a `limit` here ever asks for.
    ///
    /// **Paged by `offset` and `limit`**, and answers with `total`, the
    /// number of matches in all (3a-6). **No `filters`**, though the plan's
    /// own sketch names them — see [`Search`]'s doc comment.
    async fn search(&self, params: Search) -> Result<Value, Error> {
        let page = self
            .search
            .search_page(&params.query, params.offset, params.limit.min(MAX_PAGE))
            .await
            .map_err(query_error)?;

        let seen = self.seen();
        let mut results = Vec::with_capacity(page.items.len());
        for id in page.items {
            // `item_fields`, not `item`: a hit is shown a summary, and reading
            // every file row along with it would be a query this response
            // never uses the answer to — `library.item` is where a caller
            // reads those.
            if let Some(stored) = self
                .store
                .item_fields(id)
                .await
                .map_err(|error| Error::failed(error.to_string()))?
            {
                results.push(hit(&stored, &seen));
            }
        }
        Ok(json!({ "results": results, "total": page.total }))
    }

    /// `library.list` — every item, a page at a time, for browsing (3a-6).
    ///
    /// Ordered by title, ignoring case, untitled last — see [`Store::page`],
    /// including why a title starting with an accented letter sorts after
    /// `Z`. Answers with the same summaries as `library.search`, and `total`,
    /// the number of items in all. An offset past the end is an empty page,
    /// not an error: it is where a caller who asked for one page too many
    /// honestly is.
    async fn list(&self, params: List) -> Result<Value, Error> {
        let page = self
            .store
            .page(params.offset, params.limit.min(MAX_PAGE))
            .await
            .map_err(|error| Error::failed(error.to_string()))?;
        let seen = self.seen();
        let results: Vec<Value> = page.items.iter().map(|stored| hit(stored, &seen)).collect();
        Ok(json!({ "results": results, "total": page.total }))
    }

    /// `library.item` — the full record the read model holds for one item;
    /// its `availability`: `held`, `providers`, and the online `holders` and
    /// `unknown` members behind them; and current members' `ratings`,
    /// `reviews` and `bookmarks`.
    async fn item(&self, params: ItemParams) -> Result<Value, Error> {
        let stored = self
            .store
            .item(params.item_id)
            .await
            .map_err(|error| Error::failed(error.to_string()))?
            .ok_or_else(|| Error::failed(format!("no such item: {}", params.item_id)))?;

        let mut record = summary(&stored);
        record["lang"] = json!(stored.item.lang);
        record["description"] = json!(stored.item.description);
        record["files"] = json!(stored.item.files);
        // Whoever wrote one of its file keys, members or not any more.
        record["added_by"] = json!(stored.creators());
        // Null until custody has been computed.
        record["custodians"] = json!(self.custody.custodians(&stored.custody_item()));
        record["last_modified"] = json!(stored.last_modified);
        record["availability"] = self.seen().of(&params.item_id);
        record["ratings"] = ratings_summary(&stored.ratings, self.node.id());
        record["reviews"] = stored
            .reviews
            .iter()
            .map(|(member, review)| json!({ "member": member, "review": review }))
            .collect();
        record["bookmarks"] = self
            .store
            .bookmarks_of(params.item_id)
            .await
            .map_err(|error| Error::failed(error.to_string()))?
            .iter()
            .map(bookmark_json)
            .collect();
        Ok(record)
    }

    /// Who is online and what they hold, as of now.
    fn seen(&self) -> Seen<'_> {
        Seen::now(&self.availability, self.catalogue.holdings())
    }

    /// `library.edit_metadata` — writes the fields given, and only those.
    ///
    /// **One entry per field, and a field not named is not touched.** The
    /// catalogue is last-writer-wins per key, so two members editing the same
    /// item's title and genres at the same time both keep their edit; writing
    /// the whole record would have the second erase the first's. That is also
    /// why **a field cannot be cleared** here: "no value" has no entry to write
    /// that would not erase somebody else's concurrent one, and the catalogue
    /// has no tombstone for a field yet.
    ///
    /// **The item must already be in this node's document**, which is asked
    /// rather than the read model, since the document is what gets written.
    /// Editing an item nobody added would create one out of nothing but
    /// metadata — no files, and an id nothing fingerprints to.
    async fn edit_metadata(&self, params: EditMetadata) -> Result<Value, Error> {
        let EditMetadata { item_id, fields } = params;
        let mut edit = Item::new(item_id);
        edit.set(fields);
        if edit == Item::new(item_id) {
            return Err(Error::invalid_params(
                "library.edit_metadata needs at least one field to write",
            ));
        }
        self.refuse_unknown_item(item_id).await?;
        self.catalogue.write(&edit).await.map_err(sync_error)?;
        Ok(json!({ "item_id": item_id }))
    }

    /// `community.rate` — rates an item as this node's member, or clears that
    /// rating with `rating: null`. `library.item` shows it once projected.
    async fn rate(&self, params: Rate) -> Result<Value, Error> {
        let Rate { item_id, rating } = params;
        self.refuse_unknown_item(item_id).await?;
        match rating {
            Some(rating) => self.catalogue.rate(item_id, rating).await,
            None => self.catalogue.clear_rating(item_id).await,
        }
        .map_err(sync_error)?;
        Ok(json!({ "item_id": item_id }))
    }

    /// `community.review` — reviews an item as this node's member, or clears
    /// that review with `review: null`.
    async fn review(&self, params: ReviewParams) -> Result<Value, Error> {
        let ReviewParams { item_id, review } = params;
        self.refuse_unknown_item(item_id).await?;
        match review {
            Some(review) => self.catalogue.review(item_id, &review).await,
            None => self.catalogue.clear_review(item_id).await,
        }
        .map_err(sync_error)?;
        Ok(json!({ "item_id": item_id }))
    }

    /// `community.bookmark` — leaves a bookmark on an item as this node's
    /// member, or edits one of its own when `bookmark_id` names it. An edit
    /// keeps when the bookmark was made.
    async fn bookmark(&self, params: BookmarkParams) -> Result<Value, Error> {
        let BookmarkParams {
            item_id,
            bookmark_id,
            position,
            note,
        } = params;
        self.refuse_unknown_item(item_id).await?;
        let (bookmark_id, created_at) = match bookmark_id {
            Some(id) => (id, self.own_bookmark(item_id, id).await?.created_at()),
            None => (
                BookmarkId::random().map_err(|error| Error::failed(error.to_string()))?,
                micros_since_epoch(SystemTime::now()),
            ),
        };
        let bookmark = Bookmark::new(position, note, created_at)
            .map_err(|error| Error::invalid_params(error.to_string()))?;
        self.catalogue
            .bookmark(item_id, bookmark_id, &bookmark)
            .await
            .map_err(sync_error)?;
        Ok(json!({ "item_id": item_id, "bookmark_id": bookmark_id }))
    }

    /// `community.bookmark_delete` — deletes one of this node's member's
    /// bookmarks.
    async fn bookmark_delete(&self, params: BookmarkDelete) -> Result<Value, Error> {
        let BookmarkDelete {
            item_id,
            bookmark_id,
        } = params;
        self.own_bookmark(item_id, bookmark_id).await?;
        self.catalogue
            .delete_bookmark(item_id, bookmark_id)
            .await
            .map_err(sync_error)?;
        Ok(json!({ "item_id": item_id, "bookmark_id": bookmark_id }))
    }

    /// `community.bookmarks` — current members' bookmarks, newest first,
    /// narrowed by `q` (text in the note, the item's title or the member's
    /// name), `member` and `item_id`. "Mine" is `member` set to this node's
    /// own (D13).
    async fn bookmarks(&self, params: Bookmarks) -> Result<Value, Error> {
        let Bookmarks { q, member, item_id } = params;
        let found = self
            .store
            .find_bookmarks(BookmarkFilter {
                text: q,
                member,
                item: item_id,
            })
            .await
            .map_err(|error| Error::failed(error.to_string()))?;
        let bookmarks: Vec<Value> = found
            .iter()
            .map(|listed| {
                let mut value = bookmark_json(&listed.stored);
                value["item_id"] = json!(listed.item);
                value["title"] = json!(listed.item_title);
                value["member_name"] = json!(listed.member_name);
                value
            })
            .collect();
        Ok(json!({ "bookmarks": bookmarks }))
    }

    /// `community.wish_create` — wishes for something as this node's member: a
    /// new wish, with this member as its creator (D10).
    async fn wish_create(&self, params: WishCreate) -> Result<Value, Error> {
        let WishCreate {
            kind,
            title,
            authors,
            description,
        } = params;
        if title.trim().is_empty() {
            return Err(Error::invalid_params("a wish needs a title"));
        }
        let wish_id = WishId::random().map_err(|error| Error::failed(error.to_string()))?;
        let entry = Wish::new(WishFields {
            kind,
            title: Some(title),
            authors: (!authors.is_empty()).then_some(authors),
            description,
            created_at: Some(micros_since_epoch(SystemTime::now())),
            status: WishStatus::Open,
        })
        .map_err(|error| Error::invalid_params(error.to_string()))?;
        self.catalogue
            .wish(wish_id, &entry)
            .await
            .map_err(sync_error)?;
        Ok(json!({ "wish_id": wish_id }))
    }

    /// `community.wish_list` — every wish the read model holds, resolved
    /// (D10), newest first, with its comments; `status` keeps only the open
    /// or the fulfilled ones.
    async fn wish_list(&self, params: WishList) -> Result<Value, Error> {
        let mut wishes: Vec<(WishId, ResolvedWish, BTreeMap<MemberId, Comment>)> = self
            .store
            .all_wishes()
            .await
            .map_err(|error| Error::failed(error.to_string()))?
            .into_iter()
            .filter_map(|(id, stored)| {
                Some((id, ResolvedWish::resolve(&stored.entries)?, stored.comments))
            })
            .filter(|(_, wish, _)| match params.status {
                None => true,
                Some(WishListStatus::Open) => !wish.is_fulfilled(),
                Some(WishListStatus::Fulfilled) => wish.is_fulfilled(),
            })
            .collect();
        wishes.sort_by_key(|(_, wish, _)| Reverse(wish.wished.created_at));

        // Each fulfilling item's title, read once however many wishes it
        // answers; `None` for one the read model has not projected yet.
        let mut titles: BTreeMap<ItemId, Option<String>> = BTreeMap::new();
        for (_, wish, _) in &wishes {
            for item_id in wish.fulfilled_by.values() {
                if !titles.contains_key(item_id) {
                    let stored = self
                        .store
                        .item_fields(*item_id)
                        .await
                        .map_err(|error| Error::failed(error.to_string()))?;
                    titles.insert(*item_id, stored.and_then(|stored| stored.item.title));
                }
            }
        }
        let wishes: Vec<Value> = wishes
            .iter()
            .map(|(id, wish, comments)| wish_json(*id, wish, comments, &titles))
            .collect();
        Ok(json!({ "wishes": wishes }))
    }

    /// `community.wish_comment` — comments on a wish as this node's member,
    /// replacing its earlier comment, or clears it with `comment: null`.
    async fn wish_comment(&self, params: WishCommentParams) -> Result<Value, Error> {
        let WishCommentParams { wish_id, comment } = params;
        self.known_wish_entries(wish_id).await?;
        match comment {
            Some(comment) => self.catalogue.wish_comment(wish_id, &comment).await,
            None => self.catalogue.clear_wish_comment(wish_id).await,
        }
        .map_err(sync_error)?;
        Ok(json!({ "wish_id": wish_id }))
    }

    /// `community.wish_fulfill` — says, as this node's member, that `item_id`
    /// answers the wish. Written into this member's own entry, so a creator
    /// fulfilling their own wish keeps what they wished for.
    async fn wish_fulfill(&self, params: WishFulfill) -> Result<Value, Error> {
        let WishFulfill { wish_id, item_id } = params;
        let entries = self.known_wish_entries(wish_id).await?;
        self.refuse_unknown_item(item_id).await?;
        let mine = entries.get(&self.node.id()).map_or_else(
            || WishFields {
                kind: None,
                title: None,
                authors: None,
                description: None,
                created_at: Some(micros_since_epoch(SystemTime::now())),
                status: WishStatus::Open,
            },
            |entry| entry.fields().clone(),
        );
        let entry = Wish::new(WishFields {
            status: WishStatus::Fulfilled { item_id },
            ..mine
        })
        .map_err(|error| Error::invalid_params(error.to_string()))?;
        self.catalogue
            .wish(wish_id, &entry)
            .await
            .map_err(sync_error)?;
        Ok(json!({ "wish_id": wish_id, "item_id": item_id }))
    }

    /// Every member's entry for `wish_id` in this node's document, or a
    /// refusal if none of them says what is wished for.
    async fn known_wish_entries(&self, wish_id: WishId) -> Result<BTreeMap<MemberId, Wish>, Error> {
        let entries = self.catalogue.wishes(wish_id).await.map_err(sync_error)?;
        match ResolvedWish::resolve(&entries) {
            Some(_) => Ok(entries),
            None => Err(Error::failed(format!("no such wish: {wish_id}"))),
        }
    }

    /// This node's member's bookmark `id` on `item_id`, as the document holds
    /// it, or a refusal saying there is none.
    async fn own_bookmark(&self, item_id: ItemId, id: BookmarkId) -> Result<Bookmark, Error> {
        let me = self.node.id();
        self.catalogue
            .bookmarks(item_id)
            .await
            .map_err(sync_error)?
            .into_iter()
            .find(|read| read.member == me && read.id == id)
            .map(|read| read.bookmark)
            .ok_or_else(|| Error::failed(format!("you have no bookmark {id} on {item_id}")))
    }

    /// Refuses an item this node's document has no entry of: writing about it
    /// would describe an item nobody added.
    async fn refuse_unknown_item(&self, item_id: ItemId) -> Result<(), Error> {
        match self.catalogue.item(item_id).await.map_err(sync_error)? {
            Some(_) => Ok(()),
            None => Err(Error::failed(format!("no such item: {item_id}"))),
        }
    }

    /// `library.add` — hash a file set, store it as blobs, and write a
    /// catalogue entry for it (2b-2; §6.1's "v1 — exact" dedup).
    ///
    /// Every file is `role: content`: the files `item_id` fingerprints (§5.2,
    /// D4). An item already at that id keeps its metadata, so a worse guess at
    /// a title cannot replace a better one, and gains only the files it lacks.
    /// Two nodes adding the same files compute the same id, so they converge
    /// on one item without coordinating.
    ///
    /// Every file imported is kept as this node's own, whether or not the
    /// item was new: whoever adds files means to share them.
    async fn add(&self, params: Add) -> Result<Value, Error> {
        // Taken first, so a refused call still deletes its uploads, when
        // `_staged` is dropped.
        let (uploaded, _staged) = self.take_uploads(&params.uploads).await?;
        let (item, imported) = self
            .item_from_files(files_or_uploads(&params.files, &uploaded)?)
            .await?;
        self.check_room_for(&item).await?;
        let id = item.id;
        let answer = match self.catalogue.item(id).await.map_err(sync_error)? {
            Some(existing) => self.add_missing_files(item, &existing).await?,
            None => self.write_new_item(item, params).await?,
        };
        // After the write, so a refused or failed add leaves nothing kept.
        for file in &imported {
            self.blobs
                .keep(KeepReason::Own, id, file.hash)
                .await
                .map_err(net_error)?;
        }
        // The projection rechecked it before these tags, when it was not kept.
        self.recheck_holdings(id).await;
        self.make_room_for_add(id, &imported).await;
        Ok(answer)
    }

    /// Refuses `item` when keeping its files as own would grow own and custody
    /// past the storage limit. An add that grows them by nothing goes through,
    /// even when they are over it already.
    async fn check_room_for(&self, item: &Item) -> Result<(), Error> {
        let Some(limit) = self.storage.limit() else {
            return Ok(());
        };
        let kept = self.blobs.kept_blobs().await.map_err(net_error)?;
        let growth =
            kept.mandatory_growth(item.files.iter().map(|(hash, file)| (*hash, file.size)));
        let mandatory = kept.usage().mandatory_bytes() + growth;
        if growth > 0 && mandatory > limit.get() {
            return Err(Error::failed(format!(
                "adding these files would keep {mandatory} bytes this node cannot free, own and \
                 custody, over its storage limit of {limit} bytes"
            )));
        }
        Ok(())
    }

    /// Evicts what is cached to bring everything kept back under the limit,
    /// now that `imported` is kept as own. An add that would take own and
    /// custody past the limit was refused before it was written
    /// ([`Self::check_room_for`]), so this is short only when they were over
    /// it already. The add has happened by now, so a failure is logged rather
    /// than answered.
    async fn make_room_for_add(&self, item_id: ItemId, imported: &[ImportedFile]) {
        let files = imported.iter().map(|file| (file.hash, file.size));
        match self.storage.make_room(item_id, files).await {
            Ok(Room::Made) => {}
            Ok(Room::Short(short_by)) => {
                tracing::warn!(item = %item_id, short_by, "over the storage limit with the whole cache evicted");
            }
            Err(error) => {
                tracing::error!(item = %item_id, %error, "added, but could not make room for it; this node may stay over its storage limit");
            }
        }
    }

    /// Takes the uploads named: their paths, and the guards that delete them
    /// when dropped.
    async fn take_uploads(
        &self,
        uploads: &[UploadId],
    ) -> Result<(Vec<PathBuf>, Vec<Staged>), Error> {
        let mut paths = Vec::with_capacity(uploads.len());
        let mut staged = Vec::with_capacity(uploads.len());
        for upload in uploads {
            let (path, guard) = self
                .uploads
                .take(*upload)
                .await
                .map_err(|error| Error::invalid_params(error.to_string()))?;
            paths.push(path);
            staged.push(guard);
        }
        Ok((paths, staged))
    }

    /// An item of the files at `paths`, its id the fingerprint of what it
    /// holds, and the imported files, kept until they are tagged. Two
    /// different files under one name are refused here, while whoever named
    /// them can still rename one.
    async fn item_from_files(&self, paths: &[PathBuf]) -> Result<(Item, Vec<ImportedFile>), Error> {
        // Keyed on the blob, so the same file named twice is not a collision.
        let mut named: BTreeMap<String, &PathBuf> = BTreeMap::new();
        // A placeholder id until every file is hashed.
        let mut item = Item::new(ItemId::from_bytes([0; 32]));
        let mut imported = Vec::with_capacity(paths.len());
        for path in paths {
            let (file, record) = self.hash_file(path).await?;
            let hash = file.hash;
            imported.push(file);
            if !item.files.contains_key(&hash)
                && let Some(first) = named.get(&record.filename)
            {
                return Err(Error::invalid_params(format!(
                    "{} and {} are two different files both called {} — rename one, since an \
                     item's files have to be tellable apart by the names they are stored under",
                    first.display(),
                    path.display(),
                    record.filename,
                )));
            }
            named.insert(record.filename.clone(), path);
            item.files.insert(hash, record);
        }
        item.id = item
            .fingerprint()
            .expect("`files` is non-empty, and only `role: content` files are ever inserted here");
        Ok((item, imported))
    }

    /// Writes the files of `item` that `existing`, at the same id, lacks, and
    /// leaves its metadata alone. Usually there are none: only an earlier
    /// write interrupted partway leaves an item without all of its files.
    async fn add_missing_files(&self, item: Item, existing: &Item) -> Result<Value, Error> {
        let id = item.id;
        let missing: BTreeMap<ContentHash, FileRecord> = item
            .files
            .into_iter()
            .filter(|(hash, _)| !existing.files.contains_key(hash))
            .collect();
        let contributed: Vec<ContentHash> = missing.keys().copied().collect();
        if !missing.is_empty() {
            self.catalogue
                .write(&Item {
                    files: missing,
                    ..Item::new(id)
                })
                .await
                .map_err(sync_error)?;
        }
        Ok(json!({
            "item_id": id,
            "created": false,
            "title": existing.title,
            "contributed_files": contributed,
        }))
    }

    /// Writes `item` as a new item, with every field `params` gives.
    async fn write_new_item(&self, mut item: Item, params: Add) -> Result<Value, Error> {
        item.kind = Some(params.kind);
        item.title = params.title;
        item.authors = (!params.authors.is_empty()).then_some(params.authors);
        item.genres = (!params.genres.is_empty()).then_some(params.genres);
        item.series = params.series;
        item.year = params.year;
        item.lang = params.lang;
        item.description = params.description;
        self.catalogue.write(&item).await.map_err(sync_error)?;
        Ok(json!({
            "item_id": item.id,
            "created": true,
            "title": item.title,
            "contributed_files": Vec::<ContentHash>::new(),
        }))
    }

    /// `library.download` — fetch an item's files from whoever has them, and
    /// write them out to a directory (2b-3).
    ///
    /// Everything that can be the caller's mistake is refused before the
    /// answer. The fetching then runs as a task that `library.task` reports
    /// on, so a page reload does not end it (phase 3's D4). `file` names a
    /// content hash, the key of an item's files. No deadline: a provider that
    /// cannot be reached fails rather than hangs.
    ///
    /// Nothing is overwritten, and a failed download leaves the files it
    /// finished: asking again fetches only what is missing.
    async fn download(&self, params: Download) -> Result<Value, Error> {
        let dest = self.download_destination(params.dest)?;
        // An item the read model does not have yet is one this node has not
        // synced, so "no such item" is the truth about this node.
        let item = self
            .store
            .item(params.item_id)
            .await
            .map_err(|error| Error::failed(error.to_string()))?
            .ok_or_else(|| Error::failed(format!("no such item: {}", params.item_id)))?
            .item;
        let wanted = files_to_download(params.item_id, item.files, params.file)?;
        let targets = targets_in(&dest, wanted).await?;
        self.store
            .mark_used(params.item_id, SystemTime::now())
            .await
            .map_err(query_error)?;
        self.make_room_for_download(params.item_id, &targets)
            .await?;

        let bytes = targets.iter().map(|target| target.record.size).sum();
        let task = self.tasks.start_download(
            params.item_id,
            item.title.clone(),
            targets.len() as u64,
            bytes,
        );
        let answer = json!({
            "task_id": task.id(),
            "item_id": params.item_id,
            "title": item.title,
            "files": targets
                .iter()
                .map(|target| json!({
                    "file": target.hash,
                    "filename": target.record.filename,
                    "path": target.path,
                }))
                .collect::<Vec<_>>(),
        });
        let providers = self.providers_of(&params.item_id);
        self.finish_download_in_background(params.item_id, targets, providers, task);
        Ok(answer)
    }

    /// Makes room for the files of `targets` not at their destination yet,
    /// and refuses the download when even the whole cache is not room
    /// enough. The cache is evicted all the same, so asking again after
    /// freeing some space finds it free.
    async fn make_room_for_download(
        &self,
        item_id: ItemId,
        targets: &[Target],
    ) -> Result<(), Error> {
        let files = targets
            .iter()
            .filter(|target| !target.in_place)
            .map(|target| (target.hash, target.record.size));
        let room = self
            .storage
            .make_room(item_id, files)
            .await
            .map_err(storage_error)?;
        if let Room::Short(short_by) = room {
            return Err(Error::failed(format!(
                "no room for this download: it would take this node {short_by} bytes over its \
                 storage limit even with the whole cache evicted"
            )));
        }
        Ok(())
    }

    /// Where a download writes: the directory named, which has to exist
    /// already, since creating it would turn a mistyped path into a new
    /// directory; or the node's own, created if need be.
    fn download_destination(&self, dest: Option<PathBuf>) -> Result<PathBuf, Error> {
        let dest = match dest {
            Some(dest) => dest,
            None => {
                std::fs::create_dir_all(&self.downloads).map_err(|error| {
                    Error::failed(format!("{}: {error}", self.downloads.display()))
                })?;
                self.downloads.clone()
            }
        };
        if !dest.is_dir() {
            return Err(Error::invalid_params(format!(
                "{}: not a directory to write files into",
                dest.display()
            )));
        }
        Ok(dest)
    }

    /// Every other member, in the order to ask them for `item_id`.
    fn providers_of(&self, item_id: &ItemId) -> Vec<MemberId> {
        let me = self.node.id();
        self.seen().ask_order(
            item_id,
            self.node
                .membership()
                .members()
                .map(|record| record.member_id)
                .filter(|member| *member != me),
        )
    }

    /// Fetches and writes `targets` for `task` in the background, then
    /// rechecks whether this node holds the item before the task says it has
    /// finished, so whoever waits on it finds the item held.
    fn finish_download_in_background(
        &self,
        item_id: ItemId,
        targets: Vec<Target>,
        providers: Vec<MemberId>,
        mut task: tasks::Download,
    ) {
        // A clone, because the task outlives this call; every field is a
        // handle onto something shared.
        let api = self.clone();
        tokio::spawn(async move {
            match api.fetch_all(item_id, targets, &providers, &mut task).await {
                Ok(files) => {
                    api.recheck_holdings(item_id).await;
                    task.finish(files);
                }
                Err(error) => task.fail(error.message),
            }
        });
    }

    /// Rechecks whether this node holds `item_id`. A download is the one way
    /// content arrives that the document never hears of. The item is read
    /// again, since a file may have been added to it during the download.
    async fn recheck_holdings(&self, item_id: ItemId) {
        let rechecked = async {
            let Some(item) = self.catalogue.item(item_id).await? else {
                return Ok(false);
            };
            self.catalogue.holdings().recheck(&item).await
        };
        if let Err(error) = rechecked.await {
            tracing::warn!(item = %item_id, %error, "could not work out whether this node now holds an item");
        }
    }

    /// `library.download`'s background half: fetches what is not here yet and
    /// writes every file out, reporting progress to `task` as it goes.
    ///
    /// Progress is counted across all of the files, so a bar fills once per
    /// download rather than once per file: a file already here counts as done
    /// the moment it is found, and one being fetched counts what it has so far.
    /// A file counts towards `files_done` once it is written out — or at once,
    /// if it was at its destination already.
    ///
    /// Every file taken from the store is kept as cache, tagged before it is
    /// fetched so its bytes are kept from the moment they land.
    async fn fetch_all(
        &self,
        item_id: ItemId,
        targets: Vec<Target>,
        providers: &[MemberId],
        task: &mut tasks::Download,
    ) -> Result<Vec<Downloaded>, Error> {
        let mut files = Vec::with_capacity(targets.len());
        let mut refresh = OneRefresh::of(self, providers);
        let mut done = 0;
        for Target {
            hash,
            record,
            path,
            in_place,
        } in targets
        {
            if in_place {
                done += record.size;
                task.file_written(done);
                files.push(Downloaded {
                    file: hash,
                    filename: record.filename,
                    path,
                    from: Source::Destination,
                });
                continue;
            }
            self.blobs
                .keep(KeepReason::Cache, item_id, hash)
                .await
                .map_err(net_error)?;
            // Asked before the network is: this node may be the one that
            // added the item, or may have downloaded it before, and in a
            // group of one there is nobody to ask at all.
            let already_here = self.blobs.has(hash).await.map_err(net_error)?;
            if !already_here && let Err(failure) = self.fetch(hash, providers, done, task).await {
                // A fetch that failed is the first evidence that this node's
                // idea of where everybody is might be out of date, so it is
                // worth one question and one more attempt. The second error
                // is the one that surfaces: it is the more recent account of
                // the same thing, and the refresh happened in between.
                if !refresh.spend().await {
                    return Err(net_error(failure));
                }
                self.fetch(hash, providers, done, task)
                    .await
                    .map_err(net_error)?;
            }
            done += record.size;
            self.blobs.export(hash, &path).await.map_err(net_error)?;
            task.file_written(done);
            files.push(Downloaded {
                file: hash,
                filename: record.filename,
                path,
                from: if already_here {
                    Source::Store
                } else {
                    Source::Network
                },
            });
        }
        Ok(files)
    }

    /// Fetches one file, reporting to `task` how far the whole download has
    /// got: `before` bytes from the files already done, plus this one's.
    async fn fetch(
        &self,
        hash: ContentHash,
        providers: &[MemberId],
        before: u64,
        task: &mut tasks::Download,
    ) -> Result<(), NetError> {
        self.blobs
            .fetch_with_progress(hash, providers.to_vec(), |got| task.progress(before + got))
            .await
    }

    /// `library.task` — how a download started by `library.download` is
    /// getting on, or how it ended.
    ///
    /// A task this node never started, or finished long enough ago to have
    /// been pruned, is refused alike: either way there is nothing to report.
    fn task(&self, params: TaskParams) -> Result<Value, Error> {
        let state = self
            .tasks
            .get(params.task_id)
            .ok_or_else(|| Error::failed(format!("no such task: {}", params.task_id)))?;
        serde_json::to_value(state).map_err(|error| Error::failed(error.to_string()))
    }

    /// Asks a core node where the providers are.
    ///
    /// Called by [`OneRefresh`], which is what holds it to once per
    /// `library.download` and to only after a fetch has already failed.
    ///
    /// **The gap this closes, found by running §9's own acceptance.** A
    /// follower asks the core group for the directory exactly once, at
    /// startup, and latches on whatever came back. Two followers that start
    /// close together lose that race in one direction: the one already in the
    /// gossip swarm hears the other announce and learns it, while the one
    /// that arrives later hears nothing, because an announcement is an event
    /// and nothing repeats it for a newcomer. Nothing then asks again, so a
    /// member who holds the only copy of a file can stay unreachable for as
    /// long as both nodes run.
    ///
    /// **How often that really happens is not settled**, and the honest
    /// account is worth more here than a confident one. With a core node up,
    /// the startup ask usually covers it — a follower that arrives late gets
    /// the earlier one's address from the directory, and a peer that moves
    /// re-announces to everyone already in the swarm. Both were re-run by hand
    /// while this was moved onto the failure path, and both reached the holder
    /// on the first try. What is certain is the shape of the hole: one ask,
    /// at startup, with nothing that asks again, and a restarted core node
    /// answering it out of a directory that came back empty. This closes that
    /// without needing to know how wide it is, because on the path it now sits
    /// on it costs nothing until something has already gone wrong.
    ///
    /// **Why here rather than deeper down.** The phase plan carried this out
    /// of phase 2 saying what was missing was *a single point where "we had
    /// no address for this member" is observable* — a dial failure surfaces
    /// inside iroh, inside iroh-docs' downloader and at each protocol client,
    /// and none of them agree on what the phrase means. This method is such a
    /// point: it chose the provider list itself, it can ask the directory
    /// which of them it cannot place, and it is about to fail in front of
    /// somebody if it goes ahead regardless.
    ///
    /// **The trigger is a fetch that failed, not "we hold no address for this
    /// member".** The second was written first and never fired once: the
    /// common case is a peer that restarted on a new port, where the address
    /// lookup answers perfectly well — with somewhere nobody is listening any
    /// more. A node that cannot be placed at all and a node placed wrongly
    /// fail identically from here, and only the fetch knows the difference.
    /// It also means the happy path costs nothing: a download whose providers
    /// are all reachable never asks anybody anything.
    ///
    /// **Best effort, and deliberately quiet about its own failure.** A core
    /// node that is down or has nothing to say leaves the download exactly
    /// where it would have been, and the *fetch's* error is the better one to
    /// report — "could not fetch X from any of the offered providers" tells
    /// somebody who asked for a file more than "could not ask about an
    /// address" does.
    ///
    /// **What it cannot do**, said here because the acceptance run found it:
    /// the directory lives on core nodes, so a group whose core is entirely
    /// unreachable cannot learn anything new about where anybody is. A
    /// follower holding a stale address for a peer, with no core node to ask,
    /// stays stuck until one comes back. That is a bigger question than this
    /// method — see delta P2-25.
    async fn find_the_providers(&self, providers: &[MemberId]) -> bool {
        // Counted for the log rather than to decide anything, and it earns
        // that: it separates the two failures that look alike from here.
        // `unplaced=0` says every provider resolved and the fetch still
        // failed, so an address is stale or a holder is down; a non-zero
        // count says this node simply never learned where somebody is. They
        // want different things done about them, and the distinction cost a
        // day to establish the first time.
        let unplaced = providers
            .iter()
            .filter(|member| self.node.known_addresses().address_of(**member).is_none())
            .count();
        tracing::debug!(
            unplaced,
            of = providers.len(),
            "a fetch failed; asking a core node where the providers are"
        );

        let answered = self.node.refresh_addresses().await;
        if !answered {
            tracing::debug!("no core node said where the providers are");
        }
        answered
    }

    /// Hashes and stores one local file for `library.add`, and describes it
    /// the way §5.2's per-file record does.
    ///
    /// **`format` is the file's extension, lower-cased.** Sniffing the actual
    /// container would be more honest, but nothing here reads file contents
    /// beyond hashing them, and an operator naming a `.epub` file is not a
    /// case worth a parsing dependency over. `seq`, `disc` and `duration` are
    /// left `None` for the same reason `library.add` takes no per-file
    /// arguments for them yet — chaptered audiobooks are real, but nothing
    /// calls this with that shape today.
    async fn hash_file(&self, path: &std::path::Path) -> Result<(ImportedFile, FileRecord), Error> {
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| Error::invalid_params(format!("{}: not a file path", path.display())))?
            .to_owned();
        let format = path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(str::to_lowercase)
            .ok_or_else(|| {
                Error::invalid_params(format!(
                    "{}: no file extension to record as its format",
                    path.display()
                ))
            })?;

        let file = self
            .catalogue
            .add_file(path)
            .await
            .map_err(|error| match error {
                SyncError::LocalFile { .. } => Error::invalid_params(error.to_string()),
                other => Error::failed(other.to_string()),
            })?;

        let size = file.size;
        Ok((
            file,
            FileRecord {
                role: FileRole::Content,
                format,
                size,
                filename,
                seq: None,
                disc: None,
                title: None,
                duration: None,
            },
        ))
    }

    /// Commits an event, and reports what became of it.
    ///
    /// **`applied` is the field that matters**, and the reason this returns
    /// more than it used to: since 2.2-1 a committed proposal may be waiting
    /// for core approvals rather than in effect, and a caller told only
    /// "committed" would report success for something that has not happened
    /// yet. The entry's own log index answers it — a proposal still in
    /// `pending` under that index is waiting — which is exact where matching on
    /// the event's content would not be, since two proposals can say the same
    /// thing.
    ///
    /// `changed_at` stays for the caller about to propose again: it is the view
    /// their next proposal will be checked against.
    async fn propose(&self, event: MembershipEvent) -> Result<Value, Error> {
        let proposal = self.commit(event).await?;
        Ok(self.outcome(proposal))
    }

    /// Commits an event, answering with the log index it was applied at.
    async fn commit(&self, event: MembershipEvent) -> Result<u64, Error> {
        self.node
            .propose(event, &self.secret)
            .await
            .map_err(|error| Error::failed(error.to_string()))
    }

    /// Where the proposal at `proposal` now stands.
    ///
    /// One function for both the caller who just made it and the caller who
    /// just approved it, because they are asking the same question — has this
    /// change happened yet, and if not what is it waiting for — and two
    /// implementations of it would be free to disagree.
    ///
    /// Reads this node's membership, which is only an answer because
    /// `MembershipNode::propose` returns once the entry is applied *here*.
    /// Before that, a node that was not the leader found nothing pending under
    /// the index and reported a waiting proposal as applied.
    fn outcome(&self, proposal: u64) -> Value {
        let membership = self.node.membership();
        let waiting = membership
            .pending()
            .find(|(index, _)| *index == proposal)
            .map(|(_, entry)| {
                json!({
                    "approvals": membership.approvals_counting(entry).count(),
                    "needed": membership.approvals_needed(entry.event()),
                })
            });

        json!({
            "changed_at": membership.changed_at(),
            "proposal": proposal,
            "applied": waiting.is_none(),
            "waiting": waiting,
        })
    }
}

/// `node.status`'s view of the catalogue's swarm (C14): who this node is a
/// gossip neighbour of, and how its last sync round with each peer went.
///
/// Times in microseconds since the epoch, like `last_modified`. A clock that
/// reads before 1970 gives 0 rather than an error: it is a display, and the
/// rest of the status is worth more than refusing over it.
fn sync_block(sync: &SyncState) -> Value {
    let last_sync: Vec<Value> = sync
        .last_sync
        .iter()
        .map(|(member, last)| {
            let finished = micros_since_epoch(last.finished);
            json!({ "member": member, "finished": finished, "ok": last.ok })
        })
        .collect();
    json!({ "neighbours": sync.neighbours, "last_sync": last_sync })
}

/// `time` in microseconds since the epoch, as every time in this API is. A
/// clock before 1970 gives 0 rather than an error.
fn micros_since_epoch(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).map_or(0, |since| {
        u64::try_from(since.as_micros()).unwrap_or(u64::MAX)
    })
}

/// One bookmark as `library.item` and `community.bookmarks` give it.
fn bookmark_json(stored: &StoredBookmark) -> Value {
    json!({
        "member": stored.member,
        "bookmark_id": stored.id,
        "position": stored.bookmark.position(),
        "note": stored.bookmark.note(),
        "created_at": stored.bookmark.created_at(),
        "last_modified": stored.last_modified,
    })
}

/// One wish as `community.wish_list` gives it, each fulfilling item with its
/// title from `titles`.
fn wish_json(
    id: WishId,
    wish: &ResolvedWish,
    comments: &BTreeMap<MemberId, Comment>,
    titles: &BTreeMap<ItemId, Option<String>>,
) -> Value {
    let wished = &wish.wished;
    json!({
        "wish_id": id,
        "creator": wish.creator,
        "kind": wished.kind,
        "title": wished.title,
        "authors": wished.authors,
        "description": wished.description,
        "created_at": wished.created_at,
        "status": if wish.is_fulfilled() { "fulfilled" } else { "open" },
        "fulfilled_by": wish
            .fulfilled_by
            .iter()
            .map(|(member, item_id)| {
                let title = titles.get(item_id).cloned().flatten();
                json!({ "member": member, "item_id": item_id, "title": title })
            })
            .collect::<Vec<_>>(),
        "comments": comments
            .iter()
            .map(|(member, comment)| json!({ "member": member, "comment": comment }))
            .collect::<Vec<_>>(),
    })
}

/// A search or list hit: the summary, `held` and `providers`.
fn hit(stored: &StoredItem, seen: &Seen<'_>) -> Value {
    let mut hit = summary(stored);
    hit["held"] = json!(seen.held(&stored.item.id));
    hit["providers"] = json!(seen.providers(&stored.item.id));
    hit
}

/// The fields shown for a search hit — a summary, not `library.item`'s full
/// record. Also that record's starting point, patched with the fields a
/// summary leaves out, so the two answers cannot say different things about
/// the fields they share.
fn summary(stored: &StoredItem) -> Value {
    let item = &stored.item;
    json!({
        "item_id": item.id,
        "kind": item.kind,
        "title": item.title,
        "authors": item.authors,
        "genres": item.genres,
        "series": item.series,
        "year": item.year,
    })
}

/// `library.item`'s `ratings`: the average of current members' ratings, how
/// many there are, and this node's member's own.
fn ratings_summary(ratings: &BTreeMap<MemberId, Rating>, me: MemberId) -> Value {
    let (total, count) = ratings
        .values()
        .fold((0_u32, 0_u32), |(total, count), rating| {
            (total + u32::from(rating.get()), count + 1)
        });
    let average = (count > 0).then(|| f64::from(total) / f64::from(count));
    json!({ "average": average, "count": count, "mine": ratings.get(&me) })
}

/// A malformed query is the caller's mistake; anything else out of
/// `distlib-store` is this method failing for its own reasons.
fn query_error(error: StoreError) -> Error {
    match error {
        StoreError::Query { .. } => Error::invalid_params(error.to_string()),
        other => Error::failed(other.to_string()),
    }
}

/// The files `library.add` was given: paths on this node, or uploads, not
/// both.
fn files_or_uploads<'a>(
    files: &'a [PathBuf],
    uploaded: &'a [PathBuf],
) -> Result<&'a [PathBuf], Error> {
    match (files.is_empty(), uploaded.is_empty()) {
        (false, true) => Ok(files),
        (true, false) => Ok(uploaded),
        (true, true) => Err(Error::invalid_params("library.add needs at least one file")),
        (false, false) => Err(Error::invalid_params(
            "library.add takes files or uploads, not both",
        )),
    }
}

/// The files of `item_id` a download asks for: the one `file` names, or all.
fn files_to_download(
    item_id: ItemId,
    files: BTreeMap<ContentHash, FileRecord>,
    file: Option<ContentHash>,
) -> Result<Vec<(ContentHash, FileRecord)>, Error> {
    let wanted: Vec<(ContentHash, FileRecord)> = match file {
        Some(hash) => {
            let record = files
                .get(&hash)
                .cloned()
                .ok_or_else(|| Error::invalid_params(format!("{item_id} has no file {hash}")))?;
            vec![(hash, record)]
        }
        None => files.into_iter().collect(),
    };
    if wanted.is_empty() {
        return Err(Error::failed(format!(
            "{item_id}: this item has no files yet"
        )));
    }
    Ok(wanted)
}

/// Where in `dest` each file goes, checked before anything is fetched.
///
/// Nothing is overwritten. A file already at its target is refused unless it
/// is this file byte for byte, which is what a failed earlier download leaves.
/// Two files on one path are refused too: `library.add` refuses them, but
/// another member's build may have written them, and `file` picks one. A
/// filename comes from the shared document, so only its last component is
/// used.
async fn targets_in(
    dest: &Path,
    wanted: Vec<(ContentHash, FileRecord)>,
) -> Result<Vec<Target>, Error> {
    let mut taken: BTreeMap<PathBuf, ContentHash> = BTreeMap::new();
    let mut targets: Vec<Target> = Vec::with_capacity(wanted.len());
    for (hash, record) in wanted {
        let name = Path::new(&record.filename).file_name().ok_or_else(|| {
            Error::failed(format!(
                "{hash}: {:?} is not a filename this can write",
                record.filename
            ))
        })?;
        let target = dest.join(name);
        if let Some(other) = taken.get(&target) {
            return Err(Error::failed(format!(
                "{} and {other} are both called {}; download them one at a time with `file`",
                hash,
                target.display()
            )));
        }
        let in_place = target.exists();
        if in_place && !already_there(&target, hash, record.size).await? {
            return Err(Error::failed(format!(
                "{} already exists and is not this file; move or delete it to download \
                 this file again",
                target.display()
            )));
        }
        taken.insert(target.clone(), hash);
        targets.push(Target {
            hash,
            record,
            path: target,
            in_place,
        });
    }
    Ok(targets)
}

/// One file `library.download` is to write, worked out and checked before the
/// task starts.
struct Target {
    hash: ContentHash,
    record: FileRecord,
    path: PathBuf,
    /// Already at `path`, byte for byte: nothing to fetch or write.
    in_place: bool,
}

/// Whether `path` holds exactly the `size` bytes `hash` names.
///
/// A content hash is iroh-blobs' hash, and that is the plain BLAKE3 hash of
/// the bytes — its tree only shapes the verified-streaming outboard, not the
/// root — so the file is hashed as it stands.
/// `downloading_again_takes_only_what_is_missing` is what would notice if
/// that stopped being true. On a blocking thread: it reads the whole
/// file, and a multi-gigabyte one takes seconds.
async fn already_there(path: &Path, hash: ContentHash, size: u64) -> Result<bool, Error> {
    let owned = path.to_owned();
    let same = tokio::task::spawn_blocking(move || -> std::io::Result<bool> {
        let metadata = std::fs::metadata(&owned)?;
        if !metadata.is_file() || metadata.len() != size {
            return Ok(false);
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update_reader(std::fs::File::open(&owned)?)?;
        Ok(hasher.finalize().as_bytes() == hash.as_bytes())
    })
    .await
    .map_err(|error| Error::failed(error.to_string()))?;
    same.map_err(|error| Error::failed(format!("{}: {error}", path.display())))
}

/// The single directory refresh a `library.download` is allowed, and whether
/// it has been used.
///
/// Exists so that "once per call" is a property of a thing with a name rather
/// than a flag passed around and checked. A download offers every member as a
/// provider, so a fetch that fails is the first evidence that this node's idea
/// of where anybody is may be out of date — worth one question to a core node,
/// and worth it for whichever file fails first rather than only for the first
/// file, which is why the state outlives any one fetch.
struct OneRefresh<'a> {
    api: &'a Api,
    providers: &'a [MemberId],
    spent: bool,
}

impl<'a> OneRefresh<'a> {
    fn of(api: &'a Api, providers: &'a [MemberId]) -> Self {
        Self {
            api,
            providers,
            spent: false,
        }
    }

    /// Spends the refresh, and answers whether the fetch that just failed is
    /// worth trying again.
    ///
    /// `false` once it has been spent, whatever the answer was that time: the
    /// directory is then as fresh as this node can make it, so a fetch failing
    /// afterwards is failing for a reason no amount of asking will move. That
    /// is also what keeps this from becoming the thing the phase plan warned
    /// against — a node answering every transient failure with an RPC.
    async fn spend(&mut self) -> bool {
        !std::mem::replace(&mut self.spent, true)
            && self.api.find_the_providers(self.providers).await
    }
}

/// Every `SyncError` a catalogue read or write can fail with is this method
/// failing for its own reasons — none of them are the caller's mistake the
/// way an invalid file path is, which [`Api::hash_file`] reports separately.
fn sync_error(error: SyncError) -> Error {
    Error::failed(error.to_string())
}

/// Every `NetError` `library.download` can meet is this method failing for
/// its own reasons rather than the caller's mistake: the two the caller could
/// have made — an item that is not here, a destination that is not a
/// directory — are both refused before [`Blobs`] is touched at all.
fn net_error(error: NetError) -> Error {
    Error::failed(error.to_string())
}

fn storage_error(error: StorageError) -> Error {
    Error::failed(error.to_string())
}

/// A one-line description of a proposal, for an operator deciding about it.
///
/// Rendered here rather than at the CLI because the API is the surface a
/// caller writes against, and a caller that had to match on the event shape to
/// print it would be reimplementing this.
///
/// Takes the membership because one event cannot be read without it.
/// [`MembershipEvent::CoreGroupChanged`] carries the whole desired core group
/// rather than a delta (P1-23), so printing its contents tells an approver who
/// would be left and not who would go — and a demotion is the only core-group
/// change that ever waits for approval, which makes that the one case this has
/// to get right. Diffed against the core group *as it stands now*, which is
/// also what the fold will compare against if the next approval decides it.
fn describe(event: &MembershipEvent, membership: &MembershipState) -> String {
    match event {
        MembershipEvent::MemberAdded { member } => {
            format!("admit {} ({})", member.member_id, member.display_name)
        }
        MembershipEvent::MemberExpelled { member, reason } => {
            format!("expel {member}: {reason}")
        }
        MembershipEvent::CoreGroupChanged { core } => describe_core(core, membership),
        MembershipEvent::PledgeChanged {
            member,
            pledge_bytes,
        } => format!("set the pledge of {member} to {pledge_bytes} bytes"),
        // Neither is ever a pending proposal — both are dispatched by the fold
        // rather than held — so this is unreachable rather than a real case.
        MembershipEvent::GroupFounded { group_id, .. } => format!("found group {group_id}"),
        MembershipEvent::Approved { proposal } => format!("approve {proposal}"),
        MembershipEvent::Withdrawn { proposal } => format!("withdraw {proposal}"),
    }
}

/// What a proposed core group would change, said as changes.
///
/// Three kinds, and all three are listed rather than only the first, because a
/// map can say more than one thing at once and an approver agreeing to "drop
/// bob" should not find they also agreed to move carol. A move carries the
/// address it would move them to for the same reason: it is the whole content
/// of that change, and a line saying only "move carol" asks somebody to agree
/// to something it has not told them.
///
/// Falls back to naming the whole group when it would change nothing — which is
/// not reachable through `group.propose_core`, since that refuses a no-op, but
/// is reachable by anybody proposing the event directly, and "" is not an
/// answer.
fn describe_core(proposed: &[(MemberId, NodeAddr)], membership: &MembershipState) -> String {
    let now = membership.core();
    let wanted: BTreeMap<MemberId, &NodeAddr> = proposed
        .iter()
        .map(|(member, addr)| (*member, addr))
        .collect();

    let mut changes: Vec<String> = Vec::new();
    changes.extend(
        now.keys()
            .filter(|member| !wanted.contains_key(member))
            .map(|member| format!("drop {member} from the core group")),
    );
    changes.extend(
        wanted
            .iter()
            .filter_map(|(member, addr)| match now.get(member) {
                None => Some(format!("add {member} to the core group")),
                Some(was) if was != *addr => Some(format!(
                    "move {member} to {}",
                    addr.direct
                        .iter()
                        .map(ToString::to_string)
                        .chain(addr.relay.clone())
                        .collect::<Vec<_>>()
                        .join(", ")
                )),
                Some(_) => None,
            }),
    );

    if changes.is_empty() {
        return format!("leave the core group at its {} members", now.len());
    }
    changes.join(", ")
}

/// `deny_unknown_fields` throughout: a caller passing a parameter a method does
/// not have has misunderstood something, and silence would let them believe it
/// took effect. `group.pledge_set` is the sharp case — it takes no `member`,
/// because a pledge belongs to whoever sets it, and quietly ignoring one would
/// look exactly like setting somebody else's.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProposeAdd {
    member: MemberId,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProposeExpel {
    member: MemberId,
    reason: String,
}

/// What `group.propose_core` is asked to do: one member, and which of the two
/// things should become of them.
///
/// **Tagged, rather than inferred from whether an address was given.** The
/// shape this replaced was a single `addr: Option<NodeAddr>` where `null` meant
/// "drop them" — one field answering two unrelated questions, *where are they*
/// and *should they vote*, so the difference between moving a node and demoting
/// it was a value rather than a word. It also needed a custom deserialiser to
/// stop serde reading a **missing** `addr` as `None`, which is to say: a
/// forgotten field would have demoted a voter, and the only thing standing in
/// the way was a helper somebody had to remember to keep. A tag makes that
/// unrepresentable instead of guarded against, and it reads the same way the
/// two CLI verbs do.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "change", rename_all = "snake_case", deny_unknown_fields)]
enum ProposeCore {
    /// Where to reach `member` once they vote — moving them if they are
    /// already a core node, adding them if they are not.
    Set { member: MemberId, addr: NodeAddr },

    /// Stop counting `member` as a voter. They stay a member.
    Remove { member: MemberId },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PledgeSet {
    bytes: u64,
}

/// What `group.approve` and `group.withdraw` name: the log index a proposal was
/// made at, which is what `group.pending` reports and what the log itself
/// speaks in.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Proposal {
    proposal: u64,
}

/// `library.search`'s params.
///
/// **Flat, where §7.1's own sketch nests `page`** — `{query,
/// filters{type,genre,lang,author,series}, page}`. The plan calls the whole
/// `library.*` group "a sketch" and leaves the request and response shapes to
/// be filled in, so `limit` follows this file's existing style
/// (`Proposal`, `PledgeSet`) rather than adding a nested object this crate has
/// no other one of.
///
/// **`filters` is not implemented** (P2-21). `authors`, `genres` and `series`
/// are already free-text fields a query can point at directly —
/// `authors:herbert` is valid tantivy syntax today — so a `filters` object
/// would either duplicate that or paper over `kind` and `lang`, which tantivy
/// never indexes at all: they live in SQLite only, and filtering by them needs
/// a real design, not a parameter nobody reads. **`offset` is** — 3a-6's
/// browse page is the caller phase 2 was waiting for.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Search {
    query: String,
    #[serde(default)]
    offset: usize,
    #[serde(default = "default_page_limit")]
    limit: usize,
}

/// `library.list`'s params.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct List {
    #[serde(default)]
    offset: usize,
    #[serde(default = "default_page_limit")]
    limit: usize,
}

/// A page without an explicit `limit`. Small enough to read in one screen,
/// generous enough that "did my one test item show up" never needs one.
fn default_page_limit() -> usize {
    20
}

/// `library.item`'s params.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ItemParams {
    item_id: ItemId,
}

/// `library.add`'s params.
///
/// `files` are local paths on the machine this node's API runs on — the
/// listener is loopback and token-gated (§7.1, P1-25), so "local to the
/// caller" and "local to this node" are the same machine. Every one is
/// `role: content`; see [`Api::add`] for why the surface stops there for now.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Add {
    kind: ItemKind,
    /// Paths on the node's machine — the CLI's door.
    #[serde(default)]
    files: Vec<PathBuf>,
    /// What `POST /upload` answered with — the browser's. Exactly one of the
    /// two is given.
    #[serde(default)]
    uploads: Vec<UploadId>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    authors: Vec<String>,
    #[serde(default)]
    genres: Vec<String>,
    #[serde(default)]
    series: Option<Series>,
    #[serde(default)]
    year: Option<i32>,
    #[serde(default)]
    lang: Option<String>,
    #[serde(default)]
    description: Option<String>,
}

/// `library.edit_metadata`'s params.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct EditMetadata {
    item_id: ItemId,
    fields: ItemFields,
}

/// `community.rate`'s params.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Rate {
    item_id: ItemId,
    /// Required: `null` clears. Serde would otherwise read a missing field
    /// as `null`, and a call that left it out would clear the rating.
    #[serde(deserialize_with = "Option::deserialize")]
    rating: Option<Rating>,
}

/// `community.review`'s params.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReviewParams {
    item_id: ItemId,
    /// Required: `null` clears, as [`Rate::rating`].
    #[serde(deserialize_with = "Option::deserialize")]
    review: Option<Review>,
}

/// `community.bookmark`'s params.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BookmarkParams {
    item_id: ItemId,
    /// One of this member's bookmarks to edit; a new one when left out.
    #[serde(default)]
    bookmark_id: Option<BookmarkId>,
    position: String,
    #[serde(default)]
    note: String,
}

/// `community.bookmark_delete`'s params.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BookmarkDelete {
    item_id: ItemId,
    bookmark_id: BookmarkId,
}

/// `community.bookmarks`' params: each one given narrows the list.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Bookmarks {
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    member: Option<MemberId>,
    #[serde(default)]
    item_id: Option<ItemId>,
}

/// `community.wish_create`'s params.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WishCreate {
    #[serde(default)]
    kind: Option<ItemKind>,
    title: String,
    #[serde(default)]
    authors: Vec<String>,
    #[serde(default)]
    description: Option<String>,
}

/// `community.wish_list`'s params.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WishList {
    #[serde(default)]
    status: Option<WishListStatus>,
}

/// Which wishes `community.wish_list` keeps.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum WishListStatus {
    Open,
    Fulfilled,
}

/// `community.wish_comment`'s params.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WishCommentParams {
    wish_id: WishId,
    /// Required: `null` clears, as [`Rate::rating`].
    #[serde(deserialize_with = "Option::deserialize")]
    comment: Option<Comment>,
}

/// `community.wish_fulfill`'s params.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WishFulfill {
    wish_id: WishId,
    item_id: ItemId,
}

/// `library.download`'s params.
///
/// `dest` is a directory on the machine this node's API runs on, for the same
/// reason `library.add`'s `files` are local to it: the listener is loopback
/// and token-gated (§7.1, P1-25), so the caller's machine and the node's are
/// the same one. It must already exist — a download is not a reason to
/// create a directory tree somebody may have typed wrong.
///
/// `file` picks one of the item's files by content hash; left out, every file
/// the item has is downloaded. See [`Api::download`] for why a hash rather
/// than §7.1's `file_index`.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Download {
    item_id: ItemId,
    /// The node's own `downloads` when left out.
    #[serde(default)]
    dest: Option<PathBuf>,
    #[serde(default)]
    file: Option<ContentHash>,
}

/// `library.task`'s params.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TaskParams {
    task_id: TaskId,
}

/// Reads the params a method expects, or says what was wrong with them.
fn parse<T: for<'de> Deserialize<'de>>(params: Option<Value>) -> Result<T, Error> {
    serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|error| Error::invalid_params(error.to_string()))
}
