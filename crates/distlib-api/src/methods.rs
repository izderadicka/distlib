//! What each JSON-RPC method does.
//!
//! Method names come from §7.1 verbatim, so phase 3 extends this set rather
//! than renaming it: `library.*` and the SSE stream land beside these, and a
//! caller written against `group.members` today keeps working.

use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use distlib_consensus::{MemberRecord, MembershipEvent, MembershipNode, MembershipState};
use distlib_core::{
    ContentHash, FileRecord, FileRole, Item, ItemId, ItemKind, MemberId, NetConfig, NodeAddr,
    Series, Ticket,
};
use distlib_net::{Blobs, NetError};
use distlib_store::{ReindexHandle, SearchIndex, Store, StoreError, StoredItem};
use distlib_sync::{Catalogue, SyncError};
use iroh::SecretKey;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::rpc::Error;

/// Everything the methods need: the running node and the key it signs with.
///
/// A ceiling on `library.search`'s `limit`, whatever a caller asks for.
///
/// This listener is loopback and token-gated (§7.1, P1-25), so this is a
/// guard against a mistake rather than an attacker — but tantivy's
/// `TopDocs::with_limit` allocates a heap sized to it, and no legitimate
/// caller of a personal library's search needs a single page bigger than
/// this.
const MAX_SEARCH_RESULTS: usize = 500;

/// The node is shared rather than owned: whoever started it keeps serving the
/// group with it while this answers questions about it.
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
}

impl Api {
    /// Dispatches one call.
    pub async fn call(&self, method: &str, params: Option<Value>) -> Result<Value, Error> {
        match method {
            "node.status" => self.status(),
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
            "library.item" => self.item(parse(params)?).await,
            "library.add" => self.add(parse(params)?).await,
            "library.download" => self.download(parse(params)?).await,
            other => Err(Error::method_not_found(other)),
        }
    }

    /// `node.status` — who this node is and where it stands in its group.
    fn status(&self) -> Result<Value, Error> {
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
    fn pending(&self) -> Result<Value, Error> {
        let membership = self.node.membership();
        let pending: Vec<Value> = membership
            .pending()
            .map(|(proposal, entry)| {
                json!({
                    "proposal": proposal,
                    "proposer": entry.proposer(),
                    "what": describe(entry.event(), &membership),
                    "approvals": membership.approvals_counting(entry).collect::<Vec<_>>(),
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
    async fn approve(&self, params: Proposal) -> Result<Value, Error> {
        self.commit(MembershipEvent::Approved {
            proposal: params.proposal,
        })
        .await?;
        Ok(self.outcome(params.proposal))
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
    /// **No `filters` and no paging**, though the plan's own sketch names
    /// both (`{query, filters{type,genre,lang,author,series}, page}`) — see
    /// [`Search`]'s doc comment for why each is left for whoever needs the
    /// first real one.
    async fn search(&self, params: Search) -> Result<Value, Error> {
        let ids = self
            .search
            .search(&params.query, params.limit.min(MAX_SEARCH_RESULTS))
            .await
            .map_err(query_error)?;

        let mut results = Vec::with_capacity(ids.len());
        for id in ids {
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
                results.push(summary(&stored));
            }
        }
        Ok(json!({ "results": results }))
    }

    /// `library.item` — the full record the read model holds for one item.
    ///
    /// **No ratings or availability**, though the plan's sketch promises both
    /// in this answer: nothing populates either yet. `schema.rs` gives the
    /// same reason for leaving the `reviews` table out of `distlib-store`
    /// altogether — a field nothing fills is a commitment made before the
    /// thing it describes exists.
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
        record["replicas"] = json!(stored.item.replicas);
        record["files"] = json!(stored.item.files);
        record["last_modified"] = json!(stored.last_modified);
        Ok(record)
    }

    /// `library.add` — hash a file set, store it as blobs, and write a
    /// catalogue entry for it (2b-2; §6.1's "v1 — exact" dedup).
    ///
    /// **`files` are all `role: content`.** They are what §5.2's `item_id`
    /// fingerprints (D4), and it is the only role this surface writes —
    /// covers, subtitles and the rest of [`distlib_core::FileRole`] are left
    /// for whoever needs the first caller that wants one, the way `filters`
    /// and paging were left out of `library.search` (P2-21).
    ///
    /// **The dedup guard runs before anything is written.** The fingerprint
    /// is known before the catalogue is asked anything (§6.1's "add-time
    /// assist"). A hit means somebody's copy of this exact file set is
    /// already an item: **metadata already there is left alone** — this call
    /// never overwrites a `title` or `kind` a hit already holds, so a second
    /// caller with a worse guess at the title cannot clobber a better one —
    /// and only the files this call brought that the item did not already
    /// have are written, which is the "offering to contribute any files it
    /// is missing" half of the same rule. A miss creates the item with every
    /// field this call was given.
    ///
    /// **This is also what makes the phase's own acceptance hold without any
    /// coordination.** `item_id` is a pure function of the content hashes
    /// (§5.2), so two nodes adding the identical file set independently
    /// compute the same id and converge on one item by construction — the
    /// guard here only decides what a *second* write to that id is allowed
    /// to change, not whether the two nodes end up looking at the same
    /// item.
    ///
    /// **The id is `item.fingerprint()`, not a hash list of its own.**
    /// `files` is keyed by content hash, so two paths given that hash the
    /// same — literal duplicates, or two different files with identical
    /// bytes — collapse to one entry before the id is ever computed;
    /// [`Item::fingerprint`] reads exactly that map. Hashing `params.files`
    /// into a separate `Vec` alongside it, the way an earlier version of
    /// this did, can disagree with `files`' own key set the moment a caller
    /// repeats a path — the id it stored under would then differ from the
    /// item's own fingerprint of what it actually holds, which
    /// `converge.rs`'s `it is what it contains` pins as an invariant nothing
    /// upstream of the domain type should be able to break twice.
    async fn add(&self, params: Add) -> Result<Value, Error> {
        if params.files.is_empty() {
            return Err(Error::invalid_params("library.add needs at least one file"));
        }

        // A placeholder: `fingerprint` reads only `files`, so the real id
        // is not known until every path is hashed into the same
        // deduplicated map the item is actually built from.
        // **Two different blobs cannot share a filename within one item**,
        // and this is the place to say so: an item's file names are decided
        // here, while the person who chose them is still in the room and can
        // rename one. `library.download` refuses the same collision, because
        // the catalogue is a shared document and another member's build can
        // write what this one refuses — but catching it only there means
        // telling somebody else, later, that an item they did not create
        // cannot be written into one directory. That is a worse conversation
        // at a worse moment, and this is the "check it on creation in the
        // first place" half of it.
        //
        // Keyed on the blob, so the *same* file named twice — one path
        // repeated, or two paths with identical bytes — is one entry and not
        // a collision. That case is legal and
        // `adding_two_paths_with_identical_bytes_keeps_the_id_and_the_items_own_fingerprint_in_step`
        // says so.
        let mut named: BTreeMap<String, PathBuf> = BTreeMap::new();
        let mut item = Item::new(ItemId::from_bytes([0; 32]));
        for path in &params.files {
            let (hash, record) = self.hash_file(path).await?;
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
            named.insert(record.filename.clone(), path.clone());
            item.files.insert(hash, record);
        }
        item.id = item
            .fingerprint()
            .expect("`files` is non-empty, and only `role: content` files are ever inserted here");
        let id = item.id;

        if let Some(existing) = self.catalogue.item(id).await.map_err(sync_error)? {
            // In today's code, `contributed` is rarely non-empty. `id` is
            // the fingerprint of exactly `item.files`' key set, so a hit
            // here means some earlier write already produced an item at
            // this same id from what must have been (barring a hash
            // collision) that identical set — `existing.files` should
            // already hold every hash `item.files` does. The exception,
            // and the reason this stays rather than becoming an assert, is
            // the case the guard exists for: an earlier `Catalogue::write`
            // interrupted partway through its per-key loop, leaving
            // `existing` with fewer file entries than its own id implies.
            // Every file `library.add` writes is `role: content` today, so
            // there is no *other* way for this to end up non-empty — that
            // changes the moment a role that does not take part in the
            // fingerprint (a cover, say) is wired in here too.
            let contributed: Vec<ContentHash> = item
                .files
                .keys()
                .filter(|hash| !existing.files.contains_key(hash))
                .copied()
                .collect();
            if !contributed.is_empty() {
                let files = item
                    .files
                    .into_iter()
                    .filter(|(hash, _)| contributed.contains(hash))
                    .collect();
                self.catalogue
                    .write(&Item {
                        files,
                        ..Item::new(id)
                    })
                    .await
                    .map_err(sync_error)?;
            }
            return Ok(json!({
                "item_id": id,
                "created": false,
                "title": existing.title,
                "contributed_files": contributed,
            }));
        }

        item.kind = Some(params.kind);
        item.title = params.title.clone();
        item.authors = (!params.authors.is_empty()).then_some(params.authors);
        item.genres = (!params.genres.is_empty()).then_some(params.genres);
        item.series = params.series;
        item.year = params.year;
        item.lang = params.lang;
        item.description = params.description;
        self.catalogue.write(&item).await.map_err(sync_error)?;

        Ok(json!({
            "item_id": id,
            "created": true,
            "title": params.title,
            "contributed_files": Vec::<ContentHash>::new(),
        }))
    }

    /// `library.download` — fetch an item's files from whoever has them, and
    /// write them out to a directory (2b-3).
    ///
    /// **Synchronous, where §7.1's sketch answers with a `task_id`.** A task
    /// id is only useful next to the `download.progress` events §7.2 puts on
    /// the SSE stream, and that stream is phase 3 — so the id would be a
    /// handle to nothing, and a caller would have to poll for a completion
    /// that has no representation either. Returning when the files are on
    /// disk says the same thing with nothing to build first, and turning it
    /// into a task later is an addition rather than a change: the answer
    /// gains a field. The same cut P2-21 made for `library.search`'s
    /// `filters`.
    ///
    /// **`file` names a content hash, where the sketch says `file_index`.**
    /// An item's files are a map keyed by content hash (§5.2), so the only
    /// index there is to give is a position in hash order — which is stable,
    /// meaningless to a human, and silently renumbers the moment a second
    /// member contributes a missing file. The hash is what `library.item`
    /// already reports and what the record itself is keyed by.
    ///
    /// **Every member is offered as a provider**, minus this node. §5.6's
    /// availability index is phase 4, so there is nothing to ask *who* holds
    /// a hash; what makes the naive list workable is that a member who does
    /// not have it is a provider skipped rather than a download failed, which
    /// `distlib-net`'s own tests pin. It is O(members) dials in the worst
    /// case and wrong at the thousands §2 allows — the same shape as the
    /// peer-offer sweep already carried out of this phase, and it should be
    /// answered by the availability index rather than guessed at here.
    ///
    /// **No deadline.** A media file is as slow as it is big, and a number
    /// picked here would be a guess about file sizes and links this method
    /// knows nothing about. The failure this would otherwise guard against
    /// does not need it: a provider that cannot be reached fails rather than
    /// hangs (`fetching_from_an_unreachable_provider_fails_rather_than_hangs`).
    ///
    /// **A failure partway through a multi-file download leaves the files
    /// already written where they are**, and the error names only what
    /// failed. That is deliberate rather than overlooked: those files are
    /// complete and verified, deleting them would be this method destroying
    /// something on its way out, and asking again writes the rest — the
    /// ones already here are refused by name, which is what says so.
    ///
    /// **Nothing is "registered".** The fetch lands the bytes in the store
    /// `Catalogue::protocols`' `BlobsProtocol` serves from, so this node is a
    /// holder from the moment it returns and stays one across a restart,
    /// because that store is on disk. §5.6's heartbeat is what would announce
    /// it, and it does not exist yet.
    async fn download(&self, params: Download) -> Result<Value, Error> {
        if !params.dest.is_dir() {
            return Err(Error::invalid_params(format!(
                "{}: not a directory to write files into",
                params.dest.display()
            )));
        }

        // The read model, like every other `library.*` read. It is derived
        // from the document rather than a second copy of it, so an item it
        // does not have yet is one this node has not synced — and answering
        // "no such item" is then the truth about this node, which is what a
        // caller about to wait and retry needs to hear.
        let stored = self
            .store
            .item(params.item_id)
            .await
            .map_err(|error| Error::failed(error.to_string()))?
            .ok_or_else(|| Error::failed(format!("no such item: {}", params.item_id)))?;

        let wanted: Vec<(ContentHash, FileRecord)> = match params.file {
            Some(hash) => {
                let record = stored.item.files.get(&hash).cloned().ok_or_else(|| {
                    Error::invalid_params(format!("{} has no file {hash}", params.item_id))
                })?;
                vec![(hash, record)]
            }
            None => stored.item.files.into_iter().collect(),
        };
        if wanted.is_empty() {
            return Err(Error::failed(format!(
                "{}: this item has no files yet",
                params.item_id
            )));
        }

        // Every target is worked out and checked before anything is fetched,
        // so a caller who mistyped a destination or asked for something that
        // cannot be written is not made to wait for a transfer first.
        //
        // **Nothing here overwrites anything.** Three ways that could happen
        // and all three are refused. A file already at the target is the
        // operator's — they may have edited or replaced it — and a download
        // is not a reason to assume otherwise; deleting it is an instruction,
        // overwriting it would be a guess. Two of this item's own files
        // landing on one path is the same loss by a different route. And a
        // `filename` is a string some other member's build wrote into the
        // document, so it is reduced to its last component before it is
        // joined — §2 says members do not attack the protocol, but
        // `dest.join("/etc/passwd")` is `/etc/passwd`, and that is a foot-gun
        // whether or not anybody means it.
        //
        // **The name collision is refused at `library.add` as well, and that
        // is where it is meant to be caught** — by whoever chose the names,
        // while they can still rename one. This is the second line, and it
        // is not redundant: the catalogue is one document for the whole
        // group, so an item's files can be written by a member running an
        // older build, or by a future one filling the roles `library.add`
        // does not write yet. What arrives here is data, and a method about
        // to write files to a disk checks it rather than trusting that
        // whoever wrote it ran this build.
        //
        // The refusal names `file`, which is the way through: a hash says
        // which of two identically-named files is wanted where their names
        // cannot. Writing them both under disambiguated names would be the
        // more useful answer and wants `FileRecord`'s own `disc` and `seq`
        // to do it with — they are the right disambiguator and nothing fills
        // them yet (P2-23).
        let mut taken: BTreeMap<PathBuf, ContentHash> = BTreeMap::new();
        let mut targets: Vec<(ContentHash, FileRecord, PathBuf)> = Vec::with_capacity(wanted.len());
        for (hash, record) in wanted {
            let name = std::path::Path::new(&record.filename)
                .file_name()
                .ok_or_else(|| {
                    Error::failed(format!(
                        "{hash}: {:?} is not a filename this can write",
                        record.filename
                    ))
                })?;
            let target = params.dest.join(name);
            if let Some(other) = taken.get(&target) {
                return Err(Error::failed(format!(
                    "{} and {other} are both called {}; download them one at a time with `file`",
                    hash,
                    target.display()
                )));
            }
            if target.exists() {
                return Err(Error::failed(format!(
                    "{} already exists; move or delete it to download this file again",
                    target.display()
                )));
            }
            taken.insert(target.clone(), hash);
            targets.push((hash, record, target));
        }

        let me = self.node.id();
        let providers: Vec<MemberId> = self
            .node
            .membership()
            .members()
            .map(|record| record.member_id)
            .filter(|member| *member != me)
            .collect();

        let mut files = Vec::with_capacity(targets.len());
        // Whether the one directory refresh has already been spent.
        let mut asked_where_everybody_is = false;
        for (hash, record, target) in targets {
            // Asked before the network is: this node may be the one that
            // added the item, or may have downloaded it before, and in a
            // group of one there is nobody to ask at all.
            let already_here = self.blobs.has(hash).await.map_err(net_error)?;
            if !already_here {
                self.fetch(hash, &providers, &mut asked_where_everybody_is)
                    .await?;
            }
            self.blobs.export(hash, &target).await.map_err(net_error)?;
            files.push(json!({
                "file": hash,
                "filename": record.filename,
                "path": target,
                // Whether it had to come over the network. A caller cannot
                // act on it, but an operator watching a download of an item
                // half of which was already here can read it.
                "fetched": !already_here,
            }));
        }

        Ok(json!({
            "item_id": params.item_id,
            "title": stored.item.title,
            "files": files,
        }))
    }

    /// Fetches one blob from `providers`, and on failure asks a core node
    /// where they are and tries once more.
    ///
    /// `asked` is the one refresh this call is allowed, spent across the whole
    /// download rather than per file — see [`Self::find_the_providers`] for
    /// what that buys and why the retry is worth a second attempt at all.
    ///
    /// The retry's own error is what surfaces if it fails too: it is the more
    /// recent account of the same thing, and having refreshed in between, the
    /// more informative one.
    async fn fetch(
        &self,
        hash: ContentHash,
        providers: &[MemberId],
        asked: &mut bool,
    ) -> Result<(), Error> {
        let failure = match self.blobs.fetch(hash, providers.to_vec()).await {
            Ok(()) => return Ok(()),
            Err(failure) => failure,
        };
        if *asked {
            return Err(net_error(failure));
        }
        *asked = true;
        if !self.find_the_providers(providers).await {
            return Err(net_error(failure));
        }
        self.blobs
            .fetch(hash, providers.to_vec())
            .await
            .map_err(net_error)
    }

    /// Asks a core node where the providers are — once per `library.download`,
    /// and only after a fetch has already failed.
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
    /// address" does. **Once per call, whatever happens**, so this cannot
    /// become the thing the plan warned against: a node answering every
    /// transient failure with an RPC — and because a refresh that did not help
    /// the first file will not help the fifth either.
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
    async fn hash_file(&self, path: &std::path::Path) -> Result<(ContentHash, FileRecord), Error> {
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

        let (hash, size) = self
            .catalogue
            .add_file(path)
            .await
            .map_err(|error| match error {
                SyncError::LocalFile { .. } => Error::invalid_params(error.to_string()),
                other => Error::failed(other.to_string()),
            })?;

        Ok((
            hash,
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

/// A malformed query is the caller's mistake; anything else out of
/// `distlib-store` is this method failing for its own reasons.
fn query_error(error: StoreError) -> Error {
    match error {
        StoreError::Query { .. } => Error::invalid_params(error.to_string()),
        other => Error::failed(other.to_string()),
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
/// **Neither `filters` nor an `offset` is implemented.** `authors`, `genres`
/// and `series` are already free-text fields a query can point at directly —
/// `authors:herbert` is valid tantivy syntax today — so a `filters` object
/// would either duplicate that or paper over `kind` and `lang`, which tantivy
/// never indexes at all: they live in SQLite only, and filtering by them needs
/// a real design, not a parameter nobody reads. Paging has the same shape of
/// gap: tantivy supports an offset natively, but nothing in 2a-4's acceptance
/// asks for a second page, and `SearchIndex::search` would have to grow the
/// parameter — and every existing call to it — for a caller that does not
/// exist yet. Left for whoever writes the first one.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Search {
    query: String,
    #[serde(default = "default_search_limit")]
    limit: usize,
}

/// `library.search` without an explicit `limit`. Small enough to read in one
/// screen, generous enough that "did my one test item show up" never needs one.
fn default_search_limit() -> usize {
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
    files: Vec<PathBuf>,
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
    dest: PathBuf,
    #[serde(default)]
    file: Option<ContentHash>,
}

/// Reads the params a method expects, or says what was wrong with them.
fn parse<T: for<'de> Deserialize<'de>>(params: Option<Value>) -> Result<T, Error> {
    serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|error| Error::invalid_params(error.to_string()))
}
