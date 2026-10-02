# Ivan's  notes

PLEASE IGNORE - just my intermediary notes - night be completely wrong

## SignedAddress

```
/// [`Self::applied`] is the exception that proves it: it moves, but only when
/// the log does, which is exactly when a statement is genuinely a new one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedAddress {
    member: MemberId,
    addr: NodeAddr,
    applied: u64,
    signature: Signature,
}
```

Is applied really so harmless?  What happens when node restarts with exactly same address but newer log? On the other hand how to detect stale addresses?   

## Memory efficiency
We do hold a lot of history to limit duplications -  is it bounded well?

## Catalogue sync
Still not convinced about sync algorithm,  but let it be for now - see later in full experiments how it works.
I think it bit overcomplicated - Agent is inventing more and more complex constructs like `fetch_content_nobody_offered`

## Tests
Fast tests are no longer fast - we will need to reclassify some tests, which run longer like `a_member_who_arrives_with_the_content_is_asked_at_once`

Why some tests cannot run in parallel?  If it is about ports or some shared resources we should rather solve that rather then serialize tests

## Deleting files on upsert
```
// Cleared rather than merged: a file whose record is no longer readable is
    // a file this node can no longer offer, and leaving the row behind would
    // make the tables depend on what was projected into them before.
    tx.execute("DELETE FROM item_files WHERE item = ?1", params![id])?;
```

Why to delete all files?   Item ID is hash of all of it's content files -   so if they change, then ID has to change - so they cannot change.
So I think it'll be enough to delete only non-content files -   and add them again ?

## String literals
Do not like much string literals uses as constants - like method names,  etc. We should replace them with enums, where possible

## Parallel add/upload and download
Can we have parallel upload and download -  for items with multiple file.  For downloads from different peers - as many parallel as there is peer having that file.

## Manual-check test after phase 3

### Sync errors

When node is expelled other nodes still try to sync with it (even after node restart!):
```
2026-09-30T19:30:49.025719Z  INFO sync{me=66aa7448ad}:connect{peer=2fc0c54594 namespace=50539dccba}:connect{me=66aa7448ad alpn=/iroh-sync/1 remote=2fc0c54594}: refused to dial a non-member peer=2fc0c545945a4445ccc5ae2ac0da79cd427b782842fac577aba7453565cb0fc3 alpn=/iroh-sync/1
2026-09-30T19:30:49.026080Z  WARN sync{me=66aa7448ad}:connect{peer=2fc0c54594 namespace=50539dccba}: sync failed origin=Connect(DirectJoin) err=Failed to establish connection

Caused by:
    0: Connection was rejected locally
    1: Connection was rejected locally
```

Also getting this for other running node (bob):
```
2026-09-30T19:34:13.281077Z  WARN sync{me=66aa7448ad}:connect{peer=1139382057 namespace=50539dccba}: sync state finish called but not in running state
```

Also getting this error on node (dave):
```
2026-10-01T06:47:11.620635Z  WARN sync{me=c5db881349}:connect{peer=2fc0c54594 namespace=50539dccba}:connect{me=c5db881349 alpn=/iroh-sync/1 remote=2fc0c54594}: failed closing path err=MultipathNotNegotiated
```
### chapter 5a is strange - possibly outdated

Chapter ## 5a. A core node moves house — clause P1-23, "a core node changes IP or port" is confusing -  we already have node address annoucement - which did worked in this case

### More expel tests
To test on expel core member:
- if expelled by core member I should have already one approval (from that member)
- what happens when leader is expelled

### Expel CLI - do not know if pending for approval
Expelling core member requires approvals,  but when proposing expelling it looks like this:
```
 dl -d $DL/d expel $B --reason "manual check: two operators"
expelled    1139382057cab1e377a74c67ef34c5a02aa7702ae04a9ee5b49338076a19088a
```
So it's not possible to view, that it's pending approval.  Output is always the same.

### Approving alread approved

Approving already approved is done without any information that it's already approved. Pending is also showing ones I approved.
```
~ $ dl -d $DL/a core set $B --addr 127.0.0.1:11305
proposed    1139382057cab1e377a74c67ef34c5a02aa7702ae04a9ee5b49338076a19088a at 127.0.0.1:11305
            waiting for core approval — 1 of 2 so far
            a core member approves it with `distlib approve 19`
~ $ dl -d $DL/a pending
19      add 1139382057cab1e377a74c67ef34c5a02aa7702ae04a9ee5b49338076a19088a to the core group
        1 of 2 approvals, proposed by 66aa7448ad684e2576c034f8b4c84074765480ef9989035787b2bfbf5fc33ac5
        stops waiting after 127 more change(s) to the group

Approve one with `distlib approve <index>`, or take back your own
with `distlib withdraw <index>`.
~ $ dl -d $DL/a approve 19
approved    19 — 1 of 2, still waiting for others
~ $ dl -d $DL/a approve 19
approved    19 — 1 of 2, still waiting for others
```

### Document is messy 

Chapters numbers mixed. Relicts from earlier phases, which were changed.  Reset in middle - WTF -  it should be one process from start to end - simple, easy - to test basic functionality.

Chapter 10. setup -  why redoing setup again - NO do not want to.  Manual check should follow logical built of community - 3 core members, some additional members, test expel,  add, search ..., in CLI and UI.  No special bullshit about edge cases (they should be covered by automated tests) - real life - so people can try how it'll work.


### Deleting after clean stop and start of node 

Got this - do not understand what it is (if it's normal - why to log as info?):

```
2026-10-01T19:01:26.676812Z  INFO Running garbage collection
2026-10-01T19:01:26.676856Z  INFO Garbage collect
2026-10-01T19:01:26.677509Z  INFO Deleted "5a287953919a4f2d9243c2d53af148dc.store"
2026-10-01T19:01:26.677608Z  INFO Deleted "03943c893fd74d3bb6525d617bbf2a29.fast"
2026-10-01T19:01:26.678444Z  INFO Deleted "03943c893fd74d3bb6525d617bbf2a29.store"
2026-10-01T19:01:26.678560Z  INFO Deleted "03943c893fd74d3bb6525d617bbf2a29.35.del"
2026-10-01T19:01:26.678590Z  INFO Deleted "37302b63a24448f4977e856f83a0dcf4.term"
2026-10-01T19:01:26.678614Z  INFO Deleted "03943c893fd74d3bb6525d617bbf2a29.pos"
2026-10-01T19:01:26.678632Z  INFO Deleted "37302b63a24448f4977e856f83a0dcf4.fast"
2026-10-01T19:01:26.678650Z  INFO Deleted "37302b63a24448f4977e856f83a0dcf4.fieldnorm"
2026-10-01T19:01:26.678715Z  INFO Deleted "37302b63a24448f4977e856f83a0dcf4.pos"
2026-10-01T19:01:26.678804Z  INFO Deleted "37302b63a24448f4977e856f83a0dcf4.store"
2026-10-01T19:01:26.678855Z  INFO Deleted "5a287953919a4f2d9243c2d53af148dc.idx"
2026-10-01T19:01:26.678899Z  INFO Deleted "5a287953919a4f2d9243c2d53af148dc.35.del"
2026-10-01T19:01:26.678946Z  INFO Deleted "5a287953919a4f2d9243c2d53af148dc.pos"
2026-10-01T19:01:26.678969Z  INFO Deleted "03943c893fd74d3bb6525d617bbf2a29.term"
2026-10-01T19:01:26.678986Z  INFO Deleted "37302b63a24448f4977e856f83a0dcf4.idx"
2026-10-01T19:01:26.679023Z  INFO Deleted "03943c893fd74d3bb6525d617bbf2a29.fieldnorm"
2026-10-01T19:01:26.679043Z  INFO Deleted "5a287953919a4f2d9243c2d53af148dc.fast"
2026-10-01T19:01:26.679060Z  INFO Deleted "5a287953919a4f2d9243c2d53af148dc.term"
2026-10-01T19:01:26.679087Z  INFO Deleted "37302b63a24448f4977e856f83a0dcf4.35.del"
2026-10-01T19:01:26.679189Z  INFO Deleted "03943c893fd74d3bb6525d617bbf2a29.idx"
```

### Sync activity

Bit concerned about ongoing syncs -  every 15s  each node is syncing with (not sure if all) other nodes even when nothing is happening - in bigger group it can be problem. And log level should be DEBUG for these anyhow.

```
2026-10-02T05:43:39.914036Z  INFO sync{me=66aa7448ad}:accept{peer=c5db881349 namespace=50539dccba}: sync finished sent=0 recv=0 t_connect=4.541µs t_process=27.533664ms
2026-10-02T05:43:47.222555Z  INFO sync{me=66aa7448ad}:connect{peer=c5db881349 namespace=50539dccba}: sync finished sent=0 recv=0 t_connect=16.206054ms t_process=6.940809ms
2026-10-02T05:43:47.223845Z  INFO sync{me=66aa7448ad}:connect{peer=e7cbcee878 namespace=50539dccba}: sync finished sent=0 recv=0 t_connect=17.356515ms t_process=7.014341ms
2026-10-02T05:43:47.242754Z  INFO sync{me=66aa7448ad}:accept{peer=2fc0c54594 namespace=50539dccba}: sync finished sent=0 recv=0 t_connect=2.394233ms t_process=29.073235ms
2026-10-02T05:43:47.248025Z  INFO sync{me=66aa7448ad}:accept{peer=1139382057 namespace=50539dccba}: sync finished sent=0 recv=0 t_connect=4.455µs t_process=28.742589ms
2026-10-02T05:43:54.891633Z  INFO sync{me=66aa7448ad}:accept{peer=c5db881349 namespace=50539dccba}: sync finished sent=0 recv=0 t_connect=101.444µs t_process=2.555934ms
2026-10-02T05:44:00.568236Z  INFO sync{me=66aa7448ad}:connect{peer=e7cbcee878 namespace=50539dccba}: sync finished sent=0 recv=0 t_connect=5.432362ms t_process=10.626861ms
2026-10-02T05:44:00.569661Z  INFO sync{me=66aa7448ad}:accept{peer=1139382057 namespace=50539dccba}: sync finished sent=0 recv=0 t_connect=1.198696ms t_process=2.098558ms
2026-10-02T05:44:00.571245Z  INFO sync{me=66aa7448ad}:accept{peer=2fc0c54594 namespace=50539dccba}: sync finished sent=0 recv=0 t_connect=3.026µs t_process=4.037142ms
2026-10-02T05:44:00.574748Z  INFO sync{me=66aa7448ad}:accept{peer=e7cbcee878 namespace=50539dccba}: sync finished sent=0 recv=0 t_connect=1.506475ms t_process=2.11978ms
2026-10-02T05:44:00.594240Z  INFO sync{me=66aa7448ad}:connect{peer=c5db881349 namespace=50539dccba}: sync finished sent=0 recv=0 t_connect=8.689316ms t_process=34.659285ms
```

Randomly this error appears - even when all nodes are running -  the peer does not have anything in it's log:
```
2026-10-02T05:44:43.939485Z  WARN sync{me=66aa7448ad}:connect{peer=1139382057 namespace=50539dccba}: sync state finish called but not in running state
```

### Testing UI

It takes quite some time before connecting switches to live - and all worked in meanwhile - can see library, nodes, download.

In library it would be good to see which items are local -  fetched and exported.

Language is not in full text search?