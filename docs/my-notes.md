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