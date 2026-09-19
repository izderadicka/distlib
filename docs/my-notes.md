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