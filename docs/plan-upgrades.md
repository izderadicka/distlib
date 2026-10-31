# Upgrading live groups (design note)

**Status:** agreed with Ivan (§7); the v1 hooks are built in phase 5's 5d. **Goal:** from v1 on, a
group keeps working while its members upgrade one by one, over weeks or months, and some never do.

## 1. The constraint

Members upgrade when they get round to it. So for a long time, old and new versions run in one
group, and the group cannot wait for the last one. Three kinds of compatibility are at stake:

- **Talking:** old and new nodes must still understand each other's messages.
- **Agreeing:** where nodes compute the same answer independently, old and new must give the same
  answer. Examples are the membership fold and the custody ranking.
- **Storing:** a new binary must open an old data directory. This is local to one node, so there is
  no skew, only migration.

One fact makes this stricter than usual. **Every node replays the membership log from index 1**:
a promotion blanks the state machine, and a new follower fetches the whole log. The log is never
purged (5c-0). So every encoding and every fold rule that ever reached the log stays in the code
for good.

## 2. What crosses a version boundary today

| What | Where | Encoding | What an old node does with something new | Risk |
|---|---|---|---|---|
| Membership events (`SignedEvent`) | Raft log (redb), Raft RPC, memberlog | postcard. The signature covers a **re-encoding** of the decoded event (`signed.rs`) | A new variant or field fails to decode. On a core node the Raft RPC fails, so it falls behind; a follower's fetch fails and it retries silently | High: decides membership |
| `GroupSettings`, `MemberRecord` | inside `GroupFounded` / `MemberAdded` | postcard | A field added anywhere breaks decoding | High |
| Fold rules (`MembershipState::apply`) | every node | code | A changed verdict on the same entry splits the membership | High |
| Raft RPC (`distlib/raft/0`) | core nodes | postcard of our enums plus **openraft's own types** | An appended variant fails to decode, so the connection errors (PR #160's `Retired` treats that as "no") | Medium. An openraft upgrade changes its types |
| Memberlog (`distlib/memberlog/0`), ping (`distlib/ping/0`) | all members | postcard | As above | Medium |
| Gossip announcements (`Applied`, `ReachableAt`) | consensus topic | postcard | Dropped | Low: a follower falls back to its poll |
| Heartbeats (`SignedHeartbeat`) | availability topic | postcard, signed over a re-encoding, `HEARTBEAT_MAX` | Dropped, **so the new member looks offline**: wrong holders, needless emergency copies | High for availability |
| Catalogue entries | iroh-docs | string keys, JSON values | **Already tolerant**: unknown keys are skipped (`Key::parse` → `Absorbed::Unknown`), and serde ignores unknown JSON fields | Low, as long as an existing key keeps its meaning |
| Shared computations | each node, no consensus | code | Custody ranking (rendezvous score, weights, `CUSTODY_CONTEXT`), red at `RED_FROM_PROBERS`, emergency rule, settling. A change makes nodes disagree on who keeps what, and a node can then **release a copy that the other side thinks it keeps** | High: data loss |
| Join tickets | out of band | postcard + base32, versioned prefix | Refused as "wrong version" | Low; already handled |
| Local stores | data dir | redb (Raft log, state machine), SQLite projection (`user_version`, rebuilt by replay), iroh-blobs and iroh-docs stores, identity, config | Not applicable | Medium: needs migration and protection against downgrade |
| Dependencies | wire and disk | iroh 1.0.3; iroh-blobs 0.103, iroh-docs 0.101, iroh-gossip 0.101 (pre-1.0); openraft 0.9.25 | An upgrade can change ALPNs, wire or disk formats | **Highest**: we can't serve two versions of a dependency's protocol cheaply |

A smaller finding: the custody score uses the platform's `ln`. The comment in `custody.rs` notes
that glibc and the Windows runtime may differ, so even nodes on the **same** version can rank
custodians differently. Fixing it (`libm::log`) changes the ranking, which makes it a protocol
change once v1 is out. It should be fixed before v1. *Fixed in 5d-5 (#175): `libm::log`, with the
scores' bits pinned.*

## 3. Principles

1. **New code reads everything old code ever wrote**, for good (the log replay above).
2. **New code writes only what old code understands, until the group switches**: one switch for
   the whole group, recorded in the log.
3. **A node never guesses.** An entry it can't decode, or a switch beyond what it supports, makes
   it stop cleanly and say **"upgrade required"**. It doesn't crash-loop, and it doesn't retry
   silently.
4. **When versions disagree, keep more copies.** A node never releases data because of a rule the
   group hasn't switched on.
5. **The catalogue stays tolerant**: add keys and optional fields; never change what an existing
   key or field means.

## 4. The mechanism: a group protocol version

- **In the membership state.** `protocol`, founded at 1.
- **Each member says what it supports, in the log.** A self-signed event, owner-only like a pledge:
  `ProtocolSupported { max }`. Every node then sees the same picture, so the raise can be decided by
  the fold. A version shown from heartbeats couldn't be.
- **The raise.** `ProtocolRaised { to }` needs a majority of the core, like any core decision, and
  is proposed by hand (`distlib group raise-protocol`, Q3).
  - The fold refuses it unless every core member supports `to`. The core runs consensus, so the
    core has to be upgraded first.
  - Followers need not support it (Q1); those that don't are left behind, below.
  - Once raised, the fold also refuses to make a member core if it does not support the group's
    protocol.
- **Rules are switched by version:** `if self.protocol >= 2 { new } else { old }`. The raise sits
  at a fixed index, so a replay gives the same answer on every node.
- **A node below the active protocol is in "upgrade required" mode** (Q1, Q2). So is a node that
  meets a log entry it cannot decode.
  - It stops applying the membership log at that entry, so its allowlist freezes: it may go on
    admitting a member expelled after the raise, which is accepted (Q2).
  - It keeps serving blobs and the copies it is custodian of, and may still fetch, but **releases
    nothing** until it upgrades (principle 4).
  - Status, CLI and UI say "upgrade required". The process stays up.
  - Upgrading the binary resumes from where it stopped.
- **Upgrading the core.** One node at a time while the rest still form a majority. Until a raise,
  old and new run the same rules, so a mixed core is fine.

## 5. By area

- **Membership log**, strict:
  - never change the layout of an existing variant or of a type inside one;
  - append new variants only at the end, and use them only after a raise;
  - for a decode failure, apply principle 3 rather than a Raft storage error or a silent retry.
- **Our RPCs:**
  - an appended request variant must fail harmlessly on an old peer (`Retired` does);
  - an incompatible change gets a new ALPN (`…/1`), and new nodes serve both for as long as any
    member speaks `/0`, dialling `/1` first and falling back;
  - the serve-both code is built only when it is first needed.
- **Heartbeats and address statements**, which are frequent and matter for availability:
  - new nodes send the old format until a raise, and go on reading the previous one (Q4);
  - strict, like everything else (Q6). Verifying the signature over the exact bytes received
    would let a field appended at the end need no raise; it is not needed, so it is not built.
- **Catalogue:**
  - add keys and `#[serde(default)]` fields;
  - give a key a new meaning only behind a raise, and then only by writing a **new** key;
  - the probe keys and the availability base count as protocol (red, custody).
- **Shared computations:** any change is a protocol change, made behind a raise, with both versions
  kept in the code. During a mixed period nothing releases a copy on the strength of the new rule
  alone.
- **Local storage:**
  - a format version in the data dir;
  - a new binary migrates forward on start, after keeping a backup of what it changes;
  - it refuses a data dir newer than it knows, with a clear message;
  - the SQLite projection keeps being rebuilt rather than migrated.
- **Dependencies:**
  - an upgrade that changes any wire or disk format is a protocol change;
  - stay on the same iroh-blobs/docs/gossip and openraft releases through v1.x unless an upgrade is
    shown compatible;
  - an openraft major or an iroh-docs protocol change needs a migration plan of its own.

## 6. What v1 must have, and what can wait

**Must be in v1.** These are the hooks old nodes need in order to react, and an old node can't
learn them later:

1. **Golden encoding tests**: fixed bytes for every wire and stored type, which fail when an
   encoding changes. This is the cheapest guard against an accidental protocol change. *(5d-4)*
2. Fix the custody score's `ln` before the ranking is frozen. *(5d-5)*
3. A data-dir format version: refuse newer; a migration hook (empty at v1). *(5d-6, done in #176)*
4. `protocol` in the membership state, with `ProtocolSupported` and `ProtocolRaised`, their fold
   rules, and the manual raise. *(5d-7: the log and the fold in #177, the rest in 5d-7b)*
5. "Upgrade required" mode: on a raise beyond `max`, and on an undecodable log entry or memberlog
   page. Shown in `status`, the CLI and the UI. *(5d-8)*

Heartbeats stay strict (Q6), so exact-bytes signatures are not needed. The fold rules #154 still
asks for (pledge 0 on admission, expiry by log index) also go in before v1 *(5d-2)*: after v1, a
change to what the fold accepts is a protocol change.

**Can wait** until the first change that needs it:
- serving two ALPN versions;
- actual migrations;
- keeping the previous release's binary in CI for mixed-version tests, from v1.1 on.

## 7. Decisions (Ivan)

- **Q1. Laggard followers at a raise.** The raise needs only the core's support: every core member
  must upgrade first, followers may be left behind. Because many members may lag for a while, a
  laggard must go on serving blobs and the copies it keeps, so custody does not suffer (§4).
- **Q2. What a laggard does.** It stays in the group, serving and keeping content; its frozen
  allowlist admitting a member expelled after the raise is accepted. Serving custodied content
  matters more.
- **Q3. Who raises.** By hand, `distlib group raise-protocol`, approved by the core like any core
  decision.
- **Q4. Support window.** A release talks the latest protocol and the one before it. Upgrading in
  time is the group's responsibility; the software gives the path, not a longer window.
- **Q5. Downgrades.** Not supported, neither after a data-dir migration nor after a raise.
- **Q6. Heartbeats.** Whichever is easier: strict and gated like everything else, which needs no
  code now.
