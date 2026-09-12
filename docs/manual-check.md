# Manual check: the phase 1 acceptance criteria, by hand

§9's acceptance sentence, run through the CLI in five separate processes:

> 3-core-node cluster + 2 follower nodes; add a member → it can connect; expel it →
> open connection drops, reconnect refused; kill one core node → group still admits
> members.

`crates/distlib-consensus/tests/acceptance.rs` already runs that in-process on every
commit, and it is the gate. This is the other half: what a test cannot check is whether
a *person* can do it — config-file friction, misleading output, port collisions, an
error that does not say what to do next, a ticket that carries an address nobody can
dial. Worth running at the end of a phase, and after anything that touches the CLI, the
config file or the join flow.

Everything is on loopback with `relay_mode = "disabled"` — no relay, no DNS, offline
and deterministic.

| Node | Role | `[net] bind_addr_v4` | `[api] bind_addr` |
|---|---|---|---|
| `a` | core, founder | `127.0.0.1:11204` | `127.0.0.1:11280` |
| `b` | core | `127.0.0.1:11205` | `127.0.0.1:11281` |
| `c` | core | `127.0.0.1:11206` | `127.0.0.1:11282` |
| `d` | follower | `127.0.0.1:11207` | `127.0.0.1:11283` |
| `e` | follower | `127.0.0.1:11208` | `127.0.0.1:11284` |
| `f` | admitted at the end, never started | — | — |

Six terminals: five for running nodes, one to drive from. Every node runs with `-v` —
gossip announcements and follow-loop progress log at `debug`, and without them "did
that arrive by gossip or by the 30-second poll?" is unanswerable.

**In every one of the six**, from the repo root:

```sh
export DL=$HOME/tmp/dl
export BIN=$PWD/target/debug/distlib
dl() { $BIN "$@"; }
```

A mistyped `--data-dir` used to be silent: `run` would mint an identity in the empty
directory, read defaults because there was no config file, and start a brand new node
that looked perfectly healthy. It no longer can — `run` refuses a directory with no
identity, and its first log line names both the data directory and the config file it
loaded:

```text
INFO starting data_dir=/home/you/tmp/dl/b config=/home/you/tmp/dl/b/config.toml
```

`config=... (absent, using defaults)` means the config file is not where the node
looked. That, plus `listening addr=0.0.0.0:<random>` under `relay_mode = "disabled"`
and an api on 11280, is the shape of a node reading no configuration — check the data
directory first.

---

## Setup (terminal 0)

```sh
cargo build
for n in a b c d e f; do dl -d $DL/$n init; done
```

**Collect ids.** `whoami` prints one whether or not a port is pinned.

```sh
for n in a b c d e f; do
  eval "$(echo $n | tr a-f A-F)=$(dl -d $DL/$n whoami | awk '/^identity/{print $2}')"
done
echo $A $B $C $D $E $F
```

**Write the configs.** Each node needs its own pair of ports — two on one machine
collide on both, and the API collision has its own error message pointing at
`[api] bind_addr`. The core nodes get the founding core group, identical in all three;
d and e get an empty one, which `join` fills in later.

```sh
CORE="core = [
  { member = \"$A\", name = \"alice\", addrs = [\"127.0.0.1:11204\"] },
  { member = \"$B\", name = \"bob\",   addrs = [\"127.0.0.1:11205\"] },
  { member = \"$C\", name = \"carol\", addrs = [\"127.0.0.1:11206\"] },
]"

i=4
for n in a b c d e; do
  case $n in a|b|c) core="$CORE" ;; *) core="core = []" ;; esac
  cat > $DL/$n/config.toml <<EOF
[net]
bind_addr_v4 = "127.0.0.1:1120$i"
relay_mode = "disabled"
relay_urls = []

[consensus]
$core

[api]
enabled = true
bind_addr = "127.0.0.1:1128$((i-4))"
EOF
  i=$((i+1))
done
cat $DL/a/config.toml
```

Core nodes must be pinned *before* founding: founding records the address in the log
and nothing rewrites it.

Now read one `whoami` in full, with a port pinned — it prints the line a founder's
friend is supposed to send them, and whether that reads properly is one of the things
being checked:

```sh
dl -d $DL/b whoami
```

---

## 1. Found — clause "a 3-core-node cluster"

Start b and c first; they warn they are in no group. Then found from a. In each node's
own terminal:

```sh
dl -v -d $DL/b run                 # terminal 2
dl -v -d $DL/c run                 # terminal 3
dl -v -d $DL/a run --found-group   # terminal 1
```

Each should log `listening addr=127.0.0.1:1120…` and `local api listening
addr=127.0.0.1:1128…` with its own ports, and no relay. Anything else means it is not
reading the config file you wrote.

Expect `founding the group` on a, then `membership group=… members=3 core=3` on **all
three** — b and c were told nothing, they replicated it. A couple of openraft `WARN`
lines around the election are normal.

```sh
dl -d $DL/b status      # group; role core member; Raft role; Raft leader
```

---

## 2. Followers — clause "+ 2 follower nodes"

Admit first: until the log says so, nothing will talk to them.

```sh
dl -d $DL/a admit $D --name dave
TICKET=$(dl -d $DL/a ticket | head -1)
dl -d $DL/d join $TICKET
cat $DL/d/config.toml       # check: core has the 3 pinned addrs; ports survived join
```

The ticket's addresses come from Raft's `StoredMembership`, populated at founding from
the founders' configured `addrs`. With relays disabled there is no discovery to fall
back on, so an empty or wildcard address here is a dead follower — cheaper to see in
the file than to debug across four terminals.

```sh
dl -v -d $DL/d run                 # terminal 4
```

```sh
dl -d $DL/d status      # role member; "follows the log to index N"; no Raft lines
dl -d $DL/d members     # 4 so far: 3 core + dave
```

Repeat for e — `admit $E --name erin`, `join`, terminal 5 — then:

```sh
dl -d $DL/d members     # 5 members, 3 core
```

---

## 3. It can connect — clause "add a member → it can connect"

Both pings run from a data dir whose node is running, so both need a throwaway port:
the pinned one is held by the node itself.

```sh
DISTLIB_NET__BIND_ADDR_V4=127.0.0.1:0 dl -d $DL/d ping $A --addr 127.0.0.1:11204
DISTLIB_NET__BIND_ADDR_V4=127.0.0.1:0 dl -d $DL/a ping $D --addr 127.0.0.1:11207
```

Both echo `ping`. The load-bearing proof of this clause is `d members` above — the
follower fetched the whole log over a real connection. The pings corroborate it.

Each ping binds a *second* endpoint under a key a running node already holds. Direct
dial with `--addr` and no relay is fine; if one misbehaves, that duplicate identity is
the first suspect, not the allowlist.

---

## 4. Expel — clause "expel → connection drops, reconnect refused"

From b, not the founder: any member may propose one.

```sh
dl -d $DL/b expel $E --reason "manual check"
DISTLIB_NET__BIND_ADDR_V4=127.0.0.1:0 dl -d $DL/e ping $A --addr 127.0.0.1:11204
```

Expected: the ping fails with `is not a member, or does not consider us one`, and **a's
terminal** logs `rejected a connection from a non-member`. Those two are the clause.

Watch e's terminal, but do not assert anything about it in advance. Once the expulsion
commits the core nodes refuse e at the allowlist, so e most likely never receives the
entry expelling itself — expect its open connections to close and its fetches to be
refused, with `dl -d $DL/e members` still listing e. There is a race where it catches
the entry just before the allowlist updates. Both outcomes are correct.

The reverse direction (a → e) is **not** the check: `ping` treats the id on the command
line as consent, and e still allows a, since a is still in e's copy of the log.

---

## 5. Kill the leader — clause "kill one core node → group still admits members"

```sh
dl -d $DL/a status      # the "Raft leader" line names it
```

Ctrl-C that terminal, then **immediately**, from a surviving core node:

```sh
time dl -d $DL/<survivor> admit $F --name frank
```

Must return within seconds. Two of three voters are still a quorum, and this is exactly
where the forty-five-second forward-to-a-dead-leader bug lived (P1-38). Then:

```sh
dl -d $DL/d members     # frank appears on a follower nobody told
```

---

## 5a. A core node moves house — clause P1-23, "a core node changes IP or port"

The one failure in phase 1 that had no way out: a core node in a group with
`relay_mode = "disabled"` gets a new port, nobody can reach it again, and nothing in the
log could say where it went. Refounding the group was the only fix. Do it on purpose.

Run it with all three voters up — two of three is a quorum, one of two is not — so this
comes before the core expulsion below. Check `dl -d $DL/a status` names a or c as the
leader (if it names b, kill a different one and adjust the ids below), then Ctrl-C **b**'s
terminal. Then edit its `config.toml`, change `[net] bind_addr_v4` to a port
nothing else is using, and start it again with `dl -d $DL/b run`.

It comes up, says `members=… core=…` from the log it already had, and then goes quiet.
It can still reach the others — it has their addresses from the log — so you will see it
call elections it cannot win. What it cannot do is be *reached*. Prove it: admit somebody
from a node that is still where it was.

```sh
dl -d $DL/g init >/dev/null
G=$(dl -d $DL/g whoami | awk '/^identity/{print $2}')
dl -d $DL/a admit $G --name grace
dl -d $DL/a members            # grace is there
dl -d $DL/d members            # and on a follower nobody told
```

The moved node's terminal never mentions grace: its member count stays where it was.
Now tell the group where it went, from any core member:

```sh
dl -d $DL/a core set $B --addr 127.0.0.1:<b's new port>
```

Expected: `core set    <id> at 127.0.0.1:<port>` — **applied, not proposed**. Moving a
core node does not change *who* votes, so it takes one core approval and the proposer's
own is it. Compare with the other verb, which does change who votes:

```sh
dl -d $DL/a core remove $B     # expect: proposed … waiting for core approval — 1 of 2
dl -d $DL/a pending            # it is there, with what it is waiting for
dl -d $DL/a withdraw <N>       # take it back; b is wanted, and §5b needs three voters
```

Within a second or two of the `core set`, the moved node's terminal should print a
membership line naming grace. That is the whole claim: it was replicated to, at an
address it only learned by being told.

Worth watching, because it is what made this hard to find — how long the gap is between
the `core set` returning and the moved node catching up. It should be about a second. If
it is thirty, the connection-closing half of the fix has stopped working and the node is
waiting for a dead path to time out rather than being redialled.

---

## 5b. Two operators agree — removing a **core** member (§4.4 step 2)

Everything above removes a *follower*, which any core member does alone. Removing a
voter is the other rule, and it is the first thing in this runbook that needs two people
to agree. Run it with three voters still up, so restart the node killed in §5 first.

Propose it from a **follower** — e is expelled by now, so use f from §5, or any member
that is not a voter. Submitting is open to every member; deciding is not.

```sh
dl -d $DL/f expel $C --reason "manual check: two operators"
```

Expected — and the point of this section — is that it does **not** say `expelled`:

```
proposed    <c>
            waiting for core approval — 0 of 2 so far
            a core member approves it with `distlib approve <N>`
```

Note the index. On **a**:

```sh
dl -d $DL/a pending      # lists it: what, who proposed it, how many approvals
dl -d $DL/a approve <N>
```

Expected: `approved <N> — 1 of 2, still waiting for others`. One of three voters is not a
majority, and `dl -d $DL/a status` still shows three in the core group. Then on **b**:

```sh
dl -d $DL/b approve <N>
```

Expected: `approved <N> — it has taken effect`, `dl -d $DL/a members` no longer lists c,
and c's core-group line drops to two. **c's own terminal** behaves like e's did in §4 —
it is no longer a member, so it most likely never sees the entry removing it.

Then the other half, which is what makes this safe rather than merely ceremonious. Try
to withdraw somebody else's proposal — propose a fresh one from f and, from **a**:

```sh
dl -d $DL/f expel $B --reason "manual check: withdrawal"
dl -d $DL/a withdraw <N>       # expect a refusal: a did not propose it
dl -d $DL/f withdraw <N>       # expect: withdrew <N>
```

A core member who could withdraw anyone's proposal would hold a veto over a decision the
rest of the core group was reaching, so only the proposer may. Check `dl -d $DL/a
pending` is empty afterwards.

---

## 5c. A follower starts voting — clause P1-30, "promotion needs a restart"

The other half of the core group being changeable, and the one phase 1 deferred: until
2.3-2 a node served consensus only if it had *started* as a voter, so the core group
could shrink and move but never grow. Adding one was refused outright.

Do it to d, which has been following since §2. First, what it says about itself:

```sh
dl -d $DL/d status
```

Expected: `role        member` and a `follows     the log to index N` line — no Raft
role at all, because it does not vote and is not pretending to.

§5b expelled c from the core group, so the voters are a and b. Adding a third changes
who votes, which takes a majority of the two — a proposes, b agrees:

```sh
dl -d $DL/a core set $D --addr 127.0.0.1:11207
dl -d $DL/a pending            # note the index
dl -d $DL/b approve <N>
```

**Watch d's terminal.** In order, and each line is a step that could not happen before:

* `the log says this node votes now; taking a seat in consensus`
* a membership line, then `this node is now a voter`

Between those two it empties its own copy of the log. Nothing in the terminal says so —
the membership line is only printed once there is a group, so the node simply goes quiet
for a moment — but `dl -d $DL/d status` caught in that window reads `group  none yet`
and `members  0 (0 core)`. That is deliberate: a node holding a log but no voters of its
own is exactly what this codebase refuses to speak consensus to, so it has to look like
a node that has not started yet for as long as it takes the leader to catch it up. What
it does *not* do in that window is forget who it will talk to — it keeps enforcing the
allowlist it already had, which is why it stays reachable throughout.

Then ask it again:

```sh
dl -d $DL/d status
```

Expected: `role        core member`, and a `Raft role` line saying `Follower` or
`Leader` — **not** `Learner`. Learner means it is being replicated to but is not yet
counted in a quorum, which is the halfway state promotion passes through; it should take
a second or two to leave it.

Prove it is really voting rather than only being told things. Stop a — the group is
three voters now, so two is still a majority — and admit somebody from b:

```sh
# Ctrl-C a's terminal
dl -d $DL/b admit $C --name "carol, back again"
dl -d $DL/d members
```

That commit needed d's vote. Before this sub-phase, the same group would have had two
voters with one of them down, and nothing would have committed at all.

---

## 5d. A voter stands down — clause MEM-04, "a demoted node keeps its seat"

The mirror of §5c, and until the review-findings PR the half that did not work: a node
the log dropped from the core group kept its Raft, went on answering `distlib/raft/0`,
and never started following — so it froze at the membership it held and went on
enforcing that allowlist for as long as it ran.

Do it to d, which §5c just promoted. The voters are a, b and d, so demoting one takes a
majority of three — two of the others have to say so. a is stopped from the end of §5c,
so restart it first and let it catch up:

```sh
dl -d $DL/a run &              # or its own terminal
dl -d $DL/a status             # wait for `role  core member`
dl -d $DL/b core remove $D
dl -d $DL/b pending            # note the index
dl -d $DL/a approve <N>
```

**Watch d's terminal:**

* `the log says this node no longer votes; standing down`
* `this node is now a follower`

Then ask it:

```sh
dl -d $DL/d status
```

Expected: `role  member` with no `Raft role` line at all — it has given the seat up
rather than sitting in it as a non-voter — and a `follows  the log to index N` line
whose N is where its Raft had got to, not zero. That number is the point: it picks up
where it left off instead of re-fetching the whole log.

It is still a member, so prove it is keeping up rather than frozen. Admit somebody from
b, then ask d:

```sh
dl -d $DL/b admit $C --name "carol, once more"
dl -d $DL/d members
```

**Be patient: this takes up to thirty seconds.** A demoted node keeps the announcing
half of gossip it had as a voter, so nothing pokes its new follow loop and it waits out
the idle poll. That is recorded in the phase-2 register, not a fault in this step — but
it is why `dl -d $DL/d members` immediately after the admit will not show carol yet.

---

## 6. After

Restart the killed node and watch it rejoin and catch up — beyond §9, but the first
thing anyone would actually do next.

```sh
pgrep -af distlib       # must be empty once everything is stopped
rm -rf $DL
```

---

## Watch for, beyond pass/fail

- Does any error leave you without a next step?
- Does `join` preserve what was set before it? It re-renders the whole config file.
- Does a follower's `status` read sensibly while it is behind?
- How long does a change take to reach a follower — gossip, or the 30-second poll?
- Anything the README quickstart gets wrong now that followers exist.
- Does `pending` tell you enough to decide, without going to the log for it?
- Is it obvious from `admit`/`expel` output alone whether anything actually happened?
- Does `core set` say enough for you to tell an applied change from a waiting one?
- Is a node in the middle of being promoted alarming to watch? It goes quiet, and
  `status` says it is in no group. Nothing explains that while it is happening — should
  it?
- Is there anything that tells you a core node is unreachable *before* you notice it
  has stopped keeping up?
