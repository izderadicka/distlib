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

`run` creates an identity in a directory that has none and reads defaults when there is
no config file, so a mistyped `--data-dir` does not fail — it silently starts a brand
new node. The symptom is unmistakable once you know it: `listening addr=0.0.0.0:<random>`,
a relay in `relay_mode = "disabled"`, `local api listening addr=127.0.0.1:11280`, and a
member id that is not the one you expect. If a node looks like that, it is reading no
config, and the data directory is the first thing to check.

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
dl -d $DL/b status      # group, standing core member, raft state, leader
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
dl -d $DL/d status      # standing member; "follows the log to index N"; no raft line
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
dl -d $DL/a status      # the "leader" line names it
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
