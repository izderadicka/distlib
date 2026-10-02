# Manual check: a small group, from founding to losing its leader

One story, start to finish. Three friends found a group, two more people join, they share
books and a lecture, find and download each other's things through the command line and
the browser, someone leaves, someone is trusted with a vote, someone steps back, and the
node holding the group together goes away.

This is not where edge cases are tested — the automated suites do that on every commit.
What a test cannot tell you is whether a **person** can do all this: whether the output
says what happened, whether an error says what to do next, whether the pages make sense
to somebody who did not write them. Run it at the end of each phase, and after anything
that changes the CLI, the config file, the join flow or the pages.

Everything runs on one machine, on loopback, with relays off — no internet needed.

| Who | Node | Role | `[net] bind_addr_v4` | `[api] bind_addr` |
|---|---|---|---|---|
| alice | `a` | founds the group, core | `127.0.0.1:11204` | `127.0.0.1:11280` |
| bob | `b` | founder, core | `127.0.0.1:11205` | `127.0.0.1:11281` |
| carol | `c` | founder, core | `127.0.0.1:11206` | `127.0.0.1:11282` |
| dave | `d` | joins later; later gets a vote | `127.0.0.1:11207` | `127.0.0.1:11283` |
| erin | `e` | joins later; leaves | `127.0.0.1:11208` | `127.0.0.1:11284` |
| frank | `f` | admitted at the very end, never started | — | — |

You need six terminals — one per node, plus one to type commands in (terminal 0) — and two
browser windows, alice's and dave's, side by side.

---

## 0. Prepare (terminal 0)

**In every terminal**, from the repository root:

```sh
export DL=$HOME/tmp/dl
export BIN=$PWD/target/debug/distlib
dl() { $BIN "$@"; }
```

Build the web UI, then the binary — without a built UI every page is a placeholder that
says so:

```sh
(cd crates/distlib-ui/web && npm ci && npm run build)
cargo build
```

Create the six nodes and collect their ids:

```sh
rm -rf $DL
for n in a b c d e f; do dl -d $DL/$n init; done
for n in a b c d e f; do
  eval "$(echo $n | tr a-f A-F)=$(dl -d $DL/$n whoami | awk '/^identity/{print $2}')"
done
echo $A $B $C $D $E $F
```

Write their configuration. Alice, bob and carol get the same founding core group; dave
and erin get an empty one, which `join` fills in for them later.

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
```

Something to share: three small books, a fourth by somebody else, and one **1 GB**
recording — big enough that a download takes long enough to watch.

```sh
mkdir -p $DL/files
for f in dune dune-messiah children-of-dune left-hand; do
  yes "$f, in full" | head -20000 > $DL/files/$f.epub
done
head -c 1G /dev/urandom > $DL/files/lectures.mkv
```

**On WSL** a Windows browser reaches the nodes on `127.0.0.1` as usual, and its file
picker finds these under `\\wsl.localhost\<distro>` followed by what `echo $DL/files`
prints.

---

## 1. Three friends found the group

Bob and carol first — they will say they are in no group yet — then alice founds it:

```sh
dl -d $DL/b run                   # terminal b
dl -d $DL/c run                   # terminal c
dl -d $DL/a run --found-group     # terminal a
```

Each first line names its data directory and config file, and each says `listening
addr=127.0.0.1:1120…` with its own port. `(absent, using defaults)` after the config path
means the file is not where the node looked.

**Pass:** a `membership … members=3 core=3` line on **all three** — bob and carol were
told nothing, they received it.

```sh
dl -d $DL/b status       # group; role core member; a Raft leader
```

---

## 2. Alice opens her page

```sh
dl -d $DL/a ui
```

Open the link in the first browser window. **Pass:** the page says **Live**; the address
bar no longer shows a `#token=`; **Node** shows alice's node, the group, and three
members. Leave it open on **Node**.

---

## 3. Dave and erin join

Alice admits them, and sends them a ticket:

```sh
dl -d $DL/a admit $D --name dave      # admitted  <dave's id>
dl -d $DL/a admit $E --name erin
TICKET=$(dl -d $DL/a ticket | head -1)
dl -d $DL/d join $TICKET
dl -d $DL/e join $TICKET
```

**Pass:** each admission says `admitted` — one core member's word is enough to let
somebody in — and both appear on alice's **Node** page without a reload.

```sh
dl -d $DL/d run                   # terminal d
dl -d $DL/e run                   # terminal e
```

**Pass:** each prints a `membership … members=5 core=3` line.

```sh
dl -d $DL/d status       # role member; "follows the log to index N"; no Raft lines
dl -d $DL/d members      # five, three of them core
```

---

## 4. Alice and bob share things

From the command line — the three Dune books as one item, and bob's book from his node:

```sh
dl -d $DL/a add $DL/files/dune.epub $DL/files/dune-messiah.epub $DL/files/children-of-dune.epub \
  --kind ebook --title "Dune" --author "Frank Herbert"
dl -d $DL/b add $DL/files/left-hand.epub \
  --kind ebook --title "The Left Hand of Darkness" --author "Ursula K. Le Guin"
```

**Pass:** `added  <id>  Dune`, and the same for bob's.

And from alice's page: **Add**, choose `lectures.mkv`, title **Lectures**, type video,
**Add**. **Pass:** the bar moves steadily — the gigabyte is written twice, once as it
arrives and once into the store, so it takes a while — and the item's page opens.

---

## 5. Dave finds them and downloads

From the command line:

```sh
dl -d $DL/d search herbert
DUNE=$(dl -d $DL/d search herbert | awk '{print $1; exit}')
dl -d $DL/d item $DUNE           # three files, by name, with sizes
mkdir -p $DL/d-books
dl -d $DL/d download $DUNE --dest $DL/d-books     # fetched, three times
cmp $DL/d-books/dune.epub $DL/files/dune.epub
```

And from dave's page, in the second browser window:

```sh
dl -d $DL/d ui
```

**Pass:** the **Library** lists all three items. Search `herbert`: Dune is found, and the
address bar now carries `?q=herbert`. Clear it, open **Lectures**, **Download**: a bar
with files and bytes climbs to the end, then shows where the file went.

```sh
cmp $DL/d/downloads/lectures.mkv $DL/files/lectures.mkv
```

---

## 6. Dave corrects a detail; alice sees it

On dave's page, open **Dune**, **Edit**, set the year to **1965**, **Save**. **Pass:** the
form closes and the page shows the year. On alice's page open **Dune** — or have it open
already — **Pass:** the year appears without a reload, a moment later.

---

## 7. Erin leaves

Bob takes her out. She is not a voter, so one core member decides it:

```sh
dl -d $DL/b expel $E --reason "moved away"        # expelled  <erin's id>
```

**Pass:**

- `expelled`, not `proposed`;
- **erin's terminal** says `this node has been expelled from the group; it will stop
  following` and shuts down;
- she disappears from alice's **Node** page without a reload;
- `dl -d $DL/d members` lists four.

---

## 8. Dave is trusted with a vote

Adding a voter changes who decides things, so it takes a majority of the voters — two of
the three. Alice proposes, and that is her approval:

```sh
dl -d $DL/a core set $D --addr 127.0.0.1:11207
```

**Pass:** `proposed … waiting for core approval — 1 of 2 so far`, with the number to
approve it by. Bob looks, and agrees:

```sh
dl -d $DL/b pending              # what it is, who proposed it, 1 of 2
dl -d $DL/b approve <N>          # approved <N> — it has taken effect
```

**Pass:** dave's terminal says `the log says this node votes now; taking a seat in
consensus`, then `this node is now a voter`.

```sh
dl -d $DL/d status               # role core member; Raft role Follower (or Leader)
```

---

## 9. Carol steps back

Removing a voter also takes a majority — now three of the four. Dave proposes it, and his
own approval counts:

```sh
dl -d $DL/d expel $C --reason "stepping back"     # proposed … 1 of 3 so far
dl -d $DL/d pending              # 1 of 3 approvals (yours among them)
```

Alice agrees — and, by mistake, agrees again:

```sh
dl -d $DL/a approve <N>          # approved <N> — 2 of 3, still waiting for others
dl -d $DL/a approve <N>          # approved <N> (you had already) — 2 of 3, …
```

**Pass:** the second says so, and nothing has happened to carol yet. Bob decides it:

```sh
dl -d $DL/b approve <N>          # approved <N> — it has taken effect
dl -d $DL/a members              # three, all core: alice, bob, dave
```

Carol's terminal first gives up her seat — `this node is now a follower` — then says it
has been expelled, as erin's did, and shuts down.

---

## 10. The leader goes down

```sh
dl -d $DL/b status               # the "Raft leader" line names it
```

It is most likely alice. Ctrl-C that node's terminal, and **straight away**, from one of
the other two:

```sh
time dl -d $DL/<a survivor> admit $F --name frank
```

**Pass:** `admitted` within seconds — two of three voters are still a majority — and
`dl -d $DL/d members` lists frank.

If it was alice: her page now says **Cannot reach the node**, and keeps showing what it
had. Start her again:

```sh
dl -d $DL/a run                   # terminal a
```

**Pass:** her terminal prints a `membership` line that includes frank — she caught up on
what she missed — and her page goes back to **Live** by itself within about thirty
seconds, answering clicks again.

---

## 11. Afterwards

Ctrl-C every terminal, then:

```sh
pgrep -af distlib       # must be empty
rm -rf $DL
```

---

## Watch for, beyond pass and fail

- Does any error leave you without a next step?
- Is it obvious from `admit`, `expel` and `approve` alone whether anything happened yet?
- Does `pending` tell you enough to decide, without going to a log?
- Is a node taking its seat (§8) alarming to watch? It goes quiet for a moment.
- How long does a change take to show up on another node — on the command line, and in
  the other browser window?
- Does an item that arrives field by field look broken while it does?
- Does any page leave you without a next step — an empty library, a node that went away,
  a tab that is not signed in?
- Is it clear, after an upload's bar is full, that the node is still adding it?
- Is there anything that tells you a core node is unreachable *before* you notice it has
  stopped keeping up?
- Anything the README's quickstart gets wrong.

Write what you find down; it becomes the next round of fixes.
