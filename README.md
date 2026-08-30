# distlib

A distributed media library for a closed, trusted group — a handful of people who
already know each other sharing books, audiobooks and video, with no central server
and no account to sign up for.

Every member is an ed25519 keypair. There is nothing to steal server-side and nothing
to revoke except membership itself. Nodes connect peer-to-peer over
[iroh](https://iroh.computer) (QUIC, with hole-punching through relays when a direct
path is not available), and a node refuses to talk to anyone outside the group — in
both directions.

Who is in the group is decided by the group. Members are admitted and expelled through
a signed, replicated log that every node folds into the same answer, and each node
enforces *its own copy* of that answer. There is no administrator and no config file
to edit.

See [docs/distlib-plan.md](docs/distlib-plan.md) for the design, and
[docs/plan-deltas.md](docs/plan-deltas.md) for where the implementation has
deliberately diverged from it.

> **Status: phase 1a.** Identity, transport, and the membership log work: a group can
> be founded, members admitted and expelled, and every node derives what it will talk
> to from the committed log. There is no catalogue, no content transfer and no UI yet.
> Members who are not part of the core group cannot yet follow the log — that is
> phase 1b — so for now every member is a founder.

## Build

Requires Rust 1.91 or newer.

```sh
cargo build --release
```

## Quickstart: found a group

Each node keeps everything under a single data directory, so a second node on the same
machine is just a second `--data-dir`.

**1. Create both identities.**

```sh
distlib --data-dir /tmp/a init
distlib --data-dir /tmp/b init
```

Each prints a member id — the node's public key, and the only name it has:

```
wrote      /tmp/a/config.toml
identity   07238d74e704f99a52d4616ebea33e2f044d89271d355b2685c632af3bfefc92 (new)
data dir   /tmp/a
```

`init` is safe to run twice: it reports an existing identity rather than replacing it.
The key is written to `<data-dir>/keys/node.key` with mode `0600`, and a node that
finds it readable by anyone else refuses to start.

**2. Pin a port**, in each `<data-dir>/config.toml`. Founding writes each node's
address into the log, and an OS-chosen port is a different port after the next restart:

```toml
[net]
bind_addr_v4 = "127.0.0.1:11204"   # 11205 for B
relay_mode   = "disabled"          # loopback needs no relay
```

**3. Ask each node who it is.** Founding needs every founder's id and address, and
there is no group yet to ask — so this exchange happens out of band, once:

```sh
distlib --data-dir /tmp/b whoami
```

```
identity   d7e1c2bc242366f2fd5a8221ac32bd5b252525aecd7b03b9d495356c37aa6d84
data dir   /tmp/b

Send this to whoever is founding the group, for their [consensus] core:

  { member = "d7e1c2bc…", name = "", addrs = ["127.0.0.1:11205"] }
```

`whoami` creates the identity if there is not one yet, so it is all a founder's friends
need to run. It prints the member id, never the secret key.

**4. Agree on the founding core group.** This is the one thing that cannot come from
the log, because it is what makes reading the log possible — founders have to reach
each other to replicate the first entry, and they will not connect to anyone they have
not been told about. Whoever is founding collects the lines, adds names, and sends the
finished block back, so it is identical in *every* founder's config:

```toml
[consensus]
core = [
  { member = "<A's id>", name = "alice", addrs = ["127.0.0.1:11204"] },
  { member = "<B's id>", name = "bob",   addrs = ["127.0.0.1:11205"] },
]
```

That scales to however many of you there are: three friends who all want a say run
`whoami`, one of them assembles the list, and all three configs end up the same. Across
the internet rather than loopback, leave `relay_mode = "default"` and drop `addrs` — the
relays handle discovery and hole-punching, and the member id is enough.

**5. Start B**, which has nothing to do but wait:

```sh
distlib --data-dir /tmp/b run
```

```
INFO node started member=d7e1c2bc242366f2fd5a8221ac32bd5b252525aecd7b03b9d495356c37aa6d84
INFO listening addr=127.0.0.1:11205
WARN this node is in no group; found one with `distlib run --found-group`, or wait for a founder to admit it
```

**6. Found the group from A**, in another terminal:

```sh
distlib --data-dir /tmp/a run --found-group
```

```
INFO founding the group founders="alice (07238d74…), bob (d7e1c2bc…)"
INFO membership group=a287e5e86a780cd4… members=2 core=2 who="alice (07238d74…), bob (d7e1c2bc…)"
```

B logs the same line a moment later, without having been told anything: it received
the founding entry by replication and folded it into the same membership. A couple of
`WARN` lines from openraft around the election are normal — it logs its own bookkeeping
at that level.

Run `--found-group` on exactly one founder, once. The others just `run`.

**7. Look at the group.** Stop a node and ask it who it thinks is in the group:

```sh
distlib --data-dir /tmp/b members
```

```
group      a287e5e86a780cd49f6269e794faaaa2e3d3c28cc6f7c8d26d73bd50e9168529
members    2 (2 core)
  alice (07238d74e704f99a52d4616ebea33e2f044d89271d355b2685c632af3bfefc92)  core
  bob (d7e1c2bc242366f2fd5a8221ac32bd5b252525aecd7b03b9d495356c37aa6d84)  core
```

It needs the node stopped: the database is held exclusively by the running process.
While a node runs, it logs the membership on every change instead — the `INFO
membership` line above is how you watch a group live.

## Watching membership do its job

Membership is not advisory. Start A again, and have a node that is in no group try to
reach it:

```sh
distlib --data-dir /tmp/c ping <A's id> --addr 127.0.0.1:11204
```

```
Error: ping to 07238d74e704f99a52d4616ebea33e2f044d89271d355b2685c632af3bfefc92 failed

Caused by:
    07238d74e704f99a52d4616ebea33e2f044d89271d355b2685c632af3bfefc92 is not a member, or does not consider us one
```

A logs who tried:

```
INFO rejected a connection from a non-member peer=000f146b2481608c…
```

while B, who is in the log, is answered:

```sh
distlib --data-dir /tmp/b ping <A's id> --addr 127.0.0.1:11204
ping
```

Nothing in A's config file mentions C, or B. A admits B and refuses C entirely because
of what its copy of the log says.

The same holds in the other direction — a node will not *dial* a non-member either, and
refuses before a packet leaves the machine. There is no way to provoke that from the
commands above (`ping` treats the id you typed as consent, and nothing else dials a
non-member), so it is covered by `distlib-net`'s tests rather than shown here.

Both checks live on the endpoint rather than in any one protocol, which is what makes
them hold for every protocol added later.

## Commands

| Command | What it does |
|---|---|
| `distlib init [--force]` | Create the data directory, generate the identity, write a starter config. `--force` replaces an existing identity — which is how a node leaves its group. |
| `distlib whoami` | Print this node's id as a line for a founder's `[consensus] core`. Creates the identity if there is not one. |
| `distlib run` | Run the node until `Ctrl-C`. |
| `distlib run --found-group` | Run, founding the group in `[consensus] core` first. One founder, once. |
| `distlib members` | List the group as this node's log has it. Needs the node stopped. |
| `distlib status [--online]` | Identity, paths, relay mode, group and standing. `--online` also binds and prints the dialable address. |
| `distlib ping <member> [--addr] [--relay]` | Send a ping and wait for the echo. |

Global flags: `--data-dir/-d`, `--config/-c`, `--verbose/-v` (repeat for more).

`-v` also turns openraft's own logging up; it is kept at `warn` by default, where it
would otherwise bury everything else.

## Configuration

`<data-dir>/config.toml`, written by `init` with comments. Any key can be overridden
by an environment variable — prefix `DISTLIB_`, separate nested keys with a double
underscore:

```sh
DISTLIB_NET__BIND_ADDR_V4=0.0.0.0:11204 distlib run
```

`[consensus] core` is the only place membership is ever configured, it is only read
before a group exists, and it is never consulted again once one does. Everything else
about who belongs comes from the log.

The data directory is the one thing that cannot be configured in the file, since the
file lives inside it. Use `--data-dir` or `DISTLIB_DATA_DIR`; setting `data_dir` in the
config is an error rather than being silently ignored.

## Development

```sh
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace
```

The test suite never contacts the public relay or DNS infrastructure — endpoints are
either given explicit addresses or pointed at an in-process relay, so the suite is
deterministic and works offline. Engineering standards are in
[CLAUDE.md](CLAUDE.md); changes go through pull requests.

## Licence

MIT or Apache-2.0, at your option.
