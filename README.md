# distlib

A distributed media library for a closed, trusted group — a handful of people who
already know each other sharing books, audiobooks and video, with no central server
and no account to sign up for.

Every member is an ed25519 keypair. There is nothing to steal server-side and nothing
to revoke except membership itself. Nodes connect peer-to-peer over
[iroh](https://iroh.computer) (QUIC, with hole-punching through relays when a direct
path is not available), and a node refuses to talk to anyone outside its allowlist —
in both directions.

See [docs/distlib-plan.md](docs/distlib-plan.md) for the design, and
[docs/plan-deltas.md](docs/plan-deltas.md) for where the implementation has
deliberately diverged from it.

> **Status: phase 0.** Identity, configuration, transport and membership enforcement
> work. There is no catalogue, no content transfer, no consensus and no UI yet — the
> only thing two nodes can currently do is prove they can reach each other. The
> allowlist is static, read from the config file; from phase 1 it comes from a
> replicated membership log.

## Build

Requires Rust 1.91 or newer.

```sh
cargo build --release
```

## Quickstart: two nodes, one ping

Each node keeps everything under a single data directory, so a second node on the
same machine is just a second `--data-dir`.

**1. Create both identities.**

```sh
distlib --data-dir /tmp/a init
distlib --data-dir /tmp/b init
```

Each prints a member id — the node's public key, and the only name it has:

```
wrote      /tmp/a/config.toml
identity   46db77a915c57d0e8861986ca17e8c6e0f1c99d077fcd5b97de1ff8874752bb6 (new)
data dir   /tmp/a
```

`init` is safe to run twice: it reports an existing identity rather than replacing
it. The key is written to `<data-dir>/keys/node.key` with mode `0600`, and a node
that finds it readable by anyone else refuses to start.

**2. Admit each other.** Membership is mutual and explicit — put each node's id in
the other's `allowlist` in `<data-dir>/config.toml`, and give A a fixed port so B
knows where to find it:

```toml
# /tmp/a/config.toml
[net]
bind_addr_v4 = "127.0.0.1:11204"
relay_mode   = "disabled"          # loopback needs no relay
allowlist    = ["<B's member id>"]
```

```toml
# /tmp/b/config.toml
[net]
bind_addr_v4 = "127.0.0.1:0"
relay_mode   = "disabled"
allowlist    = ["<A's member id>"]
```

**3. Run A**, in one terminal:

```sh
distlib --data-dir /tmp/a run
```

```
INFO node started member=46db77a915c57d0e…
INFO listening addr=127.0.0.1:11204
```

**4. Ping it from B**, in another:

```sh
distlib --data-dir /tmp/b ping <A's member id> --addr 127.0.0.1:11204
ping
```

Across the internet rather than loopback, leave `relay_mode = "default"` and drop
`--addr`: the relays handle discovery and hole-punching, and the member id is enough.

## Watching the allowlist work

Take B out of A's `allowlist`, restart A, and ping again:

```
Error: ping to 46db77a915c57d0e… failed

Caused by:
    46db77a915c57d0e… is not a member, or does not consider us one
```

A logs who tried:

```
INFO rejected a connection from a non-member peer=05469a8446acb8b4…
```

The same happens in the other direction — a node will not *dial* someone missing
from its own allowlist, and refuses before a single packet leaves the machine:

```
INFO refused to dial a non-member peer=46db77a915c57d0e… alpn=distlib/ping/0
```

That check lives on the endpoint rather than in any one protocol, which is what
makes it hold for every protocol added later.

## Commands

| Command | What it does |
|---|---|
| `distlib init [--force]` | Create the data directory, generate the identity, write a starter config. `--force` replaces an existing identity — which is how a node leaves its group. |
| `distlib run` | Run the node until `Ctrl-C`. |
| `distlib status [--online]` | Identity, paths, relay mode, allowlist size. `--online` also binds and prints the dialable address. |
| `distlib ping <member> [--addr] [--relay]` | Send a ping and wait for the echo. |

Global flags: `--data-dir/-d`, `--config/-c`, `--verbose/-v` (repeat for more).

## Configuration

`<data-dir>/config.toml`, written by `init` with comments. Any key can be overridden
by an environment variable — prefix `DISTLIB_`, separate nested keys with a double
underscore:

```sh
DISTLIB_NET__BIND_ADDR_V4=0.0.0.0:11204 distlib run
```

The data directory is the one thing that cannot be configured in the file, since the
file lives inside it. Use `--data-dir` or `DISTLIB_DATA_DIR`; setting `data_dir` in
the config is an error rather than being silently ignored.

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
