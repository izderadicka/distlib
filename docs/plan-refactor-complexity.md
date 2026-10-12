# Refactoring for complexity and coverage (CRAP)

A plan, not done yet. **Scheduled after 4c-7, before phase 5**, as a run of small PRs that change no
behaviour.

---

## Context

A CRAP run on master at `5777fb9` (4c-2c merged):

```sh
cargo llvm-cov nextest --workspace --all-features --lcov --output-path lcov.info
cargo crap --lcov lcov.info --min 15 --exclude '**/tests/**'
```

`lcov.info` includes the tests that run the `distlib` binary as a subprocess
(`crates/distlib/tests/common/process.rs`). So a CLI function at 0% really is never run.

The run found 18 functions over 15 and 5 over the default threshold of 30. CRAP is
CC² · (1 − coverage)³ + CC, so a function scores high for one of two reasons:

- **No coverage:** `init` (72), `print_stored_status` (42), `ping` (42) and `status` (38), all in
  `crates/distlib/src/commands.rs`.
- **Fully covered but branchy:** these score exactly their CC. `Api::call` (33) is an example.

A third finding is not in the numbers: **long functions that do several things in a row, with
comments that explain at essay length what step names should say.**

## Left alone on purpose

Flat, exhaustive matches, one arm per case. Splitting them would scatter a table that reads well in
one place:

- `Api::call` (`distlib-api/src/methods.rs`)
- `main` (`distlib/src/main.rs`)
- `CommunityKey::parse`, `Key::parse` (`distlib-core`)
- `MembershipState::apply` (`distlib-consensus/src/state.rs`)
- gossip `listen` (`distlib-consensus/src/gossip.rs`)

## Refactors: split into named steps

Each function becomes a short body that calls steps whose names say what they do. On the way, its
comments are cut to a sentence or two. One PR per group.

- **R1, `distlib-api`**
  - `Api::download` (189 lines, about 70 of them doc comment) → destination / files to download /
    plan targets (name collisions, the no-overwrite check) / start the task / the background finish
    (recheck holdings).
  - `Api::add` (130) → take uploads / build the item from its files (filename collision) / add
    missing files to an existing item / write a new one.
  - **Done.** In code lines, `add` went from 90 to 10 and `download` from 124 to 35. The steps:
    `take_uploads`, `files_or_uploads`, `item_from_files`, `add_missing_files`, `write_new_item`;
    and `download_destination`, `files_to_download`, `targets_in`, `providers_of`,
    `finish_download_in_background`, `recheck_holdings`. The longest is `targets_in`, at 39.
- **R2, `distlib-store` and the CLI**
  - `upsert_item` (77): one helper for "delete the rows of X, insert them again". Files, ratings,
    reviews, bookmarks and wishes all repeat it now.
  - `commands::run` (118) → build the API from the runtime / wait for a stop or an expulsion / shut
    down.
  - **Done.** `OwnedRows` in `store.rs` is a table whose rows belong to one key, replaced together.
    Files, ratings, reviews, bookmarks and wish entries and comments use it. In code lines,
    `commands::run` went from 78 to 36: `found_or_say_there_is_no_group`, `start_local_api`,
    `spawn_reporters`, `wait_for_stop_or_expulsion`. `members` is replaced whole rather than by
    key, so it stays as it was.
- **R3, sync and consensus**
  - `fetch_content_nobody_offered` (97, `distlib-sync/src/catalogue.rs`) → wait for the next round
    / pick providers / fetch one hash.
  - Rename `ask_around` (`distlib-consensus/src/raft/follower.rs`) to say what it does, e.g.
    `fetch_log_from_core_nodes`. Also rename `heard` in `distlib-net/src/directory.rs`.

## Coverage gaps

- **T1, CLI tests** through `process.rs`:
  - `init`: new, existing, `--force` replacing the identity, the config written once;
  - `status`: no identity yet, and the node not running;
  - `ping`: against a running node, if it is cheap.
  - `run`. These are the paths R2's mutations showed no test reaches: an expelled node exits with
    its error; a node with the API disabled serves none; a node in no group says so; a founding
    that fails is reported, and the node shut down; the API server is shut down with the node; and
    the runtime's shutdown keeps what a clean stop should keep (the blob store's records).
- **T2, `commit_as_follower` (50%)**: its fallbacks — first candidate unreachable, all
  unreachable, rejected. It also carries the question raised in #110: the doc comment says a core
  node that is not the leader forwards through its own Raft, and the code does not. **A correctness
  item: it may move ahead of this plan.**
- **Later**: a Raft snapshot sent between nodes. `RaftClient::install_snapshot` is at 0%; the state
  machine side is tested (`distlib-consensus/tests/state_machine.rs`).

## The guard

- Turn on `clippy::too_many_lines`, with `too-many-lines-threshold` in `clippy.toml`. The number
  gets set after R1–R3, somewhere around 60–80. It counts code lines, not comments, and runs in the
  clippy step CI already has.
- Not `cognitive_complexity`: it is in the nursery, and it would flag the flat matches above.
- cargo-crap stays a pointer that someone runs by hand now and then, not a CI gate.

## Order

R1 → R2 → R3 → T1 → the guard. T2 when Ivan decides.
