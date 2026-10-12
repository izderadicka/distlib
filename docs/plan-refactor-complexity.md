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
  - **Done.** `fetch_content_nobody_offered` now has a generation type, `AskedThisGeneration`, and
    two steps, `wait_for_the_next_round` and `fetch_one`. Its 43-line doc comment is down to two
    short paragraphs. `ask_around` is now `fetch_log_from_core_nodes`, and the `heard` field is
    `signed_addresses`.

## Coverage gaps

- **T1, CLI tests** through `process.rs`:
  - `init`: new, existing, `--force` replacing the identity, the config written once;
  - `status`: no identity yet, and the node not running;
  - `ping`: against a running node, if it is cheap.
  - **Done** (`tests/node_commands.rs`, about a second each):
    - `init` new; `init` again, which keeps the identity and an edited config; `init --force`,
      with its warning.
    - `status` with no identity; not running and in no group; a stopped founder's role, read from
      the database.
    - `ping` from an admitted member, echoed. A member nobody admitted is refused.
  - `run`: **done** (`tests/running.rs`, and the clean-stop test in `founding.rs`).
    - An expelled node exits with its error, as one sentence.
    - With the API turned off, nothing listens on its port.
    - A node in no group says how to get into one, and keeps running.
    - A refused founding is reported, and the node shut down rather than dropped.
    - A clean stop shuts the runtime down.

    Of R2's seven survivors, six are now caught. The seventh, "the API server dropped as soon as
    started", is caught since dropping a `Server` stops it (`impl Drop`, replacing `shutdown()`).
- **T2, `commit_as_follower` (50%)**: its fallbacks — first candidate unreachable, all
  unreachable, rejected. It also carries the question raised in #110: the doc comment says a core
  node that is not the leader forwards through its own Raft, and the code does not. **A correctness
  item: it may move ahead of this plan.**
  - **Done.** A core node that is not the leader now hands a member's proposal on to the leader,
    one hop only: a proposal from another core node is not handed on again, because that node's
    own Raft finds the leader. Tests in `distlib-consensus/tests/node.rs`:
    - a non-voter asking a core node that is not the leader: committed; a refusal comes back as
      a refusal; a core node's own proposal is not handed on;
    - a follower hears a refusal as the rules' verdict, not as a core node that failed;
    - a follower that reaches no core node names the one it tried (slow lane).
  - Not covered: "first candidate unreachable, the next one commits". The leader is always
    tried first and the sources are rewritten after every fetch, so a test could not pin that
    order without depending on timing.
- **Later**: a Raft snapshot sent between nodes. `RaftClient::install_snapshot` is at 0%; the state
  machine side is tested (`distlib-consensus/tests/state_machine.rs`).
  - **Done** (`a_node_promoted_after_the_log_was_purged_is_sent_a_snapshot`). It found a bug: a
    promoted node installing a snapshot taken before its promotion saw itself as not core,
    stood down, and looped. Now only a membership newer than the one that seated a node can
    stand it down.

## The guard

- Turn on `clippy::too_many_lines`, with `too-many-lines-threshold` in `clippy.toml`. The number
  gets set after R1–R3, somewhere around 60–80. It counts code lines, not comments, and runs in the
  clippy step CI already has.
- Not `cognitive_complexity`: it is in the nursery, and it would flag the flat matches above.
- cargo-crap stays a pointer that someone runs by hand now and then, not a CI gate.
- **Done.** `too_many_lines = "warn"` is in the workspace lints, and `too-many-lines-threshold =
  80` in `clippy.toml`; CI's `-D warnings` makes it an error.
  - The five functions over 80 say why with `#[expect(clippy::too_many_lines, reason = ...)]`, so
    clippy fails once one is shortened and the exemption is no longer needed:
    - `main` (a flat match) and `Config::to_starter_toml` (a template) stay as they are;
    - `consensus::node::start` (137), `Runtime::start` (91) and `commands::item` (89) are refactor
      candidates.
  - Test files with scenario tests over 80 lines allow the lint at the file level, as they already
    allow `unwrap_used`.

## Order

R1 → R2 → R3 → T1 → the guard → T2. All done.
