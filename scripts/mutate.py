#!/usr/bin/env python3
"""Checks that the tests catch hand-written mutations of the code.

    scripts/mutate.py <mutations.toml> [base]

Each mutation replaces one piece of text in one file:

    [[mutation]]
    name = "uploads deleted before hashing"
    file = "crates/distlib-api/src/methods.rs"
    find = "let (uploaded, _staged) ="
    replace = "let (uploaded, _) ="
    tests = ["rpc", "library"]

`tests` names the test targets that should catch it: the files in a crate's
`tests/`, or "lib" for the unit tests. Only those are built, which is most of
the time saved. A file under crates/distlib-ui/web/ is a UI file, and its
`tests` are Vitest test files, relative to that directory:
`tests = ["src/OwnText.test.ts"]`.

`find` has to occur exactly once. The mutated code runs only the tests named,
stopping at the first failure. A mutation they do not catch is run against the
whole suite before it is called a survivor, so naming too few tests only costs
time. A test that hangs is stopped and counts as a failure. The file is put
back after each mutation, whatever happens.

Before any mutation runs, every block of code this branch changed since it
left `base` (master if not given) has to be touched by one: a block is a run of changed lines
without a blank or an unchanged line in it, in a .rs, .ts or .svelte file that
is not a test. Comments and imports do not count. A block that has no
mutation worth making is listed, with why, instead:

    [[unmutated]]
    file = "crates/distlib-api/src/methods.rs"
    find = "struct Download {"
    reason = "a params struct: serde's own tests cover it"

Any block left without either is printed, and nothing runs.

An entry without one of its keys, or naming a file that is not there, is
reported before anything else, and nothing runs.

Exits non-zero if a block is left out, or if a mutation survives, does not
build, or is written wrongly.
"""

import os
import re
import subprocess
import sys
import time
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WEB = ROOT / "crates/distlib-ui/web"

# nextest's exit code for a run in which a test failed.
TESTS_FAILED = 100

NEXTEST = ["cargo", "nextest", "run", "--workspace", "--all-features", "--profile", "mutants"]
VITEST = ["npx", "vitest", "run", "--bail", "1"]


def rust_run(targets: list[str] | None) -> tuple[str, str]:
    """Runs nextest, on the test `targets` or all of them: an outcome and its detail."""
    selection = [arg for target in targets or [] for arg in (["--lib"] if target == "lib" else ["--test", target])]
    done = subprocess.run(NEXTEST + selection, cwd=ROOT, capture_output=True, text=True)
    output = done.stdout + done.stderr
    if done.returncode == 0:
        return "passed", ""
    if done.returncode == TESTS_FAILED:
        failed = re.search(r"^\s*(FAIL|TIMEOUT|SIG\w+) \[.*?\] (?:\(.*?\) )?(\S+ \S+)", output, re.M)
        return "caught", f"{failed.group(1)} {failed.group(2)}" if failed else "a test failed"
    return "broken", first_error(output)


def ui_run(test_files: list[str] | None) -> tuple[str, str]:
    """Runs Vitest, on `test_files` or all of them: an outcome and its detail."""
    done = subprocess.run(VITEST + (test_files or []), cwd=WEB, capture_output=True, text=True)
    output = done.stdout + done.stderr
    if done.returncode == 0:
        return "passed", ""
    failed = re.search(r"^\s*FAIL\s+(.+)$", output, re.M)
    if failed:
        return "caught", failed.group(1).strip()
    return "broken", first_error(output)


def first_error(output: str) -> str:
    """The first error in a build's or a run's output, or its end if none is marked."""
    lines = output.splitlines()
    start = next((at for at, line in enumerate(lines) if line.lstrip().lower().startswith("error")), len(lines) - 12)
    return "\n".join(lines[max(start, 0) : start + 12])


def check(mutation: dict) -> tuple[str, str]:
    """Applies `mutation`, runs its tests and then, if they pass, all; puts the file back."""
    path = ROOT / mutation["file"]
    original = path.read_text()
    found = original.count(mutation["find"])
    if found != 1:
        return "bad mutation", f"`find` occurs {found} times in {mutation['file']}"
    run = ui_run if path.is_relative_to(WEB) else rust_run
    path.write_text(original.replace(mutation["find"], mutation["replace"]))
    try:
        outcome, detail = run(mutation["tests"])
        if outcome == "passed":
            outcome, detail = run(None)
            if outcome == "caught":
                detail += " (only by the whole suite)"
        return ("survived", "") if outcome == "passed" else (outcome, detail)
    finally:
        path.write_text(original)
        # A newer timestamp, so the next build does not reuse the mutant.
        os.utime(path)


TEST_PATHS = re.compile(r"/tests/|/e2e/|\.test\.ts$|/testing\.ts$|/test-setup\.ts$")


def changed_blocks(base: str) -> list[tuple[str, int, int]]:
    """Every block of code changed since `base`, as (file, first line, last line)."""
    base = git("merge-base", base, "HEAD").strip()
    changed: dict[str, set[int]] = {}
    file = None
    for line in git("diff", "-U0", "-w", "--diff-algorithm=histogram", base).splitlines():
        if line.startswith("+++ "):
            file = line[6:] if line.startswith("+++ b/") else None
        elif line.startswith("@@") and file:
            start, _, count = re.search(r"\+(\d+)(,(\d+))?", line).groups()
            first, length = int(start), int(count) if count is not None else 1
            changed.setdefault(file, set()).update(range(first, first + length))
    for file in untracked():
        if (ROOT / file).is_file():
            changed[file] = set(range(1, len((ROOT / file).read_text().splitlines()) + 1))
    blocks = []
    for file, numbers in sorted(changed.items()):
        if not file.endswith((".rs", ".ts", ".svelte")) or TEST_PATHS.search("/" + file):
            continue
        kinds = line_kinds(file, (ROOT / file).read_text().splitlines())
        block: list[int] = []
        for number, kind in enumerate(kinds, start=1):
            if kind == "code" and number in numbers:
                block.append(number)
            elif kind != "neutral" and block:
                blocks.append((file, block[0], block[-1]))
                block = []
        if block:
            blocks.append((file, block[0], block[-1]))
    return blocks


def line_kinds(file: str, lines: list[str]) -> list[str]:
    """Each line as "code", "blank" (ends a block), or "neutral": a comment, an import,
    brackets alone, a test."""
    kinds = []
    in_import = in_tests = False
    for line in lines:
        text = line.strip()
        if file.endswith(".rs") and text == "#[cfg(test)]":
            in_tests = True
        starts_import = re.match(r"(pub(\(crate\))? )?use |import ", text) is not None
        closing = text and not text.strip("{}()[];,")
        if in_tests or in_import or starts_import or closing or text.startswith(("//", "/*", "*", "<!--")):
            kinds.append("neutral")
        else:
            kinds.append("code" if text else "blank")
        if starts_import or in_import:
            in_import = not (text.endswith(";") or " from " in text or text.startswith("} from"))
    return kinds


def lines_of(entry: dict) -> tuple[int, int] | None:
    """The lines an entry's `find` spans in its file, if it occurs once."""
    text = (ROOT / entry["file"]).read_text()
    if text.count(entry["find"]) != 1:
        return None
    first = text[: text.index(entry["find"])].count("\n") + 1
    return first, first + entry["find"].count("\n")


def unmutated(blocks: list[tuple[str, int, int]], entries: list[dict]) -> list[tuple[str, int, int]]:
    """The blocks no entry's `find` touches."""
    spans = [(entry["file"], lines_of(entry)) for entry in entries]
    return [
        (file, first, last)
        for file, first, last in blocks
        if not any(at == file and span and span[0] <= last and first <= span[1] for at, span in spans)
    ]


def git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout


def untracked() -> set[str]:
    return set(git("ls-files", "--others", "--exclude-standard").splitlines())


REQUIRED = {"mutation": ("name", "file", "find", "replace", "tests"), "unmutated": ("file", "find", "reason")}


def problems(plan: dict) -> list[str]:
    """What is missing from the plan's entries: a key, or the file named."""
    found = []
    for table, keys in REQUIRED.items():
        for number, entry in enumerate(plan.get(table, []), start=1):
            where = f"[[{table}]] {number} ({entry.get('name') or entry.get('file', '?')})"
            found += [f"{where}: no `{key}`" for key in keys if key not in entry]
            if "file" in entry and not (ROOT / entry["file"]).is_file():
                found.append(f"{where}: no file {entry['file']}")
    return found


def main() -> int:
    if len(sys.argv) not in (2, 3):
        print(__doc__, file=sys.stderr)
        return 2
    plan = tomllib.loads(Path(sys.argv[1]).read_text())
    if found := problems(plan):
        print("\n".join(found), file=sys.stderr)
        return 2
    mutations = plan.get("mutation", [])
    for entry in plan.get("unmutated", []):
        print(f"{'UNMUTATED':>12}  {entry['file']}: {entry['reason']}")
    base = sys.argv[2] if len(sys.argv) == 3 else "master"
    left_out = unmutated(changed_blocks(base), mutations + plan.get("unmutated", []))
    for file, first, last in left_out:
        code = (ROOT / file).read_text().splitlines()[first - 1].strip()
        print(f"{'NO MUTATION':>12}  {file}:{first}-{last}  {code[:70]}")
    if left_out:
        print(f"\n{len(left_out)} changed blocks have no mutation; add one, or list them as [[unmutated]]")
        return 1
    before = untracked()
    results = []
    for mutation in mutations:
        started = time.monotonic()
        outcome, detail = check(mutation)
        results.append(outcome)
        took = f"{time.monotonic() - started:.0f}s"
        print(f"{outcome.upper():>12}  {mutation['name']} ({took})" + (f"\n{'':>14}{detail}" if detail else ""), flush=True)
    # A failing property test leaves its regressions file behind.
    for stray in untracked() - before:
        if stray.endswith(".proptest-regressions"):
            (ROOT / stray).unlink()
    caught = results.count("caught")
    print(f"\n{caught} of {len(results)} caught")
    return 0 if caught == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
