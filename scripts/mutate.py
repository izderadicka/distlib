#!/usr/bin/env python3
"""Checks that the tests catch hand-written mutations of the code.

    scripts/mutate.py <mutations.toml>

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

Exits non-zero if a mutation survives, does not build, or is written wrongly.
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


def untracked() -> set[str]:
    listed = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard"], cwd=ROOT, capture_output=True, text=True, check=True
    )
    return set(listed.stdout.splitlines())


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    mutations = tomllib.loads(Path(sys.argv[1]).read_text())["mutation"]
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
