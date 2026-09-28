#!/usr/bin/env bash
# Checks that a distlib binary, on its own, serves the real web UI.
#
#   scripts/smoke-test.sh <path to distlib>
#
# Copies the binary into an empty directory, starts it there as a fresh node,
# and runs `check-ui.sh` against it. What a release ships is the binary alone,
# so nothing beside it may be what makes this pass.
set -euo pipefail

check=$(cd "$(dirname "$0")" && pwd)/check-ui.sh
binary=$1
port=11299
work=$(mktemp -d)
pid=

cleanup() {
  if [[ -n $pid ]]; then
    kill "$pid" 2> /dev/null || true
    wait "$pid" 2> /dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

mkdir "$work/bin"
cp "$binary" "$work/bin/"
node=$work/bin/$(basename "$binary")
cd "$work"

"$node" --data-dir "$work/data" init > /dev/null
DISTLIB_API__BIND_ADDR=127.0.0.1:$port "$node" --data-dir "$work/data" run > "$work/node.log" 2>&1 &
pid=$!

if ! "$check" "http://127.0.0.1:$port"; then
  sed 's/^/  node: /' "$work/node.log" >&2
  exit 1
fi
