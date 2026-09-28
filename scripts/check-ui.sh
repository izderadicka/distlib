#!/usr/bin/env bash
# Checks that a running distlib node serves the real web UI.
#
#   scripts/check-ui.sh <base url>     e.g. http://127.0.0.1:11280
#
# Waits up to a minute for the node to answer, then fetches `/` and the script
# bundle the page names: the page must be the built UI, not the placeholder a
# binary built without the UI shows.
set -euo pipefail

base=${1%/}
page=$(mktemp)
trap 'rm -f "$page"' EXIT

fail() {
  echo "check-ui: $*" >&2
  exit 1
}

for _ in $(seq 60); do
  curl -fsS "$base/" -o "$page" 2> /dev/null && break
  sleep 1
done
[[ -s $page ]] || fail "nothing answered at $base within a minute"

grep -q 'built without its UI' "$page" && fail "$base serves the placeholder: the UI was not embedded"
grep -q 'id="app"' "$page" || fail "$base serves a page that is not the UI: $(head -c 300 "$page")"
bundle=$(grep -o '/assets/[^"]*\.js' "$page" | head -n 1)
[[ -n $bundle ]] || fail "the page names no script bundle"
curl -fsS "$base$bundle" -o /dev/null || fail "the page's own bundle $bundle is not served"

echo "check-ui: $base serves the UI, bundle $bundle included"
