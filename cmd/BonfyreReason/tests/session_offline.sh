#!/bin/sh
# Local session lifecycle without invoking any model or provider.
set -eu
unset BONFYRE_REASON_DIR BONFYRE_REASON_ENDPOINT BONFYRE_REASON_MODEL
bin=${BIN:-$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/bonfyre-reason}
home=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-reason-offline.XXXXXX")
trap 'rm -rf "$home"' EXIT INT TERM
run() { HOME="$home" "$bin" "$@"; }
base="$home/.local/share/bonfyre/reason/offline"
run start --prompt 'A local session holds one explicit prompt.' --session offline >/dev/null
test -s "$base/prompt.txt"
if run start --prompt 'another' --session offline >/dev/null 2>&1; then echo 'duplicate prompt admitted' >&2; exit 1; fi
run branch --modes cautious,adversarial --session offline >/dev/null
run status --session offline | grep -q 'branch cautious: not run'
run status --session offline | grep -q 'branch adversarial: not run'
if run diff cautious adversarial --session offline >/dev/null 2>&1; then echo 'unrun branches compared' >&2; exit 1; fi
if run commit cautious --session offline >/dev/null 2>&1; then echo 'unrun answer committed' >&2; exit 1; fi
test ! -e "$base/answer.txt"
echo 'reason offline session: PASS'
