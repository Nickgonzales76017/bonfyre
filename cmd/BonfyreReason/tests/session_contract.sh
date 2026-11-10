#!/bin/sh
# What this proves: a branch is a model run. Its answer is what the endpoint
# returned, a branch that was not run has none, and a dead endpoint leaves none.
#   BONFYRE_REASON_ENDPOINT=http://127.0.0.1:7000 BONFYRE_REASON_MODEL=<id> sh tests/session_contract.sh
set -eu
bin=${BIN:-$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/bonfyre-reason}
: "${BONFYRE_REASON_ENDPOINT:?set BONFYRE_REASON_ENDPOINT to a running chat-completions endpoint}"
: "${BONFYRE_REASON_MODEL:?set BONFYRE_REASON_MODEL to a model that endpoint serves}"
home=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-reason-contract.XXXXXX")
trap 'rm -rf "$home"' EXIT INT TERM
run() { HOME="$home" "$bin" "$@"; }
base="$home/.local/share/bonfyre/reason/first"

run start --prompt 'Name one risk of charging a saved card with no approval step.' --session first >/dev/null
test -s "$base/prompt.txt"
if run start --prompt 'another' --session first >/dev/null 2>&1; then echo 'a session took a second prompt' >&2; exit 1; fi

run branch --modes cautious,adversarial --session first >/dev/null
run status --session first | grep -q 'branch cautious: not run'
if run diff cautious adversarial --session first >/dev/null 2>&1; then echo 'compared branches that were never run' >&2; exit 1; fi
if run commit cautious --session first >/dev/null 2>&1; then echo 'committed an answer that does not exist' >&2; exit 1; fi

run run --max-tokens 96 --session first >/dev/null
test -s "$base/runs/cautious.txt"
test -s "$base/runs/adversarial.txt"
grep -q "^model $BONFYRE_REASON_MODEL\$" "$base/runs/cautious.receipt"
# the receipt's digest is the digest of the answer on disk
want=$(sed -n 's/^answer_sha256 //p' "$base/runs/cautious.receipt")
got=$(shasum -a 256 "$base/runs/cautious.txt" | cut -d' ' -f1)
[ "$want" = "$got" ]
run diff cautious adversarial --session first | grep -q '^shared words:'
run commit adversarial --session first >/dev/null
cmp "$base/answer.txt" "$base/runs/adversarial.txt"

# creative was never branched, so it was never run
[ ! -e "$base/runs/creative.txt" ]
# an endpoint that does not answer leaves the branch without a result
if run run --endpoint http://127.0.0.1:1 --mode cautious --session first >/dev/null 2>&1; then exit 1; fi
[ ! -e "$base/runs/cautious.txt" ]

echo "reason session contract: PASS"
