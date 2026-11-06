#!/bin/sh
set -eu

binary=${1:?agent-contract binary required}
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-agent-contract.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

# The frontier crosses provider/native-session, compaction, strategy, and an
# external turn.  It is only valid when the same sequence/origin evidence has
# all of the kind-specific identities the native contract requires.
frontier() {
    transport=${1-https:external-review}
    printf '%s\n' \
      'session|agent:source-release|profile:codex-operator|work:source-release' \
      'receipt|receipt:source-release|work:source-release|occurrence:source-release|completed|1|4' \
      'event|1|model_turn|subject|work:source-release|2026-09-24T14:23:00Z|codex|gpt-5|verify:host:turn-1|carrier:codex:thread-1||||||sha256:turn-1|' \
      'event|2|context_compaction|host|work:source-release|2026-09-24T14:24:00Z|context-compiler||||context:full|context:cut-1|||policy:bounded-context|sha256:compaction|' \
      'event|3|strategy_change|host|work:source-release|2026-09-24T14:25:00Z|agent-session|||||strategy:compose-all|strategy:source-depth-cut|observation:affected-closure|sha256:strategy|' \
      "event|4|model_turn|external|work:release-review|2026-09-24T14:26:00Z|external-review|review-model|verify:external:turn-4|carrier:external:session-4||||||sha256:turn-4|$transport"
}
frontier | "$binary" > "$tmp/out"
grep -q '"events":4' "$tmp/out"
grep -q '"session_valid":"ok"' "$tmp/out"
grep -q '"receipt_covers_session":"ok"' "$tmp/out"

# External origin without its transport coordinate is a carrier assertion, not
# a valid ReceiptEnvelope; the adapter must refuse rather than guess one.
if frontier '' | "$binary" >/dev/null 2>&1; then
    echo 'agent contract: external turn without transport was accepted' >&2
    exit 1
fi
echo 'agent receipt frontier: PASS'
