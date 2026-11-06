#!/bin/sh
# Uses the real native tokenizer bridge. By default a declared byte-level BPE
# fixture makes this repeatable; BONFYRE_TOKENIZER selects an actual model's
# tokenizer for its separate integration check. Neither run loads a model.
#   BONFYRE_GIGATOKEN_BIN=... BONFYRE_TOKENIZER=.../tokenizer.json sh tests/context_contract.sh
set -eu

here=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
bin=${BIN:-$here/bonfyre-kvcache}
bridge=${BONFYRE_GIGATOKEN_BIN:-$here/../BonfyreContextCut/bonfyre-gigatoken-native}
tokenizer=${BONFYRE_TOKENIZER:-}
[ -x "$bridge" ] || { echo "no tokenizer bridge at $bridge" >&2; exit 1; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-kv-contract.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
if [ -z "$tokenizer" ]; then
    tokenizer="$tmp/byte-bpe-fixture.tokenizer.json"
    node "$here/tests/tokenizer_fixture.mjs" "$tokenizer"
    printf '%s\n' 'context tokenizer: native GigaToken with byte-BPE fixture (not model-quality evidence)'
    printf 'the and\n' > "$tmp/probe.txt"
    "$bridge" "$tokenizer" "$tmp/probe.txt" > "$tmp/probe.bin"
    node --input-type=module - "$tmp/probe.bin" <<'JS'
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';
const bytes=readFileSync(process.argv[2]);
assert.equal(bytes.length,24);
assert.deepEqual(Array.from({length:6},(_,i)=>bytes.readUInt32LE(i*4)),[1,4,257,32,259,10]);
console.log('native BPE probe: 8 source bytes -> 4 exact tokens, four merge rules exercised');
JS
else
    printf '%s\n' "context tokenizer: explicit $tokenizer"
fi

# Bodies are this repository's own working agreement, one section per body.
mkdir "$tmp/bodies"
awk -v d="$tmp/bodies" '/^## /{n++} {printf "%s\n", $0 > sprintf("%s/%03d.md", d, n)}' "$here/../../CLAUDE.md"
[ "$(ls "$tmp/bodies" | wc -l)" -ge 4 ]
keep=$(awk 'index($0,"No maturity laundering."){print; exit}' "$here/../../CLAUDE.md")
[ -n "$keep" ]
grep -F -q -- "$keep" "$here/../../CLAUDE.md"
printf '%s\n' "$keep" > "$tmp/witnesses"

compile() { "$bin" context-compile --bodies "$tmp/bodies" --tokenizer "$tokenizer" --gigatoken-bin "$bridge" "$@"; }

compile --witnesses "$tmp/witnesses" --token-budget 100000 --out "$tmp/all.txt" > "$tmp/all.json"
grep -q '"verdict": "PASS"' "$tmp/all.json"
grep -q '"witnesses_preserved": 1' "$tmp/all.json"
grep -F -q -- "$keep" "$tmp/all.txt"

# A budget too small for everything still keeps the body the sentence is in.
need=$(sed -n 's/.*"tokens": \([0-9]*\), "required": true.*/\1/p' "$tmp/all.json" | head -1)
[ "$need" -gt 0 ]
compile --witnesses "$tmp/witnesses" --token-budget "$((need + 8))" --out "$tmp/tight.txt" > "$tmp/tight.json"
grep -q '"verdict": "PASS"' "$tmp/tight.json"
grep -F -q -- "$keep" "$tmp/tight.txt"
[ "$(wc -c < "$tmp/tight.txt")" -lt "$(wc -c < "$tmp/all.txt")" ]
written=$(sed -n 's/.*"context": {.*"tokens": \([0-9]*\)}.*/\1/p' "$tmp/tight.json")
[ "$written" -le "$((need + 8))" ]

# A budget below the required body is refused, and nothing is left behind.
if compile --witnesses "$tmp/witnesses" --token-budget "$((need - 1))" --out "$tmp/none.txt" > "$tmp/none.json"; then exit 1; fi
grep -q '"verdict": "FAIL"' "$tmp/none.json"
[ ! -e "$tmp/none.txt" ]

# A sentence no body carries cannot be preserved, and is said to be absent.
printf '%s\n' 'this sentence appears in no body at all' >> "$tmp/witnesses"
if compile --witnesses "$tmp/witnesses" --token-budget 100000 --out "$tmp/absent.txt" > "$tmp/absent.json"; then exit 1; fi
grep -q '"in_source": false' "$tmp/absent.json"
[ ! -e "$tmp/absent.txt" ]

# With no tokenizer there is no count, so there is no context.
if "$bin" context-compile --bodies "$tmp/bodies" --tokenizer "$tokenizer" --gigatoken-bin "$tmp/missing" --token-budget 100 --out "$tmp/x.txt" 2>/dev/null; then exit 1; fi
[ ! -e "$tmp/x.txt" ]

# The simulated planners are gone.
if "$bin" context-compile-smoke >/dev/null 2>&1; then exit 1; fi
if "$bin" context-plan >/dev/null 2>&1; then exit 1; fi

# A verified handoff: the context is bound to one task and revision, and is
# refused once anything it was compiled from or into has moved.
mkdir "$tmp/h"; cp "$tmp"/bodies/* "$tmp/h/"
printf '%s\n' "$keep" > "$tmp/h.witnesses"
"$bin" context-compile --bodies "$tmp/h" --tokenizer "$tokenizer" --gigatoken-bin "$bridge" --witnesses "$tmp/h.witnesses" \
    --token-budget "$((need + 8))" --task account-fabric-t1 --revision rev-abc123 --out "$tmp/h.txt" > "$tmp/h.json"
grep -q '"task": "account-fabric-t1"' "$tmp/h.json"
grep -q "\"sha256\": \"$(shasum -a 256 "$tmp/h.txt" | cut -c1-64)\"" "$tmp/h.json"
verify() { "$bin" context-verify --receipt "$tmp/h.json" --bodies "$tmp/h" --context "$tmp/h.txt" "$@"; }
verify --task account-fabric-t1 --revision rev-abc123 > "$tmp/v.json"
grep -q '"verdict": "PASS"' "$tmp/v.json"
grep -q '"token_basis": "tokenizer_count_not_provider_usage"' "$tmp/v.json"
refused() { want=$1; shift; if "$@" > "$tmp/r.json"; then echo "handoff admitted: $want" >&2; exit 1; fi
            grep -q "\"verdict\": \"REFUSED\"" "$tmp/r.json" && grep -q "$want" "$tmp/r.json" || { cat "$tmp/r.json" >&2; exit 1; }; }
refused 'another task' verify --task account-fabric-t2
refused 'another source revision' verify --revision rev-other
# A receipt without a task cannot be handed to a task.
"$bin" context-compile --bodies "$tmp/h" --tokenizer "$tokenizer" --gigatoken-bin "$bridge" --witnesses "$tmp/h.witnesses" \
    --token-budget "$((need + 8))" --out "$tmp/u.txt" > "$tmp/u.json"
refused 'not bound to a task' "$bin" context-verify --receipt "$tmp/u.json" --bodies "$tmp/h" --context "$tmp/u.txt" --task account-fabric-t1
# No witness, no verified handoff.
"$bin" context-compile --bodies "$tmp/h" --tokenizer "$tokenizer" --gigatoken-bin "$bridge" \
    --token-budget 100000 --task account-fabric-t1 --out "$tmp/n.txt" > "$tmp/n.json"
refused 'at least one witness' "$bin" context-verify --receipt "$tmp/n.json" --bodies "$tmp/h" --context "$tmp/n.txt"
# Stale context: edited context, edited body, new body, removed body.
cp "$tmp/h.txt" "$tmp/h.keep"; printf 'x' >> "$tmp/h.txt"
refused 'context file changed' verify
cp "$tmp/h.keep" "$tmp/h.txt"; first=$(ls "$tmp/h" | head -1); cp "$tmp/h/$first" "$tmp/b.keep"
printf '\nedited\n' >> "$tmp/h/$first"
refused 'body changed' verify
cp "$tmp/b.keep" "$tmp/h/$first"; printf 'new\n' > "$tmp/h/zzz-new.md"
refused 'body appeared' verify
rm "$tmp/h/zzz-new.md"; mv "$tmp/h/$first" "$tmp/b.moved"
refused 'is gone' verify
mv "$tmp/b.moved" "$tmp/h/$first"
verify --task account-fabric-t1 > /dev/null
printf '%s\n' 'context handoff verify: PASS'

printf '%s\n' 'context contract: PASS'
