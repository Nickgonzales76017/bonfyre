#!/bin/sh
set -eu

estate="$1"
workers="$2"
protocols="$3"
fixture="$4"

[ -x "$estate" ] || exit 2
[ -x "$workers" ] || exit 2
[ -f "$protocols" ] || exit 2
[ -f "$fixture" ] || exit 2

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
index="$tmp/address.sqlite"
derived="$tmp/derived.sqlite"
inbox="$tmp/inbox"
mkdir -p "$inbox"

"$estate" compile-advertisements --db "$index" --protocols "$protocols" > "$tmp/compile.jsonl"
python3 - "$tmp/compile.jsonl" <<'PY'
import json, sys
rows=[json.loads(x) for x in open(sys.argv[1], encoding="utf-8") if x.strip()]
imp=next(x for x in rows if "imported_candidate_advertisements" in x)
gen=next(x for x in rows if "compiled_owner_populations" in x)
assert imp["imported_candidate_advertisements"] == 108, imp
assert gen["compiled_owner_populations"] == 1, gen
PY

"$estate" fact-delta --db "$index" --delta "$fixture" > "$tmp/cut.json"
python3 - "$tmp/cut.json" <<'PY'
import json, sys
d=json.load(open(sys.argv[1], encoding="utf-8"))
assert d["delta_id"] == "proof:protocol:x402"
assert d["affected_count"] == 1 and d["truth"] == "dependency_cut_only", d
assert d["affected"] == [{"identity":"protocol:x402","candidate_kind":"protocol_chain","owner_ref":"ProtocolCommons:x402"}], d
PY

"$estate" drain-lowering-queue --db "$index" --dir "$inbox" --limit 8 > "$tmp/dispatch.json"
grep -q '"lowering_handoffs_dispatched":1' "$tmp/dispatch.json"

"$workers" drain --db "$derived" --lowerer-dir "$inbox" --protocols "$protocols" --limit 8 > "$tmp/lowered.json"
python3 - "$tmp/lowered.json" "$protocols" "$derived" <<'PY'
import json, sqlite3, sys
summary=json.load(open(sys.argv[1], encoding="utf-8"))
assert summary["seen"] == 1 and summary["handled"] == 1 and summary["failed_or_deferred"] == 0, summary
source=next(json.loads(x) for x in open(sys.argv[2], encoding="utf-8") if x.strip() and json.loads(x).get("id")=="x402")
db=sqlite3.connect(sys.argv[3])
row=db.execute("SELECT identity,program_digest,program_json FROM protocol_compilation_artifact").fetchone()
assert row is not None and row[0] == "protocol:x402", row
assert row[1].startswith("sha256:"), row[1]
hexpart=row[1][7:]
assert len(hexpart) == 64 and all(c in "0123456789abcdef" for c in hexpart), row[1]
assert json.loads(row[2]) == source, (json.loads(row[2]), source)
assert db.execute("SELECT count(*) FROM protocol_compilation_artifact").fetchone()[0] == 1
PY

# Compilation is not enactment: no effect/settlement/authority state is created here.
python3 - "$derived" <<'PY'
import sqlite3, sys
db=sqlite3.connect(sys.argv[1])
tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
for forbidden in ("commitments","commitment_events","authority_edges","settlements","effects"):
    assert forbidden not in tables, (forbidden, sorted(tables))
PY

"$estate" fact-delta --db "$index" --delta "$fixture" > "$tmp/replay.json"
grep -q '"state":"already_processed"' "$tmp/replay.json"
"$estate" drain-lowering-queue --db "$index" --dir "$inbox" --limit 8 > "$tmp/redispatch.json"
grep -q '"lowering_handoffs_dispatched":0' "$tmp/redispatch.json"
"$workers" drain --db "$derived" --lowerer-dir "$inbox" --protocols "$protocols" --limit 8 > "$tmp/relower.json"
grep -q '"seen":0,"handled":0' "$tmp/relower.json"

echo "protocol lowering convergence: PASS (108 ProtocolCommons rows -> exact x402 cut -> compiled source-identical artifact; no enactment/effect inference; replay idempotent)"
