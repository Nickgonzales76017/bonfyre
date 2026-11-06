#!/bin/sh
set -eu

estate="$1"
workers="$2"
cms="$3"
fixture="$4"

[ -x "$estate" ] || { echo "missing external-estate binary: $estate" >&2; exit 2; }
[ -x "$workers" ] || { echo "missing lowering-workers binary: $workers" >&2; exit 2; }
[ -f "$cms" ] || { echo "missing live CMS database: $cms" >&2; exit 2; }
[ -f "$fixture" ] || { echo "missing fact-delta fixture: $fixture" >&2; exit 2; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
index="$tmp/address.sqlite"
derived="$tmp/derived.sqlite"
inbox="$tmp/inbox"
demands="$tmp/demands"
mkdir -p "$inbox" "$demands"

"$estate" compile-advertisements --db "$index" --cms-db "$cms" > "$tmp/compile.jsonl"
python3 - "$tmp/compile.jsonl" <<'PY'
import json, sys
rows=[json.loads(x) for x in open(sys.argv[1], encoding="utf-8") if x.strip()]
imp=next(x for x in rows if "imported_candidate_advertisements" in x)
gen=next(x for x in rows if "compiled_owner_populations" in x)
assert imp["imported_candidate_advertisements"] >= 500, imp
assert gen == {"compiled_owner_populations":1,"state":"published_derived_generation","truth":"eligible_for_further_consideration"}, gen
PY

"$estate" fact-delta --db "$index" --delta "$fixture" > "$tmp/cut.json"
python3 - "$tmp/cut.json" <<'PY'
import json, sys
d=json.load(open(sys.argv[1], encoding="utf-8"))
assert d["delta_id"] == "proof:cms-source:external_ext-00068"
assert d["affected_count"] == 1 and d["lowering_state"] == "queued" and d["truth"] == "dependency_cut_only", d
assert d["affected"] == [{"identity":"cms:internet_source:external_ext-00068","candidate_kind":"external_source","owner_ref":"CMS:internet_source:external_ext-00068"}], d
PY

"$estate" drain-lowering-queue --db "$index" --dir "$inbox" --limit 8 > "$tmp/dispatch.json"
grep -q '"lowering_handoffs_dispatched":1' "$tmp/dispatch.json"
grep -q '"truth":"handoff_only"' "$tmp/dispatch.json"

"$workers" drain --db "$derived" --lowerer-dir "$inbox" --cms "$cms" --demand-dir "$demands" --limit 8 > "$tmp/lowered.json"
python3 - "$tmp/lowered.json" <<'PY'
import json, sys
d=json.load(open(sys.argv[1], encoding="utf-8"))
assert d["seen"] == 1 and d["handled"] == 1, d
assert d["failed_or_deferred"] == 0 and d["truth"] == "derived_artifacts_and_observation_demands_only", d
PY

demand="$demands/cms-internet_source-external_ext-00068.json"
[ -f "$demand" ]
python3 - "$demand" <<'PY'
import json, sys
d=json.load(open(sys.argv[1], encoding="utf-8"))
assert d["schema"] == "bonfyre-foreign-observation-demand.v1", d
assert d["identity"] == "cms:internet_source:external_ext-00068", d
assert isinstance(d.get("topic"), str) and d["topic"], d
assert d["truth"] == "observation_requested_not_performed", d
assert "body_path" not in d and "content_sha256" not in d and "observed_at" not in d, d
PY

[ "$(sqlite3 "$derived" "SELECT count(*) FROM material_observation_demand WHERE identity='cms:internet_source:external_ext-00068' AND owner_ref='CMS:internet_source:external_ext-00068' AND state='queued';")" = 1 ]

# Durable idempotence: the same owner delta is a single event, and handled
# handoffs do not reappear as new observation work.
"$estate" fact-delta --db "$index" --delta "$fixture" > "$tmp/replay.json"
grep -q '"state":"already_processed"' "$tmp/replay.json"
"$estate" drain-lowering-queue --db "$index" --dir "$inbox" --limit 8 > "$tmp/redispatch.json"
grep -q '"lowering_handoffs_dispatched":0' "$tmp/redispatch.json"
"$workers" drain --db "$derived" --lowerer-dir "$inbox" --cms "$cms" --demand-dir "$demands" --limit 8 > "$tmp/relower.json"
python3 - "$tmp/relower.json" <<'PY'
import json, sys
d=json.load(open(sys.argv[1], encoding="utf-8"))
assert d["seen"] == 0 and d["handled"] == 0 and d["failed_or_deferred"] == 0, d
PY

echo "live CMS source lowering: PASS (500+ owner rows -> exact 1-row cut -> durable handoff -> 1 request-only ForeignTwin demand; replay idempotent)"
