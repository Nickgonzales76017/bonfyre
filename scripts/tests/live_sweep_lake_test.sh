#!/bin/sh
# The live sweep turns a source-lake regression into one operator goal and
# resolves it when the lake holds again; the floor only rises on a clean cut.
# The fronts and the edge are stubbed: nothing here reaches the network.
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
maturity=${BONFYRE_SURFACE_MATURITY_BINARY:-$repo/cmd/BonfyreSurfaceMaturity/bonfyre-surface-maturity}
[ -x "$maturity" ] || { echo "surface maturity binary required: $maturity" >&2; exit 1; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/live-sweep-lake.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

mkdir -p "$tmp/root/scripts/release" "$tmp/estate" "$tmp/state"
cp "$repo/scripts/release/live_sweep.sh" "$tmp/root/scripts/release/"
: > "$tmp/root/scripts/release/front_integrity.mjs"
printf '#!/bin/sh\nexit 0\n' > "$tmp/root/scripts/release/edge_sweep.sh"

sqlite3 "$tmp/operator.db" "CREATE TABLE goals(id TEXT PRIMARY KEY, dedupe_key TEXT UNIQUE, title TEXT NOT NULL, details TEXT NOT NULL,
  priority INTEGER NOT NULL, state TEXT NOT NULL, source TEXT NOT NULL, recurring INTEGER NOT NULL DEFAULT 0,
  interval_seconds INTEGER NOT NULL DEFAULT 0, retry_at REAL NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL, updated_at REAL NOT NULL);"
sqlite3 "$tmp/estate/cms.db" "CREATE TABLE internet_source(source TEXT,status TEXT,publisher_ident TEXT,publisher_name TEXT);
  INSERT INTO internet_source VALUES('feed-lab','published','Q1','Lab Test');
  CREATE TABLE site_presentation(status TEXT,subject_address TEXT);
  INSERT INTO site_presentation VALUES('published','institution:publisher:lab-test');"
sqlite3 "$tmp/estate/items.db" "CREATE TABLE item(source TEXT,publisher TEXT);
  INSERT INTO item VALUES('feed-lab','Lab Test'),('feed-lab','Lab Test'),('feed-lab','Lab Test');"
sqlite3 "$tmp/estate/frontier.db" "CREATE TABLE channel(state TEXT,census_source TEXT); INSERT INTO channel VALUES('active','feed-lab');"
sqlite3 "$tmp/estate/subject_fabric.db" "CREATE TABLE subject_bundle(subject TEXT);
  INSERT INTO subject_bundle VALUES('institution:publisher:lab-test');
  CREATE TABLE subject_reader(subject TEXT,census_source TEXT,state TEXT);
  INSERT INTO subject_reader VALUES('institution:publisher:lab-test','feed-lab','active');"

sweep() {
  BONFYRE_OPERATOR_DB="$tmp/operator.db" BONFYRE_SURFACE_MATURITY_BINARY="$maturity" \
  BONFYRE_ESTATE_DIR="$tmp/estate" BONFYRE_SUBJECT_LAKE_FLOOR="$tmp/state/subject-lake.floor" \
    sh "$tmp/root/scripts/release/live_sweep.sh" >/dev/null
}
open_lake() { sqlite3 "$tmp/operator.db" "SELECT COUNT(*) FROM goals WHERE source='live-sweep' AND dedupe_key='live-sweep:subject-lake@estate' AND state='blocked'"; }

sweep
[ "$(open_lake)" = 0 ] || { echo "a held lake opened a goal" >&2; exit 1; }
grep -q '^items_with_subject 3$' "$tmp/state/subject-lake.floor" || { echo "the first clean cut did not set the floor" >&2; exit 1; }

sqlite3 "$tmp/estate/subject_fabric.db" "DELETE FROM subject_reader"
sweep
[ "$(open_lake)" = 1 ] || { echo "a lost reader did not open one lake goal" >&2; exit 1; }
sqlite3 "$tmp/operator.db" "SELECT title||' '||details FROM goals WHERE dedupe_key='live-sweep:subject-lake@estate'" | grep -q 'items_with_subject fell from 3 to 0' \
  || { echo "the goal does not say what fell" >&2; exit 1; }
grep -q '^items_with_subject 3$' "$tmp/state/subject-lake.floor" || { echo "a regressed cut lowered the floor" >&2; exit 1; }

BONFYRE_SWEEP_DRY=1 BONFYRE_OPERATOR_DB="$tmp/operator.db" BONFYRE_SURFACE_MATURITY_BINARY="$maturity" \
  BONFYRE_ESTATE_DIR="$tmp/estate" BONFYRE_SUBJECT_LAKE_FLOOR="$tmp/state/subject-lake.floor" \
  sh "$tmp/root/scripts/release/live_sweep.sh" | grep -q 'subject-lake@estate' || { echo "a dry sweep hid the lake regression" >&2; exit 1; }

sqlite3 "$tmp/estate/subject_fabric.db" "INSERT INTO subject_reader VALUES('institution:publisher:lab-test','feed-lab','active')"
sleep 1  # the sweep resolves goals not refreshed in an earlier second
sweep
[ "$(sqlite3 "$tmp/operator.db" "SELECT state FROM goals WHERE dedupe_key='live-sweep:subject-lake@estate'")" = resolved ] \
  || { echo "a restored lake did not resolve its goal" >&2; exit 1; }
echo "live_sweep_lake_test: PASS"
