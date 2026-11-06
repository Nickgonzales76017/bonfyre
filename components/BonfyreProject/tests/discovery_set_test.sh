#!/bin/sh
# Criteria that run, and a machine that cannot accept.
#
# The estate's older collections name their members in an array somebody typed,
# so nothing can say why a publisher is in one and nothing can propose a new one.
# This holds the other shape. The interesting assertions are all refusals:
#
#   a criterion kind nobody declared  -> refused, named
#   a set with no criteria            -> refused rather than matching everything
#   a malformed criterion             -> refused rather than ignored
#   a run                             -> may only write the verdict a machine may write
#   accepting                         -> refused to a machine verdict, an undeclared one,
#                                       an unproposed publisher, and an unnamed actor
#   a re-run                          -> leaves a person's decision alone
#   a bounded page                    -> never reported as the number matched
set -eu

bin=${1:-./bonfyre-project}
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
command -v sqlite3 >/dev/null 2>&1 || { echo "discovery set: SKIP (no sqlite3)"; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-discovery-set.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
cms="$tmp/cms.db"; frontier="$tmp/frontier.db"
now='2026-09-26T12:00:00Z'

sqlite3 "$cms" "
  CREATE TABLE discovery_set(namespace TEXT, created_at TEXT, updated_at TEXT,
    published_at TEXT, status TEXT, set_id TEXT, label TEXT, criteria TEXT,
    monitoring TEXT, opened_by TEXT, opened_at TEXT, last_run_at TEXT, cannot_infer TEXT);
  CREATE TABLE discovery_candidate(namespace TEXT, created_at TEXT, updated_at TEXT,
    published_at TEXT, status TEXT, candidate_id TEXT, set_id TEXT, root_host TEXT,
    display_name TEXT, verdict TEXT, matched TEXT, failed_criterion TEXT,
    decided_by TEXT, decided_at TEXT, reason TEXT, evaluated_at TEXT, cannot_infer TEXT);
  CREATE UNIQUE INDEX dc_id ON discovery_candidate(namespace, candidate_id);
  INSERT INTO discovery_set VALUES('root','$now','$now','$now','published','schools',
    'Schools with a calendar','host_suffix=.edu;publishes_plane=calendar','weekly','operator','$now','','stated');
  INSERT INTO discovery_set VALUES('root','$now','$now','$now','published','bogus',
    'Names a kind nobody declared','vibe_check=good','weekly','operator','$now','','stated');
  INSERT INTO discovery_set VALUES('root','$now','$now','$now','published','empty',
    'No criteria at all','','weekly','operator','$now','','stated');
  INSERT INTO discovery_set VALUES('root','$now','$now','$now','published','malformed',
    'Not kind=value','host_suffix','weekly','operator','$now','','stated');
"
# Two publishers qualify, two do not: one lacks the suffix, one lacks a calendar.
sqlite3 "$frontier" "
  CREATE TABLE root(root_id TEXT PRIMARY KEY, host TEXT, name TEXT, identity TEXT,
    kind TEXT, robots TEXT, economic TEXT, first_seen TEXT, last_seen TEXT);
  CREATE TABLE channel(channel_id TEXT PRIMARY KEY, family_id TEXT, root_id TEXT,
    address TEXT, exact_address TEXT, protocol TEXT, state TEXT, truth_level TEXT);
  INSERT INTO root VALUES
    ('r1','calendar.a.edu','A District','','','unknown','',NULL,NULL),
    ('r2','b.edu','B District','','','unknown','',NULL,NULL),
    ('r3','c.example','C Not A School','','','unknown','',NULL,NULL),
    ('r4','d.edu','D No Calendar','','','unknown','',NULL,NULL);
  INSERT INTO channel VALUES
    ('c1','f','r1','a','https://calendar.a.edu/term.ics','html','admitted','listed'),
    ('c2','f','r2','a','https://b.edu/events/calendar/','html','admitted','listed'),
    ('c3','f','r3','a','https://c.example/term.ics','html','admitted','listed'),
    ('c4','f','r4','a','https://d.edu/news/feed','rss','admitted','listed');
"

run() { BONFYRE_CMS_DB="$cms" BONFYRE_SOURCE_FRONTIER_DB="$frontier" "$bin" "$@"; }
decide() { BONFYRE_CMS_DB="$cms" BONFYRE_ACTOR="${ACTOR-}" "$bin" "$@"; }

# ------------------------------------------------------------- refusals
for bad in bogus empty malformed; do
  run discovery run "$bad" >"$tmp/$bad.json" 2>/dev/null || true
  grep -q '"state":"refused"' "$tmp/$bad.json" || { echo "FAIL: set '$bad' was not refused"; exit 1; }
done
grep -q "no criterion kind called 'vibe_check'" "$tmp/bogus.json" || { echo "FAIL: the undeclared kind was not named"; exit 1; }
grep -q 'would propose the whole estate' "$tmp/empty.json" || { echo "FAIL: an empty set gave no reason"; exit 1; }
run discovery run no-such-set >"$tmp/missing.json" 2>/dev/null || true
grep -q '"state":"refused"' "$tmp/missing.json" || { echo "FAIL: an unknown set was not refused"; exit 1; }
echo "refused: an undeclared criterion kind, a set with no criteria, a malformed one, an unknown set"

# ------------------------------------------------------- criteria really filter
run discovery run schools >"$tmp/preview.json"
python3 - "$tmp/preview.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
hosts = sorted(p["host"] for p in d["proposed"])
# calendar.a.edu has an .ics; b.edu has a /calendar/ path. c.example has a
# calendar and the wrong suffix; d.edu has the suffix and only a feed.
assert hosts == ["b.edu", "calendar.a.edu"], hosts
assert d["publishers_considered"] == 4, d
assert d["matched"] == 2, d
assert d["mode"] == "preview"
assert all(c["over"] and c["column"] for c in d["criteria"]), d["criteria"]
assert all(c["cannot_infer"] for c in d["criteria"]), "a criterion must say what it cannot mean"
print("criteria filtered 4 publishers to 2 on suffix and plane, and named the column each read")
PY

# A bounded page is never reported as the number matched.
run discovery run schools --limit 1 >"$tmp/bounded.json"
python3 - "$tmp/bounded.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
assert d["matched"] == 2, "the count must be the count, not the page: %r" % d["matched"]
assert d["page_rows"] == 1, d["page_rows"]
assert d["page_is_bounded"] is True
print("a one-row page still reports two matched")
PY

# --------------------------------------------------- a run only proposes
run discovery run schools --write >"$tmp/written.json"
python3 - "$tmp/written.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
assert d["recorded"] == 2, d
assert d["verdict_written"] == "pending", "a run wrote something other than pending"
assert d["verdicts_needing_a_person"] >= 1, d
print("a run recorded %d candidates, all pending" % d["recorded"])
PY
verdicts=$(sqlite3 "$cms" "SELECT DISTINCT verdict FROM discovery_candidate;")
[ "$verdicts" = "pending" ] || { echo "FAIL: a run produced verdicts other than pending ($verdicts)"; exit 1; }

# ------------------------------------------------------ deciding is a person's
ACTOR=nick decide discovery decide schools calendar.a.edu pending >"$tmp/d1.json" 2>/dev/null || true
grep -q '"state":"refused"' "$tmp/d1.json" || { echo "FAIL: a machine verdict was accepted as a decision"; exit 1; }
ACTOR=nick decide discovery decide schools calendar.a.edu approved >"$tmp/d2.json" 2>/dev/null || true
grep -q '"state":"refused"' "$tmp/d2.json" || { echo "FAIL: an undeclared verdict was accepted"; exit 1; }
ACTOR=nick decide discovery decide schools never-proposed.test accepted >"$tmp/d3.json" 2>/dev/null || true
grep -q '"state":"refused"' "$tmp/d3.json" || { echo "FAIL: an unproposed publisher was accepted"; exit 1; }
# A variable assignment before a *function* call persists in POSIX sh, so ACTOR
# has to be cleared rather than merely omitted here.
ACTOR='' decide discovery decide schools calendar.a.edu accepted >"$tmp/d4.json" 2>&1 || true
grep -q 'BONFYRE_ACTOR' "$tmp/d4.json" || { echo "FAIL: an anonymous decision was allowed"; exit 1; }
echo "refused: a machine verdict, an undeclared verdict, an unproposed publisher, an unnamed actor"

ACTOR=nick decide discovery decide schools calendar.a.edu accepted "a real district calendar" >"$tmp/ok.json"
grep -q '"state":"recorded"' "$tmp/ok.json" || { echo "FAIL: a named decision was not recorded"; exit 1; }
row=$(sqlite3 "$cms" "SELECT verdict || '|' || decided_by || '|' || (reason <> '') FROM discovery_candidate WHERE root_host='calendar.a.edu';")
[ "$row" = "accepted|nick|1" ] || { echo "FAIL: the decision was not recorded with its author and reason ($row)"; exit 1; }

# --------------------------------------------- a re-run leaves decisions alone
run discovery run schools --write >/dev/null
after=$(sqlite3 "$cms" "SELECT verdict || '|' || decided_by FROM discovery_candidate WHERE root_host='calendar.a.edu';")
[ "$after" = "accepted|nick" ] || { echo "FAIL: a re-run overwrote a person's decision ($after)"; exit 1; }
echo "a re-run re-evaluated without touching the decided candidate"

echo "discovery set contract: PASS"
