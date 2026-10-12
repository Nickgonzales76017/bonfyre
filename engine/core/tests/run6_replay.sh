#!/usr/bin/env bash
# Run6 replay proof: fold the REAL recorded occurrences in control_plane.db
# through the native projection AND the Python reference, over copies of the
# same data, and assert the resulting projection is identical row-for-row.
#
# This is not a synthetic fixture -- it replays the actual occurrences captured
# during the run6 lineage (github replies + state changes from bernstein,
# deputy, agentguard, and the ACM/UT-Dallas outreach) and proves the native
# OccurrenceSpine reproduces the Python folded state exactly.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
CORE="$(cd "$HERE/.." && pwd)"
CP="$CORE/../../10-Code/BonfyreControlPlane"
SRC="${1:-$CP/control_plane.db}"
NOW="2026-08-16T00:00:00+00:00"

if [ ! -f "$SRC" ]; then
  echo "no control-plane db at $SRC -- nothing to replay" >&2
  exit 0
fi

# Build the native replay driver against the built library.
cc -O2 -Wall -Wextra -std=c11 -I"$CORE/include" -I"$CORE/../../lib/libbonfyre/include" \
   "$HERE/occurrence_replay.c" "$CORE/libbonfyre-fabric.a" -lsqlite3 \
   -o "$CORE/obj/occurrence_replay"

A="$(mktemp -u /tmp/replay_native_XXXXXX).db"
B="$(mktemp -u /tmp/replay_python_XXXXXX).db"
trap 'rm -f "$A" "$B" "$A"-wal "$A"-shm "$B"-wal "$B"-shm' EXIT

for D in "$A" "$B"; do
  sqlite3 "$SRC" ".backup '$D'"
  sqlite3 "$D" "UPDATE external_event_log SET projected_at=NULL; DROP TABLE IF EXISTS occurrence_projection;"
done

"$CORE/obj/occurrence_replay" "$A" "$NOW" > /tmp/native_proj.txt 2>/dev/null

python3 -c "
import sqlite3, datetime as dt, sys
sys.path.insert(0, '$CP')
import external_events as ee
db=sqlite3.connect('$B')
ee.ensure_schema(db)
db.execute('CREATE TABLE IF NOT EXISTS occurrence_projection(event_id INTEGER PRIMARY KEY, actor TEXT NOT NULL, event_kind TEXT NOT NULL, status TEXT NOT NULL, projected_at TEXT NOT NULL)')
now=dt.datetime.fromisoformat('$NOW')
def apply_status(actor, status, event):
    db.execute('INSERT OR REPLACE INTO occurrence_projection(event_id,actor,event_kind,status,projected_at) VALUES(?,?,?,?,?)',(event.id,actor,event.event_kind,status,now.isoformat()))
ee.project(db, apply_status, now=now)
db.commit()
for r in db.execute('SELECT event_id,actor,event_kind,status FROM occurrence_projection ORDER BY event_id'):
    print('|'.join(str(x) for x in r))
" > /tmp/py_proj.txt

if diff -q /tmp/native_proj.txt /tmp/py_proj.txt >/dev/null; then
  echo "RUN6 REPLAY PARITY: MATCH ($(wc -l < /tmp/native_proj.txt | tr -d ' ') rows)"
else
  echo "RUN6 REPLAY PARITY: MISMATCH"
  diff /tmp/native_proj.txt /tmp/py_proj.txt || true
  exit 1
fi
