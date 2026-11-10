#!/bin/sh
# Read-only historical Fabric hook audit. Prints counts, never event bodies.
set -eu
db=${BONFYRE_FABRIC_DB:-"${HOME}/.bonfyre/dev-fabric/fabric.sqlite3"}
if [ ! -e "$db" ]; then
  echo "fabric historical hooks: SKIP (no owner database)"
  exit 0
fi
if [ ! -f "$db" ] || [ -L "$db" ]; then
  echo "fabric historical hooks: REFUSED (database not a regular file)" >&2
  exit 1
fi
command -v sqlite3 >/dev/null 2>&1 || { echo "fabric historical hooks: sqlite3 unavailable" >&2; exit 1; }
count=$(sqlite3 -readonly "$db" "
  PRAGMA query_only=ON;
  SELECT COUNT(*),
    COALESCE(SUM(CASE WHEN instr(payload_json,char(34)||'prompt'||char(34))>0
      OR instr(payload_json,char(34)||'tool_input'||char(34))>0
      OR instr(payload_json,char(34)||'tool_response'||char(34))>0
      OR instr(payload_json,char(34)||'cwd'||char(34))>0
      THEN 1 ELSE 0 END),0)
  FROM events WHERE source IN ('codex','claude');
")
case "$count" in
  *"|"*) ;;
  *) echo "fabric historical hooks: invalid audit result" >&2; exit 1 ;;
esac
echo "fabric historical hooks: observed|flagged_legacy = $count (heuristic; no payload readback)"
