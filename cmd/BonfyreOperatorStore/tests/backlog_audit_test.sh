#!/bin/sh
# Audit current goal states and run history without rewriting either.
set -eu
bin=$1
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
db="$tmp/operator.sqlite3"
"$bin" init --db "$db" > /dev/null
sqlite3 "$db" <<'SQL'
INSERT INTO goals(id,dedupe_key,title,details,priority,state,source,recurring,interval_seconds,retry_at,attempts,last_error,created_at,updated_at)
VALUES ('00000000000000000000000000000001','audit-one','Same title','',100,'queued','first',0,0,0,0,'',1,1),
       ('00000000000000000000000000000002','audit-two','Same title','',99,'queued','second',0,0,0,0,'',1,2),
       ('00000000000000000000000000000003','audit-three','Recurring','',98,'queued','schedule',1,3600,0,0,'',1,3),
       ('00000000000000000000000000000004','audit-four','Failed release','',90,'blocked','release',0,0,0,0,'rollback',1,4);
INSERT INTO runs(id,goal_id,started_at,completed_at,state,response_id,summary,usage_json)
VALUES('completed-2','00000000000000000000000000000002',1,2,'completed',NULL,'history', '{}');
SQL
sqlite3 "$db" "UPDATE goals SET details='Reviewed acceptance pending' WHERE id='00000000000000000000000000000001';"
before=$(shasum -a 256 "$db" | cut -d ' ' -f 1)
"$bin" backlog-audit --db "$db" --state queued > "$tmp/queued.json"
"$bin" backlog-audit --db "$db" --state blocked > "$tmp/blocked.json"
"$bin" backlog-audit --db "$db" --limit 1 > "$tmp/limited.json"
"$bin" backlog-audit --db "$db" --limit 1 --offset 1 > "$tmp/second.json"
field() { sqlite3 :memory: "SELECT json_extract(readfile('$1'),'$2');"; }
row_field() {
  sqlite3 :memory: "SELECT json_extract(value,'$3') FROM json_each(readfile('$1'),'$.rows') WHERE json_extract(value,'$.id')='$2';"
}
[ "$(field "$tmp/queued.json" '$.returned')" = 3 ]
[ "$(field "$tmp/queued.json" '$.native_archive_review_candidates')" = 2 ]
[ "$(field "$tmp/queued.json" '$.candidates_for_exact_review')" = 1 ]
[ "$(field "$tmp/queued.json" '$.read_only')" = 1 ]
[ "$(row_field "$tmp/queued.json" 00000000000000000000000000000001 '$.candidate_for_exact_review')" = 1 ]
[ "$(row_field "$tmp/queued.json" 00000000000000000000000000000001 '$.dedupe_key')" = audit-one ]
[ "$(row_field "$tmp/queued.json" 00000000000000000000000000000001 '$.details_excerpt')" = 'Reviewed acceptance pending' ]
[ "$(row_field "$tmp/queued.json" 00000000000000000000000000000002 '$.run_count')" = 1 ]
[ "$(row_field "$tmp/queued.json" 00000000000000000000000000000002 '$.candidate_for_exact_review')" = 0 ]
[ "$(row_field "$tmp/queued.json" 00000000000000000000000000000003 '$.recurring')" = 1 ]
[ "$(row_field "$tmp/queued.json" 00000000000000000000000000000003 '$.candidate_for_exact_review')" = 0 ]
[ "$(field "$tmp/blocked.json" '$.returned')" = 1 ]
[ "$(field "$tmp/blocked.json" '$.candidates_for_exact_review')" = 0 ]
[ "$(field "$tmp/blocked.json" '$.native_archive_review_candidates')" = 1 ]
[ "$(row_field "$tmp/blocked.json" 00000000000000000000000000000004 '$.last_error_excerpt')" = rollback ]
[ "$(field "$tmp/limited.json" '$.returned')" = 1 ]
[ "$(field "$tmp/limited.json" '$.limit')" = 1 ]
[ "$(field "$tmp/limited.json" '$.has_more')" = 1 ]
[ "$(field "$tmp/second.json" '$.offset')" = 1 ]
[ "$(field "$tmp/second.json" '$.total_in_state')" = 3 ]
[ "$(field "$tmp/second.json" '$.returned')" = 1 ]
[ "$(sqlite3 :memory: "SELECT json_extract(value,'$.run_count') FROM json_each(readfile('$tmp/second.json'),'$.rows') LIMIT 1;")" = 1 ]
[ "$(sqlite3 :memory: "SELECT COUNT(*) FROM json_each(readfile('$tmp/queued.json'),'$.rows') WHERE json_extract(value,'$.title')='Same title';")" = 2 ]
if "$bin" backlog-audit --db "$db" --state unknown > /dev/null 2>&1; then exit 1; fi
if "$bin" backlog-audit --db "$db" --limit 201 > /dev/null 2>&1; then exit 1; fi
if "$bin" backlog-audit --db "$db" --offset -1 > /dev/null 2>&1; then exit 1; fi
if "$bin" backlog-audit --db "$db" --offset 1000001 > /dev/null 2>&1; then exit 1; fi
if "$bin" backlog-audit --db "$db" --state queued --state queued > /dev/null 2>&1; then exit 1; fi
if "$bin" backlog-audit --db "$tmp/missing.sqlite3" > /dev/null 2>&1; then exit 1; fi
[ ! -e "$tmp/missing.sqlite3" ]
ln -s "$db" "$tmp/link.sqlite3"
if "$bin" backlog-audit --db "$tmp/link.sqlite3" > /dev/null 2>&1; then exit 1; fi
[ "$(shasum -a 256 "$db" | cut -d ' ' -f 1)" = "$before" ]
[ "$(sqlite3 "$db" "SELECT COUNT(*) FROM runs;")" = 1 ]
[ "$(sqlite3 "$db" "SELECT COUNT(*) FROM goals;")" = 4 ]
printf '%s\n' 'operator backlog audit: read-only goal/run parity and refusals PASS'
