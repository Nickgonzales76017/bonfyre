#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
bin=${BONFYRE_OUTREACH_BINARY:-$root/bonfyre-outreach}
tmp=$(mktemp -d "${TMPDIR:-/tmp}/outreach-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
call() { "$bin" "$@" >"$tmp/out" 2>"$tmp/err"; }
has() { grep -Eq "$1" "$tmp/out"; }
not_has() { if grep -Eiq "$1" "$tmp/out"; then exit 1; fi; }
eqsql() { [ "$(sqlite3 "$db" "$1")" = "$2" ]; }
for channel in dm listing post; do
  call templates --channel "$channel"
  not_has '\$[0-9]|same.day|satisfaction guaranteed|delivered same.day|try one for free'
done
call templates --channel listing; has 'INTERNAL DRAFT ONLY'; has 'actual buyer request'
db=$tmp/states.db
call --db "$db" send --channel dm --target regression@example.invalid --offer unpriced-evidence
has 'delivery NOT verified by this command'
eqsql 'SELECT count(*) FROM sends' 1
eqsql "SELECT target FROM sends" regression@example.invalid
eqsql 'SELECT reply_type IS NULL FROM sends' 1
sqlite3 "$db" "UPDATE sends SET sent_at='2026-01-01T00:00:00Z'"
call --db "$db" report; has 'this store does not verify external delivery'; has 'not verified sales'
call --db "$db" status; has 'Logged entries \(external delivery unverified\): 1'; has 'Unclassified: 1'
call --db "$db" followup --id 1; has 'REVIEW ONLY'; not_has 'I put together this offer|one more nudge'
db=$tmp/provenance.db
call --db "$db" backfill --channel email --target buyer@example.invalid --offer contract:one --source gmail:aaaaaaaaaaaaaaaa --sent-at 2026-10-08T21:30:37
has 'NO dispatch'
call --db "$db" backfill --channel email --target other@example.invalid --offer contract:changed --source gmail:aaaaaaaaaaaaaaaa --sent-at 2026-10-08T21:30:37
has 'not resent'
eqsql 'SELECT count(*) FROM sends' 1
eqsql 'SELECT sent_at FROM sends' '2026-10-08T21:30:37'
eqsql 'SELECT offer_name FROM sends' 'contract:one'
eqsql 'SELECT source_message_id FROM sends' 'gmail:aaaaaaaaaaaaaaaa'
eqsql 'SELECT source_sent_at FROM sends' '2026-10-08T21:30:37'
call --db "$db" reply --id 1 --type positive --notes 'Gmail human response: gmail:bbbbbbbbbbbbbbbb; requested scope, not a sale'
eqsql 'SELECT reply_type FROM sends' positive
eqsql "SELECT instr(reply_notes,'gmail:bbbbbbbbbbbbbbbb') > 0 FROM sends" 1
call --db "$db" status; has 'Classified responses: 1'; has 'Recorded positive: 1'
db=$tmp/replies.db
call --db "$db" backfill --channel email --target sales@example.invalid --offer contract:buyer --source gmail:aaaaaaaaaaaaaaaa --sent-at 2026-10-08T21:30:37
call --db "$db" backfill --channel email --target partners@example.invalid --offer contract:buyer --source gmail:bbbbbbbbbbbbbbbb --sent-at 2026-10-08T21:30:37
call --db "$db" reply-evidence --id 1 --type positive --source gmail:cccccccccccccccc --at 2026-10-09T05:57:19 --notes 'requested prior work and contracting details'
call --db "$db" reply-evidence --id 1 --type positive --source gmail:cccccccccccccccc --at 2026-10-09T05:57:19
has 'already linked'
rc=0; call --db "$db" reply-evidence --id 1 --type negative --source gmail:cccccccccccccccc --at 2026-10-09T05:57:19 || rc=$?
[ "$rc" = 2 ]
call --db "$db" reply-evidence --id 1 --type negative --source gmail:dddddddddddddddd --at 2026-10-09T09:57:19 --notes 'buyer declined after review'
call --db "$db" reply-evidence --id 2 --type automated --source gmail:eeeeeeeeeeeeeeee --at 2026-10-09T05:58:19
eqsql 'SELECT group_concat(reply_type) FROM (SELECT reply_type FROM sends ORDER BY id)' 'negative,automated'
eqsql 'SELECT reply_message_id FROM sends WHERE id=1' 'gmail:dddddddddddddddd'
eqsql 'SELECT replied_at FROM sends WHERE id=1' '2026-10-09T09:57:19'
eqsql 'SELECT group_concat(classification) FROM (SELECT classification FROM outreach_responses ORDER BY id)' 'positive,negative,automated'
call --db "$db" status; has 'Classified responses: 1'; has 'Automated acknowledgments .*: 1'
call --db "$db" report; has 'email[[:space:]]+2[[:space:]]+0[[:space:]]+1'
db=$tmp/migration.db
sqlite3 "$db" "CREATE TABLE sends(id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, target TEXT NOT NULL, offer_name TEXT, template TEXT, message_preview TEXT, sent_at TEXT NOT NULL, reply_type TEXT, reply_notes TEXT, replied_at TEXT); INSERT INTO sends(channel,target,offer_name,sent_at) VALUES('email','legacy@example.invalid','opportunity:one|gmail:ffffffffffffffff','2026-10-10T06:01:00');"
call --db "$db" backfill --channel email --target legacy@example.invalid --offer opportunity:other --source gmail:ffffffffffffffff --sent-at 2026-10-08T21:30:37
rc=0; call --db "$db" backfill --channel email --target legacy@example.invalid --offer opportunity:other --source gmail:ffffffffffffffff --sent-at 2026-10-09T21:30:37 || rc=$?
[ "$rc" = 2 ]
eqsql 'SELECT count(*) FROM sends' 1
eqsql 'SELECT offer_name FROM sends' 'opportunity:one|gmail:ffffffffffffffff'
eqsql 'SELECT sent_at FROM sends' '2026-10-10T06:01:00'
eqsql 'SELECT source_sent_at FROM sends' '2026-10-08T21:30:37'
eqsql 'SELECT source_message_id FROM sends' 'gmail:ffffffffffffffff'
echo 'commercial outreach: 5 cases PASS'
