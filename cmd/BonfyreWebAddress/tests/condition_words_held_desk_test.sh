#!/bin/sh
# Two things the composer reads from stores other organs write: what a field
# depends on, said from its compiled plan in the DocType's own labels; and the
# messages that were written and are being held. Neither is this organ's to
# compute, and a stranger is told a held message exists and not what it says.
set -eu
web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
compiler="$root/cmd/BonfyreFrappeCompiler/bonfyre-frappe-compiler"
queue="$root/cmd/BonfyreQueue/bonfyre-queue"
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
lex="$root/cmd/BonfyreLex/bonfyre-lex"
app="$root/integrations/frappe-bench/apps/frappe"
hrms="$root/integrations/frappe-bench/apps/hrms"
for need in "$compiler" "$queue" "$auth" "$lex"; do
  [ -x "$need" ] || { echo "condition words and held desk: SKIP ($(basename "$need") not built)"; exit 0; }
done
[ -d "$app" ] && [ -d "$hrms" ] || { echo 'condition words and held desk: SKIP (no donor metadata)'; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-condition-desk.XXXXXX")
pid=''
phase='fixture setup'
cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "FAIL: condition words and held desk during $phase (exit $rc)" >&2
    [ ! -f "$tmp/server.log" ] || tail -30 "$tmp/server.log" >&2
  fi
  [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

"$compiler" --root "$root" --emit-surface-db "$tmp/surfaces.db" -o "$tmp/pack" "$app" >/dev/null 2>&1
# A second application into the same store: its number cards and charts are
# read plans the room says in words.
"$compiler" --root "$root" --emit-surface-db "$tmp/surfaces.db" -o "$tmp/pack-hrms" "$hrms" >/dev/null 2>&1
definition=$(find "$hrms" -path '*/notification/retention_bonus/*.json' | head -1)
held=$("$compiler" --prepare-notification "$definition" --event Daily --today 2026-10-01 \
  --doc '{"name":"RB-1","employee":"E-7","employee_name":"Sam","bonus_payment_date":"2026-10-15","docstatus":1}')
"$queue" enqueue notification.compose "$held" --family held-message --mission held-fixture --if-absent --db "$tmp/queue.db" >/dev/null
"$queue" mission held-fixture --db "$tmp/queue.db" >"$tmp/held-before.json"

# Records of two compiled kinds, kept the way the content store keeps them:
# its own id, namespace, timestamps and status beside the record's fields.
# The native metric reader uses SQLite date('now') in UTC. At a local midnight
# or month boundary, fixtures must use that same clock or scope comparisons lie.
month=$(date -u +%Y-%m)
sqlite3 "$tmp/cms.db" "
  CREATE TABLE frappe_hrms_job_applicant(id INTEGER PRIMARY KEY, namespace TEXT, created_at TEXT, updated_at TEXT, published_at TEXT, status TEXT, applicant_name TEXT, country TEXT);
  INSERT INTO frappe_hrms_job_applicant(namespace,created_at,updated_at,status,applicant_name,country) VALUES
    ('root','2026-01-01','2026-01-01','published','Ada','Kenya'),('root','2026-01-01','2026-01-01','published','Bo','Kenya'),
    ('root','2026-01-01','2026-01-01','draft','Cy','Peru');
  CREATE TABLE frappe_hrms_training_event(id INTEGER PRIMARY KEY, namespace TEXT, created_at TEXT, updated_at TEXT, published_at TEXT, status TEXT, event_name TEXT, company TEXT, start_time TEXT);
  INSERT INTO frappe_hrms_training_event(namespace,created_at,updated_at,status,event_name,company,start_time) VALUES
    ('root','2026-01-01','2026-01-01','published','Safety','Acme','$month-02 09:00:00'),('root','2026-01-01','2026-01-01','published','Ladders','Acme','$month-03 09:00:00'),
    ('root','2026-01-01','2026-01-01','published','Forklift','Bolt','$month-04 09:00:00'),('root','2026-01-01','2026-01-01','published','Old','Acme','2020-01-04 09:00:00');"
cp "$tmp/cms.db" "$tmp/cms-before.db"

"$web" bootstrap-owned --db "$tmp/matrix.db" >/dev/null
phase='server startup'
ready=0
# Other local witnesses may be serving at the same time. A failed bind exits
# with the binary's usage text, so choose another port rather than turning a
# valid authorization witness into a flaky connection refusal.
for attempt in 0 1 2 3 4; do
  port=$((21000 + $$ % 20000 + attempt * 137))
  BONFYRE_FRAPPE_SURFACE_DB="$tmp/surfaces.db" BONFYRE_HELD_MESSAGE_DB="$tmp/queue.db" \
    BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_FRAPPE_COMPILER="$compiler" \
    BONFYRE_AUTH_BINARY="$auth" "$web" serve --db "$tmp/matrix.db" --auth-db "$tmp/auth.db" \
    --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
  pid=$!
  for _ in $(seq 1 60); do
    if curl -fsS -o /dev/null "http://127.0.0.1:$port/healthz" 2>/dev/null && kill -0 "$pid" 2>/dev/null; then
      ready=1
      break
    fi
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.2
  done
  [ "$ready" -eq 0 ] || break
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  pid=''
done
[ "$ready" -eq 1 ] || { echo 'FAIL: no fixture server port available' >&2; exit 1; }
get() { curl -s -H 'Host: aurekai.life' -H 'Accept: text/html' "http://127.0.0.1:$port$1"; }

phase='record-kind view'
get /s/record-kind/frappe-event >"$tmp/event.html"
said='only when Repeat this Event is ticked and Repeat On is &quot;Weekly&quot;'
[ "$(grep -o "$said" "$tmp/event.html" | wc -l | tr -d ' ')" = 7 ] \
  || { echo 'FAIL: the seven weekday fields do not say their condition' >&2; exit 1; }
phase='counted view'
applicant=$(sqlite3 "$tmp/surfaces.db" "SELECT slug FROM frappe_program WHERE app='hrms' AND doctype='Job Applicant'")
get "/s/record-kind/$applicant" >"$tmp/applicant.html"
grep -q 'What is counted from it' "$tmp/applicant.html" \
  || { echo 'FAIL: a counted record kind does not say what is counted from it' >&2; exit 1; }
grep -q 'Job Applicants by Country</span><span class="desk-s">how many there are, for each country<' "$tmp/applicant.html" \
  || { echo 'FAIL: a grouped chart is not said in words' >&2; exit 1; }
# One row per definition the store holds for that kind, and no donor code named.
[ "$(grep -o 'how many there are\|the total of\|the average\|not counted here' "$tmp/applicant.html" | wc -l | tr -d ' ')" = \
  "$(sqlite3 "$tmp/surfaces.db" "SELECT COUNT(*) FROM frappe_metric WHERE doctype='Job Applicant'")" ] \
  || { echo 'FAIL: the room does not carry one line per compiled read' >&2; exit 1; }
requisition=$(sqlite3 "$tmp/surfaces.db" "SELECT slug FROM frappe_program WHERE app='hrms' AND doctype='Job Requisition'")
get "/s/record-kind/$requisition" >"$tmp/requisition.html"
grep -q 'Time to Fill</span><span class="desk-s">not counted here: the application works it out with code of its own<' "$tmp/requisition.html" \
  || { echo 'FAIL: a refused metric is not named as not counted, with why' >&2; exit 1; }
! grep -q 'get_avg_time_to_fill\|method:\|COUNT\|frappe_metric' "$tmp/requisition.html" \
  || { echo 'FAIL: the room shows donor code or machine vocabulary for a metric' >&2; exit 1; }
# Records are kept, and a stranger is told so and given no number and no name from them.
grep -q 'Their numbers are said to somebody signed in, not to a stranger' "$tmp/applicant.html" \
  || { echo 'FAIL: a stranger is not told the numbers are withheld' >&2; exit 1; }
! grep -q 'Kenya\|Peru\|Acme\|Bolt\|from the records kept here' "$tmp/applicant.html" \
  || { echo 'FAIL: a number or a name from a record reached a stranger' >&2; exit 1; }
grep -q 'No records of this kind are kept here yet' "$tmp/requisition.html" \
  || { echo 'FAIL: a kind with no records kept does not say so' >&2; exit 1; }
# A kind nothing counts says nothing about counting.
! grep -q 'What is counted from it' "$tmp/event.html" \
  || { echo 'FAIL: an uncounted record kind claims a count' >&2; exit 1; }

# The expression the DocType wrote is not shown to a reader.
if grep -q 'eval:' "$tmp/event.html"; then echo 'FAIL: an expression reached the page' >&2; exit 1; fi

phase='anonymous desk view'
get /studio/ >"$tmp/desk.html"
grep -q '<h2>Messages written and held</h2><span class="ak-tag is-on">1</span>' "$tmp/desk.html" \
  || { echo 'FAIL: the desk does not count the held message' >&2; exit 1; }
# The studio workbench (Stitch policy workbench) states the count in each panel's tag.
# A stranger is told there is one, and neither its subject nor whom it is about.
if grep -q 'Retention Bonus alert\|RB-1\|E-7' "$tmp/desk.html"; then
  echo 'FAIL: a held message was named to somebody not signed in' >&2; exit 1
fi

# A real, temporary read-scoped session still sees only the count. The named
# row is visible only after the account has write scope, and revocation closes
# that view again. No live account or session is needed for this witness.
phase='fixture accounts'
password='held-message-desk-fixture-passphrase'
"$auth" --db "$tmp/auth.db" signup --email reader@example.test --name Reader --password "$password" >/dev/null
"$auth" --db "$tmp/auth.db" signup --email writer@example.test --name Writer --password "$password" --tier pro >/dev/null
reader=$("$auth" --db "$tmp/auth.db" login --email reader@example.test --password "$password" | "$lex" field --kind json_string --key session.token)
writer=$("$auth" --db "$tmp/auth.db" login --email writer@example.test --password "$password" | "$lex" field --kind json_string --key session.token)
[ -n "$reader" ] && [ -n "$writer" ] || { echo 'FAIL: fixture login issued no token' >&2; exit 1; }
base="http://127.0.0.1:$port/studio/"
phase='signed-in counted view'
sget() { curl -s -H 'Host: aurekai.life' -H 'Accept: text/html' -H "Authorization: Bearer $reader" "http://127.0.0.1:$port$1"; }
sget "/s/record-kind/$applicant" >"$tmp/applicant-in.html"
# Grouped, largest first, from the three records kept.
grep -q 'Job Applicants by Country</span><span class="desk-a">Kenya 2, Peru 1</span><span class="desk-s">how many there are, for each country, from the records kept here<' "$tmp/applicant-in.html" \
  || { echo 'FAIL: a grouped chart is not answered from the records kept' >&2; exit 1; }
# This one filters on the donor's status. The store's status is its own
# draft-or-published, so there is no number rather than a number about nothing.
grep -q 'Accepted Job Applicants</span><span class="desk-s">how many there are; no number, because the records kept here do not carry status<' "$tmp/applicant-in.html" \
  || { echo 'FAIL: the store status was read as the donor status' >&2; exit 1; }
training=$(sqlite3 "$tmp/surfaces.db" "SELECT slug FROM frappe_program WHERE app='hrms' AND doctype='Training Event'")
sget "/s/record-kind/$training" >"$tmp/training-in.html"
# The donor would ask the reader's company; each company gets its own number
# for this month and there is no number across both.
grep -q 'Trainings (This Month)</span><span class="desk-a">at Acme: 2; at Bolt: 1</span>' "$tmp/training-in.html" \
  || { echo 'FAIL: a company-scoped card is not answered per company' >&2; exit 1; }
! grep -q '<span class="desk-a">3</span>' "$tmp/training-in.html" \
  || { echo 'FAIL: a number was taken across every company' >&2; exit 1; }
cmp -s "$tmp/cms.db" "$tmp/cms-before.db" \
  || { echo 'FAIL: answering a metric changed the store' >&2; exit 1; }

phase='read-scoped desk view'
curl -fsS -H 'Host: aurekai.life' -H 'Accept: text/html' -H "Authorization: Bearer $reader" "$base" >"$tmp/reader.html"
grep -q '<h2>Messages written and held</h2><span class="ak-tag is-on">1</span>' "$tmp/reader.html" \
  || { echo 'FAIL: read-scoped account lost the held count' >&2; exit 1; }
if grep -q 'Retention Bonus alert\|RB-1\|E-7' "$tmp/reader.html"; then
  echo 'FAIL: a held message was named to a read-scoped account' >&2; exit 1
fi
phase='write-scoped desk view'
curl -fsS -H 'Host: aurekai.life' -H 'Accept: text/html' -H "Cookie: bonfyre_session=$writer" "$base" >"$tmp/writer.html"
grep -q 'Retention Bonus alert' "$tmp/writer.html" \
  || { echo 'FAIL: write-scoped account cannot see held subject' >&2; exit 1; }
grep -q 'RB-1' "$tmp/writer.html" \
  || { echo 'FAIL: write-scoped account cannot see held document' >&2; exit 1; }
phase='revoked desk view'
"$auth" --db "$tmp/auth.db" logout --token "$writer" >/dev/null
curl -fsS -H 'Host: aurekai.life' -H 'Accept: text/html' -H "Cookie: bonfyre_session=$writer" "$base" >"$tmp/revoked.html"
if grep -q 'Retention Bonus alert\|RB-1\|E-7' "$tmp/revoked.html"; then
  echo 'FAIL: a revoked write session still sees held message names' >&2; exit 1
fi
"$queue" mission held-fixture --db "$tmp/queue.db" >"$tmp/held-after.json"
cmp -s "$tmp/held-before.json" "$tmp/held-after.json" \
  || { echo 'FAIL: reading the desk changed the held queue node' >&2; exit 1; }

echo 'condition words and held desk: PASS'
