#!/bin/sh
# A donor Number Card or Dashboard Chart is one read. Every definition is
# either compiled to a plan or refused by the name of the donor code it needs,
# and a compiled one is answered from a store only when that store carries the
# fields it reads and the reader has said what the donor would have asked them.
set -eu

compiler="$1"
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
hrms="$root/integrations/frappe-bench/apps/hrms"
erpnext="$root/integrations/frappe-bench/apps/erpnext"
tmp=$(mktemp -d)
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

"$compiler" --root "$root" --emit-metric-plans "$tmp/plans.json" -o "$tmp/pack" "$hrms" >"$tmp/out"
jq -e '
  .schema == "bonfyre.frappe-metric-plans.v1"
  and (.counts.metrics == (.metrics | length)) and (.counts.metrics > 40)
  and (.counts.compiled + .counts.unsupported == .counts.metrics)
  and all(.metrics[]; (.status == "compiled") == (.missing_operation == ""))
  and (.counts.unsupported == ([.missing_operations[].definitions] | add // 0))
  # what is refused is the donor running its own code, or a child table
  and all(.missing_operations[]; .operation | test("^(report|method|source):|^child_table_metric$"))
' "$tmp/plans.json" >/dev/null || { echo "FAIL: metric accounting" >&2; exit 1; }

# The same accounting reaches the store the composer reads, one row a definition,
# each compiled one carrying words and each refused one its missing operation.
"$compiler" --root "$root" --emit-surface-db "$tmp/surfaces.db" -o "$tmp/pack-db" "$hrms" >/dev/null
[ "$(sqlite3 "$tmp/surfaces.db" "SELECT COUNT(*), SUM(status='compiled') FROM frappe_metric")" = \
  "$(jq -r '"\(.counts.metrics)|\(.counts.compiled)"' "$tmp/plans.json")" ] \
  || { echo "FAIL: surface store does not carry the metric accounting" >&2; exit 1; }
[ "$(sqlite3 "$tmp/surfaces.db" "SELECT COUNT(*) FROM frappe_metric WHERE (status='compiled') != (said != '') OR (status='compiled') != (missing_operation = '')")" = 0 ] \
  || { echo "FAIL: a metric row is neither said nor refused" >&2; exit 1; }

sqlite3 "$tmp/store.db" "
  CREATE TABLE employee(id INTEGER PRIMARY KEY, status TEXT, company TEXT, department TEXT, date_of_joining TEXT, active TEXT);
  INSERT INTO employee(status,company,department,date_of_joining,active) VALUES
    ('published','Acme','Ops','2026-10-05','Active'),('published','Acme','Ops','2026-07-01','Active'),
    ('draft','Acme','Lab','2026-11-20','Active'),('published','Bolt','Ops','2026-10-06','Active');
  CREATE TABLE purchase_invoice(id INTEGER PRIMARY KEY, posting_date TEXT, base_net_total REAL, docstatus INT);
  INSERT INTO purchase_invoice(posting_date,base_net_total,docstatus) VALUES
    ('2025-10-15',100,1),('2025-10-20',50,1),('2025-11-02',7,0),('2026-03-31',25.5,1),('2026-10-01',10,1),('2024-01-01',999,1);"
def() { find "$1" -name "$2.json" | head -1; }
read_metric() {
  definition="$1"; table="$2"; shift 2
  "$compiler" --read-metric "$definition" --store "$tmp/store.db" --table "$table" --today 2026-10-08 "$@"
}

joining=$(def "$hrms" 'employees_joining_(this_quarter)')
# The donor would ask the reader's default company; unsaid, there is no answer.
read_metric "$joining" employee | jq -e '.status == "refused" and .reader_must_say == "Company"' >/dev/null
# Said: joined this quarter, at that company. Oct-Dec 2026 holds two at Acme.
[ "$(read_metric "$joining" employee --param Company=Acme | jq -r '.status, .value' | tr '\n' ' ')" = "read 2 " ]
[ "$(read_metric "$joining" employee --param Company=Bolt | jq -r .value)" = 1 ]

# A chart grouped by a field, largest first.
department=$(def "$hrms" department_wise_employee_count)
# Its filter reads status, which this store keeps for its own use.
read_metric "$department" employee --param Company=Acme --not-carried status \
  | jq -e '.status == "refused" and .not_carried == "status"' >/dev/null
# A store whose status really is the donor field answers it.
sqlite3 "$tmp/store.db" "CREATE TABLE employee_donor AS SELECT id, active AS status, company, department, date_of_joining FROM employee"
read_metric "$department" employee_donor --param Company=Acme \
  | jq -e '.status == "read" and .rows == [{"label":"Ops","value":2},{"label":"Lab","value":1}]' >/dev/null \
  || { echo "FAIL: grouped chart" >&2; exit 1; }

# A chart over time: last year by month, one value per month up to today,
# drafts left out by the definition's own filter, older rows by its range.
bills=$(def "$erpnext" 'incoming_bills_(purchase_invoice)')
read_metric "$bills" purchase_invoice >"$tmp/series.json"
jq -e '.status == "read" and .every == "Monthly" and (.periods | length == 13)
       and .periods[0] == {"until":"2025-10-31","value":150}
       and (.periods[] | select(.until == "2025-11-30") | .value == 0)
       and (.periods[] | select(.until == "2026-03-31") | .value == 25.5)
       and .periods[-1] == {"until":"2026-10-31","value":10}' "$tmp/series.json" >/dev/null \
  || { cat "$tmp/series.json" >&2; echo "FAIL: chart over time" >&2; exit 1; }

# A table that does not hold the field is said so, not counted as zero.
read_metric "$joining" purchase_invoice --param Company=Acme | jq -e '.status == "refused" and .not_carried != ""' >/dev/null

jq -r '"metrics: \(.counts.metrics) definitions, \(.counts.compiled) compiled, \(.counts.unsupported) refused by name"' "$tmp/plans.json"
echo "metric read: PASS"
