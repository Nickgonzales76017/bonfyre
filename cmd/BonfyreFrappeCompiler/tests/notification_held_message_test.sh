#!/bin/sh
# A donor Notification is bound to two owners this estate already has: the
# condition plan decides whether it fires, and what it produces is a held
# message filed through the queue. Nothing in the path can send.
set -eu

compiler="$1"
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
queue="$root/cmd/BonfyreQueue/bonfyre-queue"
app="$root/integrations/frappe-bench/apps/hrms"
tmp=$(mktemp -d)
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

"$compiler" --root "$root" --emit-notification-plans "$tmp/plans.json" -o "$tmp/pack" "$app" >"$tmp/out"

# Every definition is accounted for: this estate can say whether each fires,
# and where it cannot write the text it names the template operation missing.
jq -e '
  .schema == "bonfyre.frappe-notification-plans.v1"
  and (.counts.notifications == (.notifications | length)) and (.counts.notifications >= 4)
  and (.counts.decides == ([.notifications[] | select(.decides)] | length))
  and (.counts.composes == ([.notifications[] | select(.composes)] | length))
  and (.counts.composes == .counts.notifications)
  and all(.notifications[]; .composes or (.decides | not)
          or (.subject.missing_operation != "") or (.message.missing_operation != ""))
  and all(.notifications[] | select(.condition.status == "compiled"); .condition.plan | startswith("(py "))
' "$tmp/plans.json" >/dev/null || { echo "FAIL: notification accounting" >&2; exit 1; }

definition() { find "$app" -path "*/notification/$1/*.json" | head -1; }
prepare() { "$compiler" --prepare-notification "$1" --event "$2" --today 2026-10-01 --doc "$3"; }

# Days Before, with a condition: fires only on the day and only while it holds.
exit_interview=$(definition exit_interview_scheduled)
ready='{"name":"EI-1","employee":"E-7","employee_name":"Sam","date":"2026-10-02","email":"sam@example.org, pat@example.org","docstatus":1,"status":"Scheduled","interviewers":[{"user":"a@x.org"},{"user":"b@x.org"}]}'
prepare "$exit_interview" Daily "$ready" >"$tmp/held.json"
jq -e '.fires == true and .addressed == 2 and .transmitted == false
       and .subject == "Exit Interview Scheduled: EI-1"
       and ([.recipients[] | select(.selector == "assignees")][0].address == null)
       # the date is written in the format this estate states, a row per interviewer,
       # and the name of the record without the link into a desk nobody serves here
       and .state == "composed" and (.body | contains("2026-10-02 Fri"))
       and (.body | contains("a@x.org")) and (.body | contains("b@x.org"))
       and .links_withheld == true and (.body | contains("href") | not)' "$tmp/held.json" >/dev/null \
  || { echo "FAIL: exit interview held message" >&2; exit 1; }
[ "$(prepare "$exit_interview" Daily '{"name":"EI-1","date":"2026-10-02","email":"sam@example.org","docstatus":2,"status":"Scheduled"}' | jq -r .fires)" = false ]
[ "$(prepare "$exit_interview" Daily '{"name":"EI-1","date":"2026-10-09","email":"sam@example.org","docstatus":1,"status":"Scheduled"}' | jq -r .fires)" = false ]
[ "$(prepare "$exit_interview" Submit "$ready" | jq -r .fires)" = false ]

# A template that sets variables, compares two dates and formats by its own
# explicit pattern is run as written.
training=$(definition training_scheduled)
prepare "$training" Submit '{"name":"TE-1","event_name":"Safety","start_time":"2026-10-02 09:00:00","end_time":"2026-10-02 17:30:00","location":"Hall B","introduction":"Bring boots","is_mandatory":1,"employee_emails":"a@x.org"}' >"$tmp/training.json"
jq -e '.fires == true and .state == "composed" and .addressed == 1
       and (.body | contains("Friday, 02 Oct 2026")) and (.body | contains("09:00 AM to 05:30 PM"))
       and (.body | contains("This Training Event is mandatory")) and (.body | contains("{%") | not)' "$tmp/training.json" >/dev/null \
  || { echo "FAIL: training scheduled body" >&2; exit 1; }
# The same event across two days takes the other branch.
[ "$(prepare "$training" Submit '{"name":"TE-2","start_time":"2026-10-02 09:00:00","end_time":"2026-10-03 12:00:00"}' | jq -r '.body | contains("Friday, 02 Oct 2026 at 09:00 AM")')" = true ]

# One definition is composed in full, to a role nobody here holds.
bonus=$(definition retention_bonus)
prepare "$bonus" Daily '{"name":"RB-1","employee":"E-7","employee_name":"Sam","bonus_payment_date":"2026-10-15","docstatus":1}' >"$tmp/bonus.json"
jq -e '.fires == true and .state == "composed" and .transmitted == false and .addressed == 0
       and .subject == "Retention Bonus alert for E-7"
       and (.body | contains("Retention Bonus for Sam due on 2026-10-15"))
       and (.recipients[0] | .selector == "role" and .address == null and .unresolved != "")' "$tmp/bonus.json" >/dev/null \
  || { echo "FAIL: retention bonus held message" >&2; exit 1; }

# A body with a loop, a test and a rounded number is written as the donor
# writes it: rows only for what was received, floats printed as floats, an
# absent field as None.
receipt=$(find "$root/integrations/frappe-bench/apps/erpnext" -path '*/notification/material_request_receipt_notification/*.json' | head -1)
if [ -n "$receipt" ]; then
  "$compiler" --prepare-notification "$receipt" --event "Value Change" --previous-doc '{"status":"Pending"}' \
    --doc '{"name":"MR-1","status":"Received","material_request_type":"Purchase","items":[{"item_code":"BOLT","received_qty":5},{"item_code":"NUT","received_qty":0},{"item_code":"WASHER","received_qty":2.456}]}' >"$tmp/receipt.json"
  jq -e '.fires == true and .state == "composed" and .body_format == "markdown"
         and (.body | contains("<td>BOLT</td>")) and (.body | contains("<td>5.0</td>"))
         and (.body | contains("<td>2.46</td>")) and (.body | contains("NUT") | not)
         and (.body | contains("<b>Company</b>: None")) and (.body | contains("{%") | not)' "$tmp/receipt.json" >/dev/null \
    || { echo "FAIL: material request receipt body" >&2; exit 1; }
  [ "$("$compiler" --prepare-notification "$receipt" --event "Value Change" --previous-doc '{"status":"Received"}' --doc '{"name":"MR-1","status":"Received"}' | jq -r .fires)" = false ]
fi

# Filed through the queue the estate already runs: once, however often asked.
if [ -x "$queue" ]; then
  key=$(jq -r .key "$tmp/bonus.json")
  for _ in 1 2; do
    "$queue" enqueue notification.compose "$(cat "$tmp/bonus.json")" --family held-message \
      --mission "held-$key" --if-absent --db "$tmp/queue.db" >/dev/null
  done
  [ "$("$queue" depth --family held-message --db "$tmp/queue.db" | jq -r .total)" = 1 ]
  filed="filed once through the queue"
else
  filed="queue not built, filing not exercised"
fi

jq -r --arg filed "$filed" '"notifications: \(.counts.notifications) definitions, \(.counts.decides) decided, \(.counts.composes) composed in full, \($filed)"' "$tmp/plans.json"
echo "notification held message: PASS"
