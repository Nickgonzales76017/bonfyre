#!/bin/sh
set -eu
compiler=$1
root=$(CDPATH='' cd -- "$(dirname -- "$0")/../../.." && pwd)
case "$compiler" in /*) ;; *) compiler="$(pwd)/$compiler" ;; esac
tmp=$(mktemp -d "${TMPDIR:-/tmp}/frappe-status.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/types" "$tmp/fixture/frappe/desk/doctype/todo"
donor="$root/integrations/frappe-bench/apps/frappe/frappe/desk/doctype/todo/todo.json"
schema="$tmp/types/todo.json"
"$compiler" --root "$root" --phase 1 --emit-cms-schema "$schema" --cms-doctype ToDo "$root/integrations/frappe-bench/apps/frappe" >/dev/null
python3 - "$schema" <<'PY'
import json,sys
s=json.load(open(sys.argv[1])); a=s['attributes']
assert s['bonfyre']['field_aliases']=={'status':'frappe_status'}
assert 'status' not in a and a['frappe_status']['enum']==['Open','Closed','Cancelled']
PY
cms=${BONFYRE_TEST_CMS_BINARY:-"$root/cmd/BonfyreCMS/bonfyre-cms"}
if [ ! -x "$cms" ]; then make -C "$root/cmd/BonfyreCMS" bonfyre-cms >/dev/null; fi
"$cms" schema apply "$schema" --db "$tmp/cms.db" --schemas "$tmp/types" >/dev/null
"$cms" entry create frappe_frappe_todo '{"description":"Follow up","frappe_status":"Open","status":"draft"}' --db "$tmp/cms.db" --schemas "$tmp/types" >/dev/null
"$cms" entry update frappe_frappe_todo 1 '{"frappe_status":"Closed"}' --db "$tmp/cms.db" --schemas "$tmp/types" >/dev/null
[ "$(sqlite3 "$tmp/cms.db" 'SELECT status || "|" || frappe_status || "|" || description FROM frappe_frappe_todo WHERE id=1')" = 'draft|Closed|Follow up' ]
python3 - "$donor" "$tmp/fixture/frappe/desk/doctype/todo/todo.json" <<'PY'
import json,sys
v=json.load(open(sys.argv[1]))
for f in v['fields']:
    if f.get('fieldname')=='description':
        f['reqd']=0;f['mandatory_depends_on']="eval:doc.status == 'Closed'"
json.dump(v,open(sys.argv[2],'w'))
PY
"$compiler" --root "$root" --phase 1 --emit-cms-schema "$tmp/conditional.json" --cms-doctype ToDo "$tmp/fixture/frappe" >/dev/null
python3 - "$tmp/conditional.json" <<'PY'
import json,sys
s=json.load(open(sys.argv[1]));a=s['attributes']['description']
assert 'requiredWhen' not in a
assert any(x['field']=='description' and 'donor status' in x['reason'] for x in s['bonfyre']['conditions_withheld'])
PY
python3 - "$tmp/fixture/frappe/desk/doctype/todo/todo.json" <<'PY'
import json,sys
p=sys.argv[1];v=json.load(open(p));v['fields'].append({'fieldname':'frappe_status','fieldtype':'Data','label':'Collision'});json.dump(v,open(p,'w'))
PY
printf 'preserve existing file' > "$tmp/collision.json"
if "$compiler" --root "$root" --phase 1 --emit-cms-schema "$tmp/collision.json" --cms-doctype ToDo "$tmp/fixture/frappe" >"$tmp/out" 2>"$tmp/err"; then
  echo 'Frappe status collision was accepted' >&2; exit 1
fi
grep -q 'both status and frappe_status' "$tmp/err"
[ "$(cat "$tmp/collision.json")" = 'preserve existing file' ]
python3 - "$tmp/fixture/frappe/desk/doctype/todo/todo.json" <<'PY'
import json,sys
p=sys.argv[1];v=json.load(open(p));v['fields']=[f for f in v['fields'] if f.get('fieldname')!='status'];json.dump(v,open(p,'w'))
PY
"$compiler" --root "$root" --phase 1 --emit-cms-schema "$tmp/no-status.json" --cms-doctype ToDo "$tmp/fixture/frappe" >/dev/null
python3 - "$tmp/no-status.json" <<'PY'
import json,sys
s=json.load(open(sys.argv[1]));assert 'field_aliases' not in s['bonfyre'];assert s['attributes']['frappe_status']['type']=='string'
PY
echo 'Frappe CMS status round trip: PASS'
