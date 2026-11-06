#!/bin/sh
# The legacy migration practice, built as a draft site from its own source:
# every page answers, every practice page leads to a form that asks the facts
# its assessment needs, every price is the brief types' range and says it is an
# estimate, nothing on the pages claims a client, a testimonial or a guarantee,
# and the site was never shared.  A request left through the hosting form
# reaches the owner's inbox with its answers under their fact names.
set -eu
web=${1:-${BONFYRE_TEST_WEB_BINARY:-}}
[ -n "$web" ] || { echo "practice site: SKIP (pass the web-address binary or set BONFYRE_TEST_WEB_BINARY)"; exit 0; }
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
here=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
root=$(CDPATH= cd -- "$here/../.." && pwd)
for organ in BonfyreCMS/bonfyre-cms BonfyreIngest/bonfyre-ingest BonfyreParagraph/bonfyre-paragraph BonfyreRecall/bonfyre-recall; do
  [ -x "$root/cmd/$organ" ] || { echo "practice site: SKIP ($organ not built)"; exit 0; }
done
export BONFYRE_CMS_BINARY="$root/cmd/BonfyreCMS/bonfyre-cms" BONFYRE_INGEST_BINARY="$root/cmd/BonfyreIngest/bonfyre-ingest"
export BONFYRE_PARAGRAPH_BINARY="$root/cmd/BonfyreParagraph/bonfyre-paragraph" BONFYRE_RECALL_BINARY="$root/cmd/BonfyreRecall/bonfyre-recall"
export BONFYRE_CMS_SCHEMAS="$root/cmd/BonfyreCMS/content-types" BONFYRE_SITE_ROOT="$root/site/matrix"
home=$(mktemp -d "${TMPDIR:-/tmp}/practice-site.XXXXXX")
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
cleanup() { "$web" site stop --home "$home" >/dev/null 2>&1 || true; rm -rf "$home"; }
trap cleanup EXIT INT TERM
fail() { echo "practice site: $1" >&2; exit 1; }
host=migrations.localhost
get() { curl -s -m 10 --resolve "$host:$port:127.0.0.1" -H 'Accept: text/html' "http://$host:$port$1"; }
code() { curl -s -o /dev/null -w '%{http_code}' -m 10 --resolve "$host:$port:127.0.0.1" -H 'Accept: text/html' "http://$host:$port$1"; }
# Visible text only: tags, scripts and styles removed.
text() { get "$1" | perl -0pe 's/<(script|style)\b.*?<\/\1>//gs; s/<[^>]+>/ /g; s/&amp;/&/g; s/\s+/ /g'; }

sh "$here/practice-site/build.sh" "$web" "$home" "$port" >"$home/build.out" || fail "build: $(cat "$home/build.out")"
grep -q '"published":false' "$home/build.out" || fail "build did not report a draft"

routes="/ /how-a-move-runs /what-you-receive /sample-assessment-brief /offers /hosting-accounts /php-and-wordpress-sites /databases
/erp-and-business-systems /public-records-and-case-systems /cad-bim-and-design-files /documents-and-archives /custom-legacy-applications
/leaving-interworx /leaving-cpanel /leaving-plesk /agencies /contact"
for r in $routes; do [ "$(code "$r")" = 200 ] || fail "$r does not answer"; done
"$web" site check migrations --json --home "$home" --port "$port" | grep -q '"failed":0' || fail "the site fails its own check: $("$web" site check migrations --home "$home" --port "$port" || true)"

# Every range on the pricing page is the brief type's own, worded as an estimate.
types="$here/data/migration-brief-types.json"
offers=$(text /offers)
for id in hosting-account-acquisition php-rescue sql-erp-migration cad-ifc civic-legacy-modernization verification-rollback; do
  want=$(sqlite3 :memory: "SELECT printf('Estimated \$%,d to \$%,d per %s', json_extract(t.value,'\$.estimate.low'), json_extract(t.value,'\$.estimate.high'),
         json_extract(t.value,'\$.estimate.unit')) FROM json_each(readfile('$types'),'\$.types') t WHERE json_extract(t.value,'\$.id')='$id'")
  printf '%s' "$offers" | grep -qF "$want" || fail "pricing lacks the $id range ($want)"
done
[ "$(printf '%s' "$offers" | grep -o 'Priced after the assessment' | wc -l | tr -d ' ')" = 2 ] || fail "unpriced work is not marked as priced after the assessment"
printf '%s' "$offers" | grep -q 'planning estimate' || fail "the pricing page does not say its figures are estimates"
# No dollar figure anywhere that is not part of an estimated range.
for r in $routes; do
  if text "$r" | grep -oE '[^ ]+ \$[0-9][0-9,]*' | grep -vqE '^(Estimated|to) '; then fail "$r carries a bare price"; fi
done
[ "$(sqlite3 -readonly "$home/cms.db" "SELECT count(*) FROM site_fragment WHERE namespace='migrations' AND kind='offer' AND json_extract(projection_contract,'\$.amount') IS NOT NULL")" = 0 ] \
  || fail "a range was stored as an amount"

# Every practice and variation page leads to a form, and that form asks its questions.
for r in /hosting-accounts /php-and-wordpress-sites /databases /erp-and-business-systems /public-records-and-case-systems /cad-bim-and-design-files \
         /documents-and-archives /custom-legacy-applications /leaving-interworx /leaving-cpanel /leaving-plesk /agencies; do
  link=$(get "$r" | grep -o 'href="/contact?about=[^"]*"' | head -1 | cut -d'"' -f2)
  [ -n "$link" ] || fail "$r does not lead to a form"
  get "$link" | grep -q '<fieldset class="contact-intake">' || fail "$r leads to $link, which asks nothing"
done
# Each offer's questions are named by facts the brief types use where one exists.
for fact in site_count mailbox_count database_count dns_provider backup_location php_version target_php_version plugin_list source_engine \
            destination_engine object_counts source_formats editability_requirements personal_data_classes public_endpoints sign_off_owner; do
  sqlite3 -readonly "$home/cms.db" "SELECT 1 FROM site_fragment f, json_each(f.projection_contract,'\$.asks') a WHERE f.namespace='migrations'
     AND json_extract(a.value,'\$.fact')='$fact'" | grep -q 1 || fail "no offer asks for $fact"
  grep -q "\"$fact\"" "$types" || fail "$fact is not a fact the brief types name"
done

# Honest scope: no clients, testimonials, guarantees, internal words or draft leakage.
all=""; for r in $routes; do all="$all $(text "$r")"; done
for banned in 'testimonial' 'our clients' 'trusted by' 'guarantee' 'Upper90' 'case study' '100%' 'receipt' 'cannot_infer' 'organ' 'World' \
              'closure' 'witness' 'This site is live' 'Lorem'; do
  if printf '%s' "$all" | grep -qiw -- "$banned"; then fail "a page says '$banned'"; fi
done
printf '%s' "$(text /)" | grep -q 'Draft for review. Not published.' || fail "the front page does not say it is a draft"
printf '%s' "$(text /how-a-move-runs)" | grep -q 'Promise a move with no downtime' || fail "the limits of the work are not stated"

# The draft was never shared: no public address, and the site names only loopback.
[ ! -e "$home/share.json" ] && ! ls "$home" | grep -q '^share' || fail "the draft was shared"
"$web" site show migrations --json --home "$home" --port "$port" | grep -q '"host":"migrations.localhost"' || fail "the draft answers under another name"

# A request through the hosting form arrives with its answers.
status=$(curl -s -o "$home/form.out" -w '%{http_code}' -m 15 --resolve "$host:$port:127.0.0.1" -X POST "http://$host:$port/contact" \
  --data-urlencode 'name=Test Owner' --data-urlencode 'contact=owner@example.test' --data-urlencode 'message=About: Hosting account move. Thirty sites on one server.' \
  --data-urlencode 'about=Hosting account move' --data-urlencode 'ask_source_panel=InterWorx' --data-urlencode 'ask_site_count=30')
[ "$status" = 200 ] || fail "the hosting form refused a request: $(cat "$home/form.out")"
"$web" site inbox migrations --json --home "$home" --port "$port" | grep -q '"answers":{"source_panel":"InterWorx","site_count":"30"}' || fail "the request's answers are not in the inbox"
echo "practice site: PASS ($(echo $routes | wc -w | tr -d ' ') routes)"
