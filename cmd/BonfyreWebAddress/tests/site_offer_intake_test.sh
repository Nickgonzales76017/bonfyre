#!/bin/sh
# An offer can carry intake questions.  Whoever takes it up from the offers
# page meets those questions on the contact form; the answers they give land in
# the owner's inbox under the fact each question names, and nothing else does.
# An offer without questions keeps the plain form.
set -eu
web=${1:-${BONFYRE_TEST_WEB_BINARY:?web-address binary required}}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
for organ in BonfyreCMS/bonfyre-cms BonfyreIngest/bonfyre-ingest BonfyreParagraph/bonfyre-paragraph BonfyreRecall/bonfyre-recall; do
  [ -x "$root/cmd/$organ" ] || { echo "site offer intake: SKIP ($organ not built)"; exit 0; }
done
export BONFYRE_CMS_BINARY="$root/cmd/BonfyreCMS/bonfyre-cms" BONFYRE_INGEST_BINARY="$root/cmd/BonfyreIngest/bonfyre-ingest"
export BONFYRE_PARAGRAPH_BINARY="$root/cmd/BonfyreParagraph/bonfyre-paragraph" BONFYRE_RECALL_BINARY="$root/cmd/BonfyreRecall/bonfyre-recall"
export BONFYRE_CMS_SCHEMAS="$root/cmd/BonfyreCMS/content-types" BONFYRE_SITE_ROOT="$root/site/matrix"
home=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-intake.XXXXXX")
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
cleanup() { "$web" site stop --home "$home" >/dev/null 2>&1 || true; rm -rf "$home"; }
trap cleanup EXIT INT TERM
fail() { echo "site offer intake: $1" >&2; exit 1; }
site() { "$web" site "$@" --home "$home" --port "$port"; }
get() { curl -s -m 10 --resolve "movers.localhost:$port:127.0.0.1" -H 'Accept: text/html' "http://movers.localhost:$port$1"; }
form() { curl -s -m 15 -o "$home/form.out" -w '%{http_code}' --resolve "movers.localhost:$port:127.0.0.1" -X POST "http://movers.localhost:$port/contact" "$@"; }

site new movers --title 'Movers' --no-open >/dev/null
# Questions that are not FACT=Question, facts that are not plain names, a fact
# asked twice, and a ninth question are each refused before anything is kept.
if site offer movers --title X --text 'x y z' --ask 'no equals sign' >/dev/null 2>&1; then fail "a question without a fact was taken"; fi
if site offer movers --title X --text 'x y z' --ask 'Site Count=How many?' >/dev/null 2>&1; then fail "a fact that is not a plain name was taken"; fi
if site offer movers --title X --text 'x y z' --ask 'a=How many?' --ask 'a=Again?' >/dev/null 2>&1; then fail "a fact asked twice was taken"; fi
nine=""; for n in 1 2 3 4 5 6 7 8 9; do nine="$nine --ask f$n=Question$n"; done
# shellcheck disable=SC2086
if site offer movers --title X --text 'x y z' $nine >/dev/null 2>&1; then fail "nine questions were taken"; fi
[ "$(sqlite3 -readonly "$home/cms.db" "SELECT count(*) FROM site_fragment WHERE namespace='movers' AND kind='offer'")" = 0 ] || fail "a refused offer left a record"

made=$(site offer movers --title 'Hosting move' --price 'Estimated $300 to $900 per site' --text 'Each site staged and checked.' \
  --ask 'site_count=About how many sites?' --ask 'dns_provider=Who looks after <your> DNS?')
printf '%s' "$made" | grep -q '"intake_questions":2,"on_page":true' || fail "offer with questions: $made"
site offer movers --title 'Plain call' --price 'Free' --text 'A short call.' | grep -q '"intake_questions":0' || fail "offer without questions"
# A range in words is not read as an amount.
[ "$(sqlite3 -readonly "$home/cms.db" "SELECT COALESCE(json_extract(projection_contract,'\$.amount'),'none') FROM site_fragment WHERE namespace='movers' AND heading='Hosting move'")" = none ] || fail "a range was read as an amount"

page=$(get '/contact?about=Hosting%20move')
printf '%s' "$page" | grep -q '<fieldset class="contact-intake">' || fail "the form for the offer does not ask its questions"
printf '%s' "$page" | grep -q '<label>About how many sites?<input name="ask_site_count"' || fail "first question missing"
printf '%s' "$page" | grep -q 'Who looks after &lt;your&gt; DNS?' || fail "a question was written as markup"
if get '/contact?about=Plain%20call' | grep -q 'contact-intake'; then fail "an offer without questions grew a fieldset"; fi
if get '/contact' | grep -q 'contact-intake'; then fail "the plain contact page asks questions"; fi
if get '/contact?about=Nothing%20here' | grep -q 'contact-intake'; then fail "a made-up subject asks questions"; fi
get /offers | grep -q 'href="/contact?about=Hosting%20move"' || fail "the offer does not lead to its form"

[ "$(form --data-urlencode 'name=Ada' --data-urlencode 'contact=ada@example.test' --data-urlencode 'message=About: Hosting move. Please look at our server.' \
      --data-urlencode 'about=Hosting move' --data-urlencode 'ask_site_count=about 30' --data-urlencode 'ask_dns_provider=' \
      --data-urlencode 'ask_not_asked=sneaky')" = 200 ] || fail "the form refused a person: $(cat "$home/form.out")"
grep -q 'Thank you, Ada' "$home/form.out" || fail "no confirmation"
inbox=$(site inbox movers --json)
printf '%s' "$inbox" | grep -q '"about":"Hosting move","answers":{"site_count":"about 30"}' || fail "the answers are not in the inbox: $inbox"
if printf '%s' "$inbox" | grep -q 'sneaky\|dns_provider'; then fail "an unasked or blank answer was kept"; fi
site inbox movers | grep -q '^  site_count: about 30$' || fail "the inbox text does not show the answer"
# Answers to a different offer's questions are not taken under another subject.
form --data-urlencode 'name=Grace' --data-urlencode 'contact=grace@example.test' --data-urlencode 'message=Just a call please.' \
     --data-urlencode 'about=Plain call' --data-urlencode 'ask_site_count=12' >/dev/null
if site inbox movers --json | grep -q '"site_count":"12"'; then fail "an answer was taken for a question that offer does not ask"; fi
echo "site offer intake: PASS"
