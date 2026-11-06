#!/bin/sh
# A property is a real, continuous subject.  Search finds it by address, its
# page reads the observed Tarrant roll every time it opens, its actions are the
# parcel aperture's own in plain words, a signed-in person can keep it, and a
# machine asking for the same address still gets the machine answer.
#
# Needs the local columnar estate; set BONFYRE_TEST_FLEMING_ROOT or keep the
# repository's fleming evidence pack in place.
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
fleming="${BONFYRE_TEST_FLEMING_ROOT:-$root/packs/fleming-equity-group/evidence}"
query_bin=$(command -v bonfyre-query || true)
[ -x "$cms" ] && [ -x "$auth" ] || { echo 'page parcel: SKIP (cms/auth not built)'; exit 0; }
[ -f "$fleming/estate-lite.db" ] && [ -n "$query_bin" ] || { echo 'page parcel: SKIP (no local estate or bonfyre-query)'; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-page-parcel.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM
mkdir -p "$tmp/schemas" "$tmp/saved-type" "$tmp/bin" "$tmp/duckdb"
ln -s "$query_bin" "$tmp/bin/bonfyre-query"
for type in world internet_source publication_occurrence content_flag; do cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$tmp/schemas/"; done
cp "$root/cmd/BonfyreCMS/content-types/saved_item.json" "$tmp/saved-type/"
"$cms" schema migrate --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null

"$web" bootstrap-owned --db "$tmp/matrix.db" >/dev/null
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_AUTH_BINARY="$auth" BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_SAVED_ITEM_SCHEMAS="$tmp/saved-type" \
BONFYRE_FLEMING_ROOT="$fleming" BONFYRE_BIN_DIR="$tmp/bin" BONFYRE_DUCKDB_HOME="$tmp/duckdb" \
  "$web" serve --db "$tmp/matrix.db" --auth-db "$tmp/auth.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
attempt=0
until curl -sS -o /dev/null -H 'Host: aurekai.work' "http://127.0.0.1:$port/healthz" 2>/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 80 ] || ! kill -0 "$pid" 2>/dev/null; then cat "$tmp/server.log" >&2; echo 'page parcel: server did not start' >&2; exit 1; fi
  sleep 0.05
done

python3 - "$port" <<'PY' || exit 1
import json, re, subprocess, sys, html as H
port = sys.argv[1]; base = "http://127.0.0.1:%s" % port
problems = []
def check(ok, why):
    if not ok: problems.append(why)
def call(method, path, cookie=None, data=None, accept="text/html", ctype=None, origin=None):
    cmd = ["curl", "-sS", "-i", "-X", method, "-H", "Host: aurekai.work", "-H", "Accept: " + accept]
    if cookie: cmd += ["-H", "Cookie: bonfyre_session=" + cookie]
    if ctype: cmd += ["-H", "Content-Type: " + ctype]
    if origin: cmd += ["-H", "Origin: " + origin]
    if data is not None: cmd += ["--data", data]
    out = subprocess.run(cmd + [base + path], capture_output=True).stdout.decode("utf-8", "replace")
    head, _, body = out.partition("\r\n\r\n")
    return head.split(" ")[1] if " " in head else "", head, body

code, _, found = call("GET", "/search?q=117%20e%20belknap")
check(code == "200" and 'region-properties' in found, "search reaches the property roll")
check('href="/s/parcel/00000051"' in found and "117 E Belknap St" in found, "a street address opens its property")

code, head, page = call("GET", "/s/parcel/00000051")
check(code == "200" and "<title>117 E Belknap St</title>" in page, "the property page is titled by its address")
for fact in ("Owner of record", "Fort Worth City Of", "Assessed total", "$450,000", "Legal description", "Fort Worth Original Town Block 2 Lot 1"):
    check(fact in page, "the page states: " + fact)
check("Ft Worth, TX" in page, "a state stays a state")
check('href="/property/market?account=00000051"' in page and "See this market" in page, "the parcel's own market action is offered in words")
check('href="/login?next=/s/parcel/00000051"' in page, "saving a property asks a person to sign in and comes back")
neighbours = re.findall(r'href="(/s/parcel/\d+)"', page.split("Also on")[-1]) if "Also on" in page else []
check(len(neighbours) >= 3 and "/s/parcel/00000051" not in neighbours, "neighbours on the same street are other properties (%d)" % len(neighbours))
text = H.unescape(re.sub(r"<[^>]+>", " ", re.sub(r"<(script|style|template)[^>]*>.*?</\1>", " ", page.split("</head>", 1)[-1], flags=re.S)))
leak = re.findall(r"\b(World|Surface|Scene|Closure|Runtime|Semantic|Occurrence|Projection|Extent|Aperture|Frontier|Candidate|ForeignTwin|AddressPlane)\b", text)
check(not leak, "no machine vocabulary on the property page: %s" % sorted(set(leak)))
if neighbours:
    code, _, other = call("GET", neighbours[0])
    check(code == "200" and "Also on E Belknap St" in other, "a neighbour opens its own property page")

code, _, missing = call("GET", "/s/parcel/99999999")
check("Owner of record" not in missing and "Also on" not in missing, "an account the roll does not hold is not given a made-up property page (%s)" % code)
code, head, machine = call("GET", "/s/parcel/00000051", accept="application/json")
check("<title>117 E Belknap St</title>" not in machine, "a machine asking for the property keeps its own answer")

pw = "a-long-enough-passphrase"
call("POST", "/signup", data=json.dumps({"email": "p@t.test", "name": "P", "password": pw}), accept="application/json", ctype="application/json")
_, _, body = call("POST", "/login", data=json.dumps({"email": "p@t.test", "password": pw}), accept="application/json", ctype="application/json")
token = json.loads(body)["session"]["token"]
code, head, _ = call("POST", "/o/save", cookie=token, data="item=parcel/00000051&return=/s/parcel/00000051", origin="http://aurekai.work")
check(code == "303" and "Location: /s/parcel/00000051" in head, "saving a property returns to it (%s)" % code)
_, _, saved = call("GET", "/saved", cookie=token)
check("117 E Belknap St" in saved and 'href="/s/parcel/00000051"' in saved, "a saved property is listed and opens its page")
_, _, again = call("GET", "/s/parcel/00000051", cookie=token)
check('aria-pressed="true"' in again and "<span>Saved</span>" in again, "the property shows it is saved")

if problems:
    print("page parcel: FAIL")
    for p in problems: print("  -", p)
    sys.exit(1)
PY
echo 'page parcel: PASS'
