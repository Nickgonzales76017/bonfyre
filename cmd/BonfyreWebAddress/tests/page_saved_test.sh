#!/bin/sh
# Save is real: a signed-in person keeps something, it is theirs alone, it
# survives its source moving on, and it can be let go.  An anonymous visitor
# is sent to sign in and brought straight back.  A save attempted from another
# site is refused.  No step needs a script.
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
[ -x "$cms" ] || { echo 'page saved: SKIP (cms not built)'; exit 0; }
[ -x "$auth" ] || { echo 'page saved: SKIP (auth not built)'; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-page-saved.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

schemas="$tmp/schemas"; saved_schema="$tmp/saved-type"
mkdir -p "$schemas" "$saved_schema" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
for type in world world_fragment internet_source publication_occurrence content_flag; do
  cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$schemas/"
done
cp "$root/cmd/BonfyreCMS/content-types/saved_item.json" "$saved_schema/"
cmsdb="$tmp/cms.db"
"$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create internet_source '{"family":"news","source":"newsdesk","label":"x","says":"x","url":"https://www.newsdesk.test/rss","twin":"newsdesk-twin","state":"observed","extract":"rss:item","title_field":"title","meta_field":"","link_field":"feed:auto","image_field":"","image_base":"","sub_field":"","body_regime":"retained-public","status":"published","tier":"warm","kind":"post"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
printf '%s\n' "{\"body_path\":\"$tmp/foreign/bodies/newsdesk.body\"}" >"$tmp/foreign/receipts/newsdesk-twin.json"
cat >"$tmp/foreign/bodies/newsdesk.body" <<'XML'
<?xml version="1.0"?><rss xmlns:media="http://search.yahoo.com/mrss/"><channel>
<item><title>City opens a new library</title><link>https://www.newsdesk.test/library</link><pubDate>Tue, 15 Sep 2026 10:00:00 GMT</pubDate><media:thumbnail url="https://www.newsdesk.test/library.jpg"/></item>
<item><title>Council votes on transit plan</title><link>https://www.newsdesk.test/transit</link><pubDate>Tue, 15 Sep 2026 09:00:00 GMT</pubDate></item>
</channel></rss>
XML

matrix="$tmp/matrix.db"
"$web" bootstrap-owned --db "$matrix" >/dev/null
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_AUTH_BINARY="$auth" BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" \
BONFYRE_SAVED_ITEM_SCHEMAS="$saved_schema" BONFYRE_FOREIGN_ROOT="$tmp/foreign" \
  "$web" serve --db "$matrix" --auth-db "$tmp/auth.db" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
attempt=0
until curl -sS -o /dev/null -H 'Host: aurekai.life' "http://127.0.0.1:$port/healthz" 2>/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 80 ] || ! kill -0 "$pid" 2>/dev/null; then cat "$tmp/server.log" >&2; echo 'page saved: server did not start' >&2; exit 1; fi
  sleep 0.05
done

python3 - "$port" "$tmp" <<'PY' || exit 1
import json, re, subprocess, sys
from urllib.parse import urlsplit, parse_qs
port, tmp = sys.argv[1], sys.argv[2]
base = "http://127.0.0.1:%s" % port
problems = []
def check(ok, why):
    if not ok: problems.append(why)
def call(method, path, cookie=None, data=None, origin=None, accept="text/html", ctype=None):
    cmd = ["curl", "-sS", "-i", "-X", method, "-H", "Host: aurekai.life", "-H", "Accept: " + accept]
    if cookie: cmd += ["-H", "Cookie: bonfyre_session=" + cookie]
    if origin: cmd += ["-H", "Origin: " + origin]
    if ctype: cmd += ["-H", "Content-Type: " + ctype]
    if data is not None: cmd += ["--data", data]
    out = subprocess.run(cmd + [base + path], capture_output=True).stdout.decode("utf-8", "replace")
    head, _, body = out.partition("\r\n\r\n")
    code = head.split(" ")[1] if " " in head else ""
    return code, head, body

def token_for(email):
    pw = "a-long-enough-passphrase"
    call("POST", "/signup", data=json.dumps({"email": email, "name": "Tester", "password": pw}), accept="application/json", ctype="application/json")
    _, _, body = call("POST", "/login", data=json.dumps({"email": email, "password": pw}), accept="application/json", ctype="application/json")
    return json.loads(body)["session"]["token"]

code, head, discover = call("GET", "/discover")
found = re.search(r'href="/o/(newsdesk/[0-9a-f]{24})"', discover)
if not found:
    b = discover.split("</head>")[-1]; print("page saved: FAIL: no item", code, b[:1500]); sys.exit(1)
item = found.group(1)
path = "/o/" + item

code, _, landing = call("GET", "/discover")
check(('data-item-keep href="' + path + '#item-keep"') in landing, "feed leads into the exact retained item Save action")
code, head, anon = call("GET", path)
check(code == "200" and 'href="/login?next=%2Fo%2F' not in anon and ('href="/login?next=' + path + '"') in anon, "anonymous item page offers sign in that returns here")
entry = re.search(r'<a\b[^>]*id="session-link"[^>]*href="([^"]+)"', anon)
entry_url = urlsplit(entry.group(1) if entry else "")
check(entry_url.path == "/session/continue" and parse_qs(entry_url.query).get("next") == [path],
      "the shared sign-in entry retains the exact item destination")
code, head, _ = call("POST", "/o/save", data="item=%s&return=%s" % (item, path), origin="http://aurekai.life")
check(code == "303" and ("Location: /login?next=" + path) in head, "an anonymous save goes to sign in and returns (%s)" % code)

maya = token_for("maya@t.test")
sam = token_for("sam@t.test")
code, head, signed = call("GET", path, cookie=maya)
check('id="item-keep"' in signed, "native item page exposes the precise Keep section anchor")
check('action="/o/save"' in signed and 'aria-pressed="false"' in signed, "a signed-in person sees a real Save form")
check("private, no-store" in head, "a page that knows its reader is never publicly cached")
menu = re.search(r'<nav class="ak-user-links"[^>]*>(.*?)</nav>', signed, re.S)
check(menu is not None and re.search(r'<a\b[^>]*href="/saved"[^>]*>.*?Saved material</a>', menu.group(1), re.S),
      "a signed-in person reaches saved material through the native account menu")

code, head, _ = call("POST", "/o/save", cookie=maya, data="item=%s&return=%s" % (item, path), origin="https://evil.test")
check(code == "403", "a save attempted from another site is refused (%s)" % code)
_, _, still = call("GET", "/saved", cookie=maya)
check("City opens a new library" not in still, "the refused save kept nothing")

code, head, _ = call("POST", "/o/save", cookie=maya, data="item=%s&return=%s" % (item, path), origin="http://aurekai.life")
check(code == "303" and ("Location: " + path) in head, "saving returns to the page (%s)" % code)
_, _, after = call("GET", path, cookie=maya)
check('Add to a World' in after and '/profile?tab=sites' in after, "saved source offers the existing owner profile World setup")
check('action="/o/unsave"' in after and 'aria-pressed="true"' in after and "<span>Saved</span>" in after, "the page shows it is saved")
_, _, saved = call("GET", "/saved", cookie=maya)
check("<title>Saved</title>" in saved and "City opens a new library" in saved, "the saved page lists it")
check(('href="' + path + '"') in saved, "a saved item opens its page")
call("POST", "/o/save", cookie=maya, data="item=%s&return=%s" % (item, path), origin="http://aurekai.life")
rows = subprocess.run(["sqlite3", tmp + "/cms.db", "select count(*) from saved_item"], capture_output=True, text=True).stdout.strip()
check(rows == "1", "saving twice keeps one saved item (%s)" % rows)

_, _, sams = call("GET", "/saved", cookie=sam)
check("City opens a new library" not in sams, "another person cannot see what someone saved")
code, head, _ = call("GET", "/saved")
check(code == "303" and "Location: /login?next=/saved" in head, "saved things need a signed-in person")

# The source moves on: the item leaves its feed.
body = open(tmp + "/foreign/bodies/newsdesk.body").read()
open(tmp + "/foreign/bodies/newsdesk.body", "w").write(re.sub(r"<item><title>City opens a new library.*?</item>", "", body, flags=re.S))
code, _, kept = call("GET", path, cookie=maya)
check(code == "200" and "You saved this" in kept and "City opens a new library" in kept, "a kept thing outlives its source's feed for the person who kept it")
check('href="https://www.newsdesk.test/library"' in kept, "its original link is still there")
code, _, gone = call("GET", path)
check(code == "404", "for everyone else it is gone, as its source says (%s)" % code)

# Signing in through the page's own form returns the person to where they were.
code, head, _ = call("POST", "/login", data="email=maya%40t.test&password=a-long-enough-passphrase&next=%2Fsaved")
check(code == "303" and "Location: /saved" in head and "bonfyre_session=" in head, "a sign-in form returns to its next page with a session (%s)" % code)
code, head, _ = call("POST", "/login", data="email=maya%40t.test&password=wrong-password-entirely&next=%2Fsaved")
retry_match = re.search(r"(?im)^Location: ([^\r\n]+)", head)
retry = urlsplit(retry_match.group(1) if retry_match else "")
# Compare the exact destination and fields, not equivalent percent-encoding spellings.
check(code == "303" and not retry.scheme and not retry.netloc and retry.path == "/login" and parse_qs(retry.query, keep_blank_values=True) == {"next": ["/saved"], "failed": ["1"]} and "bonfyre_session=" not in head, "a failed sign-in goes back to sign in, keeping where it was going")
code, head, _ = call("POST", "/login", data="email=maya%40t.test&password=a-long-enough-passphrase&next=%2F%2Fevil.test")
check(code == "303" and "Location: /account" in head, "a next that leaves the site is never followed")

code, head, _ = call("POST", "/o/unsave", cookie=maya, data="item=%s&return=/saved" % item, origin="http://aurekai.life")
check(code == "303" and "Location: /saved" in head, "letting go returns where asked")
_, _, emptied = call("GET", "/saved", cookie=maya)
check("City opens a new library" not in emptied, "letting go removes it")

if problems:
    print("page saved: FAIL")
    for p in problems: print("  -", p)
    sys.exit(1)
PY
grep -q 'if(/^\\/(?!\[\\/\\\\\])\[^\\s\]\*$/.test(candidate)) return candidate;' "$root/site/matrix/surfaces/login.html" || { echo 'page saved: FAIL: sign-in does not return to a same-site page'; exit 1; }
echo 'page saved: PASS'
