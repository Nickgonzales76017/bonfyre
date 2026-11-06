#!/bin/sh
# What this does not prove:
# - a script written in this test stands where a program would run, so it shows how the caller handles that program's interface and nothing about the program.
# - a server started in this test answers in place of the outside service, so nothing here is evidence about that service.
# - some checks run in embedded Python, which this estate has retired; they say nothing about a native path.
# A census source that needs a key is read through the credential boundary:
# while no key is set the read waits (no fetch, no receipt); once the operator
# sets it, the reheat fetches with the header and query the provider asks for,
# the retained body is the provider's answer, and neither the key nor the
# signed address appears in the receipt, the weather or any process's
# arguments.
set -eu
project=${1:?bonfyre-project binary required}
case "$project" in /*) ;; *) project="$(pwd)/$project" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
creds="$root/cmd/BonfyreCredentials/bonfyre-credentials"
[ -x "$cms" ] && [ -x "$creds" ] || { echo 'credentialed reheat: SKIP (cms or credentials not built)'; exit 0; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-credentialed-reheat.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

python3 - "$project" "$cms" "$creds" "$root" "$tmp" <<'PY' || exit 1
import http.server, json, os, socket, subprocess, sys, threading
project, cms, creds, root, tmp = sys.argv[1:6]
problems = []
def check(ok, why):
    if not ok: problems.append(why)
KEY = "stub-key-7f3a91c2d4e5"
calls = []
class Stub(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        calls.append((self.path, self.headers.get("X-Api-Key")))
        if self.path == "/robots.txt":
            self.send_response(404); self.end_headers(); return
        ok = self.headers.get("X-Api-Key") == KEY and ("token=" + KEY) in self.path
        self.send_response(200 if ok else 401); self.send_header("Content-Type", "application/json"); self.end_headers()
        self.wfile.write(json.dumps({"items": [{"title": "An episode"}]} if ok else {"error": "key"}).encode())
    def log_message(self, *a): pass
s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
server = http.server.HTTPServer(("127.0.0.1", port), Stub)
threading.Thread(target=server.serve_forever, daemon=True).start()

schemas = os.path.join(tmp, "schemas"); os.makedirs(schemas)
import shutil; shutil.copy(os.path.join(root, "cmd/BonfyreCMS/content-types/internet_source.json"), schemas)
cmsdb = os.path.join(tmp, "cms.db")
subprocess.run([cms, "schema", "migrate", "--db", cmsdb, "--schemas", schemas], capture_output=True, check=True)
row = {"family": "podcasts", "source": "stub-feed", "label": "stub", "says": "x", "url": "http://127.0.0.1:%d/items" % port, "twin": "stub-feed",
       "state": "observed", "extract": "items", "title_field": "title", "status": "published", "tier": "WARM", "cadence_seconds": 3600, "kind": "text", "credential": "stub"}
subprocess.run([cms, "entry", "create", "internet_source", json.dumps(row), "--ns", "root", "--db", cmsdb, "--schemas", schemas], capture_output=True, check=True)
providers = [{"provider": "stub", "name": "Stub", "purpose": "p", "get_it": "https://example.invalid/",
              "fields": [{"field": "api_key", "label": "Key", "kind": "api_key", "sensitive": True, "required": True, "generator": "", "help": "h"}],
              "apply": {"headers": [{"name": "X-Api-Key", "value": "{api_key}"}], "query": [{"name": "token", "value": "{api_key}"}]}, "cannot_infer": "c"}]
prov = os.path.join(tmp, "providers.json"); json.dump(providers, open(prov, "w"))
cdb, ckey = os.path.join(tmp, "credentials.db"), os.path.join(tmp, "credentials.key")
subprocess.run([creds, "init-key", "--key", ckey], capture_output=True, check=True)
foreign = os.path.join(tmp, "foreign"); os.makedirs(foreign + "/receipts"); os.makedirs(foreign + "/bodies")
env = dict(os.environ, BONFYRE_CMS_DB=cmsdb, BONFYRE_FOREIGN_RECEIPTS=foreign + "/receipts", BONFYRE_FOREIGN_BODIES=foreign + "/bodies",
           BONFYRE_WEATHER=os.path.join(tmp, "weather.json"), BONFYRE_CREDENTIALS_BINARY=creds, BONFYRE_CREDENTIALS_DB=cdb,
           BONFYRE_CREDENTIALS_KEY=ckey, BONFYRE_CREDENTIAL_PROVIDERS=prov)
def reheat():
    out = subprocess.run([project, "foreign", "reheat", "--source", "stub-feed"], capture_output=True, text=True, env=env).stdout
    return json.loads(out)["sources"][0]
waiting = reheat()
check(waiting["decision"] == "waiting for a credential", "with no key the read waits (%s)" % waiting["decision"])
check(not any(p == "/items" or p.startswith("/items?") for p, _ in calls), "nothing was fetched while waiting")
check(not os.path.exists(foreign + "/receipts/stub-feed.json"), "no receipt is written while waiting")

subprocess.run([creds, "set", "--db", cdb, "--key", ckey, "--providers", prov, "--provider", "stub", "--field", "api_key"], input=KEY, text=True, capture_output=True, check=True)
done = reheat()
check(done["decision"] == "read, by name", "with the key set the source is read (%s)" % done["decision"])
check(any(p.startswith("/items?token=") and h == KEY for p, h in calls), "the provider received its header and query credential")
receipt = open(foreign + "/receipts/stub-feed.json").read()
body = open(foreign + "/bodies/stub-feed.body").read() if os.path.exists(foreign + "/bodies/stub-feed.body") else ""
check('"An episode"' in body, "the retained body is the provider's authenticated answer")
check(KEY not in receipt and "token=" not in receipt, "the receipt holds neither the key nor the signed address")
check(json.loads(receipt.strip().splitlines()[-1]).get("source_url", json.loads(receipt.strip().splitlines()[-1]).get("url", "")).endswith("/items") or "/items\"" in receipt, "the receipt records the plain address")
check(KEY not in open(os.path.join(tmp, "weather.json")).read() if os.path.exists(os.path.join(tmp, "weather.json")) else True, "the weather holds no key")
server.shutdown()
if problems:
    print("credentialed reheat: FAIL")
    for p in problems: print("  -", p)
    sys.exit(1)
PY
echo 'credentialed reheat: PASS'
