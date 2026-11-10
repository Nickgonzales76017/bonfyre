#!/bin/sh
# What this does not prove:
# - a server started in this test answers in place of the outside service, so nothing here is evidence about that service.
# - some checks run in embedded Python, which this estate has retired; they say nothing about a native path.
# The credential boundary keeps its promises:
#   - a key file is created once, private, and a readable one is refused;
#   - a value is sealed (no plaintext in the store), never shown again, and
#     bound to its provider and field;
#   - a generated secret is shown once, at generation;
#   - a fetcher gets the request it needs (Podcast Index's SHA-1 signature is
#     right), and a missing required field refuses;
#   - an OpenAPI document's security schemes become the fields and how they
#     are applied;
#   - verification reaches the provider with headers that never touch argv;
#   - the page answers only to a loopback Host, only a Bonfyre admin signs in,
#     a write needs the session's CSRF token and a same-site Origin, and no
#     stored value appears in any page.
set -eu
bin=${1:?credentials binary required}
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
[ -x "$auth" ] || { echo 'credentials: SKIP (auth not built)'; exit 0; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-credentials.XXXXXX")
pids=''
cleanup() { for p in $pids; do kill "$p" 2>/dev/null || true; done; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

python3 - "$bin" "$auth" "$tmp" <<'PY' || exit 1
import hashlib, http.server, json, os, re, socket, sqlite3, stat, subprocess, sys, threading, time, urllib.request, urllib.error
bin, auth, tmp = sys.argv[1:4]
problems = []
def check(ok, why):
    if not ok: problems.append(why)
def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p

# A stand-in provider that accepts only the right header.
seen = {}
class Stub(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        import base64
        length = int(self.headers.get("Content-Length") or 0); body = self.rfile.read(length).decode()
        ok = self.path == "/token" and "grant_type=client_credentials" in body and self.headers.get("Authorization") == "Basic " + base64.b64encode(b"cid-123:csecret-456").decode()
        seen["token_calls"] = seen.get("token_calls", 0) + 1
        self.send_response(200 if ok else 401); self.send_header("Content-Type", "application/json"); self.end_headers()
        self.wfile.write(json.dumps({"access_token": "exchanged-token-789", "expires_in": 3600} if ok else {"error": "invalid_client"}).encode())
    def do_GET(self):
        if self.path.startswith("/demo"):
            seen["demo"] = self.path
            self.send_response(200 if "api_key=DEMO_KEY" in self.path else 403); self.end_headers(); self.wfile.write(b"{}"); return
        seen["auth"] = self.headers.get("Authorization"); seen["path"] = self.path
        self.send_response(200 if self.headers.get("Authorization") == "Bearer good-token-value-1234" else 401)
        self.end_headers(); self.wfile.write(b"{}")
    def log_message(self, *a): pass
stub_port = free_port()
server = http.server.HTTPServer(("127.0.0.1", stub_port), Stub)
threading.Thread(target=server.serve_forever, daemon=True).start()

providers = [
    {"provider": "podcast-index", "name": "Podcast Index", "purpose": "p", "get_it": "https://api.podcastindex.org/signup",
     "fields": [{"field": "api_key", "label": "API key", "kind": "api_key", "sensitive": True, "required": True, "generator": "", "help": "h"},
                {"field": "api_secret", "label": "API secret", "kind": "secret", "sensitive": True, "required": True, "generator": "", "help": "h"}],
     "apply": {"headers": [{"name": "X-Auth-Key", "value": "{api_key}"}, {"name": "X-Auth-Date", "value": "{epoch}"}, {"name": "Authorization", "value": "{sha1:api_key+api_secret+epoch}"}], "query": []},
     "cannot_infer": "c"},
    {"provider": "stub", "name": "Stub service", "purpose": "p", "get_it": "https://example.invalid/",
     "fields": [{"field": "token", "label": "Token", "kind": "token", "sensitive": True, "required": True, "generator": "", "help": "h"},
                {"field": "account_id", "label": "Account", "kind": "account_id", "sensitive": False, "required": False, "generator": "", "help": "h"}],
     "apply": {"headers": [{"name": "Authorization", "value": "Bearer {token}"}], "query": [{"name": "account", "value": "{account_id}"}]},
     "verify": {"url": "http://127.0.0.1:%d/check" % stub_port, "expect": 200}, "cannot_infer": "c"},
    {"provider": "demo", "name": "Demo federal API", "purpose": "p", "get_it": "https://api.data.gov/signup/",
     "fields": [{"field": "api_key", "label": "Key", "kind": "api_key", "sensitive": True, "required": True, "generator": "", "public_default": "DEMO_KEY", "help": "h"}],
     "apply": {"headers": [], "query": [{"name": "api_key", "value": "{api_key}"}]},
     "verify": {"url": "http://127.0.0.1:%d/demo" % stub_port, "expect": 200}, "cannot_infer": "c"},
    {"provider": "oauth-svc", "name": "OAuth service", "purpose": "p", "get_it": "https://example.invalid/",
     "fields": [{"field": "client_id", "label": "Client ID", "kind": "client_id", "sensitive": False, "required": True, "generator": "", "help": "h"},
                {"field": "client_secret", "label": "Client secret", "kind": "client_secret", "sensitive": True, "required": True, "generator": "", "help": "h"}],
     "oauth": {"token_url": "http://127.0.0.1:%d/token" % stub_port, "client_id_field": "client_id", "client_secret_field": "client_secret"},
     "apply": {"headers": [{"name": "Authorization", "value": "Bearer {oauth_token}"}], "query": []}, "cannot_infer": "c"},
    {"provider": "aurekai-webhooks", "name": "Webhooks", "purpose": "p", "get_it": "https://aurekai.work/",
     "fields": [{"field": "signing_secret", "label": "Signing secret", "kind": "signing_key", "sensitive": True, "required": True, "generator": "random-base64url-32", "help": "h"}],
     "apply": {"headers": [], "query": []}, "cannot_infer": "c"},
]
prov = os.path.join(tmp, "providers.json"); json.dump(providers, open(prov, "w"))
db = os.path.join(tmp, "credentials.db"); key = os.path.join(tmp, "credentials.key")
def run(*args, stdin=None, codes=(0,)):
    p = subprocess.run([bin] + list(args), input=stdin, capture_output=True, text=True)
    if p.returncode not in codes: problems.append("%s -> %d %s" % (args[0], p.returncode, p.stderr.strip()[:160]))
    return p
base = ["--db", db, "--key", key, "--providers", prov]

check(run("init-key", "--key", key).returncode == 0, "a key is created")
check(stat.S_IMODE(os.stat(key).st_mode) == 0o600, "the key file is private")
check(run("init-key", "--key", key, codes=(1,)).returncode == 1, "an existing key is never replaced")
loose = os.path.join(tmp, "loose.key"); open(loose, "wb").write(os.urandom(32)); os.chmod(loose, 0o644)
check(run("status", "--db", db, "--providers", prov).returncode == 0, "status needs no key")
check(run("set", "--db", db, "--key", loose, "--providers", prov, "--provider", "stub", "--field", "token", stdin="x", codes=(2,)).returncode == 2, "a key readable by others is refused")

secret_key, secret = "PODKEYVALUE1234567890", "PODSECRETVALUE0987654321"
run("set", *base, "--provider", "podcast-index", "--field", "api_key", stdin=secret_key + "\n")
run("set", *base, "--provider", "podcast-index", "--field", "api_secret", stdin=secret)
status = json.loads(run("status", "--db", db, "--providers", prov).stdout)
fields = {f["field"]: f for p in status["providers"] if p["provider"] == "podcast-index" for f in p["fields"]}
check(fields["api_key"]["state"] == "set" and fields["api_key"]["shows"] == "7890", "status says set and shows only the last four")
check(secret_key not in json.dumps(status) and secret not in json.dumps(status), "status never carries a stored value")
raw = b"".join(bytes(r[0]) for r in sqlite3.connect(db).execute("select sealed from sealed_field"))
check(secret_key.encode() not in raw and secret.encode() not in raw, "the store holds no plaintext")
check(stat.S_IMODE(os.stat(db).st_mode) == 0o600, "the store is private")

applied = run("apply", *base, "--provider", "podcast-index", "--url", "https://api.podcastindex.org/api/1.0/recent/episodes?max=1")
answer = json.loads(applied.stdout or "{}")
headers = dict(h.split(": ", 1) for h in answer.get("headers", []))
epoch = headers.get("X-Auth-Date", "")
check(headers.get("X-Auth-Key") == secret_key, "the fetcher gets the key header")
check(headers.get("Authorization") == hashlib.sha1((secret_key + secret + epoch).encode()).hexdigest(), "the Podcast Index signature is sha1(key + secret + epoch)")
check(run("apply", *base, "--provider", "stub", "--url", "https://x.invalid/", codes=(4,)).returncode == 4, "a missing required field refuses the request")

# AAD binds a value to its slot: moving the sealed key into the secret's slot breaks it.
con = sqlite3.connect(db)
blob = con.execute("select sealed from sealed_field where provider='podcast-index' and field='api_key'").fetchone()[0]
con.execute("update sealed_field set sealed=? where provider='podcast-index' and field='api_secret'", (blob,)); con.commit()
check(run("apply", *base, "--provider", "podcast-index", "--url", "https://x.invalid/", codes=(4,)).returncode == 4, "a sealed value moved to another field does not open")
run("set", *base, "--provider", "podcast-index", "--field", "api_secret", stdin=secret)

# Automation: public defaults, OAuth exchange, readiness hook, bootstrap, verify-all.
demo = json.loads(run("apply", *base, "--provider", "demo", "--url", "https://api.nasa.gov/planetary/apod").stdout or "{}")
check(demo.get("url", "").endswith("api_key=DEMO_KEY"), "a provider's documented public default is used until a key is set")
check('"state":"public default"' in run("status", "--db", db, "--providers", prov).stdout, "status says the public default is in use")
hook_log = os.path.join(tmp, "ready.log")
hook = os.path.join(tmp, "hook.sh"); open(hook, "w").write("#!/bin/sh\necho \"$1\" >> %s\n" % hook_log); os.chmod(hook, 0o755)
env_hook = dict(os.environ, BONFYRE_CREDENTIALS_ON_READY=hook)
subprocess.run([bin, "set", *base, "--provider", "oauth-svc", "--field", "client_id"], input="cid-123", text=True, capture_output=True, env=env_hook)
check(not os.path.exists(hook_log), "a provider still missing a field does not announce itself ready")
subprocess.run([bin, "set", *base, "--provider", "oauth-svc", "--field", "client_secret"], input="csecret-456", text=True, capture_output=True, env=env_hook)
for _ in range(40):
    if os.path.exists(hook_log): break
    time.sleep(0.05)
check(os.path.exists(hook_log) and open(hook_log).read().strip() == "oauth-svc", "the moment a provider is ready, waiting reads are told its id and nothing else")
exchanged = json.loads(run("apply", *base, "--provider", "oauth-svc", "--url", "https://api.example.invalid/x").stdout or "{}")
check("Authorization: Bearer exchanged-token-789" in exchanged.get("headers", []), "a client-credentials token is exchanged and applied")
run("apply", *base, "--provider", "oauth-svc", "--url", "https://api.example.invalid/x")
check(seen.get("token_calls") == 1, "the exchanged token is cached sealed, not exchanged again (%s)" % seen.get("token_calls"))
raw_tokens = b"".join(bytes(r[0]) for r in sqlite3.connect(db).execute("select sealed from token_cache"))
check(b"exchanged-token-789" not in raw_tokens, "the cached token is sealed")
boot = json.loads(run("bootstrap", *base).stdout)
check({"provider": "aurekai-webhooks", "field": "signing_secret"} in boot["generated"] and boot["shown"] is False, "bootstrap generates what the estate can make, without showing it")
check(json.loads(run("bootstrap", *base).stdout)["count"] == 0, "bootstrap is idempotent")
verified = json.loads(run("verify-all", *base).stdout)
outcomes = {c["provider"]: c["outcome"] for c in verified["checks"]}
check(outcomes.get("demo") == "accepted" and "api_key=DEMO_KEY" in seen.get("demo", ""), "verify-all checks every ready provider, including one on its public default")
run("clear", "--db", db, "--providers", prov, "--provider", "aurekai-webhooks", "--field", "signing_secret")

gen = json.loads(run("generate", *base, "--provider", "aurekai-webhooks", "--field", "signing_secret", "--reveal").stdout)
shown = gen.get("value_shown_once", "")
check(len(shown) >= 40, "a generated secret is shown once")
later = run("status", "--db", db, "--providers", prov).stdout
check(shown not in later and '"state":"generated"' in later, "afterwards it is only 'generated'")

spec = {"openapi": "3.1.0", "info": {"title": "Weather Service"}, "servers": [{"url": "https://api.weather.example"}],
        "components": {"securitySchemes": {"KeyHeader": {"type": "apiKey", "in": "header", "name": "X-Api-Key"},
                                           "KeyQuery": {"type": "apiKey", "in": "query", "name": "appid"},
                                           "Bearer": {"type": "http", "scheme": "bearer"},
                                           "Basic": {"type": "http", "scheme": "basic"},
                                           "OAuth": {"type": "oauth2", "flows": {"clientCredentials": {"tokenUrl": "https://auth.weather.example/token", "scopes": {}}}}}}}
specfile = os.path.join(tmp, "spec.json"); json.dump(spec, open(specfile, "w"))
declared = json.loads(run("openapi", "--db", db, "--provider", "weather", "--spec", specfile).stdout)
names = [f["field"] for f in declared["fields"]]
check({"keyheader", "keyquery", "bearer", "basic_username", "basic_password", "oauth_client_id", "oauth_client_secret"} <= set(names), "every security scheme becomes its fields (%s)" % names)
check({"name": "X-Api-Key", "value": "{keyheader}"} in declared["apply"]["headers"] and {"name": "appid", "value": "{keyquery}"} in declared["apply"]["query"], "header and query keys apply where the document says")
check(declared.get("oauth", {}).get("token_url") == "https://auth.weather.example/token" and {"name": "Authorization", "value": "Bearer {oauth_token}"} in declared["apply"]["headers"], "an OAuth client-credentials scheme is exchanged by the boundary itself")
check(any(p["provider"] == "weather" and p["origin"] == "openapi" for p in json.loads(run("status", "--db", db, "--providers", prov).stdout)["providers"]), "an imported service is listed with the declared ones")

# The page
uid_line = subprocess.run([auth, "--db", os.path.join(tmp, "auth.db"), "http-signup"], input=json.dumps({"email": "op@t.test", "name": "Op", "password": "a-long-enough-passphrase"}), capture_output=True, text=True).stdout
subprocess.run([auth, "--db", os.path.join(tmp, "auth.db"), "http-signup"], input=json.dumps({"email": "reader@t.test", "name": "R", "password": "a-long-enough-passphrase"}), capture_output=True, text=True)
uid = json.loads(uid_line)["user"]["id"]
subprocess.run([auth, "--db", os.path.join(tmp, "auth.db"), "institution-grant", "--user-id", str(uid), "--institution", "bonfyre", "--role", "admin"], capture_output=True, text=True)
port = free_port()
page = subprocess.Popen([bin, "serve", *base, "--auth-db", os.path.join(tmp, "auth.db"), "--auth-binary", auth, "--port", str(port)], stderr=subprocess.DEVNULL)
open(os.path.join(tmp, "pid"), "w").write(str(page.pid))
for _ in range(80):
    try: socket.create_connection(("127.0.0.1", port), 0.2).close(); break
    except OSError: time.sleep(0.05)
def http(method, path, host=None, cookie=None, data=None, origin=None):
    host = host or "127.0.0.1:%d" % port
    s = socket.create_connection(("127.0.0.1", port), 5)
    body = data.encode() if data else b""
    head = "%s %s HTTP/1.1\r\nHost: %s\r\nContent-Length: %d\r\nContent-Type: application/x-www-form-urlencoded\r\n" % (method, path, host, len(body))
    if cookie: head += "Cookie: bonfyre_credentials=%s\r\n" % cookie
    if origin: head += "Origin: %s\r\n" % origin
    s.sendall(head.encode() + b"\r\n" + body)
    chunks = []
    while True:
        c = s.recv(65536)
        if not c: break
        chunks.append(c)
    s.close()
    text = b"".join(chunks).decode("utf-8", "replace")
    h, _, b = text.partition("\r\n\r\n")
    return int(h.split(" ")[1]), h, b
code, _, _ = http("GET", "/", host="attacker.example")
check(code == 421, "a Host that is not loopback is refused (%s)" % code)
code, _, body = http("GET", "/")
check(code == 200 and 'action="/login"' in body, "the page asks for sign-in")
code, _, _ = http("POST", "/login", data="email=reader%40t.test&password=a-long-enough-passphrase")
check(code == 403, "a signed-in person who is not a Bonfyre admin is refused")
code, head, _ = http("POST", "/login", data="email=op%40t.test&password=a-long-enough-passphrase")
cookie = re.search(r"bonfyre_credentials=([^;]+)", head)
check(code == 303 and cookie and "HttpOnly" in head and "SameSite=Strict" in head, "an admin signs in with a strict, http-only cookie")
cookie = cookie.group(1) if cookie else ""
code, head, body = http("GET", "/", cookie=cookie)
csrf = re.search(r'name="csrf" value="([0-9a-f]{64})"', body)
check(code == 200 and csrf and "Podcast Index" in body and "Weather Service" in body, "the admin sees every provider")
check(secret_key not in body and secret not in body and shown not in body, "no stored or generated value appears on the page")
check("no-store" in head and "frame-ancestors 'none'" in head, "the page is never cached or framed")
csrf = csrf.group(1) if csrf else ""
code, _, _ = http("POST", "/set", cookie=cookie, data="provider=stub&field=token&value=good-token-value-1234")
check(code == 403, "a write without the session's CSRF token is refused")
code, _, _ = http("POST", "/set", cookie=cookie, data="csrf=%s&provider=stub&field=token&value=good-token-value-1234" % csrf, origin="https://attacker.example")
check(code == 403, "a write from another origin is refused")
code, _, body = http("POST", "/set", cookie=cookie, data="csrf=%s&provider=stub&field=token&value=good-token-value-1234" % csrf, origin="http://127.0.0.1:%d" % port)
check(code == 200 and "Saved and sealed." in body and "good-token-value-1234" not in body, "an admin write is sealed and not echoed")
code, _, body = http("POST", "/verify", cookie=cookie, data="csrf=%s&provider=stub" % csrf)
check("It works" in body and seen.get("auth") == "Bearer good-token-value-1234", "checking it reaches the provider with the stored credential")
ps = subprocess.run(["ps", "-ax", "-o", "args"], capture_output=True, text=True).stdout
check("good-token-value-1234" not in ps, "no credential appears in any process's arguments")
code, _, body = http("POST", "/generate", cookie=cookie, data="csrf=%s&provider=aurekai-webhooks&field=signing_secret" % csrf)
once = re.search(r'<p class="reveal">([^<]+)</p>', body)
check(once is not None, "generating on the page shows the new value once")
code, _, body = http("GET", "/", cookie=cookie)
check(once is None or once.group(1) not in body, "and never again")
audit = sqlite3.connect(db).execute("select group_concat(action||':'||outcome, ' ') from audit").fetchone()[0]
check("sign-in:refused" in audit and "set:stored" in audit and "verify:accepted" in audit, "every act is audited without its value")
check("good-token-value-1234" not in audit, "the audit holds no value")
page.terminate()
server.shutdown()
if problems:
    print("credentials: FAIL")
    for p in problems: print("  -", p)
    sys.exit(1)
PY
echo 'credentials: PASS'
