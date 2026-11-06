#!/bin/sh
# Witness for customer containers: open makes and reads back one container per
# customer, a second open does not duplicate it, close removes it, the held key
# never reaches argv or output, and a bad customer name or API is refused.
set -eu
bin=$(CDPATH= cd -- "$(dirname -- "$1")" && pwd)/$(basename -- "$1")
t=$(mktemp -d "${TMPDIR:-/tmp}/customer.XXXXXX")
fail() { echo "customer: FAIL: $1" >&2; exit 1; }
port=$((20000 + $$ % 20000))
python3 -I - "$port" <<'PY' &
import http.server, json, sys, urllib.parse
port = int(sys.argv[1]); projects = {}; posts = []
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def reply(self, code, body):
        b = json.dumps(body).encode(); self.send_response(code); self.send_header('Content-Length', str(len(b))); self.end_headers(); self.wfile.write(b)
    def ok(self): return self.headers.get('Authorization') == 'Bearer held-neon-key'
    def do_GET(self):
        if not self.ok(): return self.reply(401, {})
        u = urllib.parse.urlparse(self.path); q = urllib.parse.parse_qs(u.query)
        if u.path == '/users/me/organizations': return self.reply(200, {'organizations': [{'id': 'org-test'}]})
        if u.path == '/projects' and q.get('org_id') == ['org-test']:
            return self.reply(200, {'projects': [{'id': i, 'name': n} for i, n in projects.items() if q.get('search', [''])[0] in n]})
        if u.path == '/posts': return self.reply(200, {'n': len(posts)})
        return self.reply(404, {})
    def do_POST(self):
        if not self.ok(): return self.reply(401, {})
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        if self.path == '/projects' and body['project']['org_id'] == 'org-test':
            pid = 'p-%d' % (len(projects) + 1); projects[pid] = body['project']['name']; posts.append(pid)
            return self.reply(201, {'project': {'id': pid}, 'connection_uris': [{'connection_uri': 'postgres://u:SECRETPW@h/db'}]})
        return self.reply(400, {})
    def do_DELETE(self):
        if not self.ok(): return self.reply(401, {})
        pid = self.path.rsplit('/', 1)[1]
        return self.reply(200, {}) if projects.pop(pid, None) else self.reply(404, {})
http.server.ThreadingHTTPServer(('127.0.0.1', port), H).serve_forever()
PY
api=$!; trap 'kill $api 2>/dev/null; rm -rf "$t"' EXIT INT TERM
sleep 1
printf 'export NEON_API_KEY="held-neon-key"\n' > "$t/env"
c() { "$bin" customer "$@" --provider neon --env "$t/env" --api "http://127.0.0.1:$port"; }
if c show --customer acme >/dev/null; then fail "a customer with no container was shown as open"; fi
out=$(c open --customer acme) || fail "open failed"
echo "$out" | grep -q '"state":"open","id":"p-1","container":"org-test/aurekai-c-acme"' || fail "open did not read the container back: $out"
case "$out" in *SECRETPW*|*held-neon-key*) fail "open printed a secret" ;; esac
c open --customer acme | grep -q '"id":"p-1"' || fail "a second open did not find the first container"
[ "$(curl -s -H 'Authorization: Bearer held-neon-key' "http://127.0.0.1:$port/posts")" = '{"n": 1}' ] || fail "a second open made a duplicate"
c open --customer acme-2 | grep -q '"id":"p-2"' || fail "a second customer did not get its own container"
c close --customer acme | grep -q '"state":"closed"' || fail "close did not remove the container"
if c show --customer acme >/dev/null; then fail "a closed container still showed"; fi
c show --customer acme-2 | grep -q '"state":"open"' || fail "closing one customer touched another"
if BONFYRE_PROVIDER_CUSTOMER_MAX=1 c open --customer acme-3 >/dev/null 2>&1; then fail "a new customer was opened past the ceiling"; fi
BONFYRE_PROVIDER_CUSTOMER_MAX=1 c open --customer acme-2 | grep -q '"state":"open"' || fail "an existing customer was refused by the ceiling"
if c open --customer 'Acme;rm' >/dev/null 2>&1; then fail "a malformed customer name was used"; fi
if "$bin" customer open --provider neon --customer acme --env "$t/env" --api "http://example.com" >/dev/null 2>&1; then fail "a non-local test API was used"; fi
printf 'export NEON_API_KEY="wrong"\n' > "$t/bad"
if "$bin" customer open --provider neon --customer acme --env "$t/bad" --api "http://127.0.0.1:$port" >/dev/null 2>&1; then fail "a refused key was reported as working"; fi
echo "customer: PASS"
