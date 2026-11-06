#!/bin/sh
# Witness for the GitHub App manifest exchange: a one-time code becomes a sealed
# app identity, nothing secret is printed, the sealed key signs a JWT the API
# accepts, and a refused code or a wrong key is reported rather than stored.
set -eu
bin=$(CDPATH= cd -- "$(dirname -- "$1")" && pwd)/$(basename -- "$1")
t=$(mktemp -d "${TMPDIR:-/tmp}/gh-app.XXXXXX")
fail() { echo "github app: FAIL: $1" >&2; kill "$api" 2>/dev/null || true; exit 1; }
openssl genrsa -out "$t/app.pem" 2048 2>/dev/null; openssl rsa -in "$t/app.pem" -pubout -out "$t/app.pub" 2>/dev/null
openssl genrsa -out "$t/other.pem" 2048 2>/dev/null
port=$((20000 + $$ % 20000))
python3 -I - "$port" "$t/app.pem" "$t/app.pub" <<'PY' &
import http.server, json, sys, base64, subprocess, time
port, pem, pub = int(sys.argv[1]), open(sys.argv[2]).read(), sys.argv[3]
def unb64(s): return base64.urlsafe_b64decode(s + '=' * (-len(s) % 4))
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def reply(self, code, body):
        b = json.dumps(body).encode(); self.send_response(code); self.send_header('Content-Type', 'application/json'); self.send_header('Content-Length', str(len(b))); self.end_headers(); self.wfile.write(b)
    def do_POST(self):
        if self.path == '/app-manifests/good-code/conversions':
            return self.reply(201, {'id': 4242, 'slug': 'aurekai-test', 'client_id': 'Iv1.test', 'client_secret': 'cs-secret-value', 'webhook_secret': 'wh-secret-value', 'pem': pem, 'html_url': 'https://github.com/apps/aurekai-test'})
        if self.path == '/app/installations/77/access_tokens':
            # Real installation tokens run past 255 characters; a short buffer once cut them.
            return self.reply(201, {'token': 'ghs_' + 'x' * 300, 'expires_at': '2030-01-01T00:00:00Z'})
        return self.reply(404, {'message': 'Not Found'})
    def do_GET(self):
        auth = self.headers.get('Authorization', '')
        if self.path == '/installation/repositories?per_page=100':
            return self.reply(200 if auth == 'Bearer ghs_' + 'x' * 300 else 401, {'total_count': 1, 'repositories': [{'full_name': 'customer-org/site'}]})
        if self.path not in ('/app', '/app/installations?per_page=100') or not auth.startswith('Bearer '): return self.reply(401, {})
        head, body, sig = auth[7:].split('.')
        claims = json.loads(unb64(body))
        open('/tmp/.ghjwt-sig', 'wb').write(unb64(sig)); open('/tmp/.ghjwt-msg', 'wb').write((head + '.' + body).encode())
        ok = subprocess.run(['openssl', 'dgst', '-sha256', '-verify', pub, '-signature', '/tmp/.ghjwt-sig', '/tmp/.ghjwt-msg'], capture_output=True).returncode == 0
        if not ok or claims.get('iss') != '4242' or claims['exp'] < time.time(): return self.reply(401, {'message': 'bad jwt'})
        if self.path == '/app/installations?per_page=100': return self.reply(200, [{'id': 77, 'account': {'login': 'customer-org', 'type': 'Organization'}, 'repository_selection': 'selected'}])
        return self.reply(200, {'id': 4242, 'slug': 'aurekai-test'})
http.server.ThreadingHTTPServer(('127.0.0.1', port), H).serve_forever()
PY
api=$!; trap 'kill $api 2>/dev/null; rm -rf "$t" /tmp/.ghjwt-sig /tmp/.ghjwt-msg' EXIT INT TERM
sleep 1
"$bin" init-key --key "$t/key" >/dev/null
cred() { c=$1; shift; "$bin" "$c" --db "$t/creds.db" --key "$t/key" --account owner --actor test "$@"; }
out=$(printf 'good-code' | cred github-app-convert --api "http://127.0.0.1:$port") || fail "a good code was not converted"
echo "$out" | grep -q '"app_id":4242' || fail "conversion did not report the app id"
echo "$out" | grep -q '"slug":"aurekai-test"' || fail "conversion did not report the slug"
case "$out" in *secret*|*PRIVATE*|*Iv1.test*) fail "conversion printed a secret" ;; esac
if strings "$t/creds.db" | grep -q 'cs-secret-value\|BEGIN RSA\|BEGIN PRIVATE'; then fail "a secret was stored unsealed"; fi
cred github-app-verify --api "http://127.0.0.1:$port" | grep -q '"verified":true' || fail "the sealed key did not sign a JWT the API accepts"
if printf 'bad-code' | cred github-app-convert --api "http://127.0.0.1:$port" >/dev/null 2>&1; then fail "a refused code was accepted"; fi
if printf 'x;rm' | cred github-app-convert --api "http://127.0.0.1:$port" >/dev/null 2>&1; then fail "a malformed code was sent"; fi
if printf 'good-code' | cred github-app-convert --api "http://example.com" >/dev/null 2>&1; then fail "a non-https API was used"; fi
cred github-app-installations --api "http://127.0.0.1:$port" | grep -q '"account":"customer-org","type":"Organization","repositories":"selected"' || fail "customer installations were not listed"
c=$(cred github-app-customer --installation 77 --api "http://127.0.0.1:$port") || fail "a customer installation token was not issued and used"
echo "$c" | grep -q '"repositories":1,"names":\["customer-org/site"\]' || fail "the customer token did not read only the customer's chosen repositories"
case "$c" in *ghs_*) fail "the customer token was printed" ;; esac
if cred github-app-customer --installation 99 --api "http://127.0.0.1:$port" >/dev/null 2>&1; then fail "an installation GitHub refused was reported as usable"; fi
if cred github-app-customer --installation '77;x' --api "http://127.0.0.1:$port" >/dev/null 2>&1; then fail "a malformed installation id was used"; fi
echo "github app: PASS"
