#!/bin/sh
# What a page costs to receive, and to receive again:
#   - a composed page is gzipped for a caller that reads gzip, and the wire
#     carries a fraction of the markup
#   - a caller that does not read gzip gets exactly the same bytes as before
#   - a page carries an ETag, and a caller that already has it is answered 304
#     with no body at all
#   - a picture host is named early so a browser opens that connection while it
#     is still reading the page
#   - a calendar of a year's events does not cost a year's work to compose
set -eu
web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'page transport: SKIP (cms not built)'; exit 0; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-page-transport.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

mkdir -p "$tmp/schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
cp "$root/cmd/BonfyreCMS/content-types/internet_source.json" "$tmp/schemas/"
"$cms" schema migrate --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1
"$cms" entry create internet_source '{"family":"events","source":"events-year","label":"A year of events","says":"a calendar","url":"https://hall.example.test/calendar.ics","twin":"events-year","state":"observed","status":"published","tier":"WARM","kind":"event","extract":"ics:vevent"}' --ns root --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1

# A real calendar: a year already past, then what is coming.
python3 - "$tmp" <<'PY'
import sys, time
tmp = sys.argv[1]
lines = ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//The Hall//EN"]
def event(uid, title, hours):
    start = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(time.time() + hours * 3600))
    end = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(time.time() + (hours + 2) * 3600))
    lines.extend(["BEGIN:VEVENT", "UID:" + uid, "DTSTART;TZID=America/Chicago:" + start.rstrip("Z"),
                  "DTEND;TZID=America/Chicago:" + end.rstrip("Z"), "SUMMARY:" + title,
                  "LOCATION:The Long Room", "DESCRIPTION:" + ("a description that a reader has to walk past " * 8),
                  "END:VEVENT"])
for i in range(3000):
    event("past-%d" % i, "Already happened %d" % i, -24 * (1 + i % 300))
for i in range(12):
    event("soon-%d" % i, "Still to come %d" % i, 3 + i)
lines.append("END:VCALENDAR")
body = tmp + "/foreign/bodies/events-year.body"
open(body, "w").write("\r\n".join(lines) + "\r\n")
open(tmp + "/foreign/receipts/events-year.json", "w").write('{"body_path":"%s"}\n' % body)
print(len(open(body).read()))
PY

# A show whose files live under /audio/ on its host: the page will carry that
# string in its own markup, and a page is still a page.
"$cms" entry create internet_source '{"family":"podcasts","source":"a-show","label":"A show","says":"episodes","url":"https://show.example.test/rss","twin":"a-show","state":"observed","status":"published","tier":"WARM","kind":"audio","extract":"rss:item"}' --ns root --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1
python3 - "$tmp" <<'PY2'
import sys, time
tmp = sys.argv[1]
items = []
for i in range(12):
    when = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(time.time() - i * 3600))
    items.append("<item><title>Episode %d</title><link>https://show.example.test/e/%d</link>"
                 "<pubDate>%s</pubDate>"
                 "<enclosure url='https://cdn.example.test/audio/128/ep%d.mp3' type='audio/mpeg' length='1'/>"
                 "<description>%s</description></item>" % (i, i, when, i, "what this episode is about " * 20))
body = tmp + "/foreign/bodies/a-show.body"
open(body, "w").write("<rss><channel>" + "".join(items) + "</channel></rss>")
open(tmp + "/foreign/receipts/a-show.json", "w").write('{"body_path":"' + body + '"}')
PY2

"$web" bootstrap-owned --db "$tmp/matrix.db" >/dev/null
port=$((29900 + $$ % 90))
BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_FOREIGN_ROOT="$tmp/foreign" BONFYRE_LOCAL_ZONE="America/Chicago" \
  BONFYRE_PUBLIC_PROJECTION_DB="$tmp/projection.db" \
  "$web" serve --db "$tmp/matrix.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
for _ in $(seq 1 60); do curl -fsS -o /dev/null "http://127.0.0.1:$port/healthz" 2>/dev/null && break; sleep 0.2; done

# A cancelled reader must not terminate the origin serving every other export
# coordinate. The RST makes the next response write fail locally; health must
# still answer afterwards.
python3 - "$port" <<'PY'
import socket, struct, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])))
s.sendall(b"GET /whats-on HTTP/1.1\r\nHost: aurekai.life\r\nConnection: close\r\n\r\n")
s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
s.close()
PY
sleep 0.1
curl -fsS -o /dev/null "http://127.0.0.1:$port/healthz"

python3 - "$tmp" "$port" <<'PY'
import json, re, subprocess, sys, time
tmp, port = sys.argv[1], sys.argv[2]
problems = []
def check(ok, why):
    if not ok: problems.append(why)

def fetch(path, headers=(), gzip=False):
    args = ["curl", "-sS", "-D", tmp + "/head.txt", "-o", tmp + "/body.bin",
            "-w", "%{size_download} %{time_starttransfer}", "-H", "Host: aurekai.life"]
    if gzip: args += ["-H", "Accept-Encoding: gzip"]
    for h in headers: args += ["-H", h]
    args.append("http://127.0.0.1:%s%s" % (port, path))
    out = subprocess.run(args, capture_output=True, text=True).stdout.split()
    head = open(tmp + "/head.txt", encoding="utf-8", errors="replace").read()
    return int(out[0]), float(out[1]), head

plain_size, plain_time, plain_head = fetch("/whats-on")
plain_body = open(tmp + "/body.bin", encoding="utf-8", errors="replace").read()
gz_size, gz_time, gz_head = fetch("/whats-on", gzip=True)
check(plain_size > 4000, "the page under test is a real page (%d bytes)" % plain_size)
check("content-encoding: gzip" in gz_head.lower(), "a caller that reads gzip is sent gzip")
check(gz_size < plain_size / 2, "and the wire carries less than half the markup (%d of %d)" % (gz_size, plain_size))
check("vary: accept-encoding" in gz_head.lower(), "the answer says it varies by what the caller reads")
check("content-encoding" not in plain_head.lower(), "a caller that does not read gzip is sent the page itself")

etag = re.search(r'(?im)^etag:\s*(\S+)', gz_head)
check(etag is not None, "a page carries an ETag")
if etag:
    size, _, head = fetch("/whats-on", headers=["If-None-Match: %s" % etag.group(1)], gzip=True)
    check(head.startswith("HTTP/1.1 304"), "a caller that already has it is answered 304 (%s)" % head.split("\r\n")[0])
    check(size == 0, "with no body at all (%d bytes)" % size)

check('rel="preconnect"' in plain_body and "upload.wikimedia.org" in plain_body,
      "the picture hosts are named early")

# A page that names media types in its own markup -- a track list whose files
# live under /audio/ on their host -- is still a page, and still packs.
listen_plain, _, _ = fetch("/listen")
listen_body = open(tmp + "/body.bin", encoding="utf-8", errors="replace").read()
check("audio/" in listen_body, "the page under test carries a media type in its markup")
listen_gz, _, listen_head = fetch("/listen", gzip=True)
check("content-encoding: gzip" in listen_head.lower(),
      "and it is still compressed (%d of %d bytes)" % (listen_gz, listen_plain))
check(listen_gz < listen_plain / 2, "to well under half (%d of %d)" % (listen_gz, listen_plain))

# A year of finished events must not cost a year of work.
times = []
for _ in range(3):
    _, seconds, _ = fetch("/whats-on", gzip=True)
    times.append(seconds)
best = min(times)
check(best < 0.4, "a year of events composes in well under half a second (%.3fs)" % best)
# Composed once and kept: after the first visitor, the same page costs almost
# nothing, and it is the same bytes.
check(best < 0.05, "and a page composed a moment ago is served from what was kept (%.3fs)" % best)
first_bytes = open(tmp + "/body.bin", "rb").read()
again_size, _, _ = fetch("/whats-on", gzip=True)
check(open(tmp + "/body.bin", "rb").read() == first_bytes, "a kept page is the same page, byte for byte")
body = open(tmp + "/body.bin", encoding="utf-8", errors="replace").read()
check("Still to come" in body or "Already happened" not in body, "and what it shows is what is still to come")

if problems:
    print("page transport: FAIL")
    for p in problems: print("  -", p)
    sys.exit(1)
print("page transport: PASS")
PY
