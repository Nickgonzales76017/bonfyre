#!/bin/sh
# Walking a ledger of thousands, four depths deep:
#   - the ledger's own shape, in words, before anything is asked for
#   - the publishers, a bounded page at a time
#   - one publisher's lattice, and one way in to it
#   - one channel: its standing, how it is known, what it also carries
#   - and a cut across every publisher at once: all the channels read one way
# Nothing here is fetched, admitted or ranked, and a refusal keeps its reason.
set -eu
web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'page channels: SKIP (cms not built)'; exit 0; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-page-channels.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

# A ledger with two publishers, three ways in, a refusal and a measured carriage.
python3 - "$tmp" <<'PY'
import sqlite3, sys
db = sqlite3.connect(sys.argv[1] + "/frontier.db")
db.executescript("""
CREATE TABLE root(root_id TEXT PRIMARY KEY, host TEXT NOT NULL, name TEXT DEFAULT '', identity TEXT DEFAULT '', first_seen TEXT);
CREATE TABLE family(family_id TEXT PRIMARY KEY, root_id TEXT NOT NULL, protocol TEXT NOT NULL, endpoint TEXT DEFAULT '', discovered_from TEXT DEFAULT '', first_seen TEXT);
CREATE TABLE channel(channel_id TEXT PRIMARY KEY, family_id TEXT NOT NULL, root_id TEXT NOT NULL, address TEXT NOT NULL,
 exact_address TEXT NOT NULL, title TEXT DEFAULT '', material TEXT DEFAULT '', protocol TEXT DEFAULT 'unknown',
 state TEXT NOT NULL DEFAULT 'candidate', truth_level TEXT DEFAULT 'listed', refusal TEXT DEFAULT '', virtual INTEGER DEFAULT 0,
 census_source TEXT DEFAULT '', credential TEXT DEFAULT '', rights TEXT DEFAULT '', cadence TEXT DEFAULT '',
 discovered_from TEXT DEFAULT '', body_sha256 TEXT DEFAULT '', identity TEXT DEFAULT '', first_seen TEXT, last_seen TEXT,
 last_contributed TEXT DEFAULT '', contributions INTEGER DEFAULT 0, carries TEXT DEFAULT '');
""")
db.execute("INSERT INTO root VALUES('r1','hall.example.test','The Long Room','long-room','2026-09-01')")
db.execute("INSERT INTO root VALUES('r2','paper.example.test','The County Paper','','2026-09-02')")
db.execute("INSERT INTO family VALUES('f1','r1','rss','','listing','2026-09-01')")
db.execute("INSERT INTO family VALUES('f2','r2','rss','','listing','2026-09-02')")
db.execute("INSERT INTO family VALUES('f3','r2','ics','','listing','2026-09-02')")
rows = [
 ('c1','f1','r1','https://hall.example.test/feed','https://hall.example.test/feed','The Long Room weekly','news','rss',
  'admitted','observed_candidate','','','hall-weekly','','','1h','listing','','','2026-09-01','2026-09-17','2026-09-17',12,'podcasts'),
 ('c2','f2','r2','https://paper.example.test/rss','https://paper.example.test/rss','County Paper headlines','news','rss',
  'admitted','observed_candidate','','','county-headlines','','','1h','listing','','','2026-09-02','2026-09-17','2026-09-16',7,''),
 ('c3','f2','r2','https://paper.example.test/comments','https://paper.example.test/comments','County Paper comments','news','rss',
  'refused','observed_candidate','a comment feed is readers talking, not the publisher publishing','','','','','','listing','','','2026-09-02','2026-09-02','',0,''),
 ('c4','f3','r2','https://paper.example.test/events.ics','https://paper.example.test/events.ics','County diary','events','ics',
  'candidate','protocol_derived_candidate','','','','','','','convention','','','2026-09-02','','',0,''),
]
# An exact external coordinate is allowed to be much larger than an ordinary
# route slug.  This catches a transport/parser truncation that used to make a
# retained source unreachable during static materialization.
long_address = 'https://qlever.example.test/api?query=' + ('SELECT%20%3Fx%20WHERE%20%7B%20%3Fx%20%3Fp%20%3Fo%20.%20%7D%20' * 100)
rows.append(('c5','f1','r1',long_address,long_address,'Long exact query source','data','rss',
  'admitted','observed_candidate','','','long-exact-query','','','1h','listing','','','2026-09-03','2026-09-17','2026-09-17',1,''))
db.executemany("INSERT INTO channel VALUES(" + ",".join("?" * 24) + ")", rows)
db.commit()
PY

mkdir -p "$tmp/schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
for type in world world_fragment internet_source publication_occurrence content_flag; do
    cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$tmp/schemas/"
done
"$cms" schema migrate --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1
"$cms" entry create internet_source '{"family":"forums","source":"hall-weekly","label":"The Long Room weekly","says":"what the Long Room publishes","url":"https://hall.example.test/feed","twin":"hall-weekly","state":"observed","status":"published","tier":"WARM","kind":"text","extract":"rss:item"}' --ns root --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1
printf '%s' '<rss><channel><item><title>A note from the Long Room</title><link>https://hall.example.test/a</link><pubDate>Wed, 17 Sep 2026 10:00:00 GMT</pubDate></item></channel></rss>' > "$tmp/foreign/bodies/hall-weekly.body"
printf '{"body_path":"%s"}\n' "$tmp/foreign/bodies/hall-weekly.body" > "$tmp/foreign/receipts/hall-weekly.json"

"$web" bootstrap-owned --db "$tmp/matrix.db" >/dev/null
port=$((29700 + $$ % 90))
BONFYRE_SOURCE_FRONTIER_DB="$tmp/frontier.db" BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_FOREIGN_ROOT="$tmp/foreign" \
  "$web" serve --db "$tmp/matrix.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
for _ in $(seq 1 60); do curl -fsS -o /dev/null "http://127.0.0.1:$port/healthz" 2>/dev/null && break; sleep 0.2; done

python3 - "$tmp" "$port" <<'PY'
import re, subprocess, sys, urllib.parse
tmp, port = sys.argv[1], sys.argv[2]
problems = []
def check(ok, why):
    if not ok: problems.append(why)

def get(path):
    out = subprocess.run(["curl", "-sS", "-H", "Host: aurekai.life",
                          "http://127.0.0.1:%s%s" % (port, path)], capture_output=True, text=True).stdout
    return out[out.find("region-depth"):] if "region-depth" in out else out

def words(html):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))

# 1. The ledger's shape, unasked.
top = words(get("/channels"))
check("2 publishers, 5 channels" in top, "the ledger says how much it holds: %s" % top[:120])
check("3 are read here" in top and "2 are listed and unread" in top, "and how much of it is read")
check("a comment feed is readers talking" in top, "a refusal keeps its reason where everyone can read it")
check("The Long Room" in top and "The County Paper" in top, "the publishers themselves are on the first page")

# 2. A cut across every publisher: all the channels read one way.
rss = words(get("/channels?way=rss"))
check("4 here, across every publisher" in rss, "one way in, across publishers: %s" % rss[:120])
check("County Paper headlines" in rss and "The Long Room weekly" in rss, "and it carries channels from both")
ics = words(get("/channels?way=ics"))
check("1 here, across every publisher" in ics, "and a way with one channel says one")

# 3. One publisher's lattice, and one way into it.
one = get("/channels?root=paper.example.test")
check("The County Paper" in words(one), "a publisher's own page")
check("3 channels at this publisher" in words(one), "with everything listed under it (%s)" % words(one)[-160:])
check("channel=" in one, "and every channel is itself addressable")
narrow = words(get("/channels?root=paper.example.test&family=ics"))
check("1 channel at this publisher" in narrow, "one way in narrows to what came that way")

# 4. One channel: standing, provenance, measured carriage.
addr = urllib.parse.quote("https://hall.example.test/feed", safe="")
chan = words(get("/channels?channel=%s" % addr))
check("The Long Room weekly" in chan, "a channel has its own page")
check("read here" in chan, "with what this estate has done with it")
check("observed_candidate" in chan, "how the address came to be known")
check("Also carries" in chan and "podcasts" in chan, "and what a reading of its body actually found (%s)" % chan[:160])
check("measured from what it sent" in chan, "said as measurement, not as a claim about the publisher")
check("never a judgement of what it publishes" in chan, "a standing is not a verdict on a publisher")

long_address = 'https://qlever.example.test/api?query=' + ('SELECT%20%3Fx%20WHERE%20%7B%20%3Fx%20%3Fp%20%3Fo%20.%20%7D%20' * 100)
long = words(get("/channels?channel=%s" % urllib.parse.quote(long_address, safe="")))
check("Long exact query source" in long, "a multi-kilobyte exact coordinate remains addressable, not truncated")

refused = words(get("/channels?channel=%s" % urllib.parse.quote("https://paper.example.test/comments", safe="")))
check("a comment feed is readers talking" in refused, "a refused channel states its refusal on its own page")

if problems:
    print("page channels: FAIL")
    for p in problems: print("  -", p)
    sys.exit(1)
print("page channels: PASS")
PY
