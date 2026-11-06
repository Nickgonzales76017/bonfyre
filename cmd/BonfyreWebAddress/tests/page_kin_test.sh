#!/bin/sh
# What this does not prove:
# - some checks run in embedded Python, which this estate has retired; they say nothing about a native path.
# The same story, told by someone else.
#
# Every census item has a coordinate in the estate's own chart of the words it
# uses.  The kin kernel compares all of them on the vGPU, once, and well above
# the noise what it finds is one story two publishers both carried.  This walks
# the whole path on real organs: two feeds, the census, the item archive, the
# kernel on whichever backend exists here, and the page that reads the result.
set -eu
web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
kernel="$root/cmd/BonfyreSurfaceScene/runtime/kin-compute-vgpu.mjs"
[ -x "$cms" ] || { echo 'page kin: SKIP (cms not built)'; exit 0; }
command -v node >/dev/null 2>&1 || { echo 'page kin: SKIP (node not installed)'; exit 0; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-page-kin.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

mkdir -p "$tmp/schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
for type in world world_fragment internet_source publication_occurrence content_flag; do
    cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$tmp/schemas/"
done
"$cms" schema migrate --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1
for pair in "paper-one:The County Paper" "paper-two:The Second Paper"; do
    source=${pair%%:*}; name=${pair#*:}
    "$cms" entry create internet_source "{\"family\":\"news\",\"source\":\"$source\",\"label\":\"$name\",\"says\":\"what $name publishes\",\"url\":\"https://$source.example.test/rss\",\"twin\":\"$source\",\"state\":\"observed\",\"status\":\"published\",\"tier\":\"WARM\",\"kind\":\"text\",\"extract\":\"rss:item\",\"publisher_name\":\"$name\"}" \
        --ns root --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1
done

# One story both papers carried, and one story each that the other did not.
python3 - "$tmp" <<'PY'
import sys, time
tmp = sys.argv[1]
shared = "Council approves the river bridge after a decade of argument"
feeds = {
    "paper-one": [(shared, "a1"), ("A recipe for late summer tomatoes", "a2")],
    "paper-two": [(shared, "b1"), ("Swimming pool hours change for the winter", "b2")],
}
for source, items in feeds.items():
    rows = []
    for title, slug in items:
        when = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(time.time() - 3600))
        rows.append("<item><title>%s</title><link>https://%s.example.test/%s</link>"
                    "<pubDate>%s</pubDate><description>%s</description></item>"
                    % (title, source, slug, when, title + ". " + ("the same words either way " * 12)))
    body = tmp + "/foreign/bodies/%s.body" % source
    open(body, "w").write("<rss><channel>" + "".join(rows) + "</channel></rss>")
    open(tmp + "/foreign/receipts/%s.json" % source, "w").write('{"body_path":"' + body + '"}')
PY

BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_FOREIGN_ROOT="$tmp/foreign" \
  "$web" census-items > "$tmp/items.jsonl" 2>/dev/null
BONFYRE_ITEM_ARCHIVE_DB="$tmp/items.db" "$web" archive-items --items "$tmp/items.jsonl" >/dev/null

# The chart: the same words give the same coordinate.  The real estate's chart
# is a trigram hash from the latent atlas; what this test pins is the kernel,
# the store and what the page may say, not the encoder.
python3 - "$tmp" <<'PY'
import hashlib, json, math, struct, sqlite3, sys
tmp = sys.argv[1]
dims = 64
rows = sqlite3.connect(tmp + "/items.db").execute(
    "SELECT source, key, title FROM item ORDER BY source, key").fetchall()
keys, vectors = [], []
for source, key, title in rows:
    v = [((hashlib.sha256(("%s|%d" % (title, d)).encode()).digest()[0] / 255.0) - 0.5)
         for d in range(dims)]
    norm = math.sqrt(sum(x * x for x in v)) or 1.0
    keys.append("o/%s/%s" % (source, key))
    vectors.append([x / norm for x in v])
with open(tmp + "/vectors.f32", "wb") as out:
    for v in vectors:
        out.write(struct.pack("<%df" % dims, *v))
json.dump(keys, open(tmp + "/keys.json", "w"))
print(len(keys))
PY

node "$kernel" --vectors "$tmp/vectors.f32" --keys "$tmp/keys.json" --dims 64 \
     --k 4 --min 0.85 --backend auto > "$tmp/kin.json" 2>"$tmp/kin.err" || {
  echo 'page kin: FAIL'; echo '  - the kernel did not run:'; tail -2 "$tmp/kin.err"; exit 1; }

python3 - "$tmp" <<'PY'
import json, sqlite3, sys
tmp = sys.argv[1]
answer = json.load(open(tmp + "/kin.json"))
problems = []
def check(ok, why):
    if not ok: problems.append(why)

receipt = answer["receipt"]
check(receipt["actual_backend"] in ("vgpu-node", "vgpu-mock", "cpu"),
      "the receipt says which backend ran (%s)" % receipt.get("actual_backend"))
check("not a shared subject" in receipt["cannot_infer"],
      "and what a score cannot be read to mean travels with it")

pairs = {}
for row in answer["nearest"]:
    for near in row["near"]:
        pairs[(row["key"], near["key"])] = near["score"]
check(("o/paper-one/a1", "o/paper-two/b1") in pairs or
      any(a.startswith("o/paper-one/") and b.startswith("o/paper-two/") for a, b in pairs),
      "one story carried by two publishers finds its twin")
check(all(a != b for a, b in pairs), "nothing is its own kin")
check(all(score >= 0.85 for score in pairs.values()),
      "and nothing below the floor is kept (%s)" % min(pairs.values(), default=1))

db = sqlite3.connect(tmp + "/items.db")
db.execute("CREATE TABLE IF NOT EXISTS kin(address TEXT, kin_address TEXT, chart TEXT, score REAL,"
           " rank INTEGER, computed TEXT, PRIMARY KEY(address, kin_address))")
for (a, b), score in pairs.items():
    db.execute("INSERT OR REPLACE INTO kin VALUES(?,?,'words-64',?,0,'2026-09-18')", (a, b, score))
db.commit()
# The address of the story both papers carried, as the page will ask for it.
row = db.execute("SELECT source, key FROM item WHERE title LIKE 'Council approves%' AND source='paper-one'").fetchone()
open(tmp + "/address.txt", "w").write("/o/%s/%s" % row if row else "")
db.close()
check(row is not None, "the shared story is in the archive")

if problems:
    print("page kin: FAIL")
    for p in problems: print("  -", p)
    sys.exit(1)
PY

"$web" bootstrap-owned --db "$tmp/matrix.db" >/dev/null
port=$((29600 + $$ % 90))
BONFYRE_ITEM_ARCHIVE_DB="$tmp/items.db" BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_FOREIGN_ROOT="$tmp/foreign" \
  "$web" serve --db "$tmp/matrix.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
for _ in $(seq 1 60); do curl -fsS -o /dev/null "http://127.0.0.1:$port/healthz" 2>/dev/null && break; sleep 0.2; done

python3 - "$tmp" "$port" <<'PY'
import re, subprocess, sys
tmp, port = sys.argv[1], sys.argv[2]
problems = []
def check(ok, why):
    if not ok: problems.append(why)

address = open(tmp + "/address.txt").read().strip()
page = subprocess.run(["curl", "-sS", "-H", "Host: aurekai.life",
                       "http://127.0.0.1:%s%s" % (port, address)], capture_output=True, text=True).stdout
def words_of(html):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))
words = words_of(page)
check("Council approves the river bridge" in words, "the story's own page answers (%s)" % address)
check("Also reported by" in words, "and says another publisher carried the same story")
# Only what the kin section itself says: the rest of the page carries this
# source's own earlier stories, which are not kin and are not claimed to be.
start = page.find("region-kin")
section = words_of(page[start:page.find("</section>", start)]) if start > 0 else ""
check("/o/paper-two/" in page[start:page.find("</section>", start)] if start > 0 else False,
      "and the twin it names is the other publisher's copy")
check("Council approves the river bridge" in section, "shown as the story it is")
check("A recipe for late summer tomatoes" not in section, "an unrelated story is not offered as kin")
check("Swimming pool hours change" not in section, "nor is an unrelated story from the other publisher")
check("not a shared source" in words and "no claim about who published first" in words,
      "and the page states what the resemblance is not")

if problems:
    print("page kin: FAIL")
    for p in problems: print("  -", p)
    sys.exit(1)
print("page kin: PASS")
PY
