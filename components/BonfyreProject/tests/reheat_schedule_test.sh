#!/bin/sh
# The scheduler is a budget, not a sweep:
#   - a census larger than any fixed array is read whole, not truncated
#   - a source nobody has ever read is the most overdue thing there is
#   - the most overdue go first, and what is due but unaffordable this run
#     says so rather than being called "not due"
#   - one publisher cannot take a whole run
#   - a frozen source is still never asked
set -eu
project=${1:?project binary required}
case "$project" in /*) ;; *) project="$(pwd)/$project" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'reheat schedule: SKIP (cms not built)'; exit 0; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-reheat-schedule.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
mkdir -p "$tmp/schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
cp "$root/cmd/BonfyreCMS/content-types/internet_source.json" "$tmp/schemas/"
"$cms" schema migrate --db "$tmp/cms.db" --schemas "$tmp/schemas" >/dev/null 2>&1

# A census of 600 rows, which is more than any fixed array this organ used to
# carry, across three publishers, plus one frozen row.
python3 - "$tmp" "$cms" <<'PY'
import json, subprocess, sys, time
tmp, cms = sys.argv[1], sys.argv[2]
rows = []
for i in range(600):
    host = ["alpha.example.test", "beta.example.test", "gamma.example.test"][i % 3]
    rows.append({"family": "news", "source": "s%03d" % i, "label": "source %d" % i, "says": "a feed",
                 "url": "https://%s/feed/%d" % (host, i), "twin": "s%03d" % i, "state": "observed",
                 "status": "published", "tier": "WARM", "kind": "text", "cadence_seconds": 600})
rows[3]["tier"] = "COLD"        # a cold source that a page nonetheless showed
rows.append({"family": "news", "source": "frozen-one", "label": "refused", "says": "a feed that said no",
             "url": "https://delta.example.test/feed", "twin": "frozen-one", "state": "observed",
             "status": "published", "tier": "FROZEN", "kind": "text", "cadence_seconds": 600})
for r in rows:
    subprocess.run([cms, "entry", "create", "internet_source", json.dumps(r), "--ns", "root",
                    "--db", tmp + "/cms.db", "--schemas", tmp + "/schemas"], capture_output=True)
# Most were read a while ago; twenty were read long ago; ten never at all.
now = time.time()
for i in range(590):
    age = 7200 if i < 570 else 864000
    at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - age))
    open("%s/foreign/receipts/s%03d.json" % (tmp, i), "w").write(
        json.dumps({"observed_at": at, "http_status": 200, "policy_decision": "allow",
                    "body_path": "%s/foreign/bodies/s%03d.body" % (tmp, i)}) + "\n")
    open("%s/foreign/bodies/s%03d.body" % (tmp, i), "w").write("<rss/>")
PY

# What the public surfaces showed: one cold source a page carried yesterday.
frontier="$root/cmd/BonfyreSourceFrontier/bonfyre-source-frontier"
if [ -x "$frontier" ]; then
    "$frontier" init --db "$tmp/ledger.db" >/dev/null
    printf '{"address":"https://alpha.example.test/feed/3","protocol":"rss","state":"admitted","census_source":"s003"}\n' >"$tmp/chan.jsonl"
    "$frontier" import --db "$tmp/ledger.db" --label TEST --jsonl "$tmp/chan.jsonl" >/dev/null
    printf '{"route":"/discover","source":"s003","items":4,"day":"%s"}\n' "$(date -u +%Y-%m-%d)" >"$tmp/receipts.jsonl"
    "$frontier" contributions --db "$tmp/ledger.db" --jsonl "$tmp/receipts.jsonl" >/dev/null
fi

BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_FOREIGN_ROOT="$tmp/foreign" BONFYRE_SOURCE_FRONTIER_DB="$tmp/ledger.db" \
  "$project" foreign reheat --dry-run --budget 25 --per-host 3 >"$tmp/dry.json" 2>/dev/null
BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_FOREIGN_ROOT="$tmp/foreign" \
  "$project" foreign reheat --budget 25 --per-host 3 >"$tmp/run.json" 2>/dev/null

python3 - "$tmp" <<'PY'
import json, os, sys
tmp = sys.argv[1]
problems = []
def check(ok, why):
    if not ok: problems.append(why)
run = json.loads([l for l in open(tmp + "/run.json") if l.startswith("{")][-1])
dry = json.loads([l for l in open(tmp + "/dry.json") if l.startswith("{")][-1])
chosen = {s["source"] for s in dry["sources"] if s["decision"] == "due"}
check(len(chosen) <= 25, "a dry run shows this run's budget, not every due row (%d)" % len(chosen))
check(dry["waiting_its_turn"] > 400, "and says how many are waiting (%s)" % dry["waiting_its_turn"])
by_host = {}
for s in dry["sources"]:
    if s["source"] in chosen:
        host = int(s["source"][1:]) % 3 if s["source"].startswith("s") else -1
        by_host[host] = by_host.get(host, 0) + 1
# Three publishers and a budget of 25: a fair share is nine each, so no
# publisher may take more than that, and none may be starved either.
check(all(v <= 9 for v in by_host.values()), "no publisher takes more than its share of one run (%s)" % by_host)
check(len(by_host) == 3, "and every publisher with something due gets a turn (%s)" % by_host)
check(run["census"] == 601, "the whole census is considered, not the first few hundred (%s)" % run["census"])
check(run["never_read"] == 11, "a source nobody has read is counted as never read (%s)" % run["never_read"])
check(run["read"] <= 25, "a run spends no more than its budget (%s)" % run["read"])
check(run["read"] >= 20, "and spends it (%s)" % run["read"])
check(run["waiting_its_turn"] > 400, "what is due but unaffordable says so (%s)" % run["waiting_its_turn"])
sources = {s["source"]: s for s in run["sources"]}
read = [s for s in run["sources"] if s["decision"].startswith("read") or s["decision"] == "due"]
waiting = [s for s in run["sources"] if "waiting its turn" in s["decision"]]
check(waiting, "and the weather names them")
check(all(s["decision"] != "not due" for s in read), "nothing read is also called not due")
# the ten never-read rows are the most overdue, so they go first
never = {"s%03d" % i for i in range(590, 600)}
check(len(never & chosen) >= 9, "the never-read go first (%d of 10)" % len(never & chosen))
long_ago = {"s%03d" % i for i in range(570, 590)}
check(long_ago & chosen, "and the longest-unread follow")
recent = {"s%03d" % i for i in range(0, 60)}
check(not (recent & chosen) or len(recent & chosen) < 5, "before anything read two hours ago (%d)" % len(recent & chosen))
check(sources["frozen-one"]["decision"].startswith("frozen"), "a frozen source is still never asked")
shown = [s for s in dry["sources"] if s["source"] == "s003"]
if shown and os.path.exists(tmp + "/ledger.db"):
    check(shown[0]["tier"] == "WARM" and shown[0]["demanded"], "a source a page actually showed is warm whatever it was authored as (%s)" % shown[0]["tier"])
hosts = {}
for s in run["sources"]:
    if "read" in s["decision"]:
        hosts[s["source"][:1]] = hosts.get(s["source"][:1], 0) + 1
if problems:
    print("reheat schedule: FAIL")
    for p in problems: print("  -", p)
    sys.exit(1)
print("reheat schedule: PASS")
PY
