#!/bin/sh
# A Stage is the same Event reopened with the material concretely attached to it.
# This fixture pins stable source/key addressing and spatial closure:
#   focal Event -> Stage
#   nearby Event (same two-kilometre scene) -> included
#   far Event from the same organizer -> excluded
set -eu
web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'page stage: SKIP (cms not built)'; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-page-stage.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

schemas="$tmp/schemas"
mkdir -p "$schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
for type in world world_fragment internet_source publication_occurrence content_flag; do
    cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$schemas/"
done
cmsdb="$tmp/cms.db"
"$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create internet_source '{"family":"events","source":"events-hall","label":"The Hall events","says":"what the Hall says is happening","url":"https://hall.example.test/calendar.ics","twin":"events-hall","state":"observed","status":"published","tier":"WARM","kind":"event","extract":"ics:vevent","publisher_ident":"Q16558","publisher_name":"The Hall"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create internet_source '{"family":"forums","source":"talk-board","label":"Talk board","says":"what its users discuss","url":"https://talk.example.test/feed.json","twin":"talk-board","state":"observed","status":"published","tier":"WARM","kind":"feed","extract":"items"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null

python3 - "$tmp" <<'PY'
import sys,time
tmp=sys.argv[1]
def stamp(hours):
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(time.time()+hours*3600))
lines=["BEGIN:VCALENDAR","VERSION:2.0","PRODID:-//The Hall//EN"]
def event(uid,title,start,end,geo):
    lines.extend([
        "BEGIN:VEVENT","UID:"+uid,"DTSTART:"+start,"DTEND:"+end,
        "SUMMARY:"+title,"LOCATION:The Long Room","GEO:"+geo,
        "URL:https://hall.example.test/event/"+uid,"END:VEVENT"
    ])
event("focus","Stage focus: doors are open",stamp(-1),stamp(2),"32.75600;-97.33250")
event("near","Nearby aftershow",stamp(3),stamp(5),"32.75800;-97.33000")
event("far","Far away lecture",stamp(4),stamp(6),"32.95000;-97.10000")
lines.append("END:VCALENDAR")
body=tmp+"/foreign/bodies/events-hall.body"
open(body,"w").write("\r\n".join(lines)+"\r\n")
open(tmp+"/foreign/receipts/events-hall.json","w").write('{"body_path":"%s"}\n'%body)
open(tmp+"/foreign/bodies/talk-board.body","w").write('{"items":[{"title":"Generic discussion that is not an event","link":"https://talk.example.test/thread/1","published":"2026-09-25T12:00:00Z"}]}')
open(tmp+"/foreign/receipts/talk-board.json","w").write('{"body_path":"%s/foreign/bodies/talk-board.body"}\n'%tmp)
PY

"$web" bootstrap-owned --db "$tmp/matrix.db" >/dev/null
BONFYRE_CMS_DB="$cmsdb" BONFYRE_FOREIGN_ROOT="$tmp/foreign" "$web" census-items >"$tmp/items.jsonl" 2>/dev/null

port=$((29600 + $$ % 300))
BONFYRE_CMS_DB="$cmsdb" BONFYRE_FOREIGN_ROOT="$tmp/foreign" BONFYRE_LOCAL_ZONE="America/Chicago" \
  "$web" serve --db "$tmp/matrix.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
for _ in $(seq 1 60); do
    curl -fsS -o /dev/null "http://127.0.0.1:$port/healthz" 2>/dev/null && break
    sleep 0.2
done

python3 - "$port" <<'PY'
import re,subprocess,sys
port=sys.argv[1]
problems=[]
def check(ok,why):
    if not ok: problems.append(why)
def get(path):
    return subprocess.run(
        ["curl","-s","-H","Host: aurekai.life","http://127.0.0.1:%s%s"%(port,path)],
        capture_output=True,text=True
    ).stdout

agenda=get("/whats-on")
focus=re.search(r'href="(/o/events-hall/[0-9a-f]{24})"[^>]*>\s*<b>Stage focus',agenda)
if not focus:
    focus=re.search(r'href="(/o/events-hall/[0-9a-f]{24})"[^>]*>[^<]*Stage focus',agenda)
check(focus is not None,"the focal Event has its own page")

if focus:
    item=get(focus.group(1))
    stage_link=re.search(r'href="(/stage[?]q=[^"]+)"[^>]*>.*?Open the live stage',item,re.S)
    check(stage_link is not None,"the Event exposes its Stage continuation")
    if stage_link:
        href=stage_link.group(1).replace("&amp;","&")
        check("q=item:events-hall/" in href,"Stage uses stable source/key addressing (%s)"%href)
        check("Stage%20focus" not in href and "doors%20are%20open" not in href,
              "the Stage address is not title text (%s)"%href)
        stage=get(href)
        check("<h1>Stage</h1>" in stage or ">Stage</h1>" in stage,
              "the Stage page program outranks the donor shell")
        check("Stage focus: doors are open" in stage,"the focal Event remains on its Stage")
        check("Nearby aftershow" in stage,
              "a separately titled Event inside the two-kilometre spatial closure survives")
        check("Far away lecture" not in stage,
              "another Event from the same organizer but outside the spatial closure is excluded")
        check("Generic discussion that is not an event" not in stage,
              "a non-event source cannot bleed into the Stage program")
        check('class="agenda"' in stage,
              "two retained Events compile into an agenda rhythm rather than a flat feed")
        check("What is happening around this event" in stage,
              "Stage carries the convergence program's own human lede")

if problems:
    print("page stage: FAIL")
    for p in problems: print("  -",p)
    sys.exit(1)
print("page stage: PASS")
PY
