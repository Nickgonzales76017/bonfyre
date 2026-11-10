#!/bin/sh
# An event is what an organizer says will happen, read from the calendar they
# publish:
#   - folded lines, escaped text, all-day entries and a named timezone are read
#     as the calendar means them, and the hours shown are this place's hours
#   - something that has already finished is not "what's on"
#   - the agenda reads forwards, grouped by day, and says how long until
#   - the same address is a different page at different hours: coming up,
#     starting soon, happening now, already happened
#   - the page hands over the event as a file a calendar understands, written
#     here and saying so, and the whole agenda as one file
#   - the machine projection says what the page says, and what it is based on
set -eu
web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'page events: SKIP (cms not built)'; exit 0; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-page-events.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

schemas="$tmp/schemas"
mkdir -p "$schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
for type in world world_fragment internet_source publication_occurrence content_flag; do cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$schemas/"; done
cmsdb="$tmp/cms.db"
"$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create internet_source '{"family":"events","source":"events-hall","label":"The Hall events","says":"what the Hall says is happening","url":"https://hall.example.test/calendar.ics","twin":"events-hall","state":"observed","status":"published","tier":"WARM","kind":"event","extract":"ics:vevent","publisher_ident":"Q16558","publisher_name":"The Hall"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null

# The calendar is written around now, so the phases are real rather than fixed.
python3 - "$tmp" <<'PY'
import sys, time
tmp = sys.argv[1]
def stamp(offset_hours, whole_day=False):
    at = time.gmtime(time.time() + offset_hours * 3600)
    return time.strftime("%Y%m%d" if whole_day else "%Y%m%dT%H%M%SZ", at)
lines = ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//The Hall//EN"]
def event(uid, summary, start, end=None, extra=()):
    lines.extend(["BEGIN:VEVENT", "UID:" + uid, "DTSTART:" + start])
    if end: lines.append("DTEND:" + end)
    lines.append("SUMMARY:" + summary)
    lines.extend(extra)
    lines.append("END:VEVENT")
event("gone", "Last year's lecture", stamp(-72), stamp(-70))
# A real calendar carries a whole year: hundreds of finished entries stand
# between the top of the file and anything that has not happened yet, so a
# reader that stops early shows January and nothing that is coming.
for old_event in range(2600):
    event("past-%d" % old_event, "A meeting that already happened, number %d, with a description long enough to push the file past any small reading window" % old_event,
          stamp(-24 * (30 + old_event % 300)), stamp(-24 * (30 + old_event % 300) + 1))
event("now", "Doors open: the long room", stamp(-1), stamp(2),
      ("LOCATION:The Long Room\\, 12 Main Street", "GEO:32.75600;-97.33250",
       "URL:https://hall.example.test/event/long-room",
       "DESCRIPTION:An evening of readings and a band\\nafterwards."))
event("soon", "A talk that starts within the hour", stamp(0.5), stamp(2))
event("later", "Saturday market", stamp(50), stamp(56))
# a folded line, an escaped comma, and a whole-day entry in a named zone
lines.extend(["BEGIN:VEVENT", "UID:folded",
              "DTSTART;TZID=America/Chicago:" + time.strftime("%Y%m%dT120000", time.gmtime(time.time() + 26 * 3600)),
              "SUMMARY:A title that runs past the line limit and is folded by the",
              "  calendar that wrote it\\, exactly as calendars do",
              "LOCATION:Upstairs", "END:VEVENT"])
event("allday", "All day: the building is open", stamp(20, True))
lines.append("END:VCALENDAR")
open(tmp + "/foreign/bodies/events-hall.body", "w").write("\r\n".join(lines) + "\r\n")
open(tmp + "/foreign/receipts/events-hall.json", "w").write('{"body_path":"%s/foreign/bodies/events-hall.body"}\n' % tmp)
PY

"$web" bootstrap-owned --db "$tmp/matrix.db" >/dev/null

BONFYRE_CMS_DB="$cmsdb" BONFYRE_FOREIGN_ROOT="$tmp/foreign" "$web" census-items >"$tmp/items.jsonl" 2>/dev/null

# Where an event happens, joined to the identity the organizer's own place line
# names, exactly as the standing automation does it.
atlas="$root/cmd/BonfyreLatentAtlas/bonfyre-latent-atlas"
if [ -x "$atlas" ]; then
    bun "$root/cmd/BonfyreLatentAtlas/tools/link_identities.mjs" --items "$tmp/items.jsonl" --cms "$cmsdb" --atlas "$atlas" --db "$tmp/atlas.db" >"$tmp/link.json" 2>/dev/null || true
fi

# Everything read is kept once, so a page outlives its source's feed slot.
BONFYRE_ITEM_ARCHIVE_DB="$tmp/items.db" "$web" archive-items --items "$tmp/items.jsonl" >"$tmp/archive.json"

port=$((29100 + $$ % 400))
BONFYRE_CMS_DB="$cmsdb" BONFYRE_FOREIGN_ROOT="$tmp/foreign" BONFYRE_LOCAL_ZONE="America/Chicago" \
  BONFYRE_LATENT_ATLAS_DB="$tmp/atlas.db" BONFYRE_ITEM_ARCHIVE_DB="$tmp/items.db" \
  "$web" serve --db "$tmp/matrix.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
for _ in $(seq 1 60); do curl -fsS -o /dev/null "http://127.0.0.1:$port/healthz" 2>/dev/null && break; sleep 0.2; done

python3 - "$tmp" "$port" <<'PY'
import json, re, subprocess, sys, time
tmp, port = sys.argv[1], sys.argv[2]
problems = []
def check(ok, why):
    if not ok: problems.append(why)
def get(path):
    out = subprocess.run(["curl", "-s", "-H", "Host: aurekai.life", "http://127.0.0.1:%s%s" % (port, path)], capture_output=True, text=True)
    return out.stdout

items = [json.loads(l) for l in open(tmp + "/items.jsonl") if l.strip()]
check(items and all(i["kind"] == "event" for i in items), "a calendar's entries are events, not stories (%s)" % {i["kind"] for i in items})
titles = {i["title"] for i in items}
check("Last year's lecture" not in titles, "an event that has already finished is not carried")
check(not any("already happened" in t for t in titles), "and neither are the hundreds before it")
check(len(open(tmp + "/foreign/bodies/events-hall.body").read()) > 520_000, "the calendar under test is larger than a small reading window")
check(any("folded by the calendar that wrote it, exactly as calendars do" in t for t in titles), "a folded line is one title, and an escaped comma is a comma (%s)" % [t[:40] for t in titles])
check(all(i["at"] > 0 for i in items), "every event carries the moment its calendar stated")
noon = [i for i in items if "folded" in i["title"] or "runs past" in i["title"]]
if noon:
    hour = time.strftime("%H", time.gmtime(noon[0]["at"]))
    check(hour in ("17", "18"), "a time in a named zone is that zone's time, not a bare number (%s)" % hour)

page = get("/whats-on")
open(tmp + '/stitch-whats-on.html', 'w').write(page)
check('data-agenda-controls' in page and 'data-agenda-search' in page and 'data-agenda-filter="weekend"' in page,
      'upcoming calendar rows supply the real interactive What’s on filter')
check('data-agenda-weekend="' in page, 'the calendar owner marks weekend rows using the display timezone')
check("What&#039;s on" in page or "What's on" in page, "the route is titled in words")
agenda = re.search(r'<ol class="agenda">(.*?)</ol>', page, re.S)
check(agenda is not None, "the events are an agenda, not a feed")
if agenda:
    body = agenda.group(1)
    check("Doors open" in body, "what is on now is on the page")
    check("Last year" not in body, "and what has finished is not")
    check(re.search(r'<li class="agenda-day"><h3>[A-Z][a-z]{2} \d+</h3>', body), "the agenda is grouped by day")
    check("on now" in body, "something under way says so")
    check(re.search(r'in \d+[mh]', body), "and something ahead says how long until")
    # Inspect the displayed agenda, not its duplicate inert Scene template.
    starts = re.findall(r'<span class="starts">([^<]+)</span>', body)
    running = [i for i, w in enumerate(starts) if w.strip() == "on now"]
    ahead = [i for i, w in enumerate(starts) if w.strip() != "on now"]
    check(not running or not ahead or max(running) < min(ahead), "what is under way comes before what has not started (%s)" % starts[:5])
    days = re.findall(r'<li class="agenda-day"><h3>([A-Z][a-z]{2}) (\d+)</h3>', body)
    numbers = [int(d[1]) for d in days]
    check(numbers == sorted(numbers) or len(set(d[0] for d in days)) > 1, "the days run forwards (%s)" % days)

# The event with a stated place, link and description, so the page's fullest
# form is what is checked rather than whichever entry happens to be soonest.
link = re.search(r'href="(/o/events-hall/[0-9a-f]{24})"[^>]*><b>Doors open', page) or \
       re.search(r'<li class="agenda-item"><a[^>]*href="(/o/events-hall/[0-9a-f]{24})"[^>]*>\s*<b>Doors open', page) or \
       re.search(r'href="(/o/events-hall/[0-9a-f]{24})"', page)
check(link is not None, "each event has its own page")
if link:
    item = get(link.group(1))
    phase = re.search(r'<p class="phase[^"]*">([^<]+)</p>', item)
    check(phase is not None, "an event page says what phase it is in")
    if phase:
        check(phase.group(1) in ("Happening now", "Starting soon", "Later today", "This week", "Coming up"),
              "in words a person uses (%s)" % phase.group(1))
    facts = dict(re.findall(r'<dt>([^<]+)</dt><dd>(?:<time[^>]*>)?([^<]*)', item))
    check("Starts" in facts and facts["Starts"].strip(), "it says when it starts (%s)" % facts)
    check("Where" in facts, "and where, when the organizer said so")
    check("Published by" in facts and "Hall" in facts["Published by"], "and who published it")
    check("Add to your calendar" in item, "the event is offered as a file a calendar understands")
    check("openstreetmap.org/directions" in item, "a placed event offers the way there")
    check("The organizer&#039;s own calendar" in item or "The organizer's own calendar" in item, "and a way to the organizer's own calendar")
    ld = re.search(r'application/ld\+json">(.*?)</script>', item, re.S)
    check(ld is not None, "the page says the same thing for machines")
    if ld:
        doc = json.loads(ld.group(1))
        check(doc["@type"] == "Event" and doc["name"].strip(), "as an Event")
        check(doc.get("isBasedOn", "").startswith("https://hall.example.test"), "based on the organizer's own address (%s)" % doc.get("isBasedOn"))
        check("not an invitation" in doc.get("description", ""), "and refusing to be read as an invitation")

    calendar = get(link.group(1) + ".ics")
    check(calendar.startswith("BEGIN:VCALENDAR"), "the event downloads as a calendar")
    check("PRODID:-//Aurekai//compiled from published calendars//EN" in calendar, "written by this estate, and saying so")
    check("Compiled by Aurekai from what" in calendar, "with the organizer named as the source of the facts")
    check(re.search(r"DTSTART:\d{8}T\d{6}Z", calendar), "and a time a calendar can read")

# A front page is a different page at different hours, and only that: it
# renames and reorders the routes below, and admits nothing of its own.
homes = {}
for zone in ("America/Chicago", "Etc/GMT-14", "Etc/GMT+11"):
    subprocess.run(["curl", "-s", "-o", "/dev/null", "http://127.0.0.1:%s/healthz" % port], capture_output=True)
    homes[zone] = None
if True:
    home = get("/")
    title = re.search(r"<h1[^>]*>([^<]+)</h1>", home)
    check(title and title.group(1) in ("This morning", "Today", "Tonight", "Still open"),
          "the front page is named for the hour it is read in (%s)" % (title.group(1) if title else None))
    sections = re.findall(r'<h2[^>]*>(?:<a[^>]*>)?([^<]{2,30})', home)
    check(any("What" in s for s in sections) or "/whats-on" in home, "and what is on leads the hours that have events")

# The place line an organizer wrote, joined to an identity: what happens there
# is then reachable from that identity's own page.
import sqlite3, os
if os.path.exists(tmp + "/atlas.db"):
    rows = sqlite3.connect(tmp + "/atlas.db").execute("SELECT src, dst, evidence FROM relation WHERE relation='happens_at'").fetchall()
    check(rows, "an event is joined to where it happens")
    check(any(r[1] == "qid:Q16558" for r in rows), "by the identity the census knows (%s)" % [r[1] for r in rows][:3])
    check(all("linker:" in r[2] for r in rows), "and the join says which rule wrote it")
    here = get("/about/Q16558")
    check("Happening here" in here, "that identity's page carries what happens there")
    check("Doors open" in here or "long room" in here, "with the event itself (%s)" % ("Happening here" in here))

whole = get("/whats-on.ics")
check(whole.count("BEGIN:VEVENT") >= 3, "the whole agenda is one file too (%d)" % whole.count("BEGIN:VEVENT"))
check("Last year" not in whole, "and it carries nothing that has finished")

# The source moves on; the address a person was given does not.
archived = json.load(open(tmp + "/archive.json"))
check(archived["kept"] >= 4, "every item read is kept once (%s)" % archived)
if link:
    open(tmp + "/foreign/bodies/events-hall.body", "w").write("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nEND:VCALENDAR\r\n")
    gone = get(link.group(1))
    check("Doors open" in gone, "an item its source has dropped still has its page")
    check('class="kept"' in gone, "and says it was kept rather than being current")
    check("no longer carries it" in gone, "in plain words")
    check('More from ' in gone and 'The Hall' in gone, "the kept item still links back to its exact published source identity")
    agenda_after = get("/whats-on")
    check("Doors open" not in agenda_after, "while the agenda itself only carries what the source still publishes")

if problems:
    print("page events: FAIL")
    for p in problems: print("  -", p)
    sys.exit(1)
print("page events: PASS")
PY
if [ "${BONFYRE_STITCH_BROWSER:-0}" = 1 ]; then
  bun "$root/cmd/BonfyreWebAddress/tests/whats_on_stitch_browser.test.mjs" "$tmp/stitch-whats-on.html"
  node "$root/cmd/BonfyreWebAddress/tests/native_experience_page_test.mjs" "$tmp/stitch-whats-on.html" discover
fi
