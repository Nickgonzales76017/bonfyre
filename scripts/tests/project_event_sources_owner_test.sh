#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
test -x "$cms" || { echo 'project event sources: CMS binary missing' >&2; exit 2; }
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
mkdir -p "$tmp/estate" "$tmp/estate/schemas"
cp "$root/cmd/BonfyreCMS/content-types/internet_source.json" "$tmp/estate/schemas/"
"$cms" schema migrate --db "$tmp/estate/cms.db" --schemas "$tmp/estate/schemas" >/dev/null 2>&1
sqlite3 "$tmp/estate/cms.db" "
INSERT INTO internet_source(namespace,created_at,updated_at,published_at,status,family,source,label,says,url,twin,state,tier,kind,extract)
 VALUES('root','','','','published','news','calendar-a','Calendar A','calendar','https://calendar.example/events.ics','calendar-a','observed','WARM','event','ics:vevent');
INSERT INTO internet_source(namespace,created_at,updated_at,published_at,status,family,source,label,says,url,twin,state,tier,kind,extract)
 VALUES('root','','','','published','news','story-a','Story A','story','https://example.test/feed','story-a','observed','WARM','text','rss:item');
"
bun "$root/scripts/project_event_sources.mjs" --estate "$tmp/estate" --repo "$root" --apply > "$tmp/first.json"
rg -q '"event_sources_refiled": 1' "$tmp/first.json"
bun "$root/scripts/project_event_sources.mjs" --estate "$tmp/estate" --repo "$root" --apply > "$tmp/second.json"
rg -q '"event_sources_refiled": 0' "$tmp/second.json"
test "$(sqlite3 "$tmp/estate/cms.db" "SELECT family FROM internet_source WHERE source='calendar-a'")" = events
test "$(sqlite3 "$tmp/estate/cms.db" "SELECT family FROM internet_source WHERE source='story-a'")" = news
echo 'project event sources owner: PASS'
