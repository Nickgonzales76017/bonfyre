#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-circulation-audit.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
mkdir -p "$tmp/estate" "$tmp/dist/discover" "$tmp/dist/watch" "$tmp/dist/channels/__q" "$tmp/generated"
bun -e 'import {Database} from "bun:sqlite"; const db=new Database(process.argv[1]); db.exec("CREATE TABLE channel(census_source TEXT,state TEXT)"); const q=db.query("INSERT INTO channel VALUES(?,?)"); for(const row of [["one","admitted"],["two","active"],["three","admitted"],["old","refused"]]) q.run(...row); db.close()' "$tmp/estate/frontier.db"
cat >"$tmp/generated/native_page_composition.h" <<'HEADER'
static const BfPageProgram bf_page_programs[] = {
  {"/discover","any"},
  {"/watch","any"},
  {"/channels","any"},
};
HEADER
printf '<a href="/o/one/a">one</a><a href="/o/old/a">old</a>' >"$tmp/dist/discover.html"
printf '<a href="/o/two/b">two</a>' >"$tmp/dist/discover/2.html"
printf '<a href="/o/three/c">three</a>' >"$tmp/dist/watch.html"
printf '<a href="/o/three/c">three</a>' >"$tmp/dist/channels/__q/channel=three.html"
out=$(bun "$root/scripts/audit_public_circulation.mjs" --estate "$tmp/estate" --dist "$tmp/dist" --program-header "$tmp/generated/native_page_composition.h" --json)
bun -e 'const x=JSON.parse(process.argv[1]); for(const [actual,expected] of [[x.admitted_sources,3],[x.publicly_circulating_admitted_sources,3],[x.linked_but_not_currently_admitted,1],[x.routes["/discover"].pages,2],[x.routes["/discover"].admitted_sources,2],[x.routes["/channels"].pages,1],[x.routes["/channels"].admitted_sources,1]]) if(actual!==expected) throw Error(JSON.stringify(x))' "$out"
echo 'public circulation audit: PASS'
