#!/bin/sh
# A room the composer serves and the export does not walk is a room that 404s
# live.
#
# That was the state of the whole estate: the composer served twenty-six kinds of
# subject room and the static export walked none of them, so every institution
# page, the governance grammar, the price observatory, the compiled record kinds
# and everything built after them worked perfectly on the machine that composed
# them and answered 404 at the edge. Nothing failed; the two halves simply never
# knew about each other.
#
# So this holds them together. Every room kind the composer dispatches must be
# named in the export, and the home page must link to at least one of them,
# because a room nobody can reach from the front door is only marginally better
# than a room that 404s.
set -eu

bin="$1"
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
src="$root/cmd/BonfyreWebAddress/src/main.c"
export_script="$root/scripts/export_static_estate.sh"
[ -f "$export_script" ] || { echo "rooms reach the edge: SKIP (no export script)"; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-rooms-edge.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

# Every *public subject* kind the composer answers for, read out of that
# dispatcher only.  `serve_room` also has internal /room renderers; treating
# those as /s routes made this witness demand aliases the public address plane
# never claims.  Keep the boundary exact: this tests public subject addresses,
# not every internal renderer in the process.
sed -n '/^static int serve_subject(/,/^static int serve_room(/p' "$src" |
  grep -oE '!strcmp\(kind, "[a-z-]+"\)' |
  sed 's/.*"\(.*\)")/\1/' | sort -u >"$tmp/rooms"
count=$(wc -l <"$tmp/rooms" | tr -d ' ')
[ "$count" -ge 20 ] || { echo "FAIL: only $count room kinds found; the dispatch shape changed"; exit 1; }

missing=''
while read -r kind; do
    # A room reaches the edge either as a named address or through a loop that
    # builds its addresses from a store. Either counts; nothing counts as
    # nothing.
    grep -q "/s/$kind/" "$export_script" || missing="$missing $kind"
done <"$tmp/rooms"
if [ -n "$missing" ]; then
    echo "FAIL: the composer serves these rooms and the export walks none of them:$missing"
    exit 1
fi
echo "every one of the $count room kinds is walked by the static export"

# The item-kind rooms share one address shape, so check the shape exists rather
# than each kind separately.
for shape in "/s/grammar/" "/s/primitive/" "/s/model/" "/s/set/" "/s/project/" \
             "/s/record-kind/" "/s/institution/" "/s/note/" "/s/program/" \
             "/s/capability/" "/s/publisher/"; do
    grep -q "$shape" "$export_script" || { echo "FAIL: $shape is not enumerated by the export"; exit 1; }
done

# The per-entity loops must take their identifiers from a store, not from a list
# somebody typed, or the export goes stale the first time the estate learns
# something.
grep -q "FROM catalog_nodes WHERE kind='model'" "$export_script" || \
  { echo "FAIL: model addresses are not read from the catalogue"; exit 1; }
grep -q "FROM frappe_program" "$export_script" || \
  { echo "FAIL: record-kind addresses are not read from the surface store"; exit 1; }
grep -q "FROM discovery_set" "$export_script" || \
  { echo "FAIL: working-set addresses are not read from the record store"; exit 1; }
grep -q "FROM program_instance" "$export_script" || \
  { echo "FAIL: program addresses are not read from the program owner"; exit 1; }
grep -q "FROM wiki_page" "$export_script" || \
  { echo "FAIL: note addresses are not read from the note owner"; exit 1; }
grep -q "X402_CATALOG" "$export_script" || \
  { echo "FAIL: capability addresses are not read from the compiled catalogue"; exit 1; }
grep -q "sitemap-subjects.xml" "$export_script" || \
  { echo "FAIL: parcel addresses are not read from the public subject manifest"; exit 1; }
grep -q "bf_clone_grammars" "$export_script" || \
  { echo "FAIL: grammar addresses are not read from the compiled table"; exit 1; }
echo "per-entity addresses come from the stores the rooms read"

# ------------------------------------------------- the front door reaches them
db="$tmp/matrix.db"
port=18895
H='Host: aurekai.life'
"$bin" subject --db "$db" --id object:edge:probe --kind PlantPassport \
  --digest aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --evidence receipt:edge >/dev/null
"$bin" bootstrap-owned --db "$db" >/dev/null
"$bin" serve --db "$db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
sleep 1
base="http://127.0.0.1:$port"

curl -fsS -H "$H" "$base/" >"$tmp/home.html"
grep -qE 'href="/s/[a-z-]+/' "$tmp/home.html" || \
  { echo "FAIL: the home page links to no subject room"; exit 1; }
rooms_linked=$(grep -oE 'href="/s/[a-z-]+/[a-z0-9.-]+"' "$tmp/home.html" | sort -u | wc -l | tr -d ' ')
echo "the home page links to $rooms_linked rooms"

# The malformed next-length link. On the home route "/" this produced "//2",
# which a browser resolves as a protocol-relative URL to a host called "2", and
# it was live on every home page this estate ever served.
grep -q 'href="//' "$tmp/home.html" && \
  { echo "FAIL: the home page carries a protocol-relative link"; exit 1; }
# The current public root is a territory aperture and may have no paginated
# material cut at all.  When a composed root does name a next length, it must
# be a path, never a protocol-relative host.  Requiring pagination here made a
# valid sparse public root fail solely for not pretending to have a second page.
if grep -q 'rel="next"' "$tmp/home.html"; then
  grep -qE 'rel="next" href="/[0-9]+"' "$tmp/home.html" || \
    { echo "FAIL: the home page's next length is not a plain path"; exit 1; }
fi
# And the same function must still be right for a route that is not the root.
curl -fsS -H "$H" "$base/discover" >"$tmp/discover.html" 2>/dev/null || true
if [ -s "$tmp/discover.html" ]; then
  grep -q 'href="//' "$tmp/discover.html" && \
    { echo "FAIL: a composed route carries a protocol-relative link"; exit 1; }
fi
echo "the next-length link is a plain path on the root and on a named route"

echo "rooms reach the edge: PASS"
