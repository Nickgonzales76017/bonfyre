#!/bin/sh
# The spatial owner is only worth having if a past answer is reproducible, so
# every assertion below is about a question that a store of current state cannot
# answer at all, or answers wrongly.
set -eu
bin=$(cd "$(dirname -- "$1")" && pwd)/$(basename -- "$1")
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
db="$tmp/spatial.db"

j() { python3 -c 'import json,sys;d=json.load(sys.stdin);print(eval(sys.argv[1],{"d":d}))' "$1"; }

# An aircraft observed twice. The second fix was seen at 22:00:30 and did not
# reach this estate until 22:00:45 -- the gap that makes valid time and
# knowledge time different questions.
"$bin" observe --db "$db" --observation fix-a --subject subject:aircraft:abc123 \
  --geometry 'POINT(-96.7836 32.7843)' --lon -96.7836 --lat 32.7843 --crs EPSG:4326 \
  --observed-at 2026-09-12T22:00:00Z --recorded-at 2026-09-12T22:00:02Z \
  --mode observed --source-occurrence external:ext-00001 --provenance opensky > /dev/null
"$bin" observe --db "$db" --observation fix-b --subject subject:aircraft:abc123 \
  --geometry 'POINT(-96.7836 32.7933)' --lon -96.7836 --lat 32.7933 --crs EPSG:4326 \
  --observed-at 2026-09-12T22:00:30Z --recorded-at 2026-09-12T22:00:45Z \
  --mode observed --source-occurrence external:ext-00001 --provenance opensky > /dev/null

# --- the store refuses to be rewritten, by the store and not by convention ---
! sqlite3 "$db" "UPDATE spatial_fact SET lat=0 WHERE observation_id='fix-a';" 2>/dev/null
! sqlite3 "$db" "DELETE FROM spatial_fact WHERE observation_id='fix-a';" 2>/dev/null
[ "$(sqlite3 "$db" "SELECT lat FROM spatial_fact WHERE observation_id='fix-a';")" = "32.7843" ]

# --- knowledge time is not valid time -------------------------------------
# At 22:00:40 the second fix already held in the world and this estate had not
# yet received it. A store of current state answers this with fix-b and is
# wrong about what was knowable.
"$bin" reconstruct --db "$db" --subject subject:aircraft:abc123 \
  --at 2026-09-12T22:00:40Z --as-of 2026-09-12T22:00:40Z > "$tmp/before-arrival.json"
[ "$(j 'd["position"]["observation"]' < "$tmp/before-arrival.json")" = "fix-a" ]
# Asked again once it had arrived, the same valid instant answers differently.
"$bin" reconstruct --db "$db" --subject subject:aircraft:abc123 \
  --at 2026-09-12T22:00:40Z --as-of 2026-09-12T22:01:00Z > "$tmp/after-arrival.json"
[ "$(j 'd["position"]["observation"]' < "$tmp/after-arrival.json")" = "fix-b" ]

# --- a derivation is not an observation ------------------------------------
"$bin" observe --db "$db" --observation display-1 --subject subject:aircraft:abc123 \
  --geometry 'POINT(-96.7836 32.7888)' --lon -96.7836 --lat 32.7888 --crs EPSG:4326 \
  --observed-at 2026-09-12T22:00:17Z --recorded-at 2026-09-12T22:00:46Z \
  --mode interpolated --derived-from '["fix-a","fix-b"]' \
  --source-occurrence external:ext-00001 --provenance bonfyre-dead-reckoning > /dev/null
"$bin" reconstruct --db "$db" --subject subject:aircraft:abc123 \
  --at 2026-09-12T22:00:20Z --as-of 2026-09-12T22:01:00Z > "$tmp/any.json"
[ "$(j 'd["position"]["observation"]' < "$tmp/any.json")" = "display-1" ]
"$bin" reconstruct --db "$db" --subject subject:aircraft:abc123 \
  --at 2026-09-12T22:00:20Z --as-of 2026-09-12T22:01:00Z --source-claims-only > "$tmp/claims.json"
[ "$(j 'd["position"]["observation"]' < "$tmp/claims.json")" = "fix-a" ]

# A derivation that names no basis cannot be told from an observation
# afterwards, so it is refused at the door. So is a source claim that pretends
# to be derived.
! "$bin" observe --db "$db" --observation bad-1 --subject subject:aircraft:abc123 \
  --geometry 'POINT(0 0)' --crs EPSG:4326 --observed-at 2026-09-12T22:00:00Z \
  --recorded-at 2026-09-12T22:00:00Z --mode interpolated \
  --source-occurrence external:ext-00001 --provenance x 2>/dev/null
! "$bin" observe --db "$db" --observation bad-2 --subject subject:aircraft:abc123 \
  --geometry 'POINT(0 0)' --crs EPSG:4326 --observed-at 2026-09-12T22:00:00Z \
  --recorded-at 2026-09-12T22:00:00Z --mode observed --derived-from '["fix-a"]' \
  --source-occurrence external:ext-00001 --provenance x 2>/dev/null

# --- a claim expires rather than silently persisting -----------------------
"$bin" observe --db "$db" --observation truck-1 --subject subject:vendor:taco-truck \
  --geometry 'POINT(-96.7840 32.7845)' --lon -96.7840 --lat 32.7845 --crs EPSG:4326 \
  --observed-at 2026-09-12T22:00:00Z --valid-until 2026-09-12T23:00:00Z \
  --recorded-at 2026-09-12T22:00:00Z --mode reported \
  --source-occurrence external:ext-00002 --provenance vendor-self-report > /dev/null
"$bin" reconstruct --db "$db" --subject subject:vendor:taco-truck \
  --at 2026-09-12T22:30:00Z > "$tmp/truck-live.json"
[ "$(j 'd["position"]["observation"]' < "$tmp/truck-live.json")" = "truck-1" ]
# After expiry there is no current position, and that is different from never
# having had one. Both are reported, distinctly.
"$bin" reconstruct --db "$db" --subject subject:vendor:taco-truck \
  --at 2026-09-12T23:30:00Z > "$tmp/truck-expired.json"
[ "$(j 'd["state"]' < "$tmp/truck-expired.json")" = "expired" ]
[ "$(j 'd["position"] is None' < "$tmp/truck-expired.json")" = "True" ]
"$bin" reconstruct --db "$db" --subject subject:vendor:never-seen \
  --at 2026-09-12T23:30:00Z > "$tmp/truck-unknown.json"
[ "$(j 'd["state"]' < "$tmp/truck-unknown.json")" = "unknown" ]

# --- retraction leaves earlier knowledge reproducible ----------------------
fact_b=$(sqlite3 "$db" "SELECT fact_id FROM spatial_fact WHERE observation_id='fix-b';")
"$bin" retract --db "$db" --fact "$fact_b" --reason "source replayed a stale frame" \
  --recorded-at 2026-09-12T23:00:00Z > /dev/null
# What we believed at 22:01 is still what we believed at 22:01.
"$bin" reconstruct --db "$db" --subject subject:aircraft:abc123 \
  --at 2026-09-12T22:00:40Z --as-of 2026-09-12T22:01:00Z > "$tmp/pre-retraction.json"
[ "$(j 'd["position"]["observation"]' < "$tmp/pre-retraction.json")" = "fix-b" ]
# What we believe now is not.
"$bin" reconstruct --db "$db" --subject subject:aircraft:abc123 \
  --at 2026-09-12T22:00:40Z --as-of 2026-09-12T23:30:00Z --source-claims-only > "$tmp/post-retraction.json"
[ "$(j 'd["position"]["observation"]' < "$tmp/post-retraction.json")" = "fix-a" ]

# A derivation whose basis was retracted does not quietly outlive it.
"$bin" reconstruct --db "$db" --subject subject:aircraft:abc123 \
  --at 2026-09-12T22:00:20Z --as-of 2026-09-12T23:30:00Z > "$tmp/orphan.json"
[ "$(j 'd["position"]["observation"]' < "$tmp/orphan.json")" = "display-1" ]
[ "$(j 'd["position"]["derivation"]' < "$tmp/orphan.json")" = "orphaned_basis_retracted" ]
# And before the retraction it was not orphaned.
"$bin" reconstruct --db "$db" --subject subject:aircraft:abc123 \
  --at 2026-09-12T22:00:20Z --as-of 2026-09-12T22:01:00Z > "$tmp/not-orphan.json"
[ "$(j '"derivation" in d["position"]' < "$tmp/not-orphan.json")" = "False" ]

# --- the exact closure, run at a frontier ---------------------------------
# fix-a and fix-b are 0.009 degrees of latitude apart: a shade over a kilometre.
"$bin" near --db "$db" --at 2026-09-12T22:00:40Z --as-of 2026-09-12T22:01:00Z \
  --lon -96.7836 --lat 32.7843 --radius-m 1500 --source-claims-only > "$tmp/near.json"
[ "$(j 'len(d["selected"])' < "$tmp/near.json")" = "2" ]
[ "$(j 'int(round([s for s in d["selected"] if s["subject"]=="subject:aircraft:abc123"][0]["distance_m"]))' < "$tmp/near.json")" -gt 995 ]
[ "$(j 'int(round([s for s in d["selected"] if s["subject"]=="subject:aircraft:abc123"][0]["distance_m"]))' < "$tmp/near.json")" -lt 1010 ]
# A tight radius excludes the aircraft and keeps the truck.
"$bin" near --db "$db" --at 2026-09-12T22:00:40Z --as-of 2026-09-12T22:01:00Z \
  --lon -96.7836 --lat 32.7843 --radius-m 100 --source-claims-only > "$tmp/near-tight.json"
[ "$(j 'len(d["selected"])' < "$tmp/near-tight.json")" = "1" ]
[ "$(j 'd["selected"][0]["subject"]' < "$tmp/near-tight.json")" = "subject:vendor:taco-truck" ]
# A subject with three facts is one thing in a place, not three.
"$bin" nearest --db "$db" --at 2026-09-12T22:00:40Z --as-of 2026-09-12T22:01:00Z \
  --lon -96.7836 --lat 32.7843 --n 5 > "$tmp/nearest.json"
[ "$(j 'len([s for s in d["selected"] if s["subject"]=="subject:aircraft:abc123"])' < "$tmp/nearest.json")" = "1" ]
[ "$(j 'd["selected"][0]["subject"]' < "$tmp/nearest.json")" = "subject:vendor:taco-truck" ]
# The same closure after expiry drops the truck without being told to.
"$bin" near --db "$db" --at 2026-09-12T23:30:00Z --as-of 2026-09-12T23:30:00Z \
  --lon -96.7836 --lat 32.7843 --radius-m 100 > "$tmp/near-after-expiry.json"
[ "$(j 'len(d["selected"])' < "$tmp/near-after-expiry.json")" = "0" ]
"$bin" bbox --db "$db" --at 2026-09-12T22:00:40Z --as-of 2026-09-12T22:01:00Z \
  --min-lon -96.79 --min-lat 32.78 --max-lon -96.78 --max-lat 32.80 \
  --source-claims-only > "$tmp/bbox.json"
[ "$(j 'len(d["selected"])' < "$tmp/bbox.json")" = "2" ]
# A box that excludes the aircraft's position at this frontier excludes the
# aircraft. It is filtered on where it was then, not on where it has ever been.
"$bin" bbox --db "$db" --at 2026-09-12T22:00:40Z --as-of 2026-09-12T22:01:00Z \
  --min-lon -96.79 --min-lat 32.78 --max-lon -96.78 --max-lat 32.785 \
  --source-claims-only > "$tmp/bbox-tight.json"
[ "$(j 'len(d["selected"])' < "$tmp/bbox-tight.json")" = "1" ]
[ "$(j 'd["selected"][0]["subject"]' < "$tmp/bbox-tight.json")" = "subject:vendor:taco-truck" ]
# At an earlier knowledge time the aircraft was still at fix-a, which is inside
# that same tight box: the box did not move, the knowledge did.
"$bin" bbox --db "$db" --at 2026-09-12T22:00:40Z --as-of 2026-09-12T22:00:40Z \
  --min-lon -96.79 --min-lat 32.78 --max-lon -96.78 --max-lat 32.785 \
  --source-claims-only > "$tmp/bbox-earlier.json"
[ "$(j 'len(d["selected"])' < "$tmp/bbox-earlier.json")" = "2" ]

# --- instants are refused rather than coerced ------------------------------
! "$bin" reconstruct --db "$db" --subject subject:aircraft:abc123 --at "2026-09-12 22:00" 2>/dev/null
! "$bin" reconstruct --db "$db" --subject subject:aircraft:abc123 --at 2026-09-12T22:00:00 2>/dev/null
! "$bin" observe --db "$db" --observation bad-3 --subject s --geometry g --crs c \
  --observed-at 2026-09-12T22:00:30Z --effective-at 2026-09-12T22:00:00Z \
  --recorded-at 2026-09-12T22:00:30Z --mode observed --source-occurrence o --provenance p 2>/dev/null
! "$bin" observe --db "$db" --observation bad-4 --subject s --geometry g --crs c \
  --observed-at 2026-09-12T22:00:00Z --valid-until 2026-09-12T21:00:00Z \
  --recorded-at 2026-09-12T22:00:00Z --mode observed --source-occurrence o --provenance p 2>/dev/null

# --- the log still holds everything, including what was retracted ----------
"$bin" history --db "$db" --subject subject:aircraft:abc123 > "$tmp/history.json"
[ "$(j 'len(d["facts"])' < "$tmp/history.json")" = "4" ]
[ "$(j 'len([f for f in d["facts"] if "retracts" in f])' < "$tmp/history.json")" = "1" ]

# --- the addressable frontier ---------------------------------------------
# An address is a pure function of its coordinates and carries no population.
addr=$("$bin" address --world deep-ellum --at 2026-09-12T22:00:40Z \
         --as-of 2026-09-12T22:01:00Z --extent district --cell dfw-0912)
echo "$addr" | grep -q 'at=2026-09-12T22:00:40Z'
echo "$addr" | grep -q 'as-of=2026-09-12T22:01:00Z'
# Nothing about who or what is there may ride along in the link.
! echo "$addr" | grep -qi 'aircraft\|taco-truck\|POINT(\|32\.78'
# It round-trips to exactly itself: the pair means one thing on both sides, or
# two people opening the same URL see different pasts and neither is told.
round=$("$bin" address --parse "$(echo "$addr" | tr -d '"')" | j 'd["address"]')
[ "$round" = "$(echo "$addr" | tr -d '"')" ]
# Refused rather than repaired: an address with no frontier, a malformed
# instant, or a coordinate nobody declared.
! "$bin" address --parse '/w/deep-ellum/spatial' 2>/dev/null
! "$bin" address --parse '/w/deep-ellum/spatial?at=2026-09-12' 2>/dev/null
! "$bin" address --parse '/w/deep-ellum/spatial?at=2026-09-12T22:00:40Z&layers=everything' 2>/dev/null
! "$bin" address --parse '/w//spatial?at=2026-09-12T22:00:40Z' 2>/dev/null

# --- the two stages, composed over one frontier ---------------------------
estate="$(cd "$(dirname -- "$0")/../.." && pwd)/BonfyreExternalEstate/bonfyre-external-estate"
[ -x "$estate" ] || make -s -C "$(dirname -- "$estate")" bonfyre-external-estate >/dev/null
[ -x "$estate" ]

# The CMS advertises fix-a and the taco truck. It deliberately does NOT
# advertise fix-b, which the owner's log holds and which is the later fact at
# the frontier. If the plane were deciding truth, the world closure would
# answer fix-a; the plane only names candidate subjects, so it answers fix-b.
sqlite3 "$tmp/cms.db" "CREATE TABLE spatial_observation(observation_id TEXT,subject_address TEXT,geometry_kind TEXT,geometry TEXT,crs TEXT,horizontal_datum TEXT,observed_at TEXT,source_occurrence TEXT,observation_mode TEXT,derived_from TEXT,mobility TEXT,scale TEXT,region TEXT,cell TEXT,provenance TEXT,status TEXT); INSERT INTO spatial_observation VALUES('fix-a','subject:aircraft:abc123','point','POINT(-96.7836 32.7843)','EPSG:4326','WGS84','2026-09-12T22:00:00Z','external:ext-00001','observed','[]','moving','city','dfw','dfw-0912','opensky','published'); INSERT INTO spatial_observation VALUES('truck-1','subject:vendor:taco-truck','point','POINT(-96.7840 32.7845)','EPSG:4326','WGS84','2026-09-12T22:00:00Z','external:ext-00002','reported','[]','static','street','dfw','dfw-0912','vendor-self-report','published'); INSERT INTO spatial_observation VALUES('elsewhere-1','subject:vessel:far-away','point','POINT(-80.0 25.0)','EPSG:4326','WGS84','2026-09-12T22:00:00Z','external:ext-00003','observed','[]','moving','city','mia','mia-0912','ais','published');"
"$estate" emit-cms --cms-db "$tmp/cms.db" > "$tmp/spatial-ads.jsonl"
"$estate" import-advertisements --db "$tmp/plane.db" --advertisements "$tmp/spatial-ads.jsonl" > /dev/null
[ "$(sqlite3 "$tmp/plane.db" "SELECT count(*) FROM candidate_advertisement WHERE candidate_kind='spatial_observation';")" = "3" ]

# The vessel is in the log too, so its absence below is the cut's doing.
"$bin" observe --db "$db" --observation elsewhere-1 --subject subject:vessel:far-away \
  --geometry 'POINT(-80.0 25.0)' --lon -80.0 --lat 25.0 --crs EPSG:4326 \
  --observed-at 2026-09-12T22:00:00Z --recorded-at 2026-09-12T22:00:00Z \
  --mode observed --source-occurrence external:ext-00003 --provenance ais > /dev/null

"$bin" world --db "$db" --index "$tmp/plane.db" --world deep-ellum \
  --at 2026-09-12T22:00:40Z --as-of 2026-09-12T22:01:00Z --cell dfw-0912 \
  --source-claims-only > "$tmp/world.json"
cp "$tmp/plane.db" "$tmp/plane?#'quoted.db"
[ "$(sqlite3 "$tmp/plane?#'quoted.db" "SELECT count(*) FROM candidate_advertisement WHERE candidate_kind='spatial_observation';")" = "3" ]
"$bin" world --db "$db" --index "$tmp/plane?#'quoted.db" --world deep-ellum \
  --at 2026-09-12T22:00:40Z --as-of 2026-09-12T22:01:00Z --cell dfw-0912 \
  --source-claims-only > "$tmp/world-special-path.json"
[ "$(j 'd["coarse_considered"]' < "$tmp/world-special-path.json")" = "3" ]
# The coarse cut narrowed, and can be shown to have rather than said to have.
[ "$(j 'd["coarse_considered"]' < "$tmp/world.json")" = "3" ]
[ "$(j 'd["coarse_survivors"]' < "$tmp/world.json")" = "2" ]
! grep -q 'far-away' "$tmp/world.json"
# The plane named the subject; the owner answered from its own complete log,
# with the later fact the plane never advertised.
[ "$(j '[p for p in d["present"] if p["subject"]=="subject:aircraft:abc123"][0]["observation"]' < "$tmp/world.json")" = "fix-b" ]
# The same world at an earlier knowledge frontier is a different world.
"$bin" world --db "$db" --index "$tmp/plane.db" --world deep-ellum \
  --at 2026-09-12T22:00:40Z --as-of 2026-09-12T22:00:40Z --cell dfw-0912 \
  --source-claims-only > "$tmp/world-earlier.json"
[ "$(j '[p for p in d["present"] if p["subject"]=="subject:aircraft:abc123"][0]["observation"]' < "$tmp/world-earlier.json")" = "fix-a" ]
# And after the vendor's claim expires the vendor is simply not present.
"$bin" world --db "$db" --index "$tmp/plane.db" --world deep-ellum \
  --at 2026-09-12T23:30:00Z --cell dfw-0912 --source-claims-only > "$tmp/world-late.json"
! grep -q 'taco-truck' "$tmp/world-late.json"
# A radius composes with the cut rather than replacing it.
"$bin" world --db "$db" --index "$tmp/plane.db" --world deep-ellum \
  --at 2026-09-12T22:00:40Z --as-of 2026-09-12T22:01:00Z --cell dfw-0912 \
  --lon -96.7836 --lat 32.7843 --radius-m 100 --source-claims-only > "$tmp/world-near.json"
[ "$(j 'd["selected"]' < "$tmp/world-near.json")" = "1" ]
[ "$(j 'd["coarse_survivors"]' < "$tmp/world-near.json")" = "2" ]

# --- intake ---------------------------------------------------------------
# A GeoJSON document shaped like the ones real feeds publish: a canonical
# identifier at the feature root, an epoch-millisecond instant in properties,
# and three features that must each be refused for a different reason.
cat > "$tmp/feed.geojson" <<'JSON'
{"type":"FeatureCollection","features":[
 {"type":"Feature","id":"q1","properties":{"time":1789000000000},
  "geometry":{"type":"Point","coordinates":[-96.7836,32.7843,2.1]}},
 {"type":"Feature","id":"q2","properties":{"time":1789000060000},
  "geometry":{"type":"Point","coordinates":[-96.7900,32.7900]}},
 {"type":"Feature","id":"no-time","properties":{"mag":3.1},
  "geometry":{"type":"Point","coordinates":[-96.80,32.80]}},
 {"type":"Feature","id":"a-line","properties":{"time":1789000000000},
  "geometry":{"type":"LineString","coordinates":[[-96.8,32.8],[-96.7,32.7]]}},
 {"type":"Feature","id":"a-collection","properties":{"time":1789000000000},
  "geometry":{"type":"GeometryCollection","geometries":[]}},
 {"type":"Feature","id":"missing-geometry-kind","properties":{"time":1789000000000},
  "geometry":{"coordinates":[-96.81,32.81]}},
 {"type":"Feature","properties":{"time":1789000000000},
  "geometry":{"type":"Point","coordinates":[-96.81,32.81]}}
]}
JSON
ingest_db="$tmp/ingest.db"
"$bin" ingest --db "$ingest_db" --geojson "$tmp/feed.geojson" --profile testfeed \
  --source-occurrence external:test:feed --source-digest deadbeefcafe0001 \
  --recorded-at 2026-09-12T22:00:00Z --provenance test-source \
  --subject-property '@id' --subject-kind quake \
  --time-property time --time-format epoch_ms > "$tmp/ingest.json"
# The line is placed like anything else with an extent; only a geometry this
# owner cannot place at all is refused.
[ "$(j 'd["ingested"]' < "$tmp/ingest.json")" = "3" ]
# Each refusal is counted and named rather than silently dropped.
[ "$(j 'd["refused_no_source_instant"]' < "$tmp/ingest.json")" = "1" ]
[ "$(j 'd["refused_geometry"]' < "$tmp/ingest.json")" = "2" ]
[ "$(j 'd["refused_no_subject"]' < "$tmp/ingest.json")" = "1" ]
# The refusal that matters most: a feature with no source instant does not get
# stamped with the moment of ingest, which would be indistinguishable from a
# real observation forever afterwards.
[ "$(sqlite3 "$ingest_db" "SELECT count(*) FROM spatial_fact WHERE observed_at='2026-09-12T22:00:00Z';")" = "0" ]
# The source's own instants survive, and are not the ingest instant.
[ "$(sqlite3 "$ingest_db" "SELECT observed_at FROM spatial_fact ORDER BY observed_at LIMIT 1;")" = "2026-09-10T00:26:40Z" ]
[ "$(sqlite3 "$ingest_db" "SELECT count(DISTINCT recorded_at) FROM spatial_fact;")" = "1" ]
# Observing the same document twice is one observation of one thing.
"$bin" ingest --db "$ingest_db" --geojson "$tmp/feed.geojson" --profile testfeed \
  --source-occurrence external:test:feed --source-digest deadbeefcafe0001 \
  --recorded-at 2026-09-12T23:00:00Z --provenance test-source \
  --subject-property '@id' --subject-kind quake \
  --time-property time --time-format epoch_ms > "$tmp/ingest2.json"
[ "$(j 'd["ingested"]' < "$tmp/ingest2.json")" = "0" ]
[ "$(j 'd["already_present"]' < "$tmp/ingest2.json")" = "3" ]
# A different document is a different observation of the same subjects.
"$bin" ingest --db "$ingest_db" --geojson "$tmp/feed.geojson" --profile testfeed \
  --source-occurrence external:test:feed --source-digest deadbeefcafe0002 \
  --recorded-at 2026-09-12T23:00:00Z --provenance test-source \
  --subject-property '@id' --subject-kind quake \
  --time-property time --time-format epoch_ms > "$tmp/ingest3.json"
[ "$(j 'd["ingested"]' < "$tmp/ingest3.json")" = "3" ]
# An estate cannot have learned a position before it was observed.
! "$bin" ingest --db "$tmp/backdated.db" --geojson "$tmp/feed.geojson" --profile testfeed \
  --source-occurrence external:test:feed --source-digest deadbeefcafe0003 \
  --recorded-at 2026-09-09T00:00:00Z --provenance test-source \
  --subject-property '@id' --subject-kind quake \
  --time-property time --time-format epoch_ms > "$tmp/backdated.json" || true
[ "$(j 'd["refused_observed_after_recorded"]' < "$tmp/backdated.json")" = "3" ]
[ "$(j 'd["ingested"]' < "$tmp/backdated.json")" = "0" ]
# Intake produces source claims only. A derivation has to name the facts it came
# from and a source did not compute one.
! "$bin" ingest --db "$tmp/derived.db" --geojson "$tmp/feed.geojson" --profile testfeed \
  --source-occurrence external:test:feed --source-digest deadbeefcafe0004 \
  --recorded-at 2026-09-12T22:00:00Z --provenance test-source --mode interpolated \
  --subject-property '@id' --subject-kind quake \
  --time-property time --time-format epoch_ms 2>/dev/null
# A document that failed to parse is a failed observation, never an empty one:
# an empty ingest would look like a source that reported nothing.
printf 'not json at all' > "$tmp/broken.geojson"
! "$bin" ingest --db "$tmp/broken.db" --geojson "$tmp/broken.geojson" --profile testfeed \
  --source-occurrence external:test:feed --source-digest deadbeefcafe0005 \
  --recorded-at 2026-09-12T22:00:00Z --provenance test-source \
  --subject-property '@id' --subject-kind quake \
  --time-property time --time-format epoch_ms 2>/dev/null
# Ingested facts reconstruct like any other, at a frontier.
"$bin" reconstruct --db "$ingest_db" --subject subject:quake:q1 \
  --at 2026-09-12T22:30:00Z --as-of 2026-09-12T22:30:00Z > "$tmp/ingest-recon.json"
[ "$(j 'd["position"]["mode"]' < "$tmp/ingest-recon.json")" = "observed" ]
# Before the estate had received the document, it knew nothing.
"$bin" reconstruct --db "$ingest_db" --subject subject:quake:q1 \
  --at 2026-09-12T21:50:00Z --as-of 2026-09-12T21:50:00Z > "$tmp/ingest-early.json"
[ "$(j 'd["state"]' < "$tmp/ingest-early.json")" = "unknown" ]

# --- intake driven by a receipt rather than by hand -------------------------
# The occurrence and the digest were being typed on the command line, so nothing
# checked that they described the bytes being read. A receipt carries both and
# the bytes sit beside it, so the check is available and therefore mandatory.
body="$tmp/receipted.geojson"
cp "$tmp/feed.geojson" "$body"
digest=$(shasum -a 256 "$body" | awk '{print $1}')
python3 - "$body" "$digest" "$tmp/receipt.json" <<'PYR'
import json, sys
body, digest, out = sys.argv[1], sys.argv[2], sys.argv[3]
json.dump({"body_path": body, "content_sha256": digest,
           "source_url": "https://example.test/feed.geojson",
           "observed_at": "2026-09-12T22:00:00Z"}, open(out, "w"))
PYR
"$bin" from-receipt --db "$tmp/receipted.db" --receipt "$tmp/receipt.json" \
  --profile receipted --subject-property '@id' --subject-kind quake \
  --time-property time --time-format epoch_ms > "$tmp/receipted.json"
[ "$(j 'd["ingested"]' < "$tmp/receipted.json")" = "3" ]
# The digest is the receipt's, not one the caller asserted.
[ "$(j 'd["source_digest"]' < "$tmp/receipted.json")" = "$digest" ]
# The occurrence names the source the receipt observed.
[ "$(j '"example.test" in d["source_occurrence"]' < "$tmp/receipted.json")" = "True" ]
# The estate learned of these bytes when it observed them, not when it parsed them.
[ "$(sqlite3 "$tmp/receipted.db" "SELECT DISTINCT recorded_at FROM spatial_fact;")" = "2026-09-12T22:00:00Z" ]
# One byte of drift and nothing is read. Bytes that no longer hash to their
# receipt are not the bytes that were observed.
printf 'x' >> "$body"
! "$bin" from-receipt --db "$tmp/receipted.db" --receipt "$tmp/receipt.json" \
  --profile receipted --subject-property '@id' --subject-kind quake \
  --time-property time --time-format epoch_ms > "$tmp/drift.json"
[ "$(j 'd["error"]' < "$tmp/drift.json")" = "body_does_not_match_receipt" ]
[ "$(j 'd["expected_sha256"] != d["actual_sha256"]' < "$tmp/drift.json")" = "True" ]
[ "$(sqlite3 "$tmp/receipted.db" "SELECT count(*) FROM spatial_fact;")" = "3" ]
# A receipt naming no body and no digest cannot support a reading of anything.
printf '{"source_url":"https://example.test/x"}' > "$tmp/hollow.json"
! "$bin" from-receipt --db "$tmp/hollow.db" --receipt "$tmp/hollow.json" \
  --profile receipted --subject-property '@id' --subject-kind quake \
  --time-property time --time-format epoch_ms 2>/dev/null

# --- extents -------------------------------------------------------------
# A perimeter with a courtyard in it, a rail line, and a dot. The cheap way to
# handle the first two is to store a centroid and let every distance query
# pretend they are points, which answers "how far to the middle of it" while
# reporting "how far to it" -- wrong by the size of the thing, and most wrong
# exactly where the thing is biggest.
cat > "$tmp/extent.geojson" <<'JSON'
{"type":"FeatureCollection","features":[
 {"type":"Feature","id":"perimeter-1","properties":{"t":1789000000000},
  "geometry":{"type":"Polygon","coordinates":[
    [[-96.80,32.78],[-96.76,32.78],[-96.76,32.80],[-96.80,32.80],[-96.80,32.78]],
    [[-96.79,32.785],[-96.785,32.785],[-96.785,32.79],[-96.79,32.79],[-96.79,32.785]]]}},
 {"type":"Feature","id":"route-1","properties":{"t":1789000000000},
  "geometry":{"type":"LineString","coordinates":[[-96.70,32.70],[-96.70,32.75]]}},
 {"type":"Feature","id":"dot-1","properties":{"t":1789000000000},
  "geometry":{"type":"Point","coordinates":[-96.7836,32.7843]}}
]}
JSON
xdb="$tmp/extent.db"
"$bin" ingest --db "$xdb" --geojson "$tmp/extent.geojson" --profile extents \
  --source-occurrence external:test:extents --source-digest ex7e9711111111111111 \
  --recorded-at 2026-09-11T00:00:00Z --provenance test-source \
  --subject-property '@id' --subject-kind zone \
  --time-property t --time-format epoch_ms > "$tmp/extent-ingest.json"
[ "$(j 'd["ingested"]' < "$tmp/extent-ingest.json")" = "3" ]
[ "$(j 'd["refused_geometry"]' < "$tmp/extent-ingest.json")" = "0" ]
# An extent has no point, and is not given one. A NULL here is what stops a
# later distance query treating a perimeter as the dot at its middle.
[ "$(sqlite3 "$xdb" "SELECT count(*) FROM spatial_fact WHERE lon IS NULL;")" = "2" ]
[ "$(sqlite3 "$xdb" "SELECT count(*) FROM spatial_fact WHERE min_lon IS NOT NULL;")" = "3" ]
xf="--at 2026-09-11T00:00:00Z"
# Containment is a question only an area can answer.
"$bin" contains --db "$xdb" $xf --lon -96.77 --lat 32.79 > "$tmp/in.json"
[ "$(j '[s["subject"] for s in d["selected"]]' < "$tmp/in.json")" = "['subject:zone:perimeter-1']" ]
# A courtyard is not the building: a hole subtracts rather than being ignored.
"$bin" contains --db "$xdb" $xf --lon -96.7875 --lat 32.7875 > "$tmp/hole.json"
[ "$(j 'len(d["selected"])' < "$tmp/hole.json")" = "0" ]
"$bin" contains --db "$xdb" $xf --lon -96.5 --lat 32.0 > "$tmp/out.json"
[ "$(j 'len(d["selected"])' < "$tmp/out.json")" = "0" ]
# Distance to a line is distance to the line. The test point sits 0.001 degrees
# of longitude beside the middle of a north-south segment: about 94 m across at
# this latitude, where the nearest endpoint is over two kilometres away.
"$bin" near --db "$xdb" $xf --lon -96.701 --lat 32.72 --radius-m 200 > "$tmp/line.json"
[ "$(j 'd["selected"][0]["subject"]' < "$tmp/line.json")" = "subject:zone:route-1" ]
[ "$(j 'int(round(d["selected"][0]["distance_m"]))' < "$tmp/line.json")" -gt 90 ]
[ "$(j 'int(round(d["selected"][0]["distance_m"]))' < "$tmp/line.json")" -lt 98 ]
[ "$(j 'd["selected"][0]["measured_against"]' < "$tmp/line.json")" = "the_shape_itself_not_a_centroid" ]
# A point inside an area is at distance zero from it, not at distance to its edge.
"$bin" near --db "$xdb" $xf --lon -96.77 --lat 32.79 --radius-m 10 > "$tmp/inside-near.json"
[ "$(j '[s["subject"] for s in d["selected"] if s["distance_m"] == 0]' < "$tmp/inside-near.json")" = "['subject:zone:perimeter-1']" ]

# --- along a route, and within a declared reach ---------------------------
# A corridor along a line, not a circle round its midpoint.
cat > "$tmp/route.geojson" <<'JSON'
{"type":"LineString","coordinates":[[-96.70,32.70],[-96.70,32.76]]}
JSON
"$bin" along-route --db "$xdb" $xf --route-geojson "$tmp/route.geojson" \
  --corridor-m 150 > "$tmp/route-near.json"
[ "$(j '[s["subject"] for s in d["selected"]]' < "$tmp/route-near.json")" = "['subject:zone:route-1']" ]
# The polygon is kilometres off the route and stays out however wide the circle
# round the route's midpoint would have been.
[ "$(j '"subject:zone:perimeter-1" in [s["subject"] for s in d["selected"]]' < "$tmp/route-near.json")" = "False" ]
# A corridor narrow enough to exclude everything excludes everything.
"$bin" along-route --db "$xdb" $xf --route-geojson "$tmp/route.geojson" \
  --corridor-m 1 > "$tmp/route-tight.json"
[ "$(j 'len(d["selected"])' < "$tmp/route-tight.json")" = "1" ]

# Within a declared reach: inside the range and inside the angular sweep.
"$bin" visible-from --db "$xdb" $xf --lon -96.7836 --lat 32.70 \
  --heading 0 --fov 60 --range-m 20000 > "$tmp/north.json"
[ "$(j 'sorted(s["subject"] for s in d["selected"])' < "$tmp/north.json")" \
  = "['subject:zone:dot-1', 'subject:zone:perimeter-1']" ]
# Turned around, the same reach sees nothing.
"$bin" visible-from --db "$xdb" $xf --lon -96.7836 --lat 32.70 \
  --heading 180 --fov 60 --range-m 20000 > "$tmp/south.json"
[ "$(j 'len(d["selected"])' < "$tmp/south.json")" = "0" ]
# A range shorter than the distance sees nothing, whatever it is pointed at.
"$bin" visible-from --db "$xdb" $xf --lon -96.7836 --lat 32.70 \
  --heading 0 --fov 360 --range-m 100 > "$tmp/short.json"
[ "$(j 'len(d["selected"])' < "$tmp/short.json")" = "0" ]
# And it says what it is not: no elevation model is held here, so nothing
# occludes and a result is reach rather than sight.
[ "$(j 'd["cannot_infer"]' < "$tmp/north.json")" = "within_declared_reach_is_not_line_of_sight_no_terrain_occludes" ]

# --- identifier lineage ---------------------------------------------------
# A source may change the identifier it prefers while still publishing the ones
# it used before; USGS says exactly that about its GeoJSON `id` and its `ids`.
# Deriving a subject from the preferred id alone splits one earthquake into two
# the moment the preference moves.
cat > "$tmp/lin1.geojson" <<'JSON'
{"type":"FeatureCollection","features":[
 {"type":"Feature","id":"nc-first","properties":{"time":1789000000000,"ids":",nc-first,us-second,"},
  "geometry":{"type":"Point","coordinates":[-122.79,38.83]}}]}
JSON
cat > "$tmp/lin2.geojson" <<'JSON'
{"type":"FeatureCollection","features":[
 {"type":"Feature","id":"us-second","properties":{"time":1789000600000,"ids":",nc-first,us-second,"},
  "geometry":{"type":"Point","coordinates":[-122.80,38.84]}}]}
JSON
ldb="$tmp/lineage.db"
"$bin" ingest --db "$ldb" --geojson "$tmp/lin1.geojson" --profile usgs \
  --source-occurrence external:ext-02176 --source-digest lin1000000000000000 \
  --recorded-at 2026-09-12T22:00:00Z --provenance usgs \
  --subject-property '@id' --subject-kind earthquake \
  --time-property time --time-format epoch_ms --alias-property ids > "$tmp/lin1.json"
[ "$(j 'd["rejoined_by_identifier_lineage"]' < "$tmp/lin1.json")" = "0" ]
"$bin" ingest --db "$ldb" --geojson "$tmp/lin2.geojson" --profile usgs \
  --source-occurrence external:ext-02176 --source-digest lin2000000000000000 \
  --recorded-at 2026-09-12T22:10:00Z --provenance usgs \
  --subject-property '@id' --subject-kind earthquake \
  --time-property time --time-format epoch_ms --alias-property ids > "$tmp/lin2.json"
[ "$(j 'd["ingested"]' < "$tmp/lin2.json")" = "1" ]
[ "$(j 'd["rejoined_by_identifier_lineage"]' < "$tmp/lin2.json")" = "1" ]
# One subject, two observations, whichever identifier the source preferred.
[ "$(sqlite3 "$ldb" "SELECT count(DISTINCT subject) FROM spatial_fact;")" = "1" ]
[ "$(sqlite3 "$ldb" "SELECT count(*) FROM spatial_fact;")" = "2" ]
[ "$(sqlite3 "$ldb" "SELECT DISTINCT subject FROM spatial_fact;")" = "subject:earthquake:nc-first" ]
# Both identifiers resolve to it, and when each was first seen is recorded.
[ "$(sqlite3 "$ldb" "SELECT count(*) FROM subject_alias WHERE subject='subject:earthquake:nc-first';")" = "2" ]
[ "$(sqlite3 "$ldb" "SELECT first_seen FROM subject_alias WHERE alias='subject:earthquake:us-second';")" = "2026-09-10T00:26:40Z" ]
# Without the lineage the same two feeds split into two subjects, which is the
# failure this exists to prevent.
sdb="$tmp/split.db"
for f in lin1 lin2; do
  "$bin" ingest --db "$sdb" --geojson "$tmp/$f.geojson" --profile usgs \
    --source-occurrence external:ext-02176 --source-digest "${f}zzzzzzzzzzzzzzzzzz" \
    --recorded-at 2026-09-12T22:20:00Z --provenance usgs \
    --subject-property '@id' --subject-kind earthquake \
    --time-property time --time-format epoch_ms > /dev/null
done
[ "$(sqlite3 "$sdb" "SELECT count(DISTINCT subject) FROM spatial_fact;")" = "2" ]

# --- occurrence-keyed owner evidence -------------------------------------
# An owner handoff that leaves no occurrence-keyed evidence is a handoff nobody
# can measure. The coordinate has to be the receipt's own, not one synthesized
# from a URL that no ExternalEstate occurrence ever used.
"$bin" ingest --db "$tmp/ev.db" --geojson "$tmp/lin1.geojson" --profile usgs \
  --source-occurrence external:ext-02176 --source-digest ev10000000000000000 \
  --recorded-at 2026-09-12T22:00:00Z --provenance usgs \
  --subject-property '@id' --subject-kind earthquake \
  --time-property time --time-format epoch_ms --evidence-out "$tmp/evidence.jsonl" > /dev/null
[ "$(j 'd["occurrence_id"]' < "$tmp/evidence.jsonl")" = "external:ext-02176" ]
[ "$(j 'd["from_owner"]' < "$tmp/evidence.jsonl")" = "ForeignTwin" ]
[ "$(j 'd["to_owner"]' < "$tmp/evidence.jsonl")" = "SpatialObservation" ]
[ "$(j 'd["axis"]' < "$tmp/evidence.jsonl")" = "semantic" ]
# `interpreted` is the honest rung: intake produced typed facts and no
# DomainProfile is bound here yet.
[ "$(j 'd["state"]' < "$tmp/evidence.jsonl")" = "interpreted" ]
# An empty ingest is not a handoff and records none.
cat > "$tmp/empty.geojson" <<'JSON'
{"type":"FeatureCollection","features":[]}
JSON
"$bin" ingest --db "$tmp/ev.db" --geojson "$tmp/empty.geojson" --profile usgs \
  --source-occurrence external:ext-02176 --source-digest ev20000000000000000 \
  --recorded-at 2026-09-12T22:05:00Z --provenance usgs \
  --subject-property '@id' --subject-kind earthquake \
  --time-property time --time-format epoch_ms --evidence-out "$tmp/evidence.jsonl" > /dev/null
[ "$(wc -l < "$tmp/evidence.jsonl" | tr -d ' ')" = "1" ]
# A receipt carrying a twin_id hands that exact coordinate to the facts rather
# than synthesizing one from the URL.
python3 - "$tmp/lin1.geojson" "$(shasum -a 256 "$tmp/lin1.geojson" | awk '{print $1}')" "$tmp/twin-receipt.json" <<'PYR'
import json, sys
json.dump({"body_path": sys.argv[1], "content_sha256": sys.argv[2],
           "twin_id": "external:ext-02176",
           "source_url": "https://earthquake.usgs.gov/fdsnws/event/1/",
           "observed_at": "2026-09-12T22:00:00Z"}, open(sys.argv[3], "w"))
PYR
"$bin" from-receipt --db "$tmp/twin.db" --receipt "$tmp/twin-receipt.json" \
  --profile usgs --subject-property '@id' --subject-kind earthquake \
  --time-property time --time-format epoch_ms \
  --evidence-out "$tmp/twin-evidence.jsonl" > "$tmp/twin-ingest.json"
[ "$(j 'd["source_occurrence"]' < "$tmp/twin-ingest.json")" = "external:ext-02176" ]
[ "$(sqlite3 "$tmp/twin.db" "SELECT DISTINCT source_occurrence FROM spatial_fact;")" = "external:ext-02176" ]
[ "$(j 'd["occurrence_id"]' < "$tmp/twin-evidence.jsonl")" = "external:ext-02176" ]

# --- maintained live state ------------------------------------------------
# A historical frontier has to be recomputed. The live one does not, because an
# append-only log only ever moves the latest fact forward. What has to be true
# is that the shortcut and the long way agree.
mdb="$tmp/maintained.db"
for i in 1 2 3; do
  "$bin" observe --db "$mdb" --observation "m-$i" --subject "subject:bus:$i" \
    --geometry "POINT(-96.78 32.78)" --lon -96.78 --lat 32.78 --crs EPSG:4326 \
    --observed-at "2026-09-12T22:0$i:00Z" --recorded-at "2026-09-12T22:0$i:05Z" \
    --mode observed --source-occurrence o --provenance fleet > /dev/null
done
"$bin" maintain --db "$mdb" > "$tmp/maint1.json"
[ "$(j 'd["advanced"]' < "$tmp/maint1.json")" = "3" ]
[ "$(j 'd["maintained_through"]' < "$tmp/maint1.json")" = "3" ]
live_at=2026-09-12T23:00:00Z
"$bin" nearest --db "$mdb" --at "$live_at" --lon -96.78 --lat 32.78 --n 5 > "$tmp/recomputed.json"
"$bin" nearest --db "$mdb" --live --at "$live_at" --lon -96.78 --lat 32.78 --n 5 > "$tmp/live.json"
[ "$(j '[(s["subject"],s["fact"]) for s in d["selected"]]' < "$tmp/recomputed.json")" \
  = "$(j '[(s["subject"],s["fact"]) for s in d["selected"]]' < "$tmp/live.json")" ]

# A later fact for a subject moves the maintained view forward, and the work is
# proportional to what arrived rather than to the whole log.
"$bin" observe --db "$mdb" --observation m-later --subject subject:bus:1 \
  --geometry 'POINT(-96.70 32.70)' --lon -96.70 --lat 32.70 --crs EPSG:4326 \
  --observed-at 2026-09-12T22:30:00Z --recorded-at 2026-09-12T22:30:05Z \
  --mode observed --source-occurrence o --provenance fleet > /dev/null
"$bin" maintain --db "$mdb" > "$tmp/maint2.json"
[ "$(j 'd["maintained_from"]' < "$tmp/maint2.json")" = "3" ]
[ "$(j 'd["advanced"]' < "$tmp/maint2.json")" = "1" ]
[ "$(sqlite3 "$mdb" "SELECT fact_id FROM current_spatial_state WHERE subject='subject:bus:1';")" = "4" ]

# --live is refused while the view is behind, rather than answering from a
# frontier it has not caught up to.
"$bin" observe --db "$mdb" --observation m-unmaintained --subject subject:bus:9 \
  --geometry 'POINT(-96.60 32.60)' --lon -96.60 --lat 32.60 --crs EPSG:4326 \
  --observed-at 2026-09-12T22:40:00Z --recorded-at 2026-09-12T22:40:05Z \
  --mode observed --source-occurrence o --provenance fleet > /dev/null
! "$bin" nearest --db "$mdb" --live --at "$live_at" --lon -96.78 --lat 32.78 --n 5 2>/dev/null
"$bin" maintain --db "$mdb" > /dev/null

# A retraction moves the answer backwards, so the subject is recomputed rather
# than patched forward, and the maintained view still agrees with the log.
"$bin" retract --db "$mdb" --fact 4 --reason "replayed a stale frame" \
  --recorded-at 2026-09-12T23:10:00Z > /dev/null
"$bin" maintain --db "$mdb" > "$tmp/maint3.json"
[ "$(j 'd["recomputed_subjects"]' < "$tmp/maint3.json")" -ge 1 ]
after=2026-09-12T23:20:00Z
"$bin" nearest --db "$mdb" --at "$after" --lon -96.78 --lat 32.78 --n 5 > "$tmp/recomputed2.json"
"$bin" nearest --db "$mdb" --live --at "$after" --lon -96.78 --lat 32.78 --n 5 > "$tmp/live2.json"
[ "$(j '[(s["subject"],s["fact"]) for s in d["selected"]]' < "$tmp/recomputed2.json")" \
  = "$(j '[(s["subject"],s["fact"]) for s in d["selected"]]' < "$tmp/live2.json")" ]
# The retracted fact is gone from the maintained view, not merely hidden.
[ "$(sqlite3 "$mdb" "SELECT COALESCE(fact_id,0) FROM current_spatial_state WHERE subject='subject:bus:1';")" != "4" ]

# Expiry is still decided at read time. A maintained latest fact is not a stored
# assertion that it still holds.
"$bin" observe --db "$mdb" --observation m-expiring --subject subject:stall:1 \
  --geometry 'POINT(-96.78 32.78)' --lon -96.78 --lat 32.78 --crs EPSG:4326 \
  --observed-at 2026-09-12T23:00:00Z --valid-until 2026-09-12T23:15:00Z \
  --recorded-at 2026-09-12T23:00:05Z --mode reported \
  --source-occurrence o --provenance vendor > /dev/null
"$bin" maintain --db "$mdb" > /dev/null
"$bin" nearest --db "$mdb" --live --at 2026-09-12T23:10:00Z --lon -96.78 --lat 32.78 --n 9 > "$tmp/before-expiry.json"
[ "$(j '"subject:stall:1" in [s["subject"] for s in d["selected"]]' < "$tmp/before-expiry.json")" = "True" ]
"$bin" nearest --db "$mdb" --live --at 2026-09-12T23:30:00Z --lon -96.78 --lat 32.78 --n 9 > "$tmp/after-expiry.json"
[ "$(j '"subject:stall:1" in [s["subject"] for s in d["selected"]]' < "$tmp/after-expiry.json")" = "False" ]

# --- durable tier ---------------------------------------------------------
# SQLite is where the log is written and queried. It is not where a million
# facts should be kept. A cold tier that cannot be restored is a deletion with
# extra steps, so the round-trip is what gets proved rather than the file size.
if command -v "${BONFYRE_DUCKDB:-duckdb}" > /dev/null 2>&1; then
  "$bin" durable --db "$mdb" --out "$tmp/cold.parquet" > "$tmp/export.json"
  [ "$(j 'd["representation"]' < "$tmp/export.json")" = "parquet+zstd" ]
  [ -s "$tmp/cold.parquet" ]
  "$bin" durable --db "$mdb" --out "$tmp/cold.parquet" --verify > "$tmp/verify.json"
  [ "$(j 'd["identical"]' < "$tmp/verify.json")" = "True" ]
  # Equality is by content, so a hot store that has moved on is reported as
  # different rather than as still matching.
  "$bin" observe --db "$mdb" --observation m-after-export --subject subject:bus:42 \
    --geometry 'POINT(-96.5 32.5)' --lon -96.5 --lat 32.5 --crs EPSG:4326 \
    --observed-at 2026-09-12T23:40:00Z --recorded-at 2026-09-12T23:40:05Z \
    --mode observed --source-occurrence o --provenance fleet > /dev/null
  ! "$bin" durable --db "$mdb" --out "$tmp/cold.parquet" --verify > "$tmp/verify2.json"
  [ "$(j 'd["identical"]' < "$tmp/verify2.json")" = "False" ]
  [ "$(j 'd["hot"] != d["cold"]' < "$tmp/verify2.json")" = "True" ]
  # Thawed and read back, the cold representation answers the same question the
  # same way.
  "$bin" durable --db "$mdb" --thaw --from "$tmp/cold.parquet" --out "$tmp/thawed.db" > /dev/null
  "$bin" nearest --db "$tmp/thawed.db" --at "$after" --lon -96.78 --lat 32.78 --n 5 > "$tmp/thawed-read.json"
  [ "$(j '[(s["subject"],s["fact"]) for s in d["selected"]]' < "$tmp/recomputed2.json")" \
    = "$(j '[(s["subject"],s["fact"]) for s in d["selected"]]' < "$tmp/thawed-read.json")" ]
else
  echo "note: duckdb absent, durable tier not exercised" >&2
fi

echo "native spatial owner: ok"
