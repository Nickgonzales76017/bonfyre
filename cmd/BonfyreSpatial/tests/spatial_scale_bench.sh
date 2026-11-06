#!/bin/sh
# Reproduces the scale measurement recorded in architecture/spatial/spatial.yaff.
#
# Not part of `make`: it writes a 276 MB store and takes about half a minute to
# build. Run it when the numbers in the atlas need to be re-earned, and change
# the atlas when they move.
#
# The load is written straight to the table rather than through `observe`,
# because the point is to measure reads. Append-only is enforced by triggers on
# UPDATE and DELETE, which this does not touch.
set -eu
bin=$(cd "$(dirname -- "$1")" && pwd)/$(basename -- "$1")
facts=${2:-1000000}
subjects=${3:-20000}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
db="$tmp/scale.db"

"$bin" init --db "$db"
echo "loading $facts facts over $subjects subjects..."
sqlite3 "$db" "
PRAGMA synchronous=OFF;
INSERT INTO spatial_fact(observation_id,subject,geometry_kind,lon,lat,geometry,crs,
  observed_at,effective_at,valid_until,recorded_at,observation_mode,
  source_occurrence,derived_from,provenance)
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<$facts)
SELECT 'obs-'||i, 'subject:vehicle:'||(i%$subjects), 'point',
       -96.9 + (i%997)/10000.0, 32.7 + (i%991)/10000.0, 'POINT(x y)', 'EPSG:4326',
       strftime('%Y-%m-%dT%H:%M:%SZ', 1789000000 + i, 'unixepoch'),
       strftime('%Y-%m-%dT%H:%M:%SZ', 1789000000 + i, 'unixepoch'), NULL,
       strftime('%Y-%m-%dT%H:%M:%SZ', 1789000000 + i + 5, 'unixepoch'),
       'observed','external:load','[]','load-generator' FROM n;"

# First open builds the partial indexes; keep that out of the read timings.
"$bin" history --db "$db" --subject subject:vehicle:none > /dev/null

live=2026-09-12T12:00:00Z
past=2026-09-11T06:00:00Z
echo "store: $(sqlite3 "$db" 'SELECT COUNT(*) FROM spatial_fact;') facts, \
$(sqlite3 "$db" 'SELECT COUNT(DISTINCT subject) FROM spatial_fact;') subjects, \
$(du -h "$db" | cut -f1)"

echo "--- one subject reconstructed at a frontier"
time "$bin" reconstruct --db "$db" --subject subject:vehicle:137 --at "$live" > /dev/null
echo "--- every subject, exact closure at a live frontier"
time "$bin" nearest --db "$db" --at "$live" --lon -96.85 --lat 32.75 --n 5 > /dev/null
echo "--- every subject, exact closure at a historical frontier"
time "$bin" nearest --db "$db" --at "$past" --lon -96.85 --lat 32.75 --n 5 > /dev/null

echo "--- maintenance: a full pass over the whole log"
time "$bin" maintain --db "$db" --rebuild > /dev/null
echo "--- maintenance: an incremental pass over 200 newly appended facts"
sqlite3 "$db" "
INSERT INTO spatial_fact(observation_id,subject,geometry_kind,lon,lat,geometry,crs,
  observed_at,effective_at,valid_until,recorded_at,observation_mode,
  source_occurrence,derived_from,provenance)
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<=200)
SELECT 'delta-'||i, 'subject:vehicle:'||(i%200), 'point', -96.85, 32.75, 'P','EPSG:4326',
       strftime('%Y-%m-%dT%H:%M:%SZ', 1789500000 + i, 'unixepoch'),
       strftime('%Y-%m-%dT%H:%M:%SZ', 1789500000 + i, 'unixepoch'), NULL,
       strftime('%Y-%m-%dT%H:%M:%SZ', 1789500000 + i, 'unixepoch'),
       'observed','external:load','[]','load-generator' FROM n;"
time "$bin" maintain --db "$db" > /dev/null

if command -v "${BONFYRE_DUCKDB:-duckdb}" > /dev/null 2>&1; then
  echo "--- durable tier: hot SQLite against cold Parquet"
  time "$bin" durable --db "$db" --out "$tmp/cold.parquet" > /dev/null
  echo "hot  $(du -h "$db" | cut -f1)   cold $(du -h "$tmp/cold.parquet" | cut -f1)"
  "$bin" durable --db "$db" --out "$tmp/cold.parquet" --verify
fi
