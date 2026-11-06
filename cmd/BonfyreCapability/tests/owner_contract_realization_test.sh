#!/bin/sh
set -eu

bin=${1:?native capability binary required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
db="$root/capability.db"

# Seed the pre-migration defects as they existed in the deployed registry.
sqlite3 "$db" "CREATE TABLE capabilities(id TEXT PRIMARY KEY,name TEXT NOT NULL,description TEXT NOT NULL,tags TEXT,binary TEXT NOT NULL,command TEXT,model_id TEXT,hardware_tier TEXT,cost_estimate REAL,latency_tier TEXT,stage_class TEXT,artifact_out TEXT,keywords TEXT,source TEXT,updated INTEGER); INSERT INTO capabilities VALUES('audio-ingest','Audio Ingest','stale','', 'bonfyre-intake','add','-','cpu',.001,'instant','capability','-','', 'manual',0); INSERT INTO capabilities VALUES('translate-text','Text Translation','stale','', 'bonfyre-translate','run','gpt-4o','cpu',.005,'fast','capability','-','', 'manual',0);"

BONFYRE_CAPABILITY_DB="$db" "$bin" show audio-ingest > "$root/audio.txt"
grep -q '^binary        : bonfyre-ingest$' "$root/audio.txt"
grep -q '^command       : ingest-audio$' "$root/audio.txt"

if BONFYRE_CAPABILITY_DB="$db" "$bin" show translate-text > "$root/translation.txt" 2>&1; then
  echo 'unrealized translation remained executable' >&2
  exit 1
fi

# Builtin recipe/run rows must resolve through the Runtime owner rather than
# absent standalone managed binaries.
BONFYRE_CAPABILITY_DB="$db" "$bin" show recipe > "$root/recipe.txt"
BONFYRE_CAPABILITY_DB="$db" "$bin" show run > "$root/run.txt"
grep -q '^binary        : bonfyre-runtime$' "$root/recipe.txt"
grep -q '^command       : recipe$' "$root/recipe.txt"
grep -q '^binary        : bonfyre-runtime$' "$root/run.txt"
grep -q '^command       : run-recipe$' "$root/run.txt"

echo 'BonfyreCapability owner-contract realization: PASS'
