#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
base="${BONFYRE_STATE_DIR:-/tmp/bonfyre-workgraph-migration}/migration-$$"
fabric="$root/programs/bonfyre/bonfyre"
mkdir -p "$base/fresh"
make -C "$root/engine/core" >/dev/null
make -C "$root/programs/bonfyre" >/dev/null

BONFYRE_STATE_DIR="$base/fresh" "$fabric" fabric init >/dev/null
BONFYRE_STATE_DIR="$base/fresh" "$fabric" fabric init >/dev/null
test "$(sqlite3 "$base/fresh/fabric.db" 'SELECT max(version) FROM schema_migrations')" = 8
for version in 4 5 6 7 8; do
    test "$(sqlite3 "$base/fresh/fabric.db" "SELECT count(*) FROM schema_migrations WHERE version=$version")" = 1
done

# Build a version-3 shape without scheduler data, then invoke only the public runtime.
mkdir -p "$base/v3"
sqlite3 "$base/v3/fabric.db" <<'SQL'
CREATE TABLE fabric_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY,applied_at TEXT NOT NULL);
INSERT INTO schema_migrations VALUES(1,'v1'),(2,'v2'),(3,'v3');
INSERT INTO fabric_meta VALUES('schema_version','3');
CREATE TABLE missions(id TEXT PRIMARY KEY,status TEXT NOT NULL,context_generation TEXT NOT NULL,catalog_generation TEXT NOT NULL,provider_generation TEXT NOT NULL,input_snapshot TEXT NOT NULL,artifact_root TEXT NOT NULL,workgraph_cursor TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE workgraph_nodes(mission_id TEXT NOT NULL,node_id TEXT NOT NULL,operator_id TEXT NOT NULL,status TEXT NOT NULL,attempt INTEGER NOT NULL DEFAULT 0,retry_limit INTEGER NOT NULL DEFAULT 0,timeout_seconds INTEGER NOT NULL,PRIMARY KEY(mission_id,node_id));
CREATE TABLE events(id TEXT PRIMARY KEY,mission_id TEXT,task_id TEXT,attempt INTEGER NOT NULL,actor TEXT NOT NULL,operator_id TEXT,provider_id TEXT,model_id TEXT,start_at TEXT NOT NULL,end_at TEXT,duration_ms INTEGER,input_uri TEXT,output_uri TEXT,effect_class TEXT NOT NULL,status TEXT NOT NULL,error_code TEXT,receipt_id TEXT);
CREATE TABLE receipts(id TEXT PRIMARY KEY,subject_kind TEXT NOT NULL,subject_id TEXT NOT NULL,content_hash TEXT NOT NULL,payload TEXT NOT NULL,created_at TEXT NOT NULL);
SQL
BONFYRE_STATE_DIR="$base/v3" "$fabric" fabric init >/dev/null
test "$(sqlite3 "$base/v3/fabric.db" 'SELECT max(version) FROM schema_migrations')" = 8
for version in 4 5 6 7 8; do
    test "$(sqlite3 "$base/v3/fabric.db" "SELECT count(*) FROM schema_migrations WHERE version=$version")" = 1
done

mkdir -p "$base/malformed"
sqlite3 "$base/malformed/fabric.db" <<'SQL'
CREATE TABLE fabric_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY,applied_at TEXT NOT NULL);
INSERT INTO schema_migrations VALUES(1,'v1'),(2,'v2'),(3,'v3');
INSERT INTO fabric_meta VALUES('schema_version','3');
CREATE TABLE missions(id TEXT PRIMARY KEY,status TEXT);
CREATE TABLE workgraph_nodes(mission_id TEXT,node_id TEXT,status TEXT);
CREATE TABLE events(id TEXT PRIMARY KEY);
CREATE TABLE receipts(id TEXT PRIMARY KEY);
SQL
if BONFYRE_STATE_DIR="$base/malformed" "$fabric" fabric init >/dev/null 2>&1; then
    echo 'malformed version-3 database was accepted' >&2
    exit 1
fi
test "$(sqlite3 "$base/malformed/fabric.db" 'SELECT max(version) FROM schema_migrations')" = 3
