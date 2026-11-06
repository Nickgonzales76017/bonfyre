#!/bin/sh
# `bonfyre-model local` judges each checkpoint against the shared memory ledger:
# a model held by an open grant is loaded, one too big for what is spendable waits.
set -eu
bin=$1
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/models/a" "$tmp/models/b"
truncate -s 200M "$tmp/models/a/small.gguf"
truncate -s 900G "$tmp/models/b/huge.gguf"
sqlite3 "$tmp/admission.db" "CREATE TABLE resource_grants(id INTEGER PRIMARY KEY AUTOINCREMENT, plane TEXT NOT NULL, kind TEXT NOT NULL, volume TEXT NOT NULL, estimated_bytes INTEGER NOT NULL, granted_at TEXT NOT NULL, expires_at TEXT, released_at TEXT, detail TEXT);
  INSERT INTO resource_grants(plane,kind,volume,estimated_bytes,granted_at,detail) VALUES('moe-serve:7000','model-resident','memory',1,'2026-10-16T00:00:00Z','$tmp/models/a/small.gguf');"
out=$(BONFYRE_MODEL_ROOT="$tmp/models" BONFYRE_ADMISSION_DB="$tmp/admission.db" "$bin" local --json)
echo "$out" | grep -q '"path":"'"$tmp"'/models/a/small.gguf","bytes":209715200,"needs_bytes":[0-9]*,"state":"loaded","held_by":"moe-serve:7000"'
echo "$out" | grep -q '"path":"'"$tmp"'/models/b/huge.gguf".*"state":"waits"'
human=$(BONFYRE_MODEL_ROOT="$tmp/models" BONFYRE_ADMISSION_DB="$tmp/admission.db" "$bin" local)
echo "$human" | grep -q 'loaded   small.gguf'
echo "$human" | grep -q 'waits    huge.gguf'
[ ! -e "$tmp/models/.bonfyre" ]
echo "BonfyreModel local: PASS"
