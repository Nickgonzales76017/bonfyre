#!/bin/sh
# The harvest on a made-up volume: it takes the estate's state and nothing
# else, checks every copy, names keys without copying them, catches a damaged
# database, and never writes to the volume.
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd); harvest="$here/../oracle_volume_harvest.sh"
tmp=$(mktemp -d); trap 'chmod -R u+w "$tmp" 2>/dev/null; rm -rf "$tmp"' EXIT INT TERM
fail() { echo "FAIL: $1" >&2; exit 1; }
v="$tmp/volume"; mkdir -p "$v/var/lib/bonfyre/cms" "$v/var/lib/bonfyre/foreign/receipts" "$v/etc/bonfyre" "$v/usr/lib/big" "$v/home/ubuntu/.bonfyre" "$v/var/cache/apt"
sqlite3 "$v/var/lib/bonfyre/cms/bonfyre_cms.db" "CREATE TABLE entry(id INTEGER PRIMARY KEY, name TEXT); INSERT INTO entry(name) VALUES('a'),('b');"
sqlite3 "$v/var/lib/bonfyre/auth.db" "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT); INSERT INTO users(email) VALUES('x@example.test');"
head -c 4096 /dev/urandom > "$v/var/lib/bonfyre/broken.db"
i=0; while [ $i -lt 25 ]; do echo "{\"source_url\":\"https://e.example/$i\"}" > "$v/var/lib/bonfyre/foreign/receipts/r$i.json"; i=$((i+1)); done
echo "listen 443" > "$v/etc/bonfyre/site.conf"; echo "PRIVATE" > "$v/etc/bonfyre/tls.key"; echo "K=1" > "$v/home/ubuntu/.bonfyre/cloud.env"
head -c 200000 /dev/zero > "$v/usr/lib/big/libsomething.so"; echo cache > "$v/var/cache/apt/pkg"
before=$(find "$v" -type f -exec shasum -a 256 {} + | sort | shasum | cut -d' ' -f1)
chmod -R a-w "$v"
receipt=$(sh "$harvest" "$v" "$tmp/out" 2>/dev/null)
echo "$receipt" | grep -q '"taken":29,"left_for_a_person":2,"databases":3,"databases_failing_integrity":1,' || fail "counts: $receipt"
echo "$receipt" | grep -q '"volume_written":false' || fail "receipt does not say the volume was untouched"
[ "$before" = "$(find "$v" -type f -exec shasum -a 256 {} + | sort | shasum | cut -d' ' -f1)" ] || fail "the volume changed"
[ ! -e "$tmp/out/tree/etc/bonfyre/tls.key" ] && [ ! -e "$tmp/out/tree/home/ubuntu/.bonfyre/cloud.env" ] || fail "a key or credential file was copied"
grep -q '"path":"etc/bonfyre/tls.key".*"taken":false' "$tmp/out/manifest.jsonl" || fail "the key is not named in the manifest"
[ ! -e "$tmp/out/tree/usr" ] && [ ! -e "$tmp/out/tree/var/cache" ] || fail "operating-system files were taken"
[ "$(sqlite3 "$tmp/out/tree/var/lib/bonfyre/auth.db" "select email from users")" = "x@example.test" ] || fail "the auth store did not come through"
grep '"path":"var/lib/bonfyre/broken.db"' "$tmp/out/manifest.jsonl" | grep -vq '"integrity":"ok"' || fail "a damaged database was called sound"
[ "$(zstd -dc --long=27 "$tmp/out/estate-state.tar.zst" | tar -tf - | wc -l | tr -d ' ')" = 29 ] || fail "the archive does not hold every taken file"
sh "$harvest" "$v" "$v/out" >/dev/null 2>&1 && fail "it agreed to write onto the volume"
sh "$harvest" "$tmp/nowhere" "$tmp/o2" >/dev/null 2>&1 && fail "it accepted a volume that is not mounted"
echo "PASS oracle volume harvest: 29 files taken, 2 keys left for a person, 1 damaged database named"
