#!/bin/sh
# Rows moved between real database servers, and the receipt checked against
# tampering.  SQLite is always exercised.  PostgreSQL and MariaDB servers are
# started here on loopback sockets when their binaries are installed; without
# them that leg is skipped and says so.  No fixture stands in for a server.
set -eu
bin=${1:?bridge binary required}
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
command -v duckdb >/dev/null || { echo 'db bridge: SKIP (duckdb not installed)'; exit 0; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bfbridge.XXXXXX")
pgpid='' mypid=''
cleanup() {
  [ -n "$mypid" ] && kill "$mypid" 2>/dev/null || true
  [ -d "$tmp/pg" ] && pg_ctl -D "$tmp/pg" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM
fail() { echo "db bridge: $1" >&2; exit 1; }
state() { printf '%s' "$1" | grep -o "\"table\":\"$2\",\"state\":\"[a-z_]*\"" | sed 's/.*"state":"//; s/"//'; }

sqlite3 "$tmp/a.db" "CREATE TABLE supplier(id INTEGER PRIMARY KEY, name TEXT, quoted_usd REAL, active INTEGER, quoted_at TEXT);
INSERT INTO supplier VALUES (1,'Haiphong Freight',630.5,1,'2026-10-08 09:00:00'),(2,'Stamped Steel Co',250,0,'2026-10-09 14:30:00'),(3,'Ünïcode & \"quotes\"',NULL,1,NULL);
CREATE TABLE note(id INTEGER, body TEXT); INSERT INTO note VALUES (1,'first'),(1,'first'),(2,'second'),(3,''),(4,NULL);"

out=$("$bin" db --from "sqlite:$tmp/a.db" --to "sqlite:$tmp/b.db")
printf '%s' "$out" | grep -q '"arrived":2,"differ":0,"not_moved":0,"rows_arrived":8,"verified":true' || fail "sqlite move not verified: $out"
# An existing table is not overwritten unless asked.
if "$bin" db --from "sqlite:$tmp/a.db" --to "sqlite:$tmp/b.db" >/dev/null 2>&1; then fail "an existing table was overwritten"; fi
"$bin" db --from "sqlite:$tmp/a.db" --to "sqlite:$tmp/b.db" --replace | grep -q '"verified":true' || fail "--replace did not move"
# The receipt has to notice a single changed value, a missing row, and an
# empty string turned into an absent value.
sqlite3 "$tmp/b.db" "UPDATE supplier SET quoted_usd=631 WHERE id=1"
out=$("$bin" db-verify --from "sqlite:$tmp/a.db" --to "sqlite:$tmp/b.db" || true)
[ "$(state "$out" supplier)" = values_differ ] || fail "one changed value went unnoticed"
[ "$(state "$out" note)" = arrived ] || fail "an untouched table was reported changed"
sqlite3 "$tmp/b.db" "UPDATE supplier SET quoted_usd=630.5 WHERE id=1; UPDATE note SET body=NULL WHERE id=3"
out=$("$bin" db-verify --from "sqlite:$tmp/a.db" --to "sqlite:$tmp/b.db" || true)
[ "$(state "$out" note)" = values_differ ] || fail "an empty string became absent unnoticed"
sqlite3 "$tmp/b.db" "DELETE FROM note WHERE id=4"
out=$("$bin" db-verify --from "sqlite:$tmp/a.db" --to "sqlite:$tmp/b.db" || true)
[ "$(state "$out" note)" = rows_differ ] || fail "a missing row went unnoticed"
# The source was opened read-only and is as it was.
[ "$(sqlite3 "$tmp/a.db" 'SELECT count(*) FROM note')" = 5 ] || fail "the source changed"
# A password in a URL never reaches the receipt.
out=$(BRIDGE_TO="postgres://u:s3cretpw@127.0.0.1:1/none" "$bin" db --from "sqlite:$tmp/a.db" --to env:BRIDGE_TO 2>&1 || true)
if printf '%s' "$out" | grep -q s3cretpw; then fail "a password was printed"; fi
echo 'db bridge: sqlite PASS'

# Verification cannot create a destination or mutate either existing file.
before_source=$(cksum "$tmp/a.db")
before_target=$(cksum "$tmp/b.db")
"$bin" db-verify --from "sqlite:$tmp/a.db" --to "sqlite:$tmp/b.db" >/dev/null || true
[ "$(cksum "$tmp/a.db")" = "$before_source" ] || fail "verification changed the source file"
[ "$(cksum "$tmp/b.db")" = "$before_target" ] || fail "verification changed the target file"
if "$bin" db-verify --from "sqlite:$tmp/a.db" --to "sqlite:$tmp/missing.db" >/dev/null 2>&1; then fail "missing target verified"; fi
[ ! -e "$tmp/missing.db" ] || fail "verification created the missing target"

# Field boundaries and NULL must survive the digest input. The old joined
# strings were identical for these two different pairs of values.
sqlite3 "$tmp/fields-source.db" "CREATE TABLE boundary(a TEXT,b TEXT);
INSERT INTO boundary VALUES('left'||char(31)||'middle','right');
CREATE TABLE absence(a TEXT,b TEXT); INSERT INTO absence VALUES(NULL,'right');"
cp "$tmp/fields-source.db" "$tmp/fields-target.db"
"$bin" db-verify --from "sqlite:$tmp/fields-source.db" --to "sqlite:$tmp/fields-target.db" | grep -q '"verified":true' || fail "equal special-character rows differ"
sqlite3 "$tmp/fields-target.db" "UPDATE boundary SET a='left',b='middle'||char(31)||'right'; UPDATE absence SET a=char(0);"
if "$bin" db-verify --from "sqlite:$tmp/fields-source.db" --to "sqlite:$tmp/fields-target.db" > "$tmp/fields.json"; then fail "changed special-character rows verified"; fi
out=$(cat "$tmp/fields.json")
[ "$(state "$out" boundary)" = values_differ ] || fail "field separator collision went unnoticed"
[ "$(state "$out" absence)" = values_differ ] || fail "NULL became NUL text unnoticed"
echo 'db bridge: read-only verification and field boundaries PASS'

command -v initdb >/dev/null && command -v pg_ctl >/dev/null && command -v mariadbd >/dev/null && command -v mariadb-install-db >/dev/null || {
  echo 'db bridge: SKIP server leg (postgres or mariadb server not installed)'; exit 0; }
initdb -D "$tmp/pg" -U bridge --auth=trust >/dev/null 2>&1 || fail "initdb"
pgport=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
pg_ctl -D "$tmp/pg" -o "-p $pgport -c listen_addresses=127.0.0.1 -c unix_socket_directories=$tmp" -l "$tmp/pg.log" -w start >/dev/null 2>&1 || { cat "$tmp/pg.log" >&2; fail "postgres did not start"; }
createdb -h 127.0.0.1 -p "$pgport" -U bridge target
myport=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
mariadb-install-db --datadir="$tmp/my" --auth-root-authentication-method=normal >/dev/null 2>&1 || fail "mariadb-install-db"
mariadbd --no-defaults --datadir="$tmp/my" --port="$myport" --bind-address=127.0.0.1 --socket="$tmp/my.sock" --skip-grant-tables --skip-networking=0 >"$tmp/my.log" 2>&1 &
mypid=$!
n=0; until mariadb --no-defaults -h 127.0.0.1 -P "$myport" -u root -e 'select 1' >/dev/null 2>&1; do
  n=$((n+1)); [ "$n" -lt 100 ] && kill -0 "$mypid" 2>/dev/null || { tail -5 "$tmp/my.log" >&2; fail "mariadb did not start"; }; sleep 0.2; done
mariadb --no-defaults -h 127.0.0.1 -P "$myport" -u root -e "CREATE DATABASE shop CHARACTER SET utf8mb4; CREATE DATABASE back CHARACTER SET utf8mb4; USE shop;
CREATE TABLE orders(id INT PRIMARY KEY, buyer VARCHAR(80), total DECIMAL(10,2), paid TINYINT(1), placed_at DATETIME, note TEXT);
INSERT INTO orders VALUES (1,'Ünïcode GmbH',450.00,1,'2026-10-08 09:26:25','first'),(2,'Acme',1299.99,0,'2026-10-09 00:00:00',NULL),(3,'Zero',0.00,0,'2026-10-10 23:59:59','');
CREATE TABLE order_line(order_id INT, sku VARCHAR(40), qty INT); INSERT INTO order_line VALUES (1,'ENC-14',20),(1,'ENC-14',20),(2,'BRK-2',1);"
my="mysql://root@127.0.0.1:$myport/shop" pg="postgres://bridge@127.0.0.1:$pgport/target" back="mysql://root@127.0.0.1:$myport/back"

out=$("$bin" db --from "$my" --to "$pg") || fail "mysql to postgres: $out"
printf '%s' "$out" | grep -q '"arrived":2,"differ":0,"not_moved":0,"rows_arrived":6,"verified":true' || fail "mysql to postgres not verified: $out"
[ "$(psql -h 127.0.0.1 -p "$pgport" -U bridge -d target -Atc "SELECT buyer||'|'||total FROM orders WHERE id=1")" = 'Ünïcode GmbH|450.00' ] || fail "postgres does not hold the row as written"
out=$("$bin" db --from "$pg" --to "$back") || fail "postgres to mysql: $out"
printf '%s' "$out" | grep -q '"arrived":2,"differ":0,"not_moved":0,"rows_arrived":6,"verified":true' || fail "postgres to mysql not verified: $out"
# The round trip agrees with where it started, read by the bridge and by MySQL.
"$bin" db-verify --from "$my" --to "$back" | grep -q '"verified":true' || fail "round trip does not match its origin"
[ "$(mariadb --no-defaults -h 127.0.0.1 -P "$myport" -u root -N -e "SELECT total FROM back.orders WHERE id=2")" = 1299.99 ] || fail "decimal changed on the way back"
psql -h 127.0.0.1 -p "$pgport" -U bridge -d target -qc "UPDATE orders SET total=451 WHERE id=1" >/dev/null
out=$("$bin" db-verify --from "$my" --to "$pg" || true)
[ "$(state "$out" orders)" = values_differ ] || fail "a changed value in postgres went unnoticed"
echo 'db bridge: mysql <-> postgres PASS'
