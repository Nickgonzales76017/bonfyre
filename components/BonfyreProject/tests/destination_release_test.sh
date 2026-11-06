#!/bin/sh
# Readiness is not delivery, and the negative cases are the contract.
#
# The release projection used to report readiness per path and print a literal
# zero where a delivery count belonged. The zero was the only field capable of
# contradicting the readiness figures beside it, and it had nothing behind it, so
# an operator could not tell whether nothing had been delivered or nothing was
# being counted. This holds the lifecycle that replaced it.
#
# Every assertion here is a refusal or a distinction:
#
#   a blocked destination            -> refused, with the missing binding named
#   a wrong generation               -> refused rather than delivered
#   a destination with no materializer -> refused rather than marked done
#   a source with nothing retained   -> delivers nothing
#   recompiling admissibility        -> does not touch a delivery
#   a price change                   -> does not stale a destination without amounts
#   archive drift                    -> does stale the destination that carries it
#   a staled delivery                -> keeps its receipt; stale is not withdrawn
set -eu

bin=${1:-./bonfyre-project}
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
command -v sqlite3 >/dev/null 2>&1 || { echo "destination release: SKIP (no sqlite3)"; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-destination-release.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

cms="$tmp/cms.db"; frontier="$tmp/frontier.db"; items="$tmp/items.db"
prices="$tmp/prices.db"; releases="$tmp/releases"
mkdir -p "$releases"

# rich  : retained material and a bound price -- search and shop both admissible
# quiet : a published contract with nothing retained -- search blocked
sqlite3 "$cms" "
  CREATE TABLE internet_source(
    url TEXT, status TEXT, source TEXT, family TEXT, carries TEXT,
    body_regime TEXT, tier TEXT, last_seen TEXT, cadence_seconds INTEGER,
    place_field TEXT, media_field TEXT, rights_field TEXT, publisher_ident TEXT
  );
  INSERT INTO internet_source VALUES
    ('https://rich.example/catalog','published','rich-catalog','commerce','product',
     'REMOTE','WARM','2026-09-26T12:00:00Z',900,'','','','rich-example'),
    ('https://quiet.example/feed','published','quiet-feed','publication','article',
     'REMOTE','COLD','',21600,'','','','');
  CREATE TABLE destination_release(
    namespace TEXT, created_at TEXT, updated_at TEXT, published_at TEXT, status TEXT,
    release_id TEXT, source_ref TEXT, destination TEXT, state TEXT, generation TEXT,
    blocked_binding TEXT, receipt_ref TEXT, materialized_count TEXT, admitted_by TEXT,
    state_changed_at TEXT, reason TEXT, cannot_infer TEXT);
  CREATE UNIQUE INDEX destination_release_id ON destination_release(namespace, release_id);
  CREATE TABLE source_release_delta(
    namespace TEXT, created_at TEXT, updated_at TEXT, published_at TEXT, status TEXT,
    delta_id TEXT, source_ref TEXT, from_generation TEXT, to_generation TEXT,
    consequence TEXT, field TEXT, added TEXT, removed TEXT, changed TEXT,
    observed_at TEXT, cannot_infer TEXT);
  CREATE UNIQUE INDEX source_release_delta_id ON source_release_delta(namespace, delta_id);
"
sqlite3 "$frontier" "
  CREATE TABLE channel(address TEXT, exact_address TEXT, state TEXT);
  INSERT INTO channel VALUES
    ('https://rich.example/catalog','https://rich.example/catalog','admitted'),
    ('https://quiet.example/feed','https://quiet.example/feed','eligible');
"
sqlite3 "$items" "
  CREATE TABLE item(source TEXT, key TEXT, kind TEXT, family TEXT, title TEXT,
    body TEXT, href TEXT, image TEXT, by TEXT, publisher TEXT, ident TEXT,
    at TEXT, first_read TEXT, last_read TEXT);
  INSERT INTO item(source,key,title,href,publisher,at,kind) VALUES
    ('rich-catalog','k1','Brake lever','https://rich.example/p/1','Rich','2026-09-26T10:00:00Z','product'),
    ('rich-catalog','k2','Clutch cable','https://rich.example/p/2','Rich','2026-09-26T11:00:00Z','product');
"
sqlite3 "$prices" "
  CREATE TABLE observed_price(source TEXT, publisher TEXT, item TEXT, amount REAL,
    currency TEXT, unit TEXT, region TEXT, kind TEXT, evidence_ref TEXT, observed_at TEXT);
  INSERT INTO observed_price VALUES
    ('rich-catalog','Rich','brake lever',18.50,'USD','each','Dallas','retail','source stated','2026-09-26T10:00:00Z'),
    ('rich-catalog','Rich','brake lever',19.75,'USD','each','Dallas','retail','source stated','2026-09-26T12:00:00Z');
"

run() {
  BONFYRE_CMS_DB="$cms" BONFYRE_SOURCE_FRONTIER_DB="$frontier" \
  BONFYRE_ITEM_ARCHIVE_DB="$items" BONFYRE_PRICES_DB="$prices" \
  BONFYRE_RELEASE_ROOT="$releases" "$bin" "$@"
}

# ---------------------------------------------------------------- compile
run foreign release-compile --write >"$tmp/compile.json"
python3 - "$tmp/compile.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
assert d["mode"] == "written", d["mode"]
assert d["delivered"] == 0, "compiling admissibility must deliver nothing"
assert d["admissible"] > 0 and d["blocked"] > 0, d
assert d["deltas_found"] >= 1, "one item observed at two amounts is a delta"
print("compiled: %d admissible, %d blocked, %d deltas"
      % (d["admissible"], d["blocked"], d["deltas_found"]))
PY

# Every blocked row names the binding it is missing. A blocked state with an
# empty reason is indistinguishable from a bug.
unnamed=$(sqlite3 "$cms" "SELECT COUNT(*) FROM destination_release WHERE state='blocked' AND COALESCE(blocked_binding,'')='';")
[ "$unnamed" = "0" ] || { echo "FAIL: $unnamed blocked rows name no binding"; exit 1; }
# Nothing the compiler writes may be in a delivering state.
bad=$(sqlite3 "$cms" "SELECT COUNT(*) FROM destination_release WHERE state IN ('delivered','stale','superseded');")
[ "$bad" = "0" ] || { echo "FAIL: compile produced $bad delivering rows"; exit 1; }

generation=$(python3 -c "import json;print(json.load(open('$tmp/compile.json'))['catalogue_generation'])")

# ---------------------------------------------------------------- refusals
# A destination with no materializer is refused, not marked done.
run foreign release-deliver 'https://rich.example/catalog' shop >"$tmp/shop.json" 2>/dev/null || true
grep -q '"state":"refused"' "$tmp/shop.json" || { echo "FAIL: shop was not refused"; exit 1; }

# A source with nothing retained delivers nothing.
run foreign release-deliver 'https://quiet.example/feed' search >"$tmp/quiet.json" 2>/dev/null || true
grep -qE '"state":"(blocked|refused)"' "$tmp/quiet.json" || { echo "FAIL: empty source was not refused"; exit 1; }
grep -q 'nothing retained' "$tmp/quiet.json" || { echo "FAIL: the refusal did not name the missing material"; exit 1; }

# A caller naming a generation this catalogue has moved past is refused rather
# than delivered under the wrong snapshot's name.
run foreign release-deliver 'https://rich.example/catalog' search \
    --expect-generation 'foreign-v1-notthisone' >"$tmp/gen.json" 2>/dev/null || true
grep -q '"state":"snapshot_mismatch"' "$tmp/gen.json" || { echo "FAIL: wrong generation was not refused"; exit 1; }
delivered=$(sqlite3 "$cms" "SELECT COUNT(*) FROM destination_release WHERE state='delivered';")
[ "$delivered" = "0" ] || { echo "FAIL: a refused delivery still moved a row"; exit 1; }

# ---------------------------------------------------------------- delivery
run foreign release-deliver 'https://rich.example/catalog' search \
    --expect-generation "$generation" >"$tmp/deliver.json"
python3 - "$tmp/deliver.json" <<'PY'
import json, sys, hashlib, os
d = json.load(open(sys.argv[1]))
assert d["state"] == "delivered", d
assert d["delivering_state"] is True, "the lifecycle must agree this state delivers"
assert d["materialized"] > 0, d
body = open(d["fragment"], "rb").read()
got = hashlib.sha256(body).hexdigest()
assert got == d["receipt_digest"], "the receipt names bytes that are not on disk: %s vs %s" % (got, d["receipt_digest"])
assert b"materialized_at" not in body, \
    "a fragment that carries when it was written can never hash the same twice"
print("delivered %d items, receipt verified against %d bytes on disk" % (d["materialized"], len(body)))
PY

# ------------------------------------------------- a recompile owns two states
# Recompiling admissibility must not reset a delivery. This is the regression
# that REPLACE caused: the delivery and its receipt reference vanished.
before=$(sqlite3 "$cms" "SELECT receipt_ref FROM destination_release WHERE state='delivered';")
run foreign release-compile --write >/dev/null
after=$(sqlite3 "$cms" "SELECT receipt_ref FROM destination_release WHERE state='delivered';")
[ -n "$after" ] && [ "$before" = "$after" ] || { echo "FAIL: a recompile destroyed the delivery"; exit 1; }

# ---------------------------------------------------------------- impact
# A price moved, and a search fragment carries no amount. The delivery stands.
run foreign release-impact >"$tmp/impact1.json"
python3 - "$tmp/impact1.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
assert d["invalidating"] >= 1, "the price delta must be invalidating in principle"
assert d["drifted"] == 0, "nothing drifted, so nothing may be staled"
assert d["still_hash_to_their_receipt"] == 1, d
assert not any(a["becomes"] == "stale" for a in d["affected"]), \
    "a price change staled a destination that carries no amount"
print("price change did not stale a fragment without amounts")
PY

# Now the material a delivered fragment actually carries changes.
sqlite3 "$items" "INSERT INTO item(source,key,title,href,publisher,at,kind) VALUES
  ('rich-catalog','k3','Throttle cable','https://rich.example/p/3','Rich','2026-09-26T13:00:00Z','product');"
run foreign release-impact --write >"$tmp/impact2.json"
python3 - "$tmp/impact2.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
assert d["drifted"] == 1, d
assert d["staled"] == 1, d
hit = [a for a in d["affected"] if a["becomes"] == "stale"]
assert hit and hit[0]["destination"] == "search", d["affected"]
assert hit[0]["receipt_digest"] != hit[0]["recomposes_to"], "drift was reported with equal digests"
print("archive drift staled the destination that carries it")
PY

# Stale is not withdrawn: the receipt and the count both survive.
row=$(sqlite3 "$cms" "SELECT state || '|' || (receipt_ref <> '') || '|' || materialized_count FROM destination_release WHERE state='stale';")
[ "$row" = "stale|1|2" ] || { echo "FAIL: a staled delivery lost its receipt or its count ($row)"; exit 1; }

# ------------------------------------------------------- the projection agrees
BONFYRE_CMS_DB="$cms" BONFYRE_SOURCE_FRONTIER_DB="$frontier" \
BONFYRE_ITEM_ARCHIVE_DB="$items" BONFYRE_PRICES_DB="$prices" \
  "$bin" foreign twins --limit 1 >"$tmp/twins.json" 2>/dev/null
python3 - "$tmp/twins.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
rel = d["destination_releases"]
assert rel["state"] == "projected", rel
assert rel["delivering_states"] == 3, "the lifecycle declares three delivering states"
assert rel["unrecognised_state"] == 0, rel
# The field that used to be a printed zero must now equal what is receipted.
assert d["release_summary"]["public_program_delivery_verified"] == rel["delivered_with_receipt"], d
assert rel["delivered_with_receipt"] == 1, rel
print("projection: %d rows, %d delivered with a receipt, %d blocked"
      % (rel["rows"], rel["delivered_with_receipt"], rel["blocked"]))
PY

echo "destination release contract: PASS"
