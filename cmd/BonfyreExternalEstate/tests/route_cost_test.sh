#!/bin/sh
# Cost routing, and the two mistakes that make it dangerous rather than wrong.
#
# First: units. `"price":"$0.005"` is dollars and `"amount":"20000"` is USDC
# base units at six decimals, which is two cents. Read as dollars it is twenty
# thousand. A router comparing that against a free source is not slightly off,
# it is comparing numbers that do not mean the same thing, and it will refuse a
# two-cent call as if it cost more than a car.
#
# Second: precedence. Every metered service serves its own description for
# nothing, so the estate has a proven free read of the landing page of something
# it has never paid for. Counting that read as evidence the service is free
# removed every priced source from the metered side of the route, which read as
# "nothing here costs anything" rather than as a bug.
set -eu
bin=${1:?binary required}
bin=$(CDPATH= cd -- "$(dirname -- "$bin")" && pwd -P)/$(basename -- "$bin")
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

sqlite3 "$tmp/estate.db" "
CREATE TABLE occurrence(id TEXT PRIMARY KEY, observed_name TEXT, source_ref TEXT, primary_url TEXT, source_kind TEXT, section TEXT, raw_json TEXT);
CREATE TABLE occurrence_term(occurrence_id TEXT, axis TEXT, term TEXT);
INSERT INTO occurrence VALUES('external:free-1','Free Source','r','https://a.test','public_api','s','{}');
INSERT INTO occurrence VALUES('external:paid-1','Cheap Metered','r','https://b.test','x402','s','{}');
INSERT INTO occurrence VALUES('external:paid-2','Dear Metered','r','https://c.test','x402','s','{}');
INSERT INTO occurrence VALUES('external:unknown','Never Read','r','https://d.test','public_api','s','{}');
INSERT INTO occurrence_term VALUES('external:free-1','affordance','search');
INSERT INTO occurrence_term VALUES('external:paid-1','affordance','search');
INSERT INTO occurrence_term VALUES('external:paid-2','affordance','search');
INSERT INTO occurrence_term VALUES('external:unknown','affordance','search');"

# paid-1 is two cents expressed in base units; paid-2 is fifty cents in dollars.
# Both metered sources were also read for free at their landing page.
cat > "$tmp/prices.jsonl" <<'JSON'
{"schema": "bonfyre-owner-participation.v1", "occurrence_id": "external:paid-1", "axis": "economic", "state": "quoted", "min_price_usd": 0.02, "max_price_usd": 0.02}
{"schema": "bonfyre-owner-participation.v1", "occurrence_id": "external:paid-2", "axis": "economic", "state": "quoted", "min_price_usd": 0.5, "max_price_usd": 0.5}
JSON
cat > "$tmp/free.jsonl" <<'JSON'
{"schema": "bonfyre-protocol-enactment.v1", "occurrence_id": "external:free-1", "axis": "protocol", "state": "obligations_closed"}
{"schema": "bonfyre-protocol-enactment.v1", "occurrence_id": "external:paid-1", "axis": "protocol", "state": "obligations_closed"}
{"schema": "bonfyre-protocol-enactment.v1", "occurrence_id": "external:paid-2", "axis": "protocol", "state": "obligations_closed"}
JSON

"$bin" route-cost --db "$tmp/estate.db" --need affordance:search \
  --price-book "$tmp/prices.jsonl" --free-proof "$tmp/free.jsonl" > "$tmp/route.json"

python3 - "$tmp/route.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
# A priced source stays metered even though its own page was read for free.
assert d["metered"] == 2, d
assert d["free_and_proven"] == 1, d
# A source nobody read and nobody priced is neither free nor cheap; it is unknown.
assert d["unpriced_and_unread"] == 1, d
assert d["frontier"] == 4, d
# Two cents must be two cents.
assert d["metered_span_usd"] == [0.02, 0.5], d["metered_span_usd"]
a = d["avoided_cost_usd"]
assert a["floor"] == 0.02, a
assert abs(a["mean"] - 0.26) < 1e-9, a
assert abs(a["median"] - 0.26) < 1e-9, a
assert a["ceiling"] == 0.5, a
# Two sellers is not a market. A mean over a thin book must say so, because a
# two-source average of $149 and $0.40 is $74 and nobody would ever be quoted it.
assert d["thin_market"] is True, d
assert d["route"] == "external:free-1", d
# The unknown candidate is not offered as a route at any price.
ids = [c["identity"] for c in d["candidates"]]
assert "external:unknown" not in ids, ids
print("route cost: PASS (units held, priced source stayed metered, unknown stayed unknown)")
PY
