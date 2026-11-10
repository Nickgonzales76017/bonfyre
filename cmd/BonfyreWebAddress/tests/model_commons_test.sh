#!/bin/sh
# A relation is only a dependency if the thing at the other end exists.
#
# The model catalogue holds 724 edges and 471 nodes, and 547 of those edges name
# a node that was never inserted -- recipes and layer artifacts referenced by id
# and never registered. The first version of this page counted them, so one model
# appeared to have nine dependents while its own page could name two, and the
# difference was invisible.
#
# So: relation figures count only edges whose other end resolves, unresolved
# references are counted in their own right, and a model's page names the ones it
# carries rather than letting a join drop them.
set -eu

bin="$1"
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
command -v sqlite3 >/dev/null 2>&1 || { echo "model commons: SKIP (no sqlite3)"; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-model-commons.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

catalog="$tmp/catalog.db"
db="$tmp/matrix.db"
prices="$tmp/prices.db"
sqlite3 "$prices" "CREATE TABLE observed_price(item TEXT, amount REAL, currency TEXT, unit TEXT, publisher TEXT, observed_at TEXT, kind TEXT, source TEXT, evidence_ref TEXT); INSERT INTO observed_price VALUES ('Fixture One',0.000001,'USD','token in','Router A','2026-10-01','inference','a','1'), ('Fixture One',0.000002,'USD','token out','Router A','2026-10-01','inference','a','1'), ('Fixture Two',0.000003,'USD','token in','Router B','<img src=x','inference','b','2');"
port=18896
H='Host: aurekai.life'

# Two models. `alpha` has one real dependent, one real capability, one real
# source, and two references to recipes that were never registered. `beta` has
# nothing but a registration.
sqlite3 "$catalog" "
  CREATE TABLE catalog_nodes(node_id TEXT PRIMARY KEY, kind TEXT, external_id TEXT,
    name TEXT, category TEXT, summary TEXT, source_path TEXT, source_hash TEXT,
    json_data TEXT, updated_at TEXT);
  CREATE TABLE catalog_edges(src_node_id TEXT, rel TEXT, dst_node_id TEXT, meta TEXT);
  INSERT INTO catalog_nodes VALUES
    ('model:alpha','model','alpha','Alpha Model','model','A registered model.','','',
     json('{\"format\":\"gguf\",\"size_mb\":1200,\"geometry\":\"-\",\"transform_family\":\"-\",\"mean_f1\":0}'),''),
    ('model:beta','model','beta','Beta Model','model','Registered and unused.','','',
     json('{\"format\":\"bin\",\"size_mb\":50,\"geometry\":\"-\",\"transform_family\":\"-\",\"mean_f1\":0.812}'),''),
    ('capability:summarise','capability','summarise','Summarise','capability','','','','{}',''),
    ('model_source:alpha-hf','model_source','alpha-hf','hf://example/alpha.gguf','','','','','{}',''),
    ('model:delta','model','delta','Delta Embedder','model','Carries a passport and no score.','','',
     json('{\"format\":\"bfse\",\"size_mb\":4,\"geometry\":\"-\",\"transform_family\":\"-\",\"mean_f1\":0}'),''),
    ('representation:delta/v1','representation','delta/v1','delta/v1','DEFER',
     'Teacher agreement 18.5 percent at k=10; not retrieval quality.','receipts/delta.json',
     'abc123',json('{\"admission_scope\":\"embedding\",\"measurement_state\":\"measured\"}'),'');
  INSERT INTO catalog_edges VALUES
    ('model:delta','has_representation','representation:delta/v1','DEFER:embedding'),
    ('representation:delta/v1','represents_model','model:delta','DEFER:embedding'),
    ('model:alpha','powers_capability','capability:summarise',''),
    ('capability:summarise','uses_model','model:alpha',''),
    ('model:alpha','has_source','model_source:alpha-hf',''),
    ('recipe:A1','uses_model','model:alpha',''),
    ('recipe:A2','uses_model','model:alpha',''),
    ('model:alpha','has_layer_artifact','layer:never-registered','');
"

"$bin" subject --db "$db" --id object:model:probe --kind PlantPassport \
  --digest aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --evidence receipt:model >/dev/null
"$bin" bootstrap-owned --db "$db" >/dev/null
BONFYRE_PRICES_DB="$prices" BONFYRE_MODEL_CATALOG="$catalog" "$bin" serve --db "$db" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
sleep 1
base="http://127.0.0.1:$port"

curl -fsS -H "$H" "$base/s/models/all" >"$tmp/index.html"
curl -fsS -H "$H" "$base/s/model/alpha" >"$tmp/alpha.html"
curl -fsS -H "$H" "$base/s/model/beta" >"$tmp/beta.html"
curl -fsS -H "$H" "$base/s/model/delta" >"$tmp/delta.html"
# A model whose catalogue carries a measured passport shows that passport, its
# admission state and its own envelope, and does not claim nothing was measured.
grep -q 'data-representation="delta/v1"' "$tmp/delta.html" || { echo "FAIL: delta's passport was not shown"; exit 1; }
grep -q 'measured embedding, DEFER' "$tmp/delta.html" || { echo "FAIL: delta's passport state was not shown"; exit 1; }
grep -q 'Teacher agreement 18.5 percent at k=10; not retrieval quality. Receipt abc123.' "$tmp/delta.html" || { echo "FAIL: delta's envelope or receipt was not shown"; exit 1; }
! grep -q 'nothing has been measured about this model here' "$tmp/delta.html" || { echo "FAIL: delta was reported as unmeasured beside its passport"; exit 1; }
! grep -q 'Representation passport' "$tmp/alpha.html" || { echo "FAIL: alpha was given a passport it does not have"; exit 1; }
# A model nobody registered is a miss rather than an empty page.
# -f would discard the 404 body, which is where the refusal is stated.
curl -sS -H "$H" "$base/s/model/gamma" >"$tmp/gamma.html" 2>/dev/null || true
grep -q "No model with that identifier" "$tmp/gamma.html" || { echo "FAIL: an unregistered model was not refused"; exit 1; }

python3 - "$tmp/index.html" "$tmp/alpha.html" "$tmp/beta.html" <<'PY'
import re, sys, html
index, alpha, beta = (open(p).read() for p in sys.argv[1:4])
def facts(t):
    return {m.group(1): m.group(2) for m in re.finditer(r'<dt>([^<]*)</dt><dd>([^<]*)</dd>', t)}
assert 'role="region" tabindex="0" aria-labelledby="model-prices-title" aria-describedby="model-prices-note"' in index
assert '<time>&lt;img src=x</time>' in index and '<time><img' not in index
assert 'Router A' in index and '$1.00' in index and '$2.00' in index
f = facts(index)
# Six edges: four resolve, two name a node that was never inserted... plus the
# layer artifact, so three unresolved in total.
assert f["Models"] == "3", f
assert f["Relations"] == "5", "resolving relations should be 5, got %s" % f["Relations"]
assert f["Unresolved"] == "3", "unresolved should be 3, got %s" % f["Unresolved"]
assert "excluded from relation counts" in index, \
    "the page did not say what it does with an unresolved reference"
assert '<details class="model-coverage" data-model-coverage>' in index, "catalogue diagnostics must be expandable"
assert '<summary>Catalogue coverage and unresolved references</summary>' in index, "coverage section needs an accessible label"
assert index.index("Registered models") < index.index("<details class=\"model-coverage\""), "models should lead, not catalogue diagnostics"
assert re.search(r'</details><style>.*?</style><div class="model-list" data-bf-list', index, re.S), "diagnostics must close before the model cards"

# alpha's dependents count the capability and not the two phantom recipes.
row = re.search(r'model/alpha"><span class="desk-l">[^<]*</span>'
                r'<span class="desk-a">([^<]*)</span><span class="desk-s">([^<]*)', index)
assert row, "alpha was not listed"
detail = html.unescape(row.group(2))
assert "used by 1" in detail, "alpha's dependents counted a phantom recipe: %r" % detail
assert "some references unresolved" in detail, "alpha's unresolved references were not flagged"

# beta resolves cleanly and must not be flagged.
brow = re.search(r'model/beta"><span class="desk-l">[^<]*</span>'
                 r'<span class="desk-a">([^<]*)</span><span class="desk-s">([^<]*)', index)
assert brow and "unresolved" not in html.unescape(brow.group(2)), \
    "a model with no dangling edges was flagged as having some"

# alpha's own page names each unfollowable reference instead of dropping it.
named = re.findall(r'desk-l">(recipe:[^<]*|layer:[^<]*)</span>', alpha)
assert sorted(named) == ["layer:never-registered", "recipe:A1", "recipe:A2"], named
assert "was never registered as a node" in alpha

# A measured figure appears only where one exists.
assert "Measured quality" in alpha and "none" in alpha, "alpha has no measurement and did not say so"
assert "0.812" in beta, "beta's measured figure was not shown"
assert "Measured quality" not in beta, "beta has a measurement and was still reported as having none"

# Catalogue presence does not invent a price or admission. The shared account
# client asks the actual owner before it can offer review and execution.
assert "No retained provider price observation matches this catalogue" in alpha
assert "A missing price does not establish free execution" in alpha
assert 'data-aurekai-model="alpha"' in alpha and 'data-model-name="Alpha Model"' in alpha
assert '/apps/aurekai-model.mjs' in index and '/apps/aurekai-model.mjs' in alpha
assert '/apps/aurekai-model.css' in alpha
assert "Registration alone does not establish availability or permission" in alpha
print("index: 5 resolving relations, 3 unresolved; alpha names all 3 of its "
      "unfollowable references and counts 1 real dependent")
PY

echo "model commons contract: PASS"
