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
BONFYRE_PRICES_DB="$prices" BONFYRE_MODEL_CATALOG="$catalog" BONFYRE_WORKSPACE_CLIENT="$root/integrations/browser" "$bin" serve --db "$db" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
sleep 1
base="http://127.0.0.1:$port"

curl -fsS -H "$H" "$base/s/models/all" >"$tmp/index.html"
curl -fsS -H "$H" "$base/assets/model-commons.css" >"$tmp/page.css"
grep -q '\.model-price-scroll' "$tmp/page.css" || { echo 'FAIL: generated Model Commons stylesheet unavailable'; exit 1; }
# The workbench stylesheet and module come from the workspace client release.
curl -fsS -H "$H" "$base/apps/aurekai-model.css" >"$tmp/model.css"
grep -q '\.ak-model-workbench' "$tmp/model.css" || { echo 'FAIL: generated model workbench stylesheet unavailable'; exit 1; }
curl -fsS -H "$H" "$base/s/model/alpha" >"$tmp/alpha.html"
curl -fsS -H "$H" "$base/s/model/beta" >"$tmp/beta.html"
curl -fsS -H "$H" "$base/s/model/delta" >"$tmp/delta.html"
# A model whose catalogue carries a measured passport shows that passport, its
# decision and its own envelope, and does not claim nothing was measured.
grep -q 'data-representation="delta/v1"' "$tmp/delta.html" || { echo "FAIL: delta's passport was not shown"; exit 1; }
grep -q '<dt>Tested for embedding</dt><dd>measured, on hold</dd>' "$tmp/delta.html" || { echo "FAIL: delta's passport state was not shown"; exit 1; }
grep -q 'Teacher agreement 18.5 percent at k=10; not retrieval quality. Check code abc123.' "$tmp/delta.html" || { echo "FAIL: delta's envelope or check code was not shown"; exit 1; }
! grep -q 'data-unmeasured' "$tmp/delta.html" || { echo "FAIL: delta was reported as untested beside its passport"; exit 1; }
! grep -q 'data-representation' "$tmp/alpha.html" || { echo "FAIL: alpha was given a passport it does not have"; exit 1; }
# A model nobody registered is a miss rather than an empty page.
# -f would discard the 404 body, which is where the refusal is stated.
curl -sS -H "$H" "$base/s/model/gamma" >"$tmp/gamma.html" 2>/dev/null || true
grep -q "No model with that identifier" "$tmp/gamma.html" || { echo "FAIL: an unregistered model was not refused"; exit 1; }
# The filter narrows the list on the server and says when nothing matches.
curl -fsS -H "$H" "$base/s/models/all?q=summarise" >"$tmp/find.html"
grep -q 'href="/s/model/alpha"' "$tmp/find.html" && ! grep -q 'href="/s/model/beta"' "$tmp/find.html" || { echo "FAIL: finding by use did not narrow the list"; exit 1; }
curl -fsS -H "$H" "$base/s/models/all?q=%3Cnothing%3E" >"$tmp/none.html"
grep -q 'No model matches &ldquo;&lt;nothing&gt;&rdquo;' "$tmp/none.html" || { echo "FAIL: an empty find was not stated or not escaped"; exit 1; }

python3 - "$tmp/index.html" "$tmp/alpha.html" "$tmp/beta.html" <<'PY'
import re, sys, html
index, alpha, beta = (open(p).read() for p in sys.argv[1:4])
def facts(t):
    return {m.group(1): m.group(2) for m in re.finditer(r'<dt>([^<]*)</dt><dd>([^<]*)</dd>', t)}
assert 'role="region" tabindex="0" aria-labelledby="model-prices-title" aria-describedby="model-prices-note"' in index
assert '/assets/model-commons.css' in index and '<style>' not in index[index.index('class="ak-mc"'):]
assert 'style="text-align:left' not in index[index.index('id="model-prices-title"'):index.index('</table>',index.index('id="model-prices-title"'))]
assert '<time>&lt;img src=x</time>' in index and '<time><img' not in index
assert 'Router A' in index and '$1.00' in index and '$2.00' in index
# One flow: the catalogue leads, nothing is folded away, no raw action wall.
assert '<details' not in index and '<details' not in alpha, "Model Commons must not hide its work in collapses"
assert 'What you can do with this' not in index and 'What you can do with this' not in alpha
assert index.index('class="ak-mc-list"') < index.index('id="model-prices-title"'), "models should lead, prices follow"
f = facts(index)
# Six edges: four resolve, two name a node that was never inserted... plus the
# layer artifact, so three unresolved in total.
assert f["Models"] == "3", f
assert f["Connections"] == "5", "resolving relations should be 5, got %s" % f["Connections"]
assert f["Broken links"] == "3", "unresolved should be 3, got %s" % f["Broken links"]
assert "left out of these counts" in index, \
    "the page did not say what it does with an unresolved reference"

def row(name):
    m = re.search(r'href="/s/model/%s">.*?<span class="ak-mc-uses">(.*?)</span></a>' % name, index, re.S)
    assert m, "%s was not listed" % name
    return html.unescape(m.group(1))
# alpha's dependents count the capability and not the two phantom recipes.
detail = row("alpha")
assert "used by 1" in detail and "Used in Summarise" in detail, "alpha's dependents counted a phantom recipe: %r" % detail
assert "some links broken" in detail, "alpha's unresolved references were not flagged"
# beta resolves cleanly and must not be flagged.
assert "broken" not in row("beta"), "a model with no dangling edges was flagged as having some"
# A pull source is never shown as a use, so a local path cannot leak through it.
assert "alpha.gguf" not in index

# alpha's own page names each unfollowable reference instead of dropping it.
broken = alpha[alpha.index('data-broken-links'):]
named = re.findall(r'<li><code>([^<]*)</code></li>', broken)
assert sorted(named) == ["layer:never-registered", "recipe:A1", "recipe:A2"], named
assert "never added to the catalogue" in alpha

# A measured figure appears only where one exists.
assert "data-unmeasured" in alpha and "Not tested yet" in alpha, "alpha has no measurement and did not say so"
assert "0.812" in beta, "beta's measured figure was not shown"
assert "data-unmeasured" not in beta, "beta has a measurement and was still reported as having none"

# Catalogue presence does not invent a price or permission. The shared account
# client asks the actual owner before it can offer a check and a run.
assert "No price published" in alpha and "That doesn&rsquo;t make it free to run" in alpha
assert 'data-aurekai-model="alpha"' in alpha and 'data-model-name="Alpha Model"' in alpha
assert '/apps/aurekai-model.mjs' in index and '/apps/aurekai-model.mjs' in alpha
assert '/apps/aurekai-model.css' in alpha
assert "Being listed here doesn&rsquo;t mean you can run it" in alpha
# The flow reads in order: the model, then trying it, then what is known, then using it.
order = [alpha.index(s) for s in ('<h1>Alpha Model</h1>', 'class="ak-model-workbench"', 'id="ak-mc-about"', 'Use it in your work')]
assert order == sorted(order), order
print("index: 5 resolving relations, 3 unresolved; alpha names all 3 of its "
      "unfollowable references and counts 1 real dependent")
PY

echo "model commons contract: PASS"
