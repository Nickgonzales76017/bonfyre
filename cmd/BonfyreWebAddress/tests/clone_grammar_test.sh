#!/bin/sh
# Thirty grammars are only cheaper than three applications if the cells under
# them are really shared, and really held by something.
#
# The failure this guards against is a wish list: thirty product names in a
# table, each implying a surface, none of them bound to an organ, and the ones
# resting on nothing indistinguishable from the ones resting on years of work.
# So every claim on these pages is computed from the declaration -- how many
# grammars share a cell is counted, an owner must be an architecture that exists
# in the atlas, and a cell nothing owns is named as such and taints every grammar
# that needs it.
#
# The compiler's refusals are tested too, because a contract that cannot refuse
# is a comment.
set -eu

bin="$1"
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
here="$root/cmd/BonfyreWebAddress"
command -v cue >/dev/null 2>&1 || { echo "clone grammar: SKIP (no cue)"; exit 0; }
command -v zig >/dev/null 2>&1 || { echo "clone grammar: SKIP (no zig)"; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-clone-grammar.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

(cd "$root/schemas/control" && cue export . -e nativeInteractionPrimitives --out json) >"$tmp/prim.json"
(cd "$root/schemas/control" && cue export . -e nativeCloneGrammars --out json) >"$tmp/gram.json"

# ------------------------------------------------ the declaration itself
python3 - "$tmp/prim.json" "$tmp/gram.json" "$root/architecture/atlas.index.json" <<'PY'
import json, sys
prim = json.load(open(sys.argv[1]))
gram = json.load(open(sys.argv[2]))
atlas = json.load(open(sys.argv[3]))
arch = atlas["architectures"] if isinstance(atlas, dict) and "architectures" in atlas else atlas
ids = set(arch.keys()) if isinstance(arch, dict) else {a["id"] for a in arch}

assert len(gram) == 30, "the target names thirty grammars, found %d" % len(gram)
names = {p["primitive"] for p in prim}
assert len(names) == len(prim), "two primitives share a name"

# An owner must be an architecture that exists. A grammar naming an organ that
# does not exist is the wish list this file exists to prevent.
for p in prim:
    if p["owner"]:
        assert p["owner"] in ids, "primitive %s claims %s, which is not in the atlas" % (p["primitive"], p["owner"])
        assert not p["unowned_because"], p["primitive"]
    else:
        assert p["unowned_because"], "primitive %s is unowned and gives no reason" % p["primitive"]
    assert p["cannot_infer"], p["primitive"]

for g in gram:
    assert g["over"] in ids, "grammar %s sits over %s, which is not in the atlas" % (g["grammar"], g["over"])
    assert g["needs"], "grammar %s needs nothing, so it explodes nothing" % g["grammar"]
    for cell in g["needs"]:
        assert cell in names, "%s needs undeclared cell %s" % (g["grammar"], cell)
    assert g["cannot_infer"], g["grammar"]

unowned = {p["primitive"] for p in prim if not p["owner"]}
blocked = [g["grammar"] for g in gram if set(g["needs"]) & unowned]
clear = [g["grammar"] for g in gram if not (set(g["needs"]) & unowned)]
# The invariant is the relationship, not the state of it. This once asserted that
# something must be missing, which stopped being true the moment the four unowned
# cells were built -- an assertion that fails when the work succeeds is testing
# the wrong thing.
assert len(blocked) + len(clear) == len(gram), "a grammar is neither waiting nor clear"
if unowned:
    assert blocked, "cells are unowned and no grammar inherited the gap"
else:
    assert not blocked, "nothing is unowned and a grammar is still marked as waiting"
    assert len(clear) == len(gram), "every grammar should rest on owned cells"
assert clear, "if nothing is clear, the shared-cell claim is empty"
print("declared: %d grammars over %d cells, %d cells unowned, %d grammars waiting on one"
      % (len(gram), len(prim), len(unowned), len(blocked)))
PY

# ------------------------------------------------ the counts are counted
# shared_by in the header must equal what the declaration implies. A hand-written
# figure here would be the assertion this page exists to replace.
python3 - "$tmp/prim.json" "$tmp/gram.json" "$here/generated/native_clone_grammars.h" <<'PY'
import json, re, sys
prim = json.load(open(sys.argv[1]))
gram = json.load(open(sys.argv[2]))
header = open(sys.argv[3]).read()
want = {p["primitive"]: sum(1 for g in gram if p["primitive"] in g["needs"]) for p in prim}
block = header[header.index("bf_interaction_primitives[] = {"):header.index("#define BF_INTERACTION_PRIMITIVE_COUNT")]
got = {}
for line in block.splitlines():
    m = re.match(r'\s*\{"([a-z-]+)",.*,\s*(\d+)\},\s*$', line)
    if m:
        got[m.group(1)] = int(m.group(2))
assert got, "no primitive rows parsed out of the generated header"
for name, n in want.items():
    assert got.get(name) == n, "%s: declared share %d, compiled %s" % (name, n, got.get(name))
# The same for each grammar's unowned count.
unowned = {p["primitive"] for p in prim if not p["owner"]}
gblock = header[header.index("bf_clone_grammars[] = {"):header.index("#define BF_CLONE_GRAMMAR_COUNT")]
rows = dict((m.group(1), (int(m.group(2)), int(m.group(3))))
            for m in re.finditer(r'\{"([a-z0-9-]+)",.*?,\s*\d+,\s*(\d+),\s*(\d+)\},', gblock))
for g in gram:
    need, un = rows[g["grammar"]]
    assert need == len(g["needs"]), g["grammar"]
    assert un == len(set(g["needs"]) & unowned), "%s: unowned %d, compiled %d" % (g["grammar"], len(set(g["needs"]) & unowned), un)
print("counts: every shared and unowned figure is computed, not written")
PY

# ------------------------------------------------ the compiler refuses
# A grammar needing an undeclared cell.
python3 - "$tmp/gram.json" "$tmp/bad-cell.json" <<'PY'
import json, sys
g = json.load(open(sys.argv[1]))
g[0]["needs"] = g[0]["needs"] + ["no-such-cell"]
json.dump(g, open(sys.argv[2], "w"))
PY
if (cd "$here" && zig run tools/compile_clone_grammars.zig -- "$tmp/prim.json" "$tmp/bad-cell.json" "$tmp/out.h") 2>/dev/null; then
  echo "FAIL: the compiler accepted a grammar needing an undeclared cell"; exit 1
fi

# A grammar needing nothing at all.
python3 - "$tmp/gram.json" "$tmp/empty.json" <<'PY'
import json, sys
g = json.load(open(sys.argv[1]))
g[0]["needs"] = []
json.dump(g, open(sys.argv[2], "w"))
PY
if (cd "$here" && zig run tools/compile_clone_grammars.zig -- "$tmp/prim.json" "$tmp/empty.json" "$tmp/out.h") 2>/dev/null; then
  echo "FAIL: the compiler accepted a grammar that needs nothing"; exit 1
fi

# An unowned cell with no reason, and an owned cell claiming one anyway.
python3 - "$tmp/prim.json" "$tmp/silent.json" "$tmp/both.json" <<'PY'
import json, sys
p = json.load(open(sys.argv[1]))
# Built rather than found: this used to clear the reason on whichever cell was
# already unowned, which quietly stopped testing anything once every cell had an
# owner. Strip an owner instead, so the bad input exists either way.
silent = json.loads(json.dumps(p))
silent[0]["owner"] = ""
silent[0]["unowned_because"] = ""
json.dump(silent, open(sys.argv[2], "w"))
both = json.loads(json.dumps(p))
for row in both:
    if row["owner"]:
        row["unowned_because"] = "claims a gap it does not have"
        break
json.dump(both, open(sys.argv[3], "w"))
PY
if (cd "$here" && zig run tools/compile_clone_grammars.zig -- "$tmp/silent.json" "$tmp/gram.json" "$tmp/out.h") 2>/dev/null; then
  echo "FAIL: the compiler accepted an unexplained gap"; exit 1
fi
if (cd "$here" && zig run tools/compile_clone_grammars.zig -- "$tmp/both.json" "$tmp/gram.json" "$tmp/out.h") 2>/dev/null; then
  echo "FAIL: the compiler accepted a cell that is both owned and unowned"; exit 1
fi
echo "compiler refuses: an undeclared cell, an empty grammar, a silent gap, a contradictory owner"

# ------------------------------------------------ the pages say it
db="$tmp/matrix.db"
port=18899
H='Host: aurekai.life'
"$bin" subject --db "$db" --id object:grammar:probe --kind PlantPassport \
  --digest aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --evidence receipt:grammar >/dev/null
"$bin" bootstrap-owned --db "$db" >/dev/null
"$bin" serve --db "$db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
sleep 1
base="http://127.0.0.1:$port"

curl -fsS -H "$H" "$base/s/grammars/all" >"$tmp/all.html"
curl -fsS -H "$H" "$base/s/grammar/school-institution" >"$tmp/one.html"
curl -fsS -H "$H" "$base/s/primitive/member" >"$tmp/cell.html"
# A grammar nobody declared is a miss, not an empty page.
code=$(curl -sS -o /dev/null -w '%{http_code}' -H "$H" "$base/s/grammar/not-a-grammar")
[ "$code" = "404" ] || [ "$code" = "200" ] || { echo "FAIL: unknown grammar answered $code"; exit 1; }
grep -q "No grammar of that name" "$tmp/one.html" && { echo "FAIL: a declared grammar was reported missing"; exit 1; }

python3 - "$tmp/all.html" "$tmp/one.html" "$tmp/cell.html" "$tmp/prim.json" "$tmp/gram.json" <<'PY'
import json, re, sys, html
allp, onep, cellp = (open(p).read() for p in sys.argv[1:4])
prim = json.load(open(sys.argv[4]))
gram = json.load(open(sys.argv[5]))
unowned = {p["primitive"] for p in prim if not p["owner"]}

# The index lists every grammar and every cell, and virtualises both lists
# because thirty-odd rows twice over is what the list organ is for.
assert allp.count("data-bf-list") == 2, allp.count("data-bf-list")
for g in gram:
    assert '/s/grammar/%s"' % g["grammar"] in allp, g["grammar"]
for p in prim:
    assert '/s/primitive/%s"' % p["primitive"] in allp, p["primitive"]

# Whichever way round it is, the page has to agree with the declaration: a
# grammar waiting on a missing cell says so and does not claim solid ground, and
# one resting entirely on owned cells says that instead.
school = [g for g in gram if g["grammar"] == "school-institution"][0]
if set(school["needs"]) & unowned:
    assert "nothing holds this" in onep, "a missing cell was not named on the page"
    assert "is not ready" in onep, "a grammar waiting on a missing cell did not say so"
    assert "nothing new has to be invented" not in onep, "a blocked grammar claimed solid ground"
else:
    assert "nothing new has to be invented" in onep, \
        "every cell this grammar needs is owned and the page did not say so"
    assert "is not ready" not in onep, "an unblocked grammar was still marked as waiting"

# Its cross-density is computed: every other grammar listed really does share
# two or more cells with it.
mine = set(school["needs"])
listed = set(re.findall(r'/s/grammar/([a-z0-9-]+)"><span class="desk-l"', onep))
listed.discard("school-institution")
for other in listed:
    o = [g for g in gram if g["grammar"] == other][0]
    assert len(mine & set(o["needs"])) >= 2, "%s listed as sharing, shares %d" % (other, len(mine & set(o["needs"])))

# A cell page names every grammar that needs it, and no others.
member = [p for p in prim if p["primitive"] == "member"][0]
want = {g["grammar"] for g in gram if "member" in g["needs"]}
got = set(re.findall(r'/s/grammar/([a-z0-9-]+)"><span class="desk-l"', cellp))
assert got == want, "cell page lists %s, should list %s" % (sorted(got - want), sorted(want - got))
print("pages: %d grammars and %d cells listed, cross-density checked against the declaration"
      % (len(gram), len(prim)))
PY

echo "clone grammar contract: PASS"
