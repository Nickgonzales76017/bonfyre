#!/bin/sh
# The addressed atlas enforces its laws rather than describing them:
#   - arrays in different charts are refused as incomparable; a declared
#     bridge makes them comparable and carries its calibration error.
#   - a group's representation is not its members' type (points -> modes and a
#     subspace, histograms -> a mixture, hypervectors -> a bundle), and a group
#     of groups is a hierarchy node with child coordinates and a topology.
#   - roles survive binding: "artist A at venue B" is not "artist B at venue A".
#   - mutating one member changes only that member's group snapshot; the
#     parent is recomputed only beyond the preservation envelope and a hold is
#     recorded inside it.
#   - reopening a group reports members whose projection drifted since its
#     snapshot.
#   - a lens fuses per-chart ranks over an exact eligible set, widens when a
#     strict rule leaves too few, and skips charts the query is not in.
#   - exact joins write typed positives; same words at different addresses are
#     hard negatives.
set -eu
bin=${1:?latent atlas binary required}
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-latent-atlas.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
db="$tmp/atlas.db"

python3 - "$bin" "$tmp" <<'PY' || exit 1
import json, sqlite3, subprocess, sys, random
bin, tmp = sys.argv[1], sys.argv[2]
db = tmp + "/atlas.db"
problems = []
def check(ok, why):
    if not ok: problems.append(why)
def run(*args, ok_codes=(0,)):
    p = subprocess.run([bin] + list(args), capture_output=True, text=True)
    if p.returncode not in ok_codes:
        problems.append("%s exited %d: %s" % (" ".join(args[:1]), p.returncode, p.stderr.strip()[:200]))
    return p
def out(*args, ok_codes=(0,)):
    p = run(*args, ok_codes=ok_codes)
    lines = [l for l in p.stdout.splitlines() if l.strip()]
    return json.loads(lines[-1]) if lines else {}

rng = random.Random(7)
def vec(center, spread, dims=16):
    return [c + rng.uniform(-spread, spread) for c in center]
blues = [1.0 if i < 4 else 0.0 for i in range(16)]
jazz = [1.0 if 4 <= i < 8 else 0.0 for i in range(16)]
live = [1.0 if 8 <= i < 12 else 0.0 for i in range(16)]
lines = [
    {"chart": {"id": "text-hash-16", "family": "dense", "dims": 16, "version": "1", "encoder": "fixture", "universe": "u1"}},
    {"chart": {"id": "text-hash-16b", "family": "dense", "dims": 16, "version": "1", "encoder": "fixture-other", "universe": "u1"}},
    {"chart": {"id": "spectrum-8", "family": "distribution", "dims": 8, "version": "1", "encoder": "fixture", "universe": "u1"}},
    {"chart": {"id": "roles-256", "family": "hdc", "dims": 256, "version": "1", "encoder": "roles", "universe": "u1"}},
]
groups = {"group:blues": blues, "group:jazz": jazz, "group:live": live}
for g, center in groups.items():
    for i in range(6):
        a = "%s:%d" % (g.split(":")[1], i)
        lines.append({"project": {"address": a, "chart": "text-hash-16", "values": vec(center, 0.2), "source_digest": "d-%s-0" % a}})
        lines.append({"project": {"address": a, "chart": "text-hash-16b", "values": vec(center, 0.2), "source_digest": "e-%s-0" % a}})
        hist = [rng.random() + (3 if (k < 4) == (g == "group:blues") else 0) for k in range(8)]
        lines.append({"project": {"address": a, "chart": "spectrum-8", "values": hist, "source_digest": "h-%s-0" % a}})
        lines.append({"relate": {"src": a, "relation": "member_of", "dst": g, "evidence": "fixture collection"}})
        lines.append({"label": {"address": a, "label": "Track %d" % i}})
lines.append({"relate": {"src": "live:0", "relation": "performance_of", "dst": "blues:0", "evidence": "fixture exact join"}})
open(tmp + "/import.jsonl", "w").write("\n".join(json.dumps(l) for l in lines) + "\n")
check(out("import", "--db", db, "--jsonl", tmp + "/import.jsonl").get("imported") == len(lines), "every fixture line imports")

# Laws of comparison
same = out("compare", "--db", db, "--a", "blues:0", "--b", "blues:1", "--chart", "text-hash-16")
far = out("compare", "--db", db, "--a", "blues:0", "--b", "jazz:1", "--chart", "text-hash-16")
check(same.get("distance", 1) < far.get("distance", 0), "members of one family are nearer than across families")
refused = out("compare", "--db", db, "--a", "blues:0", "--b", "blues:1", "--chart", "text-hash-16", "--to-chart", "spectrum-8", ok_codes=(3,))
check(refused.get("refused") == "incomparable", "a dense chart and a distribution chart are refused as incomparable")
unbridged = out("compare", "--db", db, "--a", "blues:0", "--b", "blues:1", "--chart", "text-hash-16", "--to-chart", "text-hash-16b", ok_codes=(3,))
check(unbridged.get("refused") == "incomparable", "two dense charts with no declared bridge are refused")
open(tmp + "/bridge.jsonl", "w").write(json.dumps({"bridge": {"from": "text-hash-16", "to": "text-hash-16b", "operator": "identity-fixture", "calibration_error": 0.04, "loss_envelope": "top-8 recall >= 0.9", "validity_frontier": "u1", "provenance": "fixture"}}) + "\n")
out("import", "--db", db, "--jsonl", tmp + "/bridge.jsonl")
bridged = out("compare", "--db", db, "--a", "blues:0", "--b", "blues:1", "--chart", "text-hash-16", "--to-chart", "text-hash-16b")
check("distance" in bridged and bridged.get("calibration_error") == 0.04, "a declared bridge makes the charts comparable and carries its calibration error")

# Roles survive binding
ab = run("encode-roles", "--roles", "artist=A;venue=B", "--dims", "256").stdout.strip().strip('"')
ba = run("encode-roles", "--roles", "artist=B;venue=A", "--dims", "256").stdout.strip().strip('"')
ab2 = run("encode-roles", "--roles", "venue=B;artist=A", "--dims", "256").stdout.strip().strip('"')
check(ab == ab2 and ab != ba, "role-bound hypervectors keep roles: order-free, but swapping roles is a different situation")

# Groups lift to a different type
reps = {}
for g in groups:
    r = out("group", "--db", db, "--logical", g, "--lens", "collection", "--chart", "text-hash-16", "--universe", "u1")
    reps[g] = r
    check(r.get("member_type") == "dense" and r.get("representation", {}).get("type") == "modes+subspace", "%s lifts dense points to modes and a subspace" % g)
    check(len(r.get("snapshot", "")) == 64 and r.get("members") == 6, "%s is addressed by a snapshot over its exact six members" % g)
mix = out("group", "--db", db, "--logical", "group:blues-spectrum", "--lens", "sound", "--chart", "spectrum-8", "--universe", "u1", ok_codes=(0, 1))
# a mixture needs members of that logical group; relate the blues members to it
open(tmp + "/spec.jsonl", "w").write("\n".join(json.dumps({"relate": {"src": "blues:%d" % i, "relation": "member_of", "dst": "group:blues-spectrum", "evidence": "fixture"}}) for i in range(6)) + "\n")
out("import", "--db", db, "--jsonl", tmp + "/spec.jsonl")
mix = out("group", "--db", db, "--logical", "group:blues-spectrum", "--lens", "sound", "--chart", "spectrum-8", "--universe", "u1")
check(mix.get("representation", {}).get("type") == "mixture", "histograms lift to a mixture")

parent = out("lift", "--db", db, "--logical", "group:music", "--lens", "collection", "--children", "group:blues,group:jazz,group:live", "--universe", "u1")
prep = parent.get("representation", {})
check(prep.get("type") == "hierarchy" and prep.get("children") == 3 and len(prep.get("child_coordinates", [])) == 3, "a group of groups is a hierarchy node over three child snapshots")
check(all(abs(c["poincare"][0]) < 1 and abs(c["poincare"][1]) < 1 for c in prep.get("child_coordinates", [])), "child coordinates lie inside the Poincare ball")
check("topology" in prep and "bridges" in prep["topology"], "the hierarchy node carries a topology signature")

# Mutation touches one branch
nudge = [v + 0.001 for v in vec(blues, 0.0)]
out("mutate", "--db", db, "--address", "blues:3", "--chart", "text-hash-16", "--values", ",".join("%.5f" % v for v in nudge), "--source-digest", "d-blues:3-1")
held = out("refresh", "--db", db, "--universe", "u1", "--envelope", "0.05")
changed = [g["logical"] for g in held.get("groups", [])]
check(changed == ["group:blues"], "only the mutated member's group recomputes (%s)" % changed)
check("group:jazz" in held.get("unchanged", []) and "group:live" in held.get("unchanged", []), "untouched groups keep their snapshots")
check(held.get("parents_recomputed") == [] and "group:music" in held.get("parents_held", []), "a small move holds the parent inside the envelope")
reopened = out("reopen", "--db", db, "--logical", "group:blues")
check(reopened.get("exact") == 6 and reopened.get("drifted") == 0, "the refreshed group reopens six exact members")

far_move = vec(jazz, 0.0)
out("mutate", "--db", db, "--address", "blues:4", "--chart", "text-hash-16", "--values", ",".join("%.5f" % v for v in far_move), "--source-digest", "d-blues:4-1")
stale = out("reopen", "--db", db, "--logical", "group:blues")
check(stale.get("drifted") == 1, "reopening before refresh reports the drifted member")
moved = out("refresh", "--db", db, "--universe", "u1", "--envelope", "0.001")
check([p["logical"] for p in moved.get("parents_recomputed", [])] == ["group:music"], "a move beyond the envelope recomputes the parent")
check([g["logical"] for g in moved.get("groups", [])] == ["group:blues"], "still only the blues branch below it")

# Lens: fusion over an exact eligible set, widening, skipped charts
open(tmp + "/lens.json", "w").write(json.dumps({"query": "jazz:0", "within": "group:jazz", "charts": ["text-hash-16", "spectrum-8", "roles-256"], "fusion": "intersection", "slice": 5, "widen": True}))
lens = out("lens", "--db", db, "--program", tmp + "/lens.json")
check(lens.get("eligible") == 5, "the lens considers only the exact eligible members (%s)" % lens.get("eligible"))
check("roles-256" in lens.get("skipped_charts", []), "a chart the query has no projection in is skipped, not imputed")
check(len(lens.get("slice", [])) == 5, "the slice is filled, widening if the strict rule left too few")

near = out("neighbours", "--db", db, "--address", "live:1", "--chart", "text-hash-16", "--k", "3", "--within", "group:live")
check(len(near.get("neighbours", [])) == 3 and near.get("truth") == "near is not same", "neighbours answer within a group and say near is not same")

anchors = run("anchors", "--db", db)
rows = [json.loads(l) for l in anchors.stdout.splitlines() if l.strip()]
check(any(r["anchor"] == "positive" and r["relation"] == "performance_of" for r in rows), "an exact join becomes a typed positive anchor")
check(any(r["anchor"] == "hard_negative" and r["words"] == "Track 0" for r in rows), "the same words at different addresses become hard negatives")
check(not any(r["anchor"] == "hard_negative" and {r["a"], r["b"]} == {"live:0", "blues:0"} for r in rows), "a typed exact join is never a hard negative")

# Census items: the words chart is kept current without re-encoding unchanged words
items = [{"source": "s1", "key": "k1", "title": "Library hours extended", "body": "Branches open later", "family": "local-voices"},
         {"source": "s1", "key": "k2", "title": "Trail reopens", "body": "", "family": "local-voices"}]
open(tmp + "/items.jsonl", "w").write("\n".join(json.dumps(i) for i in items) + "\n")
first = out("project-items", "--db", db, "--items", tmp + "/items.jsonl")
check(first.get("projected") == 2 and first.get("memberships") == 2, "census items are projected with their family membership (%s)" % first)
again = out("project-items", "--db", db, "--items", tmp + "/items.jsonl")
check(again.get("projected") == 0 and again.get("unchanged") == 2, "unchanged words are not re-encoded")
items[1]["title"] = "Trail reopens between the two cities"
open(tmp + "/items.jsonl", "w").write("\n".join(json.dumps(i) for i in items) + "\n")
changed = out("project-items", "--db", db, "--items", tmp + "/items.jsonl")
check(changed.get("projected") == 1 and changed.get("unchanged") == 1, "only the item whose words changed is re-encoded")
open(tmp + "/links.jsonl", "w").write("\n".join(json.dumps(l) for l in [
    {"relate": {"src": "o/s1/k2", "relation": "mentions", "dst": "qid:Q16558", "evidence": "linker: label 'Fort Worth'"}},
    {"relate": {"src": "o/s1/k1", "relation": "mentions", "dst": "qid:Q16557", "evidence": "a person said so"}}]) + "\n")
out("import", "--db", db, "--jsonl", tmp + "/links.jsonl")
out("forget", "--db", db, "--relation", "mentions", "--evidence-prefix", "linker:")
kept = sqlite3.connect(db).execute("SELECT src FROM relation WHERE relation='mentions'").fetchall()
check(kept == [("o/s1/k1",)], "a linker forgets only the relations it wrote before writing them again (%s)" % kept)

# An atlas written before a relation type existed keeps its relations and
# accepts the new type: the type list is a CHECK constraint, so the table is
# rebuilt rather than the rows discarded.
old = tmp + "/before-happens-at.db"
con = sqlite3.connect(old)
con.execute("CREATE TABLE relation(src TEXT NOT NULL, relation TEXT NOT NULL CHECK(relation IN ('same_subject_as','mentions')),"
            " dst TEXT NOT NULL, evidence TEXT NOT NULL, PRIMARY KEY(src,relation,dst))")
con.execute("INSERT INTO relation VALUES('o/a/1','mentions','qid:Q1','written before the migration')")
con.commit(); con.close()
open(tmp + "/new-relation.jsonl", "w").write(json.dumps(
    {"relate": {"src": "o/a/2", "relation": "happens_at", "dst": "qid:Q2", "evidence": "linker: the organizer's own place line"}}) + "\n")
migrated = out("import", "--db", old, "--jsonl", tmp + "/new-relation.jsonl")
con = sqlite3.connect(old)
check(migrated.get("imported") == 1 and migrated.get("refused") == 0, "an older atlas accepts a relation type added since (%s)" % migrated)
check(con.execute("SELECT COUNT(*) FROM relation WHERE evidence='written before the migration'").fetchone()[0] == 1,
      "and keeps every relation it already held")
con.close()

if problems:
    print("latent atlas: FAIL")
    for p in problems: print("  -", p)
    sys.exit(1)
PY
echo 'latent atlas: PASS'
