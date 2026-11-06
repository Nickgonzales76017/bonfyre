#!/bin/sh
# The generalization, held to its own laws.
#
# What is proved here is not that facets compile -- that is arithmetic -- but
# that they compile from phenotype rather than domain, that the finite
# vocabularies are the ones declared in CUE, that nothing climbs past the first
# rung of the maturity ladder, and that density is measured as shape.
set -eu
bin=$(cd "$(dirname -- "$1")" && pwd)/$(basename -- "$1")
root=$(cd "$(dirname -- "$0")/../../.." && pwd)
corpus=$root/estate/generated/external-estate-v4/observed-external-estate.jsonl
schema=$root/schemas/control/protocol-facet.cue
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

j() { python3 -c 'import json,sys;d=json.load(sys.stdin);print(eval(sys.argv[1],{"d":d}))' "$1"; }

[ -f "$corpus" ]

# Unknown verbs are rejected by the command index before any corpus read.
if "$bin" unknown --corpus "$tmp/missing.jsonl" >"$tmp/unknown.txt" 2>&1; then
    echo "unknown facet verb accepted" >&2
    exit 1
fi
! grep -q 'corpus carried no occurrences' "$tmp/unknown.txt"

# --- the vocabularies are the declared ones, not a second copy ------------
# Two independently maintained lists of what a relation may be is how a finite
# vocabulary stops being finite.
"$bin" vocabulary > "$tmp/vocab.txt"
awk '$1=="relation"{print $2}' "$tmp/vocab.txt" | sort > "$tmp/native-relations.txt"
awk '$1=="phenotype"{print $2}' "$tmp/vocab.txt" | sort > "$tmp/native-phenotypes.txt"
sed -n '/^#RelationKind:/,/^$/p' "$schema" | tr '|' '\n' | sed 's/^#RelationKind://' \
  | tr -d ' \t"' | grep -v '^$' | grep -v '^//' | sort > "$tmp/cue-relations.txt"
sed -n '/^#Phenotype:/,/^$/p' "$schema" | tr '|' '\n' | sed 's/^#Phenotype://' \
  | tr -d ' \t"' | grep -v '^$' | grep -v '^//' | sort > "$tmp/cue-phenotypes.txt"
cmp -s "$tmp/native-relations.txt" "$tmp/cue-relations.txt"
cmp -s "$tmp/native-phenotypes.txt" "$tmp/cue-phenotypes.txt"
[ "$(wc -l < "$tmp/cue-relations.txt" | tr -d ' ')" = "25" ]
[ "$(wc -l < "$tmp/cue-phenotypes.txt" | tr -d ' ')" = "15" ]

# AddressPlane coordinates for a phenotype are affordances, never protocol
# names: `supports_gtfs` is the taxonomy swamp this avoids.
! grep -qiE 'gtfs|fhir|opcua|fdsn|quakeml' "$tmp/vocab.txt"
grep -q '^phenotype LIVE_ENTITY .*temporal:live' "$tmp/vocab.txt"
grep -q '^phenotype ECONOMIC_OFFER .*effect:financial' "$tmp/vocab.txt"
grep -q '^phenotype COMMANDABLE_RESOURCE .*effect:physical' "$tmp/vocab.txt"

# --- a braid input declares absence rather than inventing a default -------
"$bin" braid-input --corpus "$corpus" --occurrence external:ext-02176 > "$tmp/input.json"
[ "$(j 'd["occurrence_id"]' < "$tmp/input.json")" = "external:ext-02176" ]
[ "$(j 'len(d["phenotypes"]) > 0' < "$tmp/input.json")" = "True" ]
# No actor context means no actor-scoped facet may be compiled, rather than one
# compiled for a generic actor who does not exist.
[ "$(j '"actor_context" in d["absent"]' < "$tmp/input.json")" = "True" ]
[ "$(j '"authority_cut" in d["absent"]' < "$tmp/input.json")" = "True" ]
[ "$(j '"rights_profile" in d["absent"]' < "$tmp/input.json")" = "True" ]
# Every phenotype it claims is in the declared set.
[ "$(j 'all(p in open("'"$tmp"'/cue-phenotypes.txt").read().split() for p in d["phenotypes"])' < "$tmp/input.json")" = "True" ]

# --- facets compile from phenotype, and stop at the first rung ------------
"$bin" compile --corpus "$corpus" --occurrence external:ext-02176 > "$tmp/facets.json"
[ "$(j 'd["facet_count"] > 0' < "$tmp/facets.json")" = "True" ]
# Nothing climbs the ladder here. A candidate match is not a compiled contract,
# not an enactment, not closed obligations, and not an authorized effect.
[ "$(j 'set(f["maturity"] for f in d["facets"]) == {"candidate_match"}' < "$tmp/facets.json")" = "True" ]
# Every relation claimed is in the finite set.
[ "$(j 'all(f["relation_kind"] in open("'"$tmp"'/cue-relations.txt").read().split() for f in d["facets"])' < "$tmp/facets.json")" = "True" ]
# A facet defers to the owners of the facts it touches rather than acquiring them.
[ "$(j 'all("ForeignTwin" in f["defers_to"] and "AuthorityGraph" in f["defers_to"] for f in d["facets"])' < "$tmp/facets.json")" = "True" ]
# And it names what would invalidate it.
[ "$(j 'all(f["invalidated_by"] for f in d["facets"])' < "$tmp/facets.json")" = "True" ]
# A facet only exists where a protocol offers a relation the phenotype can
# carry: an earthquake feed is not commanded, whatever a protocol offers.
[ "$(j '"COMMAND" in set(f["relation_kind"] for f in d["facets"])' < "$tmp/facets.json")" = "False" ]
# Every facet validates against the declared contract.
python3 - "$tmp/facets.json" "$tmp/one-facet.json" <<'PYX'
import json, sys
d = json.load(open(sys.argv[1]))
f = dict(d["facets"][0])
# protocol_occurrence is this compiler's own cross-reference and is not part
# of the declared contract; everything else must validate as declared.
f.pop("protocol_occurrence", None)
json.dump(f, open(sys.argv[2], "w"))
PYX
cue vet "$schema" "$tmp/one-facet.json" -d '#ProtocolFacetContract'

# --- full depth across the estate ----------------------------------------
"$bin" estate --corpus "$corpus" > "$tmp/estate.json"
[ "$(j 'd["occurrences"]' < "$tmp/estate.json")" = "4891" ]
[ "$(j 'd["protocol_occurrences"]' < "$tmp/estate.json")" = "739" ]
[ "$(j 'd["candidate_facets"] > 1000000' < "$tmp/estate.json")" = "True" ]
# Every phenotype in the vocabulary is accounted for, including the ones this
# corpus carries no evidence for -- a missing key would hide the gap.
[ "$(j 'len(d["phenotype_distribution"]) == 15' < "$tmp/estate.json")" = "True" ]
[ "$(j 'len(d["relation_distribution"]) == 25' < "$tmp/estate.json")" = "True" ]
# The report says what it is not.
[ "$(j '"a candidate facet -> a compiled contract" in d["cannot_infer"]' < "$tmp/estate.json")" = "True" ]

# --- density is shape, not length ----------------------------------------
# A chain of six owners and a braid of six owners are distinguished, and the
# longest path is the measure that cannot tell them apart.
python3 - "$tmp/chain.jsonl" "$tmp/braid.jsonl" <<'PYD'
import json, sys
chain = [("A","B"),("B","C"),("C","D"),("D","E"),("E","F")]
braid = [("A","B"),("B","C"),("B","D"),("B","E"),("C","F"),("D","F"),("E","F")]
for path, edges in ((sys.argv[1], chain), (sys.argv[2], braid)):
    with open(path, "w") as fh:
        for f, t in edges:
            fh.write(json.dumps({"occurrence_id": "x", "axis": "semantic",
                                 "state": "interpreted", "from_owner": f,
                                 "to_owner": t}) + "\n")
PYD
"$bin" density --evidence "$tmp/chain.jsonl" --occurrence x > "$tmp/chain.json"
"$bin" density --evidence "$tmp/braid.jsonl" --occurrence x > "$tmp/braid.json"
[ "$(j 'd["branch_points"]' < "$tmp/chain.json")" = "0" ]
[ "$(j 'd["rejoin_points"]' < "$tmp/chain.json")" = "0" ]
[ "$(j 'd["braid_excess"]' < "$tmp/chain.json")" = "0" ]
[ "$(j 'd["branch_points"]' < "$tmp/braid.json")" = "1" ]
[ "$(j 'd["rejoin_points"]' < "$tmp/braid.json")" = "1" ]
[ "$(j 'd["braid_excess"]' < "$tmp/braid.json")" = "2" ]
# Both report the whole declared structure, zeros included, so an unmeasured
# transition is visible rather than looking like a design decision.
for f in "$tmp/chain.json" "$tmp/braid.json"; do
  [ "$(j 'all(k in d for k in ("protocol_contracts_enacted","obligations_closed","commitments","effects","compensations","faithful_roundtrips","drifted_roundtrips","invalidation_fanout","recompute_cut_size","rights_edges","authority_edges"))' < "$f")" = "True" ]
done
[ "$(j 'd["effects"]' < "$tmp/braid.json")" = "0" ]
python3 - "$tmp/braid.json" "$tmp/density-only.json" <<'PYE'
import json, sys
d = json.load(open(sys.argv[1]))
keep = ("distinct_owners","owner_transition_count","longest_owner_path","branch_points",
        "rejoin_points","protocol_contracts_compiled","protocol_contracts_enacted",
        "obligations_closed","semantic_profiles_used","exact_owner_queries",
        "representations_used","carrier_families_used","evidence_edges","rights_edges",
        "authority_edges","invalidation_fanout","recompute_cut_size","human_decisions",
        "machine_decisions","commitments","effects","compensations","roundtrips",
        "faithful_roundtrips","drifted_roundtrips")
json.dump({k: d[k] for k in keep}, open(sys.argv[2], "w"))
PYE
cue vet "$schema" "$tmp/density-only.json" -d '#RealizedBraidDensity'

"$bin" density --evidence "$root/cmd/BonfyreFacet/tests/density_evidence_metrics.jsonl" --occurrence source:a > "$tmp/evidence-metrics.json"
[ "$(j 'd["exact_owner_queries"]' < "$tmp/evidence-metrics.json")" = "1" ]
[ "$(j 'd["representations_used"]' < "$tmp/evidence-metrics.json")" = "1" ]
[ "$(j 'd["carrier_families_used"]' < "$tmp/evidence-metrics.json")" = "1" ]


# --- a corpus it cannot read is a failure, not an empty estate -----------
printf 'not json\n' > "$tmp/broken.jsonl"
! "$bin" estate --corpus "$tmp/broken.jsonl" 2>/dev/null
! "$bin" compile --corpus "$corpus" --occurrence external:ext-99999 2>/dev/null

# --- every phenotype populates, and the tiers are distinguishable ---------
# Affordances and roles alone left five phenotypes nearly empty and one at zero
# while the estate plainly carried the things they name. Domain evidence is
# weaker and says so, which is why both tiers are reported rather than merged.
[ "$(j 'min(d["phenotype_distribution"].values()) > 0' < "$tmp/estate.json")" = "True" ]
[ "$(j 'min(d["relation_distribution"].values()) > 0' < "$tmp/estate.json")" = "True" ]
[ "$(j 'len(d["phenotype_from_shape_alone"]) == 15' < "$tmp/estate.json")" = "True" ]
# SUPPLY_CHAIN_OBJECT is carried by domain evidence and by no shape evidence at
# all; a merged report would hide exactly that.
[ "$(j 'd["phenotype_from_shape_alone"]["SUPPLY_CHAIN_OBJECT"] == 0' < "$tmp/estate.json")" = "True" ]
[ "$(j 'd["phenotype_distribution"]["SUPPLY_CHAIN_OBJECT"] > 0' < "$tmp/estate.json")" = "True" ]
# Every occurrence's shape tier is a subset of its total.
"$bin" braid-input --corpus "$corpus" --occurrence external:ext-02176 > "$tmp/input.json"
[ "$(j 'set(d["phenotypes_from_shape"]) <= set(d["phenotypes"])' < "$tmp/input.json")" = "True" ]
[ "$(j 'set(d["phenotypes_from_domain"]) <= set(d["phenotypes"])' < "$tmp/input.json")" = "True" ]

# --- a facet names its carriers and what actually invalidates it -----------
# A facet that says a relation is possible but not over what is half an answer,
# and density is only affordable if invalidation is precise: more projections
# must not mean more recomputation.
"$bin" compile --corpus "$corpus" --occurrence external:ext-00439 > "$tmp/mobility.json"
[ "$(j 'any(f["via_phenotype"] == "LIVE_ENTITY" for f in d["facets"])' < "$tmp/mobility.json")" = "True" ]
[ "$(j '[f for f in d["facets"] if f["via_phenotype"]=="LIVE_ENTITY"][0]["carrier_candidates"]' < "$tmp/mobility.json")" \
  = "['GTFS-RT', 'SIRI', 'FIXM', 'AIS', 'MOQT', 'MQTT']" ]
# A moving thing is invalidated by moving, and not by a price changing.
[ "$(j '"position:changed" in [f for f in d["facets"] if f["via_phenotype"]=="LIVE_ENTITY"][0]["invalidated_by"]' < "$tmp/mobility.json")" = "True" ]
[ "$(j '"price:changed" in [f for f in d["facets"] if f["via_phenotype"]=="LIVE_ENTITY"][0]["invalidated_by"]' < "$tmp/mobility.json")" = "False" ]
# Every facet names the phenotype that admitted it and how that phenotype was
# derived; a facet that cannot is a domain match wearing a phenotype label.
[ "$(j 'all(f["via_phenotype"] and f["phenotype_derivation"] in ("shape","domain") for f in d["facets"])' < "$tmp/mobility.json")" = "True" ]
[ "$(j 'all(f["via_phenotype"] in open("'"$tmp"'/cue-phenotypes.txt").read().split() for f in d["facets"])' < "$tmp/mobility.json")" = "True" ]
# The relation a facet claims is one its phenotype can carry.
[ "$(j 'all(not (f["via_phenotype"]=="LIVE_ENTITY" and f["relation_kind"]=="SETTLE") for f in d["facets"])' < "$tmp/mobility.json")" = "True" ]

# --- acceptance is the whole machine, not one nucleus ---------------------
# A compiler demonstrated on one earthquake feed is a compiler demonstrated on
# one earthquake feed, and picking a favourable specimen per domain is the same
# mistake with more steps. Every domain the corpus carries is reported, so one
# that compiles nothing would be visible here.
"$bin" domains --corpus "$corpus" > "$tmp/domains.json"
[ "$(j 'd["domain_count"] > 50' < "$tmp/domains.json")" = "True" ]
[ "$(j 'all(r["candidate_facets"] > 0 for r in d["domains"])' < "$tmp/domains.json")" = "True" ]
[ "$(j 'all(r["occurrences"] > 0 for r in d["domains"])' < "$tmp/domains.json")" = "True" ]
# The domains the brief names across the machine all compile.
for want in vehicles energy manufacturing medicine finance media research buildings law food telecommunications computers retail; do
  [ "$(j 'any(r["domain"]=="'"$want"'" and r["candidate_facets"]>0 for r in d["domains"])' < "$tmp/domains.json")" = "True" ]
done
# The larger domains exercise the whole vocabulary rather than a corner of it.
[ "$(j 'max(r["relations_exercised"] for r in d["domains"])' < "$tmp/domains.json")" = "25" ]
[ "$(j 'max(r["phenotypes_reached"] for r in d["domains"])' < "$tmp/domains.json")" = "15" ]
# And it still refuses to be read as integration.
[ "$(j '"a domain compiling facets -> that domain being integrated" in d["cannot_infer"]' < "$tmp/domains.json")" = "True" ]

# --- the next rung: candidate_match -> contract_compiled -----------------
# Promotion is real only because three things are checked rather than asserted:
# the facet is one this compiler would emit, the source was actually observed,
# and the native ProtocolContract owner did the registering.
body="$tmp/observed.txt"
printf 'observed body\n' > "$body"
digest=$(shasum -a 256 "$body" | awk '{print $1}')
python3 - "$body" "$digest" "$tmp/receipt.json" external:ext-02176 <<'PYR'
import json, sys
json.dump({"body_path": sys.argv[1], "content_sha256": sys.argv[2],
           "twin_id": sys.argv[4], "source_url": "https://example.test/src",
           "observed_at": "2026-09-12T22:00:00Z"}, open(sys.argv[3], "w"))
PYR
pick=$("$bin" compile --corpus "$corpus" --occurrence external:ext-02176 \
       | python3 -c 'import sys,json;f=json.load(sys.stdin)["facets"][0];print(f["protocol_occurrence"],f["relation_kind"])')
proto=$(echo "$pick" | awk '{print $1}')
rel=$(echo "$pick" | awk '{print $2}')
"$bin" contract --corpus "$corpus" --occurrence external:ext-02176 --protocol "$proto" \
  --relation "$rel" --receipt "$tmp/receipt.json" --out "$tmp/rung.jsonl" > "$tmp/rung.json"
[ "$(j 'd["programs"] >= 1' < "$tmp/rung.json")" = "True" ]
[ "$(j 'd["registered_by"]' < "$tmp/rung.json")" = "bf_protocol_derivation_register" ]
[ "$(j 'd["truth"]' < "$tmp/rung.json")" = "contract_compiled_not_enacted_not_closed_not_authorized" ]
# Three bindings: observed domain, facet-to-contract handoff, then compiled protocol.
[ "$(wc -l < "$tmp/rung.jsonl" | tr -d ' ')" = "3" ]
[ "$(head -1 "$tmp/rung.jsonl" | j 'd["schema"]')" = "bonfyre-domain-binding.v1" ]
[ "$(head -1 "$tmp/rung.jsonl" | j 'd["state"]')" = "profile_bound" ]
[ "$(sed -n '2p' "$tmp/rung.jsonl" | j 'd["state"]')" = "contract_bound" ]
[ "$(sed -n '2p' "$tmp/rung.jsonl" | j 'd["from_owner"]')" = "ProtocolFacetCompiler" ]
[ "$(sed -n '2p' "$tmp/rung.jsonl" | j 'd["to_owner"]')" = "ProtocolContract" ]
[ "$(tail -1 "$tmp/rung.jsonl" | j 'd["schema"]')" = "bonfyre-protocol-binding.v1" ]
[ "$(tail -1 "$tmp/rung.jsonl" | j 'd["state"]')" = "compiled" ]
# The digest chain: the protocol binding carries the domain binding's digest and
# the domain binding carries the receipt's. Remove a link and it stops verifying.
[ "$(head -1 "$tmp/rung.jsonl" | j 'd["foreign_receipt_sha256"]')" = "$digest" ]
[ "$(python3 -c "
import json
a=json.loads(open('$tmp/rung.jsonl').readlines()[0])
b=json.loads(open('$tmp/rung.jsonl').readlines()[2])
print(a['semantic_record_sha256'] == b['semantic_record_sha256'])")" = "True" ]
for f in foreign_receipt_sha256 semantic_record_sha256; do
  [ "$(head -1 "$tmp/rung.jsonl" | j 'len(d["'"$f"'"]) == 64')" = "True" ]
done
for f in semantic_record_sha256 domain_binding_sha256 native_registration_receipt_sha256; do
  [ "$(tail -1 "$tmp/rung.jsonl" | j 'len(d["'"$f"'"]) == 64')" = "True" ]
done

sed -n '1p;3p' "$tmp/rung.jsonl" > "$tmp/legacy-rung.jsonl"
"$bin" reconcile-contract-joins --corpus "$corpus" --evidence "$tmp/legacy-rung.jsonl" --out "$tmp/rejoined-rung.jsonl" > "$tmp/reconcile.json"
[ "$(j 'd["compiled_rows"]' < "$tmp/reconcile.json")" = "1" ]
[ "$(j 'd["repaired"]' < "$tmp/reconcile.json")" = "1" ]
[ "$(j 'd["refused"]' < "$tmp/reconcile.json")" = "0" ]

# --- and the three refusals that make it mean something ------------------
# A facet the compiler would not emit cannot be promoted.
! "$bin" contract --corpus "$corpus" --occurrence external:ext-02176 --protocol "$proto" \
  --relation COMMAND --receipt "$tmp/receipt.json" 2>/dev/null
# A receipt observing a different occurrence may not be borrowed.
python3 - "$body" "$digest" "$tmp/wrong.json" external:ext-00003 <<'PYR'
import json, sys
json.dump({"body_path": sys.argv[1], "content_sha256": sys.argv[2],
           "twin_id": sys.argv[4], "observed_at": "2026-09-12T22:00:00Z"}, open(sys.argv[3], "w"))
PYR
! "$bin" contract --corpus "$corpus" --occurrence external:ext-02176 --protocol "$proto" \
  --relation "$rel" --receipt "$tmp/wrong.json" 2>/dev/null
# Bytes that have drifted from their receipt compile nothing.
printf 'x' >> "$body"
! "$bin" contract --corpus "$corpus" --occurrence external:ext-02176 --protocol "$proto" \
  --relation "$rel" --receipt "$tmp/receipt.json" 2>/dev/null

# --- the rung is not one specimen ----------------------------------------
# A compiled contract that only ever names one standard is a specimen. The
# contract name is derived from the protocol and the relation, so a different
# occurrence in a different domain compiles a differently named contract
# through the same code path.
printf 'another observed body\n' > "$tmp/body2.txt"
digest2=$(shasum -a 256 "$tmp/body2.txt" | awk '{print $1}')
python3 - "$tmp/body2.txt" "$digest2" "$tmp/receipt2.json" external:ext-00439 <<'PYR'
import json, sys
json.dump({"body_path": sys.argv[1], "content_sha256": sys.argv[2],
           "twin_id": sys.argv[4], "observed_at": "2026-09-12T22:00:00Z"}, open(sys.argv[3], "w"))
PYR
pick2=$("$bin" compile --corpus "$corpus" --occurrence external:ext-00439 \
        | python3 -c 'import sys,json;f=json.load(sys.stdin)["facets"][0];print(f["protocol_occurrence"],f["relation_kind"])')
# This occurrence admits its relation through domain evidence, so the weaker
# basis has to be named. That is the gate working, not an inconvenience.
"$bin" contract --corpus "$corpus" --occurrence external:ext-00439 \
  --protocol "$(echo "$pick2" | awk '{print $1}')" --relation "$(echo "$pick2" | awk '{print $2}')" \
  --receipt "$tmp/receipt2.json" --accept-domain-evidence \
  --out "$tmp/rung2.jsonl" > "$tmp/rung2.json"
[ "$(j 'd["compiled"]' < "$tmp/rung2.json")" != "" ]
# Each binding names the phenotype that admitted it and the tier it came from.
[ "$(head -1 "$tmp/rung2.jsonl" | j 'd["phenotype_derivation"] in ("shape","domain")')" = "True" ]

# --- a contract's shape follows its relation -----------------------------
# Describer/compiler/reader is lawful for a read and wrong for everything that
# changes something: a COMMAND contract with no authority-bearing role can be
# enacted by anyone who can reach it.
"$bin" compile --corpus "$corpus" --occurrence external:ext-00006 > "$tmp/shape.json"
[ "$(j 'any("authority_requirement" in f for f in d["facets"])' < "$tmp/shape.json")" = "True" ]
[ "$(j 'any("authority_requirement" not in f for f in d["facets"])' < "$tmp/shape.json")" = "True" ]
# A read carries no authority requirement; a publication does.
[ "$(j 'all("authority_requirement" not in f for f in d["facets"] if f["relation_kind"] in ("QUERY","DESCRIBE","OBSERVE","IDENTIFY","FILTER"))' < "$tmp/shape.json")" = "True" ]
[ "$(j 'all("authority_requirement" in f for f in d["facets"] if f["relation_kind"] in ("COMMAND","TASK","COMMIT","PAY","SETTLE","PUBLISH","AUTHORIZE","TRANSFER","FEDERATE"))' < "$tmp/shape.json")" = "True" ]
# Every facet names the representations its phenotype is carried in.
[ "$(j 'all(f["representation_requirements"] for f in d["facets"])' < "$tmp/shape.json")" = "True" ]
[ "$(j 'all(f["information_policy_requirement"] for f in d["facets"])' < "$tmp/shape.json")" = "True" ]

# --- domain evidence may carry a candidate and not a contract ------------
# A legal API in the `law` domain is a service capability that happens to be
# about documents. A contract asserts a shape for this source, not for sources
# like it, so the weaker basis is refused unless it is named.
dom_pick=$("$bin" braid-input --corpus "$corpus" --occurrence external:ext-00439 \
  | python3 -c 'import sys,json;d=json.load(sys.stdin);print(",".join(set(d["phenotypes"])-set(d["phenotypes_from_shape"])))')
[ -n "$dom_pick" ]
printf 'body\n' > "$tmp/dbody.txt"
ddig=$(shasum -a 256 "$tmp/dbody.txt" | awk '{print $1}')
python3 - "$tmp/dbody.txt" "$ddig" "$tmp/dreceipt.json" external:ext-00439 <<'PYR'
import json, sys
json.dump({"body_path": sys.argv[1], "content_sha256": sys.argv[2],
           "twin_id": sys.argv[4], "observed_at": "2026-09-12T22:00:00Z"}, open(sys.argv[3], "w"))
PYR
# The relation has to be one that only a domain-tier phenotype admits, which is
# computed rather than guessed: a relation some shape phenotype also admits
# would pass the gate legitimately and prove nothing.
"$bin" vocabulary > "$tmp/vocab2.txt"
"$bin" compile --corpus "$corpus" --occurrence external:ext-00439 > "$tmp/dfacets.json"
dom_relation=$(python3 - "$tmp/dfacets.json" <<'PYD'
import json, sys
d = json.load(open(sys.argv[1]))
by = {}
for f in d["facets"]:
    by.setdefault(f["relation_kind"], set()).add(f["phenotype_derivation"])
for rel, tiers in sorted(by.items()):
    if tiers == {"domain"}:
        print(rel)
        break
PYD
)
if [ -n "$dom_relation" ]; then
  dom_proto=$(python3 - "$tmp/dfacets.json" "$dom_relation" <<'PYP'
import json, sys
d = json.load(open(sys.argv[1]))
for f in d["facets"]:
    if f["relation_kind"] == sys.argv[2]:
        print(f["protocol_occurrence"]); break
PYP
)
  ! "$bin" contract --corpus "$corpus" --occurrence external:ext-00439 --protocol "$dom_proto" \
    --relation "$dom_relation" --receipt "$tmp/dreceipt.json" > /dev/null 2>&1
  # Named explicitly, it compiles and the binding records the weaker basis.
  "$bin" contract --corpus "$corpus" --occurrence external:ext-00439 --protocol "$dom_proto" \
    --relation "$dom_relation" --receipt "$tmp/dreceipt.json" --accept-domain-evidence \
    --out "$tmp/domain.jsonl" > /dev/null
  [ "$(head -1 "$tmp/domain.jsonl" | j 'd["phenotype_derivation"]')" = "domain" ]
fi

# --- enact, authorize, close: each by the owner that owns it -------------
# Authority is not granted by asking for it.
enact_pick=$("$bin" compile --corpus "$corpus" --occurrence external:ext-00006 \
  | python3 -c 'import sys,json
d=json.load(sys.stdin)
c=[f for f in d["facets"] if f["relation_kind"]=="COMMIT"]
print(c[0]["protocol_occurrence"] if c else "")')
[ -n "$enact_pick" ]
"$bin" enact --corpus "$corpus" --occurrence external:ext-00006 --protocol "$enact_pick" \
  --relation COMMIT --db "$tmp/enact.db" --actor agent:test --subject subject:t \
  --at 2026-09-12T22:00:00Z > "$tmp/refused.json" 2>/dev/null && exit 1
[ "$(j 'd["state"]' < "$tmp/refused.json")" = "refused" ]
[ "$(j 'd["refused_role"]' < "$tmp/refused.json")" = "committer" ]
[ "$(j 'd["missing_permission"]' < "$tmp/refused.json")" = "commit:offer" ]

# Granted, the native owner enacts and opens the obligation the relation creates.
rm -f "$tmp/enact.db"
"$bin" enact --corpus "$corpus" --occurrence external:ext-00006 --protocol "$enact_pick" \
  --relation COMMIT --db "$tmp/enact.db" --actor agent:test --subject subject:t \
  --at 2026-09-12T22:00:00Z --grant-authority --out "$tmp/enacted.jsonl" > "$tmp/enacted.json"
[ "$(j 'd["authorized"]' < "$tmp/enacted.json")" = "True" ]
[ "$(j 'd["obligations_opened"]' < "$tmp/enacted.json")" = "1" ]
[ "$(j 'd["enacted_by"]' < "$tmp/enacted.json")" = "bf_protocol_contract_enact_authorized" ]
# Neither axis is rounded up: an open obligation and unevidenced claims are
# reported as outstanding rather than as closed.
[ "$(j 'd["state"]' < "$tmp/enacted.json")" = "outstanding" ]
[ "$(j 'd["obligations_terminal"]' < "$tmp/enacted.json")" = "False" ]
[ "$(j 'd["evidence_closed"]' < "$tmp/enacted.json")" = "False" ]
# Three rows, one per owner that acted.
[ "$(grep -c '"state":"authorized"' "$tmp/enacted.jsonl")" = "1" ]
[ "$(grep -c '"state":"enacted"' "$tmp/enacted.jsonl")" = "1" ]

# Evidence alone does not close it, and discharge alone does not either.
rm -f "$tmp/e-ev.db" "$tmp/e-dis.db"
"$bin" enact --corpus "$corpus" --occurrence external:ext-00006 --protocol "$enact_pick" \
  --relation COMMIT --db "$tmp/e-ev.db" --actor agent:test --subject subject:t \
  --at 2026-09-12T22:00:00Z --grant-authority --evidence-stands > "$tmp/ev-only.json"
[ "$(j 'd["evidence_closed"]' < "$tmp/ev-only.json")" = "True" ]
[ "$(j 'd["obligations_terminal"]' < "$tmp/ev-only.json")" = "False" ]
[ "$(j 'd["state"]' < "$tmp/ev-only.json")" = "outstanding" ]
"$bin" enact --corpus "$corpus" --occurrence external:ext-00006 --protocol "$enact_pick" \
  --relation COMMIT --db "$tmp/e-dis.db" --actor agent:test --subject subject:t \
  --at 2026-09-12T22:00:00Z --grant-authority --discharge > "$tmp/dis-only.json"
[ "$(j 'd["obligations_terminal"]' < "$tmp/dis-only.json")" = "True" ]
[ "$(j 'd["evidence_closed"]' < "$tmp/dis-only.json")" = "False" ]
[ "$(j 'd["state"]' < "$tmp/dis-only.json")" = "outstanding" ]
# Both, and only both, close it.
rm -f "$tmp/e-both.db"
"$bin" enact --corpus "$corpus" --occurrence external:ext-00006 --protocol "$enact_pick" \
  --relation COMMIT --db "$tmp/e-both.db" --actor agent:test --subject subject:t \
  --at 2026-09-12T22:00:00Z --grant-authority --evidence-stands --discharge > "$tmp/closed.json"
[ "$(j 'd["state"]' < "$tmp/closed.json")" = "obligations_closed" ]
[ "$(j 'd["closed_by"]' < "$tmp/closed.json")" = "bf_protocol_contract_close" ]

# --- interpretation is not observation ----------------------------------
# 4,881 occurrences sat at depth zero, and the reason was never that nothing
# had happened to them: this compiler read every one. What has not happened is
# observation, and those are different axes.
"$bin" interpret --corpus "$corpus" --out "$tmp/interp.jsonl" 2>/dev/null
[ "$(wc -l < "$tmp/interp.jsonl" | tr -d ' ')" -gt 4800 ]
[ "$(head -1 "$tmp/interp.jsonl" | j 'd["axis"]')" = "semantic" ]
[ "$(head -1 "$tmp/interp.jsonl" | j 'd["state"]')" = "interpreted" ]
[ "$(head -1 "$tmp/interp.jsonl" | j 'd["from_owner"]')" = "ExternalEstate" ]
[ "$(head -1 "$tmp/interp.jsonl" | j 'd["truth"]')" = "a_description_was_typed_not_a_source_observed" ]
# It claims the weakest semantic rung and never profile_bound, which needs a
# DomainProfile nobody has bound.
! grep -q '"state":"profile_bound"' "$tmp/interp.jsonl"
# And it never claims the observation axis.
! grep -q '"axis":"observation"' "$tmp/interp.jsonl"
# An occurrence with no phenotype is left alone rather than given an empty one.
[ "$(python3 -c "
import json
n=sum(1 for l in open('$tmp/interp.jsonl') if not json.loads(l)['phenotypes'])
print(n)")" = "0" ]

# --- planning an observation is not performing one -----------------------
# Everything above the observation axis was blocked on nothing having looked at
# 4,881 occurrences. This compiler says which are worth a request; ForeignTwin
# decides robots and writes the receipt.
"$bin" observe-plan --corpus "$corpus" > "$tmp/plan.tsv" 2>"$tmp/plan.err"
[ "$(head -1 "$tmp/plan.tsv" | cut -c1-1)" = "#" ]
[ "$(grep -vc '^#' "$tmp/plan.tsv")" -gt 4000 ]
grep -q 'nothing was fetched and robots still decides at request time' "$tmp/plan.err"
# Grouped by host, so a driver can pace itself without sorting.
[ "$(grep -v '^#' "$tmp/plan.tsv" | cut -f3 | python3 -c "
import sys
hosts=[l.strip() for l in sys.stdin if l.strip()]
print('True' if hosts == sorted(hosts) else 'False')")" = "True" ]
# Every row carries a real http(s) address and a phenotype worth the request.
[ "$(grep -v '^#' "$tmp/plan.tsv" | cut -f2 | grep -cv '^http')" = "0" ]
# Exactly the phenotype-less occurrences carry no facets, and they are still
# planned: an occurrence this compiler cannot type is not an occurrence not
# worth observing. They sort last, which is the wrong end for the ones that
# would gain most, and that limitation is recorded rather than papered over.
[ "$(grep -v '^#' "$tmp/plan.tsv" | awk -F'\t' '$4 <= 0' | wc -l | tr -d ' ')" = "11" ]
[ "$(grep -v '^#' "$tmp/plan.tsv" | awk -F'\t' '$4 <= 0 && $5 != ""' | wc -l | tr -d ' ')" = "0" ]
# A limit truncates the plan rather than sampling it, so a partial sweep is a
# prefix of the full one and is reproducible.
"$bin" observe-plan --corpus "$corpus" --limit 5 2>/dev/null > "$tmp/plan5.tsv"
[ "$(grep -vc '^#' "$tmp/plan5.tsv")" = "5" ]
[ "$(head -6 "$tmp/plan.tsv" | tail -5 | cksum)" = "$(tail -5 "$tmp/plan5.tsv" | cksum)" ]

echo "facet compiler: ok"
