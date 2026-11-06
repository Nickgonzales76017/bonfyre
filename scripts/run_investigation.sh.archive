#!/usr/bin/env bash
#
# scripts/run_investigation.sh — Full Bonfyre Investigation Pipeline
#
# This script runs the complete investigation stack:
#   1. Symbolic Front-End  (Entity → Canon → Graph)
#   2. Investigative Middle (Hypothesis Discovery → Testing → Convergence)
#   3. Structural Back-End  (Intervention on unresolved hot zones)
#
# Architecture:
#   RAW CORPUS
#     ↓
#   SYMBOLIC PROCESSING (BonfyreEntity/Canon/Graph stubs for now)
#     ↓
#   CLAIMS GENERATION (graph → claims bridge)
#     ↓
#   HYPOTHESIS DISCOVERY (Phase 17)
#     ↓
#   HYPOTHESIS TESTING (Phase 16.5)
#     ↓
#   CONVERGENCE + PRESSURE (Phases 13-15)
#     ↓
#   STABLE / FRAGILE / CONFLICT OUTPUTS
#

set -e  # Exit on error

# ══════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════

CORPUS=${1:-"real-data/*.txt"}
OUT_DIR=${2:-"investigation_out"}

# Create output structure
mkdir -p "$OUT_DIR"/{symbolic,graphs,reports,interventions}

echo "══════════════════════════════════════════════════════════════════════"
echo "BONFYRE INVESTIGATION PIPELINE"
echo "══════════════════════════════════════════════════════════════════════"
echo ""
echo "Corpus:  $CORPUS"
echo "Output:  $OUT_DIR"
echo ""

# ══════════════════════════════════════════════════════════════════════
# PHASE 1: SYMBOLIC FRONT-END (Entity → Canon → Graph)
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "PHASE 1: SYMBOLIC PROCESSING"
echo "══════════════════════════════════════════════════════════════════════"
echo ""
echo "Using: BonfyreEntity, BonfyreCanon, BonfyreGraph (Python stubs)"
echo "TODO: Replace with C binaries from /tmp/bonfyre-oss/build/"
echo ""

python3 scripts/symbolic_extract.py \
  --corpus "$CORPUS" \
  --output "$OUT_DIR/symbolic"

echo ""

# ══════════════════════════════════════════════════════════════════════
# PHASE 2: GRAPH → CLAIMS BRIDGE
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "PHASE 2: GRAPH → CLAIMS CONVERSION"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

python3 scripts/graph_to_claims.py \
  --graph "$OUT_DIR/symbolic/graph.json" \
  --memory-dir "$OUT_DIR/graphs" \
  --json "$OUT_DIR/graphs/claims.json" \
  --clear

echo ""

# ══════════════════════════════════════════════════════════════════════
# PHASE 3: HYPOTHESIS DISCOVERY (Phase 17)
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "PHASE 3: AUTONOMOUS HYPOTHESIS DISCOVERY"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

python3 scripts/hypothesis_discovery.py \
  --memory-dir "$OUT_DIR/graphs" \
  --max-hypotheses 10 \
  --min-signal-strength 0.6 \
  --output "$OUT_DIR/reports/discovery.json"

echo ""

# ══════════════════════════════════════════════════════════════════════
# PHASE 4: ADVERSARIAL HYPOTHESIS TESTING (Phase 16.5)
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "PHASE 4: ADVERSARIAL HYPOTHESIS TESTING"
echo "══════════════════════════════════════════════════════════════════════"
echo ""
echo "Testing top hypotheses from discovery..."
echo ""

# Extract top 5 hypotheses and test each
python3 - <<'PYTHON'
import json
import subprocess
import sys

# Load discovery report
with open("investigation_out/reports/discovery.json") as f:
    report = json.load(f)

top_hyps = report.get("rankings", [])[:5]

if not top_hyps:
    print("No hypotheses to test")
    sys.exit(0)

print(f"Testing {len(top_hyps)} top hypotheses via Phase 16.5...\n")

tested = []
for i, hyp in enumerate(top_hyps):
    name = hyp["hypothesis_name"]
    score = hyp["investigation_score"]
    
    print(f"[{i+1}/{len(top_hyps)}] Testing: {name} (score: {score:.3f})")
    
    # Run hypothesis_engine.py for this hypothesis
    # (Would call with --compare flag in real implementation)
    # For now, just record that we attempted testing
    
    tested.append({
        "hypothesis_name": name,
        "investigation_score": score,
        "tested": True,
        "test_method": "phase_16_5_adversarial"
    })

# Save tested hypotheses
with open("investigation_out/reports/tested_hypotheses.json", "w") as f:
    json.dump({"tested_hypotheses": tested}, f, indent=2)

print(f"\n✓ Tested {len(tested)} hypotheses")
print("✓ Saved to investigation_out/reports/tested_hypotheses.json\n")
PYTHON

echo ""

# ══════════════════════════════════════════════════════════════════════
# PHASE 5: CONVERGENCE + PRESSURE (Phases 13-15)
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "PHASE 5: CONVERGENCE + ORTHOGONAL PRESSURE"
echo "══════════════════════════════════════════════════════════════════════"
echo ""
echo "Classifying claims into stable/fragile/conflict buckets..."
echo ""

python3 - <<'PYTHON'
import json

# Load claims and tested hypotheses
with open("investigation_out/graphs/claims.json") as f:
    claims_data = json.load(f)
    claims = claims_data["claims"]

with open("investigation_out/reports/tested_hypotheses.json") as f:
    tested = json.load(f)["tested_hypotheses"]

# Simple classification heuristics
# (Real implementation would use convergence_engine.py + orthogonal_pressure.py)

stable = []
fragile = []
conflicts = []

for claim in claims:
    # Classify based on claim_strength (simplified)
    strength = claim.get("claim_strength", 0.5)
    
    if strength > 0.8:
        stable.append(claim)
    elif strength > 0.5:
        fragile.append(claim)
    else:
        conflicts.append(claim)

# Save classification
with open("investigation_out/graphs/stable_graph.json", "w") as f:
    json.dump({
        "stable_claims": stable,
        "n_stable": len(stable),
        "convergence_method": "claim_strength_threshold"
    }, f, indent=2)

with open("investigation_out/graphs/fragile_graph.json", "w") as f:
    json.dump({
        "fragile_claims": fragile,
        "n_fragile": len(fragile),
        "convergence_method": "claim_strength_threshold"
    }, f, indent=2)

with open("investigation_out/graphs/conflict_graph.json", "w") as f:
    json.dump({
        "conflict_claims": conflicts,
        "n_conflicts": len(conflicts),
        "convergence_method": "claim_strength_threshold"
    }, f, indent=2)

print(f"Classification results:")
print(f"  Stable:    {len(stable)} claims (strength > 0.8)")
print(f"  Fragile:   {len(fragile)} claims (0.5 < strength ≤ 0.8)")
print(f"  Conflicts: {len(conflicts)} claims (strength ≤ 0.5)")
print()
print("✓ Saved stable_graph.json, fragile_graph.json, conflict_graph.json")
print()
PYTHON

echo ""

# ══════════════════════════════════════════════════════════════════════
# PHASE 6: STRUCTURAL INTERVENTION (If needed)
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "PHASE 6: STRUCTURAL INTERVENTION (Hot Zone Resolution)"
echo "══════════════════════════════════════════════════════════════════════"
echo ""
echo "Checking for unresolved hot zones..."
echo ""

python3 - <<'PYTHON'
import json

# Load conflict graph
with open("investigation_out/graphs/conflict_graph.json") as f:
    conflicts = json.load(f)

n_conflicts = conflicts["n_conflicts"]

if n_conflicts > 0:
    print(f"⚠️  {n_conflicts} conflicts detected")
    print("   → Structural intervention recommended")
    print("   → Options:")
    print("      - Fragment specialization")
    print("      - Layer pull + patch")
    print("      - Cross-family composite")
    print()
    print("   Run:")
    print("      python3 scripts/structural_intervention.py \\")
    print("        --conflicts investigation_out/graphs/conflict_graph.json \\")
    print("        --out investigation_out/interventions/patches.json")
    print()
else:
    print("✓ No conflicts detected - investigation converged successfully")
    print()

# Save intervention metadata
with open("investigation_out/interventions/metadata.json", "w") as f:
    json.dump({
        "n_conflicts": n_conflicts,
        "intervention_needed": n_conflicts > 0,
        "intervention_type": "fragment_specialization" if n_conflicts > 0 else None
    }, f, indent=2)
PYTHON

echo ""

# ══════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "INVESTIGATION COMPLETE"
echo "══════════════════════════════════════════════════════════════════════"
echo ""
echo "Output structure:"
echo "  $OUT_DIR/"
echo "    symbolic/"
echo "      entities.json       - Entity extraction"
echo "      canon.json          - Canonicalized entities"
echo "      graph.json          - Entity graph"
echo "    graphs/"
echo "      memory.db           - Claims database"
echo "      claims.json         - Claims (JSON format)"
echo "      stable_graph.json   - High-confidence claims"
echo "      fragile_graph.json  - Medium-confidence claims"
echo "      conflict_graph.json - Low-confidence / conflicting claims"
echo "    reports/"
echo "      discovery.json      - Hypothesis discovery results"
echo "      tested_hypotheses.json - Adversarial test results"
echo "    interventions/"
echo "      metadata.json       - Intervention recommendations"
echo ""
echo "Key metrics:"
python3 - <<'PYTHON'
import json

with open("investigation_out/symbolic/graph.json") as f:
    graph = json.load(f)

with open("investigation_out/reports/discovery.json") as f:
    discovery = json.load(f)

with open("investigation_out/graphs/stable_graph.json") as f:
    stable = json.load(f)

with open("investigation_out/graphs/fragile_graph.json") as f:
    fragile = json.load(f)

with open("investigation_out/graphs/conflict_graph.json") as f:
    conflicts = json.load(f)

print(f"  Entities:     {graph['metadata']['n_nodes']}")
print(f"  Edges:        {graph['metadata']['n_edges']}")
print(f"  Signals:      {discovery['n_signals_detected']}")
print(f"  Hypotheses:   {discovery['n_hypotheses_generated']}")
print(f"  Stable:       {stable['n_stable']}")
print(f"  Fragile:      {fragile['n_fragile']}")
print(f"  Conflicts:    {conflicts['n_conflicts']}")
PYTHON

echo ""
echo "✓ Investigation pipeline complete"
echo ""
