#!/usr/bin/env python3
"""
example_hypothesis_workflow.py

DEMONSTRATION: How to use the Phase 16 Hypothesis Engine

Shows the complete workflow from hypothesis registration to validation.
"""

import sys
from pathlib import Path

# Add scripts to path
sys.path.insert(0, str(Path(__file__).parent))

from hypothesis_engine import HypothesisEngine, Hypothesis, BUILTIN_HYPOTHESES


def main():
    print("\n" + "="*70)
    print("BONFYRE PHASE 16: HYPOTHESIS ENGINE WORKFLOW")
    print("="*70 + "\n")
    
    # Setup
    memory_dir = "/tmp/bonfyre-memory"
    models_dir = "/tmp/bonfyre-models"
    
    Path(memory_dir).mkdir(parents=True, exist_ok=True)
    Path(models_dir).mkdir(parents=True, exist_ok=True)
    
    engine = HypothesisEngine(memory_dir, models_dir)
    
    # ── STEP 1: Register built-in hypotheses ──
    print("STEP 1: Register Built-in Hypotheses")
    print("-" * 70)
    
    for name, hypothesis in BUILTIN_HYPOTHESES.items():
        engine.register_hypothesis(hypothesis)
        print(f"✓ {name:30s} | {hypothesis.target_pattern[:40]}")
    
    print(f"\nRegistered {len(engine.hypotheses)} hypotheses\n")
    
    # ── STEP 2: Create custom hypothesis ──
    print("STEP 2: Create Custom Hypothesis")
    print("-" * 70)
    
    custom = Hypothesis(
        name="meeting_detection",
        target_pattern="identify all meetings between entities",
        activation_conditions=["entity mentions", "temporal references"],
        evaluation_strategy=["lens_swarm", "temporal_pressure", "convergence"],
        success_criteria=["stable meeting events", "temporal consistency"],
        description="Extract and validate meeting events from corpus",
        lens_priorities={"event_extraction": 1.5, "temporal": 1.3},
        expected_claim_types=["met_on", "meeting_at", "attended"]
    )
    
    engine.register_hypothesis(custom)
    print(f"✓ Custom hypothesis registered: {custom.name}")
    print(f"  Target: {custom.target_pattern}")
    print(f"  Strategy: {' → '.join(custom.evaluation_strategy)}\n")
    
    # ── STEP 3: List all registered hypotheses ──
    print("STEP 3: List All Hypotheses")
    print("-" * 70)
    
    for name, h in engine.hypotheses.items():
        print(f"  {name:30s} | {h.target_pattern[:40]}")
    
    print(f"\nTotal: {len(engine.hypotheses)} hypotheses\n")
    
    # ── STEP 4: Explain evaluation workflow ──
    print("STEP 4: Evaluation Workflow")
    print("-" * 70)
    print("""
To evaluate a hypothesis on a corpus:

    corpus = ["doc1.txt", "doc2.txt", "doc3.txt"]
    
    result = engine.evaluate_hypothesis(
        hypothesis=engine.hypotheses["alias_network"],
        corpus=corpus,
        verbose=True
    )
    
Pipeline executed:
    
    [1] Run hypothesis swarm      → Generate competing claims
    [2] Apply convergence         → Extract stable/fragile graphs
    [3] Apply orthogonal pressure → Test against 5 realities
    [4] Try structural intervention → Resolve hot zones
    [5] Measure survival          → Count claims that survived
    [6] Determine status          → confirmed | refuted | inconclusive

Result:
    
    {
      "hypothesis_name": "alias_network",
      "validation_status": "confirmed",  # or "refuted" or "inconclusive"
      "n_claims_initial": 150,
      "n_claims_survived": 42,
      "survival_rate": 0.28,
      "n_stable_edges": 23,
      "orthogonal_pressure_avg": 0.65,
      "intervention_success": True
    }
    """)
    
    # ── STEP 5: CLI usage examples ──
    print("\nSTEP 5: CLI Usage Examples")
    print("-" * 70)
    print("""
# Register a built-in hypothesis
python3 scripts/hypothesis_engine.py --register alias_network

# List registered hypotheses
python3 scripts/hypothesis_engine.py --list

# Evaluate a hypothesis on corpus
python3 scripts/hypothesis_engine.py \\
    --evaluate alias_network \\
    --corpus /corpus/*.txt

# Evaluate ALL hypotheses
python3 scripts/hypothesis_engine.py \\
    --run-all \\
    --corpus /corpus/*.txt

# View evaluation history
python3 scripts/hypothesis_engine.py --history alias_network
    """)
    
    print("\n" + "="*70)
    print("HYPOTHESIS ENGINE READY")
    print("="*70)
    print("\nBonfyre can now pursue and validate specific theories about reality.")
    print("\nInstead of: 'Process these documents'")
    print("You can now: 'Test whether these 3 names are the same person'\n")


if __name__ == "__main__":
    main()
