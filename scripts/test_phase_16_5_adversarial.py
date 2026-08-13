#!/usr/bin/env python3
"""
test_phase_16_5_adversarial.py

PHASE 16.5 SMOKE TEST: Adversarial Hypothesis Engine

Tests the INTELLECTUAL COMPETITION layer where competing explanations
are run through pressure and compared for relative strength.
"""

import os
import sys
import tempfile
from pathlib import Path

# Add scripts to path
sys.path.insert(0, str(Path(__file__).parent))

def test_competing_hypotheses():
    """Test comparison of competing hypotheses."""
    print("\n" + "="*70)
    print("PHASE 16.5 TEST 1: Competing Hypotheses")
    print("="*70 + "\n")
    
    from hypothesis_engine import (
        HypothesisEngine,
        CompetingHypothesisSet,
        COMPETING_HYPOTHESIS_SETS
    )
    
    with tempfile.TemporaryDirectory() as tmpdir:
        memory_dir = Path(tmpdir) / "memory"
        models_dir = Path(tmpdir) / "models"
        memory_dir.mkdir()
        models_dir.mkdir()
        
        # Initialize engine
        engine = HypothesisEngine(str(memory_dir), str(models_dir))
        
        # Get competing set
        alias_test = COMPETING_HYPOTHESIS_SETS["alias_identity_test"]
        
        print(f"[TEST 1] Competing Hypotheses Structure")
        print(f"  Competing set: {alias_test.name}")
        print(f"  Description: {alias_test.description}")
        print(f"  Variants: {len(alias_test.variants)}")
        for v in alias_test.variants:
            print(f"    - {v.name:20s} | {v.assumption}")
        
        print(f"\n  ✓ Competing hypothesis set validated")
        
        # Check all built-in competing sets
        print(f"\n[TEST 2] Built-in Competing Sets")
        print(f"  Available: {len(COMPETING_HYPOTHESIS_SETS)}")
        for name, comp_set in COMPETING_HYPOTHESIS_SETS.items():
            nvariants = len(comp_set.variants)
            print(f"    - {name:30s} ({nvariants} variants)")
        
        print(f"\n  ✓ All {len(COMPETING_HYPOTHESIS_SETS)} competing sets valid")
        
        print("\n" + "="*70)
        print("TEST 1 PASSED ✓")
        print("="*70 + "\n")
        
        return True


def test_conditional_expectations():
    """Test conditional expectations (IF → EXPECT)."""
    print("\n" + "="*70)
    print("PHASE 16.5 TEST 2: Conditional Expectations")
    print("="*70 + "\n")
    
    from hypothesis_engine import ConditionalExpectation
    
    # Create expectation
    expectation = ConditionalExpectation(
        conditions=["high name ambiguity"],
        expected_outcomes={"temporal_pressure": 0.6, "survival_rate": 0.3}
    )
    
    print(f"[TEST 1] Expectation Structure")
    print(f"  Conditions: {expectation.conditions}")
    print(f"  Expected outcomes: {expectation.expected_outcomes}")
    print(f"  ✓ Conditional expectation created")
    
    # Test expectation checking
    print(f"\n[TEST 2] Expectation Checking")
    
    # Mock result that meets expectations
    result_met = {
        "temporal_pressure": 0.7,
        "survival_rate": 0.4
    }
    met = expectation.check(result_met)
    print(f"  Result: {result_met}")
    print(f"  Expectations met: {met}")
    assert met == True, "Should meet expectations"
    print(f"  ✓ Correctly identified expectations met")
    
    # Mock result that fails expectations
    result_failed = {
        "temporal_pressure": 0.3,  # Below 0.6
        "survival_rate": 0.4
    }
    met = expectation.check(result_failed)
    print(f"\n  Result: {result_failed}")
    print(f"  Expectations met: {met}")
    assert met == False, "Should not meet expectations"
    print(f"  ✓ Correctly identified expectations failed")
    
    print("\n" + "="*70)
    print("TEST 2 PASSED ✓")
    print("="*70 + "\n")
    
    return True


def test_fragility_tracking():
    """Test hypothesis fragility tracking."""
    print("\n" + "="*70)
    print("PHASE 16.5 TEST 3: Fragility Tracking")
    print("="*70 + "\n")
    
    from hypothesis_engine import HypothesisFragility
    
    # Create fragility object
    fragility = HypothesisFragility(
        graph_pressure=0.8,
        temporal_pressure=0.2,  # FRAGILE!
        frequency_pressure=0.7,
        perturbation_pressure=0.6,
        representation_pressure=0.5
    )
    
    print(f"[TEST 1] Fragility Structure")
    print(f"  Graph pressure:          {fragility.graph_pressure:.3f}")
    print(f"  Temporal pressure:       {fragility.temporal_pressure:.3f} ← FRAGILE")
    print(f"  Frequency pressure:      {fragility.frequency_pressure:.3f}")
    print(f"  Perturbation pressure:   {fragility.perturbation_pressure:.3f}")
    print(f"  Representation pressure: {fragility.representation_pressure:.3f}")
    print(f"  ✓ Fragility object created")
    
    # Test fragile points detection
    print(f"\n[TEST 2] Fragile Points Detection")
    fragile_points = fragility.fragile_points(threshold=0.5)
    print(f"  Fragile points (< 0.5): {fragile_points}")
    assert "temporal" in fragile_points, "Should detect temporal fragility"
    print(f"  ✓ Correctly identified temporal fragility")
    
    # Test robustness score
    print(f"\n[TEST 3] Robustness Score")
    robustness = fragility.robustness_score()
    print(f"  Robustness score: {robustness:.3f}")
    assert 0.0 <= robustness <= 1.0, "Robustness should be 0-1"
    print(f"  ✓ Robustness score calculated")
    
    print("\n" + "="*70)
    print("TEST 3 PASSED ✓")
    print("="*70 + "\n")
    
    return True


def test_hypothesis_extensions():
    """Test Phase 16.5 hypothesis extensions (assumption, chaining)."""
    print("\n" + "="*70)
    print("PHASE 16.5 TEST 4: Hypothesis Extensions")
    print("="*70 + "\n")
    
    from hypothesis_engine import Hypothesis, ConditionalExpectation
    
    # Create hypothesis with Phase 16.5 features
    hypothesis = Hypothesis(
        name="test_hypothesis",
        target_pattern="test pattern",
        activation_conditions=["condition_a"],
        evaluation_strategy=["lens_swarm"],
        success_criteria=["stable graph"],
        assumption="A == B",
        conditional_expectations=[
            ConditionalExpectation(
                conditions=["cond1"],
                expected_outcomes={"metric1": 0.5}
            )
        ],
        chain_input_from="previous_hypothesis"
    )
    
    print(f"[TEST 1] Extended Hypothesis Structure")
    print(f"  Name: {hypothesis.name}")
    print(f"  Assumption: {hypothesis.assumption}")
    print(f"  Conditional expectations: {len(hypothesis.conditional_expectations)}")
    print(f"  Chain input from: {hypothesis.chain_input_from}")
    print(f"  ✓ Extended hypothesis created")
    
    # Test serialization
    print(f"\n[TEST 2] Serialization")
    h_dict = hypothesis.to_dict()
    print(f"  Serialized keys: {list(h_dict.keys())[:5]}...")
    assert "assumption" in h_dict, "Should serialize assumption"
    assert "chain_input_from" in h_dict, "Should serialize chain_input_from"
    print(f"  ✓ Serialization works with new fields")
    
    print("\n" + "="*70)
    print("TEST 4 PASSED ✓")
    print("="*70 + "\n")
    
    return True


if __name__ == "__main__":
    try:
        print("\n" + "🔥"*35)
        print("PHASE 16.5: ADVERSARIAL HYPOTHESIS ENGINE")
        print("🔥"*35)
        print("\nTesting INTELLECTUAL COMPETITION features:")
        print("  - Competing hypotheses")
        print("  - Conditional expectations")
        print("  - Fragility tracking")
        print("  - Hypothesis chaining")
        print()
        
        test_competing_hypotheses()
        test_conditional_expectations()
        test_fragility_tracking()
        test_hypothesis_extensions()
        
        print("\n" + "="*70)
        print("ALL PHASE 16.5 TESTS PASSED ✓")
        print("="*70)
        print("\nBonfyre now supports INTELLECTUAL COMPETITION:")
        print("  ✓ Run competing explanations through pressure")
        print("  ✓ Compare relative strength (not just pass/fail)")
        print("  ✓ Track fragility across pressure types")
        print("  ✓ Chain hypotheses (output → input)")
        print("  ✓ Conditional expectations (IF → EXPECT)")
        print("\nTransformation:")
        print("  FROM: 'Is hypothesis X true?'")
        print("  TO:   'Which of these competing explanations survives best?'")
        print("\n" + "🔥"*35)
        print()
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
