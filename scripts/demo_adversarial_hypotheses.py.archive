#!/usr/bin/env python3
"""
demo_adversarial_hypotheses.py

DEMONSTRATION: Phase 16.5 Adversarial Hypothesis Engine

Shows how to use the intellectual competition features.
"""

import sys
from pathlib import Path

# Add scripts to path
sys.path.insert(0, str(Path(__file__).parent))

from hypothesis_engine import (
    HypothesisEngine,
    COMPETING_HYPOTHESIS_SETS,
    BUILTIN_HYPOTHESES
)


def demo_competing_hypotheses():
    """Demonstrate competing hypotheses comparison."""
    print("\n" + "="*70)
    print("DEMO 1: COMPETING HYPOTHESES")
    print("="*70 + "\n")
    
    print("Example: Testing identity of two entities")
    print()
    print("Question: Are 'Jeffrey Epstein' and 'J.E.' the same person?")
    print()
    print("Competing hypotheses:")
    print("  1. same_person (A == B)")
    print("  2. different_people (A != B)")
    print()
    print("How it works:")
    print("  → Run BOTH hypotheses through full pipeline")
    print("  → Measure survival under pressure for each")
    print("  → Compare composite scores:")
    print("      score = survival_rate × orthogonal_pressure × robustness")
    print("  → Winner: hypothesis with highest score")
    print()
    print("Example result:")
    print("  same_person:      0.515  (28% survival, 0.73 pressure, 0.81 robust)")
    print("  different_people: 0.001  (3% survival, 0.05 pressure, 0.23 robust)")
    print("  WINNER: same_person (515× stronger)")
    print()
    print("CLI usage:")
    print("  python3 scripts/hypothesis_engine.py \\")
    print("      --compare alias_identity_test \\")
    print("      --corpus /corpus/*.txt")
    print()


def demo_fragility_tracking():
    """Demonstrate fragility tracking."""
    print("\n" + "="*70)
    print("DEMO 2: FRAGILITY TRACKING")
    print("="*70 + "\n")
    
    print("Example: Testing timeline consistency")
    print()
    print("Hypothesis: 'Timeline is internally consistent'")
    print()
    print("Fragility analysis reveals pressure breakdown:")
    print("  Graph pressure:          0.85  ← Strong (good structure)")
    print("  Temporal pressure:       0.12  ← FRAGILE! (simultaneity violations)")
    print("  Frequency pressure:      0.73  ← Strong (plausible co-occurrence)")
    print("  Perturbation pressure:   0.68  ← Strong (survives noise)")
    print("  Representation pressure: 0.54  ← Moderate")
    print()
    print("  Robustness score: 0.58")
    print("  FRAGILE under: temporal")
    print()
    print("Interpretation:")
    print("  → Hypothesis survives most pressure types")
    print("  → But COLLAPSES under temporal pressure")
    print("  → Specific weakness: simultaneity violations")
    print("  → Action: Investigate timeline conflicts")
    print()
    print("CLI usage:")
    print("  python3 scripts/hypothesis_engine.py \\")
    print("      --evaluate timeline_reconstruction \\")
    print("      --with-fragility \\")
    print("      --corpus /corpus/*.txt")
    print()


def demo_hypothesis_chaining():
    """Demonstrate hypothesis chaining."""
    print("\n" + "="*70)
    print("DEMO 3: HYPOTHESIS CHAINING")
    print("="*70 + "\n")
    
    print("Example: Progressive investigation workflow")
    print()
    print("Chain:")
    print("  1. alias_network       → Establish entity identity")
    print("  2. network_discovery   → Find relationships (uses aliases)")
    print("  3. timeline_reconstruction → Build chronology (uses network)")
    print()
    print("Execution:")
    print("  [STEP 1/3] alias_network")
    print("    → CONFIRMED: 'J.E.' == 'Jeffrey Epstein'")
    print("    → Output: Merged entity")
    print()
    print("  [STEP 2/3] network_discovery")
    print("    → Input: Use merged entity from Step 1")
    print("    → CONFIRMED: Network of 12 connected actors")
    print("    → Output: Relationship graph")
    print()
    print("  [STEP 3/3] timeline_reconstruction")
    print("    → Input: Use relationship graph from Step 2")
    print("    → REFUTED: Timeline contains physical impossibility")
    print("    → CHAIN STOPPED")
    print()
    print("Result:")
    print("  ✓ Identity confirmed")
    print("  ✓ Network discovered")
    print("  ✗ Timeline refuted (simultaneity violation)")
    print()
    print("CLI usage:")
    print("  python3 scripts/hypothesis_engine.py \\")
    print("      --chain alias_network network_discovery timeline_reconstruction \\")
    print("      --corpus /corpus/*.txt")
    print()


def demo_conditional_expectations():
    """Demonstrate conditional expectations."""
    print("\n" + "="*70)
    print("DEMO 4: CONDITIONAL EXPECTATIONS")
    print("="*70 + "\n")
    
    print("Example: Pre-registered predictions")
    print()
    print("Hypothesis: same_person (A == B)")
    print()
    print("Conditional expectation:")
    print("  IF high_name_similarity:")
    print("    EXPECT temporal_pressure > 0.6  (no temporal conflicts)")
    print("    EXPECT survival_rate > 0.3      (moderate claim survival)")
    print()
    print("Evaluation result:")
    print("  temporal_pressure: 0.73  ✓ (met expectation)")
    print("  survival_rate: 0.42      ✓ (met expectation)")
    print("  expectations_met: True")
    print()
    print("Interpretation:")
    print("  → Hypothesis behaved as predicted")
    print("  → High name similarity → no temporal conflicts")
    print("  → Expectations validated")
    print()
    print("If expectations NOT met:")
    print("  temporal_pressure: 0.15  ✗ (below 0.6)")
    print("  survival_rate: 0.42      ✓")
    print("  expectations_met: False")
    print()
    print("  → Surprising behavior!")
    print("  → Despite name similarity, temporal conflicts exist")
    print("  → Warrants investigation")
    print()


def demo_relative_strength():
    """Demonstrate relative strength scoring."""
    print("\n" + "="*70)
    print("DEMO 5: RELATIVE STRENGTH COMPARISON")
    print("="*70 + "\n")
    
    print("Example: Comparing two explanations quantitatively")
    print()
    print("Hypothesis A: 'Subject attended meeting'")
    print("  survival_rate: 0.15")
    print("  orthogonal_pressure: 0.32")
    print("  robustness: 0.28")
    print("  composite_score: 0.15 × 0.32 × 0.28 = 0.013")
    print()
    print("Hypothesis B: 'Subject did NOT attend meeting'")
    print("  survival_rate: 0.73")
    print("  orthogonal_pressure: 0.81")
    print("  robustness: 0.76")
    print("  composite_score: 0.73 × 0.81 × 0.76 = 0.449")
    print()
    print("Comparison:")
    print("  B / A = 0.449 / 0.013 = 34.5×")
    print()
    print("Conclusion:")
    print("  → Hypothesis B is 34× stronger than A")
    print("  → Strong evidence subject did NOT attend")
    print()
    print("This is NOT binary true/false.")
    print("This is QUANTITATIVE strength comparison.")
    print()


if __name__ == "__main__":
    print("\n" + "🔥"*35)
    print("PHASE 16.5: ADVERSARIAL HYPOTHESIS ENGINE")
    print("DEMONSTRATION")
    print("🔥"*35)
    
    demo_competing_hypotheses()
    demo_fragility_tracking()
    demo_hypothesis_chaining()
    demo_conditional_expectations()
    demo_relative_strength()
    
    print("\n" + "="*70)
    print("KEY TRANSFORMATION")
    print("="*70)
    print()
    print("Phase 16:")
    print("  Question: 'Is hypothesis X true?'")
    print("  Answer:   confirmed | refuted | inconclusive")
    print()
    print("Phase 16.5:")
    print("  Question: 'Which of these competing explanations survives best?'")
    print("  Answer:   Hypothesis A (0.515) vs Hypothesis B (0.001)")
    print("            → A is 515× stronger")
    print("            → A fragile under temporal pressure")
    print("            → B is winner (survives temporal pressure)")
    print()
    print("="*70)
    print()
    print("This is INTELLECTUAL COMPETITION.")
    print()
    print("The system argues with itself and converges on the best explanation.")
    print()
    print("🔥"*35)
    print()
