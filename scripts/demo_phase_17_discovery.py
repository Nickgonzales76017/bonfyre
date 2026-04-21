#!/usr/bin/env python3
"""
demo_phase_17_discovery.py

Demonstration of Phase 17: Autonomous Hypothesis Discovery
"""

import json
import sqlite3
import tempfile
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent))

from hypothesis_discovery import (
    Signal,
    SignalDetector,
    HypothesisGenerator,
    HypothesisDiscoveryEngine
)


def demo_signal_detection():
    """Demo 1: Signal detection from corpus."""
    print("\n" + "═" * 70)
    print("DEMO 1: AUTONOMOUS SIGNAL DETECTION")
    print("═" * 70)
    
    # Create temp database with realistic data
    with tempfile.TemporaryDirectory() as tmpdir:
        memory_dir = Path(tmpdir)
        db_path = memory_dir / "memory.db"
        
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE claims (
                id INTEGER PRIMARY KEY,
                doc_id TEXT,
                subject TEXT,
                predicate TEXT,
                object TEXT,
                claim_strength REAL,
                orthogonal_pressure REAL,
                temporal_pressure REAL
            )
        """)
        
        # Insert claims showing co-occurrence anomaly
        test_claims = [
            ("doc1", "Jeffrey Epstein", "appeared_at", "event1", 0.8, 0.7, 0.9),
            ("doc1", "J.E.", "attended", "event1", 0.7, 0.6, 0.8),
            ("doc2", "Jeffrey Epstein", "met", "Alice", 0.9, 0.8, 0.9),
            ("doc2", "J.E.", "spoke_with", "Alice", 0.8, 0.7, 0.8),
            ("doc3", "Jeffrey Epstein", "flew_to", "island", 0.7, 0.6, 0.7),
            ("doc3", "J.E.", "arrived_at", "island", 0.6, 0.5, 0.6),
            ("doc4", "Jeffrey Epstein", "owns", "property", 0.9, 0.8, 0.9),
            ("doc4", "J.E.", "owns", "property", 0.8, 0.7, 0.8),
        ]
        
        for claim in test_claims:
            cur.execute("""
                INSERT INTO claims 
                (doc_id, subject, predicate, object, claim_strength, orthogonal_pressure, temporal_pressure)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, claim)
        
        conn.commit()
        conn.close()
        
        # Detect signals
        detector = SignalDetector(str(memory_dir))
        signals = detector.detect_all_signals(min_strength=0.3)
        
        print(f"\n📡 Scanned corpus (8 claims)")
        print(f"→ Detected {len(signals)} signals\n")
        
        for i, signal in enumerate(signals, 1):
            print(f"{i}. {signal.signal_type:25s} | strength: {signal.strength:.2f}")
            print(f"   Entities: {', '.join(signal.entities)}")
            print(f"   Evidence: {signal.evidence}")
            print(f"   → {signal.description}\n")
        
        print("🔥 KEY INSIGHT:")
        print("   Without Phase 17: Human must manually notice these patterns")
        print("   With Phase 17:    System auto-detects anomalies for investigation")


def demo_hypothesis_generation():
    """Demo 2: Hypothesis generation from signals."""
    print("\n" + "═" * 70)
    print("DEMO 2: AUTONOMOUS HYPOTHESIS GENERATION")
    print("═" * 70)
    
    generator = HypothesisGenerator()
    
    # Signal: High co-occurrence
    signal = Signal(
        signal_type="cooccurrence_anomaly",
        strength=0.87,
        entities=["Jeffrey Epstein", "J.E."],
        evidence={"n_cooccurrences": 47},
        description="High co-occurrence: 'Jeffrey Epstein' and 'J.E.' (47 times)"
    )
    
    print(f"\n📡 Input signal:")
    print(f"   Type: {signal.signal_type}")
    print(f"   Strength: {signal.strength}")
    print(f"   Entities: {signal.entities}")
    print(f"   Evidence: {signal.evidence}")
    
    # Generate hypotheses
    hypotheses = generator.generate_from_signal(signal)
    
    print(f"\n🧠 Generated {len(hypotheses)} competing hypotheses:\n")
    
    for i, hyp in enumerate(hypotheses, 1):
        print(f"{i}. {hyp['name']}")
        print(f"   Assumption:       {hyp.get('assumption', 'N/A')}")
        print(f"   Target pattern:   {hyp['target_pattern'][:60]}...")
        print(f"   Success criteria: {', '.join(hyp['success_criteria'])}")
        print()
    
    print("🔥 KEY TRANSFORMATION:")
    print("   Before: Human decides 'Maybe I should test if these are the same person'")
    print("   After:  System generates COMPETING hypotheses (same vs different)")
    print("           → Forces adversarial testing (Phase 16.5)")


def demo_competing_set_creation():
    """Demo 3: Auto-creation of competing sets."""
    print("\n" + "═" * 70)
    print("DEMO 3: AUTO-CREATING COMPETING SETS")
    print("═" * 70)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        memory_dir = Path(tmpdir)
        models_dir = Path(tmpdir) / "models"
        models_dir.mkdir()
        
        db_path = memory_dir / "memory.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE claims (id INTEGER PRIMARY KEY)")
        conn.close()
        
        engine = HypothesisDiscoveryEngine(str(memory_dir), str(models_dir))
        
        # Hypotheses from different signals
        hypotheses = [
            {
                "name": "alias_same_Jeffrey_Epstein_JE",
                "target_pattern": "test", "activation_conditions": [],
                "evaluation_strategy": [], "success_criteria": [],
                "assumption": "Jeffrey Epstein == J.E."
            },
            {
                "name": "alias_different_Jeffrey_Epstein_JE",
                "target_pattern": "test", "activation_conditions": [],
                "evaluation_strategy": [], "success_criteria": [],
                "assumption": "Jeffrey Epstein != J.E."
            },
            {
                "name": "timeline_consistent_Event_A",
                "target_pattern": "test", "activation_conditions": [],
                "evaluation_strategy": [], "success_criteria": [],
                "assumption": "Timeline is consistent"
            },
            {
                "name": "timeline_impossible_Event_A",
                "target_pattern": "test", "activation_conditions": [],
                "evaluation_strategy": [], "success_criteria": [],
                "assumption": "Timeline contains impossibilities"
            },
            {
                "name": "hub_role_Transaction_Network",
                "target_pattern": "test", "activation_conditions": [],
                "evaluation_strategy": [], "success_criteria": []
            }
        ]
        
        print(f"\n📊 Input: {len(hypotheses)} generated hypotheses")
        
        groups = engine._group_competing_hypotheses(hypotheses)
        
        print(f"\n🎯 Grouped into {len(groups)} sets:\n")
        
        for group_name, group_hyps in groups.items():
            if len(group_hyps) > 1:
                print(f"COMPETING SET: {group_name}")
                for hyp in group_hyps:
                    assumption = hyp.get('assumption', 'N/A')
                    print(f"  ├─ {hyp['name'][:40]:40s} | {assumption}")
                print()
            else:
                print(f"SINGLE: {group_name}")
                print(f"  └─ {group_hyps[0]['name'][:40]}")
                print()
        
        print("🔥 THE POWER:")
        print("   Competing hypotheses are AUTOMATICALLY grouped")
        print("   → No manual setup required")
        print("   → Ready to feed into Phase 16.5 adversarial testing")


def demo_hypothesis_ranking():
    """Demo 4: Hypothesis ranking by impact/stability/novelty/pressure_reduction."""
    print("\n" + "═" * 70)
    print("DEMO 4: HYPOTHESIS RANKING")
    print("═" * 70)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        memory_dir = Path(tmpdir)
        models_dir = Path(tmpdir) / "models"
        models_dir.mkdir()
        
        db_path = memory_dir / "memory.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE claims (id INTEGER PRIMARY KEY)")
        conn.close()
        
        engine = HypothesisDiscoveryEngine(str(memory_dir), str(models_dir))
        
        # Signals with different strengths
        signals = [
            Signal("conflict_cluster", 0.91, ["Entity1"], {"n": 8}, "High conflict"),
            Signal("cooccurrence_anomaly", 0.73, ["A", "B"], {"n": 12}, "Co-occur"),
            Signal("temporal_inconsistency", 0.58, ["Event"], {"n": 3}, "Temporal"),
        ]
        
        # Hypotheses from signals
        hypotheses = [
            {
                "name": "conflict_resolution_Entity1",
                "target_pattern": "resolve conflicts", "activation_conditions": [],
                "evaluation_strategy": [], "success_criteria": [],
                "signal_source": {
                    "signal_type": "conflict_cluster",
                    "strength": 0.91,
                    "evidence": {"n": 8}
                }
            },
            {
                "name": "alias_same_A_B",
                "target_pattern": "test if same", "activation_conditions": [],
                "evaluation_strategy": [], "success_criteria": [],
                "signal_source": {
                    "signal_type": "cooccurrence_anomaly",
                    "strength": 0.73,
                    "evidence": {"n": 12}
                }
            },
            {
                "name": "timeline_consistent_Event",
                "target_pattern": "test timeline", "activation_conditions": [],
                "evaluation_strategy": [], "success_criteria": [],
                "signal_source": {
                    "signal_type": "temporal_inconsistency",
                    "strength": 0.58,
                    "evidence": {"n": 3}
                }
            }
        ]
        
        print("\n📊 Ranking 3 hypotheses...\n")
        
        rankings = engine._rank_hypotheses(hypotheses, signals)
        
        print("RANKED HYPOTHESES:\n")
        print(f"{'Rank':<6} {'Hypothesis':<40} {'Overall':<8} {'Impact':<8} {'Stable':<8} {'Novel':<8} {'Pressure':<8}")
        print("─" * 90)
        
        for i, ranking in enumerate(rankings, 1):
            print(f"{i:<6} {ranking['hypothesis_name'][:39]:<40} "
                  f"{ranking['overall_rank']:.3f}    "
                  f"{ranking['impact_score']:.3f}    "
                  f"{ranking['stability_score']:.3f}    "
                  f"{ranking['novelty_score']:.3f}    "
                  f"{ranking['pressure_reduction']:.3f}")
        
        print("\n🔥 RANKING FACTORS:")
        print("   Impact:            How much would resolving this help? (signal strength)")
        print("   Stability:         How robust is the signal? (evidence count)")
        print("   Novelty:           Have we tested this before? (always 1.0 for now)")
        print("   Pressure reduction: Would this reduce unresolved pressure?")
        print()
        print("   Overall rank = 0.3×impact + 0.2×stability + 0.2×novelty + 0.3×pressure_reduction")


def demo_full_autonomous_pipeline():
    """Demo 5: Full autonomous discovery pipeline."""
    print("\n" + "═" * 70)
    print("DEMO 5: FULL AUTONOMOUS PIPELINE")
    print("═" * 70)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        memory_dir = Path(tmpdir)
        models_dir = Path(tmpdir) / "models"
        models_dir.mkdir()
        
        db_path = memory_dir / "memory.db"
        
        # Setup test data
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE claims (
                id INTEGER PRIMARY KEY, doc_id TEXT, subject TEXT,
                predicate TEXT, object TEXT,
                claim_strength REAL, orthogonal_pressure REAL, temporal_pressure REAL
            )
        """)
        
        # Multiple anomalies
        test_claims = [
            # Co-occurrence anomaly
            ("doc1", "Alice", "met", "Bob", 0.8, 0.7, 0.8),
            ("doc2", "Alice", "attended_with", "Bob", 0.7, 0.6, 0.7),
            ("doc3", "Alice", "traveled_with", "Bob", 0.9, 0.8, 0.9),
            ("doc4", "Alice", "worked_with", "Bob", 0.8, 0.7, 0.8),
            ("doc5", "Alice", "dined_with", "Bob", 0.7, 0.6, 0.7),
            ("doc6", "Alice", "met", "Bob", 0.8, 0.7, 0.8),
            
            # Temporal inconsistency
            ("doc7", "Event A", "happened_before", "Event B", 0.5, 0.6, 0.1),
            ("doc8", "Event B", "occurred_first", "Event A", 0.4, 0.5, 0.2),
            
            # Unstable region
            ("doc9", "Vague Entity", "has", "prop1", 0.2, 0.3, 0.3),
            ("doc10", "Vague Entity", "has", "prop2", 0.1, 0.2, 0.2),
            ("doc11", "Vague Entity", "has", "prop3", 0.3, 0.4, 0.4),
            ("doc12", "Vague Entity", "has", "prop4", 0.2, 0.3, 0.3),
        ]
        
        for claim in test_claims:
            cur.execute("""
                INSERT INTO claims 
                (doc_id, subject, predicate, object, claim_strength, orthogonal_pressure, temporal_pressure)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, claim)
        
        conn.commit()
        conn.close()
        
        # Run full pipeline
        engine = HypothesisDiscoveryEngine(str(memory_dir), str(models_dir))
        
        print("\n🤖 AUTONOMOUS EXECUTION:\n")
        print("   (System operates without human intervention)\n")
        print("─" * 70)
        
        report = engine.discover_and_test(
            corpus=["doc1.txt", "doc2.txt"],
            min_signal_strength=0.4,
            max_hypotheses=5,
            verbose=False
        )
        
        print(f"\n✓ Pipeline complete")
        print(f"  Signals detected:      {report['n_signals_detected']}")
        print(f"  Hypotheses generated:  {report['n_hypotheses_generated']}")
        print(f"  Hypotheses tested:     {report['n_hypotheses_tested']}")
        
        if report['signals']:
            print(f"\n  Top signal:")
            top_signal = report['signals'][0]
            print(f"    Type:     {top_signal['signal_type']}")
            print(f"    Strength: {top_signal['strength']:.2f}")
            print(f"    Entities: {', '.join(top_signal['entities'])}")
        
        if report['rankings']:
            print(f"\n  Top hypothesis:")
            top_hyp = report['rankings'][0]
            print(f"    Name:  {top_hyp['hypothesis_name']}")
            print(f"    Rank:  {top_hyp['overall_rank']:.3f}")
        
        print("\n🔥 THE TRANSFORMATION:\n")
        print("   BEFORE Phase 17:")
        print("     Human: 'Let me scan these documents for patterns...'")
        print("     Human: 'I notice Alice and Bob appear together often'")
        print("     Human: 'Maybe I should test if they're the same person'")
        print("     Human: [manually runs Phase 16.5]")
        print()
        print("   AFTER Phase 17:")
        print("     System: [scans corpus]")
        print("     System: 'Detected cooccurrence_anomaly (Alice, Bob)'")
        print("     System: 'Generated competing hypotheses (same vs different)'")
        print("     System: [auto-runs Phase 16.5]")
        print("     System: 'Winner: same_person. Confidence: high.'")
        print()
        print("   → System operates AUTONOMOUSLY")


if __name__ == "__main__":
    print("\n" + "═" * 70)
    print("PHASE 17: AUTONOMOUS HYPOTHESIS DISCOVERY — DEMONSTRATION")
    print("═" * 70)
    
    demo_signal_detection()
    demo_hypothesis_generation()
    demo_competing_set_creation()
    demo_hypothesis_ranking()
    demo_full_autonomous_pipeline()
    
    print("\n" + "═" * 70)
    print("KEY INSIGHT")
    print("═" * 70)
    print()
    print("  You've built a system that can argue about reality (Phases 13-16.5)")
    print()
    print("  Phase 17 makes it decide what arguments are worth having.")
    print()
    print("  This is the difference between:")
    print()
    print("    TOOL:  Waits for human instructions")
    print("    AGENT: Acts autonomously")
    print()
    print("  Bonfyre is now an AUTONOMOUS INVESTIGATOR.")
    print()
    print("═" * 70)
    print()
