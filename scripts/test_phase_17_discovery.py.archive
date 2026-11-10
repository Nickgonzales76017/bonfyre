#!/usr/bin/env python3
"""
test_phase_17_discovery.py

Tests for Phase 17: Autonomous Hypothesis Discovery
"""

import json
import os
import sqlite3
import tempfile
from pathlib import Path

# Import Phase 17 components
import sys
sys.path.insert(0, str(Path(__file__).parent))

from hypothesis_discovery import (
    Signal,
    SignalDetector,
    HypothesisGenerator,
    HypothesisDiscoveryEngine
)


def setup_test_db(db_path: Path):
    """Create test database with sample data."""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    
    # Create claims table (from Phase 12)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS claims (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id TEXT,
            subject TEXT,
            predicate TEXT,
            object TEXT,
            claim_strength REAL,
            orthogonal_pressure REAL,
            temporal_pressure REAL
        )
    """)
    
    # Insert test data
    test_claims = [
        # Conflict cluster: "Jeffrey Epstein"
        ("doc1", "Jeffrey Epstein", "conflict_with", "J.E.", 0.6, 0.7, 0.8),
        ("doc2", "Jeffrey Epstein", "conflict_with", "Jeffrey E.", 0.5, 0.6, 0.7),
        ("doc3", "Jeffrey Epstein", "contradicts", "J. Epstein", 0.4, 0.5, 0.6),
        ("doc4", "Jeffrey Epstein", "conflict_with", "Epstein", 0.7, 0.8, 0.9),
        
        # High co-occurrence: "Alice" and "Bob"
        ("doc5", "Alice", "knows", "Bob", 0.8, 0.8, 0.9),
        ("doc6", "Alice", "met", "Bob", 0.7, 0.7, 0.8),
        ("doc7", "Alice", "worked_with", "Bob", 0.9, 0.9, 0.9),
        ("doc8", "Alice", "attended_with", "Bob", 0.6, 0.6, 0.7),
        ("doc9", "Alice", "traveled_with", "Bob", 0.8, 0.8, 0.8),
        ("doc10", "Alice", "met", "Bob", 0.7, 0.7, 0.7),
        
        # Unstable region: "Unclear Entity"
        ("doc11", "Unclear Entity", "has", "property1", 0.2, 0.3, 0.4),
        ("doc12", "Unclear Entity", "has", "property2", 0.1, 0.2, 0.3),
        ("doc13", "Unclear Entity", "has", "property3", 0.3, 0.4, 0.5),
        ("doc14", "Unclear Entity", "has", "property4", 0.2, 0.3, 0.4),
        
        # Temporal inconsistency: "Timeline Violation"
        ("doc15", "Event A", "happened_before", "Event B", 0.4, 0.5, 0.1),
        ("doc16", "Event A", "happened_after", "Event B", 0.5, 0.6, 0.2),
        
        # Degree anomaly: "Hub Node"
        ("doc17", "Hub Node", "connects_to", "node1", 0.8, 0.8, 0.8),
        ("doc18", "Hub Node", "connects_to", "node2", 0.8, 0.8, 0.8),
        ("doc19", "Hub Node", "connects_to", "node3", 0.8, 0.8, 0.8),
        ("doc20", "Hub Node", "connects_to", "node4", 0.8, 0.8, 0.8),
        ("doc21", "Hub Node", "connects_to", "node5", 0.8, 0.8, 0.8),
        ("doc22", "Hub Node", "connects_to", "node6", 0.8, 0.8, 0.8),
        ("doc23", "Hub Node", "connects_to", "node7", 0.8, 0.8, 0.8),
        ("doc24", "Hub Node", "connects_to", "node8", 0.8, 0.8, 0.8),
        ("doc25", "Hub Node", "connects_to", "node9", 0.8, 0.8, 0.8),
        ("doc26", "Hub Node", "connects_to", "node10", 0.8, 0.8, 0.8),
        
        # Isolated cluster: "Isolated Node"
        ("doc27", "Isolated Node", "has", "prop1", 0.7, 0.7, 0.7),
        ("doc27", "Isolated Node", "has", "prop2", 0.7, 0.7, 0.7),
        ("doc27", "Isolated Node", "has", "prop3", 0.7, 0.7, 0.7),
    ]
    
    for claim in test_claims:
        cur.execute("""
            INSERT INTO claims 
            (doc_id, subject, predicate, object, claim_strength, orthogonal_pressure, temporal_pressure)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, claim)
    
    conn.commit()
    conn.close()


def test_signal_detection():
    """Test signal detection."""
    print("\n[TEST 1] Signal Detection")
    print("─" * 60)
    
    # Create temp database
    with tempfile.TemporaryDirectory() as tmpdir:
        memory_dir = Path(tmpdir)
        db_path = memory_dir / "memory.db"
        
        setup_test_db(db_path)
        
        detector = SignalDetector(str(memory_dir))
        signals = detector.detect_all_signals(min_strength=0.3)
        
        print(f"  Detected {len(signals)} signals")
        
        # Check signal types
        signal_types = {s.signal_type for s in signals}
        print(f"  Signal types: {signal_types}")
        
        # Should detect multiple types
        assert len(signals) > 0, "Should detect at least one signal"
        assert "conflict_cluster" in signal_types or \
               "cooccurrence_anomaly" in signal_types or \
               "degree_anomaly" in signal_types, \
               "Should detect expected signal types"
        
        # Check signal structure
        for signal in signals[:3]:
            assert 0.0 <= signal.strength <= 1.0, "Signal strength in valid range"
            assert len(signal.entities) > 0, "Signal has entities"
            assert signal.description, "Signal has description"
            print(f"    ✓ {signal.signal_type:25s} | {signal.strength:.2f} | {signal.entities[0]}")
        
        print("  ✓ Signal detection working")


def test_hypothesis_generation():
    """Test hypothesis generation from signals."""
    print("\n[TEST 2] Hypothesis Generation")
    print("─" * 60)
    
    generator = HypothesisGenerator()
    
    # Test 1: Conflict cluster signal
    conflict_signal = Signal(
        signal_type="conflict_cluster",
        strength=0.8,
        entities=["Jeffrey Epstein"],
        evidence={"n_conflicts": 4},
        description="High conflict density around 'Jeffrey Epstein'"
    )
    
    hypotheses = generator.generate_from_signal(conflict_signal)
    assert len(hypotheses) > 0, "Should generate hypothesis from conflict signal"
    assert "conflict_resolution" in hypotheses[0]["name"], "Hypothesis name correct"
    print(f"  ✓ Conflict signal → {len(hypotheses)} hypothesis")
    
    # Test 2: Co-occurrence anomaly (generates competing set)
    cooccur_signal = Signal(
        signal_type="cooccurrence_anomaly",
        strength=0.7,
        entities=["Alice", "Bob"],
        evidence={"n_cooccurrences": 6},
        description="High co-occurrence: 'Alice' and 'Bob'"
    )
    
    hypotheses = generator.generate_from_signal(cooccur_signal)
    assert len(hypotheses) == 2, "Should generate competing hypotheses (same vs different)"
    
    names = [h["name"] for h in hypotheses]
    assumptions = [h.get("assumption") for h in hypotheses]
    
    assert any("same" in n for n in names), "Should have 'same' hypothesis"
    assert any("different" in n for n in names), "Should have 'different' hypothesis"
    assert "Alice == Bob" in assumptions or "Alice != Bob" in assumptions, "Should have assumptions"
    
    print(f"  ✓ Co-occurrence signal → {len(hypotheses)} competing hypotheses")
    print(f"    - {hypotheses[0]['name'][:50]}")
    print(f"    - {hypotheses[1]['name'][:50]}")
    
    # Test 3: Temporal inconsistency
    temporal_signal = Signal(
        signal_type="temporal_inconsistency",
        strength=0.9,
        entities=["Event A", "Event B"],
        evidence={"temporal_pressure": 0.15, "n_violations": 2},
        description="Temporal violation between Event A and Event B"
    )
    
    hypotheses = generator.generate_from_signal(temporal_signal)
    assert len(hypotheses) == 2, "Should generate timeline hypotheses"
    assert any("consistent" in h["name"] for h in hypotheses), "Should have consistent variant"
    assert any("impossible" in h["name"] for h in hypotheses), "Should have impossible variant"
    
    print(f"  ✓ Temporal signal → {len(hypotheses)} timeline hypotheses")
    
    print("  ✓ Hypothesis generation working")


def test_competing_hypothesis_grouping():
    """Test grouping of competing hypotheses."""
    print("\n[TEST 3] Competing Hypothesis Grouping")
    print("─" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        memory_dir = Path(tmpdir)
        models_dir = Path(tmpdir) / "models"
        models_dir.mkdir()
        
        db_path = memory_dir / "memory.db"
        setup_test_db(db_path)
        
        engine = HypothesisDiscoveryEngine(str(memory_dir), str(models_dir))
        
        # Create test hypotheses
        hypotheses = [
            {"name": "alias_same_A_B", "target_pattern": "test", "activation_conditions": [],
             "evaluation_strategy": [], "success_criteria": []},
            {"name": "alias_different_A_B", "target_pattern": "test", "activation_conditions": [],
             "evaluation_strategy": [], "success_criteria": []},
            {"name": "timeline_consistent_X", "target_pattern": "test", "activation_conditions": [],
             "evaluation_strategy": [], "success_criteria": []},
            {"name": "timeline_impossible_X", "target_pattern": "test", "activation_conditions": [],
             "evaluation_strategy": [], "success_criteria": []},
            {"name": "single_hypothesis", "target_pattern": "test", "activation_conditions": [],
             "evaluation_strategy": [], "success_criteria": []}
        ]
        
        groups = engine._group_competing_hypotheses(hypotheses)
        
        print(f"  Grouped {len(hypotheses)} hypotheses into {len(groups)} groups")
        
        # Check grouping
        assert "alias_A_B" in groups, "Should group alias hypotheses"
        assert len(groups["alias_A_B"]) == 2, "Alias group should have 2 variants"
        
        assert "timeline_X" in groups, "Should group timeline hypotheses"
        assert len(groups["timeline_X"]) == 2, "Timeline group should have 2 variants"
        
        assert "single_hypothesis" in groups, "Should keep single hypothesis"
        assert len(groups["single_hypothesis"]) == 1, "Single group should have 1 hypothesis"
        
        for group_name, group_hyps in groups.items():
            print(f"    {group_name:25s} → {len(group_hyps)} hypothesis(es)")
        
        print("  ✓ Competing hypothesis grouping working")


def test_full_discovery_pipeline():
    """Test full discovery pipeline."""
    print("\n[TEST 4] Full Discovery Pipeline")
    print("─" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        memory_dir = Path(tmpdir)
        models_dir = Path(tmpdir) / "models"
        models_dir.mkdir()
        
        db_path = memory_dir / "memory.db"
        setup_test_db(db_path)
        
        engine = HypothesisDiscoveryEngine(str(memory_dir), str(models_dir))
        
        # Run discovery
        report = engine.discover_and_test(
            corpus=["test_doc_1.txt", "test_doc_2.txt"],
            min_signal_strength=0.3,
            max_hypotheses=5,
            verbose=False
        )
        
        # Check report
        assert report["n_signals_detected"] > 0, "Should detect signals"
        assert report["n_hypotheses_generated"] > 0, "Should generate hypotheses"
        
        print(f"  Signals detected:      {report['n_signals_detected']}")
        print(f"  Hypotheses generated:  {report['n_hypotheses_generated']}")
        print(f"  Hypotheses tested:     {report['n_hypotheses_tested']}")
        
        # Check database storage
        conn = sqlite3.connect(str(db_path))
        
        n_stored_signals = conn.execute("SELECT COUNT(*) FROM discovered_signals").fetchone()[0]
        n_stored_hypotheses = conn.execute("SELECT COUNT(*) FROM generated_hypotheses").fetchone()[0]
        n_stored_rankings = conn.execute("SELECT COUNT(*) FROM hypothesis_rankings").fetchone()[0]
        
        conn.close()
        
        assert n_stored_signals > 0, "Should store signals in DB"
        assert n_stored_hypotheses > 0, "Should store hypotheses in DB"
        assert n_stored_rankings > 0, "Should store rankings in DB"
        
        print(f"  DB: {n_stored_signals} signals, {n_stored_hypotheses} hypotheses, {n_stored_rankings} rankings")
        print("  ✓ Full discovery pipeline working")


def test_hypothesis_ranking():
    """Test hypothesis ranking."""
    print("\n[TEST 5] Hypothesis Ranking")
    print("─" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        memory_dir = Path(tmpdir)
        models_dir = Path(tmpdir) / "models"
        models_dir.mkdir()
        
        db_path = memory_dir / "memory.db"
        setup_test_db(db_path)
        
        engine = HypothesisDiscoveryEngine(str(memory_dir), str(models_dir))
        
        # Create test signals
        signals = [
            Signal("conflict_cluster", 0.9, ["Entity1"], {"n": 5}, "High conflict"),
            Signal("unstable_region", 0.7, ["Entity2"], {"n": 3}, "Unstable"),
            Signal("cooccurrence_anomaly", 0.5, ["A", "B"], {"n": 6}, "Co-occur")
        ]
        
        # Create test hypotheses
        hypotheses = [
            {
                "name": "hyp1", "target_pattern": "test", "activation_conditions": [],
                "evaluation_strategy": [], "success_criteria": [],
                "signal_source": {"signal_type": "conflict_cluster", "strength": 0.9, "evidence": {"n": 5}}
            },
            {
                "name": "hyp2", "target_pattern": "test", "activation_conditions": [],
                "evaluation_strategy": [], "success_criteria": [],
                "signal_source": {"signal_type": "unstable_region", "strength": 0.7, "evidence": {"n": 3}}
            },
            {
                "name": "hyp3", "target_pattern": "test", "activation_conditions": [],
                "evaluation_strategy": [], "success_criteria": [],
                "signal_source": {"signal_type": "cooccurrence_anomaly", "strength": 0.5, "evidence": {"n": 6}}
            }
        ]
        
        rankings = engine._rank_hypotheses(hypotheses, signals)
        
        assert len(rankings) == 3, "Should rank all hypotheses"
        
        # Check ranking structure
        for ranking in rankings:
            assert "impact_score" in ranking, "Should have impact score"
            assert "stability_score" in ranking, "Should have stability score"
            assert "novelty_score" in ranking, "Should have novelty score"
            assert "pressure_reduction" in ranking, "Should have pressure reduction"
            assert "overall_rank" in ranking, "Should have overall rank"
            assert 0.0 <= ranking["overall_rank"] <= 1.0, "Overall rank in valid range"
        
        # Check sorting (highest rank first)
        assert rankings[0]["overall_rank"] >= rankings[1]["overall_rank"], "Should be sorted by rank"
        assert rankings[1]["overall_rank"] >= rankings[2]["overall_rank"], "Should be sorted by rank"
        
        print(f"  Ranked {len(rankings)} hypotheses:")
        for i, ranking in enumerate(rankings, 1):
            print(f"    {i}. {ranking['hypothesis_name']:20s} | rank: {ranking['overall_rank']:.3f}")
        
        print("  ✓ Hypothesis ranking working")


if __name__ == "__main__":
    print("\n" + "═" * 70)
    print("PHASE 17: AUTONOMOUS HYPOTHESIS DISCOVERY — TEST SUITE")
    print("═" * 70)
    
    try:
        test_signal_detection()
        test_hypothesis_generation()
        test_competing_hypothesis_grouping()
        test_full_discovery_pipeline()
        test_hypothesis_ranking()
        
        print("\n" + "═" * 70)
        print("ALL PHASE 17 TESTS PASSED ✓")
        print("═" * 70)
        print()
        
    except AssertionError as e:
        print(f"\n✗ TEST FAILED: {e}\n")
        raise
    except Exception as e:
        print(f"\n✗ ERROR: {e}\n")
        raise
