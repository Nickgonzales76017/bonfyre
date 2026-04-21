#!/usr/bin/env python3
"""
hypothesis_discovery.py

PHASE 17: AUTONOMOUS HYPOTHESIS DISCOVERY

PURPOSE:
--------
Phases 13-16.5 built the machinery to TEST hypotheses.
Phase 17 adds AUTONOMY: the system decides WHAT to test.

CORE INSIGHT:
-------------
Before: Human says "Test whether A == B"
After:  System scans corpus, detects anomaly, generates hypothesis, tests it

THE TRANSFORMATION:
-------------------
Phase 16:   "Test this hypothesis" (user-driven)
Phase 16.5: "Compare these hypotheses" (user-driven)
Phase 17:   "I found something suspicious — let me test it" (autonomous)

WORKFLOW:
---------
1. SCAN FOR SIGNALS:
   - High conflict clusters
   - Co-occurrence anomalies
   - Unstable graph regions
   - Repeated structural failures
   - Temporal inconsistencies

2. GENERATE CANDIDATE HYPOTHESES:
   - alias_hypothesis (name variants → same person?)
   - timeline_inconsistency (temporal violations → impossible sequence?)
   - hidden_cluster (entity island → missing links?)
   - role_asymmetry (degree anomaly → hub vs peripheral?)
   - missing_link_hypothesis (gap in network → should connect?)

3. AUTO-CREATE COMPETING SETS:
   - X exists vs X does not exist
   - X caused Y vs coincidence
   - A == B vs A != B
   - Timeline consistent vs impossible

4. FEED INTO PHASE 16.5:
   - Discovered hypothesis → adversarial evaluation
   - Survival comparison → winner
   - Store results

5. RANK HYPOTHESES BY:
   - Impact: how much would this resolve?
   - Stability: how robust is the signal?
   - Novelty: have we tested this before?
   - Pressure reduction: would this reduce unresolved pressure?

NEW CAPABILITY:
---------------
Bonfyre becomes an AUTONOMOUS INVESTIGATOR that:
  - Notices anomalies
  - Generates explanations
  - Tests competing theories
  - Reports findings

This is where Bonfyre stops being a tool and starts acting like an agent.
"""

import json
import os
import sqlite3
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict, Counter

# ═══════════════════════════════════════════════════════════════════════════
# SIGNAL DETECTION
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class Signal:
    """
    An anomaly or pattern detected in the corpus/claim graph.
    
    Signals indicate something worth investigating.
    """
    signal_type: str  # conflict_cluster, cooccurrence_anomaly, temporal_violation, etc.
    strength: float   # How strong is the signal? (0.0-1.0)
    entities: List[str]  # Entities involved
    evidence: Dict[str, Any]  # Supporting data
    description: str  # Human-readable description
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SignalDetector:
    """
    Scans corpus and claim graph for anomalies worth investigating.
    """
    
    def __init__(self, memory_dir: str):
        self.memory_dir = Path(memory_dir)
        self.db_path = self.memory_dir / "memory.db"
    
    def detect_all_signals(self, min_strength: float = 0.5) -> List[Signal]:
        """
        Run all signal detectors and return high-strength signals.
        
        Args:
            min_strength: Minimum signal strength to return
        
        Returns:
            List of detected signals
        """
        signals = []
        
        # Run all detectors
        signals.extend(self.detect_conflict_clusters())
        signals.extend(self.detect_cooccurrence_anomalies())
        signals.extend(self.detect_unstable_regions())
        signals.extend(self.detect_temporal_inconsistencies())
        signals.extend(self.detect_degree_anomalies())
        signals.extend(self.detect_isolated_clusters())
        
        # Filter by strength
        strong_signals = [s for s in signals if s.strength >= min_strength]
        
        # Sort by strength
        strong_signals.sort(key=lambda s: s.strength, reverse=True)
        
        return strong_signals
    
    def detect_conflict_clusters(self) -> List[Signal]:
        """
        Detect regions with high conflict density.
        
        Multiple lenses making contradictory claims about same entities.
        """
        signals = []
        
        try:
            conn = sqlite3.connect(str(self.db_path))
            
            # Find entities involved in many conflicts
            rows = conn.execute("""
                SELECT subject, COUNT(*) as n_conflicts
                FROM claims
                WHERE predicate LIKE '%conflict%' OR predicate LIKE '%contradict%'
                GROUP BY subject
                HAVING n_conflicts > 2
                ORDER BY n_conflicts DESC
                LIMIT 10
            """).fetchall()
            
            for subject, n_conflicts in rows:
                strength = min(n_conflicts / 10.0, 1.0)  # Normalize
                
                signals.append(Signal(
                    signal_type="conflict_cluster",
                    strength=strength,
                    entities=[subject],
                    evidence={"n_conflicts": n_conflicts},
                    description=f"High conflict density around '{subject}' ({n_conflicts} conflicts)"
                ))
            
            conn.close()
            
        except Exception as e:
            print(f"[signal_detector] WARNING: conflict_clusters failed: {e}")
        
        return signals
    
    def detect_cooccurrence_anomalies(self) -> List[Signal]:
        """
        Detect unexpected entity co-occurrences.
        
        Entities that appear together far more or less than expected.
        """
        signals = []
        
        try:
            conn = sqlite3.connect(str(self.db_path))
            
            # Find entity pairs with high co-occurrence
            rows = conn.execute("""
                SELECT c1.subject, c2.subject, COUNT(*) as cooccur
                FROM claims c1
                JOIN claims c2 ON c1.doc_id = c2.doc_id
                WHERE c1.subject < c2.subject
                  AND c1.subject NOT LIKE '%http%'
                  AND c2.subject NOT LIKE '%http%'
                GROUP BY c1.subject, c2.subject
                HAVING cooccur > 5
                ORDER BY cooccur DESC
                LIMIT 10
            """).fetchall()
            
            for subj1, subj2, cooccur in rows:
                strength = min(cooccur / 20.0, 1.0)
                
                signals.append(Signal(
                    signal_type="cooccurrence_anomaly",
                    strength=strength,
                    entities=[subj1, subj2],
                    evidence={"n_cooccurrences": cooccur},
                    description=f"High co-occurrence: '{subj1}' and '{subj2}' ({cooccur} times)"
                ))
            
            conn.close()
            
        except Exception as e:
            print(f"[signal_detector] WARNING: cooccurrence_anomalies failed: {e}")
        
        return signals
    
    def detect_unstable_regions(self) -> List[Signal]:
        """
        Detect graph regions with low claim strength.
        
        Areas where many claims are fragile or have low orthogonal pressure.
        """
        signals = []
        
        try:
            conn = sqlite3.connect(str(self.db_path))
            
            # Find entities with many low-strength claims
            rows = conn.execute("""
                SELECT subject, 
                       AVG(claim_strength) as avg_strength,
                       AVG(COALESCE(orthogonal_pressure, 0.5)) as avg_pressure,
                       COUNT(*) as n_claims
                FROM claims
                WHERE claim_strength IS NOT NULL
                GROUP BY subject
                HAVING avg_strength < 0.3 AND n_claims > 3
                ORDER BY avg_strength ASC
                LIMIT 10
            """).fetchall()
            
            for subject, avg_strength, avg_pressure, n_claims in rows:
                strength = 1.0 - avg_strength  # Invert: low strength = high signal
                
                signals.append(Signal(
                    signal_type="unstable_region",
                    strength=strength,
                    entities=[subject],
                    evidence={
                        "avg_claim_strength": avg_strength,
                        "avg_orthogonal_pressure": avg_pressure,
                        "n_claims": n_claims
                    },
                    description=f"Unstable region around '{subject}' (avg strength: {avg_strength:.2f})"
                ))
            
            conn.close()
            
        except Exception as e:
            print(f"[signal_detector] WARNING: unstable_regions failed: {e}")
        
        return signals
    
    def detect_temporal_inconsistencies(self) -> List[Signal]:
        """
        Detect temporal violations.
        
        Claims with low temporal pressure (simultaneity violations, etc.).
        """
        signals = []
        
        try:
            conn = sqlite3.connect(str(self.db_path))
            
            # Find claims with low temporal pressure
            rows = conn.execute("""
                SELECT subject, object, predicate,
                       temporal_pressure,
                       COUNT(*) as n_violations
                FROM claims
                WHERE temporal_pressure IS NOT NULL
                  AND temporal_pressure < 0.3
                GROUP BY subject, object
                HAVING n_violations > 1
                ORDER BY temporal_pressure ASC
                LIMIT 10
            """).fetchall()
            
            for subject, obj, predicate, temp_pressure, n_violations in rows:
                strength = 1.0 - temp_pressure  # Low pressure = high signal
                
                signals.append(Signal(
                    signal_type="temporal_inconsistency",
                    strength=strength,
                    entities=[subject, obj] if obj else [subject],
                    evidence={
                        "temporal_pressure": temp_pressure,
                        "n_violations": n_violations,
                        "predicate": predicate
                    },
                    description=f"Temporal violation: '{subject}' {predicate} '{obj}' (pressure: {temp_pressure:.2f})"
                ))
            
            conn.close()
            
        except Exception as e:
            print(f"[signal_detector] WARNING: temporal_inconsistencies failed: {e}")
        
        return signals
    
    def detect_degree_anomalies(self) -> List[Signal]:
        """
        Detect entities with anomalous degree (too many or too few connections).
        
        Hubs or isolated nodes.
        """
        signals = []
        
        try:
            conn = sqlite3.connect(str(self.db_path))
            
            # Find entities with high degree
            rows = conn.execute("""
                SELECT subject, COUNT(*) as degree
                FROM claims
                WHERE subject NOT LIKE '%http%'
                GROUP BY subject
                ORDER BY degree DESC
                LIMIT 20
            """).fetchall()
            
            if not rows:
                conn.close()
                return signals
            
            # Compute average degree
            degrees = [deg for _, deg in rows]
            avg_degree = sum(degrees) / len(degrees)
            
            # Flag anomalies (degree > 3× average)
            for subject, degree in rows:
                if degree > avg_degree * 3:
                    strength = min(degree / (avg_degree * 10), 1.0)
                    
                    signals.append(Signal(
                        signal_type="degree_anomaly",
                        strength=strength,
                        entities=[subject],
                        evidence={
                            "degree": degree,
                            "avg_degree": avg_degree,
                            "anomaly_ratio": degree / avg_degree
                        },
                        description=f"Degree anomaly: '{subject}' has {degree} connections ({degree/avg_degree:.1f}× average)"
                    ))
            
            conn.close()
            
        except Exception as e:
            print(f"[signal_detector] WARNING: degree_anomalies failed: {e}")
        
        return signals
    
    def detect_isolated_clusters(self) -> List[Signal]:
        """
        Detect isolated entity groups (potential missing links).
        
        Entities that form a cluster but have no connections to main graph.
        """
        signals = []
        
        try:
            conn = sqlite3.connect(str(self.db_path))
            
            # Find entities with claims but no cross-references
            # (simplified heuristic: entities mentioned in few docs)
            rows = conn.execute("""
                SELECT subject, COUNT(DISTINCT doc_id) as n_docs, COUNT(*) as n_claims
                FROM claims
                WHERE subject NOT LIKE '%http%'
                GROUP BY subject
                HAVING n_docs < 3 AND n_claims > 2
                ORDER BY n_claims DESC
                LIMIT 10
            """).fetchall()
            
            for subject, n_docs, n_claims in rows:
                strength = min(n_claims / 10.0, 1.0)
                
                signals.append(Signal(
                    signal_type="isolated_cluster",
                    strength=strength,
                    entities=[subject],
                    evidence={"n_docs": n_docs, "n_claims": n_claims},
                    description=f"Isolated entity: '{subject}' ({n_claims} claims in only {n_docs} docs)"
                ))
            
            conn.close()
            
        except Exception as e:
            print(f"[signal_detector] WARNING: isolated_clusters failed: {e}")
        
        return signals


# ═══════════════════════════════════════════════════════════════════════════
# HYPOTHESIS GENERATION
# ═══════════════════════════════════════════════════════════════════════════

class HypothesisGenerator:
    """
    Generates candidate hypotheses from detected signals.
    """
    
    def generate_from_signal(self, signal: Signal) -> List[Dict[str, Any]]:
        """
        Generate candidate hypotheses from a signal.
        
        Returns list of hypothesis specs (to be converted to Hypothesis objects).
        """
        if signal.signal_type == "conflict_cluster":
            return self._generate_conflict_hypotheses(signal)
        
        elif signal.signal_type == "cooccurrence_anomaly":
            return self._generate_alias_hypotheses(signal)
        
        elif signal.signal_type == "unstable_region":
            return self._generate_stability_hypotheses(signal)
        
        elif signal.signal_type == "temporal_inconsistency":
            return self._generate_timeline_hypotheses(signal)
        
        elif signal.signal_type == "degree_anomaly":
            return self._generate_role_hypotheses(signal)
        
        elif signal.signal_type == "isolated_cluster":
            return self._generate_missing_link_hypotheses(signal)
        
        return []
    
    def _generate_conflict_hypotheses(self, signal: Signal) -> List[Dict[str, Any]]:
        """Generate hypotheses for conflict clusters."""
        entity = signal.entities[0]
        
        return [{
            "name": f"conflict_resolution_{entity.replace(' ', '_')}",
            "target_pattern": f"resolve conflicts about '{entity}'",
            "activation_conditions": ["high conflict density"],
            "evaluation_strategy": ["lens_swarm", "convergence", "conflict_clustering"],
            "success_criteria": ["reduced conflict clusters", "stable resolution"],
            "description": f"Resolve conflicting claims about '{entity}'",
            "signal_source": signal.to_dict()
        }]
    
    def _generate_alias_hypotheses(self, signal: Signal) -> List[Dict[str, Any]]:
        """Generate alias hypotheses for co-occurrence anomalies."""
        if len(signal.entities) < 2:
            return []
        
        entity_a, entity_b = signal.entities[0], signal.entities[1]
        
        # Generate competing set: same vs different
        return [
            {
                "name": f"alias_same_{entity_a.replace(' ', '_')}_{entity_b.replace(' ', '_')}",
                "target_pattern": f"evidence that '{entity_a}' and '{entity_b}' are the same",
                "activation_conditions": ["high co-occurrence"],
                "evaluation_strategy": ["lens_swarm", "orthogonal_pressure", "convergence"],
                "success_criteria": ["stable cluster", "temporal consistency"],
                "assumption": f"{entity_a} == {entity_b}",
                "description": f"Test whether '{entity_a}' and '{entity_b}' are the same entity",
                "signal_source": signal.to_dict()
            },
            {
                "name": f"alias_different_{entity_a.replace(' ', '_')}_{entity_b.replace(' ', '_')}",
                "target_pattern": f"evidence that '{entity_a}' and '{entity_b}' are distinct",
                "activation_conditions": ["high co-occurrence"],
                "evaluation_strategy": ["lens_swarm", "temporal_pressure", "convergence"],
                "success_criteria": ["temporal violations", "location impossibilities"],
                "assumption": f"{entity_a} != {entity_b}",
                "description": f"Test whether '{entity_a}' and '{entity_b}' are different entities",
                "signal_source": signal.to_dict()
            }
        ]
    
    def _generate_stability_hypotheses(self, signal: Signal) -> List[Dict[str, Any]]:
        """Generate hypotheses for unstable regions."""
        entity = signal.entities[0]
        
        return [{
            "name": f"stabilize_{entity.replace(' ', '_')}",
            "target_pattern": f"find stable claims about '{entity}'",
            "activation_conditions": ["low claim strength"],
            "evaluation_strategy": ["lens_swarm", "orthogonal_pressure", "fragment_intervention"],
            "success_criteria": ["increased claim strength", "stable edges"],
            "description": f"Stabilize claims about '{entity}'",
            "signal_source": signal.to_dict()
        }]
    
    def _generate_timeline_hypotheses(self, signal: Signal) -> List[Dict[str, Any]]:
        """Generate timeline consistency hypotheses."""
        entities = signal.entities
        entity_str = " and ".join([f"'{e}'" for e in entities[:2]])
        
        return [
            {
                "name": f"timeline_consistent_{entities[0].replace(' ', '_')}",
                "target_pattern": f"events involving {entity_str} are temporally consistent",
                "activation_conditions": ["temporal references"],
                "evaluation_strategy": ["lens_swarm", "temporal_pressure", "convergence"],
                "success_criteria": ["no temporal violations", "high temporal pressure"],
                "assumption": "timeline is consistent",
                "description": f"Test temporal consistency of events involving {entity_str}",
                "signal_source": signal.to_dict()
            },
            {
                "name": f"timeline_impossible_{entities[0].replace(' ', '_')}",
                "target_pattern": f"events involving {entity_str} contain impossibilities",
                "activation_conditions": ["temporal violations"],
                "evaluation_strategy": ["lens_swarm", "temporal_pressure"],
                "success_criteria": ["detected violations", "low temporal pressure"],
                "assumption": "timeline contains impossibilities",
                "description": f"Test for temporal impossibilities involving {entity_str}",
                "signal_source": signal.to_dict()
            }
        ]
    
    def _generate_role_hypotheses(self, signal: Signal) -> List[Dict[str, Any]]:
        """Generate role/importance hypotheses for degree anomalies."""
        entity = signal.entities[0]
        degree = signal.evidence.get("degree", 0)
        
        return [{
            "name": f"hub_role_{entity.replace(' ', '_')}",
            "target_pattern": f"'{entity}' plays central role (hub)",
            "activation_conditions": ["high degree"],
            "evaluation_strategy": ["lens_swarm", "graph_pressure", "convergence"],
            "success_criteria": ["stable hub position", "connected components"],
            "description": f"Test whether '{entity}' is a network hub ({degree} connections)",
            "signal_source": signal.to_dict()
        }]
    
    def _generate_missing_link_hypotheses(self, signal: Signal) -> List[Dict[str, Any]]:
        """Generate missing link hypotheses for isolated clusters."""
        entity = signal.entities[0]
        
        return [{
            "name": f"missing_link_{entity.replace(' ', '_')}",
            "target_pattern": f"find connections from '{entity}' to main graph",
            "activation_conditions": ["isolated cluster"],
            "evaluation_strategy": ["lens_swarm", "graph_pressure", "convergence"],
            "success_criteria": ["found connections", "reduced isolation"],
            "description": f"Find missing links connecting '{entity}' to main graph",
            "signal_source": signal.to_dict()
        }]


# ═══════════════════════════════════════════════════════════════════════════
# HYPOTHESIS DISCOVERY ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class HypothesisDiscoveryEngine:
    """
    Autonomous hypothesis discovery and testing.
    
    Orchestrates:
    1. Signal detection
    2. Hypothesis generation
    3. Adversarial testing (Phase 16.5)
    4. Result ranking
    """
    
    def __init__(self, memory_dir: str, models_dir: str):
        self.memory_dir = Path(memory_dir)
        self.models_dir = Path(models_dir)
        self.db_path = self.memory_dir / "memory.db"
        
        self.signal_detector = SignalDetector(str(self.memory_dir))
        self.hypothesis_generator = HypothesisGenerator()
        
        # Initialize discovery tracking database
        self._init_db()
    
    def _init_db(self):
        """Initialize database for tracking discoveries."""
        conn = sqlite3.connect(str(self.db_path))
        cur = conn.cursor()
        
        # Track discovered signals
        cur.execute("""
            CREATE TABLE IF NOT EXISTS discovered_signals (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_type     TEXT NOT NULL,
                strength        REAL NOT NULL,
                entities        TEXT NOT NULL,  -- JSON array
                evidence        TEXT NOT NULL,  -- JSON
                description     TEXT,
                discovered_at   TEXT NOT NULL
            )
        """)
        
        # Track generated hypotheses
        cur.execute("""
            CREATE TABLE IF NOT EXISTS generated_hypotheses (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id       INTEGER REFERENCES discovered_signals(id),
                hypothesis_name TEXT NOT NULL,
                target_pattern  TEXT NOT NULL,
                assumption      TEXT,
                test_result     TEXT,  -- confirmed|refuted|inconclusive|untested
                composite_score REAL,
                generated_at    TEXT NOT NULL,
                tested_at       TEXT
            )
        """)
        
        # Track hypothesis rankings
        cur.execute("""
            CREATE TABLE IF NOT EXISTS hypothesis_rankings (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                hypothesis_id   INTEGER REFERENCES generated_hypotheses(id),
                impact_score    REAL,
                stability_score REAL,
                novelty_score   REAL,
                pressure_reduction REAL,
                overall_rank    REAL,
                ranked_at       TEXT NOT NULL
            )
        """)
        
        conn.commit()
        conn.close()
    
    def discover_and_test(
        self,
        corpus: List[str],
        min_signal_strength: float = 0.5,
        max_hypotheses: int = 10,
        verbose: bool = True
    ) -> Dict[str, Any]:
        """
        Full autonomous discovery pipeline.
        
        1. Detect signals
        2. Generate hypotheses
        3. Test hypotheses (via Phase 16.5)
        4. Rank results
        
        Args:
            corpus: Document corpus
            min_signal_strength: Minimum signal strength to investigate
            max_hypotheses: Max hypotheses to test
            verbose: Print progress
        
        Returns:
            Discovery report
        """
        if verbose:
            print(f"\n{'═'*70}")
            print(f"PHASE 17: AUTONOMOUS HYPOTHESIS DISCOVERY")
            print(f"{'═'*70}\n")
        
        # ── STEP 1: Detect signals ──
        if verbose:
            print("[1/4] Detecting signals...")
        
        signals = self.signal_detector.detect_all_signals(min_strength=min_signal_strength)
        
        if verbose:
            print(f"  → Found {len(signals)} strong signals")
            for i, signal in enumerate(signals[:5], 1):
                print(f"    {i}. {signal.signal_type:25s} | {signal.strength:.2f} | {signal.description[:50]}")
        
        # Store signals
        self._store_signals(signals)
        
        # ── STEP 2: Generate hypotheses ──
        if verbose:
            print(f"\n[2/4] Generating hypotheses...")
        
        all_hypotheses = []
        for signal in signals[:max_hypotheses]:
            hyp_specs = self.hypothesis_generator.generate_from_signal(signal)
            all_hypotheses.extend(hyp_specs)
        
        if verbose:
            print(f"  → Generated {len(all_hypotheses)} candidate hypotheses")
            for i, hyp in enumerate(all_hypotheses[:5], 1):
                print(f"    {i}. {hyp['name'][:50]}")
        
        # Store generated hypotheses
        self._store_generated_hypotheses(all_hypotheses, signals)
        
        # ── STEP 3: Test hypotheses (via Phase 16.5) ──
        if verbose:
            print(f"\n[3/4] Testing hypotheses...")
        
        test_results = []
        
        # Check if Phase 16.5 available
        try:
            from hypothesis_engine import HypothesisEngine, Hypothesis, CompetingHypothesisSet
            
            engine = HypothesisEngine(str(self.memory_dir), str(self.models_dir))
            
            # Group competing hypotheses
            competing_groups = self._group_competing_hypotheses(all_hypotheses)
            
            for group_name, hyp_specs in competing_groups.items():
                if len(hyp_specs) > 1:
                    # Competing set
                    if verbose:
                        print(f"\n  Testing competing set: {group_name}")
                    
                    # Convert to Hypothesis objects
                    variants = [self._spec_to_hypothesis(spec) for spec in hyp_specs]
                    
                    competing_set = CompetingHypothesisSet(
                        name=group_name,
                        description=f"Auto-discovered competing hypotheses from {group_name}",
                        variants=variants
                    )
                    
                    # Run competition (this would call full pipeline)
                    # For now, simulate result
                    if verbose:
                        print(f"    [simulation] Would run Phase 16.5 competition")
                    
                    test_results.append({
                        "group": group_name,
                        "type": "competing",
                        "n_variants": len(variants),
                        "status": "simulated"
                    })
                
                else:
                    # Single hypothesis
                    if verbose:
                        print(f"\n  Testing single hypothesis: {hyp_specs[0]['name']}")
                    
                    test_results.append({
                        "hypothesis": hyp_specs[0]['name'],
                        "type": "single",
                        "status": "simulated"
                    })
        
        except ImportError:
            if verbose:
                print("    [warning] Phase 16.5 not available, skipping testing")
        
        # ── STEP 4: Rank hypotheses ──
        if verbose:
            print(f"\n[4/4] Ranking hypotheses...")
        
        rankings = self._rank_hypotheses(all_hypotheses, signals)
        
        if verbose:
            print(f"  → Ranked {len(rankings)} hypotheses")
            print(f"\n  Top 5 by overall rank:")
            for i, ranking in enumerate(rankings[:5], 1):
                print(f"    {i}. {ranking['hypothesis_name'][:45]:45s} | score: {ranking['overall_rank']:.3f}")
        
        # Build report
        report = {
            "n_signals_detected": len(signals),
            "n_hypotheses_generated": len(all_hypotheses),
            "n_hypotheses_tested": len(test_results),
            "signals": [s.to_dict() for s in signals[:10]],
            "hypotheses": all_hypotheses[:10],
            "rankings": rankings[:10],
            "test_results": test_results
        }
        
        if verbose:
            print(f"\n{'═'*70}")
            print(f"DISCOVERY COMPLETE")
            print(f"{'═'*70}")
            print(f"  Signals detected:      {report['n_signals_detected']}")
            print(f"  Hypotheses generated:  {report['n_hypotheses_generated']}")
            print(f"  Hypotheses tested:     {report['n_hypotheses_tested']}")
            print(f"{'═'*70}\n")
        
        return report
    
    def _spec_to_hypothesis(self, spec: Dict[str, Any]):
        """Convert hypothesis spec dict to Hypothesis object."""
        from hypothesis_engine import Hypothesis
        
        return Hypothesis(
            name=spec["name"],
            target_pattern=spec["target_pattern"],
            activation_conditions=spec["activation_conditions"],
            evaluation_strategy=spec["evaluation_strategy"],
            success_criteria=spec["success_criteria"],
            description=spec.get("description"),
            assumption=spec.get("assumption")
        )
    
    def _group_competing_hypotheses(self, hypotheses: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group hypotheses into competing sets."""
        groups = defaultdict(list)
        
        for hyp in hypotheses:
            # Extract base name (before last underscore)
            name = hyp["name"]
            
            # Group alias hypotheses
            if "alias_same_" in name or "alias_different_" in name:
                base = name.replace("alias_same_", "").replace("alias_different_", "")
                groups[f"alias_{base}"].append(hyp)
            
            # Group timeline hypotheses
            elif "timeline_consistent_" in name or "timeline_impossible_" in name:
                base = name.replace("timeline_consistent_", "").replace("timeline_impossible_", "")
                groups[f"timeline_{base}"].append(hyp)
            
            # Single hypothesis
            else:
                groups[name].append(hyp)
        
        return dict(groups)
    
    def _store_signals(self, signals: List[Signal]):
        """Store detected signals in database."""
        conn = sqlite3.connect(str(self.db_path))
        cur = conn.cursor()
        
        for signal in signals:
            cur.execute("""
                INSERT INTO discovered_signals 
                (signal_type, strength, entities, evidence, description, discovered_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                signal.signal_type,
                signal.strength,
                json.dumps(signal.entities),
                json.dumps(signal.evidence),
                signal.description,
                time.strftime("%Y-%m-%d %H:%M:%S")
            ))
        
        conn.commit()
        conn.close()
    
    def _store_generated_hypotheses(self, hypotheses: List[Dict[str, Any]], signals: List[Signal]):
        """Store generated hypotheses in database."""
        conn = sqlite3.connect(str(self.db_path))
        cur = conn.cursor()
        
        # Get most recent signal IDs
        signal_ids = cur.execute("""
            SELECT id FROM discovered_signals
            ORDER BY id DESC
            LIMIT ?
        """, (len(signals),)).fetchall()
        signal_ids = [sid[0] for sid in signal_ids]
        
        for i, hyp in enumerate(hypotheses):
            signal_id = signal_ids[min(i, len(signal_ids)-1)] if signal_ids else None
            
            cur.execute("""
                INSERT INTO generated_hypotheses
                (signal_id, hypothesis_name, target_pattern, assumption, 
                 test_result, generated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                signal_id,
                hyp["name"],
                hyp["target_pattern"],
                hyp.get("assumption"),
                "untested",
                time.strftime("%Y-%m-%d %H:%M:%S")
            ))
        
        conn.commit()
        conn.close()
    
    def _rank_hypotheses(
        self, 
        hypotheses: List[Dict[str, Any]], 
        signals: List[Signal]
    ) -> List[Dict[str, Any]]:
        """
        Rank hypotheses by impact, stability, novelty, pressure reduction.
        
        Returns ranked list.
        """
        rankings = []
        
        # Build signal strength map
        signal_strength_map = {s.description: s.strength for s in signals}
        
        for hyp in hypotheses:
            # IMPACT: How much would resolving this help?
            # Heuristic: signal strength
            signal_source = hyp.get("signal_source", {})
            impact_score = signal_source.get("strength", 0.5)
            
            # STABILITY: How robust is the signal?
            # Heuristic: evidence count
            evidence = signal_source.get("evidence", {})
            n_evidence = len(evidence)
            stability_score = min(n_evidence / 5.0, 1.0)
            
            # NOVELTY: Have we tested this before?
            # Heuristic: always novel in first implementation
            novelty_score = 1.0
            
            # PRESSURE REDUCTION: Would this reduce unresolved pressure?
            # Heuristic: conflict/unstable signals have high reduction potential
            signal_type = signal_source.get("signal_type", "")
            if signal_type in ["conflict_cluster", "unstable_region", "temporal_inconsistency"]:
                pressure_reduction = 0.9
            else:
                pressure_reduction = 0.5
            
            # OVERALL RANK: Weighted average
            overall_rank = (
                impact_score * 0.3 +
                stability_score * 0.2 +
                novelty_score * 0.2 +
                pressure_reduction * 0.3
            )
            
            rankings.append({
                "hypothesis_name": hyp["name"],
                "impact_score": impact_score,
                "stability_score": stability_score,
                "novelty_score": novelty_score,
                "pressure_reduction": pressure_reduction,
                "overall_rank": overall_rank
            })
        
        # Sort by overall rank
        rankings.sort(key=lambda r: r["overall_rank"], reverse=True)
        
        # Store rankings
        self._store_rankings(rankings)
        
        return rankings
    
    def _store_rankings(self, rankings: List[Dict[str, Any]]):
        """Store hypothesis rankings in database."""
        conn = sqlite3.connect(str(self.db_path))
        cur = conn.cursor()
        
        # Get hypothesis IDs
        hyp_ids = {}
        for ranking in rankings:
            hyp_id = cur.execute("""
                SELECT id FROM generated_hypotheses
                WHERE hypothesis_name = ?
                ORDER BY id DESC
                LIMIT 1
            """, (ranking["hypothesis_name"],)).fetchone()
            
            if hyp_id:
                hyp_ids[ranking["hypothesis_name"]] = hyp_id[0]
        
        # Store rankings
        for ranking in rankings:
            hyp_id = hyp_ids.get(ranking["hypothesis_name"])
            if hyp_id:
                cur.execute("""
                    INSERT INTO hypothesis_rankings
                    (hypothesis_id, impact_score, stability_score, novelty_score,
                     pressure_reduction, overall_rank, ranked_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    hyp_id,
                    ranking["impact_score"],
                    ranking["stability_score"],
                    ranking["novelty_score"],
                    ranking["pressure_reduction"],
                    ranking["overall_rank"],
                    time.strftime("%Y-%m-%d %H:%M:%S")
                ))
        
        conn.commit()
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# CLI INTERFACE
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Autonomous hypothesis discovery (Phase 17)"
    )
    parser.add_argument("--memory-dir", default="/tmp/bonfyre-memory",
                        help="Memory directory")
    parser.add_argument("--models-dir", default="/tmp/bonfyre-models",
                        help="Models directory")
    parser.add_argument("--corpus", nargs="+",
                        help="Document paths for discovery")
    parser.add_argument("--min-signal-strength", type=float, default=0.5,
                        help="Minimum signal strength (0.0-1.0)")
    parser.add_argument("--max-hypotheses", type=int, default=10,
                        help="Maximum hypotheses to generate")
    parser.add_argument("--signals-only", action="store_true",
                        help="Only detect signals (no hypothesis generation)")
    parser.add_argument("--output", type=str,
                        help="Output file for discovery report (JSON)")
    
    args = parser.parse_args()
    
    if args.signals_only:
        # Just detect signals
        detector = SignalDetector(args.memory_dir)
        signals = detector.detect_all_signals(min_strength=args.min_signal_strength)
        
        print(f"\nDetected {len(signals)} signals:\n")
        for i, signal in enumerate(signals, 1):
            print(f"{i}. [{signal.signal_type:25s}] {signal.strength:.2f} | {signal.description}")
        print()
    
    elif args.corpus:
        # Full discovery pipeline
        engine = HypothesisDiscoveryEngine(args.memory_dir, args.models_dir)
        
        report = engine.discover_and_test(
            corpus=args.corpus,
            min_signal_strength=args.min_signal_strength,
            max_hypotheses=args.max_hypotheses,
            verbose=True
        )
        
        # Save report if requested
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(report, f, indent=2)
            print(f"✓ Report saved to {args.output}")
    
    else:
        parser.print_help()
