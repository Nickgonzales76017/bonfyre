#!/usr/bin/env python3
"""
hypothesis_engine.py

PHASE 16.5: ADVERSARIAL HYPOTHESIS ENGINE

PURPOSE:
--------
Phase 16 added PURPOSE: test specific ideas.
Phase 16.5 adds INTELLECTUAL COMPETITION: run competing explanations through
pressure and see which survives.

CORE INSIGHT:
-------------
Phase 16:   Test single hypothesis → confirmed/refuted
Phase 16.5: Run competing hypotheses → compare relative strength

NOT: "Is hypothesis X true?"
BUT: "Which of these competing explanations best survives reality testing?"

ARCHITECTURE:
-------------
1. COMPETING HYPOTHESES:
   - Multiple variants of same question
   - Example: "A == B" vs "A != B"

2. CONDITIONAL EVALUATION:
   - IF conditions → EXPECT outcome under pressure
   - Track whether expectations hold

3. RELATIVE STRENGTH SCORING:
   - Not just pass/fail
   - Compare survival rates between competing hypotheses

4. HYPOTHESIS CHAINING:
   - Output of one hypothesis feeds into next
   - Example: alias → network → timeline

5. FRAGILITY TRACKING:
   - Does hypothesis collapse under:
     • Perturbation pressure
     • Temporal pressure
     • Fragment-only view
     • Representation pressure

WORKFLOW:
---------
For competing hypotheses:
  1. Run each variant through full pipeline
  2. Measure survival under each pressure type
  3. Compare relative strength
  4. Identify fragility points
  5. Determine winner by robustness

BUILT-IN COMPETING SETS:
------------------------
1. alias_identity_test:
   - same_person (A == B)
   - different_people (A != B)

2. timeline_consistency_test:
   - consistent_sequence
   - impossible_sequence

3. network_structure_test:
   - connected_network
   - isolated_actors

NEW CAPABILITY:
---------------
Phase 16:   "Test whether A == B"
Phase 16.5: "Test A == B vs A != B and see which survives pressure"

This is INTELLECTUAL COMPETITION — the system argues with itself
and converges on the best explanation.
"""

import json
import os
import sqlite3
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Any, Optional

# ═══════════════════════════════════════════════════════════════════════════
# HYPOTHESIS DATA MODEL (PHASE 16.5: ADVERSARIAL)
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ConditionalExpectation:
    """
    IF conditions → EXPECT outcome under pressure
    
    Example:
        IF high_name_ambiguity:
            EXPECT temporal_pressure > 0.6
            EXPECT survival_rate > 0.3
    """
    conditions: List[str]           # Activation conditions
    expected_outcomes: Dict[str, float]  # Expected metrics
    
    def check(self, result: Dict[str, Any]) -> bool:
        """Check if expectations were met."""
        for metric, expected_value in self.expected_outcomes.items():
            actual_value = result.get(metric, 0.0)
            if actual_value < expected_value:
                return False
        return True


@dataclass
class HypothesisFragility:
    """
    Track which pressure types cause hypothesis to collapse.
    
    Example:
        graph_pressure: 0.8  (survives)
        temporal_pressure: 0.2  (collapses!)
        frequency_pressure: 0.7  (survives)
    """
    graph_pressure: Optional[float] = None
    temporal_pressure: Optional[float] = None
    frequency_pressure: Optional[float] = None
    perturbation_pressure: Optional[float] = None
    representation_pressure: Optional[float] = None
    
    def fragile_points(self, threshold: float = 0.5) -> List[str]:
        """Return pressure types where hypothesis is fragile."""
        fragile = []
        for pressure_type in ['graph', 'temporal', 'frequency', 'perturbation', 'representation']:
            value = getattr(self, f"{pressure_type}_pressure")
            if value is not None and value < threshold:
                fragile.append(pressure_type)
        return fragile
    
    def robustness_score(self) -> float:
        """Overall robustness across all pressure types."""
        pressures = [
            self.graph_pressure,
            self.temporal_pressure,
            self.frequency_pressure,
            self.perturbation_pressure,
            self.representation_pressure
        ]
        valid_pressures = [p for p in pressures if p is not None]
        return sum(valid_pressures) / len(valid_pressures) if valid_pressures else 0.0


@dataclass
class Hypothesis:
    """
    A testable theory about the corpus.
    
    Each hypothesis is a claim about reality that can be validated
    by running the full Bonfyre pipeline (swarm → pressure → intervention).
    
    PHASE 16.5 ADDITIONS:
    - assumption:            Core assumption (e.g., "A == B")
    - conditional_expectations: IF/THEN expectations
    - chain_input_from:      Name of hypothesis whose output feeds into this one
    """
    name: str
    target_pattern: str
    activation_conditions: List[str]
    evaluation_strategy: List[str]
    success_criteria: List[str]
    
    # Optional metadata
    description: Optional[str] = None
    lens_priorities: Optional[Dict[str, float]] = None
    expected_claim_types: Optional[List[str]] = None
    
    # PHASE 16.5: Adversarial features
    assumption: Optional[str] = None  # Core assumption being tested
    conditional_expectations: Optional[List[ConditionalExpectation]] = None
    chain_input_from: Optional[str] = None  # Hypothesis to chain from
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for storage."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Hypothesis":
        """Deserialize from dict."""
        # Handle nested objects
        if "conditional_expectations" in data and data["conditional_expectations"]:
            data["conditional_expectations"] = [
                ConditionalExpectation(**exp) if isinstance(exp, dict) else exp
                for exp in data["conditional_expectations"]
            ]
        return cls(**data)


@dataclass
class CompetingHypothesisSet:
    """
    A set of mutually exclusive hypotheses tested against each other.
    
    Example:
        name: "alias_identity_test"
        variants: [
            Hypothesis(name="same_person", assumption="A == B", ...),
            Hypothesis(name="different_people", assumption="A != B", ...)
        ]
    
    The variants compete — only one can be the best explanation.
    """
    name: str
    description: str
    variants: List[Hypothesis]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "variants": [v.to_dict() for v in self.variants]
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Hypothesis":
        """Deserialize from dict."""
        return cls(**data)


# ═══════════════════════════════════════════════════════════════════════════
# BUILT-IN HYPOTHESIS TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════

BUILTIN_HYPOTHESES = {
    "alias_network": Hypothesis(
        name="alias_network",
        target_pattern="entities that may refer to the same person",
        activation_conditions=["high name ambiguity", "multiple name variants"],
        evaluation_strategy=["lens_swarm", "orthogonal_pressure", "fragment_intervention"],
        success_criteria=["stable cluster under pressure", "temporal consistency"],
        description="Test whether multiple names/aliases refer to the same individual",
        lens_priorities={"entity_linking": 1.5, "identity": 1.3},
        expected_claim_types=["same_as", "alias_of", "also_known_as"]
    ),
    
    "timeline_reconstruction": Hypothesis(
        name="timeline_reconstruction",
        target_pattern="chronological sequence of events",
        activation_conditions=["multiple temporal references", "dated events"],
        evaluation_strategy=["lens_swarm", "temporal_pressure", "convergence"],
        success_criteria=["no temporal violations", "stable event sequence"],
        description="Build a consistent timeline from temporal references",
        lens_priorities={"temporal": 1.5, "event_extraction": 1.3},
        expected_claim_types=["happened_on", "located_at", "arrived_on", "departed_on"]
    ),
    
    "network_discovery": Hypothesis(
        name="network_discovery",
        target_pattern="key actors and their relationships",
        activation_conditions=["multiple entities", "relationship mentions"],
        evaluation_strategy=["lens_swarm", "graph_pressure", "convergence"],
        success_criteria=["stable relationship graph", "connected components"],
        description="Discover the network of actors and their connections",
        lens_priorities={"relationship": 1.5, "entity_linking": 1.2},
        expected_claim_types=["knows", "works_with", "associated_with", "connected_to"]
    ),
    
    "contradiction_detection": Hypothesis(
        name="contradiction_detection",
        target_pattern="conflicting claims about same fact",
        activation_conditions=["multiple sources", "overlapping topics"],
        evaluation_strategy=["lens_swarm", "convergence", "conflict_clustering"],
        success_criteria=["identified conflict clusters", "stable contradictions"],
        description="Find where sources disagree about the same facts",
        lens_priorities={"fact_extraction": 1.4, "claim_detection": 1.3},
        expected_claim_types=["contradicts", "conflicts_with", "disagrees_with"]
    ),
    
    "gap_identification": Hypothesis(
        name="gap_identification",
        target_pattern="missing information or unexplained transitions",
        activation_conditions=["temporal gaps", "logical discontinuities"],
        evaluation_strategy=["lens_swarm", "temporal_pressure", "graph_pressure"],
        success_criteria=["identified missing links", "fragile regions flagged"],
        description="Identify what's missing from the record",
        lens_priorities={"temporal": 1.3, "event_extraction": 1.2},
        expected_claim_types=["gap_before", "gap_after", "missing_connection"]
    ),
}


# ═══════════════════════════════════════════════════════════════════════════
# BUILT-IN COMPETING HYPOTHESIS SETS (PHASE 16.5)
# ═══════════════════════════════════════════════════════════════════════════

COMPETING_HYPOTHESIS_SETS = {
    "alias_identity_test": CompetingHypothesisSet(
        name="alias_identity_test",
        description="Test whether two entities are the same person vs different people",
        variants=[
            Hypothesis(
                name="same_person",
                target_pattern="evidence that A and B are the same person",
                activation_conditions=["name similarity", "overlapping contexts"],
                evaluation_strategy=["lens_swarm", "orthogonal_pressure", "convergence"],
                success_criteria=["stable cluster under pressure", "temporal consistency"],
                assumption="A == B",
                lens_priorities={"entity_linking": 2.0, "identity": 1.8},
                expected_claim_types=["same_as", "alias_of", "also_known_as"],
                conditional_expectations=[
                    ConditionalExpectation(
                        conditions=["high name similarity"],
                        expected_outcomes={"temporal_pressure": 0.6, "survival_rate": 0.3}
                    )
                ]
            ),
            Hypothesis(
                name="different_people",
                target_pattern="evidence that A and B are distinct individuals",
                activation_conditions=["temporal conflicts", "location conflicts"],
                evaluation_strategy=["lens_swarm", "temporal_pressure", "convergence"],
                success_criteria=["temporal violations", "location impossibilities"],
                assumption="A != B",
                lens_priorities={"temporal": 2.0, "location": 1.8},
                expected_claim_types=["different_from", "conflicts_with"],
                conditional_expectations=[
                    ConditionalExpectation(
                        conditions=["temporal conflicts"],
                        expected_outcomes={"temporal_pressure": 0.0, "survival_rate": 0.7}
                    )
                ]
            ),
        ]
    ),
    
    "timeline_consistency_test": CompetingHypothesisSet(
        name="timeline_consistency_test",
        description="Test whether events form consistent sequence vs impossible sequence",
        variants=[
            Hypothesis(
                name="consistent_sequence",
                target_pattern="events that form coherent chronology",
                activation_conditions=["temporal references", "dated events"],
                evaluation_strategy=["lens_swarm", "temporal_pressure", "convergence"],
                success_criteria=["no temporal violations", "stable event sequence"],
                assumption="timeline is internally consistent",
                lens_priorities={"temporal": 2.0, "event_extraction": 1.5},
                conditional_expectations=[
                    ConditionalExpectation(
                        conditions=["dated events"],
                        expected_outcomes={"temporal_pressure": 0.8, "n_stable_edges": 10}
                    )
                ]
            ),
            Hypothesis(
                name="impossible_sequence",
                target_pattern="events that cannot coexist temporally",
                activation_conditions=["simultaneity violations", "causality breaks"],
                evaluation_strategy=["lens_swarm", "temporal_pressure"],
                success_criteria=["detected violations", "low temporal pressure"],
                assumption="timeline contains impossibilities",
                lens_priorities={"temporal": 2.0},
                conditional_expectations=[
                    ConditionalExpectation(
                        conditions=["simultaneity detected"],
                        expected_outcomes={"temporal_pressure": 0.0}
                    )
                ]
            ),
        ]
    ),
    
    "network_structure_test": CompetingHypothesisSet(
        name="network_structure_test",
        description="Test whether entities form connected network vs isolated actors",
        variants=[
            Hypothesis(
                name="connected_network",
                target_pattern="entities with established relationships",
                activation_conditions=["relationship mentions", "multiple actors"],
                evaluation_strategy=["lens_swarm", "graph_pressure", "convergence"],
                success_criteria=["connected components", "stable relationship graph"],
                assumption="actors form interconnected network",
                lens_priorities={"relationship": 2.0, "entity_linking": 1.5},
                conditional_expectations=[
                    ConditionalExpectation(
                        conditions=["multiple actors"],
                        expected_outcomes={"n_stable_edges": 5, "graph_pressure": 0.7}
                    )
                ]
            ),
            Hypothesis(
                name="isolated_actors",
                target_pattern="entities with no clear relationships",
                activation_conditions=["entity mentions", "no relationship indicators"],
                evaluation_strategy=["lens_swarm", "graph_pressure"],
                success_criteria=["isolated nodes", "low edge density"],
                assumption="actors operate independently",
                lens_priorities={"entity_linking": 1.5},
                conditional_expectations=[
                    ConditionalExpectation(
                        conditions=["no relationships"],
                        expected_outcomes={"n_stable_edges": 0, "graph_pressure": 0.3}
                    )
                ]
            ),
        ]
    ),
}


# ═══════════════════════════════════════════════════════════════════════════
# HYPOTHESIS ENGINE (PHASE 16.5: ADVERSARIAL CAPABILITIES)
# ═══════════════════════════════════════════════════════════════════════════

class HypothesisEngine:
    """
    Orchestrates hypothesis-driven investigation.
    
    This is the PURPOSE layer that sits above Phases 12-15.
    Instead of blindly processing documents, we now test specific theories.
    """
    
    def __init__(self, memory_dir: str, models_dir: str):
        self.memory_dir = Path(memory_dir)
        self.models_dir = Path(models_dir)
        self.db_path = self.memory_dir / "memory.db"
        
        # Initialize database
        self._init_db()
        
        # Load existing hypotheses
        self.hypotheses: Dict[str, Hypothesis] = {}
        self._load_hypotheses()
    
    def _init_db(self):
        """Initialize database schema for hypothesis tracking."""
        conn = sqlite3.connect(str(self.db_path))
        cur = conn.cursor()
        
        # Track registered hypotheses
        cur.execute("""
            CREATE TABLE IF NOT EXISTS hypotheses (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                name                    TEXT UNIQUE NOT NULL,
                target_pattern          TEXT NOT NULL,
                activation_conditions   TEXT NOT NULL,  -- JSON array
                evaluation_strategy     TEXT NOT NULL,  -- JSON array
                success_criteria        TEXT NOT NULL,  -- JSON array
                description             TEXT,
                lens_priorities         TEXT,           -- JSON dict
                expected_claim_types    TEXT,           -- JSON array
                created_at              TEXT NOT NULL
            )
        """)
        
        # Track hypothesis evaluations
        cur.execute("""
            CREATE TABLE IF NOT EXISTS hypothesis_evaluations (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                hypothesis_id           INTEGER REFERENCES hypotheses(id),
                corpus_hash             TEXT NOT NULL,
                n_docs                  INTEGER,
                n_claims_initial        INTEGER,
                n_claims_survived       INTEGER,
                survival_rate           REAL,
                n_stable_edges          INTEGER,
                n_fragile_edges         INTEGER,
                n_conflict_clusters     INTEGER,
                orthogonal_pressure_avg REAL,
                intervention_attempted  INTEGER DEFAULT 0,
                intervention_success    INTEGER DEFAULT 0,
                validation_status       TEXT NOT NULL,  -- confirmed|refuted|inconclusive
                evidence_snapshot       TEXT,           -- JSON of key claims
                created_at              TEXT NOT NULL
            )
        """)
        
        conn.commit()
        conn.close()
    
    def _load_hypotheses(self):
        """Load registered hypotheses from database."""
        conn = sqlite3.connect(str(self.db_path))
        cur = conn.cursor()
        
        rows = cur.execute("""
            SELECT name, target_pattern, activation_conditions, 
                   evaluation_strategy, success_criteria, description,
                   lens_priorities, expected_claim_types
            FROM hypotheses
        """).fetchall()
        
        for row in rows:
            name, target, conds, strategy, criteria, desc, priorities, claim_types = row
            
            hypothesis = Hypothesis(
                name=name,
                target_pattern=target,
                activation_conditions=json.loads(conds),
                evaluation_strategy=json.loads(strategy),
                success_criteria=json.loads(criteria),
                description=desc,
                lens_priorities=json.loads(priorities) if priorities else None,
                expected_claim_types=json.loads(claim_types) if claim_types else None
            )
            self.hypotheses[name] = hypothesis
        
        conn.close()
    
    def register_hypothesis(self, hypothesis: Hypothesis):
        """
        Register a new hypothesis for testing.
        
        Args:
            hypothesis: Hypothesis object to register
        """
        conn = sqlite3.connect(str(self.db_path))
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO hypotheses (
                    name, target_pattern, activation_conditions,
                    evaluation_strategy, success_criteria, description,
                    lens_priorities, expected_claim_types, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                hypothesis.name,
                hypothesis.target_pattern,
                json.dumps(hypothesis.activation_conditions),
                json.dumps(hypothesis.evaluation_strategy),
                json.dumps(hypothesis.success_criteria),
                hypothesis.description,
                json.dumps(hypothesis.lens_priorities) if hypothesis.lens_priorities else None,
                json.dumps(hypothesis.expected_claim_types) if hypothesis.expected_claim_types else None,
                time.strftime("%Y-%m-%d %H:%M:%S")
            ))
            conn.commit()
            self.hypotheses[hypothesis.name] = hypothesis
            print(f"[hypothesis_engine] Registered hypothesis: {hypothesis.name}")
        except sqlite3.IntegrityError:
            print(f"[hypothesis_engine] Hypothesis '{hypothesis.name}' already exists")
        finally:
            conn.close()
    
    def evaluate_hypothesis(
        self, 
        hypothesis: Hypothesis, 
        corpus: List[str],
        verbose: bool = True
    ) -> Dict[str, Any]:
        """
        Test a hypothesis using the full Bonfyre pipeline.
        
        Pipeline:
        1. Run hypothesis swarm      (Phase 12)
        2. Apply convergence         (Phase 13)
        3. Apply orthogonal pressure (Phase 14)
        4. Try structural intervention (Phase 15)
        5. Measure survival
        6. Determine validation status
        
        Args:
            hypothesis: Hypothesis to test
            corpus: List of document paths
            verbose: Print progress messages
        
        Returns:
            Evaluation result dict with validation status and evidence
        """
        if verbose:
            print(f"\n{'═'*70}")
            print(f"HYPOTHESIS EVALUATION: {hypothesis.name}")
            print(f"{'═'*70}")
            print(f"Target: {hypothesis.target_pattern}")
            print(f"Strategy: {' → '.join(hypothesis.evaluation_strategy)}")
            print(f"{'─'*70}\n")
        
        # Import pipeline components
        try:
            from scripts.hypothesis_swarm import HypothesisSwarm
            from scripts.convergence_engine import StructuralConvergenceEngine
            from scripts.orthogonal_pressure import OrthogonalPressure
            from scripts.structural_intervention import StructuralInterventionEngine
            from scripts.claim_graph import ClaimGraph
        except ImportError as e:
            return {
                "hypothesis_name": hypothesis.name,
                "validation_status": "error",
                "error": f"Failed to import pipeline components: {e}"
            }
        
        start_time = time.time()
        
        # ── STEP 1: Hypothesis Swarm (Phase 12) ──
        if verbose:
            print("[1/5] Running hypothesis swarm...")
        
        swarm = HypothesisSwarm(str(self.memory_dir), str(self.models_dir))
        
        # Use lens priorities if specified
        lens_config = None
        if hypothesis.lens_priorities:
            lens_config = hypothesis.lens_priorities
        
        swarm_result = swarm.run_swarm(corpus, lens_config=lens_config)
        n_claims_initial = swarm_result.get("n_claims_total", 0)
        
        if verbose:
            print(f"  → Generated {n_claims_initial} claims from {len(corpus)} documents")
        
        # ── STEP 2: Structural Convergence (Phase 13) ──
        if verbose:
            print("[2/5] Applying structural convergence...")
        
        convergence = StructuralConvergenceEngine(str(self.memory_dir), str(self.models_dir))
        convergence_result = convergence.run_convergence(
            corpus=corpus,
            iterations=5,
            pressure_threshold=1.5
        )
        
        n_stable_edges = convergence_result.get("n_stable_edges", 0)
        n_fragile_edges = convergence_result.get("n_fragile_edges", 0)
        n_conflict_clusters = convergence_result.get("n_conflict_clusters", 0)
        
        if verbose:
            print(f"  → Stable edges: {n_stable_edges}, Fragile: {n_fragile_edges}, Conflicts: {n_conflict_clusters}")
        
        # ── STEP 3: Orthogonal Pressure (Phase 14) ──
        if verbose:
            print("[3/5] Applying orthogonal pressure...")
        
        cg = ClaimGraph(str(self.memory_dir))
        cg.compute_orthogonal_pressure(corpus=corpus)
        cg.recompute_final_strength()
        
        # Get average orthogonal pressure for survived claims
        conn = sqlite3.connect(str(self.db_path))
        pressure_avg = conn.execute("""
            SELECT AVG(orthogonal_pressure)
            FROM claims
            WHERE orthogonal_pressure IS NOT NULL
              AND claim_strength > 0.3
        """).fetchone()[0] or 0.0
        conn.close()
        
        if verbose:
            print(f"  → Average orthogonal pressure: {pressure_avg:.3f}")
        
        # ── STEP 4: Structural Intervention (Phase 15) ──
        intervention_attempted = False
        intervention_success = False
        
        if "fragment_intervention" in hypothesis.evaluation_strategy:
            if verbose:
                print("[4/5] Attempting structural intervention...")
            
            # Get hot zones from convergence result
            hot_zones = convergence_result.get("hot_zones", [])
            
            if hot_zones:
                intervention_engine = StructuralInterventionEngine(
                    str(self.memory_dir), 
                    str(self.models_dir)
                )
                
                for hz in hot_zones[:3]:  # Try top 3 hot zones
                    result = intervention_engine.try_intervention(hz, corpus, strategy="auto")
                    intervention_attempted = True
                    if result.get("success"):
                        intervention_success = True
                        if verbose:
                            print(f"  → Intervention succeeded on hot zone {hz['id']}")
                        break
                
                if not intervention_success and verbose:
                    print(f"  → No successful interventions")
            else:
                if verbose:
                    print(f"  → No hot zones detected, skipping intervention")
        else:
            if verbose:
                print("[4/5] Structural intervention not in strategy, skipping...")
        
        # ── STEP 5: Measure Survival ──
        if verbose:
            print("[5/5] Measuring claim survival...")
        
        # Count claims that survived all tests
        conn = sqlite3.connect(str(self.db_path))
        n_claims_survived = conn.execute("""
            SELECT COUNT(*)
            FROM claims
            WHERE claim_strength > 0.3
              AND COALESCE(orthogonal_pressure, 1.0) > 0.5
        """).fetchone()[0]
        conn.close()
        
        survival_rate = n_claims_survived / n_claims_initial if n_claims_initial > 0 else 0.0
        
        if verbose:
            print(f"  → Survived: {n_claims_survived}/{n_claims_initial} ({survival_rate:.1%})")
        
        # ── STEP 6: Determine Validation Status ──
        validation_status = self._determine_validation_status(
            hypothesis=hypothesis,
            survival_rate=survival_rate,
            n_stable_edges=n_stable_edges,
            orthogonal_pressure_avg=pressure_avg,
            n_conflict_clusters=n_conflict_clusters
        )
        
        elapsed = time.time() - start_time
        
        if verbose:
            print(f"\n{'─'*70}")
            print(f"VALIDATION STATUS: {validation_status.upper()}")
            print(f"Elapsed: {elapsed:.1f}s")
            print(f"{'═'*70}\n")
        
        # Store evaluation result
        result = {
            "hypothesis_name": hypothesis.name,
            "validation_status": validation_status,
            "n_claims_initial": n_claims_initial,
            "n_claims_survived": n_claims_survived,
            "survival_rate": survival_rate,
            "n_stable_edges": n_stable_edges,
            "n_fragile_edges": n_fragile_edges,
            "n_conflict_clusters": n_conflict_clusters,
            "orthogonal_pressure_avg": pressure_avg,
            "intervention_attempted": intervention_attempted,
            "intervention_success": intervention_success,
            "elapsed_seconds": elapsed,
            "corpus_hash": self._compute_corpus_hash(corpus),
        }
        
        self._store_evaluation(hypothesis, result, corpus)
        
        return result
    
    def _determine_validation_status(
        self,
        hypothesis: Hypothesis,
        survival_rate: float,
        n_stable_edges: int,
        orthogonal_pressure_avg: float,
        n_conflict_clusters: int
    ) -> str:
        """
        Determine whether hypothesis is confirmed, refuted, or inconclusive.
        
        Logic:
        - CONFIRMED: High survival rate + stable graph + high pressure
        - REFUTED: Low survival rate or many conflicts
        - INCONCLUSIVE: Insufficient evidence
        """
        # Check success criteria
        criteria_met = 0
        
        if "stable cluster under pressure" in hypothesis.success_criteria:
            if survival_rate > 0.3 and n_stable_edges > 5 and orthogonal_pressure_avg > 0.6:
                criteria_met += 1
        
        if "temporal consistency" in hypothesis.success_criteria:
            # Would check temporal_pressure violations here
            # For now, assume met if high orthogonal pressure
            if orthogonal_pressure_avg > 0.6:
                criteria_met += 1
        
        if "no temporal violations" in hypothesis.success_criteria:
            # Would check temporal violations specifically
            if orthogonal_pressure_avg > 0.7:
                criteria_met += 1
        
        if "stable event sequence" in hypothesis.success_criteria:
            if n_stable_edges > 10:
                criteria_met += 1
        
        if "stable relationship graph" in hypothesis.success_criteria:
            if n_stable_edges > 10 and n_conflict_clusters < 3:
                criteria_met += 1
        
        if "connected components" in hypothesis.success_criteria:
            if n_stable_edges > 5:
                criteria_met += 1
        
        # Determine status based on criteria met
        total_criteria = len(hypothesis.success_criteria)
        
        if criteria_met >= total_criteria * 0.7:
            return "confirmed"
        elif criteria_met < total_criteria * 0.3:
            return "refuted"
        else:
            return "inconclusive"
    
    def _compute_corpus_hash(self, corpus: List[str]) -> str:
        """Compute hash of corpus for tracking."""
        import hashlib
        content = "|".join(sorted(corpus))
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def _store_evaluation(
        self, 
        hypothesis: Hypothesis, 
        result: Dict[str, Any],
        corpus: List[str]
    ):
        """Store evaluation result in database."""
        conn = sqlite3.connect(str(self.db_path))
        cur = conn.cursor()
        
        # Get hypothesis_id
        hypothesis_id = cur.execute(
            "SELECT id FROM hypotheses WHERE name = ?", 
            (hypothesis.name,)
        ).fetchone()
        
        if not hypothesis_id:
            print(f"[hypothesis_engine] WARNING: Hypothesis '{hypothesis.name}' not registered")
            conn.close()
            return
        
        hypothesis_id = hypothesis_id[0]
        
        # Store evaluation
        cur.execute("""
            INSERT INTO hypothesis_evaluations (
                hypothesis_id, corpus_hash, n_docs,
                n_claims_initial, n_claims_survived, survival_rate,
                n_stable_edges, n_fragile_edges, n_conflict_clusters,
                orthogonal_pressure_avg, intervention_attempted, intervention_success,
                validation_status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            hypothesis_id,
            result["corpus_hash"],
            len(corpus),
            result["n_claims_initial"],
            result["n_claims_survived"],
            result["survival_rate"],
            result["n_stable_edges"],
            result["n_fragile_edges"],
            result["n_conflict_clusters"],
            result["orthogonal_pressure_avg"],
            1 if result["intervention_attempted"] else 0,
            1 if result["intervention_success"] else 0,
            result["validation_status"],
            time.strftime("%Y-%m-%d %H:%M:%S")
        ))
        
        conn.commit()
        conn.close()
    
    def run_all_hypotheses(self, corpus: List[str]) -> List[Dict[str, Any]]:
        """
        Evaluate all registered hypotheses on the corpus.
        
        Args:
            corpus: List of document paths
        
        Returns:
            List of evaluation results
        """
        results = []
        
        print(f"\n{'═'*70}")
        print(f"EVALUATING {len(self.hypotheses)} HYPOTHESES ON {len(corpus)} DOCUMENTS")
        print(f"{'═'*70}\n")
        
        for i, (name, hypothesis) in enumerate(self.hypotheses.items(), 1):
            print(f"\n[{i}/{len(self.hypotheses)}] Testing: {name}")
            result = self.evaluate_hypothesis(hypothesis, corpus, verbose=True)
            results.append(result)
        
        # Summary
        print(f"\n{'═'*70}")
        print(f"HYPOTHESIS EVALUATION SUMMARY")
        print(f"{'═'*70}")
        for result in results:
            status = result["validation_status"].upper()
            symbol = "✓" if status == "CONFIRMED" else "✗" if status == "REFUTED" else "?"
            print(f"{symbol} {result['hypothesis_name']:30s} {status}")
        print(f"{'═'*70}\n")
        
        return results
    
    # ═══════════════════════════════════════════════════════════════════════
    # PHASE 16.5: ADVERSARIAL HYPOTHESIS METHODS
    # ═══════════════════════════════════════════════════════════════════════
    
    def compare_competing_hypotheses(
        self,
        competing_set: CompetingHypothesisSet,
        corpus: List[str],
        verbose: bool = True
    ) -> Dict[str, Any]:
        """
        Run competing hypotheses against each other and compare relative strength.
        
        This is INTELLECTUAL COMPETITION — the hypotheses argue with themselves.
        
        Args:
            competing_set: Set of mutually exclusive hypotheses
            corpus: Document corpus to test against
            verbose: Print progress
        
        Returns:
            {
              "winner": hypothesis_name,
              "results": [result1, result2, ...],
              "relative_strengths": {name: score, ...},
              "fragility_comparison": {name: fragility_obj, ...}
            }
        """
        if verbose:
            print(f"\n{'═'*70}")
            print(f"COMPETING HYPOTHESES: {competing_set.name}")
            print(f"{'═'*70}")
            print(f"Description: {competing_set.description}")
            print(f"Variants: {len(competing_set.variants)}")
            for v in competing_set.variants:
                print(f"  - {v.name:20s} | {v.assumption}")
            print(f"{'─'*70}\n")
        
        results = []
        fragilities = {}
        
        # Evaluate each variant
        for i, variant in enumerate(competing_set.variants, 1):
            if verbose:
                print(f"\n[{i}/{len(competing_set.variants)}] Testing: {variant.name} ({variant.assumption})")
            
            # Evaluate with fragility tracking
            result = self.evaluate_with_fragility(variant, corpus, verbose=verbose)
            results.append(result)
            fragilities[variant.name] = result.get("fragility", None)
        
        # Compare relative strengths
        relative_strengths = {}
        for result in results:
            # Composite score: survival_rate × orthogonal_pressure × robustness
            survival = result.get("survival_rate", 0.0)
            pressure = result.get("orthogonal_pressure_avg", 0.0)
            
            fragility = result.get("fragility")
            robustness = fragility.robustness_score() if fragility else 0.5
            
            composite_score = survival * pressure * robustness
            relative_strengths[result["hypothesis_name"]] = composite_score
        
        # Determine winner
        winner = max(relative_strengths, key=relative_strengths.get)
        
        if verbose:
            print(f"\n{'─'*70}")
            print("RELATIVE STRENGTHS:")
            for name, score in sorted(relative_strengths.items(), key=lambda x: x[1], reverse=True):
                symbol = "→" if name == winner else " "
                print(f"  {symbol} {name:20s} {score:.3f}")
            print(f"\nWINNER: {winner}")
            print(f"{'═'*70}\n")
        
        return {
            "competing_set": competing_set.name,
            "winner": winner,
            "results": results,
            "relative_strengths": relative_strengths,
            "fragility_comparison": {k: asdict(v) if v else None for k, v in fragilities.items()}
        }
    
    def evaluate_with_fragility(
        self,
        hypothesis: Hypothesis,
        corpus: List[str],
        verbose: bool = False
    ) -> Dict[str, Any]:
        """
        Evaluate hypothesis and track fragility across pressure types.
        
        Tracks:
        - Does hypothesis collapse under perturbation?
        - Does it fail temporal pressure?
        - Does it survive fragment-only view?
        - Which pressure types cause collapse?
        
        Returns evaluation result with added 'fragility' field.
        """
        # Run standard evaluation first
        result = self.evaluate_hypothesis(hypothesis, corpus, verbose=verbose)
        
        # Extract fragility data from claim graph
        fragility = HypothesisFragility()
        
        try:
            conn = sqlite3.connect(str(self.db_path))
            
            # Get average of each pressure type for survived claims
            pressure_types = ['graph', 'temporal', 'frequency', 'perturbation', 'representation']
            
            for p_type in pressure_types:
                avg_pressure = conn.execute(f"""
                    SELECT AVG({p_type}_pressure)
                    FROM claims
                    WHERE {p_type}_pressure IS NOT NULL
                      AND claim_strength > 0.3
                """).fetchone()[0]
                
                setattr(fragility, f"{p_type}_pressure", avg_pressure or 0.0)
            
            conn.close()
            
        except Exception as e:
            if verbose:
                print(f"[fragility] WARNING: Could not compute fragility: {e}")
        
        result["fragility"] = fragility
        
        # Check fragile points
        fragile_points = fragility.fragile_points(threshold=0.5)
        if fragile_points and verbose:
            print(f"[fragility] FRAGILE under: {', '.join(fragile_points)}")
        
        # Check conditional expectations
        if hypothesis.conditional_expectations:
            expectations_met = []
            for expectation in hypothesis.conditional_expectations:
                met = expectation.check(result)
                expectations_met.append(met)
                if verbose:
                    status = "✓" if met else "✗"
                    print(f"[expectations] {status} {expectation.conditions} → {expectation.expected_outcomes}")
            
            result["expectations_met"] = all(expectations_met)
        
        return result
    
    def chain_hypotheses(
        self,
        hypothesis_chain: List[Hypothesis],
        corpus: List[str],
        verbose: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Chain hypotheses: output of one feeds into the next.
        
        Example:
            alias_hypothesis → network_hypothesis → timeline_hypothesis
        
        Each hypothesis can use the results/evidence from previous step.
        
        Args:
            hypothesis_chain: List of hypotheses in order
            corpus: Document corpus
            verbose: Print progress
        
        Returns:
            List of results, one per hypothesis in chain
        """
        if verbose:
            print(f"\n{'═'*70}")
            print(f"HYPOTHESIS CHAIN: {len(hypothesis_chain)} steps")
            print(f"{'═'*70}")
            for i, h in enumerate(hypothesis_chain, 1):
                print(f"  {i}. {h.name:30s} | {h.target_pattern[:40]}")
            print(f"{'─'*70}\n")
        
        results = []
        context = {}  # Track context from previous steps
        
        for i, hypothesis in enumerate(hypothesis_chain, 1):
            if verbose:
                print(f"\n[STEP {i}/{len(hypothesis_chain)}] Evaluating: {hypothesis.name}")
            
            # Evaluate hypothesis
            result = self.evaluate_with_fragility(hypothesis, corpus, verbose=verbose)
            results.append(result)
            
            # Store result for next hypothesis in chain
            context[hypothesis.name] = result
            
            # Check if chain should continue (stop if hypothesis refuted)
            if result.get("validation_status") == "refuted":
                if verbose:
                    print(f"\n[chain] STOPPED: {hypothesis.name} was refuted")
                    print(f"[chain] Remaining {len(hypothesis_chain) - i} steps skipped")
                break
        
        if verbose:
            print(f"\n{'─'*70}")
            print(f"CHAIN COMPLETED: {len(results)}/{len(hypothesis_chain)} steps")
            for i, result in enumerate(results, 1):
                status = result.get("validation_status", "unknown")
                print(f"  {i}. {result['hypothesis_name']:30s} → {status}")
            print(f"{'═'*70}\n")
        
        return results
    
    def get_evaluation_history(self, hypothesis_name: str) -> List[Dict[str, Any]]:
        """Get all evaluations for a hypothesis."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        
        rows = conn.execute("""
            SELECT e.*
            FROM hypothesis_evaluations e
            JOIN hypotheses h ON e.hypothesis_id = h.id
            WHERE h.name = ?
            ORDER BY e.created_at DESC
        """, (hypothesis_name,)).fetchall()
        
        conn.close()
        
        return [dict(row) for row in rows]


# ═══════════════════════════════════════════════════════════════════════════
# CLI INTERFACE
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Hypothesis-driven investigation engine (Phase 16)"
    )
    parser.add_argument("--memory-dir", default="/tmp/bonfyre-memory",
                        help="Memory directory for claim graph")
    parser.add_argument("--models-dir", default="/tmp/bonfyre-models",
                        help="Models directory for transforms")
    parser.add_argument("--register", choices=list(BUILTIN_HYPOTHESES.keys()),
                        help="Register a built-in hypothesis")
    parser.add_argument("--evaluate", type=str,
                        help="Evaluate a hypothesis by name")
    parser.add_argument("--corpus", nargs="+",
                        help="Document paths for evaluation")
    parser.add_argument("--list", action="store_true",
                        help="List registered hypotheses")
    parser.add_argument("--history", type=str,
                        help="Show evaluation history for hypothesis")
    parser.add_argument("--run-all", action="store_true",
                        help="Evaluate all registered hypotheses")
    
    # PHASE 16.5: Adversarial hypothesis options
    parser.add_argument("--compare", choices=list(COMPETING_HYPOTHESIS_SETS.keys()),
                        help="Compare competing hypotheses")
    parser.add_argument("--chain", nargs="+",
                        help="Chain hypotheses (space-separated names)")
    parser.add_argument("--with-fragility", action="store_true",
                        help="Evaluate with fragility tracking")
    
    args = parser.parse_args()
    
    engine = HypothesisEngine(args.memory_dir, args.models_dir)
    
    if args.register:
        hypothesis = BUILTIN_HYPOTHESES[args.register]
        engine.register_hypothesis(hypothesis)
        print(f"✓ Registered hypothesis: {args.register}")
    
    elif args.list:
        print(f"\nRegistered Hypotheses ({len(engine.hypotheses)}):")
        print("─" * 70)
        for name, h in engine.hypotheses.items():
            print(f"  {name:30s} {h.target_pattern}")
        print()
    
    elif args.history:
        history = engine.get_evaluation_history(args.history)
        print(f"\nEvaluation History: {args.history}")
        print("─" * 70)
        for eval in history:
            print(f"  {eval['created_at']} | {eval['validation_status']:12s} | "
                  f"survival: {eval['survival_rate']:.1%}")
        print()
    
    elif args.evaluate:
        if not args.corpus:
            print("ERROR: --corpus required for evaluation")
            exit(1)
        
        if args.evaluate not in engine.hypotheses:
            print(f"ERROR: Hypothesis '{args.evaluate}' not registered")
            exit(1)
        
        hypothesis = engine.hypotheses[args.evaluate]
        result = engine.evaluate_hypothesis(hypothesis, args.corpus, verbose=True)
        
        print(f"\nResult:")
        print(json.dumps(result, indent=2))
    
    elif args.run_all:
        if not args.corpus:
            print("ERROR: --corpus required for evaluation")
            exit(1)
        
        results = engine.run_all_hypotheses(args.corpus)
        
        print(f"\nResults:")
        print(json.dumps(results, indent=2))
    
    # PHASE 16.5: Adversarial commands
    elif args.compare:
        if not args.corpus:
            print("ERROR: --corpus required for comparison")
            exit(1)
        
        competing_set = COMPETING_HYPOTHESIS_SETS[args.compare]
        result = engine.compare_competing_hypotheses(competing_set, args.corpus, verbose=True)
        
        print(f"\nComparison Result:")
        print(f"  Winner: {result['winner']}")
        print(f"  Relative Strengths:")
        for name, score in sorted(result['relative_strengths'].items(), key=lambda x: x[1], reverse=True):
            print(f"    {name:20s} {score:.3f}")
    
    elif args.chain:
        if not args.corpus:
            print("ERROR: --corpus required for chaining")
            exit(1)
        
        # Build hypothesis chain
        hypothesis_chain = []
        for name in args.chain:
            if name not in engine.hypotheses:
                print(f"ERROR: Hypothesis '{name}' not registered")
                exit(1)
            hypothesis_chain.append(engine.hypotheses[name])
        
        results = engine.chain_hypotheses(hypothesis_chain, args.corpus, verbose=True)
        
        print(f"\nChain Results:")
        for i, result in enumerate(results, 1):
            print(f"  {i}. {result['hypothesis_name']:30s} → {result['validation_status']}")
    
    elif args.with_fragility and args.evaluate:
        if not args.corpus:
            print("ERROR: --corpus required for evaluation")
            exit(1)
        
        if args.evaluate not in engine.hypotheses:
            print(f"ERROR: Hypothesis '{args.evaluate}' not registered")
            exit(1)
        
        hypothesis = engine.hypotheses[args.evaluate]
        result = engine.evaluate_with_fragility(hypothesis, args.corpus, verbose=True)
        
        print(f"\nFragility Analysis:")
        fragility = result.get("fragility")
        if fragility:
            print(f"  Graph pressure:          {fragility.graph_pressure:.3f}")
            print(f"  Temporal pressure:       {fragility.temporal_pressure:.3f}")
            print(f"  Frequency pressure:      {fragility.frequency_pressure:.3f}")
            print(f"  Perturbation pressure:   {fragility.perturbation_pressure:.3f}")
            print(f"  Representation pressure: {fragility.representation_pressure:.3f}")
            print(f"  Robustness score:        {fragility.robustness_score():.3f}")
            
            fragile_points = fragility.fragile_points()
            if fragile_points:
                print(f"  FRAGILE under: {', '.join(fragile_points)}")
    
    else:
        parser.print_help()
