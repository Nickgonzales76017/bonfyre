"""Provider topology: bids scored on a vector (not a scalar), placement, and
proven carrier parity."""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import provider_graphs as pg


def _db():
    db = sqlite3.connect(":memory:")
    pg.ensure_schema(db)
    return db


def test_tournament_ranks_on_metric_vector_and_gates_authority():
    db = _db()
    # real units: cost is context bytes, latency is milliseconds -- both spent,
    # so both minimized. Nothing is pre-negated to please the scorer.
    pg.record_bid(db, capability="T_FPQ", provider="local",
                  metric={"quality": 0.9, "latency": 40.0, "authority": 1.0, "cost": 500.0})
    pg.record_bid(db, capability="T_FPQ", provider="remote",
                  metric={"quality": 0.95, "latency": 900.0, "authority": 1.0, "cost": 40000.0})
    pg.record_bid(db, capability="T_FPQ", provider="unauthorized",
                  metric={"quality": 1.0, "authority": 0.0})
    ranked = pg.tournament(db, "T_FPQ", weights={"latency": 3.0, "quality": 1.0})
    providers = [p for p, _ in ranked]
    assert providers[0] == "local"                  # lower latency wins on a latency policy
    assert "unauthorized" not in providers          # available is not authorized
    assert pg.fallback_tree(db, "T_FPQ", weights={"latency": 3.0}) == ["local", "remote"]


def test_minimized_dimensions_are_costs_not_goods():
    """The polarity regression: with an all-ones default weighting the scorer
    used to rank the most expensive, slowest, lossiest bid first -- the exact
    inversion of preferring the cheapest sufficient realization."""
    db = _db()
    pg.record_bid(db, capability="T_CTX", provider="cheap",
                  metric={"cost": 461.0, "authority": 1.0})
    pg.record_bid(db, capability="T_CTX", provider="expensive",
                  metric={"cost": 257986.0, "authority": 1.0})
    assert pg.fallback_tree(db, "T_CTX") == ["cheap", "expensive"]
    assert pg.polarity("cost") == -1
    assert pg.polarity("latency") == -1
    assert pg.polarity("semantic_loss") == -1
    assert pg.polarity("quality") == 1
    assert pg.polarity("trust") == 1


def test_a_weight_expresses_importance_never_direction():
    """A caller raising a weight must never be able to invert a dimension's
    meaning -- otherwise every producer has to guess the scorer's convention."""
    db = _db()
    pg.record_bid(db, capability="T_W", provider="cheap",
                  metric={"cost": 100.0, "authority": 1.0})
    pg.record_bid(db, capability="T_W", provider="dear",
                  metric={"cost": 10000.0, "authority": 1.0})
    for weight in (0.001, 1.0, 50.0):
        assert pg.fallback_tree(db, "T_W", weights={"cost": weight})[0] == "cheap"


def test_read_paths_work_on_a_db_that_never_took_a_write():
    """The live control plane had no provider_bids table at all, so the first
    production reader crashed instead of seeing an empty tournament."""
    fresh = sqlite3.connect(":memory:")
    assert pg.tournament(fresh, "nothing") == []
    assert pg.fallback_tree(fresh, "nothing") == []
    assert pg.placement_of(fresh, "nothing") is None
    assert pg.parity_group(fresh, "nothing") == []


def test_placement_is_recorded_not_authority():
    db = _db()
    pg.record_placement(db, resource="gpu-lease-1", location="mac/metal")
    assert pg.placement_of(db, "gpu-lease-1") == "mac/metal"
    assert pg.placement_of(db, "unknown") is None


def test_parity_group_needs_proof():
    db = _db()
    pg.record_parity(db, capability="T_FPQ", carrier_a="direct", carrier_b="mcp", proven=True)
    pg.record_parity(db, capability="T_FPQ", carrier_a="direct", carrier_b="http", proven=False)
    group = pg.parity_group(db, "T_FPQ")
    assert ("direct", "mcp") in group        # proven -> in the multipath group
    assert ("direct", "http") not in group   # ran but not proven -> excluded
