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
    pg.record_bid(db, capability="T_FPQ", provider="local",
                  metric={"quality": 0.9, "latency": 0.8, "authority": 1.0, "cost": 0.9})
    pg.record_bid(db, capability="T_FPQ", provider="remote",
                  metric={"quality": 0.95, "latency": 0.2, "authority": 1.0, "cost": 0.1})
    pg.record_bid(db, capability="T_FPQ", provider="unauthorized",
                  metric={"quality": 1.0, "authority": 0.0})
    # latency-weighted policy favors local; the unauthorized bid is not selectable
    ranked = pg.tournament(db, "T_FPQ", weights={"latency": 3.0, "quality": 1.0})
    providers = [p for p, _ in ranked]
    assert providers[0] == "local"
    assert "unauthorized" not in providers          # available is not authorized
    assert pg.fallback_tree(db, "T_FPQ", weights={"latency": 3.0}) == ["local", "remote"]


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
