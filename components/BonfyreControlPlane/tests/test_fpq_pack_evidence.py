"""Recorded FPQ pack verdicts fold into the frontier honestly: a failed quality
gate never lets a completed pack claim generation quality."""

import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import fpq_pack_evidence as fpe
import proof_frontier as pf
import serm


def _pack(tmp_path, gate, gen):
    p = tmp_path / "m.fpq-pack.json"
    p.write_text(json.dumps({
        "schema": "aurekai.fpq2_sharded_model_pack.v1", "status": "complete-pack",
        "model_repo": "Qwen/Qwen2.5-Coder-0.5B", "policy": "fpq2-target-b-v1",
        "quality_gate": gate, "generation_quality": gen,
    }))
    return p


def test_failed_gate_leaves_semantic_behavior_open(tmp_path):
    db = sqlite3.connect(":memory:")
    v = fpe.read_pack(_pack(tmp_path, "failed", "native FPQ2 parity failed"))
    out = fpe.record_pack_evidence(db, v)
    assert out["semantic_behavior"] == pf.OPEN
    # representation_abi is proven (pack complete) but semantic_behavior is not
    rep = pf.first_open_layer(db, v.subject, "fpq2-target-b-v1")
    assert rep is not None and rep[1] == "semantic_behavior"


def test_passed_gate_proves_semantic_behavior(tmp_path):
    db = sqlite3.connect(":memory:")
    v = fpe.read_pack(_pack(tmp_path, "passed", "parity ok"))
    out = fpe.record_pack_evidence(db, v)
    assert out["semantic_behavior"] == pf.PROVEN


def test_completed_pack_cannot_launder_into_a_semantic_claim(tmp_path):
    # SERM over a failed-gate subject: a contract needing semantic_behavior is
    # insufficient, because the recorded verdict was a failure -- not laundered up.
    db = sqlite3.connect(":memory:")
    v = fpe.read_pack(_pack(tmp_path, "failed", "parity failed"))
    fpe.record_pack_evidence(db, v)
    c = serm.MeasurementContract("gen", "generation parity", v.subject,
                                 "fpq2-target-b-v1", required_layers=("semantic_behavior",))
    r = serm.reduce(db, c)
    assert not r.sufficient and r.missing == ("semantic_behavior",)


def test_read_pack_ignores_non_pack_json(tmp_path):
    p = tmp_path / "x.json"
    p.write_text(json.dumps({"unrelated": True}))
    assert fpe.read_pack(p) is None
