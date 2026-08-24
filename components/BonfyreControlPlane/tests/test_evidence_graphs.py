"""The five evidence relations, each enforcing its distinct semantics."""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import evidence_graphs as eg


def _db():
    db = sqlite3.connect(":memory:")
    eg.ensure_schema(db)
    return db


def test_artifact_lineage_is_transformation_ancestry():
    db = _db()
    eg.record_transform(db, source="model.gguf", operator="BonfyreFPQ", derived="model.fpq")
    eg.record_transform(db, source="model.fpq", operator="BonfyreSLI", derived="out.bin")
    assert set(eg.lineage(db, "out.bin")) == {"model.fpq", "model.gguf"}


def test_provenance_path_rejects_untrusted_node():
    db = _db()
    eg.record_provenance(db, artifact="a", role="runtime", node="metal-local")
    eg.record_provenance(db, artifact="a", role="provider", node="sketchy-remote")
    assert eg.path_admits(db, "a", untrusted_nodes={"sketchy-remote"}) is False
    assert eg.path_admits(db, "a", untrusted_nodes={"other"}) is True


def test_evidence_support_is_not_symmetric():
    db = _db()
    eg.relate_evidence(db, evidence="fixture", kind=eg.SUPPORTS, claim="fpq-reconstructs")
    assert eg.supports(db, "fixture", "fpq-reconstructs") is True
    assert eg.supports(db, "fpq-reconstructs", "fixture") is False   # directional
    assert eg.supporters(db, "fpq-reconstructs") == ["fixture"]


def test_temporal_planes_do_not_collapse_and_stale_is_not_delete():
    db = _db()
    eg.record_times(db, "grant-deadline", {
        "event_time": "2026-01-01", "expiration_time": "2026-06-01",
        "retrieval_time": "2026-08-16"})
    t = eg.times_of(db, "grant-deadline")
    assert t["event_time"] != t["retrieval_time"]           # planes kept distinct
    assert eg.is_stale(db, "grant-deadline", "2026-08-16") is True
    # stale for a decision, but the historical fact still exists
    assert eg.times_of(db, "grant-deadline") != {}


def test_claim_dependencies_and_proof_state():
    db = _db()
    eg.add_claim(db, "identity-continuity", depends_on=["hash-chain"],
                 counters=["timezone-drift"], proof_state="measured")
    deps = eg.claim_dependencies(db, "identity-continuity")
    assert deps["depends_on"] == ["hash-chain"] and deps["counter"] == ["timezone-drift"]
    assert eg.proof_state(db, "identity-continuity") == "measured"
