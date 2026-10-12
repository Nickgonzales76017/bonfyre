"""SERM reduces existing measurements -- it does not generate them. Sufficiency
needs witness-backed coverage; honesty rejects any plane claimed without a witness."""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import proof_frontier as pf
import serm

SUBJECT = "model:fpq-fixture-tiny-f16"
PROFILE = "fpq-v3-coord-qjl"


def _db_with_reconstruction_measured():
    """Set up the state the mature FPQ pipeline already records: reconstruction
    proven with a witness. Built directly, not by re-running the binary."""
    db = sqlite3.connect(":memory:")
    pf.ensure_schema(db)
    pf.set_layer(db, SUBJECT, 2, "reconstruction", "proven",
                 subject_profile=PROFILE, witness_ref="fpq_evidence:report_sha")
    pf.record_invariant(db, pf.SolvedInvariant(
        invariant_id="fpq.reconstruction.fixture.tiny_f16", subject_resource=SUBJECT,
        subject_profile=PROFILE, layer="reconstruction",
        statement="roundtrip within tolerance", truth_plane=pf.MEASURED, status=pf.COOLED,
        proof_refs=("report:sha256:abc",), reheat_conditions=("fixture mse exceeds ceiling",)))
    return db


def test_sufficient_when_required_layer_is_witness_backed():
    db = _db_with_reconstruction_measured()
    c = serm.MeasurementContract("acm-recon", "reconstruction evidence for ACM",
                                 SUBJECT, PROFILE, required_layers=("reconstruction",))
    r = serm.reduce(db, c)
    assert r.sufficient and r.honest
    assert r.covered == ("reconstruction",) and r.missing == ()


def test_insufficient_when_a_required_layer_is_unproven():
    db = _db_with_reconstruction_measured()
    c = serm.MeasurementContract("full", "semantic behaviour proof",
                                 SUBJECT, PROFILE,
                                 required_layers=("reconstruction", "transformer_math"))
    r = serm.reduce(db, c)
    assert not r.sufficient
    assert r.missing == ("transformer_math",) and r.covered == ("reconstruction",)


def test_honesty_audit_flags_a_laundered_plane():
    db = _db_with_reconstruction_measured()
    # a measured claim with NO proof_refs -- laundering.
    pf.record_invariant(db, pf.SolvedInvariant(
        invariant_id="fpq.semantic.claim", subject_resource=SUBJECT, subject_profile=PROFILE,
        layer="semantic_behavior", statement="generation is great", truth_plane=pf.MEASURED,
        status=pf.COOLED, proof_refs=(), reheat_conditions=("a counterexample generation",)))
    c = serm.MeasurementContract("c", "d", SUBJECT, PROFILE, required_layers=("reconstruction",))
    r = serm.reduce(db, c)
    assert r.honest is False
    assert any("semantic_behavior" in x for x in r.laundered)


def test_sufficiency_becomes_a_frontier_consequence():
    db = _db_with_reconstruction_measured()
    c = serm.MeasurementContract("acm-recon", "reconstruction evidence", SUBJECT, PROFILE,
                                 required_layers=("reconstruction",))
    r = serm.reduce(db, c)
    inv_id = serm.record_sufficiency(db, c, r)
    assert inv_id == "serm.sufficiency.acm-recon"
    cooled = pf.cooled_invariants(db, SUBJECT, "sufficiency")
    assert cooled and cooled[0].truth_plane == pf.MEASURED


def test_no_consequence_when_insufficient():
    db = _db_with_reconstruction_measured()
    c = serm.MeasurementContract("x", "d", SUBJECT, PROFILE,
                                 required_layers=("transformer_math",))
    r = serm.reduce(db, c)
    assert serm.record_sufficiency(db, c, r) is None
