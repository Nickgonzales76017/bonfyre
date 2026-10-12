"""FPQ execution as live frontier evidence -- not an archive citation.

These tests require the real bonfyre-fpq binary and the golden fixture; they
skip cleanly if either is absent, because their whole point is that the numbers
come from a command that actually ran.
"""

import shutil
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import fpq_evidence as fe
import proof_frontier as pf

pytestmark = pytest.mark.skipif(
    not fe.FPQ.exists() or not fe.FIXTURE.exists(),
    reason="requires real bonfyre-fpq and the tiny-f16 golden fixture",
)


@pytest.fixture()
def db():
    con = sqlite3.connect(":memory:")
    yield con
    con.close()


def test_real_run_cools_reconstruction(db):
    out = fe.record_measured_evidence(db)
    # numbers came from a process, not a constant
    assert out["all_good"] is True
    assert out["worst_mse"] < fe.MSE_CEILING
    assert len(out["tensors"]) == 3
    inv = pf.cooled_invariants(db, fe.SUBJECT, "reconstruction")
    assert inv and inv[0].truth_plane == pf.MEASURED
    # cited by real digests, not an archive ref
    assert any(r.startswith("report:sha256:") for r in inv[0].proof_refs)


def test_fence_denies_encoder_edit_on_a_symptom(db):
    fe.record_measured_evidence(db)
    d = pf.authorize_mutation(
        db, subject_resource=fe.SUBJECT, subject_profile=fe.PROFILE,
        target_layer="reconstruction", observed_failure="generation is garbage",
    )
    assert d.verdict == pf.DENY
    # the open frontier is fair game
    d2 = pf.authorize_mutation(
        db, subject_resource=fe.SUBJECT, subject_profile=fe.PROFILE,
        target_layer="transformer_math", observed_failure="generation is garbage",
    )
    assert d2.verdict == pf.ALLOW


def test_pristine_fixture_verifies(db):
    fe.record_measured_evidence(db)
    ok, signal = fe.verify_fixture(db)
    assert ok is True and signal == ""


def test_mutated_fixture_reheats_the_layer(db, tmp_path):
    fe.record_measured_evidence(db)
    # a real regression: corrupt the fixture bytes. The digest no longer matches
    # what the invariant recorded, so verify emits the one signal the fence
    # accepts -- and only that signal reopens the cooled layer.
    mutant = tmp_path / "mutant.gguf"
    shutil.copy(fe.FIXTURE, mutant)
    raw = bytearray(mutant.read_bytes())
    raw[-64:] = bytes((b ^ 0xFF) for b in raw[-64:])  # flip last tensor's weights
    mutant.write_bytes(raw)

    ok, signal = fe.verify_fixture(db, mutant)
    assert ok is False
    assert signal in (fe.REHEAT_HASH, fe.REHEAT_MSE)

    # before the signal, an unrelated symptom cannot reopen reconstruction
    inv_id = "fpq.reconstruction.fixture.tiny_f16"
    assert pf.challenge_invariant(db, inv_id, "generation is garbage") is False
    # the real signal does
    assert pf.challenge_invariant(db, inv_id, signal) is True
    # now nothing is cooled at reconstruction, so the layer is workable again
    assert not pf.cooled_invariants(db, fe.SUBJECT, "reconstruction")


def test_determinism_holds_across_runs(db):
    a, sha_a = fe.run_roundtrip()
    b, sha_b = fe.run_roundtrip()
    assert sha_a == sha_b  # byte-identical report -> valid golden fixture
    assert [t.mse for t in a] == [t.mse for t in b]
