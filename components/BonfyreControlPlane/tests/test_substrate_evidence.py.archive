"""Consume Bonfyre's substrate probe manifests. Skips if the substrate workspace
is absent; a pass means the recorded live-upstream proofs were read, not faked."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import substrate_evidence as se


def test_reads_recorded_activation_and_proof(tmp_path):
    # hermetic: a synthetic manifest pair reads into a measured evidence record.
    d = tmp_path / "egglog"
    d.mkdir()
    (d / "activation.json").write_text(json.dumps({
        "state": "active", "evidence_class": "live-upstream", "manifest_digest": "abc"}))
    (d / "semantic-proof.json").write_text(json.dumps({
        "state": "passed", "kind": "bounded-equality-saturation-and-check",
        "output_digest": "def"}))
    ev = se.read_substrate("egglog", tmp_path)
    assert ev is not None and ev.measured is True
    assert ev.proof_kind.startswith("bounded-equality")
    assert ev.witness_ref().endswith("egglog/semantic-proof.json")


def test_failed_proof_is_not_measured(tmp_path):
    d = tmp_path / "x"
    d.mkdir()
    (d / "activation.json").write_text(json.dumps({"state": "active", "evidence_class": "live-upstream"}))
    (d / "semantic-proof.json").write_text(json.dumps({"state": "failed", "kind": "k"}))
    assert se.read_substrate("x", tmp_path).measured is False


@pytest.mark.skipif(not se.SUBSTRATES_ROOT.exists(), reason="requires the real substrate workspace")
def test_real_substrates_are_activated_and_proven():
    subs = se.all_substrates()
    # the recorded workspace has all 14 substrate probes passing live-upstream.
    assert len(subs) >= 13
    measured = se.measured_substrates()
    assert "verus" in measured and "egglog" in measured and "lance" in measured
    # verus is an SMT-backed formal verification with positive+negative controls
    assert "formal-verification" in subs["verus"].proof_kind
