"""The atlas auditing itself with CollapseFront -- and a live regression guard
that the no-maturity-laundering rule actually holds against the real files."""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ARCH = os.path.dirname(HERE)
sys.path.insert(0, ARCH)

import atlas_collapse as ac  # noqa: E402


def test_witness_kind_classifies():
    assert ac.witness_kind("external:bernstein tests/foo.py") == "external"
    assert ac.witness_kind("architecture/atlas.py") == "local"  # real repo file
    assert ac.witness_kind("architecture/does-not-exist-xyz.py") == "missing"


def test_no_true_laundering():
    """Every measured/proven claim has at least one real witness (local file or an
    honest external pointer). If this fails, a maturity label is laundered."""
    report = ac.report()
    assert report["laundered"] == [], (
        f"maturity laundering detected: {report['laundered']}"
    )


def test_external_claim_not_counted_as_laundered():
    audit = ac.build_maturity_lattice()
    ext = dict(ac.externally_witnessed_claims(audit))
    # bernstein's result-receipt bundle is witnessed only in an external repo.
    assert any("ResultReceipt" in name for name in ext), ext
    assert not set(ext) & set(ac.laundered_claims(audit)), "external != laundered"


def test_fragile_claims_are_single_witness():
    audit = ac.build_maturity_lattice()
    for name, sole in ac.fragile_claims(audit):
        assert len(audit.claims[name]["existing_witnesses"]) == 1
        assert audit.claims[name]["existing_witnesses"][0] == sole


def test_blast_radius_is_transpose_of_fragility():
    """For a single-witness claim, retracting that witness must collapse exactly
    that claim -- the transpose identity, on real registry data."""
    audit = ac.build_maturity_lattice()
    fragile = ac.fragile_claims(audit)
    assert fragile, "expected at least one single-witness claim in the real atlas"
    name, sole = fragile[0]
    radius = ac.blast_radius(audit, sole)
    assert name in radius, f"{sole} should collapse {name}"


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
