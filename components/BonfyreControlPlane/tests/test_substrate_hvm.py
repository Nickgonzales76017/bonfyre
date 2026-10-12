"""HVM substrate adapter runs a real bounded reduction. Skips if hvm is absent --
a passing test here means the substrate actually reduced."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import substrate_hvm as hvm

pytestmark = pytest.mark.skipif(not hvm.available(), reason="requires the hvm binary")


def test_real_reduction_returns_result_and_interactions():
    r = hvm.reduce("@main = 42")
    assert r.ok is True
    assert r.result == "42"
    assert r.interactions >= 1          # ITRS -- a real bounded reduction ran


def test_reduction_is_deterministic():
    a = hvm.reduce("@main = 42")
    b = hvm.reduce("@main = 42")
    assert a.result == b.result and a.interactions == b.interactions
