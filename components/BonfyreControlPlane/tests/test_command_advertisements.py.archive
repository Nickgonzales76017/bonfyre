"""The whole Bonfyre command surface advertised: every cmd/Bonfyre* classified and
mapped to the capability it provides. Static (no binary execution)."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import command_advertisements as ca  # noqa: E402

_HAS_CMD = os.path.isdir(ca.CMD)


@pytest.mark.skipif(not _HAS_CMD, reason="cmd surface not present")
def test_surface_enumerated_and_classified():
    inv = ca.inventory()
    assert inv["command_count"] >= 90, "the ~91 command surface is enumerated"
    # most commands are actually built binaries, not stubs
    assert inv["by_classification"].get("built", 0) >= 80
    # every advertisement carries an honest classification + a provided capability
    for a in inv["advertisements"]:
        assert a["classification"] in ("built", "source-only", "declared")
        assert a["provides"]


@pytest.mark.skipif(not _HAS_CMD, reason="cmd surface not present")
def test_bonfyre_auth_provides_authority_and_gates():
    ads = {a["identity"]: a for a in ca.inventory()["advertisements"]}
    assert ads["BonfyreAuth"]["provides"] == "Authority"
    assert ads["BonfyreAuth"]["authority_gated"] is True
    # a mutating command is authority-gated; a pure-read one need not be
    assert ads["BonfyreLedger"]["effect_class"] == "mutating"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
