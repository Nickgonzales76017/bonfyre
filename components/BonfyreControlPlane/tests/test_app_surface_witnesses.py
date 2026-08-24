"""Nine-app (SS30) and all-surface (SS31) witnesses: one identity, one canonical
owner, many grammars, no pairwise sync."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import witness_nine_app as na  # noqa: E402
import witness_multi_surface as ms  # noqa: E402

PROJ = os.path.expanduser("~/.bonfyre/estate-fabric/projections")


def test_nine_app_single_owner_many_grammars():
    r = na.witness()
    assert r["Actor"]["owners"] == ["ActorGraph"], "Actor has exactly one canonical owner"
    assert len(r["Actor"]["app_consumers"]) >= 2, "Actor flows to >=2 app grammars"
    # a mutation re-enters the fabric via an app publish (no pairwise sync)
    assert any("CommunicationEvent" in p for p in r["app_publishes"].values())


@pytest.mark.skipif(not os.path.exists(os.path.join(PROJ, "facts", "Actor", "index.json")),
                    reason="fabric projections not materialized")
def test_all_surface_one_identity_many_grammars():
    # materialize fresh so the projections reflect current state
    import fabric_queries as fq
    import fabric_facts as ff
    fq.publish(); ff.publish()
    r = ms.witness("org:tarbell-center")
    assert len(r["surfaces"]) >= 3, "identity appears through >=3 served grammars"
    assert r["actor_owners"] == ["ActorGraph"], "one owner, no private copies"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
