"""The real DBSP reachability circuit: retraction propagates, and an independent
proven fact survives the retraction of a dependent one. Skips if not built."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import substrate_feldera as sf

pytestmark = pytest.mark.skipif(
    not sf.available(), reason="requires the built feldera reachability circuit")


def test_organism_route_withdraws_on_reheat_and_fpq_survives():
    v = sf.run_incremental_view()
    assert v.ok
    # the FPQ->SLI->KV organism builds to 3, then the SLI reheat withdraws it
    assert v.organism_route == (1, 2, 3, 2)
    assert v.organism_withdrew_on_reheat is True
    # the decisive anti-amnesia property: FPQ is not erased by the SLI retraction
    assert v.fpq_present == (1, 1, 1, 1)
    assert v.fpq_survived is True


@pytest.mark.skipif(not sf.reachable_capacity_available(),
                    reason="requires the built ReachableCapacity relation")
def test_reachable_capacity_is_maintained_with_withdrawal():
    r = sf.run_reachable_capacity()
    assert r.ok
    # organism becomes reachable when all blockers resolve, then the SLI reheat
    # withdraws it incrementally -- maintained, not recomputed.
    assert r.organism_reachable == (0, 0, 0, 1, 1, 0)
    assert r.withdrawn_on_reheat is True


@pytest.mark.skipif(not sf.daemon_available(),
                    reason="requires the built persistent daemon circuit")
def test_persistent_circuit_maintains_across_deltas():
    # a standing circuit: resolve organism, reheat sli (withdrawn by one -1),
    # re-resolve (restored). Each step is one transaction, no reseed.
    steps = sf.run_delta_stream([
        "B\torganism\tfpq", "B\torganism\tsli", "B\torganism\tkv",
        "+\tfpq", "+\tsli", "+\tkv", "-\tsli", "+\tsli",
    ])
    by_step = {s["step"]: s["reachable"] for s in steps}
    assert by_step[5] == ["organism"]   # all resolved -> reachable
    assert by_step[6] == []             # sli retracted -> withdrawn (one -1)
    assert by_step[7] == ["organism"]   # re-resolved -> restored


@pytest.mark.skipif(not sf.route_daemon_available(),
                    reason="requires the built route-set daemon")
def test_route_withdraws_on_provider_down_with_failover():
    steps = sf.run_route_stream([
        "A\tT_FPQ\tfpq-resident-01", "A\tT_FPQ\tBonfyreAPI",
        "+\tfpq-resident-01", "+\tBonfyreAPI", "-\tfpq-resident-01",
    ])
    by_step = {s["step"]: s["routes"] for s in steps}
    assert by_step[3] == ["T_FPQ@BonfyreAPI", "T_FPQ@fpq-resident-01"]  # both up (anycast)
    # provider down -> its route withdrawn, the alternate survives (failover)
    assert by_step[4] == ["T_FPQ@BonfyreAPI"]
