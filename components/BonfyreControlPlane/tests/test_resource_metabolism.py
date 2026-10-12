from resource_metabolism import (
    GIB,
    YieldPolicy,
    active_backoff,
    expected_yield_bytes,
    inventory_fingerprint,
    low_yield_backoff,
    state_record,
)


def records(size=4 * 1024**2, digest="a" * 64):
    return [
        {
            "path": "/estate/.run6",
            "category": "history",
            "kind": "dir",
            "bytes": size,
            "root_digest_sha256": digest,
            "fate": "DELETE",
        }
    ]


def test_inventory_fingerprint_ignores_order_and_clock_noise():
    first = records() + [{"path": "/keep", "fate": "KEEP", "observed_at": "one"}]
    second = list(reversed(first))
    second[0] = {**second[0], "observed_at": "two", "free_gib": 99}
    assert inventory_fingerprint(first) == inventory_fingerprint(second)


def test_expected_yield_deduplicates_paths_and_excludes_kept_records():
    values = records() + records(size=2 * 1024**2) + [
        {"path": "/keep", "bytes": 8 * 1024**2, "fate": "KEEP"}
    ]
    assert expected_yield_bytes(values) == 4 * 1024**2


def test_roughly_four_mib_inventory_enters_exponential_backoff():
    policy = YieldPolicy(base_backoff_seconds=120, max_backoff_seconds=600)
    fingerprint = inventory_fingerprint(records())
    first = low_yield_backoff(
        None,
        fingerprint=fingerprint,
        expected_bytes=expected_yield_bytes(records()),
        now_epoch=1000,
        free_bytes=20 * GIB,
        policy=policy,
    )
    prior = state_record(
        decision=first,
        fingerprint=fingerprint,
        observed_yield_bytes=0,
        observed_at="2026-08-21T00:00:00+00:00",
        free_bytes_at_decision=20 * GIB,
    )
    second = low_yield_backoff(
        prior,
        fingerprint=fingerprint,
        expected_bytes=expected_yield_bytes(records()),
        now_epoch=1120,
        free_bytes=20 * GIB,
        policy=policy,
    )
    assert first.skip and first.backoff_until_epoch == 1120
    assert second.skip and second.inventory_cache_hit
    assert second.consecutive_yield_exhausted == 2
    assert second.backoff_until_epoch == 1360


def test_active_backoff_avoids_inventory_until_deadline():
    policy = YieldPolicy()
    previous = {
        "expected_yield_bytes": 4 * 1024**2,
        "consecutive_yield_exhausted": 2,
        "backoff_until_epoch": 2000,
        "free_bytes_at_decision": 20 * GIB,
    }
    decision = active_backoff(
        previous, now_epoch=1500, free_bytes=20 * GIB, policy=policy
    )
    assert decision.skip
    assert decision.inventory_cache_hit
    assert decision.expected_yield_bytes == 4 * 1024**2


def test_material_capacity_loss_reopens_cached_inventory_early():
    policy = YieldPolicy(pressure_reopen_bytes=GIB)
    previous = {
        "backoff_until_epoch": 2000,
        "free_bytes_at_decision": 20 * GIB,
    }
    decision = active_backoff(
        previous, now_epoch=1500, free_bytes=18 * GIB, policy=policy
    )
    assert not decision.skip
    assert "free-space loss" in decision.reason


def test_emergency_floor_bypasses_cached_and_new_low_yield_backoff():
    policy = YieldPolicy(emergency_floor_bytes=GIB)
    previous = {"backoff_until_epoch": 2000, "expected_yield_bytes": 1}
    assert not active_backoff(
        previous, now_epoch=1500, free_bytes=GIB, policy=policy
    ).skip
    assert not low_yield_backoff(
        previous,
        fingerprint="f",
        expected_bytes=1,
        now_epoch=1500,
        free_bytes=GIB,
        policy=policy,
    ).skip


def test_meaningful_expected_yield_runs_immediately():
    policy = YieldPolicy(min_expected_yield_bytes=10 * 1024**2)
    decision = low_yield_backoff(
        None,
        fingerprint="f",
        expected_bytes=11 * 1024**2,
        now_epoch=1000,
        free_bytes=20 * GIB,
        policy=policy,
    )
    assert not decision.skip
    assert decision.consecutive_yield_exhausted == 0
