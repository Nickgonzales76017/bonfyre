"""Yield-aware policy for bounded physical-resource maintenance.

ExtremeGC is useful while it can recover meaningful capacity.  Once only tiny,
regenerated caches remain, repeatedly inventorying them and writing fresh
DeletionProofs costs more than it returns.  This module keeps that marginal
yield decision deterministic and independent of the Run8 operator carrier.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping

GIB = 1024**3


@dataclass(frozen=True)
class YieldPolicy:
    """When maintenance should cool instead of chasing regenerated crumbs."""

    min_expected_yield_bytes: int = 10 * 1024**2
    base_backoff_seconds: int = 15 * 60
    max_backoff_seconds: int = 6 * 60 * 60
    emergency_floor_bytes: int = int(0.5 * GIB)
    pressure_reopen_bytes: int = GIB

    def __post_init__(self) -> None:
        if self.min_expected_yield_bytes < 0:
            raise ValueError("minimum expected yield cannot be negative")
        if self.base_backoff_seconds <= 0:
            raise ValueError("base backoff must be positive")
        if self.max_backoff_seconds < self.base_backoff_seconds:
            raise ValueError("maximum backoff cannot be shorter than base backoff")
        if self.emergency_floor_bytes < 0:
            raise ValueError("emergency floor cannot be negative")
        if self.pressure_reopen_bytes < 0:
            raise ValueError("pressure reopen threshold cannot be negative")


@dataclass(frozen=True)
class BackoffDecision:
    skip: bool
    reason: str
    inventory_cache_hit: bool
    expected_yield_bytes: int
    consecutive_yield_exhausted: int
    backoff_until_epoch: float | None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def inventory_fingerprint(records: Iterable[Mapping[str, Any]]) -> str:
    """Content-address the reclaim-relevant inventory, excluding clock/free-space noise."""

    stable = []
    for record in records:
        stable.append(
            {
                "path": str(record.get("path", "")),
                "category": str(record.get("category", "")),
                "kind": str(record.get("kind", "")),
                "bytes": max(0, int(record.get("bytes", 0))),
                "root_digest_sha256": str(record.get("root_digest_sha256", "")),
                "fate": str(record.get("fate", "")),
            }
        )
    payload = json.dumps(
        sorted(stable, key=lambda item: (item["path"], item["category"])),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def expected_yield_bytes(records: Iterable[Mapping[str, Any]]) -> int:
    """Sum unique, currently deletable inventory without double-counting a path."""

    by_path: dict[str, int] = {}
    for record in records:
        if record.get("fate") != "DELETE":
            continue
        path = str(record.get("path", ""))
        if not path:
            continue
        by_path[path] = max(by_path.get(path, 0), max(0, int(record.get("bytes", 0))))
    return sum(by_path.values())


def active_backoff(
    previous: Mapping[str, Any] | None,
    *,
    now_epoch: float,
    free_bytes: int,
    policy: YieldPolicy,
) -> BackoffDecision:
    """Return a cache hit when a prior yield-exhausted interval is still active.

    The emergency floor always overrides cached cooling.  The caller may then
    perform a fresh inventory and reclaim whatever small amount remains.
    """

    previous = previous or {}
    expected = max(0, int(previous.get("expected_yield_bytes", 0)))
    consecutive = max(0, int(previous.get("consecutive_yield_exhausted", 0)))
    raw_until = previous.get("backoff_until_epoch")
    until = float(raw_until) if raw_until is not None else None
    if free_bytes <= policy.emergency_floor_bytes:
        return BackoffDecision(
            False,
            "emergency floor overrides yield backoff",
            False,
            expected,
            consecutive,
            None,
        )
    prior_free = previous.get("free_bytes_at_decision")
    if (
        prior_free is not None
        and int(prior_free) - free_bytes >= policy.pressure_reopen_bytes
    ):
        return BackoffDecision(
            False,
            "material free-space loss invalidates cached yield backoff",
            False,
            expected,
            consecutive,
            None,
        )
    if until is not None and now_epoch < until:
        return BackoffDecision(
            True,
            "cached inventory remains inside yield-exhausted backoff",
            True,
            expected,
            consecutive,
            until,
        )
    return BackoffDecision(False, "backoff absent or expired", False, expected, consecutive, until)


def low_yield_backoff(
    previous: Mapping[str, Any] | None,
    *,
    fingerprint: str,
    expected_bytes: int,
    now_epoch: float,
    free_bytes: int,
    policy: YieldPolicy,
) -> BackoffDecision:
    """Cool an unchanged low-value inventory with bounded exponential delay."""

    expected_bytes = max(0, int(expected_bytes))
    if free_bytes <= policy.emergency_floor_bytes:
        return BackoffDecision(
            False,
            "emergency floor requires reclaim despite low expected yield",
            False,
            expected_bytes,
            0,
            None,
        )
    if expected_bytes >= policy.min_expected_yield_bytes:
        return BackoffDecision(
            False,
            "expected yield clears the maintenance floor",
            False,
            expected_bytes,
            0,
            None,
        )

    previous = previous or {}
    same_inventory = previous.get("inventory_fingerprint") == fingerprint
    prior_count = max(0, int(previous.get("consecutive_yield_exhausted", 0)))
    consecutive = prior_count + 1 if same_inventory else 1
    delay = min(
        policy.max_backoff_seconds,
        policy.base_backoff_seconds * (2 ** min(consecutive - 1, 30)),
    )
    return BackoffDecision(
        True,
        "expected reclaim yield is below the maintenance floor",
        same_inventory,
        expected_bytes,
        consecutive,
        now_epoch + delay,
    )


def state_record(
    *,
    decision: BackoffDecision,
    fingerprint: str,
    observed_yield_bytes: int,
    observed_at: str,
    free_bytes_at_decision: int,
) -> dict[str, Any]:
    """Build the durable, JSON-safe state consumed by later maintenance ticks."""

    return {
        "version": "ExtremeGCYieldState-v1",
        "observed_at": observed_at,
        "inventory_fingerprint": fingerprint,
        "inventory_cache_hit": decision.inventory_cache_hit,
        "expected_yield_bytes": decision.expected_yield_bytes,
        "observed_yield_bytes": max(0, int(observed_yield_bytes)),
        "free_bytes_at_decision": max(0, int(free_bytes_at_decision)),
        "consecutive_yield_exhausted": decision.consecutive_yield_exhausted,
        "backoff_until_epoch": decision.backoff_until_epoch,
        "decision": "backoff" if decision.skip else "run",
        "reason": decision.reason,
    }
