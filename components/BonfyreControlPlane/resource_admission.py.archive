"""Admission control for disk, checked before work starts rather than during.

Run 6's terminal failure was not provider exhaustion, it was ENOSPC. Planes
cloned Rust workspaces and filled caches until the Data volume hit 100%, and
then everything sharing that volume died at once: frontier transcripts, build
output, SQLite authority, and the supervisor's own status write. The supervisor
reported "database or disk is full" and exited.

Two things were missing. First, nothing compared a task's expected footprint
against free space before starting it. Second -- the part a simple free-space
check would still get wrong -- five planes ran concurrently, so each could
independently observe the same free bytes and all be admitted. Outstanding
grants have to be subtracted from what the next caller is allowed to see.
"""

from __future__ import annotations

import datetime as dt
import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

UTC = dt.timezone.utc

GIB = 1024**3

ADMIT = "admit"
DEFER = "defer"
REJECT = "reject"

SCHEMA = """
CREATE TABLE IF NOT EXISTS resource_grants(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  plane TEXT NOT NULL,
  kind TEXT NOT NULL,
  volume TEXT NOT NULL,
  estimated_bytes INTEGER NOT NULL,
  granted_at TEXT NOT NULL,
  expires_at TEXT,
  released_at TEXT,
  detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_resource_grants_open
  ON resource_grants(volume, released_at);
"""


@dataclass(frozen=True)
class AdmissionPolicy:
    """What the machine refuses to give away.

    `protected_floor_bytes` is space no task may consume under any
    circumstances -- it exists so SQLite can still commit and the supervisor can
    still write status when every plane is at its limit. That is precisely what
    Run 6 had no reserve for.
    """

    protected_floor_bytes: int = 10 * GIB
    per_plane_quota_bytes: int = 20 * GIB
    max_grant_bytes: int = 40 * GIB

    def __post_init__(self) -> None:
        if self.protected_floor_bytes < 0:
            raise ValueError("protected floor cannot be negative")


@dataclass(frozen=True)
class ResourceRequest:
    plane: str
    kind: str
    estimated_bytes: int
    volume: str = "/"
    detail: str = ""


@dataclass(frozen=True)
class AdmissionDecision:
    verdict: str
    reason: str
    free_bytes: int
    committed_bytes: int
    spendable_bytes: int
    request: Optional[ResourceRequest] = None

    @property
    def admitted(self) -> bool:
        return self.verdict == ADMIT


DiskProbe = Callable[[str], int]


def real_disk_probe(volume: str) -> int:
    """Free bytes on the volume holding `volume`."""
    return shutil.disk_usage(volume).free


def decide(
    request: ResourceRequest,
    policy: AdmissionPolicy,
    free_bytes: int,
    committed_bytes: int,
    plane_committed_bytes: int = 0,
) -> AdmissionDecision:
    """Pure admission decision.

    `committed_bytes` is the sum of grants already handed out on this volume and
    not yet released. Subtracting it is what stops five concurrent planes from
    each being admitted against the same free space.
    """
    spendable = free_bytes - policy.protected_floor_bytes - committed_bytes

    def build(verdict: str, reason: str) -> AdmissionDecision:
        return AdmissionDecision(
            verdict=verdict,
            reason=reason,
            free_bytes=free_bytes,
            committed_bytes=committed_bytes,
            spendable_bytes=max(spendable, 0),
            request=request,
        )

    if request.estimated_bytes <= 0:
        return build(REJECT, "request must carry a positive size estimate")

    if request.estimated_bytes > policy.max_grant_bytes:
        return build(
            REJECT,
            f"request of {_h(request.estimated_bytes)} exceeds the "
            f"{_h(policy.max_grant_bytes)} single-grant ceiling",
        )

    if plane_committed_bytes + request.estimated_bytes > policy.per_plane_quota_bytes:
        return build(
            DEFER,
            f"{request.plane} would hold "
            f"{_h(plane_committed_bytes + request.estimated_bytes)} against a "
            f"{_h(policy.per_plane_quota_bytes)} per-plane quota",
        )

    if request.estimated_bytes > spendable:
        # Distinguish "never" from "not now": if the machine could not satisfy
        # this even with every outstanding grant released, waiting is pointless.
        headroom_if_drained = free_bytes - policy.protected_floor_bytes
        if request.estimated_bytes > headroom_if_drained:
            return build(
                REJECT,
                f"{_h(request.estimated_bytes)} cannot fit above the "
                f"{_h(policy.protected_floor_bytes)} protected floor even if "
                f"every outstanding grant were released",
            )
        return build(
            DEFER,
            f"{_h(request.estimated_bytes)} exceeds {_h(max(spendable, 0))} "
            f"spendable ({_h(committed_bytes)} already committed)",
        )

    return build(ADMIT, f"{_h(spendable)} spendable above the protected floor")


def _h(size: int) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(value) < 1024 or unit == "TiB":
            return f"{value:.1f}{unit}" if unit != "B" else f"{int(value)}B"
        value /= 1024
    return f"{value:.1f}TiB"


# ---------------------------------------------------------------- persistence


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.commit()


def committed_bytes(
    db: sqlite3.Connection, volume: str, plane: Optional[str] = None
) -> int:
    if plane is None:
        row = db.execute(
            "SELECT COALESCE(SUM(estimated_bytes),0) FROM resource_grants"
            " WHERE volume=? AND released_at IS NULL",
            (volume,),
        ).fetchone()
    else:
        row = db.execute(
            "SELECT COALESCE(SUM(estimated_bytes),0) FROM resource_grants"
            " WHERE volume=? AND plane=? AND released_at IS NULL",
            (volume, plane),
        ).fetchone()
    return int(row[0])


def request_grant(
    db: sqlite3.Connection,
    request: ResourceRequest,
    policy: AdmissionPolicy,
    probe: DiskProbe = real_disk_probe,
    now: Optional[dt.datetime] = None,
) -> tuple[AdmissionDecision, Optional[int]]:
    """Decide and, when admitted, record the grant in the same transaction.

    Returns the decision and the grant id (None unless admitted). The grant must
    be released with `release_grant` when the work finishes, or it keeps holding
    space against every other plane.
    """
    ensure_schema(db)
    free = probe(request.volume)
    decision = decide(
        request,
        policy,
        free_bytes=free,
        committed_bytes=committed_bytes(db, request.volume),
        plane_committed_bytes=committed_bytes(db, request.volume, request.plane),
    )
    if not decision.admitted:
        return decision, None

    moment = (now or dt.datetime.now(UTC)).astimezone(UTC).replace(microsecond=0)
    cursor = db.execute(
        "INSERT INTO resource_grants"
        "(plane,kind,volume,estimated_bytes,granted_at,detail)"
        " VALUES(?,?,?,?,?,?)",
        (
            request.plane,
            request.kind,
            request.volume,
            request.estimated_bytes,
            moment.isoformat(),
            request.detail,
        ),
    )
    db.commit()
    return decision, int(cursor.lastrowid)


def release_grant(
    db: sqlite3.Connection, grant_id: int, now: Optional[dt.datetime] = None
) -> None:
    moment = (now or dt.datetime.now(UTC)).astimezone(UTC).replace(microsecond=0)
    db.execute(
        "UPDATE resource_grants SET released_at=? WHERE id=? AND released_at IS NULL",
        (moment.isoformat(), grant_id),
    )
    db.commit()


def reap_expired(
    db: sqlite3.Connection, older_than: dt.timedelta, now: Optional[dt.datetime] = None
) -> int:
    """Release grants a crashed plane never gave back.

    Run 6 lost planes to pipe deadlock, SIGKILL and ENOSPC. A grant ledger that
    only shrinks on a clean exit would ratchet toward permanent starvation, so
    stale grants age out.
    """
    moment = (now or dt.datetime.now(UTC)).astimezone(UTC).replace(microsecond=0)
    cutoff = (moment - older_than).isoformat()
    cursor = db.execute(
        "UPDATE resource_grants SET released_at=? "
        "WHERE released_at IS NULL AND granted_at < ?",
        (moment.isoformat(), cutoff),
    )
    db.commit()
    return cursor.rowcount
