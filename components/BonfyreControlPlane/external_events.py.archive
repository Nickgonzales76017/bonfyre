"""The occurrence spine: observations commit before anything reasons about them.

Run 6 froze with `external_events`, `commitment_ledger` and `run6_action_log`
all empty while the system had demonstrably sent email, opened PRs, filed
issues, and received replies. Workers wrote straight into specialised tables
instead, so there was no canonical record of *what happened*.

The cost was concrete. In the final Run 1 epoch the agent read three real
inbound transitions -- Weights & Biases declining, Hugging Face acknowledging a
ticket, LangChain redirecting to a sales form -- and disk exhaustion killed the
epoch before the campaign tables were updated. The freeze still records W&B as
`delivered / waiting_on Weights & Biases`. The provider transcript knew more
than the authoritative database.

So an observation is committed here, as a small durable row, *before* a model is
allowed to reason about it. Campaign state is then a projection of these events
rather than something a worker mutates directly. If the epoch dies, the fact
survives and the projector catches up later.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional

UTC = dt.timezone.utc

# What kind of external thing happened. Deliberately small and concrete: these
# are observations, not interpretations.
INBOUND_REPLY = "inbound_reply"
DECLINED = "declined"
ACCEPTED = "accepted"
ACKNOWLEDGED = "acknowledged"
REDIRECTED = "redirected"
OUTBOUND_SENT = "outbound_sent"
STATE_CHANGED = "state_changed"

EVENT_KINDS = frozenset(
    {
        INBOUND_REPLY,
        DECLINED,
        ACCEPTED,
        ACKNOWLEDGED,
        REDIRECTED,
        OUTBOUND_SENT,
        STATE_CHANGED,
    }
)

# Economic categories that must never be summed together. Straight from the
# CapitalGym lesson: a discovered opportunity is worth zero, and pipeline is not
# money. Keeping these as separate columns is what stops a single fake "value".
REALIZED_CASH = "realized_cash"
APPROVED_CREDIT = "approved_credit"
SIGNED_CONTRACT = "signed_contract"
SUBMITTED_GRANT = "submitted_grant"
QUALIFIED_ASK = "qualified_ask"
PARTNER_APPLICATION = "partner_application"
EXTERNAL_STATE_CHANGE = "external_state_change"

LEDGER_CATEGORIES = (
    REALIZED_CASH,
    APPROVED_CREDIT,
    SIGNED_CONTRACT,
    SUBMITTED_GRANT,
    QUALIFIED_ASK,
    PARTNER_APPLICATION,
    EXTERNAL_STATE_CHANGE,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS external_event_log(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  digest TEXT NOT NULL UNIQUE,
  observed_at TEXT NOT NULL,
  recorded_at TEXT NOT NULL,
  source TEXT NOT NULL,
  actor TEXT NOT NULL,
  event_kind TEXT NOT NULL,
  subject_ref TEXT NOT NULL DEFAULT '',
  payload TEXT NOT NULL DEFAULT '{}',
  evidence_ref TEXT NOT NULL DEFAULT '',
  projected_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_external_event_unprojected
  ON external_event_log(projected_at, id);
CREATE INDEX IF NOT EXISTS idx_external_event_actor
  ON external_event_log(actor, id);

CREATE TABLE IF NOT EXISTS commitment_entries(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id INTEGER REFERENCES external_event_log(id),
  actor TEXT NOT NULL,
  category TEXT NOT NULL,
  amount_usd REAL NOT NULL DEFAULT 0,
  currency TEXT NOT NULL DEFAULT 'USD',
  occurred_at TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT ''
);
"""


def _iso(moment: dt.datetime) -> str:
    return moment.astimezone(UTC).replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class ExternalEvent:
    id: int
    observed_at: dt.datetime
    source: str
    actor: str
    event_kind: str
    subject_ref: str
    payload: dict
    evidence_ref: str


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.commit()


def digest_for(
    source: str, actor: str, event_kind: str, subject_ref: str, observed_at: dt.datetime
) -> str:
    """Stable identity for one observation.

    A watcher that re-reads the same inbox must not create a second event, or
    the projection would double-count. Identity is the observation itself, not
    the moment we happened to notice it.
    """
    raw = "|".join([source, actor, event_kind, subject_ref, _iso(observed_at)])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def observe(
    db: sqlite3.Connection,
    *,
    source: str,
    actor: str,
    event_kind: str,
    subject_ref: str = "",
    observed_at: Optional[dt.datetime] = None,
    payload: Optional[dict] = None,
    evidence_ref: str = "",
    now: Optional[dt.datetime] = None,
) -> Optional[int]:
    """Commit one observation. Cheap, durable, and done before any reasoning.

    Returns the event id, or None if this exact observation was already
    recorded. This is the call a watcher makes the instant it sees something --
    long before a frontier epoch is asked what it means.
    """
    if event_kind not in EVENT_KINDS:
        raise ValueError(f"unknown event_kind {event_kind!r}")
    seen = observed_at or dt.datetime.now(UTC)
    fingerprint = digest_for(source, actor, event_kind, subject_ref, seen)
    try:
        cursor = db.execute(
            "INSERT INTO external_event_log"
            "(digest,observed_at,recorded_at,source,actor,event_kind,subject_ref,"
            " payload,evidence_ref)"
            " VALUES(?,?,?,?,?,?,?,?,?)",
            (
                fingerprint,
                _iso(seen),
                _iso(now or dt.datetime.now(UTC)),
                source,
                actor,
                event_kind,
                subject_ref,
                json.dumps(payload or {}, sort_keys=True),
                evidence_ref,
            ),
        )
    except sqlite3.IntegrityError:
        return None
    db.commit()
    return int(cursor.lastrowid)


def unprojected(db: sqlite3.Connection, limit: int = 500) -> list[ExternalEvent]:
    rows = db.execute(
        "SELECT id,observed_at,source,actor,event_kind,subject_ref,payload,evidence_ref"
        " FROM external_event_log WHERE projected_at IS NULL ORDER BY id LIMIT ?",
        (limit,),
    ).fetchall()
    return [
        ExternalEvent(
            id=row[0],
            observed_at=dt.datetime.fromisoformat(row[1]),
            source=row[2],
            actor=row[3],
            event_kind=row[4],
            subject_ref=row[5],
            payload=json.loads(row[6] or "{}"),
            evidence_ref=row[7],
        )
        for row in rows
    ]


def mark_projected(
    db: sqlite3.Connection, event_id: int, now: Optional[dt.datetime] = None
) -> None:
    db.execute(
        "UPDATE external_event_log SET projected_at=? WHERE id=?",
        (_iso(now or dt.datetime.now(UTC)), event_id),
    )
    db.commit()


# The projection each observation implies for campaign state. Interpretation
# lives here, in one reviewable place, instead of inside whatever worker
# happened to read the inbox.
_STATUS_PROJECTION = {
    DECLINED: "declined",
    ACCEPTED: "accepted",
    ACKNOWLEDGED: "acknowledged",
    REDIRECTED: "redirected",
    INBOUND_REPLY: "replied",
    STATE_CHANGED: "state_changed",
    OUTBOUND_SENT: "sent",
}

# Every declared kind must project. An unmapped kind used to be marked
# projected while doing nothing -- a silent drop, found when a GitHub `assign`
# notification for an issue that had been assigned to us vanished between the
# spine and the queue.
assert set(_STATUS_PROJECTION) == set(EVENT_KINDS), (
    "unmapped occurrence kinds would be silently dropped: "
    f"{sorted(set(EVENT_KINDS) - set(_STATUS_PROJECTION))}"
)


def project(
    db: sqlite3.Connection,
    apply_status: Callable[[str, str, ExternalEvent], None],
    now: Optional[dt.datetime] = None,
    limit: int = 500,
) -> int:
    """Fold pending observations into campaign state.

    `apply_status(actor, status, event)` performs the caller's own write, so
    this module stays independent of whichever campaign schema is in use. The
    projector is re-runnable: an event is marked projected only after its write
    succeeds, so a crash mid-projection replays rather than skips.
    """
    projected = 0
    for event in unprojected(db, limit=limit):
        status = _STATUS_PROJECTION.get(event.event_kind)
        if status is not None:
            apply_status(event.actor, status, event)
        mark_projected(db, event.id, now=now)
        projected += 1
    return projected


def record_commitment(
    db: sqlite3.Connection,
    *,
    actor: str,
    category: str,
    amount_usd: float = 0.0,
    occurred_at: Optional[dt.datetime] = None,
    event_id: Optional[int] = None,
    detail: str = "",
) -> int:
    if category not in LEDGER_CATEGORIES:
        raise ValueError(f"unknown ledger category {category!r}")
    cursor = db.execute(
        "INSERT INTO commitment_entries"
        "(event_id,actor,category,amount_usd,occurred_at,detail) VALUES(?,?,?,?,?,?)",
        (
            event_id,
            actor,
            category,
            amount_usd,
            _iso(occurred_at or dt.datetime.now(UTC)),
            detail,
        ),
    )
    db.commit()
    return int(cursor.lastrowid)


def ledger_totals(db: sqlite3.Connection) -> dict[str, float]:
    """Totals per category, never a single combined number.

    There is deliberately no `total()`. Run 6's lifetime figure of $158,540 face
    value against $0 realized is exactly the confusion this prevents: those are
    different kinds of fact and adding them produces a number that describes
    nothing.
    """
    totals = {category: 0.0 for category in LEDGER_CATEGORIES}
    for category, amount in db.execute(
        "SELECT category, COALESCE(SUM(amount_usd),0) FROM commitment_entries"
        " GROUP BY category"
    ):
        totals[category] = float(amount)
    return totals
