"""Provider state as a fold over append-only observations.

Run 6 stored provider health as one mutable row per provider and updated it in
place. That made the state last-writer-wins: Codex reported a hard usage limit
with an explicit reset of 2026-08-19, and a later unrelated transient failure
overwrote `circuit_until` with a short circuit. The guardian then relaunched
Run 5 into the same exhausted provider.

The fix is not a bigger UPDATE. It is to stop storing derived state as truth.
Observations are appended and never mutated; the circuit is a pure function of
them. A transient failure cannot shorten a hard capacity window because the
fold does not let it -- there is no write that could.
"""

from __future__ import annotations

import datetime as dt
import sqlite3
from dataclasses import dataclass, field
from typing import Iterable, Optional, Sequence

UTC = dt.timezone.utc

# Ordered by how much authority an observation carries over the circuit.
HARD_CAPACITY = "hard_capacity"
MANUAL_PAUSE = "manual_pause"
TRANSIENT_FAILURE = "transient_failure"
SUCCESS = "success"
MANUAL_RESUME = "manual_resume"

EVENT_KINDS = (HARD_CAPACITY, MANUAL_PAUSE, TRANSIENT_FAILURE, SUCCESS, MANUAL_RESUME)

# How long a transient failure cools a provider, by consecutive-failure count.
TRANSIENT_BACKOFF_MINUTES = (1, 2, 5, 15, 30)

SCHEMA = """
CREATE TABLE IF NOT EXISTS provider_observations(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider TEXT NOT NULL,
  event_kind TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  attempt_id TEXT,
  reset_at TEXT,
  confidence REAL NOT NULL DEFAULT 1.0,
  detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_provider_observations_provider
  ON provider_observations(provider, id);
"""


def _parse(ts: Optional[str]) -> Optional[dt.datetime]:
    if not ts:
        return None
    text = ts.strip().replace(" ", "T", 1) if " " in ts.strip() else ts.strip()
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _iso(moment: dt.datetime) -> str:
    return moment.astimezone(UTC).replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class Observation:
    """One thing that was observed about a provider. Never edited."""

    provider: str
    event_kind: str
    observed_at: dt.datetime
    attempt_id: Optional[str] = None
    reset_at: Optional[dt.datetime] = None
    confidence: float = 1.0
    detail: str = ""

    def __post_init__(self) -> None:
        if self.event_kind not in EVENT_KINDS:
            raise ValueError(f"unknown event_kind {self.event_kind!r}")


@dataclass(frozen=True)
class ProviderState:
    """Derived. Never stored as the source of truth."""

    provider: str
    status: str
    circuit_until: Optional[dt.datetime] = None
    consecutive_failures: int = 0
    hard_capacity_hits: int = 0
    last_success_at: Optional[dt.datetime] = None
    last_error: str = ""
    reason: str = ""

    def available_at(self, now: dt.datetime) -> bool:
        if self.status in ("manual_pause",):
            return False
        if self.circuit_until is None:
            return True
        return now >= self.circuit_until


def derive_state(
    provider: str,
    observations: Sequence[Observation],
    now: dt.datetime,
) -> ProviderState:
    """Fold observations into a circuit decision.

    The ordering rule that Run 6 lacked: a hard-capacity observation carrying an
    explicit `reset_at` in the future dominates every later transient event. A
    success does not clear it either -- a provider can serve one cached response
    and still be out of capacity. Only reaching `reset_at`, or an explicit
    manual resume, ends a hard capacity window.
    """
    ordered = sorted(observations, key=lambda o: (o.observed_at, o.event_kind))

    consecutive_failures = 0
    hard_hits = 0
    last_success: Optional[dt.datetime] = None
    last_error = ""
    transient_until: Optional[dt.datetime] = None
    hard_until: Optional[dt.datetime] = None
    paused = False
    resumed_at: Optional[dt.datetime] = None

    for observation in ordered:
        kind = observation.event_kind
        if kind == SUCCESS:
            consecutive_failures = 0
            last_success = observation.observed_at
            transient_until = None
        elif kind == TRANSIENT_FAILURE:
            consecutive_failures += 1
            last_error = observation.detail
            index = min(consecutive_failures, len(TRANSIENT_BACKOFF_MINUTES)) - 1
            minutes = TRANSIENT_BACKOFF_MINUTES[index]
            transient_until = observation.observed_at + dt.timedelta(minutes=minutes)
        elif kind == HARD_CAPACITY:
            consecutive_failures += 1
            hard_hits += 1
            last_error = observation.detail
            # An explicit reset time is authoritative. Without one, assume the
            # window outlasts anything a transient backoff would produce.
            candidate = observation.reset_at or (
                observation.observed_at + dt.timedelta(hours=24)
            )
            hard_until = max(hard_until, candidate) if hard_until else candidate
        elif kind == MANUAL_PAUSE:
            paused = True
            last_error = observation.detail
        elif kind == MANUAL_RESUME:
            paused = False
            resumed_at = observation.observed_at
            hard_until = None
            transient_until = None
            consecutive_failures = 0

    del resumed_at  # ordering already applied by the fold above

    if paused:
        return ProviderState(
            provider=provider,
            status="manual_pause",
            circuit_until=None,
            consecutive_failures=consecutive_failures,
            hard_capacity_hits=hard_hits,
            last_success_at=last_success,
            last_error=last_error,
            reason="paused by operator; requires explicit resume",
        )

    if hard_until and now < hard_until:
        return ProviderState(
            provider=provider,
            status="capacity_exhausted",
            circuit_until=hard_until,
            consecutive_failures=consecutive_failures,
            hard_capacity_hits=hard_hits,
            last_success_at=last_success,
            last_error=last_error,
            reason=f"hard capacity until {_iso(hard_until)}",
        )

    if transient_until and now < transient_until:
        return ProviderState(
            provider=provider,
            status="cooling",
            circuit_until=transient_until,
            consecutive_failures=consecutive_failures,
            hard_capacity_hits=hard_hits,
            last_success_at=last_success,
            last_error=last_error,
            reason=f"transient backoff until {_iso(transient_until)}",
        )

    return ProviderState(
        provider=provider,
        status="ready",
        circuit_until=None,
        consecutive_failures=consecutive_failures,
        hard_capacity_hits=hard_hits,
        last_success_at=last_success,
        last_error=last_error,
        reason="no active circuit",
    )


# ---------------------------------------------------------------- persistence


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.commit()


def record(
    db: sqlite3.Connection,
    provider: str,
    event_kind: str,
    *,
    observed_at: Optional[dt.datetime] = None,
    attempt_id: Optional[str] = None,
    reset_at: Optional[dt.datetime] = None,
    confidence: float = 1.0,
    detail: str = "",
) -> None:
    """Append one observation. There is deliberately no update or delete."""
    if event_kind not in EVENT_KINDS:
        raise ValueError(f"unknown event_kind {event_kind!r}")
    moment = observed_at or dt.datetime.now(UTC)
    db.execute(
        "INSERT INTO provider_observations"
        "(provider,event_kind,observed_at,attempt_id,reset_at,confidence,detail)"
        " VALUES(?,?,?,?,?,?,?)",
        (
            provider,
            event_kind,
            _iso(moment),
            attempt_id,
            _iso(reset_at) if reset_at else None,
            confidence,
            detail[-1000:],
        ),
    )
    db.commit()


def load_observations(db: sqlite3.Connection, provider: str) -> list[Observation]:
    rows = db.execute(
        "SELECT provider,event_kind,observed_at,attempt_id,reset_at,confidence,detail"
        " FROM provider_observations WHERE provider=? ORDER BY id",
        (provider,),
    ).fetchall()
    loaded = []
    for row in rows:
        observed = _parse(row[2])
        if observed is None:
            continue
        loaded.append(
            Observation(
                provider=row[0],
                event_kind=row[1],
                observed_at=observed,
                attempt_id=row[3],
                reset_at=_parse(row[4]),
                confidence=row[5],
                detail=row[6] or "",
            )
        )
    return loaded


def current_state(
    db: sqlite3.Connection, provider: str, now: Optional[dt.datetime] = None
) -> ProviderState:
    return derive_state(
        provider, load_observations(db, provider), now or dt.datetime.now(UTC)
    )


def classify_failure(
    text: str, now: Optional[dt.datetime] = None
) -> tuple[str, Optional[dt.datetime]]:
    """Map provider stderr onto an event kind, and recover an explicit reset.

    The phrase list is carried over from the Run 6 supervisor because it was
    tuned against real provider output -- including the gap the run itself
    found, where the matcher had "usage limit reached" but Codex actually says
    "You've hit your usage limit". Both are covered by the "usage limit"
    substring.

    The addition is reset-time extraction. Throwing the stated reset away is
    what forced the old code to guess a circuit length. The two providers state
    it differently, and both real forms are handled:

        codex:  "... or try again at Aug 19th, 2026 10:53 PM."
        claude: "session limit · resets 6:20am (America/Chicago)"
    """
    low = text.lower()
    hard_markers = (
        "usage limit",
        "out of credits",
        "no credits remaining",
        "purchase more credits",
        "quota exhausted",
        "insufficient_quota",
        "agentic usage limit",
        "session limit",
        "weekly limit",
        "capacity limit",
    )
    transient_markers = (
        "rate limit",
        "429",
        "temporarily unavailable",
        "service unavailable",
        "502",
        "503",
        "504",
        "timed out",
        "timeout",
        "connection reset",
        "try again later",
        "overloaded",
    )
    if any(marker in low for marker in hard_markers):
        return HARD_CAPACITY, _extract_reset(text, now or dt.datetime.now(UTC))
    if any(marker in low for marker in transient_markers):
        return TRANSIENT_FAILURE, None
    return TRANSIENT_FAILURE, None


_MONTHS = {
    m.lower(): i
    for i, m in enumerate(
        "January February March April May June July August "
        "September October November December".split(),
        start=1,
    )
}


def _to_24h(hour: int, meridiem: str) -> int:
    meridiem = meridiem.lower()
    if meridiem == "pm" and hour != 12:
        return hour + 12
    if meridiem == "am" and hour == 12:
        return 0
    return hour


def _month_number(name: str) -> Optional[int]:
    lowered = name.lower()
    if lowered in _MONTHS:
        return _MONTHS[lowered]
    for full, number in _MONTHS.items():
        if full.startswith(lowered):
            return number
    return None


def _extract_reset(text: str, now: dt.datetime) -> Optional[dt.datetime]:
    """Recover the reset instant from either provider's real wording.

    Handles, in order of specificity:
      - ISO:      "2026-08-19T22:53:00Z"
      - absolute: "Aug 19th, 2026 10:53 PM"  (note the ordinal suffix)
      - wall:     "resets 6:20am (America/Chicago)" -> next such local time
    """
    import re

    iso = re.search(r"\b(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2})?)", text)
    if iso:
        parsed = _parse(iso.group(1))
        if parsed:
            return parsed

    named = re.search(
        r"\b([A-Za-z]{3,9})\.?\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})"
        r"(?:[,\s]+(\d{1,2}):(\d{2})\s*([AaPp]\.?[Mm]\.?)?)?",
        text,
    )
    if named:
        month = _month_number(named.group(1))
        if month is not None:
            hour = int(named.group(4)) if named.group(4) else 0
            minute = int(named.group(5)) if named.group(5) else 0
            hour = _to_24h(hour, (named.group(6) or "").replace(".", ""))
            try:
                return dt.datetime(
                    int(named.group(3)),
                    month,
                    int(named.group(2)),
                    hour,
                    minute,
                    tzinfo=UTC,
                )
            except ValueError:
                return None

    # Wall-clock form with no date: the next occurrence of that local time.
    wall = re.search(
        r"resets?\s+(\d{1,2}):(\d{2})\s*([AaPp]\.?[Mm]\.?)?\s*\(([^)]+)\)", text
    )
    if not wall:
        return None
    zone = _zone(wall.group(4).strip())
    if zone is None:
        return None
    hour = _to_24h(int(wall.group(1)), (wall.group(3) or "").replace(".", ""))
    minute = int(wall.group(2))
    local_now = now.astimezone(zone)
    try:
        candidate = local_now.replace(
            hour=hour, minute=minute, second=0, microsecond=0
        )
    except ValueError:
        return None
    if candidate <= local_now:
        candidate += dt.timedelta(days=1)
    return candidate.astimezone(UTC)


def _zone(name: str) -> Optional[dt.tzinfo]:
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(name)
    except Exception:
        return None
