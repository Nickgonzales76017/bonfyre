"""Scheduling as reheat conditions, not a cron table.

Run 6's scheduling unit was "all five planes finished supercycle N", which is
why a static blocker like Proton kept consuming frontier attention: the loop had
no way to express "stop looking at this until something changes". The
retrospective's own prescription is that a provider should wake because there is
new entropy, not because it is that plane's turn.

So a watch is cooled until either a deadline arrives or a named condition
changes. Structural cooling, applied to attention:

    HOT     in a Context Cut every pass
    WARM    revisited on a schedule
    COOL    watcher-only; reheats on a specific signal
    COLD    closed
"""

from __future__ import annotations

import datetime as dt
import sqlite3
from dataclasses import dataclass
from typing import Optional

UTC = dt.timezone.utc

HOT = "hot"
WARM = "warm"
COOL = "cool"
COLD = "cold"

TEMPERATURES = (HOT, WARM, COOL, COLD)

SCHEMA = """
CREATE TABLE IF NOT EXISTS watches(
  watch_id TEXT PRIMARY KEY,
  subject TEXT NOT NULL,
  temperature TEXT NOT NULL DEFAULT 'warm',
  reheat_at TEXT,
  reheat_on TEXT NOT NULL DEFAULT '',
  attempts INTEGER NOT NULL DEFAULT 0,
  last_checked_at TEXT,
  last_change_at TEXT,
  detail TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_watches_due ON watches(temperature, reheat_at);
"""

# After this many checks with no observed change, a watch stops riding in the
# frontier cut and becomes watcher-only. Proton was checked far past this.
COOL_AFTER_UNCHANGED_ATTEMPTS = 3


def _iso(moment: dt.datetime) -> str:
    return moment.astimezone(UTC).replace(microsecond=0).isoformat()


def _parse(text: Optional[str]) -> Optional[dt.datetime]:
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


@dataclass(frozen=True)
class Watch:
    watch_id: str
    subject: str
    temperature: str
    reheat_at: Optional[dt.datetime]
    reheat_on: str
    attempts: int
    detail: str = ""


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.commit()


def schedule(
    db: sqlite3.Connection,
    watch_id: str,
    subject: str,
    *,
    temperature: str = WARM,
    reheat_at: Optional[dt.datetime] = None,
    reheat_on: str = "",
    detail: str = "",
    now: Optional[dt.datetime] = None,
) -> None:
    if temperature not in TEMPERATURES:
        raise ValueError(f"unknown temperature {temperature!r}")
    if temperature == COOL and not (reheat_at or reheat_on):
        raise ValueError(
            f"{watch_id}: a cooled watch needs a reheat condition or it is just forgotten"
        )
    db.execute(
        "INSERT INTO watches"
        "(watch_id,subject,temperature,reheat_at,reheat_on,detail,created_at)"
        " VALUES(?,?,?,?,?,?,?)"
        " ON CONFLICT(watch_id) DO UPDATE SET"
        "  subject=excluded.subject, temperature=excluded.temperature,"
        "  reheat_at=excluded.reheat_at, reheat_on=excluded.reheat_on,"
        "  detail=excluded.detail",
        (
            watch_id,
            subject,
            temperature,
            _iso(reheat_at) if reheat_at else None,
            reheat_on,
            detail,
            _iso(now or dt.datetime.now(UTC)),
        ),
    )
    db.commit()


def record_check(
    db: sqlite3.Connection,
    watch_id: str,
    changed: bool,
    now: Optional[dt.datetime] = None,
) -> str:
    """Record one observation, cooling the watch if nothing keeps changing.

    Returns the resulting temperature. This is the mechanism Proton needed: the
    deficit stays tracked, but stops being re-read by a frontier model on every
    pass once it has demonstrably not moved.
    """
    moment = now or dt.datetime.now(UTC)
    row = db.execute(
        "SELECT temperature,attempts,reheat_at,reheat_on FROM watches WHERE watch_id=?",
        (watch_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"no watch {watch_id}")
    temperature, attempts, reheat_at, reheat_on = row

    if changed:
        db.execute(
            "UPDATE watches SET attempts=0,temperature='hot',last_checked_at=?,"
            "last_change_at=? WHERE watch_id=?",
            (_iso(moment), _iso(moment), watch_id),
        )
        db.commit()
        return HOT

    attempts += 1
    new_temperature = temperature
    if temperature in (HOT, WARM) and attempts >= COOL_AFTER_UNCHANGED_ATTEMPTS:
        # Only cool into a real condition; otherwise stay warm rather than
        # silently dropping the subject.
        new_temperature = COOL if (reheat_at or reheat_on) else WARM
    db.execute(
        "UPDATE watches SET attempts=?,temperature=?,last_checked_at=? WHERE watch_id=?",
        (attempts, new_temperature, _iso(moment), watch_id),
    )
    db.commit()
    return new_temperature


def reheat(
    db: sqlite3.Connection, watch_id: str, now: Optional[dt.datetime] = None
) -> None:
    moment = now or dt.datetime.now(UTC)
    db.execute(
        "UPDATE watches SET temperature='hot',attempts=0,last_change_at=? WHERE watch_id=?",
        (_iso(moment), watch_id),
    )
    db.commit()


def due(db: sqlite3.Connection, now: Optional[dt.datetime] = None) -> list[Watch]:
    """Watches that should be looked at right now.

    Hot and warm watches are always due. A cooled watch is due only once its
    deadline has passed -- its `reheat_on` signal is delivered by whoever
    observes the signal, via `reheat`.
    """
    moment = now or dt.datetime.now(UTC)
    rows = db.execute(
        "SELECT watch_id,subject,temperature,reheat_at,reheat_on,attempts,detail"
        " FROM watches WHERE temperature!='cold' ORDER BY watch_id"
    ).fetchall()
    ready = []
    for row in rows:
        watch = Watch(
            watch_id=row[0],
            subject=row[1],
            temperature=row[2],
            reheat_at=_parse(row[3]),
            reheat_on=row[4],
            attempts=row[5],
            detail=row[6],
        )
        if watch.temperature in (HOT, WARM):
            ready.append(watch)
        elif watch.reheat_at and moment >= watch.reheat_at:
            ready.append(watch)
    return ready


def frontier_subjects(db: sqlite3.Connection, now: Optional[dt.datetime] = None) -> list[str]:
    """What belongs in the next Context Cut. Cooled watches are excluded."""
    return [watch.subject for watch in due(db, now) if watch.temperature != COOL]


def close(db: sqlite3.Connection, watch_id: str) -> None:
    db.execute("UPDATE watches SET temperature='cold' WHERE watch_id=?", (watch_id,))
    db.commit()
