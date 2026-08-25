"""What Bonfyre can actually do right now, compiled once instead of rediscovered.

Run 4 machine-verified 91 public Bonfyre command identities. A native inventory
query in the same run reported `compiled tools = 0`, and later planes burned
frontier cognition probing PATH for names like `BonfyreQueue` and
`BonfyreOffer` that did not resolve. The gap is not "some binaries were not
installed", it is that **public capability identity is not the same thing as
live callable capability**, and nothing in the run represented the difference.

The maturity ladder here is the operating model's, unchanged:

    defined -> implemented -> built -> installed -> resolvable ->
    version_aligned -> health_probed -> activated -> workload_proven ->
    quality_proven -> promoted

The rung that matters for planning is `resolvable`: below it a name is an
intention, at or above it there is something to call. `--help` working means
nothing on its own, which is why `health_probed` sits four rungs higher.
"""

from __future__ import annotations

import datetime as dt
import json
import shutil
import sqlite3
from dataclasses import dataclass
from typing import Callable, Iterable, Optional, Sequence

UTC = dt.timezone.utc

LADDER = (
    "defined",
    "implemented",
    "built",
    "installed",
    "resolvable",
    "version_aligned",
    "health_probed",
    "activated",
    "workload_proven",
    "quality_proven",
    "promoted",
)

_RUNG = {name: index for index, name in enumerate(LADDER)}

# At or above this rung there is something that can actually be invoked.
CALLABLE_FLOOR = _RUNG["resolvable"]
# At or above this rung we have evidence it does its job, not just that it runs.
PROVEN_FLOOR = _RUNG["workload_proven"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS capability_identities(
  public_name TEXT PRIMARY KEY,
  kind TEXT NOT NULL DEFAULT 'command',
  maturity TEXT NOT NULL DEFAULT 'defined',
  location TEXT NOT NULL DEFAULT '',
  contract TEXT NOT NULL DEFAULT '',
  placement TEXT NOT NULL DEFAULT 'local',
  authority TEXT NOT NULL DEFAULT 'ordinary',
  cost_hint TEXT NOT NULL DEFAULT '',
  proof_ref TEXT NOT NULL DEFAULT '',
  last_probed_at TEXT,
  probe_detail TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL
);
"""


@dataclass(frozen=True)
class Capability:
    public_name: str
    kind: str = "command"
    maturity: str = "defined"
    location: str = ""
    contract: str = ""
    placement: str = "local"
    authority: str = "ordinary"
    cost_hint: str = ""
    proof_ref: str = ""

    def __post_init__(self) -> None:
        if self.maturity not in _RUNG:
            raise ValueError(f"unknown maturity {self.maturity!r}")

    @property
    def rung(self) -> int:
        return _RUNG[self.maturity]

    @property
    def callable_now(self) -> bool:
        return self.rung >= CALLABLE_FLOOR

    @property
    def proven(self) -> bool:
        return self.rung >= PROVEN_FLOOR


def _iso(moment: dt.datetime) -> str:
    return moment.astimezone(UTC).replace(microsecond=0).isoformat()


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.commit()


def declare(
    db: sqlite3.Connection, capability: Capability, now: Optional[dt.datetime] = None
) -> None:
    db.execute(
        "INSERT INTO capability_identities"
        "(public_name,kind,maturity,location,contract,placement,authority,cost_hint,"
        " proof_ref,updated_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?)"
        " ON CONFLICT(public_name) DO UPDATE SET"
        "  kind=excluded.kind, maturity=excluded.maturity, location=excluded.location,"
        "  contract=excluded.contract, placement=excluded.placement,"
        "  authority=excluded.authority, cost_hint=excluded.cost_hint,"
        "  proof_ref=excluded.proof_ref, updated_at=excluded.updated_at",
        (
            capability.public_name,
            capability.kind,
            capability.maturity,
            capability.location,
            capability.contract,
            capability.placement,
            capability.authority,
            capability.cost_hint,
            capability.proof_ref,
            _iso(now or dt.datetime.now(UTC)),
        ),
    )
    db.commit()


Resolver = Callable[[str], Optional[str]]


def default_resolver(name: str) -> Optional[str]:
    return shutil.which(name)


def probe(
    db: sqlite3.Connection,
    resolver: Resolver = default_resolver,
    now: Optional[dt.datetime] = None,
) -> dict[str, int]:
    """Resolve every declared identity once, so no plane has to.

    A name that resolves is promoted to `resolvable` if it was below that rung.
    A name that does not resolve is demoted back to `implemented` -- the run
    that assumed installation was permanent is exactly how `BonfyreQueue` ended
    up being probed by five separate planes.
    """
    moment = now or dt.datetime.now(UTC)
    counts = {"resolved": 0, "unresolved": 0}
    rows = db.execute(
        "SELECT public_name,maturity,location FROM capability_identities"
    ).fetchall()
    for public_name, maturity, location in rows:
        target = location or public_name
        found = resolver(target)
        if found:
            counts["resolved"] += 1
            new_maturity = maturity if _RUNG[maturity] >= CALLABLE_FLOOR else "resolvable"
            db.execute(
                "UPDATE capability_identities SET maturity=?,location=?,"
                "last_probed_at=?,probe_detail=?,updated_at=? WHERE public_name=?",
                (new_maturity, found, _iso(moment), "resolved", _iso(moment), public_name),
            )
        else:
            counts["unresolved"] += 1
            demoted = "implemented" if _RUNG[maturity] >= CALLABLE_FLOOR else maturity
            db.execute(
                "UPDATE capability_identities SET maturity=?,last_probed_at=?,"
                "probe_detail=?,updated_at=? WHERE public_name=?",
                (demoted, _iso(moment), "not resolvable", _iso(moment), public_name),
            )
    db.commit()
    return counts


def load(db: sqlite3.Connection) -> list[Capability]:
    rows = db.execute(
        "SELECT public_name,kind,maturity,location,contract,placement,authority,"
        "cost_hint,proof_ref FROM capability_identities ORDER BY public_name"
    ).fetchall()
    return [
        Capability(
            public_name=row[0],
            kind=row[1],
            maturity=row[2],
            location=row[3],
            contract=row[4],
            placement=row[5],
            authority=row[6],
            cost_hint=row[7],
            proof_ref=row[8],
        )
        for row in rows
    ]


def context_packet(db: sqlite3.Connection) -> str:
    """The block a Context Cut carries so a model never shells out to `which`.

    Names below the callable floor are listed too, and explicitly marked, because
    silently omitting them is what makes a model go looking.
    """
    available, declared = [], []
    for capability in load(db):
        if capability.callable_now:
            marker = "proven" if capability.proven else capability.maturity
            available.append(
                f"  {capability.public_name}  {marker}  {capability.location}"
            )
        else:
            declared.append(f"  {capability.public_name}  {capability.maturity}")

    lines = ["AVAILABLE CAPABILITIES (callable now)"]
    lines.extend(available or ["  (none)"])
    lines.append("")
    lines.append("DECLARED BUT NOT CALLABLE (do not probe for these)")
    lines.extend(declared or ["  (none)"])
    return "\n".join(lines)


def summary(db: sqlite3.Connection) -> dict[str, int]:
    loaded = load(db)
    return {
        "declared": len(loaded),
        "callable": sum(1 for c in loaded if c.callable_now),
        "proven": sum(1 for c in loaded if c.proven),
    }
