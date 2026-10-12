"""BitNet-adjacent route ranker: rank the funnel's survivors, override nothing.

The AddressPlane funnel deterministically rejects the impossible -- capability,
authority, proof. What remains is a small set of *eligible* candidates. This is
the learned optimizer that sits after that pruning: a tiny ternary model (weights
in {-1, 0, +1}, a BitNet-shaped linear head) that ranks the survivors by predicted
success, so the exact-and-expensive closure evaluates the promising few first.

It is trained on the routing records the system already produces -- every work
item is an outcome (satisfied/effected = success, failed = failure) with
structural features. The weights are fit from the sign of each feature's
correlation with success and thresholded to ternary, so a weak signal collapses
to 0 rather than pretending to matter.

The constitutional boundary is absolute: the ranker only REORDERS eligible
survivors. It never changes eligibility, never grants authority or proves
anything, and the exact closure still verifies the top choice. A better ranking
is optimization, never a decision the fence did not already permit.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Optional, Sequence

# feature layout, fixed so weights are interpretable
F_PRIORITY = 0          # normalized priority
F_HAS_TARGET = 1        # a concrete target plane is set
F_RETRY_FREE = 2        # no retries consumed
FEATURES = 3


@dataclass(frozen=True)
class RouteRecord:
    features: tuple[float, ...]
    success: bool


@dataclass(frozen=True)
class TernaryRanker:
    weights: tuple[int, ...]     # each in {-1, 0, +1}
    trained_on: int

    def score(self, features: Sequence[float]) -> float:
        return sum(w * f for w, f in zip(self.weights, features))

    def rank(self, candidates: dict[str, Sequence[float]]) -> list[tuple[str, float]]:
        """Survivors, best predicted first. Ties are broken by id for stability."""
        return sorted(
            ((cid, self.score(f)) for cid, f in candidates.items()),
            key=lambda t: (-t[1], t[0]),
        )

    @classmethod
    def train(cls, records: Sequence[RouteRecord], *, threshold: float = 0.05) -> "TernaryRanker":
        """Fit ternary weights from feature-success correlation. A feature whose
        success/failure means differ by less than ``threshold`` gets weight 0 --
        no pretending a weak signal matters."""
        if not records:
            return cls(weights=tuple([0] * FEATURES), trained_on=0)
        n_feat = len(records[0].features)
        pos = [r for r in records if r.success]
        neg = [r for r in records if not r.success]
        weights: list[int] = []
        for j in range(n_feat):
            pm = sum(r.features[j] for r in pos) / len(pos) if pos else 0.0
            nm = sum(r.features[j] for r in neg) / len(neg) if neg else 0.0
            diff = pm - nm
            weights.append(1 if diff > threshold else (-1 if diff < -threshold else 0))
        return cls(weights=tuple(weights), trained_on=len(records))


def records_from_work(db: sqlite3.Connection) -> list[RouteRecord]:
    """Build routing records from real work-item outcomes."""
    if not _table_exists(db, "work_items"):
        return []
    rows = db.execute(
        "SELECT priority, target_plane, attempt, state FROM work_items"
    ).fetchall() if _has_columns(db, "work_items", ("priority", "target_plane", "attempt", "state")) \
        else db.execute("SELECT priority, target_plane, state FROM work_items").fetchall()
    records: list[RouteRecord] = []
    max_prio = max((r[0] or 0) for r in rows) or 1
    for r in rows:
        priority = (r[0] or 0) / max_prio
        target = r[1] or ""
        state = r[-1]
        attempt = r[2] if len(r) == 4 else 0
        features = (
            float(priority),
            1.0 if target else 0.0,
            1.0 if (attempt or 0) == 0 else 0.0,
        )
        records.append(RouteRecord(features=features, success=state in ("satisfied", "effected")))
    return records


def command_features(db: sqlite3.Connection, family: str) -> tuple[float, ...]:
    """The same feature layout, derived for a candidate command family -- so the
    ranker can score the funnel's survivors with the model trained on outcomes."""
    est = db.execute("SELECT estate FROM estate_catalog WHERE family=?", (family,)).fetchone() \
        if _table_exists(db, "estate_catalog") else None
    loc = db.execute("SELECT location FROM capability_identities WHERE public_name=?", (family,)).fetchone() \
        if _table_exists(db, "capability_identities") else None
    return (
        1.0 if est and est[0] == "model" else 0.0,
        1.0 if loc and loc[0] else 0.0,
        1.0,
    )


def rank_commands(db: sqlite3.Connection, families: Sequence[str],
                  model: TernaryRanker) -> list[tuple[str, float]]:
    """Rank the funnel's surviving command families by predicted success. This is
    the learned stage after deterministic pruning -- it reorders eligibles, never
    changes what was eligible."""
    candidates = {f: command_features(db, f) for f in families}
    return model.rank(candidates)


def records_from_capabilities(db: sqlite3.Connection) -> list[RouteRecord]:
    """Routing records from the capability estate: a command promoted to
    workload_proven actually ran a real workload -- a success. Features are the
    command's estate and whether it self-describes, both known before it runs."""
    if not _table_exists(db, "capability_identities"):
        return []
    join = ("SELECT ci.maturity, COALESCE(ec.estate,''), ci.location"
            " FROM capability_identities ci LEFT JOIN estate_catalog ec"
            " ON ci.public_name=ec.family") if _table_exists(db, "estate_catalog") \
        else "SELECT maturity, '', location FROM capability_identities"
    records: list[RouteRecord] = []
    for maturity, estate, location in db.execute(join):
        features = (
            1.0 if estate == "model" else 0.0,      # model-estate commands
            1.0 if location else 0.0,               # has a resolved binary
            1.0,                                     # bias
        )
        records.append(RouteRecord(features=features, success=(maturity == "workload_proven")))
    return records


def _table_exists(db: sqlite3.Connection, name: str) -> bool:
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _has_columns(db: sqlite3.Connection, table: str, cols: tuple[str, ...]) -> bool:
    have = {r[1] for r in db.execute(f"PRAGMA table_info({table})")}
    return all(c in have for c in cols)
