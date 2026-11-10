"""UnlockGraph: reachable institutional capacity over real control-plane state.

The atlas entered the opportunity family at ``architectural`` -- named, not built.
This builds it, and builds it the only honest way: as a reachability computation
over state that already exists. An opportunity is reachable not because a
document says so but because its blockers resolve against real actor
verification, a real proof frontier, and real work state.

The vocabulary:

    Blocker   something concrete that prevents an opportunity from being reached
              -- an unverified actor, an unproven layer, an unfinished work item.
    Unlock    a reachable action that removes a blocker, itself possibly
              depending on other blockers or opportunities. An unlock is only
              real if it is authorized; an unlock that exists but is not
              authorized does not make the blocker go away.
    Opportunity  an eligibility question with a set of blockers.

Reachability is a fixpoint. An opportunity is ``reachable_now`` when it has no
open blocker; ``unlockable`` when every open blocker has an authorized unlock
whose own preconditions hold (transitively); ``blocked`` when some blocker has
no reachable authorized unlock. The engine reports the exact blocking chain, so
the answer is never a bare yes/no.

Two laws it must not break, carried from the atlas as forbidden inferences:

  * an unlock existing does not make it authorized or free;
  * a blocker resolving does not mean the opportunity is *won* -- reachability
    stops at the point a human or an external commit takes over, exactly like
    AtomicForm stops at the submit line.

Resolvers query real tables. A blocker kind with no substrate yet (authority,
budget, human approval) resolves to *unknown* and is treated as open needing an
external unlock -- it is never silently assumed satisfied.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Optional

import authority
import relationship
import resource_activation

# ------------------------------------------------------------------ blockers

IDENTITY_VERIFICATION = "identity_verification"  # an actor must be VERIFIED
PROOF_LAYER = "proof_layer"                       # a frontier layer must be proven
WORK_DONE = "work_done"                            # a work item must be satisfied
SERVICE_BOUND = "service_bound"                     # a service must be bound
AUTHORITY = "authority"                             # an authority edge must exist
RELATIONSHIP_STAGE = "relationship_stage"           # a relationship must have advanced
RESOURCE_ACTIVE = "resource_active"                 # a resource must be activated
BUDGET = "budget"                                   # a budget/commitment must exist
HUMAN_APPROVAL = "human_approval"                   # a human must approve
FIELD = "field"                                     # a form field must be filled

# Kinds this engine can decide against real substrate today. Everything else is
# honestly unknown until its architecture is built.
_RESOLVABLE = {IDENTITY_VERIFICATION, PROOF_LAYER, WORK_DONE, SERVICE_BOUND,
               AUTHORITY, RELATIONSHIP_STAGE, RESOURCE_ACTIVE}

REACHABLE_NOW = "reachable_now"
UNLOCKABLE = "unlockable"
BLOCKED = "blocked"


@dataclass(frozen=True)
class Blocker:
    kind: str
    subject: str
    detail: str = ""
    # for proof_layer: subject is the resource; these pin the exact layer.
    profile: str = ""
    layer: str = ""
    # for authority: which actor must hold which permission over the subject.
    actor: str = ""
    permission: str = ""

    @property
    def ref(self) -> tuple[str, str]:
        return (self.kind, self.subject)


@dataclass(frozen=True)
class Unlock:
    unlock_id: str
    removes_kind: str
    removes_subject: str
    action: str
    authorized: bool = False
    # preconditions: ("blocker", kind, subject) resolved, or ("opportunity", id) reachable.
    requires: tuple[tuple[str, ...], ...] = ()

    @property
    def removes(self) -> tuple[str, str]:
        return (self.removes_kind, self.removes_subject)


@dataclass(frozen=True)
class Opportunity:
    opp_id: str
    title: str
    blockers: tuple[Blocker, ...] = ()
    eligibility: tuple[str, ...] = ()


@dataclass(frozen=True)
class OppEval:
    opp_id: str
    status: str
    open_blockers: tuple[Blocker, ...]
    covered_by: dict[tuple[str, str], str]        # blocker ref -> unlock_id
    hard_blockers: tuple[Blocker, ...]            # open, no reachable authorized unlock

    def to_dict(self) -> dict:
        return {
            "opportunity": self.opp_id,
            "status": self.status,
            "open_blockers": [{"kind": b.kind, "subject": b.subject, "detail": b.detail}
                              for b in self.open_blockers],
            "covered_by": {f"{k[0]}:{k[1]}": v for k, v in self.covered_by.items()},
            "hard_blockers": [{"kind": b.kind, "subject": b.subject} for b in self.hard_blockers],
        }


# ------------------------------------------------------------------ resolvers


def _actor_verified(db: sqlite3.Connection, actor_id: str) -> Optional[bool]:
    if not _table_exists(db, "actor_nodes"):
        return None
    row = db.execute("SELECT confidence FROM actor_nodes WHERE actor_id=?", (actor_id,)).fetchone()
    if row is None:
        return None
    return row[0] == "verified"


def _layer_proven(db: sqlite3.Connection, resource: str, profile: str, layer: str) -> Optional[bool]:
    if not _table_exists(db, "frontier_layers"):
        return None
    row = db.execute(
        "SELECT status FROM frontier_layers"
        " WHERE subject_resource=? AND subject_profile=? AND layer=?",
        (resource, profile, layer),
    ).fetchone()
    if row is None:
        return None
    return row[0] == "proven"


def _work_satisfied(db: sqlite3.Connection, subject: str) -> Optional[bool]:
    if not _table_exists(db, "work_items"):
        return None
    row = db.execute(
        "SELECT state FROM work_items WHERE subject_ref=? ORDER BY id DESC LIMIT 1",
        (subject,),
    ).fetchone()
    if row is None:
        return None
    return row[0] in ("satisfied", "effected")


def _work_satisfied_fabric(fabric_db: sqlite3.Connection, subject: str) -> Optional[bool]:
    """Is a real fabric WorkGraph node for this subject complete?

    Matches the blocker subject against a node's id or its operator, and treats
    the fabric's terminal-success status ('complete') as done. Returns None when
    no such node exists -- unknown, never assumed."""
    if not _table_exists(fabric_db, "workgraph_nodes"):
        return None
    row = fabric_db.execute(
        "SELECT status FROM workgraph_nodes WHERE node_id=? OR operator_id=?"
        " ORDER BY updated_at_ms DESC LIMIT 1",
        (subject, subject),
    ).fetchone()
    if row is None:
        return None
    return row[0] == "complete"


def blocker_resolved(
    db: sqlite3.Connection, blocker: Blocker, *,
    bound_services: frozenset[str] = frozenset(),
    fabric_db: Optional[sqlite3.Connection] = None,
) -> Optional[bool]:
    """Is this blocker currently resolved? True/False, or None when the substrate
    to decide it does not exist yet (never assumed satisfied).

    When ``fabric_db`` is given, a work_done blocker is decided against the real
    fabric WorkGraph first -- reachability reflects the system's own work, not the
    control plane's shadow copy -- falling back to the local work_items only when
    the fabric has no node for it."""
    if blocker.kind == IDENTITY_VERIFICATION:
        return _actor_verified(db, blocker.subject)
    if blocker.kind == PROOF_LAYER:
        return _layer_proven(db, blocker.subject, blocker.profile, blocker.layer)
    if blocker.kind == WORK_DONE:
        if fabric_db is not None and _table_exists(fabric_db, "workgraph_nodes"):
            # workgraph_nodes is canonical: the shadow work_items never decides
            # reachability once the real fabric WorkGraph is in play.
            return _work_satisfied_fabric(fabric_db, blocker.subject)
        return _work_satisfied(db, blocker.subject)
    if blocker.kind == SERVICE_BOUND:
        return blocker.subject in bound_services
    if blocker.kind == AUTHORITY:
        return authority.has_authority(
            db, blocker.actor, blocker.permission or authority.ACT, blocker.subject,
            purpose=blocker.detail or None,
        )
    if blocker.kind == RELATIONSHIP_STAGE:
        # subject = profile, actor = the counterpart, layer = required stage.
        return relationship.stage_at_least(db, blocker.actor, blocker.subject, blocker.layer)
    if blocker.kind == RESOURCE_ACTIVE:
        if not resource_activation._table_exists(db, "resource_candidates"):
            return None
        return resource_activation.is_activated(db, blocker.subject, bound_services=bound_services)
    return None  # budget / human_approval / field: no substrate yet


# ------------------------------------------------------------------ fixpoint


def _table_exists(db: sqlite3.Connection, name: str) -> bool:
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def reachable_capacity(
    db: sqlite3.Connection,
    opportunities: list[Opportunity],
    unlocks: list[Unlock],
    *,
    bound_services: frozenset[str] = frozenset(),
    fabric_db: Optional[sqlite3.Connection] = None,
) -> dict[str, OppEval]:
    """Compute each opportunity's reachability status to a fixpoint.

    An open blocker is *covered* by an authorized unlock whose preconditions all
    hold: a required blocker must be resolved, and a required opportunity must be
    reachable_now or unlockable. Because an unlock may depend on another
    opportunity, this iterates until no status changes.
    """
    # Resolve every blocker once against real state (None -> open, unknown cause).
    resolved: dict[tuple[str, str], bool] = {}
    for opp in opportunities:
        for b in opp.blockers:
            resolved[b.ref] = blocker_resolved(
                db, b, bound_services=bound_services, fabric_db=fabric_db) is True

    unlock_by_target: dict[tuple[str, str], list[Unlock]] = {}
    for u in unlocks:
        unlock_by_target.setdefault(u.removes, []).append(u)

    status: dict[str, str] = {opp.opp_id: BLOCKED for opp in opportunities}

    def precondition_ok(req: tuple[str, ...]) -> bool:
        if req and req[0] == "blocker" and len(req) >= 3:
            return resolved.get((req[1], req[2]), False)
        if req and req[0] == "opportunity" and len(req) >= 2:
            return status.get(req[1]) in (REACHABLE_NOW, UNLOCKABLE)
        return False

    def blocker_covered(b: Blocker) -> Optional[str]:
        for u in unlock_by_target.get(b.ref, []):
            if u.authorized and all(precondition_ok(r) for r in u.requires):
                return u.unlock_id
        return None

    evals: dict[str, OppEval] = {}
    for _ in range(len(opportunities) + 1):
        changed = False
        for opp in opportunities:
            open_blockers = tuple(b for b in opp.blockers if not resolved.get(b.ref, False))
            covered: dict[tuple[str, str], str] = {}
            hard: list[Blocker] = []
            for b in open_blockers:
                uid = blocker_covered(b)
                if uid:
                    covered[b.ref] = uid
                else:
                    hard.append(b)
            if not open_blockers:
                new_status = REACHABLE_NOW
            elif not hard:
                new_status = UNLOCKABLE
            else:
                new_status = BLOCKED
            if status[opp.opp_id] != new_status:
                status[opp.opp_id] = new_status
                changed = True
            evals[opp.opp_id] = OppEval(
                opp_id=opp.opp_id, status=new_status, open_blockers=open_blockers,
                covered_by=covered, hard_blockers=tuple(hard),
            )
        if not changed:
            break
    return evals


def load_pack(text: str) -> tuple[list[Opportunity], list[Unlock]]:
    """Parse an opportunities pack into Opportunity and Unlock objects.

    A campaign is data, not code: an opportunity is a scope and a body of
    blockers. Grammar is the estate YaFF -- ``opportunity``/``blocker``/``unlock``
    blocks with indented fields; a blocker names its parent ``opportunity``.
    """
    opp_meta: dict[str, dict[str, str]] = {}
    blockers: dict[str, list[Blocker]] = {}
    unlocks: list[Unlock] = []
    current_kind = current_id = ""
    fields: dict[str, str] = {}
    reqs: list[tuple[str, ...]] = []

    def flush() -> None:
        if current_kind == "opportunity":
            opp_meta[current_id] = dict(fields)
        elif current_kind == "blocker":
            parent = fields.get("opportunity", "")
            blockers.setdefault(parent, []).append(Blocker(
                kind=fields.get("kind", ""), subject=fields.get("subject", ""),
                detail=fields.get("detail", ""), profile=fields.get("profile", ""),
                layer=fields.get("layer", ""),
            ))
        elif current_kind == "unlock":
            unlocks.append(Unlock(
                unlock_id=current_id, removes_kind=fields.get("removes_kind", ""),
                removes_subject=fields.get("removes_subject", ""),
                action=fields.get("action", ""),
                authorized=fields.get("authorized", "false").lower() == "true",
                requires=tuple(reqs),
            ))

    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        key, _, value = raw.strip().partition(" ")
        value = value.strip()
        if raw[0].isspace():
            if key == "requires":
                reqs.append(tuple(value.split()))
            else:
                fields[key] = value
            continue
        if key in ("opportunity", "blocker", "unlock"):
            flush()
            current_kind, current_id = key, value
            fields, reqs = {}, []
    flush()

    opportunities = [
        Opportunity(
            opp_id=oid, title=meta.get("title", oid),
            blockers=tuple(blockers.get(oid, ())),
            eligibility=tuple(),
        )
        for oid, meta in opp_meta.items()
    ]
    return opportunities, unlocks


def capacity_summary(evals: dict[str, OppEval]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {REACHABLE_NOW: [], UNLOCKABLE: [], BLOCKED: []}
    for opp_id, ev in evals.items():
        out[ev.status].append(opp_id)
    return {k: sorted(v) for k, v in out.items()}
