"""AddressPlane: the compiled forwarding plane under the plural semantic estate.

The semantic graphs answer what things MEAN and stay rich and typed. This plane
answers a cheaper question first -- of the enormous state available, what is even
ELIGIBLE to participate in this operation -- so semantic richness never becomes
slow. It compiles existing state into bit masks and a ternary proof vector and
rejects vast populations with a single `(have & need) == need`, before any graph
is walked.

It reuses today's real machinery rather than inventing state:
  * authority masks are compiled from AuthorityGraph (authority.has_authority),
  * the ternary proof vector is compiled from the ProofFrontier -- +1 proven,
    0 unresolved, and -1 for a KnownNonCause: a hypothesis specifically
    disproven becomes a blackhole route the search never re-enters,
  * capability / kind masks are compiled from the estate catalog.

Constitutional line (atlas): bits are a compiled machine index, not meaning.
Eligibility never grants authority or proves anything -- it only cheaply rejects
the impossible so the exact graph closure runs on a small survivor set.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable, Optional

import authority
import proof_frontier as pf

# ternary epistemic state
PROVEN = 1
UNRESOLVED = 0
DISPROVEN = -1  # KnownNonCause -- a blackhole route


class BitRegistry:
    """Deterministic assignment of a stable bit position to each key. Sorted so
    the same key set always yields the same layout -- a compiled index, not a
    guess."""

    def __init__(self, keys: Iterable[str]) -> None:
        self.index: dict[str, int] = {k: i for i, k in enumerate(sorted(set(keys)))}

    @property
    def size(self) -> int:
        return len(self.index)

    def bit(self, key: str) -> Optional[int]:
        return self.index.get(key)

    def mask(self, keys: Iterable[str]) -> int:
        m = 0
        for k in keys:
            i = self.index.get(k)
            if i is not None:
                m |= 1 << i
        return m


def eligible(have: int, need: int) -> bool:
    """The fast stage: does the candidate hold every required bit? One AND and one
    compare reject enormous populations before any semantic check."""
    return (have & need) == need


def reject_count(need: int, candidates: dict[str, int]) -> tuple[list[str], int]:
    """Return the survivors and how many candidates were rejected by masks alone."""
    survivors = [c for c, have in candidates.items() if eligible(have, need)]
    return survivors, len(candidates) - len(survivors)


# --------------------------------------------------------------- authority mask

# Bit positions for the authority permission vocabulary, from AuthorityGraph.
AUTHORITY_REGISTRY = BitRegistry(authority.PERMISSIONS)


def authority_have_mask(db: sqlite3.Connection, actor: str, subject: str) -> int:
    """The permissions this actor currently holds over this subject, as a mask --
    compiled from real, non-revoked, in-window authority edges."""
    have = 0
    for perm in authority.PERMISSIONS:
        if authority.has_authority(db, actor, perm, subject):
            bit = AUTHORITY_REGISTRY.bit(perm)
            if bit is not None:
                have |= 1 << bit
    return have


def authority_need_mask(permissions: Iterable[str]) -> int:
    return AUTHORITY_REGISTRY.mask(permissions)


def authority_admits(db: sqlite3.Connection, actor: str, subject: str,
                     required: Iterable[str]) -> bool:
    """Fast authority rejection: does the actor hold every required permission?
    A no here stops the operation before the AuthorityGraph explains why."""
    return eligible(authority_have_mask(db, actor, subject), authority_need_mask(required))


# ------------------------------------------------------------- proof ternary

def proof_ternary(db: sqlite3.Connection, subject: str, subject_profile: str = "") -> dict[str, int]:
    """The +/?/- vector for a subject's layers, compiled from the ProofFrontier.

    A proven layer is +1; an open/blocked/untested layer is 0; a hypothesis with
    a KnownNonCause is -1 -- a blackhole the causal search must not re-enter. This
    is the anti-amnesia proof turned into forwarding state."""
    pf.ensure_schema(db)
    out: dict[str, int] = {}
    for r in db.execute(
        "SELECT layer,status FROM frontier_layers WHERE subject_resource=? AND subject_profile=?",
        (subject, subject_profile),
    ):
        out[r[0]] = PROVEN if r[1] == pf.PROVEN else UNRESOLVED
    for nc in pf.noncauses_for(db, subject):
        if nc.hypothesis:
            out[nc.hypothesis] = DISPROVEN
    return out


def is_blackholed(db: sqlite3.Connection, subject: str, hypothesis: str) -> bool:
    """Is this hypothesis a KnownNonCause for the subject -- a route the search is
    forbidden to take? The proof becomes part of the search topology."""
    h = hypothesis.strip().lower()
    for nc in pf.noncauses_for(db, subject):
        if nc.hypothesis and (nc.hypothesis.lower() in h or h in nc.hypothesis.lower()):
            return True
    return False


# --------------------------------------------------------------- capability mask

def capability_registry(db: sqlite3.Connection) -> BitRegistry:
    """A bit per capability family the estate actually has."""
    try:
        rows = [r[0] for r in db.execute("SELECT family FROM estate_catalog")]
    except sqlite3.Error:
        rows = []
    return BitRegistry(rows)


def capability_mask(registry: BitRegistry, families: Iterable[str]) -> int:
    return registry.mask(families)


# ------------------------------------------------------------- CID prefix routing

def cid_prefix(digest_hex: str, nbits: int) -> int:
    """The top ``nbits`` of a content digest, as an integer -- a placement
    partition, CIDR-style. Cryptographic hashes are random with respect to
    meaning, so prefixes are placement only, never semantic categories."""
    total = len(digest_hex) * 4
    full = int(digest_hex, 16)
    return full >> (total - nbits) if nbits < total else full


@dataclass(frozen=True)
class PrefixRoute:
    prefix: int
    nbits: int
    route: str


def route_cid(digest_hex: str, table: Iterable[PrefixRoute], default: str = "fabric-primary") -> str:
    """Longest-prefix match a content digest to a placement route. More specific
    (longer) prefixes win, exactly like CIDR forwarding."""
    for pr in sorted(table, key=lambda r: -r.nbits):
        if cid_prefix(digest_hex, pr.nbits) == pr.prefix:
            return pr.route
    return default


# ------------------------------------------------------------------ routing

@dataclass(frozen=True)
class RouteDemand:
    """What an operation needs, compiled to forwarding checks."""
    estates: tuple[str, ...] = ()               # which estates may participate
    capabilities: tuple[str, ...] = ()          # specific command families demanded
    actor: str = ""
    subject: str = ""
    subject_profile: str = ""
    required_permissions: tuple[str, ...] = ()  # actor must hold these over subject
    required_proven_layers: tuple[str, ...] = ()
    forbidden_hypotheses: tuple[str, ...] = ()  # rejected if any is blackholed


@dataclass(frozen=True)
class RouteResult:
    survivors: tuple[str, ...]
    considered: int
    rejected_by_capability: int
    rejected_by_authority: int
    rejected_by_proof: int

    def to_dict(self) -> dict:
        return {
            "survivors": list(self.survivors), "considered": self.considered,
            "rejected_by_capability": self.rejected_by_capability,
            "rejected_by_authority": self.rejected_by_authority,
            "rejected_by_proof": self.rejected_by_proof,
        }


def route(db: sqlite3.Connection, demand: RouteDemand) -> RouteResult:
    """The forwarding funnel over the real command estate: reject by capability,
    then authority, then proof -- each a cheap machine check -- so the exact
    semantic closure runs only on the survivors.

    Nothing here is authorization or proof: a survivor is merely *eligible*. The
    proof stage rejects a demand whose required layer is unproven or whose
    forbidden hypothesis is a KnownNonCause blackhole."""
    try:
        rows = [r for r in db.execute("SELECT family, estate FROM estate_catalog")]
    except sqlite3.Error:
        rows = []
    considered = len(rows)

    # stage 1: capability / estate eligibility
    if demand.capabilities:
        s1 = [f for (f, _e) in rows if f in demand.capabilities]
    elif demand.estates:
        s1 = [f for (f, e) in rows if e in demand.estates]
    else:
        s1 = [f for (f, _e) in rows]
    rej_cap = considered - len(s1)

    # stage 2: authority (all-or-nothing gate over the surviving set)
    if demand.actor and demand.required_permissions:
        admitted = authority_admits(db, demand.actor, demand.subject, demand.required_permissions)
        s2 = s1 if admitted else []
    else:
        s2 = s1
    rej_auth = len(s1) - len(s2)

    # stage 3: proof ternary -- reject unproven required layers or blackholed hypotheses
    proof_ok = True
    if demand.subject and (demand.required_proven_layers or demand.forbidden_hypotheses):
        tern = proof_ternary(db, demand.subject, demand.subject_profile)
        for layer in demand.required_proven_layers:
            if tern.get(layer, UNRESOLVED) != PROVEN:
                proof_ok = False
        for hyp in demand.forbidden_hypotheses:
            if is_blackholed(db, demand.subject, hyp):
                proof_ok = False
    s3 = s2 if proof_ok else []
    rej_proof = len(s2) - len(s3)

    return RouteResult(
        survivors=tuple(sorted(s3)), considered=considered,
        rejected_by_capability=rej_cap, rejected_by_authority=rej_auth,
        rejected_by_proof=rej_proof,
    )
