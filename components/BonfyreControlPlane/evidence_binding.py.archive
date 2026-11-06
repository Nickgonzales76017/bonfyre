"""EvidenceBindingGraph: the binding relation is itself an object.

The atlas carried this at ``architectural``, and it names a real, long-standing
gap: AtomicForm evidence slots were filled by *position* -- the first N fabric
artifacts, zipped into the first N empty slots. That is the exact
forbidden_inference the architecture warns about: an artifact existing, or
landing in a slot, is not the same as it satisfying the requirement.

This makes the binding explicit and testable. A requirement is matched to a
candidate only by a semantic test over the candidate's own metadata, and the
match produces a witness that says *why* it bound. A requirement with no matcher
binds nothing -- it never falls back to position. A requirement whose matchers
find no candidate stays unbound, and the caller learns which, rather than getting
a wrong artifact silently.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Sequence


@dataclass(frozen=True)
class EvidenceRequirement:
    """What a slot needs. Empty matchers mean "unspecified", not "anything" --
    a requirement with no matcher at all binds nothing."""
    req_id: str
    description: str = ""
    kind: str = ""                                   # candidate.kind must equal this
    name_contains: Sequence[str] = field(default_factory=tuple)  # all tokens must appear
    tags: Sequence[str] = field(default_factory=tuple)           # all tags must be present

    @property
    def has_matcher(self) -> bool:
        return bool(self.kind or self.name_contains or self.tags)


@dataclass(frozen=True)
class EvidenceCandidate:
    digest: str
    kind: str = ""
    name: str = ""
    tags: Sequence[str] = field(default_factory=tuple)


@dataclass(frozen=True)
class EvidenceBinding:
    req_id: str
    digest: str
    witness: str        # why this candidate satisfied the requirement


def matches(req: EvidenceRequirement, cand: EvidenceCandidate) -> Optional[str]:
    """Return a witness string if the candidate satisfies every specified matcher,
    else None. A requirement with no matcher matches nothing -- position is never
    a reason to bind."""
    if not req.has_matcher:
        return None
    reasons: list[str] = []
    if req.kind:
        if cand.kind != req.kind:
            return None
        reasons.append(f"kind={cand.kind}")
    lname = cand.name.lower()
    for tok in req.name_contains:
        if tok.lower() not in lname:
            return None
        reasons.append(f"name~{tok}")
    cand_tags = set(cand.tags)
    for tag in req.tags:
        if tag not in cand_tags:
            return None
        reasons.append(f"tag={tag}")
    return "; ".join(reasons)


def bind(req: EvidenceRequirement, candidates: Sequence[EvidenceCandidate]) -> Optional[EvidenceBinding]:
    """Bind the most specific matching candidate, deterministically.

    Most specific = longest witness (most matchers satisfied); ties broken by
    digest so the choice is stable. None when nothing matches."""
    scored = [(c, w) for c in candidates if (w := matches(req, c)) is not None]
    if not scored:
        return None
    scored.sort(key=lambda cw: (-len(cw[1]), cw[0].digest))
    cand, witness = scored[0]
    return EvidenceBinding(req.req_id, cand.digest, witness)


@dataclass(frozen=True)
class BindingResult:
    bound: dict            # req_id -> EvidenceBinding
    unbound: tuple[str, ...]

    @property
    def all_bound(self) -> bool:
        return not self.unbound


def bind_all(
    requirements: Sequence[EvidenceRequirement],
    candidates: Sequence[EvidenceCandidate],
    *,
    unique: bool = True,
) -> BindingResult:
    """Bind every requirement to its matching candidate.

    With ``unique`` (the default), a candidate is consumed by the first
    requirement it binds to, so two slots cannot both claim the same artifact --
    the binding is a matching, not a broadcast. Requirements are processed
    most-constrained first so a specific slot is not starved by a loose one."""
    order = sorted(
        requirements,
        key=lambda r: -(len(r.name_contains) + len(r.tags) + (1 if r.kind else 0)),
    )
    used: set[str] = set()
    bound: dict = {}
    unbound: list[str] = []
    for req in order:
        pool = [c for c in candidates if c.digest not in used] if unique else list(candidates)
        b = bind(req, pool)
        if b is None:
            unbound.append(req.req_id)
        else:
            bound[req.req_id] = b
            if unique:
                used.add(b.digest)
    return BindingResult(bound=bound, unbound=tuple(sorted(unbound)))
