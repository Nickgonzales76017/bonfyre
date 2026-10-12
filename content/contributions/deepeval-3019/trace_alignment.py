"""
Trace alignment for baseline-vs-candidate agent execution comparison.

This is the alignment layer for confident-ai/deepeval#3019. It has no
deepeval imports on purpose: the metric wrapper owns "does this fit the
repo's conventions", this module owns "is this the correct definition of
divergence". The seam between them is `AlignmentResult`.

Deterministic, single-turn, no LLM judge. Same inputs always produce the
same result, which is the whole point -- a regression detector that is
itself nondeterministic is worthless.

The hard part is not finding *a* difference. It is refusing to call
things differences when they are not:

  - the same tool called with different arguments is an argument change,
    not a path change; teams need to tell those apart
  - two independent steps that happen to be emitted in a different order
    are NOT a divergence
  - one extra retry that then rejoins the baseline path is a divergence
    that RESYNCS, not a divergence to the end of the trace

Projection is versioned. If the projection changes, previously recorded
alignments are not comparable, and a stored `projection_version` is what
lets a CI system notice that rather than silently comparing apples to
oranges.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, asdict
from typing import Any, Iterable, Sequence

TRACE_PROJECTION_VERSION = "1.0.0"

# How far ahead to look when deciding whether a mismatch is a reorder or a
# real divergence, and how many consecutive matches count as a resync.
DEFAULT_LOOKAHEAD = 4
DEFAULT_RESYNC_RUN = 2


# ----------------------------------------------------------------------
# Projection
# ----------------------------------------------------------------------

@dataclass(frozen=True)
class Event:
    """One projected step of a trace.

    `strong` distinguishes 'same tool, same arguments'.
    `weak` distinguishes 'same tool' regardless of arguments.
    Keeping both is what makes `arg_change` separable from `tool_change`.
    """

    index: int
    event_id: str
    kind: str
    name: str
    args: Any
    strong: str
    weak: str


def _canonical(value: Any) -> Any:
    """Order-insensitive canonical form for argument comparison.

    Dict key order is an encoding detail, not a behavioural difference, so
    it must not produce a divergence. Lists keep their order because for
    arguments the order usually *is* meaningful (a list of files to
    process in sequence is not the same as the reverse).
    """
    if isinstance(value, dict):
        return {k: _canonical(value[k]) for k in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical(v) for v in value]
    if isinstance(value, float) and value.is_integer():
        # 1.0 and 1 are the same argument; JSON round-trips make this common
        return int(value)
    return value


def _digest(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def _get(step: Any, *names: str, default: Any = None) -> Any:
    """Read a field from a dict or an object, whichever the caller passed."""
    for n in names:
        if isinstance(step, dict):
            if n in step and step[n] is not None:
                return step[n]
        else:
            v = getattr(step, n, None)
            if v is not None:
                return v
    return default


def project(trace: Iterable[Any]) -> list[Event]:
    """Project a raw trace into comparable events.

    Deliberately tolerant about input shape -- traces arrive as dicts from
    JSON, as deepeval spans, or as adapter objects, and the alignment
    should not care which.
    """
    events: list[Event] = []
    for i, step in enumerate(trace):
        kind = str(_get(step, "kind", "type", "span_type", default="tool"))
        name = str(_get(step, "name", "tool", "tool_name", "function", default=""))
        args = _get(step, "args", "arguments", "input", "parameters", default={})
        args = _canonical(args)
        event_id = str(_get(step, "id", "event_id", "span_id", default=f"idx-{i}"))
        weak = f"{kind}:{name}"
        strong = f"{weak}:{_digest(args)}"
        events.append(
            Event(index=i, event_id=event_id, kind=kind, name=name,
                  args=args, strong=strong, weak=weak)
        )
    return events


# ----------------------------------------------------------------------
# Result
# ----------------------------------------------------------------------

@dataclass
class AlignmentResult:
    """The seam. The metric wrapper turns this into a score and a sentence."""

    aligned: bool
    matched_prefix_len: int
    first_divergence: int | None            # index into both traces
    divergence_kind: str | None             # see DIVERGENCE_KINDS
    resync_at: int | None                   # index where the traces realign
    unmatched_baseline: list[str] = field(default_factory=list)
    unmatched_candidate: list[str] = field(default_factory=list)
    reordered: list[tuple[str, str]] = field(default_factory=list)
    baseline_len: int = 0
    candidate_len: int = 0
    projection_version: str = TRACE_PROJECTION_VERSION

    def as_dict(self) -> dict:
        return asdict(self)

    @property
    def divergence_ratio(self) -> float:
        """Fraction of the longer trace that failed to align.

        Left as a property rather than baked into a score, because the
        threshold policy belongs to the metric layer, not here.
        """
        longest = max(self.baseline_len, self.candidate_len)
        if not longest:
            return 0.0
        if self.aligned:
            return 0.0
        end = self.resync_at if self.resync_at is not None else longest
        return max(0.0, (end - self.matched_prefix_len)) / longest


DIVERGENCE_KINDS = (
    "arg_change",     # same tool, different arguments
    "tool_change",    # genuinely different step
    "order_change",   # same steps, different order -- not a real divergence
    "absent",         # baseline had a step the candidate never took
    "extra",          # candidate took a step the baseline never did
)


# ----------------------------------------------------------------------
# Alignment
# ----------------------------------------------------------------------

def _common_prefix(a: Sequence[Event], b: Sequence[Event]) -> int:
    n = 0
    while n < len(a) and n < len(b) and a[n].strong == b[n].strong:
        n += 1
    return n


def _find(seq: Sequence[Event], key: str, start: int, stop: int) -> int | None:
    for i in range(start, min(stop, len(seq))):
        if seq[i].strong == key:
            return i
    return None


def _is_reorder(base: Sequence[Event], cand: Sequence[Event], at: int,
                lookahead: int) -> tuple[bool, list[tuple[str, str]]]:
    """True when both sides contain the same multiset of steps in a window.

    Same steps, different order. Without data-dependency information we
    cannot prove the steps are independent, so this stays conservative:
    the window must match as a complete multiset, and only within a small
    lookahead. A partial overlap is treated as a real divergence.
    """
    for w in range(2, lookahead + 1):
        bw, cw = base[at:at + w], cand[at:at + w]
        if len(bw) < w or len(cw) < w:
            break
        if sorted(e.strong for e in bw) == sorted(e.strong for e in cw):
            if [e.strong for e in bw] != [e.strong for e in cw]:
                pairs = [(b.event_id, c.event_id)
                         for b, c in zip(bw, cw) if b.strong != c.strong]
                return True, pairs
    return False, []


def _classify(base: Sequence[Event], cand: Sequence[Event], at: int) -> str:
    b = base[at] if at < len(base) else None
    c = cand[at] if at < len(cand) else None
    if b is None:
        return "extra"
    if c is None:
        return "absent"
    if b.weak == c.weak:
        return "arg_change"
    # a step skipped on one side shows up as the other side's next step
    if at + 1 < len(cand) and base[at].strong == cand[at + 1].strong:
        return "extra"
    if at + 1 < len(base) and cand[at].strong == base[at + 1].strong:
        return "absent"
    return "tool_change"


def align(baseline: Iterable[Any], candidate: Iterable[Any], *,
          lookahead: int = DEFAULT_LOOKAHEAD,
          resync_run: int = DEFAULT_RESYNC_RUN,
          already_projected: bool = False) -> AlignmentResult:
    """Locate the first sustained divergence between two traces."""
    base = list(baseline) if already_projected else project(baseline)
    cand = list(candidate) if already_projected else project(candidate)

    prefix = _common_prefix(base, cand)

    # identical, or one is a clean prefix of the other with nothing after
    if prefix == len(base) == len(cand):
        return AlignmentResult(
            aligned=True, matched_prefix_len=prefix, first_divergence=None,
            divergence_kind=None, resync_at=None,
            baseline_len=len(base), candidate_len=len(cand),
        )

    # both truncated at the same point -- aligned as far as either goes
    if prefix == len(base) or prefix == len(cand):
        if abs(len(base) - len(cand)) == 0:
            aligned = True
        else:
            aligned = False
        kind = None if aligned else ("absent" if len(base) > len(cand) else "extra")
        return AlignmentResult(
            aligned=aligned, matched_prefix_len=prefix,
            first_divergence=None if aligned else prefix,
            divergence_kind=kind, resync_at=None,
            unmatched_baseline=[e.event_id for e in base[prefix:]],
            unmatched_candidate=[e.event_id for e in cand[prefix:]],
            baseline_len=len(base), candidate_len=len(cand),
        )

    # a pure reordering of the same steps is not a divergence
    reordered, pairs = _is_reorder(base, cand, prefix, lookahead)
    if reordered:
        tail = align(base[prefix + len(pairs) + 1:], cand[prefix + len(pairs) + 1:],
                     lookahead=lookahead, resync_run=resync_run,
                     already_projected=True)
        if tail.aligned:
            return AlignmentResult(
                aligned=True, matched_prefix_len=len(base),
                first_divergence=None, divergence_kind="order_change",
                resync_at=None, reordered=pairs,
                baseline_len=len(base), candidate_len=len(cand),
            )

    kind = _classify(base, cand, prefix)

    # does it come back together? scan for a run of matches after a skip
    resync_at = None
    for skew in range(1, lookahead + 1):
        for bi, ci in ((prefix + skew, prefix), (prefix, prefix + skew)):
            run = 0
            while (bi + run < len(base) and ci + run < len(cand)
                   and base[bi + run].strong == cand[ci + run].strong):
                run += 1
                if run >= resync_run:
                    break
            if run >= resync_run:
                resync_at = max(bi, ci)
                break
        if resync_at is not None:
            break

    matched = {e.strong for e in base} & {e.strong for e in cand}
    return AlignmentResult(
        aligned=False,
        matched_prefix_len=prefix,
        first_divergence=prefix,
        divergence_kind=kind,
        resync_at=resync_at,
        unmatched_baseline=[e.event_id for e in base[prefix:] if e.strong not in matched],
        unmatched_candidate=[e.event_id for e in cand[prefix:] if e.strong not in matched],
        reordered=pairs,
        baseline_len=len(base), candidate_len=len(cand),
    )
