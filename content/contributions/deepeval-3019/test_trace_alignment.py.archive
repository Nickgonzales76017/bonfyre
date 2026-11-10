"""
Fixture corpus for trace alignment (confident-ai/deepeval#3019).

These are the cases that decide whether the definition of "divergence" is
correct. Every one of them is a case where the naive answer is wrong:

  identical                   aligned, no divergence
  encoding-only difference    aligned -- dict key order is not behaviour
  same tool, different args   divergence, but arg_change NOT tool_change
  reordered independent steps NOT a divergence
  retry that rejoins          divergence with a resync point
  genuine path change         divergence to the end
  truncated candidate         divergence classified absent
  extra trailing step         divergence classified extra

Run: python3 -m pytest test_trace_alignment.py -q
 or: python3 test_trace_alignment.py     (no pytest required)
"""

from trace_alignment import (
    align, project, AlignmentResult, DIVERGENCE_KINDS, TRACE_PROJECTION_VERSION,
)


def step(name, **args):
    return {"kind": "tool", "name": name, "args": args, "id": f"{name}-{sorted(args.items())}"}


SEARCH = step("search", q="revenue 2024")
OPEN_DOC = step("open_doc", id=7)
SUMMARISE = step("summarise", style="brief")
EMAIL = step("send_email", to="cfo@example.com")


# ----------------------------------------------------------------------

def test_identical_traces_are_aligned():
    t = [SEARCH, OPEN_DOC, SUMMARISE]
    r = align(t, list(t))
    assert r.aligned
    assert r.first_divergence is None
    assert r.divergence_ratio == 0.0


def test_dict_key_order_is_not_a_divergence():
    """An encoding difference must not read as a behaviour difference.

    This is the same class of bug as CRLF-vs-LF changing a content
    address: the bytes differ, the meaning does not.
    """
    a = [{"kind": "tool", "name": "search", "args": {"q": "x", "limit": 10}}]
    b = [{"kind": "tool", "name": "search", "args": {"limit": 10, "q": "x"}}]
    assert align(a, b).aligned


def test_float_int_equivalence():
    a = [{"kind": "tool", "name": "page", "args": {"n": 1}}]
    b = [{"kind": "tool", "name": "page", "args": {"n": 1.0}}]
    assert align(a, b).aligned


def test_same_tool_different_args_is_arg_change_not_tool_change():
    base = [SEARCH, OPEN_DOC]
    cand = [step("search", q="revenue 2025"), OPEN_DOC]
    r = align(base, cand)
    assert not r.aligned
    assert r.first_divergence == 0
    assert r.divergence_kind == "arg_change", r.divergence_kind


def test_reordered_independent_steps_are_not_a_divergence():
    """Two independent lookups emitted in the other order is not a regression."""
    base = [SEARCH, OPEN_DOC, SUMMARISE]
    cand = [OPEN_DOC, SEARCH, SUMMARISE]
    r = align(base, cand)
    assert r.aligned, r
    assert r.divergence_kind == "order_change"
    assert r.reordered


def test_retry_that_rejoins_resyncs_rather_than_diverging_to_the_end():
    """One extra retry then back on path. The tail must not count as divergent."""
    base = [SEARCH, OPEN_DOC, SUMMARISE, EMAIL]
    cand = [SEARCH, step("search", q="revenue 2024", retry=1), OPEN_DOC, SUMMARISE, EMAIL]
    r = align(base, cand)
    assert not r.aligned
    assert r.first_divergence == 1
    assert r.resync_at is not None, "a retry that rejoins must produce a resync point"
    assert r.divergence_ratio < 1.0


def test_genuine_path_change_is_tool_change():
    base = [SEARCH, OPEN_DOC, SUMMARISE]
    cand = [SEARCH, step("ask_human", prompt="which doc?"), step("wait")]
    r = align(base, cand)
    assert not r.aligned
    assert r.first_divergence == 1
    assert r.divergence_kind == "tool_change", r.divergence_kind


def test_truncated_candidate_is_absent():
    base = [SEARCH, OPEN_DOC, SUMMARISE]
    cand = [SEARCH, OPEN_DOC]
    r = align(base, cand)
    assert not r.aligned
    assert r.divergence_kind == "absent"
    assert r.unmatched_baseline


def test_extra_trailing_step_is_extra():
    base = [SEARCH, OPEN_DOC]
    cand = [SEARCH, OPEN_DOC, EMAIL]
    r = align(base, cand)
    assert not r.aligned
    assert r.divergence_kind == "extra"
    assert r.unmatched_candidate


def test_both_empty_is_aligned():
    assert align([], []).aligned


def test_result_is_serialisable_and_versioned():
    r = align([SEARCH], [SEARCH])
    d = r.as_dict()
    assert d["projection_version"] == TRACE_PROJECTION_VERSION
    import json
    json.dumps(d)          # must survive a CI artifact round-trip


def test_divergence_kind_is_always_known():
    cases = [
        ([SEARCH], [step("search", q="other")]),
        ([SEARCH, OPEN_DOC], [SEARCH]),
        ([SEARCH], [SEARCH, EMAIL]),
        ([SEARCH, OPEN_DOC], [SEARCH, step("ask_human")]),
    ]
    for base, cand in cases:
        r = align(base, cand)
        assert r.divergence_kind in DIVERGENCE_KINDS, r.divergence_kind


def test_alignment_is_deterministic():
    base = [SEARCH, OPEN_DOC, SUMMARISE, EMAIL]
    cand = [SEARCH, step("search", q="x"), SUMMARISE]
    first = align(base, cand).as_dict()
    for _ in range(20):
        assert align(base, cand).as_dict() == first


def test_projection_tolerates_objects_and_dicts():
    class Span:
        def __init__(self, name, args):
            self.name, self.args = name, args
    a = project([Span("search", {"q": "x"})])
    b = project([{"name": "search", "args": {"q": "x"}}])
    assert a[0].strong == b[0].strong


if __name__ == "__main__":
    import sys, traceback
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for fn in fns:
        try:
            fn()
            print(f"  PASS  {fn.__name__}")
        except Exception:
            failed += 1
            print(f"  FAIL  {fn.__name__}")
            traceback.print_exc()
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    sys.exit(1 if failed else 0)
