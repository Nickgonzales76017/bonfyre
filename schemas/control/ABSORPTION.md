# Control-plane absorption status

Tracks the 12-step migration from the frozen Python reference
(`10-Code/BonfyreControlPlane`) into the kernel. The reference remains the
conformance oracle; the kernel is the authority.

## Deviation from the original plan

The plan named Zig as the control kernel. The kernel layer is already 7,722
lines of C11 under `engine/core`, compiled by `build.zig` with
`-Wall -Wextra -Werror`, and `CONTRIBUTING.md` mandates C11 there. Introducing
Zig alongside would make one layer bilingual — the exact sprawl this absorption
exists to reverse. The control primitives are therefore C11 in `engine/core`,
built into `libbonfyre-fabric` with everything else.

## Step status

| # | Step | Status |
|---|---|---|
| 1 | Extract YaFF schemas | **done** — `schemas/control/*.yaff` |
| 2 | Generate golden replay vectors | **done** — 16 JSON, 15 flat |
| 3 | Native kernel | **done** for provider, admission, attention, capability |
| 4 | Port WorkGraph state machine | **not needed** — see mapping below |
| 5 | Port Occurrence append/project | **not done** |
| 6 | Port CapabilityClosure | **partial** — ladder native, catalog storage not |
| 7 | Fold actors into ResourceGraph components | **not done** |
| 8 | Fold cooling into WorkGraph policy | **partial** — `bf_attention_*` is native and pure; not yet a column on a work node |
| 9 | Run vectors against native kernel | **done** — `zig build test-control`, 15/15 |
| 10 | Point Run 6 replay at the native kernel | **not done** |
| 11 | Replace supervisor call sites | **not done** |
| 12 | Delete runtime dependence on the reference | **not done** — blocked on 5 and 7 |

## Step 4: why no port

`engine/core/include/bf_workgraph.h` already owns a work lifecycle, and it is a
strict superset of what the Python reference expressed:

| Python reference | Native equivalent |
|---|---|
| `open` | `ready` |
| `leased` (holder + expiry) | `claimed` — plus a claim token, so a stale holder is detected rather than merely timed out |
| `effected` | `running` / `prepare_effect` → `commit_effect` |
| `satisfied` | `completed` |
| `blocked_external` | `blocked` |
| `superseded`, `invalidated` | `cancelled` (node or mission) |
| `failed` | `bf_workgraph_fail` with a failure class |
| lease reaping | `bf_workgraph_reap_expired` |
| transition journal | `bf_workgraph_list_transitions` |

The native version additionally has fanout/fanin, compensation with evidence,
retry policy with backoff and jitter, and `bf_workgraph_reconcile_effects` for
effects interrupted mid-commit by a crash. None of that existed in the Python.
Porting the simpler model over it would be a regression.

**One genuine gap remains.** The Python refused work aimed at an unregistered
plane at insert time — the defect behind Run 6's `coordinator` item, which its
governor could only report as `invalid_targets=1` after the fact. The native
`family` field on `BfWorkgraphNodeSpec` is a free string with no registry, so
the same item would be accepted today. Closing that means a plane/family
registry and a validation check in `bf_workgraph_add_node`.

## What the native kernel now owns

```
bf_provider_fold              provider health as a fold; a transient
                              observation cannot shorten a hard-capacity window
bf_provider_parse_reset       recovers "Aug 19th, 2026 10:53 PM" and ISO forms
bf_provider_is_hard_capacity  matches "usage limit", covering the real Codex
                              string Run 6's matcher missed
bf_admission_decide           protected floor, per-plane quota, outstanding
                              grants subtracted; admit/defer/reject distinguished
bf_attention_check            cools after repeated unchanged checks, refuses to
                              cool without a reheat condition
bf_capability_*               the maturity ladder, callable floor, proven floor,
                              and demotion when a name stops resolving
```

Verified by `zig build test-control` — 15 checks, 0 failures, against vectors
generated from the frozen reference.

## Not covered by any native code

`external_events.py` (the occurrence spine) and `actors.py` (typed relationship
edges with mandatory provenance) have no native equivalent. Until they do, the
Python reference is still the only implementation of those semantics, and step
12 cannot proceed. Both are storage-shaped rather than pure-fold-shaped, so they
need the SQLite layer `engine/core` already links, not just a header.
