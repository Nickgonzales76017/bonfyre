# BonfyreControlPlane — Python Reference v1 (FROZEN)

> **Status: frozen.** This is a semantic reference implementation and
> conformance oracle. It is **not** the runtime and must not become one.
> No substantial new behaviour goes here; new semantics extend the native
> kernel per the constitutional rule in `CONTRIBUTING.md`.
>
> It is deliberately **not** wired into `bonfyre_run6_supervisor.py`. Bolting it
> in would fix the Run 6 bugs while making the architecture worse — six Python
> services owning kernel semantics, and a supervisor that should disappear
> instead growing dependents.

The Run 6 control semantics, captured precisely enough to port. Every module
exists because a specific failure made it necessary, and every test names the
incident it makes impossible. Nothing here sends anything or contacts anyone.

## Modules and the failures they close

| Module | Run 6 failure |
|---|---|
| `provider_state.py` | Provider health was a mutable row, so a transient failure overwrote Codex's hard-capacity window and the guardian relaunched into an exhausted provider. State is a fold over append-only observations. |
| `resource_admission.py` | ENOSPC killed transcripts, builds, SQLite and the supervisor at once. Work is admitted against a protected floor, with outstanding grants subtracted so concurrent planes cannot spend the same bytes. |
| `work_graph.py` | The queue was write-only: 242 rows, all `open`. Work has a lifecycle, a lease that survives a killed plane, and a `target_plane` checked at insert. |
| `external_events.py` | `external_events`, `commitment_ledger` and `run6_action_log` were empty at freeze. Observations commit before anything reasons about them; economic categories cannot be summed into one fake number. |
| `capability_catalog.py` | 91 public identities against 0 compiled tools, with planes probing `PATH`. Identity and callability are different facts on the maturity ladder. |
| `actors.py` | Typed edges with mandatory provenance. Collapsing edge meaning is a named future error. |
| `scheduling.py` | Proton consumed frontier attention while never changing. Attention cools to watcher-only and reheats on a named signal. |
| `pack_loader.py` | Generic. Knows about actors, edges, work and watches; knows nothing about any campaign. |

## Running

```bash
python3 -m unittest discover -s tests     # 63 tests
python3 migrate_run6.py --dry-run         # read capital.db, write nothing
python3 migrate_run6.py                   # build control_plane.db
```

`capital.db` is opened read-only and never mutated. The freeze is evidence.

Conformance vectors live at `tests/conformance/control/` in the repo root:

```bash
python3 tests/conformance/control/generate_vectors.py           # write
python3 tests/conformance/control/generate_vectors.py --check   # replay
```

16 vectors, 0 mismatches against this reference. These are what the native
kernel must reproduce.

## Migration result on the real freeze

```
planes_registered            5
work_imported              241
work_rejected_unroutable     1   {'coordinator': 1}
qualified_ask         $37,500
realized_cash              $0
```

The single rejection is the `coordinator` item Run 6's governor could only
report as `invalid_targets=1` after the fact.

## Absorption target

Schemas at `schemas/control/*.yaff` are the specification. The five semantic
modules collapse into three native primitives:

```
Occurrence      <- external_events.py, provenance mutations, provider and
                   resource observations
WorkGraph       <- work_graph.py + scheduling.py (cooling is a property of a
                   work edge, not a second scheduler)
CapabilityClosure <- capability_catalog.py, generated rather than maintained
```

`actors.py` folds into ResourceGraph components rather than staying a root
class. Order of work:

1. Extract YaFF schemas — **done**, `schemas/control/`
2. Generate golden replay vectors — **done**, 16 vectors
3. Native `BonfyreControl` kernel
4. Port the WorkGraph state machine
5. Port Occurrence append/project
6. Port CapabilityClosure
7. Fold actors into ResourceGraph components
8. Fold cooling into WorkGraph policy
9. Run all vectors against the native kernel
10. Point the Run 6 replay at the native kernel
11. Only then replace supervisor call sites
12. Delete runtime dependence on this reference; keep it as the oracle

## Standing caveats

- **The Tarbell actor records are `asserted`, not `verified`.** Names, roles and
  funders come from a research brief dated 2026-08-14, unchecked against
  tarbellcenter.org by this process. Call `actors.unverified()` before using any
  of them. No message has been sent.
- **The W&B decline is not recorded.** It is absent from the evidence freeze
  *and* from the raw Codex transcripts — the disk exhaustion that ended the run
  also broke the writes to `~/.codex/sessions/*.jsonl`. `capital.db` still says
  `status=delivered, waiting_on=Weights & Biases` and stays that way until
  someone confirms it from Gmail.
