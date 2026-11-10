# Feldera: deterministic adapter status

Feldera's job in the fabric is continuous, live materialized readiness over
the same state the fabric already writes (mission/node status, effect and
compensation backlog, lease pressure, catalog binding health). Real Feldera
cannot run locally right now for a single, narrow reason: disk capacity.
Everything upstream and downstream of that boundary is real and proven.

```
feldera.adapter_implemented    true
feldera.event_contract         true
feldera.local_image_pull       capacity_pending
feldera.production_workload    pending_runtime
feldera.blocker_kind           local_storage_capacity
```

## What is real today

- **Universal event producers** — already real, predates this adapter work.
  Every node dispatch, effect commit, and compensation writes into the
  shared `events` table via `bf_workgraph_write_evidence[_ex]` and
  `emit_event`, validated against `completion/event.schema.json`
  (`fabric.events.universal_schema`).

- **Readiness schema / materialized-view definitions** — `engine/core/src/workgraph_schema.c`,
  schema version 8 (`apply_version_eight`/`verify_version_eight`). Five live
  SQL views computed directly from `workgraph_nodes`, `effects`,
  `workgraph_compensations`, and `catalog_bindings`/`fabric_meta`:
  `bf_readiness_mission`, `bf_readiness_lease_pressure`,
  `bf_readiness_effect_backlog`, `bf_readiness_compensation_backlog`,
  `bf_readiness_capability`. Each view creation is guarded by `table_exists`,
  so a minimal workgraph-only database migrates cleanly without the full
  fabric schema present.

- **Query surface** — `readiness <view> [mission-id]` CLI verb
  (`engine/core/src/fabric_exec.c`, `readiness_dispatch`), JSON output,
  whitelisted view names, real error handling for unknown views/missing args.

- **Restart behavior** — proven directly: the views are plain SQL, recomputed
  on every query, not cached — so a fresh process opening the same
  `fabric.db` sees identical, correct state with zero special-cased
  "recovery" logic. Verified by claiming a node in one process invocation
  and reading `readiness mission` back from an entirely separate process.

- **Real state-change reactivity** — proven, not assumed: claiming a node
  flips `running_nodes` live; a lease's `expired` flag flips `0`→`1` between
  two queries with zero writes in between (pure time-based recomputation);
  requesting an effect increments `bf_readiness_effect_backlog` immediately.

- **Acceptance harness** — `tests/requirements/readiness_views.sh`, gated as
  `fabric.readiness.deterministic_adapter` in `completion/requirements.yaff`,
  depends on `fabric.events.universal_schema`. Runs as part of
  `tests/unified_fabric_acceptance.sh`.

- **Catalog identity / RuntimeImage contract** — `estate/providers.yaff`
  declares Feldera (and Restate) as real providers, compiled every
  `fabric compile` via `compile_providers` (`engine/core/src/fabric_exec.c`).
  Each gets a real `catalog` row (`kind='provider'`, `source_ref`=the actual
  runtime image reference, e.g. `ghcr.io/feldera/pipeline-manager:latest`)
  and a matching `roots` entry under `authority_class='runtime-image'` whose
  `locator` is the same image reference — so the fabric has a durable,
  queryable identity for Feldera whether or not the Feldera process is
  currently running locally. Feldera's row carries
  `maturity='adapter_implemented'`, `health_state='capacity_pending'` —
  the exact status vocabulary this document opens with, now backed by a
  real row instead of prose. Each provider is also addressable through the
  fabric's real `bonfyre://` namespace at `bonfyre://provider/feldera` (and
  `.../restate`) via `fabric namespace show`, extending the unified
  namespace to providers, not just files/missions/effects/receipts. Gated
  as `fabric.providers.catalog_identity`
  (`tests/requirements/provider_catalog.sh`).

## What real Feldera changes when it can run

Only the computation engine behind the five view names above: SQL pipelines
submitted to a running Feldera instance, instead of SQLite views recomputed
per query. The **output contract is these view/column names** — that is
what downstream readers (and this adapter's own acceptance test) are
written against, so nothing downstream needs to change when Feldera takes
over.

- **Source/sink bindings** — now real, not just designed. `feldera/readiness_pipeline.sql`
  is the actual Feldera SQL program (standard ANSI/Calcite SQL, source
  table declarations + the same five views, relationally identical to the
  SQLite adapter). `scripts/bonfyre-feldera-export` is the real source
  binding: it exports the five source tables as NDJSON in Feldera's
  documented insert-format connector shape (`{"insert": {...}}` per line),
  runnable today against any real `fabric.db`. `tests/requirements/feldera_source_sink_parity.sh`
  proves semantic parity end to end: real induced state (a claimed node,
  a requested effect) → real export via the real binding script → an
  independent Python recomputation of the exact same aggregates
  `readiness_pipeline.sql` defines → exact match against the live
  deterministic adapter. Gated as `fabric.feldera.source_sink_parity`.

The only remaining boundary is swapping the independent-recomputation
half of that parity proof for real Feldera incremental execution once
local disk capacity allows it. At that point the closure sequence is:
start real Feldera → submit `readiness_pipeline.sql` → feed it the same
NDJSON `bonfyre-feldera-export` already produces → query live
materialized readiness via Feldera's own `/query` endpoint → induce state
changes → prove incremental updates → restart → prove convergence/resumption.
