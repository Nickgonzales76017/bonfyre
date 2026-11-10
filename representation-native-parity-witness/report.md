# Bonfÿre Native Parity Witness v1.3

Generated: `2026-09-29T05:51:23.496297+00:00`

## RepresentationLaw round-trip

- Exact row-for-row reconstruction: **True**
- Original durable target rows: **23,836**
- Per-kind laws: **118**
- Exception rows: **60**
- Exception groups: **2**
- Original digest: `6a5d811b45e94639ffd9123e3f725fcaf7388819fc2eac8fd34281c47b631f7f`
- Reconstructed digest: `6a5d811b45e94639ffd9123e3f725fcaf7388819fc2eac8fd34281c47b631f7f`

A matching digest proves the compact law+exception encoding can reproduce the current table exactly.

## Evidence quality

- `LOCAL_FILE_WEAK`: **41** representation kinds
- `INCIDENTAL_OR_DEPENDENCY_TEXT`: **17** representation kinds
- `NATIVE_SOURCE`: **15** representation kinds
- `UNRESOLVED_PATH_OR_TOKEN`: **15** representation kinds
- `DOCUMENT_OR_HISTORICAL_EVIDENCE`: **10** representation kinds
- `RUNTIME_EXECUTABLE`: **10** representation kinds
- `SUBSTRATE_CHECKOUT`: **8** representation kinds
- `DECLARATIVE_ARCHITECTURE`: **2** representation kinds

`evidence_present` is treated as evidence provenance, not automatically as runtime readiness.

## CAS parity

- Bodies: **201**
- Heads: **202**
- Shared physical bodies referenced by multiple heads: **1**
- Digest mismatches: **0**
- Size mismatches: **0**
- Heads without body record: **0**

## FactDelta structural parity

- `id`: **STRONG** (3734 source hits)
- `semantic_kind`: **STRONG** (396 source hits)
- `old_representation_id`: **STRONG** (31 source hits)
- `new_representation_id`: **PARTIAL** (145 source hits)
- `old_digest`: **PARTIAL** (2 source hits)
- `new_digest`: **PARTIAL** (177 source hits)
- `old_coordinate`: **UNMAPPED** (0 source hits)
- `new_coordinate`: **UNMAPPED** (0 source hits)
- `hlc`: **STRONG** (93 source hits)
- `observed_at`: **STRONG** (56 source hits)
- `reason`: **PARTIAL** (89 source hits)

## Physical algebra audit

- Lance: **37.6MiB**, ~172 dataset roots, **0 index artifacts**.
- Graph: **11.4MiB**, 116 files.
- Substrait: **194** plan files; **8** semantic kinds lack a physical plan.
- Iceberg old realization states: `[["superseded",0,202]]`
- Iceberg CAS realization states: `[["physical",1,202]]`
- Iceberg overlay states: `[["iceberg","superseded_mutable_path",202],["iceberg-cas","physical_zero_copy",202],["xtable-delta-hudi","config_ready",202]]`

## Decision boundary

This witness still does **not** mutate the estate.
A convergence mutation is safe only for a mechanism whose compact form round-trips exactly
and whose fields have an explicit native owner or are deliberately retained as a distinct semantic plane.
