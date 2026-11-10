# Bonfÿre Native Absorption Investigation v1.2

Generated: `2026-09-29T05:39:46.548062+00:00`

## Corrected architecture census

- Atlas architectures discovered recursively: **380**
- SQLite inspection mode: **stable_temp_snapshot**

## P0 conclusion

This pass does not treat semantic overlap as redundancy. It identifies where recent sidecar mechanics
should be absorbed by an existing native owner while preserving the new capability or law.

## representation_target is the largest metadata-compression opportunity

- Current rows: **23,836**
- Semantic kinds: **202**
- Representation kinds: **118**
- Exact Cartesian product: **True**
- Law rows after modal normalization: **118**
- Required exceptions: **60**
- Rows eliminated by law+exception encoding: **23,658 (99.2532%)**

## P0 absorption directions

### representation_cas
- Decision: `ABSORB_OWNER_KEEP_IMMUTABLE_BODIES`
- Exact native evidence files found: **102**
- Required parity before mutation:
  - same digest algorithm and digest identity
  - immutable-after-receipt/path law
  - same retained body can serve multiple semantic identities
  - same rights/provenance receipt survives absorption
  - native hash/body lookup resolves every current CAS body

### fact_row_delta
- Decision: `ABSORB_EVENT_ENVELOPE_KEEP_DELTA_SEMANTICS`
- Exact native evidence files found: **102**
- Required parity before mutation:
  - old/new body or representation identity preserved
  - insert/delete/retraction semantics preserved
  - event time and observation/retrieval time do not collapse
  - HLC/frontier ordering has a native equivalent or explicit retained field
  - replay and correction produce same current projection

### representation_target
- Decision: `REPLACE_CARTESIAN_ROWS_WITH_LAWS_PLUS_EXCEPTIONS`
- Exact native evidence files found: **45**
- Required parity before mutation:
  - same eligibility result for every semantic_kind × representation_kind query
  - same local exception/defer reason is recoverable
  - substrate readiness remains observable
  - materialization obligation still lowers into WorkGraph/native work
  - no semantic architecture or capability disappears

## Physical bytes

| Family | Bytes | Files | Representation instances |
|---|---:|---:|---:|
| `bodies` | 54.2MiB | 201 | 0 |
| `lance` | 37.6MiB | 604 | 150 |
| `graph` | 11.4MiB | 116 | 0 |
| `iceberg-cas` | 2.3MiB | 809 | 0 |
| `iceberg-zero-copy` | 2.3MiB | 809 | 0 |
| `duckdb` | 524.0KiB | 1 | 202 |
| `zarr` | 436.3KiB | 87 | 11 |
| `change` | 300.1KiB | 7 | 0 |
| `heads` | 106.4KiB | 202 | 0 |
| `flight` | 93.5KiB | 1 | 0 |
| `substrait` | 83.9KiB | 194 | 0 |
| `manifest.json` | 75.1KiB | 1 | 0 |
| `parquet` | 72.4KiB | 1 | 202 |
| `xtable-configs` | 69.5KiB | 202 | 0 |
| `xtable` | 63.7KiB | 1 | 0 |
| `paimon-change` | 25.7KiB | 8 | 0 |
| `actualization-frontier.json` | 4.9KiB | 1 | 0 |
| `zero-copy-report.json` | 1.3KiB | 1 | 0 |
| `fabric-report.json` | 1.0KiB | 1 | 0 |
| `zero-copy-report.txt` | 1.0KiB | 1 | 0 |
| `fabric-report.txt` | 883.0B | 1 | 0 |
| `derived` | 844.0B | 2 | 0 |
| `actualization-frontier.tsv` | 816.0B | 1 | 0 |
| `immutable-change-report.json` | 667.0B | 1 | 0 |
| `immutable-change-report.txt` | 657.0B | 1 | 0 |
| `arrow` | 0.0B | 0 | 202 |
| `delta` | 0.0B | 0 | 202 |
| `iceberg` | 0.0B | 0 | 404 |

## Possible superseded physical trees — investigation only

- `iceberg-zero-copy` + `iceberg-cas`: 4.6MiB. two Iceberg catalog trees coexist; determine whether old mutable-path catalog is fully superseded before retirement

## Mutation rule

Do **not** delete or rewrite anything from this report alone.
The next executable step is a parity-witness script for the P0 mappings.
Only after every mapped field/behavior is witnessed through its native owner should sidecar rows/files become retirement candidates.
