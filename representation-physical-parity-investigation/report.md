# Bonfÿre Physical Parity & Retirement Investigation v1.4

Generated: `2026-09-29T06:04:09.859119+00:00`

## Lance parity

- Compared pairs: **150**
- Exact reconstructable equivalents: **150**
- Distinct/mismatch: **0**
- Errors: **0**
- Equivalent Lance bytes: **29.1MiB**

A reconstructable equivalent still represents a useful Lance capability law; it simply means the resident Lance bytes may be evictable until demand justifies them.

## Graph ownership

- Bytes: **11.4MiB**
- Files: **116**
- Manifold physical-realization references under graph tree: **114**
- Available references: **114**
- Text markers: `{}`

## Iceberg supersession

- Old tree bytes: **2.3MiB**
- CAS tree bytes: **2.3MiB**
- Old realization states: `[["superseded",0,202]]`
- New realization states: `[["physical",1,202]]`
- Repo files still literally referencing `iceberg-zero-copy`: **1**
- Automated retirement candidate: **False**

## Readiness normalization

- `declared`: **33** representation kinds
- `evidence_only`: **74** representation kinds
- `physical`: **4** representation kinds
- `ready`: **7** representation kinds

## Scaffolding decomposition

- **965.6MiB** `runtime_component` — `/Users/nickgonzales/.bonfyre/representation-runtime/venv`
- **60.4MiB** `sidecar_db` — `/Users/nickgonzales/.bonfyre/estate-local/representation-manifold.db`
- **4.3MiB** `runtime_component` — `/Users/nickgonzales/.bonfyre/representation-runtime/xtable`
- **32.0KiB** `sidecar_db` — `/Users/nickgonzales/.bonfyre/estate-local/representation-manifold.db-shm`
- **28.9KiB** `shift_script` — `/Users/nickgonzales/Developer/Bonfyre/scripts/representation_manifold_forge.py`
- **27.1KiB** `runtime_component` — `/Users/nickgonzales/.bonfyre/representation-runtime/live_delta_fabric_v1_8_1.py`
- **25.4KiB** `shift_script` — `/Users/nickgonzales/Developer/Bonfyre/scripts/representation_manifold_zero_copy.py`
- **24.6KiB** `runtime_component` — `/Users/nickgonzales/.bonfyre/representation-runtime/live_delta_fabric.py`
- **17.3KiB** `shift_script` — `/Users/nickgonzales/Developer/Bonfyre/scripts/representation_manifold_shift.py`
- **14.6KiB** `shift_script` — `/Users/nickgonzales/Developer/Bonfyre/scripts/representation_manifold_fabric.py`
- **3.7KiB** `runtime_component` — `/Users/nickgonzales/.bonfyre/representation-runtime/runtime-manifest.json`
- **0.0B** `sidecar_db` — `/Users/nickgonzales/.bonfyre/estate-local/representation-manifold.db-wal`
- **0.0B** `runtime_component` — `/Users/nickgonzales/.bonfyre/representation-runtime/tmp`

## Boundary

No files or rows were deleted. A physical realization becomes an eviction/retirement candidate only when its semantic capability remains reconstructable and no live owner points at the retiring bytes.
