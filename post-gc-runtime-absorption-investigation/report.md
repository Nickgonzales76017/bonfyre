# Bonfÿre Post-GC + Runtime Absorption Investigation v2.2

Generated: `2026-09-29T06:36:05.849871+00:00`

## Post-GC invariants

- `representation_target` object type: **view**
- Legacy target object: **None**
- Representation laws: **118**
- Override groups: **2**
- Override members: **60**
- Compatibility rows: **23836**
- Compatibility digest: `6a5d811b45e94639ffd9123e3f725fcaf7388819fc2eac8fd34281c47b631f7f`

- Lance realizations: **150**
- Reconstructable Lance: **150**
- Available Lance: **0**
- Lance paths still resident: **0**
- Lance resident bytes: **0.0B**

- Old Iceberg exists: **False**
- CAS Iceberg exists: **True** (2.3MiB)

- Canonical bodies: **201**
- CAS digest mismatches: **0**
- CAS missing bodies: **0**

## Representation-runtime attribution

- Total venv bytes: **965.6MiB**
- Persistent runtime Python files: **2**
- Representation migration/probe Python files: **5**

- `MIGRATION_ONLY`: **8 packages / 438.5MiB**
- `PERSISTENT_RUNTIME`: **3 packages / 159.4MiB**
- `TOOLCHAIN_ONLY`: **3 packages / 15.6MiB**
- `TRANSITIVE_OR_UNSEEN`: **51 packages / 352.0MiB**

Largest distributions:

- **170.8MiB** `pylance 12.0.0` — `MIGRATION_ONLY`
- **152.5MiB** `polars-runtime-32 1.44.2` — `TRANSITIVE_OR_UNSEEN`
- **108.7MiB** `deltalake 1.6.6` — `MIGRATION_ONLY`
- **107.3MiB** `pyarrow 19.0.1` — `PERSISTENT_RUNTIME`
- **98.5MiB** `datafusion 54.0.0` — `MIGRATION_ONLY`
- **64.3MiB** `pandas 2.3.3` — `TRANSITIVE_OR_UNSEEN`
- **44.3MiB** `duckdb 1.5.6` — `PERSISTENT_RUNTIME`
- **33.6MiB** `opendal 0.47.10` — `MIGRATION_ONLY`
- **31.1MiB** `numpy 2.5.3` — `TRANSITIVE_OR_UNSEEN`
- **22.5MiB** `adbc-driver-flightsql 1.12.0` — `TRANSITIVE_OR_UNSEEN`
- **18.8MiB** `SQLAlchemy 2.1.1` — `TRANSITIVE_OR_UNSEEN`
- **11.5MiB** `pyiceberg 0.12.0` — `MIGRATION_ONLY`
- **10.2MiB** `pip 26.2.1` — `TOOLCHAIN_ONLY`
- **8.4MiB** `polars 1.44.2` — `MIGRATION_ONLY`
- **8.2MiB** `arro3-core 0.8.3` — `TRANSITIVE_OR_UNSEEN`
- **8.1MiB** `Pygments 2.21.0` — `TRANSITIVE_OR_UNSEEN`
- **7.7MiB** `pypaimon 2.0.0` — `PERSISTENT_RUNTIME`
- **5.2MiB** `setuptools 84.0.0` — `TOOLCHAIN_ONLY`
- **4.6MiB** `lance-namespace-urllib3-client 0.11.1` — `TRANSITIVE_OR_UNSEEN`
- **4.4MiB** `pydantic_core 2.46.5` — `TRANSITIVE_OR_UNSEEN`
- **3.8MiB** `adbc-driver-sqlite 1.12.0` — `MIGRATION_ONLY`
- **3.6MiB** `pydantic 2.13.5` — `TRANSITIVE_OR_UNSEEN`
- **3.4MiB** `cramjam 2.12.1` — `TRANSITIVE_OR_UNSEEN`
- **3.3MiB** `zarr 3.4.0` — `MIGRATION_ONLY`
- **3.0MiB** `fastavro 1.12.2` — `TRANSITIVE_OR_UNSEEN`

## Interpretation boundary

`TRANSITIVE_OR_UNSEEN` does not mean safe to delete; import scanning cannot see dynamic imports, native shared-library dependencies, entry points, or plugin loading.
The next absorption pass should build a fresh minimal runtime from the positively required dependency closure rather than deleting packages in-place.
