# vGPU compute in SurfaceScene

The vGPU code here is **local numeric processing over columns and vectors**. Its first kernels find related archive records and nearby placed observations. Their outputs are persisted for research, source discovery and editorial readers. They are **not** 3D web-asset rendering.

[Full system: YAFF/CUE, PCSS, editor, Embed/Vec and FPQ/BitNet](../../../docs/native-data-compute-and-presentation.md) · [Kin kernel](kin-compute-vgpu.mjs) · [Spatial kernel](stage-compute-vgpu.mjs) · [Native compiler tests](../tests/compile_neighbourhood_native_test.sh)

## Current executable kernels

| Entry | Inputs | Numerical work | Output and actual consuming owner |
|---|---|---|---|
| `kin-compute-vgpu.mjs` | packed `float32` chart; ordered stable item keys; exact dimensions; top-k; threshold | cosine dot products of normalized rows, tiled to stay under GPU dispatch bounds | receipt and per-item nearest candidates; [`compile_kin.mjs`](../tools/compile_kin.mjs) materializes SQLite rows used by source/publisher related-reading |
| `stage-compute-vgpu.mjs` | StageClosure with placed material and coordinates; per-source keys; radius/top-k | nearest-neighbor distance in meters, in bounded tiles | backend/geometry receipt and neighbor rows; [`compile_neighbourhood.mjs`](../tools/compile_neighbourhood.mjs) stores SQLite `neighbour` rows |
| [Chrome WGSL engine](../../../integrations/browser/engines/chromium-engines.mjs) | bounded arrays of `u32`, `i32`, `f32` plus a caller-specified WGSL program | compute shader with compile/upload/run/readback separately timed | a diagnostic GPU result, **not yet** a native table/estate application |

Both SurfaceScene kernels support `auto` (Dawn-backed `vgpu/node` where available), `mock` and deterministic `cpu`. `auto` must report which backend actually ran. The existing vGPU adapter lacks a Linux/x64 Dawn prebuild, so CPU fallback is a real production path.

~~~bash
# Already prepared raw float32 vectors + a parallel list of stable source IDs.
node cmd/BonfyreSurfaceScene/runtime/kin-compute-vgpu.mjs \
  --vectors ./vectors.f32 --keys ./keys.json --dims 256 \
  --k 8 --tile 1024 --backend auto --min 0.85

# Materialize placed-item distances from an authorized retained-items file.
bun cmd/BonfyreSurfaceScene/tools/compile_neighbourhood.mjs \
  --items ./items.jsonl --db ./archive.db --backend cpu \
  --k 8 --tile 2048 --max-km 4
~~~

**Input semantics:** the vector file is `number_of_keys × dimensions × sizeof(float32)`; keys are ordered to correspond to the rows. The math compares positions in **one** chart and cannot declare two items the same subject or current source content. Placed records must actually provide coordinates; zero/unknown coordinates are refused as evidence of placement.

## Measured result, and what it proves

[ItemArchive architectural witness](../../../architecture/surface/item-archive.yaff) records a September 2026 local run over **6,810 retained items**: **46,376,100 pairwise comparisons** completed in **1.1 seconds** on `vgpu-node`, retaining **710** pairs above a **0.85** similarity threshold. This is one host and one feature chart. The source-side verification subsequently rejected unrelated items and identified some cross-publisher duplicates. Similarity alone is not an editorial approval or duplicate-removal permission.

[The neighborhood test](../tests/compile_neighbourhood_native_test.sh) exercises deterministic CPU work, stable persisted rows, cache skip when input/code digest is unchanged, survival of unrelated archive updates and atomic rollback after an injected failure. [Kin test](../tests/compile_kin_native_test.sh) additionally checks result parity against the historical reference where enabled. The compilation owner retains a digest/receipt and updates stored rows rather than running the kernel on each HTTP request.

## Feeding large local tables and editorial

~~~mermaid
flowchart LR
  PARQUET["Parquet / SQLite / source files"] --> COLUMNS["Typed numeric projection + stable source key"]
  COLUMNS --> GPU["vGPU bounded tiles / deterministic CPU"]
  GPU --> REC["Exact backend, input, dimensions, output receipt"]
  REC --> DB["Persisted neighbor/kin SQLite table"]
  DB --> UI["Filtered table / source-linked related reading"]
  UI --> EDIT["Review original source → article section / installed app"]
  EDIT --> PUB["Native WebAddress, PCSS styles, publication, narration/export"]
~~~

This is the operational bridge: **calculate once, keep a versioned derived result, render a small reader-facing answer, and preserve a route to the underlying source**. A publisher can see candidate duplicate stories, a civic researcher can see nearby observations, a curator can see related archive material, and a large-table analyst can inspect result rows without recomputing every similarity when the page opens.

The [browser field](../../../site/matrix/field.html) is another large-data lane: DuckDB-Wasm reads Parquet by range inside the tab, and WebGPU renders a 109,761-row source-channel catalogue in a measured release. **The DuckDB SQL scan and WebGPU drawing are not vGPU table kernels.** A general GPU-backed filter/groupby/join/aggregate engine is still a development target, with correctness, null handling, type fidelity, transfer/readback cost and deterministic CPU fallback to prove against the native/SQL reference.

## How downstream optimization fits

[BonfyreEmbed](../../BonfyreEmbed/src/main.c) produces actual ONNX learned embeddings; [BonfyreVec](../../BonfyreVec/src/main.c) indexes/searches those vectors. Reuse in kin is valid only when the feature chart, model, tokenizer, dimensions, normalization, keys and thresholds match. A 256-dimensional chart is **not automatically** a 384- or 768-dimensional BonfyreEmbed/Vec corpus.

[BitNet](../../BonfyreBitNet/src/main.c) can reorder only AddressPlane-surviving candidates and is held where quality/passport admission fails. [FPQ](../../BonfyreFPQ/README.md) transforms model weights; it does not itself reduce the cost of a columnar SQL query or vGPU kernel. FPQ may eventually make a downstream source-grounded model cheaper to run once compressed generation and task parity are proven.

## Tests required for a general-purpose data-compute offering

- Verify exact numeric/select/group/top-k results versus a C/SQLite/DuckDB reference across **10k, 100k and 1M rows** with nulls, ties and narrow/wide numeric types.
- Record backend, compile/upload/compute/readback/wall-clock times, peak memory, transferred bytes, correctness tolerance, output size and failure behavior. The Chrome WGSL hash trial showed GPU loses to CPU after readback in some sizes.
- Protect each row's source identity, rights, freshness and revision. A device loss, missing Dawn, out-of-bounds input, stale generation or unavailable source must be an explicit failure/fallback.
- Verify a real browser table with viewport/windowing, keyboard navigation and source links. The downstream editor must save a reviewed section and the released article must render under the appropriate site and audience, including audio/export if selected.

**Do not call a GPU computation “published editorial,” or a high similarity score “source truth.”** The target is a complete, source-grounded user interaction, not an impressive shader benchmark or a 3D screenshot.
