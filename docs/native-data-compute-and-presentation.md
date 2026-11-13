# Native computation, data and presentation: the two compounding paths

Bonfÿre's software work becomes much easier to understand when a **native application** and a **large local dataset** are shown in the same working experience. The project already has concrete parts of both:

- **YAFF and CUE** preserve distinct architectural claims and validate narrow contracts, which are lowered to native C/Zig structures and bounded owner readers. They are not a generic universal data format or a substitute for SQL.
- **Puck, GrapesJS and Editor.js** are editor adapters over retained typed authoring state. WebAddress/SurfaceScene/MD4C/Mustach produce browser-visible editorial, profile and application content with actual CMS/publication scope.
- **PostCSS `.pcss`, Tailwind and presentation tokens** give those native pages, account applications and editor controls a common design/build path, without copying a donor frontend or inlining unrelated one-off style blocks into C.
- **BonfyreEmbed, BonfyreVec, vGPU and columnar stores** let a local machine calculate usable research views and semantic neighbors across thousands or hundreds of thousands of source records. A page or editor reads the resulting bounded tables instead of repeating the expensive work per visit.
- **BitNet and FPQ** are different representation/ranking experiments. BitNet can reorder a **previously allowed** candidate frontier; FPQ encodes model weights and measures reconstruction quality. Neither becomes a source of truth, authorization or production model quality simply by running.

These parts meet where a person reads, searches, filters, edits, listens, compares or publishes. The architectural advantage depends on their *verified interfaces*, not a claim that every document is poured into one central Matrix or capability registry.

[Main README](../README.md) · [Real workflows](real-workflows-and-interoperability.md) · [Frontend authoring](authoring-frontend-architecture.md) · [Atlas](../architecture/README.md)

## 1. Two real paths meet in the same experience

~~~mermaid
flowchart TB
  subgraph contracts["Authored contracts and real source owners"]
    YAFF["YAFF: architectures, invariants, interactions, forbidden inferences"]
    CUE["CUE: page regions, app lowerers, physical reader contracts"]
    OWNER["CMS / publication / source / account / media / finance owners"]
  end
  subgraph physical["Physical processing on the local device"]
    FILE["SQLite, Parquet, Arrow, document files and retained source material"]
    EMBED["BonfyreEmbed: pinned ONNX vectors and model digest"]
    VEC["BonfyreVec: local vector storage, exact/ANN search"]
    VGPU["vGPU compute: tiled similarities and neighborhoods"]
    DUCK["DuckDB-Wasm: Parquet SQL inside browser tab"]
    RANK["BitNet: bounded reordering of AddressPlane survivors"]
    FPQ["FPQ/FPQx: measured weight representations"]
  end
  subgraph presentation["Author and reader experience"]
    EDIT["Puck / GrapesJS / Editor.js"]
    NATIVE["Typed authoring document / native C / MD4C / Mustach"]
    PCSS[".pcss + PostCSS nested + Tailwind + closed presentation tokens"]
    BROWSER["Article / search / research table / account workspace / publication / listening"]
  end
  YAFF -. "Architectural evidence and constraints" .-> NATIVE
  CUE --> NATIVE
  OWNER --> EDIT
  EDIT --> NATIVE
  NATIVE --> BROWSER
  PCSS --> BROWSER
  OWNER --> FILE
  FILE --> EMBED --> VEC
  FILE --> DUCK
  FILE --> VGPU
  VEC -. "Only when the chart/model/passport matches" .-> VGPU
  VGPU --> MATERIAL["Persisted derived tables with input digest and backend receipt"]
  MATERIAL --> BROWSER
  DUCK --> BROWSER
  VEC --> BROWSER
  RANK -. "Only eligible, passported frontier" .-> MATERIAL
  FPQ -. "Only after full-model parity and admission" .-> EMBED
~~~

**Arrow meaning matters.** This diagram contains three different operations:

1. Native contracts define **what an application may read, write and render**. Their job is type checking and exact owner linkage, not computing nearest neighbors.
2. Local engines perform **the math and data movement**: embedding, exact or approximate search, numeric kernels, columnar selection, cached derived rows and bounded readback.
3. The browser and editorial tools use **verified outputs** in one composition. A visible related-item card, table, trend or audio control is still owned by the real source/publisher and its publication rules.

The vGPU and Embed/Vec arrows are intentionally separate: an archived story's nearest neighbor under a 256-dimensional latent chart may use a prepared numeric sidecar produced by another chart compiler; it is **not automatically** a query against BonfyreVec's current 384/768-dimensional learned model. Distances are comparable only when backend, tokenizer, model, corpus, dimensions, normalization and representation agree.

## 2. YAFF and CUE are doing different work

| File / tool | What it actually contains | Concrete consumer or witness |
|---|---|---|
| [`architecture/**/*.yaff`](../architecture/README.md) | architecture identity, source paths, status, invariants, exact interactions, what must be preserved and what cannot be inferred | native [Atlas](../architecture/README.md) and `architecture_atlas` MCP; CI validates unsupported maturity claims |
| [`architecture/interactions.yaff`](../architecture/interactions.yaff) | source → destination relation with preserved fields, dropped claims and refused inferences | Atlas expansion and architectural regression |
| [`schemas/control/page-composition.cue`](../schemas/control/page-composition.cue) | typed `#PageRegion` roles, reading order, materials, DOM topology, minimum/maximum items, subject schemes | CUE export into generated native page composition, not a fallback bag of generic cards |
| [`schemas/control/site-application-lowerers.cue`](../schemas/control/site-application-lowerers.cue) | application layout, selected canonical material owner, reader, query key, structural operations, named style asset | strict [Zig lowerer compiler](../cmd/BonfyreWebAddress/tools/compile_site_application_lowerers.zig); unknown fields/layouts/opcodes fail |
| [`architecture/native-ratchet.cue`](../architecture/native-ratchet.cue) | native program/JS budgets and tightening requirements | pre-commit, CI and native ratchet checks |
| [`cmd/BonfyreBrief/data/hosting-inventory.cue`](../cmd/BonfyreBrief/data/hosting-inventory.cue) | shape of one specific cPanel/InterWorx/Plesk import inventory | `cue vet` tests reject source claims mislabeled as customer statements |

**The useful technical distinction:** YAFF is where the architecture can tell the truth about what was implemented, measured or forbidden; CUE checks **real shapes**. The native compiler then emits bounded descriptors or uses a named source reader. CUE does not grant a customer account, and the fact that YAFF records an interaction does not execute one.

For example, a CUE page region can say that an editorial page has a `results` or `table` topology consuming a normalized publication stream. It cannot silently turn a `source_channel` candidate into a read article, or transform a vector similarity score into an accepted recommendation.

## 3. The styling system is a compiler input, not a pile of CSS overlays

The **local development tree** has an active PostCSS/Tailwind migration. It uses **`.pcss`** (PostCSS source files; do not call it Sass/SCSS), `postcss-nested@6.2.0`, `tailwindcss@3.4.19`, `@tailwindcss/typography@0.5.16` and `@tailwindcss/forms@0.5.10`, pinned by `deploy/tailwind/package.json`.

~~~mermaid
flowchart LR
  TOKENS["PresentationTemplate and authored style roles"] --> VARS["Closed CSS custom properties: --w-ground, --w-ink, --w-accent, --w-panel, etc."]
  MARKUP["C emitted class= and editor templates"] --> SCAN["Tailwind class-value extractor"]
  PCSS["deploy/tailwind/input.pcss, styles/*.pcss, browser/*.pcss"] --> POSTCSS["postcss-nested + Tailwind @apply/components/utilities"]
  SCAN --> POSTCSS
  VARS --> PAGE["Real styled site and account experience"]
  POSTCSS --> BUILT["Compiled .css at existing URLs"]
  BUILT --> PAGE
  PAGE --> CHECK["Build/check byte parity + responsive/browser acceptance"]
~~~

In [`deploy/tailwind/tailwind.config.js`](../deploy/tailwind/tailwind.config.js), the `content.transform` reader extracts **class attribute values** from C header/implementation source, MJS authoring components and HTML, including the escaped class attributes emitted by C. That keeps Tailwind from interpreting arbitrary C keywords as UI classes. The semantic `@layer components` vocabulary is included even when one current page does not use every control. The templates provide constrained color, type, spacing, density, layout and mode roles; direct user-authored arbitrary selectors/scripts cannot bypass publication scope.

The local `deploy/tailwind/input.pcss` defines real button, hover, focus and form treatment with `@apply` and tokenized custom properties; `deploy/tailwind/styles/*.pcss` compiles the main website families; `integrations/browser/*.pcss` compiles the browser workspace styles **beside their owning modules**. The build retains the existing deployed `.css` paths so serving code need not move to a new route just to adopt the compiler.

**Source synchronization:** as inspected on 2026-11-13, public GitHub `main` includes the Tailwind configuration but **does not yet include** the local `deploy/tailwind/input.pcss` or `deploy/tailwind/package.json`. The attached Codex handoff records the local migration of the site styles and eighteen browser-workspace styles plus rebuild/check commands. Do **not** claim this new CSS pipeline is on published GitHub or deployed to the four live domains until the actual source, compiled assets and release readback are reconciled.

**Open styling debt:** legacy inline `<style>` and route-specific styles remain in C and standalone HTML emitters. Compiling more CSS does not by itself remove duplicate headers, independent article shells, poor mobile grids or incomplete Stitch screen functionality. The acceptance is a **single coherent layout and component grammar** with real interactions and responsive screenshots, not prettier disconnected pages.

## 4. Puck, GrapesJS, Editor.js and the compiled frontend

The local-only `cmd/BonfyreSurfaceScene/runtime/authoring-tree.mjs` retains a typed tree with known block kinds, 27 service-section shapes, nested responsive layouts, closed style tokens, editorial blocks, exact identifiers and bounded media/URL forms. The browser adapters provide distinct editing methods: Puck (React), GrapesJS (design), Editor.js (article). The native renderer uses MD4C/Mustach/C to deliver the **same retained composition** at the selected site and public audience.

- The source allows styles such as `layout=columns`, `span=4`, `background=panel` and `typography=editorial`. It refuses arbitrary CSS objects in saved authoring state.
- Browser-facing components use shared responsive/editorial CSS; final site styling should derive from the `.pcss` build and selected presentation tokens, rather than each route issuing its own styles.
- A publication can carry formatted paragraphs, tables, figures and source links. A dense interactive result table, semantic related-story rail, financial workbook or timeline is **a typed application/record reader** and still needs its own interaction and authorization contract. The editorial canvas does not assume it is allowed to rewrite those records.
- The existing exact account CMS review controls protect revisions, file references and site namespaces. A published section does not imply every private source becomes public.

**Current acceptance gap:** the attached session reports 90 canonical journal posts and 327 sections published in the CMS, while the then-running public binary still lacked the `/blog/` route. It also records interrupted Stitch-family work, ongoing duplicate HTML shells and added narration controls. This is a **local/CMS/source versus live-release discrepancy**. The documentation must not mark the 90 journals or 91 Stitch designs as live-parity proven without four-apex and individual route readback.

## 5. Embed and Vec: local retrieval with exact representation identity

~~~mermaid
flowchart LR
  ITEM["Retained publications, passages, transcripts, documents"] --> CORPUS["semantic_corpus.mjs: stable key + text"]
  CORPUS --> EMBED["bonfyre-embed --input-jsonl --backend onnx"]
  EMBED --> MANIFEST["Model/vocab/backend digest + actual width"]
  EMBED --> VECTORS["Float vectors + source key"]
  VECTORS --> VEC["bonfyre-vec: sqlite-vec ANN / exact SIMD / neighbors"]
  VECTORS --> PARQUET["Optional DuckDB Parquet representation"]
  PARQUET --> BROWSER["Local browser SQL / research table"]
  VEC --> REVIEW["Related passages / search / editorial suggestions"]
  MANIFEST --> REVIEW
  REVIEW --> SOURCE["Resolve exact original source, rights and current owner"]
~~~

[BonfyreEmbed](../cmd/BonfyreEmbed/src/main.c) uses the **ONNX Runtime C API, BERT WordPiece tokenizer and mean pooling** for actual neural text embeddings. It has a separate deterministic hash backend; requesting ONNX and receiving hashed vectors without explicit degradation permission is a refusal. The cache identity includes model graph, vocabulary and runtime identity. A model name is **not** enough to decide vector geometry.

[BonfyreVec](../cmd/BonfyreVec/src/main.c) uses the SQLite C API with `sqlite-vec` and also implements an exact SIMD cosine path. Its CLI supports `init`, `insert`, `search`, `neighbors`, `compare`, `count` and exact passage ID deletion. The store is opened at its actual configured dimension, and queries must match the full width: a former 768-dimensional query truncated to 384 floats was a real correctness bug. The native deletion test proves that metadata and vectors are removed together with transaction rollback on failure.

The [`build_semantic_index.sh`](../scripts/build_semantic_index.sh) path constructs a corpus from the local archive, calls ONNX embeddings, inserts into Vec, and can write a **Parquet** table for other readers. This is local source processing and *optional publication of derived vectors*, not automatic permission to publish the original source material.

**Critical invariants:** learned vs hashed-token vectors never share a distance threshold; a missing model refuses rather than claiming learned similarity; current source identity and rights are rechecked for any action; and a near vector is a candidate for review, not proof that two subjects are identical.

## 6. vGPU: numerical work over large local datasets

The first production-relevant kernels are **not 3D rendering**. The pinned [vGPU Stage kernel](../cmd/BonfyreSurfaceScene/runtime/stage-compute-vgpu.mjs) computes tiled all-pairs distances among placed records. The [vGPU kin kernel](../cmd/BonfyreSurfaceScene/runtime/kin-compute-vgpu.mjs) computes cosine similarity between rows in a numeric `float32` chart and retains top-k comparable items. They run through vGPU 0.3.1 / Dawn-backed `vgpu/node` when supported, or a deterministic CPU path, with a mock compute backend for tests.

The kin kernel accepts a packed numeric file plus a **parallel key list**: `--vectors vectors.f32 --keys keys.json --dims 256 --k 8 --backend auto|mock|cpu --min 0.85`. It tiles under workgroup dispatch limits and returns a receipt identifying which backend actually ran. The Stage neighborhood compiler persists result rows in SQLite, checks an input-and-code digest, skips identical generations without re-executing a kernel, and rolls back failed changes atomically.

~~~mermaid
flowchart LR
  ROWS["Thousands of retained source rows"] --> FEATURES["Stable keys + bounded Float32 feature columns"]
  FEATURES --> TILE["vGPU tiled numeric kernels / CPU fallback"]
  TILE --> OUT["Top-k and distances, backend receipt"]
  OUT --> SQL["Materialized local SQLite neighbor / kin table"]
  SQL --> FIND["Article related reading / cross-publisher twin"]
  SQL --> MAP["Nearby records / event and place context"]
  SQL --> TABLE["Publisher research or investigation table"]
  TABLE --> EDIT["Journal / editor / downstream installed app"]
~~~

**Evidence:** the native [ItemArchive Atlas entry](../architecture/surface/item-archive.yaff) records **6,810 items, 46,376,100 candidate pairs in 1.1 seconds** on vgpu-node, with 710 above 0.85. The retained cross-publisher duplicate test identified true twins while excluding unrelated stories. The compiler's [local parity test](../cmd/BonfyreSurfaceScene/tests/compile_neighbourhood_native_test.sh) verifies deterministic CPU output, persistent rows, no repeat computation when inputs are unchanged, and transaction recovery. The measured performance is for that **one prepared estate workload and hardware**, not a guarantee for every data shape.

**Browser-scale data is another real lane:** [`site/matrix/field.html`](../site/matrix/field.html) reads an R2-hosted Parquet channel catalogue into **DuckDB-Wasm** in the tab and visualizes it with WebGPU. The Atlas records **109,761 channel rows** read by range in September 2026, of which 2,364 were admitted channels; the majority were only candidates. A later locally tested kept-copy path used origin-private storage and conditional HTTP responses, but its edge deployment was not proven. Browser SQL and GPU drawing here are **different steps**: no claim that DuckDB SQL runs on vGPU is justified.

**Separate browser compute witness:** [`chromium-engines.mjs`](../integrations/browser/engines/chromium-engines.mjs) runs a WGSL compute program over bounded `u32`, `i32` and `f32` arrays, measuring compile/upload/execute/readback independently. The Atlas recorded a 100,000/1,000,000/4,000,000-element hash exercise in which **CPU won after GPU readback overhead**, and another nearest-vector exercise where GPU beat a JavaScript reference; neither was a real estate workload against a native CPU baseline. Do not market every operation as GPU-faster.

### What this means for enormous tables

A proper user-facing table does not need to ship a million DOM rows or run an all-pairs query on each keystroke. The existing owners support an intended pipeline:

1. **Columnar input:** SQLite for current records and review decisions; Parquet/Arrow/DuckDB-Wasm for large local analytical material. Streaming, byte ranges, origin storage and stable row IDs keep browser transfer bounded.
2. **Numerical preprocessing:** source/time partitions, embeddings, vector indexing, similarity, local aggregates, de-duplication or neighborhoods. Use vGPU for the measured kernels whose workload benefits; use SQL/CPU/SIMD elsewhere.
3. **Persisted derived cuts:** retain actual backend, source/input/code hash, dimensions, row counts, rejection threshold, top-k and output fingerprint. No page-view recomputation of millions of comparisons.
4. **Reader/editor projection:** virtualized table or bounded result page, sort/filter/group/search, link to source passages, named exception flags and direct editorial continuation. Presentation tokens and PCSS define the layout; the domain owner defines the data and permissible edits.
5. **Downstream fanout:** review a cross-publisher twin, write a source-linked research note, publish a narrated article, create a CRM lead, prepare a document or create an analysis sheet **only in a matching installed application with the user's current grant**.

**This final path is a product architecture, not proof that a general-purpose vGPU table engine and all five downstream continuations are already deployed.** The present evidence covers specific vGPU numerical kernels, local derived results, DuckDB-Wasm browsing and distinct app/authoring owners. A full high-volume table/editor interaction needs end-to-end browser and release tests with the real dataset.

### The next table-compute family: what should run on vGPU

The *already demonstrated* vGPU work is tiled pairwise similarity and spatial distance. A full native **large-table compute product** needs more operations. These are engineering targets, **not yet verified vGPU kernels**; the owning SQL/column engine must remain authoritative until each kernel passes parity.

| User interaction over a large local table | Possible physical execution | Exact result and acceptance |
|---|---|---|
| Select a million source rows by bounded flags, date window, account/publisher ID or confidence threshold | DuckDB/SQLite predicate scan first; GPU predicate bitmap/compaction only if the transfer and memory envelope wins | stable source keys and exact selected-row equality, including null/date semantics and deterministic ordering |
| Group a source catalogue by provider, protocol, status or time bucket | columnar SQL aggregation first; GPU histogram/reduction only for measured numeric/encoded categories | exact integer counts/overflow handling, source-state differentiation, bounded per-group materialization |
| Compare document/vector cohorts or find cross-publisher duplicate stories | **existing vGPU kin** dot-product tiles/top-k; BonfyreVec exact/ANN where matching embedding model is appropriate | similarity score, model/chart/dimensions, cut threshold, backend receipt, top-k parity and original publisher link |
| Find anomalous outliers in a numerical Excel/finance or sensor/telemetry table | native numeric review + deterministic CPU baseline; GPU parallel statistics as a candidate | source sheet/cell ID, signedness/precision, missing-value behavior, exception row references, and human review |
| Spatially cluster observations/events or ask “what is near this item?” | **existing Stage vGPU neighborhood** and persisted SQLite `neighbour` table | exact coordinates, time validity, distance tolerance, no inferred attendance/membership/current truth |
| Join two large source tables or export a reviewed result | DuckDB/SQLite keyed join and domain writer remain primary; potential GPU join only after measured correctness | preservation of ID/provenance and stable identity across changes; format output (XLSX/PDF/Parquet) owned separately |
| Sort, page and edit the current interactive view | bounded SQL/top-k and browser virtualized/windowed DOM; GPU only if numerical sort is proven beneficial | a single row identity and current revision, keyboard/focus behavior, source link and unchanged off-screen records |

**Physical plan and economics.** The choice of GPU, SIMD CPU or DuckDB-Wasm must be based on the **whole data movement**: source bytes → decoding/column conversion → transfer to device → shader compile/reuse → kernel → *small result/readback* → serialization → page. In the Chrome WGSL hash experiment the GPU compute time alone looked good, but GPU readback made the processor faster. It would be unsound to route a 100,000-row filter through GPU because a 4-million-value kernel can run there. vGPU earns placement when the supported operation and its measured workload benefit.

**Memory placement.** Columnar Arrow/Parquet source remains a data representation; SQLite/DuckDB keeps the query, validity, data type and authoritative row IDs; GPU buffers contain only a bounded numeric projection (e.g. packed `f32` vectors, `u32` category IDs and validity bitmap). Avoid sending titles, account secrets, unredacted document bodies or opaque record envelopes to a compute shader. A GPU output should be a compact selection/top-k/aggregate keyed back to the canonical source.

**Incremental compilation.** Hash the source cohort, query/feature specification, representation/model identity, kernel bytes, numeric width, threshold and backend. Reuse a persisted derivation when those inputs are identical. When only part changes, a future delta compiler could update impacted partitions; the existing Stage neighborhood source notes that full-generation recompute remains a gap. Never report a stale derived table as current because its previous kernel succeeded.

**Downstream ownership.** A table row can be selected for review; its original source opens in an evidence pane; the user may continue it into Puck/Editor.js as a quote/section, into a CRM with the matching declared intake, into a native workbook review, or into a narrated editorial edition. Each continuation is a **separate typed effect with exact identity, permissions, revision and release**. The same source data can supply several apps without a second user-facing demo table being copied into every app.

### Benchmarks required before a general-purpose table claim

A table-compute runtime should be accepted on reproducible 10,000, 100,000 and 1,000,000-row input cohorts, with numeric widths and missing data varied, using a precise reference (native C/SQLite/DuckDB as appropriate). Report CPU wall-clock, GPU compile/upload/execute/readback, peak resident/system/device memory, final result bytes, source/model/key fingerprints, ties/null behavior, output equality or numeric tolerance, fallback path, and **time to the first useful interactive viewport**. Device loss, unavailable Dawn, workload too small to amortize transfer, stale input digest and account permission change must have deterministic refusals/fallbacks. A browser screenshot of a million dots is not this test.

**Editorial/UI acceptance** then checks a real filtered row → original publication source → user-authored saved section → published article with source link, responsive layout and approved narration or document output. This is the intended compounding of compute, frontend and publication, and it must remain distinguishable from the existing independent source-level tests.

## 7. FPQ and BitNet are downstream optimizations with different meanings

| Mechanism | What it works on | Verified fact | What cannot follow from that fact |
|---|---|---|---|
| [BonfyreEmbed](../cmd/BonfyreEmbed/src/main.c) | text + pinned ONNX model/tokenizer | actual learned vectors, cache identity and degradation refusal tests | that vectors from other models have comparable geometry |
| [BonfyreVec](../cmd/BonfyreVec/src/main.c) | stored float vectors with stable passage IDs | exact/extension search and source-specific deletion | that a neighbor is an accepted same subject or a valid licensed source |
| [vGPU kin](../cmd/BonfyreSurfaceScene/runtime/kin-compute-vgpu.mjs) | one chosen numeric chart, key list, cosine floor | large pairwise run with persisted top-k and backend receipt | that a near article is factually related, current or approved to display |
| [BonfyreBitNet](../cmd/BonfyreBitNet/src/main.c) | only *AddressPlane-surviving* descriptors; passported model embedding | 92-survivor ranking tests and preservation of exact verifier/lowerer identities | that BitNet can discover or admit a rejected source or bypass ForeignTwin |
| [BonfyreFPQ](../cmd/BonfyreFPQ/src/fpq_native.c) | model weights, factor/quantization representation | 114,688-weight Qwen tensor reconstruction at 5.75 bits/weight, cosine 0.999935, 1.23 seconds | a deployable compressed Qwen with layer/logit/generation/task parity |
| [BonfyreFPQx](../cmd/BonfyreFPQx/src/main.c) | native packed vector/representation file shapes | command/fixture readback against native binary readers | production model serving or identical behavior across arbitrary weight layouts |

[BitNet's ranking contract](../cmd/BonfyreBitNet/src/main.c) is particularly narrow: deterministic AddressPlane masks define **eligibility**; the learned ranking can only reorder remaining handles; exact owner and source verification still follow. The measured quality also includes cases where the ranking was **anti-correlated with actual usefulness**; it remains diagnostic/SHADOW for the affected frontier until an appropriate preservation envelope is admitted.

[FPQ's real-tensor receipt](../cmd/BonfyreFPQ/results/2026-09-11-qwen2.5-coder-0.5b-real-tensor-v9.json) measures **one transform and reconstruction**. The full compression-to-generation path still lacks accepted non-passthrough execution and parity. It should be documented as a serious numerical representation result with explicit remaining engineering, not as “FPQ makes all GPU tables and editorials cheap.”

The **intended compounding loop** is useful even while those representation gates are held: large local source processing yields a small, exact candidate set; the reader/editor asks for just those records; an admitted model may summarize *source-verified* material; another real application receives only an authorized continuation. FPQ may eventually reduce model residency and inferencing cost after it passes behavior tests. BitNet may narrow reviewer attention if ranking trials prove useful. Neither is a requirement for the existing vGPU and Vec lanes to work.

## 8. A worked editorial example, from 100,000 rows to one published story

**Use case: a local publisher asks which of tens of thousands of source-channel candidates are actually read, which current articles repeat across publishers, and what to include in a weekly investigative digest.**

~~~mermaid
flowchart LR
  PARQUET["source_channel.parquet: 109,761 row witness"] --> SQL["DuckDB-Wasm: select by state/protocol/publisher"]
  ARCHIVE["ItemArchive: kept story material"] --> CHART["One pinned numeric chart + stable source keys"]
  CHART --> GPU["vGPU kin: tiled cosine and thresholded pairs"]
  GPU --> DERIVED["kin SQLite table with receipt"]
  SQL --> WORK["Research/table page: admitted vs candidate channels"]
  DERIVED --> WORK
  WORK --> SOURCE["Open original publisher item; verify source/date/rights"]
  SOURCE --> EDITOR["Puck/Editor.js authored article, reviewed CMS revision"]
  EDITOR --> HTML["MD4C/C WebAddress article + PCSS/Tailwind visual language"]
  HTML --> OUTPUT["Journal, Listen narration, local PDF or approved destination"]
~~~

The existing **source-side** and **compute-side** work supports the first several stages independently. The complete clickable table-to-editor-to-audio path must be tested at the exact account and release before claiming it is already one functioning product. One article may be shared through an RSS feed or exported PDF without turning the article into a CRM lead. Any eventual narration remains with the real media/audio owner and rights check.

## 9. The acceptance ledger is per operation, not per architecture adjective

| Claim | Current witness | Remaining proof |
|---|---|---|
| YAFF/CUE source constraints and generated application lowerers | Atlas native query/validation, page composition CUE, Zig strict lowerer parser and native tests | synchronized deployment and every additional app material reader |
| PCSS + Tailwind tokens across site/browser styles | local `deploy/tailwind` build/check and saved Codex commands; Tailwind config exists publicly | **publish local PCSS sources/lockfile, reconcile built CSS assets, run release/browser parity** |
| Puck/GrapesJS/Editor.js → typed native authored site | source tree, C/MD4C renderer, page composer and browser fixtures | remove remaining independent shells; 91 Stitch routes and 90 articles live/readable |
| Native ONNX embedding and SQLite-vector search | Embed and Vec source/tests, semantic corpus script | each selected model's dims/passport/actual model readiness; per-app search quality |
| vGPU neighborhood and kin computations | native compiler/source tests and recorded 6,810-item run | Linux Dawn delivery; more workload families; invalidation beyond whole-generation cut |
| Browser analysis of large Parquet catalogue | field witness at 109,761 rows and Chrome kept-copy fixture | published edge storage/304 behavior and responsive table/analysis controls |
| BitNet learned ordering | measured 92-candidate shadow frontier and exact-owner preservation | admissible quality and representation passport, no false-prune regression |
| FPQ model representation | Qwen real tensor measured receipt | non-passthrough full-model behavior, generation/logit/task parity and residency win |
| End-to-end local table → editorial → narration and export | distinct local owners and tests for each step | one authenticated integration test with source, derived cache, UI, editor, publication and output readback |

The stronger claim for Aurekai is **local analytic depth becoming part of the same editorial and application experience**, with file portability, provenance, readable design and real user actions. That can be demonstrated by a large source table with a real query, a measured GPU/CPU computation, a retained result, an edited article and a verified published page. The documentation should keep naming the exact code, result and proof at each hop.
