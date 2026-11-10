# Bonfÿre native library extraction — product decision 2026-10-10

This is a productization handoff over the architecture atlas, NOT a replacement architecture registry.
One Aurekai business, one native execution/authority/evidence foundation; libraries are distribution cuts, not new runtime owners.

## Adopted target — portfolio industrialization, not another experiment catalogue

**User decision, October 10 2026:** The estate-wide library and markets study and its 127-row, 12-arena candidate matrix are the adopted productization target. The nine names below are illustrative distribution umbrellas, NOT a ceiling, a fixed package count, or a requirement to implement 127 packages. Preserve every candidate's source/competitive evidence and discover further cuts as the estate matures.

Execute across **four connected outputs**, not four extra runtime tiers:
1. **Core convergence** — identify one authoritative native owner for each data, representation, policy, work, finance and effect behavior; absorb repeated C/JS/Python/adapter implementations only after caller and state-conformance tests.
2. **Adoptable libraries** — extract separately installable, versioned C/JS/Wasm/WIT code that is useful without signing up for Aurekai; use sub-Bonfÿre product names only where an actual API, independent consumer and support boundary exist.
3. **Distribution adapters and application packs** — integrate those libraries through existing WordPress, Frappe, MCP, browser/HTMX, regional ERP and other third-party carriers; adapters never become shadow truth stores.
4. **Operating-business distribution** — apply the same underlying work to customers, partner agencies, foreign-market operators, institutions, manufacturers and service providers. Record proposals, accepted commitments, usage, costs and settlements in their existing owning organs.

**Portfolio selection rule:** prefer cuts with a verified existing implementation, a valuable developer/customer job, a measurable advantage or interoperability requirement, an inexpensive clean-release path, and a real route to users. Prioritize by *shared core dependencies and commercial pull*, not by command count, alphabetical naming or one flagship SDK monopolizing all work. Multiple independently verifiable library/adapter deliveries may advance together when they reuse the same owner.

**Source-based record per chosen cut:** existing canonical source and callers; retained vs upstream implementation; license and IP constraints; ABI/carrier form; present evidence (H/X/N); end-to-end negative/restart tests; one outside adoption/partner/buyer signal; cost to maintain; explicit promotion or retirement decision. Derive this from current atlas, candidate matrix, package manifests and Finance/Outreach: do not create a parallel database merely to track planning.

**Release and commercial truth:** a compiled organ is not an SDK; a domain-specific adapter is not a new engine; a listed buyer is not a signed project; a quote is not cash. Publish only with clean installation and a receiving-system witness, then reuse those proven components across distinct applications and institutions. Continue market research globally, not just for U.S. SaaS or HR.

---

## Naming and packaging boundary
First flagship: bonfyre-data — developer-grade embedded local SQL/vector/source-revision engine with optional browser/remote projections. Package as a C ABI + tiny JS/Bun facade + browser worker + MCP projection of that SAME engine. Frappe/WordPress/Turso/DuckDB adapters do not own canonical data.
Don't rewrite upstream SQLite, sqlite-vec, Turso or DuckDB. Native C kernels are justified only for proven algorithmic/performance/correctness differentiation.

## Nine facades over fewer native engines
| Library | Independent developer use | Existing source | Hard boundary |
|---|---|---|---|
| bonfyre-data | LEAD embedded local SQL + vector + evidence + browser reads | BonfyreVec, BonfyreEmbed, BonfyreCMS ANN, libbonfyre SQLite, FactDelta | No SQLite or Turso fork; one current write owner |
| bonfyre-contracts | CUE/YaFF typed schemas, policies, ABI and code generation | schemas/control, FrappeCompiler, ProtocolContract, ObjectContract | Compile != permission or delivered effect |
| bonfyre-model | Model serving, quantization, embeddings, context cuts | BonfyreQuant, SLI, FPQx, Model, ONNX, MoE | Keep numerical specializations; prove quality |
| bonfyre-work | WorkGraph/commitment/actor/occurrence and MCP capabilities | WorkGraph, Authority, OccurrenceOrigin, recursive MCP | No duplicate event or financial ledger |
| bonfyre-apps | Frappe, CRM, HRMS, LMS, CMS, WordPress integration packs | nine donor apps, 44 modules, 1152 typed records, WP bridge | Projection != donor controller or installed SaaS |
| bonfyre-media | Capture, speech, clips, media sync and distribution | BonfyreTranscribe, MediaLibrary, LiveSemanticTrack, MoQ | Licenses and correction provenance remain distinct |
| bonfyre-commerce | Supplier discovery, RFQs, quotes, deliveries and partner operations | SourceFrontier, ForeignTwin, CommerceChain, Provider | An observed price is not acceptance |
| bonfyre-finance | Schedules, statement imports, meters, payables, receipts | Finance, Pay, Meter, QuantLib, CloseConsequence | Preserve financial authority, null costs |
| bonfyre-web | htmx HTML, generic app grammars and local-first client worker | WebAddress, SiteProgram, ClientOrgans, DuckDB-Wasm | Browser is not independent authority |

Translation, regional document grammars, provider transport and app/industry variations are composable packs, not automatically their own repos or code owners.

## Bonfyre Data: 38 capabilities in five bundles
H = present in measured/implemented source; X = needs extraction/composition or conformance; N = net new. X is not yet a public SDK contract.

### 1. Native SQL and semantic material
1. [H] SQLite C + Bun SQLite database carriers
2. [X] One checked native/JS connection and error API
3. [X] Explicit read/cache/financial-durability policy (not blanket PRAGMAs)
4. [X] Bounded transaction and busy-lock readback
5. [H] CMS transactional JSONL import and derived index updates
6. [X] Compiled schema/migration revisions from existing CUE/YaFF
7. [H] Occurrence-origin/source-digest/versioned evidence
8. [X] Source-controlled materialized views and invalidation

### 2. Vector and retrieval
9. [H] sqlite-vec KNN native vector store
10. [H] NEON/scalar exact cosine + safe delete
11. [H] ONNX and native-MoE embeddings with batch insertion
12. [H] reembedding same ID transactionally replaces vector + metadata
13. [X] model/vector representation passport and dimension refusal
14. [H] CMS LSH/tensor ANN separate index
15. [N] FTS5 BM25 + vector hybrid retrieval
16. [X] source metadata/refusal-aware filters + optional SAE rerank
17. [N] version-pinned ANN backend comparison by recall/latency

### 3. Browser/analytical data
18. [H] DuckDB-Wasm and Arrow browser query assets
19. [N] SQLite-Wasm OPFS worker with Chrome/Firefox/Safari parity
20. [X] DuckDB persistent OPFS analytics and CHECKPOINT
21. [X] Arrow transfer of allowed typed rows
22. [X] Parquet snapshot/export and columnar pushdown
23. [H] client-organ worker/cache/bounded offline reads
24. [N] multi-tab locking, quota/eviction and cold-start measurements

### 4. Sync, rules and portability
25. [H] native OccurrenceOrigin/FactDelta/change provenance
26. [X] SQLite Session changeset carrier with PK & conflict semantics
27. [N] Turso Database sync/CDC as optional upstream adapter
28. [N] libSQL remote compatibility adapter (not Turso Database)
29. [X] compact repeated route rules + reconstructable exceptions
30. [X] digest-keyed version and revocation propagation
31. [N] cross-owner/offline conflict conformance suite

### 5. Developer integration
32. [X] tiny JS/Bun API with batch reads/writes, no subprocess per query
33. [N] C ABI compatible browser WASM distribution
34. [X] SQL extension functions for admissible vector/material queries
35. [X] MCP tools/resources projection through existing Protocol/Authority
36. [X] headless htmx fragments via WebAddress/SiteProgram
37. [X] Frappe/WordPress/REST/JSON backend projections
38. [N] npm+C+WASM release, ABI/benchmarks/licenses/SBOM

## Highest leverage already found
* BonfyreEmbed supports single-model batch embedding and no intermediate vector-file I/O with --insert-db. Its real store-contention test checks that reported committed rows equal SQLite rows, even under a held read transaction.
* BonfyreVec has sqlite-vec KNN plus exact SIMD and source-ID deletion; its width guard refuses geometry mismatches. BonfyreCMS independently carries LSH+4-bit quantized tensor ANN. The two are not automatically equivalent or safe to merge.
* The Sep 29 convergence radar found 131,362 subject_route_region rows with 19 distinct repeated payload signatures and estimated 99.986% repeated-payload representation reduction. This is NOT a whole-DB byte-reduction claim. Only invertible row-by-row reconstruction and rights/ordering parity permits reclamation.
* Browser estate already has DuckDB-Wasm/Arrow, client organs, offline field caching and browser engine tests. Official DuckDB-Wasm gained documented OPFS persistence in Sep 2026; its transactional and analytical use cases should be kept distinct from SQLite-Wasm.

## P0 source risks — fix these before publishing an SDK
1. lib/libbonfyre/src/bf_sqlite.c unconditionally executes WAL, synchronous=NORMAL, 64 MiB cache and 256 MiB mmap on writer opens, ignores PRAGMA return, and claims a universal 5–30x speed gain. Finance and evidence owners need intentional durability profiles and actual PRAGMA readback. NORMAL may lose latest committed work on sudden power failure.
2. BonfyreVec init still creates a 384-dimensional vec table; embedding backends can produce 768 or 2,048 dimensions. An existing embedder creates stores by returned width and the query path guards mismatch, but clean-install developer experience needs model-bound schema creation instead of an assumed 384.
3. bonfyre-vec status currently reports sqlite-vec-native as compiled backend identity without opening a database or loading the extension; this must not be sold as live extension health.
4. sqlite-vec and SQLite vec1 indexing/ANN options depend on the actual pinned extension version. Measure result recall and cost against the existing native LSH path; do not label a vec0 scan inherently approximate.
5. SQLite Session captures changes from suitable ordinary PK tables, not all virtual tables; synchronized vector indexes must be re-derived and independently verified.
6. Source subject, observed bytes/revision, model geometry and actor/authority are distinct keys. A vector similarity is not permission to disclose or external proof.
7. Preserve rights, attribution and license boundaries of all bundled upstream dependencies and samples; no proprietary ERP/customer data in distributable fixtures.

## Readiness and first release
P0: Clean-install bonfyre-data on macOS/Linux with one C handle, one JS facade. Test real source ID + model passport -> transactional native embed/vector insert -> exact retrieve -> delete/source invalidation -> persisted readback. Add negative tests for dimension mismatch, crash/restart, lock contention, duplicate entry and financial durability. No new standalone database process.
P1: ship equivalent browser worker over upstream SQLite-Wasm/OPFS and the existing client-organ contract; test Chrome/Firefox/Safari and multi-tab locks, origin isolation and quota errors.
P2: add FTS5 hybrid, deterministic language/source filters and exact compact-rule inversion, benchmark actual data and end-to-end developer workload vs raw upstream.
P3: optional Turso/PowerSync sync using their upstream transport after conflict & permission tests, DuckDB/Parquet analytical queries; no implicit syncing of private or money state.
P4: the other eight facades earn public names only after at least one independent clean consumer, native tests, a working package and licensing/effect boundary review.

## Market
Easy install -> ordinary developer adoption -> agency integrator -> customer-specific production -> recurring Aurekai operating work. A US importer, Indian exporter (ERP CoO), HR staffing agency and creator can all reuse the same native data core but must keep their separate institutional rights and contracts.

## Commercial interpretation of headless IP
UI clones and standard integration APIs are becoming cheaper. This doesn't eliminate IP rights, maintenance cost, real performance work or business value. Bonfÿre's defensible advantage is interoperable source-linked operations, reproducible behavior, low compute costs, embedded distribution and relationships. A demo clone of Adobe is not proof the proprietary source was decompiled or that production feature parity exists.

## Exit gate
Do not mark bonfyre-data 0.1 released until a clean install from a tag runs the same native/JS source+retrieval+delete tests, reports measured end-to-end performance, carries compatible license notices and one external consumer can use it without copying the estate.

## Expanded October 10 market audit — nine names are not a ceiling

The original nine facades are marketing umbrellas, not all possible libraries. Read-only source and native inventory plus current public market references produced **127 candidate cuts in 12 arenas**: 68 implementation-present components, 48 extraction/assembly candidates, and 11 net-new candidates. An implementation-present component is NOT a releasable SDK. The full 127-row research study and machine-readable TSV are supplied as ChatGPT conversation artifacts, not claimed as local checkout files.

| Arena | Candidate cuts | Main upstream and audience |
| --- | ---: | --- |
| Embedded SQL, vector, local and browser data | 13 | SQLite/vec1/sqlite-vec, Turso, DuckDB, PGlite; app developers |
| Compression, representations and memory | 10 | zstd, Arrow, content-addressed stores; offline and hosted estates |
| Model kernels, layers, serving and inference | 12 | llama.cpp, MLX, ggml, WebLLM; model runtime authors |
| Compilers, app grammar and contracts | 11 | CUE, TypeSpec, WIT/Wasmtime, Frappe; SDK/integration builders |
| Work, evidence, authorization and agents | 12 | Restate, Temporal, Cedar, OpenFGA; enterprise integration |
| Protocols, MCP, networking and toolmesh | 10 | MCP v2, Beckn, QUIC/MoQ, x402; software platforms |
| Web/browser/native interface and publishing | 10 | htmx, DuckDB-Wasm, browser workers; local applications |
| Audio, video, creator, transcription | 10 | LiveKit, Pipecat, sherpa-onnx; media/staffing/creator ops |
| Finance, procurement, wholesale, settlement | 10 | ERPNext, Blnk, QuantLib; agencies, accountants, buyers |
| Spatial, property, logistics, field and device | 8 | PostGIS/H3/PMTiles; insurers, municipalities, field service |
| International documents and trade standards | 10 | DGFT CoO, India IRP, Brazil NFS-e, GS1, Mexico CFDI, Peppol |
| App-specific distribution and operations | 11 | WordPress, Frappe Cloud, Twenty; implementers and institutions |

These 127 are *market/packaging hypotheses*, not 127 command additions or repos. Underlying native kernels, semantic/evidence owners and current state databases remain unique. Integrations, language packs, extensions and operated businesses can combine libraries without duplicating ownership.

### What present-day competitor economics imply

SQLite vec1 already exposes native SIMD and IVFADC/OPQ approximate indexing (https://sqlite.org/vec1.html). Turso Database is a distinct Rust SQLite-compatible engine, not older libSQL, and has hosted free/developer tiers (https://github.com/tursodatabase/turso ; https://turso.tech/pricing). Cloudflare Vectorize is GA with free 30M queried dimensions/month and 5M stored dimensions, and inexpensive usage (https://developers.cloudflare.com/vectorize/platform/pricing/). Pinecone has a free starter and $20/mo Builder tier (https://www.pinecone.io/pricing/). **Do not sell ordinary vector storage as a novel or premium database.** Sell source/permission correctness, local interoperability, native low-resource workflows, portable customer apps and operating support.

DuckDB-Wasm now supports persistent browser OPFS but requires pinned working versions and checkpoints (https://duckdb.org/2026/09/18/opfs-wasm). Browser SQLite and DuckDB are different transactional and analytical implementations. Avoid rewriting upstream sync or VFS without measured gaps.

WordPress 6.9 Abilities and the official MCP Adapter already expose WP operations to agents (https://developer.wordpress.org/news/2026/02/from-abilities-to-ai-agents-introducing-the-wordpress-mcp-adapter/). Our WP product should provide cross-account approval, receiving-system reconciliation, ERP/customer operations and source-linked effects rather than a generic MCP wrapper. Frappe Cloud permits free/paid OSS apps with monthly app plans and a review gate (https://docs.frappe.io/cloud/marketplace ; https://docs.frappe.io/cloud/app-plan). An actual Frappe app and sandbox are required, not just a compiler reading DocType metadata.

### More immediately purchasable international extensions

* **India DGFT CoO API**: official Sep 7 2026 opportunity, signed ERP/accounting CoO document boundary. One $450 test/mapping subcontract email actually SENT to 8848 Digital and separately backfilled to Finance/Outreach; no acceptance or cash. Source https://apeda.gov.in/dgft-trade-notice
* **Brazil NFS-e national service invoices**: as of Oct 10, a Nov 1 2026 API/invoicing obligation is approaching for many Simples Nacional micro/small providers. A Brazil-based ERP/Frappe partner could buy migration/testing, failure/readback and recurrence. Our connector not yet built, customer not yet contacted. Source https://www.gov.br/receitafederal/pt-br/assuntos/noticias/2026/agosto/simples-nacional-nfs-e-nacional-sera-obrigatoria-para-me-e-epp-a-partir-de-1o-de-novembro-de-2026
* **GS1 Digital Link / Sunrise 2027**: identity and 2D packaging need product-owner and retailer agreement; QR code alone grants no inventory, certification or sales right. Source https://www.gs1us.org/industries-and-insights/gs1-digital-link
* India GST invoice IRP, Mexico CFDI, EU/Peppol, U.S. invoice/origin and EDI are different juridical profiles across ONE typed document/ERP/protocol engine, not cloned national finance organs.

### Product ownership and extraction priority

P0 **First developer distributions**: BonfyreData (C+JS embed/vector/source receipts with explicit durability), BonfyreContracts (typed CUE/YaFF->WIT/JS/JSON/Frappe), BonfyreSpatial (bitemporal GeoJSON/Parquet), BonfyreEvidence (observed/derived/effect readback), BonfyreWordPress (signed/replay-safe receiving workflow), first real partner-ready Frappe integration. Internal reuse can promote other libraries only when clean-install/conformance and one outside consumer are evidenced.

P1 **Differentiated native ports**: BonfyreMedia source-timed transcription and clip rights, BonfyreModel compact kernel/model artifact and context, BonfyreRulepack inverse-tested law/exception compaction, BonfyreCommerce buyer RFQs and trade documents, BonfyreWeb browser local-first client semantics. Prefer existing implementation and tests; 59 app kinds and 30 visual grammars are combinable consumer designs rather than separate runtimes.

P2 **Hold**: own SQLite clone, own distributed sync protocol, generic voice-agent framework duplicate, unlicensed product UI reconstructions, premature one-off national apps, vendor-specific headless CRUD transports. These markets have substantial incumbent free OSS and hosted prices; justify a rewrite only by hard performance, exact behavior and paid adoption.

Current source risk: lib/libbonfyre/src/bf_sqlite.c blindly applies WAL/NORMAL/64MB/256MB and ignores PRAGMA refusal; cannot be a universal finance/evidence durability default. BonfyreVec init fixes table at 384 while other embeddings can be 768/2048. CMS LSH ANN, sqlite-vec, SQLite vec1 are different search kernels; benchmark recall/latency before combining. The radar's 131362 subject rows to 19 signature observation is a candidate repeated-payload factorization, NOT a reclaimed file/durability claim.

**Exit gate:** one outside user can install a C library and JS package, execute a real source-bound record and vector lifecycle, restart and reproduce it, and use that library through both WordPress and Frappe without a shadow data owner. One paid partner engagement then closes through Finance/Pay with a real receipt. Research alone satisfies none of those acceptance events.

## Adopted refinement: presentation-native products and agent-host UI (2026-10-10)

The 127-row/12-family source inventory is now 163 candidate CUTS over 16 market families after examining design systems, TUIs, MCP Apps/agent UI and business-specific presentation. The 36 added cuts are 26 existing-mechanism extractions/compositions and 10 genuinely missing carrier/test behaviors. They are NOT 36 planned native organs or independently released libraries. Detailed refined study and 163-row matrix were generated in the ChatGPT conversation, not added to a new authoritative repo registry.

### What already exists, and what does not

- Existing `cmd/BonfyreSurface/src/surface_ir.c` reads exact namespace/page/slot/audience and published CMS fragments with source and evidence fields. It is NOT yet a general response/action description for all carriers.
- `schemas/control/site-presentation.cue` already has 59 ApplicationKinds, semantic slots, SiteProgram, CapabilityProgram, and presentation extents; `presentation_template` has layout tokens; `surface_renderer` owns independent human interaction grammar; generated WebAddress programs expose 14 renderer families and 30 reusable clone grammars.
- `integrations/mcp/recursive_overlay.mjs` presently returns `structuredContent` and text; no `ui://` MCP App resources or `_meta.ui.resourceUri` were found in that inspected overlay. `cmd/BonfyreCLI` is a native dispatcher and status surface, not currently a themed TUI. `cmd/BonfyreRender` is a brief/media artifact packer, not a general UI renderer.

### One semantic result, several beautiful interfaces

Compile `read-only authorised facts + source/evidence references + currently admitted actions + chosen PresentationTemplate` into a result view. This view is an ephemeral projection, NOT a new business state owner. Preserve identical facts and closure across carriers while allowing their own layout conventions:

| Carrier | Reuse/adopt | Rule |
| --- | --- | --- |
| Terminal | OpenTUI (Zig + Bun/TS), styled components from permissive sources like termcn; Charm and ratcn as reference/optional ports | Native command truth, keyboard/screen reader/non-TTY paths; no full-C rewrite of an established terminal renderer |
| Website and mobile | Existing WebAddress/HTMX/SurfaceIR, presentation templates, client organs | Native server HTML, polished studied layouts, real author/effect owners |
| MCP Apps-compatible chats | Official `ui://` resources and `_meta.ui.resourceUri`; `structuredContent` + output schema | Host opts in; tools remain useful without UI; no authority from displaying a button |
| Declarative native UI | A2UI catalog or json-render adapter | Allowlisted components and per-host accessibility/style; do not execute arbitrary model-created HTML/JS |
| Streaming agent state | AG-UI adapter over current WorkGraph/Effect/Occurrence | Transport and state updates only; no second execution workflow |
| Plain/CI/unsupported host | Stable JSON plus concise accessible text | No vital information may depend on a renderer the host declines |

### The high-value MCP/UI change

The official MCP Apps standard is stable and supported by compatible hosts. Decouple DATA tools from RENDER tools. Let the LLM filter/compare selected IDs first, then choose a view tool that references an authored `ui://` template; its sandboxed interactive UI may issue explicitly authorised calls and update model-visible context. This does NOT force a host's entire final response to follow Bonfÿre styling; the host retains rendering control and may decline MCP Apps. Underlying private data/quotes/payment/effect truth stays in native owners. See https://github.com/modelcontextprotocol/ext-apps and https://developers.openai.com/plugins/build/chatgpt-ui and https://github.com/a2ui-project/a2ui and https://github.com/ag-ui-protocol/ag-ui .

### Release cuts, not new intermediates

1. **Surface/result contract**: reuse SurfaceIR and typed owners to project a verified commercial/document/finance result with source identity, actual lifecycle, authorized capabilities, density and theme tokens; read only.
2. **Two carrier witnesses**: same result rendered in a real terminal via upstream OpenTUI and in a negotiated MCP Apps-compatible host, with JSON/text fallback; no new database and no auto-send.
3. **Visual quality and security**: test 80-column and mobile layout, 16/256/truecolor, reduced motion and screen reader, keyboard flow, approved component/action catalog, source parity, snapshot regression and actual design review.
4. **One permitted interactive effect**: user request -> existing authorization/WorkGraph -> actual owner call -> receiving-system receipt -> exact redraw; changing theme can never change money, employment, trade status or source authority.
5. **Commercial extension**: let agency, HR, importer/exporter, property, creator and finance partners brand the same admitted operation using licensed style packs and market-specific documents. White-label design is a partner distribution lever, not a second application engine.

**Brand/library boundary:** `bonfyre-surface` is an existing core/output design owner, `bonfyre-tui` and `bonfyre-mcp-apps` are potential public carrier packages AFTER build+host tests, and themes/widgets/business workspaces are curated adapters/packs beneath them. The complete 36 added candidate rows should not be minted into 36 repos. Demand, external clean installs and source-consistent rendered decisions—not package or command counts—determine adoption.
