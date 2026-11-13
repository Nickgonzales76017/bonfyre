# Bonfÿre — native software architecture

**Bonfÿre is a native, typed software engine.** Its code, schema, runtime, source/representation machinery, interfaces and technical releases are not the assets or authority of any particular operating company. Aurekai is an independent operator and deployment example, not an engine namespace or automatically privileged tenant. See the [ownership boundary](OWNERSHIP-BOUNDARY.md).

The operating reason for the architecture is concrete: AI accelerates code
production, while lost context, specialist knowledge gaps, meetings, handoffs
and downstream integration can still block delivery. Bonfÿre makes source
identity, capability, work, authority, execution and evidence reusable so each
verified project can improve later projects rather than become an isolated
implementation.

The architecture preserves distinct semantic owners and lets many physical
realizations serve them; no single language, binary, model, agent, database or
hosted product constitutes the entire platform.

[Business, project and technical model](aurekai-bonfyre-model.md) ·
[Working public experiences](https://aurekai-work-samples.pages.dev/) ·
[Aurekai Work](https://aurekai.work) ·
[Life](https://aurekai.life) ·
[Live](https://aurekai.live) ·
[Shop](https://aurekai.shop)

## Authority and truth

Two sources answer different questions:

1. The frozen Generation-10/V8.1 map defines the cumulative semantic
   destination and its validated projection estate.
2. [`architecture/`](../architecture/) is the canonical live registry for what
   this repository currently implements, measures, or only declares.

Generated diagrams and this page are views. They never outrank the Atlas. Run:

```bash
bun architecture/atlas.mjs validate
bun architecture/atlas.mjs expand ExecutionView
bun architecture/atlas.mjs get work-graph
bun architecture/atlas.mjs loss
```

No maturity laundering is allowed: `measured` and `proven` require witnesses,
and a collapsed view must expand reversibly to real children.

## The authoring and frontend compiler

**[Unified authoring, native frontend and innovation architecture](authoring-frontend-architecture.md)** explains why Aurekai's editor work is much more than a creator site or commercial website builder.

~~~mermaid
flowchart LR
  A["Authoritative source / account / community / app"] --> T["Shared typed authoring tree"]
  E["Puck / GrapesJS / Editor.js"] <--> T
  T --> C["Revision-checked CMS fragments"]
  C --> N["Native MD4C, Mustach and C component renderer"]
  N --> S["SurfaceIR + SceneCompiler + responsive design roles"]
  S --> W["WebAddress HTML, interactions, account/public release"]
~~~

The native tree preserves source references, nested components, application bindings and desktop/tablet/phone style roles through the editors; Editor.js contributes typed editorial blocks without making raw editor HTML the document model. The development tests cover all 27 ServiceSite section owners and an actual compiled Frappe CRM Lead, rather than only the old seven-block adapter. Generated templates, licensed glyphs, studied design anatomy, and 91 retained Stitch design references compiled into 14 reusable experience patterns supply frontend breadth without importing donor runtime code.

**Why this matters:** a group's community page, a historical civic map, a scientific notebook, a personal publication, a creator's media article, and a real organization application can each be authored as a contextual projection of its actual source and rights. A site's visual design and local browser interaction can evolve without silently replacing the semantic owner. This *composition and continuity across kinds of software* is the proposed innovation; independent libraries are credited as upstream components, and live production parity remains separately verified.

## System-wide architecture — no single demo owns a capability family

The **[complete Aurekai / Bonfÿre architecture map](architecture-visuals/system-architecture.md)** now covers identities, organizations, communities/groups, address/presence, external estate, source observations, provider accounts, connections, native spatial/temporal query and maps, Git/Forgejo/Josh, model representation and exact masks, FPQ/BitNet, memory/data, audiovisual and narration, Frappe/CMS, commerce, agents, work, proof, protocol carriers and staged releases. It contains **13 native GitHub Mermaid visualizations** and a [capability-family index](architecture-visuals/system-architecture.md#12-coverage-index--capabilities-not-screenshot-examples).

The canonical [source registry](../architecture/README.md) currently has 11 expandable high-level views—IdentityView, InstitutionalView, ExternalView, ProviderResourceView, ModelView, SubstrateView, FormsView, CapabilityView, ExecutionView, EvidenceView and OpportunityView—and each zoom must retain its underlying architecture IDs, interaction contracts, forbidden inferences and measured witnesses. Those *11 summaries are not the complete domain taxonomy*; surface, spatial, social, media, application and other owner families cannot be dropped because they are missing from the shortlist.

| Core concern | Important separation |
|---|---|
| **People / institutions** | Actor, account, organization, group, community, presence and information policy are not synonyms |
| **Sources / installed apps** | Retained source identity differs from admission, source truth, downstream draft and effect |
| **Maps / space** | Temporal spatial observations and indexed queries exceed any room-planning example |
| **Models / compression** | Exact eligibility masks, SHADOW ranking, FPQ transform and ADMIT runtime parity are separate gates |
| **Git / source** | Native Forgejo + Josh checkout, GitHub installations and GitHub Pages are distinct source/delivery routes |
| **Work / finances** | A goal, quote, executed action, financial transaction and evidence of delivery are distinct records |
| **Publishing / media** | Editorial content, optional audio edition, rights, recording, player, profile, community and live release do not imply one another |
| **Frappe / applications** | DocType compilation is not controller execution; app, account and institution permissions remain with their owners |

### Specialized architectural examples

The original SVG gallery remains useful as a set of **subviews**, not the system definition.

| Example | Narrow scope and limits |
|---|---|
| [Organizations, sites and domains](architecture-visuals/README.md#1-organizations-sites-and-domains) | Host binding / SitePresentation and proposed multi-client address lifecycle |
| [One source / installed-app continuations](architecture-visuals/README.md#2-one-source-many-installed-applications) | Intake and derived use, not a universal narration pipeline |
| [Editorial publishing](architecture-visuals/README.md#3-editorial-composition-and-publication) | A publication/editing chain, not all media and communications |
| [Work and evidence](architecture-visuals/README.md#4-work-execution-and-proof) | A bounded execution/proof example, not every operational program |
| [Nine business application grammars](architecture-visuals/README.md#5-business-application-family) | Frappe-derived typed forms and records, not every enterprise service |
| [One commerce/room offer](architecture-visuals/README.md#6-a-specific-offer-product-commerce-and-room-layout) | A single catalogue/procurement/room-planning continuation, not the spatial engine |

A locally built component, a source-listed provider, an externally referenced substrate, a passed unit test, and an activated public release require different witnesses. The new diagrams indicate these boundaries instead of treating a UI illustration as deployment evidence.

## The local data and frontend engineering is a first-class architecture

The full source/execution map for **YAFF, CUE, PostCSS/Tailwind tokens, Puck/GrapesJS/Editor.js, ONNX embeddings, SQLite Vec, vGPU numerical compute, DuckDB-Wasm/Parquet, BitNet and FPQ** is [native data computation and presentation](native-data-compute-and-presentation.md). These are separate real mechanisms and named source interfaces, with *downstream* table, source, editor, CMS, narration and application consumers.

~~~mermaid
flowchart TB
  YAFF["YAFF: architecture identities and interactions"] -. "constraints and witnesses" .-> WEB
  CUE["CUE: typed PageRegions and lowerer contracts"]
  CUE --> ZIG["Zig→C native application lowerers"]
  ZIG --> WEB["WebAddress / MD4C / Mustach / typed editorial composition"]
  EDIT["Puck / GrapesJS / Editor.js"] --> WEB
  PCSS[".pcss + PostCSS + Tailwind + closed presentation roles"] --> WEB
  SQL["SQLite + Parquet/Arrow local material"] --> EMBED["BonfyreEmbed actual ONNX vector"]
  EMBED --> VEC["BonfyreVec: sqlite-vec, exact cosine"]
  SQL --> VGPU["vGPU: numerical kin/nearest-neighbour kernels; CPU fallback"]
  VEC -. "Only matching representations" .-> VGPU
  VGPU --> PERSIST["Source-scoped derived kin/neighbour SQLite tables + backend/input receipts"]
  SQL --> BROWSER["DuckDB-Wasm in-browser SQL, WebGPU estate field"]
  PERSIST --> EXPERIENCE["Interactive table / research / article evidence rail"]
  BROWSER --> EXPERIENCE
  WEB --> EXPERIENCE
  BIT["BitNet: learned rank of already eligible handles, shadow gates"] -. "After representation and quality admission" .-> EXPERIENCE
  FPQ["FPQ model weight compression"] -. "Only after generation/task parity" .-> EMBED
~~~

**What is measured:** 6,810 archived items, 46,376,100 vGPU pair comparisons in 1.1s under the recorded kin workload, 710 retained above similarity threshold; browser field reading 109,761 Parquet rows with DuckDB-Wasm and drawing them through WebGPU. **Not a claim:** vGPU executes DuckDB SQL, all document/table operations are GPU-fast, the 90 journal articles and new PCSS bundle are live, FPQ is generation-quality proven, or BitNet may admit unverified sources.

Local `deploy/tailwind/*.pcss` and build sources are currently ahead of public `main`; the existing [Tailwind config](../deploy/tailwind/tailwind.config.js) is published. The end-to-end acceptance remains a current, user-owned table → source → editorial save → published responsive page → narration/export, with releases and permission readback.

## Real, concurrent operating paths — not a universal pipeline

See the [source-linked workflow and exchange ledger](real-workflows-and-interoperability.md) before describing a specific system. **WorkGraph is appropriate for scheduled or multi-step work; Matrix is appropriate for addressed subjects/presences; neither is a mandatory hub for every parser, conversion, frontend, media renderer, database, model or provider API.**

~~~mermaid
flowchart LR
  WP["WordPress signed webhook"] --> WPCMS["Native CMS external-ID upsert"]
  WPCMS --> SITE["Published native site (review required)"]
  SITE --> X["Standalone HTML / WXR / Frappe export"]
  XLSX["XLSX archive"] --> BOOKS["BonfyreBooks: cell provenance + review"]
  BOOKS --> WBOOK["Account-owned financial workbook"]
  WBOOK --> WMCP["Workbook MCP: read / preview / update"]
  DOC["Paper / scan / PDF"] --> OCR["BonfyreOCR: positional lines"]
  MD["Authored Markdown / brief"] --> EMIT["BonfyreEmit: HTML/PDF/EPUB/TXT/RSS"]
  INTAKE["cPanel / InterWorx / Plesk export"] --> INVENTORY["BonfyreBrief: owner statements + source discrepancies"]
  INVENTORY --> DB["Migration review / BonfyreBridge verification"]
  CODE["GitHub/Forgejo code + client project"] --> PROJECT["Project MCP / patch preview / tests / native receipts"]
  EDITORS["Puck/GrapesJS/Editor.js"] --> AUTHOR["One typed CMS authoring tree"]
  AUTHOR --> RENDER["MD4C + Mustach + C WebAddress rendering"]
  CREATOR["YouTube/Patreon/Spotify public or provider account"] --> CLAIM["Profile declaration / separate auth and source checks"]
  CLAIM -. "Only scoped, verified owners" .-> SITE
~~~

Each path has its *own concrete outputs and refusals*. WordPress's HMAC event and WXR export are not interchangeable. Books can preserve cells without knowing a payment account. OCR can return page geometry without confirming the words. MCP can update a native draft without posting to a remote system. The owner that validates one step does not automatically validate the next.

### Native operational boundaries

| Operation | Source | Transformation and output | Verification scope |
|---|---|---|---|
| WordPress signed post event | [bridge](../integrations/wordpress/bridge.mjs) | `site/post/version` idempotency; remote ID retained in native CMS | Bridge/static tests; not live WordPress runtime |
| Site export | [site product](../cmd/BonfyreWebAddress/src/site_product.c) | offline HTML with embedded CSS; WXR 1.2 or Frappe records | [local export/reimport parity](../cmd/BonfyreWebAddress/tests/site_product_test.sh) |
| Hosting migration inventory | [BonfyreBrief](../cmd/BonfyreBrief/src/inventory.c) | cites cPanel/InterWorx/Plesk exports and discrepancies | No server execution/cutover |
| Excel financial review | [Books XLSX](../cmd/BonfyreDecide/src/books_xlsx.c), [workbook MCP](../integrations/mcp/workbook_mcp.mjs) | source sheet/cell positions, new XLSX and native-account revision | Not posting/financial approval |
| OCR and deliverables | [OCR](../cmd/BonfyreOCR/src/main.swift), [Emit](../cmd/BonfyreEmit/src/main.c) | positioned scan text; PDF, HTML, EPUB, plain text, RSS | Recognizer/dependencies are distinct |
| MCP project and account tools | [project MCP](../integrations/mcp/project_node.mjs), [workspace MCP](../integrations/mcp/workspace_mcp.mjs) | bounded schema-checked read bundles and revisioned saves | Each owner rechecks grants |
| Creator platforms | [auth candidates](../integrations/browser/provider-auth-capability.mjs), [interaction grammar](../cmd/BonfyreInteractionGrammar/src/main.c) | public links/declared OAuth flows or typed grammar binding | Not automatically a token, API effect or playback |
| React/Svelte/custom runtime | [native authoring](authoring-frontend-architecture.md) | React-backed Puck UI and C WebAddress output; Svelte not independently evidenced as an export lane | Per-framework source/build/browser parity is owed |

## Semantic owners

### Constitution and time

Identity, Occurrence, Work/Effect, Capability, Authority, Evidence/Receipt,
Commitment, TraceDivergence, ServiceLifecycle, ResidentResource, and the
SemanticEvolutionGraph form the contract spine. Occurrence and FactDelta keep
causal and bitemporal state explicit; current projections never erase history.

### Work and execution

WorkGraph owns missions, dependency edges, leases, cooling, commitments, and
completion contracts. CapabilityClosure composes typed transforms, binds them
to real capabilities, checks the proof frontier, selects a computational form,
and only then admits execution. EffectKernel is the final idempotency and
authority boundary.

`AgentSession` is one possible repeated realization circuit. Claude Code,
Codex CLI, MCP, A2A, and other carriers can participate, but none replaces
WorkGraph, CapabilityClosure, Authority, or named Bonfÿre Powers.

### Evidence and learning

Execution produces ReceiptEnvelopes, witnesses, trace divergence, and proof
bundles. Replay, shadow worlds, simulations, gyms, walk-forward evaluation, and
institutional backtesting compare candidate behavior with observed behavior.

The Aurekai operating business uses that evidence to improve products,
customer workflows, partner implementations and reusable capabilities.
Aurekai's optimization/learning loop can discover recurring patterns and
candidate factors. Promotion requires proof and validation; a learned route
may never mutate semantic truth or cross an authority line.

### Projection kernel — a specific compiler, not the whole engine

Contract Projection Kernel v2 materializes selected outputs demanded by an
ActiveClosure. Other paths above are **separate parsers, exporters, renderers and native or external application operations** and must not be rewritten as generic projection-kernel calls. Its projection families include:

- native C, Zig, C++, Rust, Python, and TypeScript contracts
- WIT/WASI/Wasm and component boundaries
- Arrow, Feldera, Substrait, SQL, DuckDB, Daft, Lance, and local metadata
- JSON Schema, OpenAPI, AsyncAPI, GraphQL, protobuf/gRPC, MCP, and A2A
- ActivityPub, MoQ, external participation, and host surfaces
- all nine Frappe product lineages
- finance, evidence, evolution, migration, conformance, gyms, and packs

The frozen V8.1 projection estate proves the complete target. Live repository
absorption is still tracked per organ in the Atlas.

## Product and capability surfaces

### 91 public Powers

The public Power surface contains 91 identities. Identity and callability are
different facts. The live capability ladder is:

```text
defined → implemented → built → installed → resolvable → version_aligned
        → health_probed → activated → workload_proven → quality_proven
        → promoted
```

This prevents a planner from treating a name, a `--help` response, or an
installed binary as proof that a real workload can complete.

### Frappe ×9

Frappe/Core, ERPNext, LMS, HRMS, CRM, Helpdesk, Insights, Wiki, and Drive are
first-class product grammars over shared semantic objects. The products are
installed and their DocTypes are extracted by `BonfyreFrappeCompiler`. The live
Atlas marks them implemented while honestly recording remaining runtime
write-back work.

### Model Commons

Model Commons retains separate identities for models, packs, tokenizers,
quantization, inference, context/cache, retrieval, reasoning, placement,
movement, evaluation, and speech/media. FPQ/FPQx, QwenFPQ, GGUF, BitNet,
llama.cpp, ONNX Runtime, Burn, CubeCL, Metal, Wasm, Embed, Vec, SAE, GigaToken,
KV/PreparedContext passports, and whisper.cpp are specialties, not aliases for
one generic AI runtime.

### Partner Commons

The frozen Generation-10 estate contains 112 Partner Commons profiles and 82
deep-promoted specialties. They connect through ForeignTwin, ProviderGraph,
BoundaryCompiler, WorkGraph, SurfaceIR, EvidenceGraph, and institutional
contracts. A partner provides a specialty or carrier; it does not acquire
Bonfÿre authority over work, identity, evidence, finance, or source truth.

## Operational and financial metabolism

Operational metabolism makes external-tool readiness, provider price/credit,
quota, rate limits, freshness, schedules, artifact packets, placement, and
autonomous worker dependencies part of routing and admission. Resource pressure
degrades concurrency and realization choices before it becomes an unexplained
hard stop.

Financial metabolism preserves the full state transition:

```text
usage/work occurrence → agreement + billing account → spend intent
  → funding source + payment instrument → funding circuit
  → authorization/capture/invoice/accrual/settlement/refund/credit
  → FinancialEvent → CostAllocation → ERPNext accounting → evidence
```

List price, contracted price, estimate, accrual, invoice, authorization,
capture, settlement, payment, accounting, allocation, provider credit, reward,
and economic value are not interchangeable.

## Live maturity snapshot

The Atlas currently records:

| Area | Live maturity |
|---|---|
| WorkGraph and Occurrence spine | implemented |
| Capability lattice and composition | implemented |
| CapabilityClosure and transport phenotype | measured |
| Proof frontier, ForeignTwin, and result-receipt bundle | measured |
| Nine Frappe product grammars | implemented, with write-back gaps recorded |
| Model view | mixed implemented/measured/declared |
| SurfaceIR | implemented in source registry; several legacy screens and deployed-parity paths remain incomplete |
| Full Aurekai pattern-to-promotion loop | architectural |

This table is a convenience view. The Atlas entry and its witnesses are the
source of truth.

## Invariants

- Reachability and a rendered affordance never auto-commit past the human or
  authority line.
- Candidate state is not current truth; current truth is not immutable history.
- A generated plan is not an authorized effect.
- A receipt is evidence of an occurrence, not universal proof of correctness.
- A Frappe record or external profile does not replace its Bonfÿre semantic
  owner.
- A learned pattern is not a promoted primitive.
- Optimization may change realization cost only inside a proven validity
  envelope; otherwise the machine thaws to a proven ancestor.

## Read and run next

- [Local vGPU data engines, YAFF/CUE and PCSS/Tailwind/authoring frontend](native-data-compute-and-presentation.md)

- [Concrete operational workflows, formats, MCP and external platforms](real-workflows-and-interoperability.md)
- [Unified authoring, editor and frontend source paths](authoring-frontend-architecture.md)

- [Architecture Atlas](../architecture/README.md)
- [Quickstart](../QUICKSTART.md)
- [Status and drift](bonfyre_status_and_drift.md)
- [Pages runtime](pages-runtime.md)
- [Client surfaces](client-surfaces.md)
- [Ontology surfaces](ontology_surfaces.md)
