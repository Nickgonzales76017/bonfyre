<p align="center">
  <img src="https://github.com/user-attachments/assets/7785a8d6-6e28-4892-8958-3daf85414f05" alt="Bonfÿre knight" width="520" />
</p>
<h1 align="center">Bonfÿre</h1>

<p align="center">
  <strong>The native software behind Aurekai’s businesses, applications and public network.</strong><br><small>Built on software. Driven by Aurekai.</small>
</p>

<p align="center">
  <a href="#what-the-shared-software-makes-possible">What it does</a> ·
  <a href="#building-blocks">Building blocks</a> ·
  <a href="#federated-workspace">Federated workspace</a> ·
  <a href="https://nickgonzales76017.github.io/bonfyre/modules/">Source libraries (70)</a> ·
  <a href="#start-here">Start here</a> ·
  <a href="#architecture-in-pictures">Visual architecture</a> ·
  <a href="#system-architecture">Architecture</a> ·
  <a href="docs/client-operations-portfolio.md">Client work</a> ·
  <a href="#whats-available-today">Available today</a> ·
  <a href="https://docs.aurekai.work/">Guides</a> ·
  <a href="https://aurekai-work-samples.pages.dev/">Work samples</a> ·
  <a href="https://nickgonzales76017.github.io/bonfyre/about/">About Aurekai</a> ·
  <a href="https://nickgonzales76017.github.io/bonfyre/market.html">Market research</a> ·
  <a href="docs/architecture.md">Architecture</a> ·
  <a href="CONTRIBUTING.md">Contributing</a>
</p>

**Bonfÿre is the software behind Aurekai.** We use it to build websites and applications, work with existing business systems, run local models, connect outside data and coordinate work across the products Aurekai operates.

Aurekai is one operating business that can support many different experiences. A shop can have its own brand and customers. A creator can have a profile, channel and publications. An organization can use CRM, finance, project and service tools. Bonfÿre supplies many of the common pieces while each business keeps the parts that belong to it.

This repository is the technical side of that work. Its native implementation covers Matrix identity and addressing, Source Library, data systems, business application schemas, WorkGraph, agents, Model Commons, media, commerce and public web composition. The public GitHub tree and the locally installed runtime are published on separate schedules.

## Two engineering paths that meet in the same product

**Bonfÿre compiles native experiences and processes substantial data locally.** The useful result is an authored page, an interactive research table, an account application, a source-backed publication, a reviewed document or a real handoff into the next tool. This rests on **distinct implementations**, not an assertion that everything goes into Matrix.

**[Native computation + presentation: YAFF, CUE, PostCSS, Puck, Embed, Vec, vGPU, FPQ and BitNet](docs/native-data-compute-and-presentation.md)** · [Real end-to-end workflows](docs/real-workflows-and-interoperability.md) · [MCP tools](integrations/mcp/README.md) · [WordPress transfer](integrations/wordpress/README.md)

~~~mermaid
flowchart TB
  YAFF["YAFF architecture and interactions"] -. "Constraint and evidence rules" .-> WEB
  CUE["CUE checks and strict Zig/C generated lowerers"]
  CUE --> EDIT["Typed authoring tree: Puck / GrapesJS / Editor.js"]
  EDIT --> WEB["MD4C + Mustach + native WebAddress output"]
  CSS[".pcss / PostCSS / Tailwind / closed design tokens"] --> WEB
  SOURCE["SQLite / Parquet / articles / documents / source IDs"] --> EMBED["BonfyreEmbed ONNX + BonfyreVec local search"]
  SOURCE --> VGPU["vGPU tiled numeric kernels and local derived tables"]
  EMBED --> DATA["Exact or candidate related rows with digests"]
  VGPU --> DATA
  DATA --> EXPERIENCE["Browser table, investigation, editorial or installed account app"]
  WEB --> EXPERIENCE
  BIT["BitNet: held learned rank of eligible candidates"] -. "Only passported admissible work" .-> DATA
  FPQ["FPQ: model-weight compression trials"] -. "Only after full-model parity" .-> EMBED
~~~

**One innovation is in the native frontend and application pipeline.** [YAFF](architecture/README.md) retains real architectures, invariants, interactions and gaps. [CUE page composition](schemas/control/page-composition.cue) and [strict Zig lowerers](cmd/BonfyreWebAddress/tools/compile_site_application_lowerers.zig) emit typed native readers/structural operations. The local-only `cmd/BonfyreSurfaceScene/runtime/authoring-tree.mjs` carries nested sections and responsive, closed style roles across Puck, GrapesJS and Editor.js; native C/MD4C/Mustach gives it a public website and article output. The **new local** `.pcss` + PostCSS/Tailwind build compiles shared semantic controls and tokenized presentation over C-emitted markup and browser workspace styles. As of this source cut the local `deploy/tailwind/input.pcss` and build package have **not yet been synchronized to public `main`**; [Tailwind's C-class scanning config](deploy/tailwind/tailwind.config.js) is present publicly.

**A second innovation is dense local computation feeding real interfaces.** [BonfyreEmbed](cmd/BonfyreEmbed/src/main.c) computes actual pinned-ONNX text embeddings; [BonfyreVec](cmd/BonfyreVec/src/main.c) stores/searches them with SQLite and exact vector comparison. The [vGPU kin kernel](cmd/BonfyreSurfaceScene/runtime/kin-compute-vgpu.mjs) runs tiled pairwise similarities over numeric article/source charts, then materializes selected related rows for articles and research tools. Its separate [spatial-neighborhood kernel](cmd/BonfyreSurfaceScene/runtime/stage-compute-vgpu.mjs) computes distances between placed observations. A recorded local 6,810-item run compared **46,376,100 pairs in 1.1 seconds** and retained 710 above its 0.85 similarity floor. The [Field](site/matrix/field.html) also used **DuckDB-Wasm to read 109,761 Parquet channel rows inside a browser tab**, with WebGPU visualizing the dataset. **This does not imply DuckDB SQL is executed by vGPU.** The engines and data routes are separate.

[BitNet](cmd/BonfyreBitNet/src/main.c) is an optional model-passported reorder of *already eligible* AddressPlane candidates; its tested frontier has SHADOW/quality gaps. [FPQ](cmd/BonfyreFPQ/src/fpq_native.c) measures compact neural weight representations. [The actual Qwen tensor trial](cmd/BonfyreFPQ/results/2026-09-11-qwen2.5-coder-0.5b-real-tensor-v9.json) achieved 0.999935 reconstruction cosine at 5.75 bits/weight on one tensor, **without proving generation parity**. These downstream optimizations can become useful only through actual preservation and task-quality trials. They do not define the reliability of the existing local data and editorial systems.

### What already happens, and what would make it a complete experience

| Person's task | Source and real computation | Useful next step |
|---|---|---|
| Read or investigate many publishers | DuckDB-Wasm/Parquet source catalogue, retained source IDs, vGPU kin similarities, source checks | searchable table → exact publication → editor → article or research document |
| Publish a composed journal | CMS `site_page` and nested `site_fragment`, Puck/GrapesJS/Editor.js, MD4C/Mustach/C, PCSS/tokens | source-bound page → reviewed publication → website/narration/export |
| Find related passages in thousands of documents | ONNX `bonfyre-embed` and SQLite `bonfyre-vec` with model/width identity | inspect original passage → cite it → reviewed editorial or app continuation |
| Study current and historical places | Observation time/coordinates, vGPU spatial top-k, persisted neighborhood table | bounded geo/temporal reading and map; no invented place or attendance relation |
| Work across installed apps | account-owned records, Project/Workspace/Workbook MCP, source and revision checks | source-linked CRM, workbook, procurement, PDF or creator output under its own grant |
| Reduce cost downstream | exact mask and owned material → optional BitNet ranking → admitted model representation/FPQ after parity | less work/less model residency only when receipts and benchmarks actually show it |

The quality bar is a **real interactive screen with material, controls and output**, not a generic search/card dashboard. Source-size analytics, viewport rendering, revisioned editing, publication scope and a GPU/CPU receipt each need their own acceptance.

## Why a shared experience compiler matters

An Aurekai experience is **not a screenshot, a collection of disconnected UI blocks, or a website template**. Its actual meaning is retained by a person, community, publisher, institution, source, place or application owner. Bonfÿre makes that meaning available to different authorized editors, readers, applications and delivery channels without silently copying identities or inventing capabilities.

The [unified authoring and frontend architecture](docs/authoring-frontend-architecture.md) covers Puck (structure), GrapesJS (visual composition), Editor.js (rich writing), MD4C (native Markdown), Mustach (compiled C templates), a shared tree, CMS revision controls, responsive tokens, published SurfaceIR/PageComposition, interaction grammar and browser-native behavior. The 27 ServiceSite section owners, compiled Frappe forms, 91 studied Stitch references and 14 reusable experience patterns point toward **one native authoring language across real kinds of software**, not a seven-block demo. Implementation witnesses and remaining production gaps are linked in the guide.

That is important beyond selling customer sites: a community can author participation surfaces; a researcher can combine sources, narrative and a time-aware map; a publisher can compose longform journalism and optional audio; a learner can create a project notebook; an organization can use its actual typed forms; a person can publish multiple complete sites. **The novel proposition is their shared typed and governed lifecycle**, not the claim that any single underlying open-source editor or visual library is new.

## What the shared software makes possible

**Independent customer sites.** Our [three-business example](https://docs.aurekai.work/three-businesses) shows how a retailer can get its own website, branding, content and tools while an agency manages several clients using shared infrastructure. That includes separate accounts and customer access, not just changing a page's colors.

**Business applications that talk to one another.** The Frappe-derived app work connects CRM, ERPNext, HRMS, Helpdesk, LMS, Wiki, Drive and Insights schemas to native workspaces. A company can work with relationships, finance, staff and service records without creating a separate integration system for every pair of applications.

**Information that has more than one use.** A job listing can be discovered publicly, continued into an account's CRM and used to prepare a narrated update. A publisher's content can belong to a profile and also appear in a collection or channel. The source stays identifiable while receiving applications manage their own work. These cross-application paths are ongoing product and integration work.

**Geography, spatial evidence and products.** [BonfyreSpatial](cmd/BonfyreSpatial/) and [spatial observation contracts](schemas/control/spatial-observation.cue) cover grounded location, temporal reconstruction and spatial queries across many contexts. One customer offer used those foundations for a [room planner](https://nickgonzales76017.github.io/bonfyre/spaces/product-room/) and [product configuration workspace](https://nickgonzales76017.github.io/bonfyre/spaces/catalogue/); neither defines the broader spatial architecture.

**People, groups and connected accounts.** Profiles, memberships, communities, institutions and account-owned apps have distinct identity, audience and permission contracts. [Connection Studio](integrations/browser/connection-studio.mjs), [ProviderEstate](cmd/BonfyreProviderEstate/) and [RepositoryFabric](cmd/BonfyreRepositoryFabric/) connect sources, installations, provider tenants, Git hosting and native workflows. A listed provider is not necessarily an authenticated or writable connection.

**Source and model infrastructure.** [AddressPlane](cmd/BonfyreWebAddress/src/subject_address.c) excludes impossible candidates through exact declared masks before costly work; [BitNet](cmd/BonfyreBitNet/) may diagnostically reorder survivors, while [FPQ](cmd/BonfyreFPQ/) and [QwenFPQ](cmd/BonfyreQwenFPQ/) explore more efficient weight representations. Their measurements and DEFER gates are recorded separately from production eligibility; they are not claimed as complete compressed-model parity.

**Native tools and computation.** The Bonfÿre codebase includes C/Zig-centered commands for data, models, media, workflows, finance and integrations, alongside model runtimes, agents and external backends. A developer can inspect the [technical guide](https://nickgonzales76017.github.io/bonfyre/docs.html) or start with an individual tool.

The larger business model is explained in [Aurekai and Bonfÿre](docs/aurekai-bonfyre-model.md), including how software, engineering time, external resources and distribution become reusable assets across the company.

## Why we're working in these markets

The organizations Aurekai works with already depend on many systems. In the <a href="https://www.uschamber.com/technology/2026-empowering-small-business-report">U.S. Chamber's 2026 small-business survey</a>, 41% of respondents used six or more technology platforms. Our native business-app and integration work helps with that everyday complexity: CRM, finance, inventory, content and operating tools need to work together.

There's also a large base of established websites and published work to build on. <a href="https://w3techs.com/technologies/comparison/cm-wordpress">W3Techs reported WordPress usage at 39.4% of measured websites on November 7, 2026</a>. For a company with years of content, forms, reports and customer history, modernization means carrying that business forward.

For creators, the <a href="https://www.iab.com/blog/2026-iab-creatorfronts-event-recap/">IAB's September 2026 CreatorFronts report</a> projected roughly $44 billion in U.S. creator advertising for 2026 and called for better operational infrastructure around creative work. For AI, <a href="https://hai.stanford.edu/ai-index/2026-ai-index-report/economy">Stanford's 2026 AI Index</a> reported adoption by 88% of surveyed organizations, while deployment of AI agents in particular business functions was still early. Those are useful reasons to build creator tools and native application/agent execution as part of a broader operating platform.

The [market research guide](https://nickgonzales76017.github.io/bonfyre/market.html) links the original publications, dates and specific Aurekai product areas. These are external market indicators, separate from our product and operating results.

## From the journal

The [Aurekai Journal](https://nickgonzales76017.github.io/bonfyre/blog/) now contains **46 published articles**: the original sixteen project essays plus thirty externally grounded investigations, mathematical explainers and responses to specific X posts. They follow the questions behind the software—[building the business around an AI-generated app](https://nickgonzales76017.github.io/bonfyre/blog/why-ai-needs-an-operating-company/), [moving a source into several account applications](https://nickgonzales76017.github.io/bonfyre/blog/a-job-posting-has-more-than-one-job/), [Frappe's application grammars](https://nickgonzales76017.github.io/bonfyre/blog/frappe-as-an-application-language/), and [local model performance](https://nickgonzales76017.github.io/bonfyre/blog/the-memory-wall-in-local-models/).

The editable Markdown lives in [`editorial/posts/`](editorial/posts/). The Pages pipeline uses the dependency-free [`scripts/render-editorial.sh`](scripts/render-editorial.sh) to build the articles, [RSS feed](https://nickgonzales76017.github.io/bonfyre/blog/feed.xml), and stable [post index](editorial/README.md). The public URLs are designed to stay the same when native CMS integration replaces the static builder.

## Aurekai's public network

| Destination | What people find there |
|---|---|
| [aurekai.work](https://aurekai.work) | Businesses, organizations, jobs, professional applications and opportunities |
| [aurekai.life](https://aurekai.life) | People, communities, profiles, collections and publishing |
| [aurekai.live](https://aurekai.live) | Creators, media, recordings, channels and public experiences |
| [aurekai.shop](https://aurekai.shop) | Products, sourcing, offers, catalogues and commerce |

The domains are places to enter Aurekai; businesses and creators can also have independent sites and subdomains. Browse the [product guide](https://nickgonzales76017.github.io/bonfyre/explore.html) for a fuller look at how these systems fit together.

## Use a smaller piece

You can also start without building the whole system:

| Independently scoped source | The job it does | What to inspect |
|---|---|---|
| **[Statement Import](https://github.com/Nickgonzales76017/bonfyre-statement-import)** | Convert normalized bank-statement text into CSV and refuse a result when running balances fail to reconcile | [C source and pack fixtures](https://github.com/Nickgonzales76017/bonfyre-statement-import) |
| **[Agent Trace Verifier](https://github.com/Nickgonzales76017/bonfyre-agent-trace-verifier)** | Evaluate agent-execution evidence through a deterministic, source-addressed receipt with linear-time duplicate-event validation | [Standalone verifier and examples](https://github.com/Nickgonzales76017/bonfyre-agent-trace-verifier) |

Those tools have their own repositories and licenses. For more involved deployments, [the client operations portfolio](docs/client-operations-portfolio.md) shows how local software and public applications fit WordPress, ERPNext, source documents, creator publishing, agency work and commerce. The [operating-products guide](https://docs.aurekai.work/operating-products) describes broader customer and partner applications.

## Federated workspace

The integrated native engine stays here. **69 public companion repositories are now
pinned as real Git submodules**, alongside the existing HVM4 dependency. They
are grouped under [`integrations/federated/`](integrations/federated/) and
[`examples/federated/`](examples/federated/), not added as 69 new root
directories.

**[Browse the source libraries →](https://nickgonzales76017.github.io/bonfyre/modules/)**
Every entry links to its owning GitHub repository and its exact pinned commit,
and identifies whether it is a standalone tool, an older source extraction,
an integration donor, or historical lineage. Gitlinks reference **other**
repositories; GitHub does not index their files as code within this parent
repository. Search within the linked owner repository when you need its source.

| Profile | Modules | What lives there |
|---|---:|---|
| `products` | 2 | Independent Statement Import and Agent Trace Verifier |
| `extractions` | 7 | Earlier native library/CLI extractions, including Lambda Tensors |
| `examples` | 7 | Demonstrations, creator scaffold and the real recipe collection |
| `aurekai` | 12 | The earlier Aurekai platform, native runtime, CI, IDE, data and workflow adapters |
| `frappe` | 9 | Pinned source of Frappe, ERPNext, LMS, HRMS, CRM, Helpdesk, Insights, Wiki and archived Drive |
| `interfaces` | 8 | OpenTUI, MCP Apps, A2UI, json-render, Bubble Tea, Lip Gloss, Ratatui and Ghost design/source references |
| `performance` | 22 | Optional benchmark/reference donors: memory/Arrow, search, incremental engines, Linux edge I/O, model serving |
| `lineage` | 2 | Historical bonfyre-oss and HCP work, deliberately not active runtime replacements |
| `upstream` | 1 | Existing independently maintained HVM4 |

A normal clone remains small and builds without these optional groups. To get
only the separately usable tools:

```bash
git clone https://github.com/Nickgonzales76017/bonfyre.git
cd bonfyre
bash scripts/federated_modules.sh list
bash scripts/federated_modules.sh init products
bash scripts/federated_modules.sh status products
```

Use `init extractions`, `init examples`, or `init aurekai` only for the work
you need. For one specific donor, use `init one integrations/federated/interfaces/opentui`.
The `frappe`, `interfaces`, `performance`, and `all` groups can pull **gigabytes** of source
history and require `BONFYRE_ALLOW_LARGE_MODULES=1` before initializing.
Neither the optional donors nor historical lineage is cloned by default. The script verifies the exact gitlink
commit and public URL before fetching, **never advances to a remote branch
without review**, and recurses only into the reviewed native extraction dependencies.

For any proposed optimization, the [read-only performance equivalence reviewer](scripts/performance_equivalence.mjs) compares source digests, original functionality, exact final outcomes, authorization scope, observed resource effects, priced-provider costs and rollback evidence. A pass requires zero observed false prunes on the trial, exact final-result equality, and independent evidence; it means **eligible for human promotion review**, never deployed, and missing evidence retains the exact baseline. The optional [native physical receipt readback](scripts/performance_physical_receipts.mjs) verifies persisted BonfyreCompress CopyReceipt counters against a read-only SQLite registry (Node 22+), refuses missing records or unequal useful work, and then applies the existing review gate. This readback is **not** owner authority or proof that the upstream counter instrumentation is independently accurate. Owner receipts must still be independently read back; donor benchmarks are not production evidence.

The 22 performance donors are *reference implementations* only: no automatic linking, runtime replacement, no copying third-party assets or new quantization claims. Some are large Rust/Python/C++ systems and some use platform-specific interfaces; `init one integrations/federated/performance/memory/nanoarrow` fetches only one exact pin. Promotion requires compatible licenses, independently measured behavior, full feature parity and a fallback route.

The Frappe pins are source references, **not a tested nine-app compatible bench**;
`frappe/drive` is archived upstream. The interface repos are source/design
references, **not bundled components or an implicit right to reuse assets**.

The CMS, Intake, Pipeline and Embed extraction repos now use pinned
`bonfyre-core` dependencies rather than copying `libbonfyre`; CMS also pins
Lambda Tensors. `init extractions` initializes these small nested dependencies,
while larger donor and historical lineages do not recurse.

These are *references to independently owned source histories*, not a second
owner for Bonfÿre facts or an assertion that old extracted code matches the
live `cmd/` implementation. No private client repository is included.
Review the license in each companion repo separately. The three unusable
archived gitlink coordinates remain documented in their evidence history
rather than blocking normal submodule initialization.

## Source entry points — actual code owners

These are not interchangeable modules behind a single generic call. The source file or command linked here is where the **specific format, stored record, action or output behavior** lives.

| Work | Actual owner | Preserved output or boundary |
|---|---|---|
| WordPress source changes | [signed bridge](integrations/wordpress/bridge.mjs) / [plugin](integrations/wordpress/plugin/aurekai-bridge.php) | HMAC, source site/post/version, idempotent native CMS entry |
| Site export / content transfer | [site product](cmd/BonfyreWebAddress/src/site_product.c) | offline HTML/CSS, WordPress WXR 1.2, Frappe documents |
| Legacy PHP hosting inventory | [BonfyreBrief](cmd/BonfyreBrief/src/inventory.c) | cPanel/InterWorx/Plesk counts, discrepancy exceptions, secret refusal |
| Database content verification | [BonfyreBridge](cmd/BonfyreBridge/src/main.c) | row/value/type comparison; not a remote cutover |
| XLSX, accounting review | [Books XLSX](cmd/BonfyreDecide/src/books_xlsx.c), [workbook MCP](integrations/mcp/workbook_mcp.mjs) | source sheet/cell coordinates, XLSX review, current account revision |
| Paper and PDF recognition | [BonfyreOCR](cmd/BonfyreOCR/src/main.swift) | line/page/bounding box/confidence and recognized text |
| Outbound documents | [BonfyreEmit](cmd/BonfyreEmit/src/main.c), [PandocFormatter](components/PandocFormatter/README.md) | format-dependent HTML/PDF/EPUB/TXT/RSS/DOCX |
| Frontend and authoring | [WebAddress](cmd/BonfyreWebAddress/), [SurfaceScene](cmd/BonfyreSurfaceScene/) | Puck/GrapesJS/Editor.js local editor grammar; C/MD4C native HTML and typed interactions |
| Business app surfaces | [Frappe compiler](cmd/BonfyreFrappeCompiler/) | real DocType fields/layout, independently governed record effects |
| Project code and assistants | [Project MCP](integrations/mcp/project_node.mjs), [RepositoryFabric](cmd/BonfyreRepositoryFabric/src/main.c) | schema-bound tools, Git/Forgejo/Josh source, source patches/tests/receipts |
| Installed account editors | [Workspace MCP](integrations/mcp/workspace_mcp.mjs) | same saved product record, expected revision/digest and readback |
| Provider/account enrollment | [Onboarding](integrations/browser/onboarding/README.md), [ProviderEstate](cmd/BonfyreProviderEstate/) | candidate stages, credentials, destination scope and individual remote verification |
| Community and group participation | [community body](cmd/BonfyreCMS/content-types/community_body.json) | membership, moderation, published conversation and expiry |
| Creator audio/podcasting | [account audio](cmd/BonfyreCMS/src/account_world_audio.c) | station/show schedule, transcripts, sourced episodes and playback controls |
| Spatial/temporal evidence | [spatial schema](schemas/control/spatial-observation.cue), [BonfyreSpatial](cmd/BonfyreSpatial/) | observed/effective/recorded/expiry times, indexed queries |
| Local inference/compression | [BonfyreBitNet](cmd/BonfyreBitNet/), [BonfyreFPQ](cmd/BonfyreFPQ/), [BonfyreModel](cmd/BonfyreModel/) | candidate ranking, measured representation, model-specific admission; experimental paths remain held |
| Local semantic retrieval | [BonfyreEmbed](cmd/BonfyreEmbed/src/main.c), [BonfyreVec](cmd/BonfyreVec/src/main.c) | ONNX embeddings, SQLite index, exact comparison and provenance; model/width are part of identity |
| Large data computation | [vGPU kin](cmd/BonfyreSurfaceScene/runtime/kin-compute-vgpu.mjs), [vGPU neighborhood](cmd/BonfyreSurfaceScene/runtime/stage-compute-vgpu.mjs), [field](site/matrix/field.html) | tiled GPU/CPU comparisons, persisted derived rows, browser DuckDB-Wasm/Parquet field |
| Source/frontend compilers | [YAFF Atlas](architecture/README.md), [CUE](schemas/control/page-composition.cue), [Tailwind config](deploy/tailwind/tailwind.config.js) | independently validated contracts, native app lowering and tokenized style build; new PCSS input remains local-only |

The [real-workflow guide](docs/real-workflows-and-interoperability.md) names the **input and output contracts, test, source path, missing authority and external acceptance**. The [architecture registry](architecture/README.md) supplies additional internal algorithms and relationships, but the registry's counts are not a replacement for the source-specific explanation.

## How independently owned systems connect

**An address is not a compiler.** Matrix is responsible for semantic subject addressing, presence and institution/territory scopes when an interaction needs them. It does not read XLSX, issue a WordPress webhook, perform OCR, generate a React bundle, change a CRM record, authenticate Spotify or post a payment.

**A work planner is not a document parser.** WorkGraph coordinates actual missions, dependencies, leases and completion where that behavior is needed. BonfyreBooks can inspect a local spreadsheet or BonfyreEmit can render an existing document without first making a WorkGraph claim. WorkGraph still matters for long-running migrations, multi-owner reviews, releases and accountable agents.

**An MCP surface is not an independent data owner.** Project MCP exposes native source operations/resources; WebAddress site MCP serves a particular site's audience; Workspace MCP edits the *existing product record* under its current account grant. All three need different schemas, receivers and effects.

**A web component is not the business record.** A Puck/React or GrapesJS editor can reorganize an addressable Frappe form or CMS article while the source application retains the record and authorization. MD4C/Mustach/C rendering, HTMX fragments and browser interactions are presentation paths. Svelte/React/PHP full source-conversion parity remains a different, unproven task.

~~~mermaid
flowchart TB
  DOC["XLSX, PDF, document source"] --> FORMAT["Books, OCR, Emit: format-specific outputs"]
  CMS["WordPress signed event / CMS content"] --> PUBLISH["CMS, review, WebAddress and WXR/Frappe export"]
  DEV["Git / Forgejo / Josh"] --> PATCH["Project tools, tests, committed source"]
  ACC["Installed workspace record"] --> EDIT["Workspace/Workbook MCP exact revision"]
  SERVICE["Provider/account candidate"] --> GATE["Provider auth and current-effect verification"]
  COMPOSE["Puck/GrapesJS/Editor.js"] --> NATIVE["C / MD4C native frontend"]
  PUBLISH --> NATIVE
  PATCH --> RELEASE["Staged, verified deployment"]
  NATIVE --> RELEASE
  FORMAT --> CONSUME["Distinct receiving person or application"]
  EDIT --> CONSUME
  GATE -. "Only if proven" .-> CONSUME
~~~

### Native control systems — only where the work requires them

| Control owner | Specific responsibility | What it does not replace |
|---|---|---|
| **Matrix addresses** | resolve a site/subject/presence/audience | source file formats, document renderers, OAuth, model execution |
| **WorkGraph** | dependent missions, claims, leases, cooling and completion | ordinary local parsing, one-shot format export or CMS structure |
| **CapabilityClosure** | verify that an intended *work* operation has a realizable, admissible execution | a full taxonomy of every user-visible application or industry |
| **Authority / EffectKernel** | final operation-specific grants and idempotency | supplier confirmation, financial settlement or source truth |
| **Contract Projection Kernel v2** | demand-bound typed ABI/data/API projections for supported native closures | all WordPress/PDF/XLSX/reactive browser formats or arbitrary external apps |
| **Evidence / replay** | receipts, witnesses, divergence, replay and held gaps | live publication or proof that a receiving provider accepted an export |

### Capability programs

The Generation-10 architecture preserves **91 Power identities**. Each identity moves through a
maturity ladder—defined, implemented, built, resolvable, health-probed,
workload-proven, quality-proven, promoted—so discovery never becomes a false
claim of callability. Native commands, libraries, local models, agent carriers,
distributed providers, and humans are realizations selected under capability,
cost, resource, risk, and authority constraints.

### Nine institutional product lineages

The Frappe estate is nine connected projections over shared Bonfÿre meaning:

- **Frappe/Core** — DocTypes, forms, permissions, workflows, and reports
- **ERPNext** — accounting, procurement, inventory, and realized value
- **LMS** — learning, assessment, recipes, and credentials
- **HRMS** — human/agent capacity, roles, skills, and availability
- **CRM** — relationships, communications, opportunities, and deal state
- **Helpdesk** — incidents, disputes, missing evidence, and side work
- **Insights** — derived-current operational, cost, risk, and divergence views
- **Wiki** — policy, contract, knowledge, and institutional memory
- **Drive** — artifacts, receipts, evidence, fixtures, packs, and source material

These products donate mature grammars. They do not replace Authority,
WorkGraph, EvidenceGraph, or the shared semantic owners.

### Model Commons and agentic execution

Model Commons preserves distinct model, tokenizer, quantization, inference,
context, retrieval, reasoning, movement, and evaluation specialties. FPQ/FPQx,
QwenFPQ, GGUF, llama.cpp, BitNet, ONNX Runtime, Burn, CubeCL, Metal, CPU, Wasm,
Embed, Vec, SAE, GigaToken, KV passports, and local speech all keep their own
contracts.

An agent run is a routed execution circuit:

```text
WorkNode → AgentProfile → AgentSession → capability/effect frontier
   ├─ native Power or deterministic local tool
   ├─ local model
   ├─ Claude Code / Codex CLI / another bounded carrier
   ├─ distributed or external provider
   └─ human approval or work when genuinely irreducible
                         ↓
             receipt → divergence → evidence → Aurekai
```

Claude and Codex are carrier profiles. They do not own Bonfÿre semantics.

### Partner Commons, operations, and finance

Partner Commons preserves **112 frozen Generation-10 profiles** for external
institutions, tools, protocols, providers, hosts, and distribution systems.
Profiles connect through ForeignTwin, ProviderGraph, Capability, WorkGraph,
SurfaceIR, and evidence boundaries; an integration never gains semantic or
institutional authority merely because it is reachable.

WorkGraph and ResourceGraph track provider readiness, prices, credits, quotas,
freshness, schedules, external tools, artifact packets and worker dependencies.
This is the system's operational metabolism.

Financial metabolism tracks agreements, billing accounts, spend intents, funding
sources, payment instruments, authorization, capture, settlement, refunds,
allocation and ERPNext accounting separately. It keeps quoted prices apart from
payments, provider credits apart from cash, and dashboards apart from the
accounting records they display.

## What's available today

The November 7, 2026 installed native estate and architecture registry report:

| System | Inventory |
|---|---:|
| Native command families | **93** |
| Frappe-derived application schemas | **1,840 DocTypes / 51 modules** |
| WorkGraph | **20,487 nodes / 45,412 edges** |
| Active backend adapters | **14** |
| Architecture Atlas | **556 entries: 346 implemented, 152 measured, 24 declared, 34 architectural** |

These are counts for the installed V6.1.0 estate and architecture registry; the public GitHub source and individual Aurekai deployments may use different revisions. The component Atlas and release workflows track the corresponding implementation states.

Published source currently sits under [`cmd/`](cmd/), [`engine/`](engine/),
[`lib/`](lib/) and [`integrations/`](integrations/).
Historical notebook documents live under
[`evidence/origins/notebook-era/`](evidence/origins/notebook-era/),
not in competing root folders.

## Start here

Customers and agencies can explore the [work samples](https://aurekai-work-samples.pages.dev/)
and the [operating-products guide](https://docs.aurekai.work/operating-products).
To inspect the current public source snapshot:

```bash
git clone https://github.com/Nickgonzales76017/bonfyre.git
cd bonfyre
```

Follow individual component requirements rather than assuming all the
development estate is installable from `main`.

## Read next

- [Client delivery and integration portfolio](docs/client-operations-portfolio.md)
- [Guides](https://docs.aurekai.work/)
- [Work samples](https://aurekai-work-samples.pages.dev/)
- [Architecture overview](docs/architecture.md)
- [Quickstart](QUICKSTART.md)
- [Contributing](CONTRIBUTING.md)

## License

Covered Bonfÿre source is available under the
[PolyForm Shield License 1.0.0](LICENSE). Separately licensed components and
previously distributed versions retain their applicable terms.
