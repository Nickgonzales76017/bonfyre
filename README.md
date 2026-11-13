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

## What Bonfÿre actually does — source, work, software, and delivery

Bonfÿre is **not** a central “capability matrix” through which every product passes. A WordPress plugin does not behave like a customer database; a paper form does not behave like a creator channel; an XLSX review does not behave like a website; and an MCP tool is not an independent business application. **The native tools preserve those differences while making deliberate, testable connections between them.**

**[Real workflows and exports](docs/real-workflows-and-interoperability.md)** · **[MCP operation guide](integrations/mcp/README.md)** · **[200-service and 104-provider admission guide](integrations/browser/onboarding/README.md)** · **[WordPress exchange guide](integrations/wordpress/README.md)** · [Unified editor/frontend](docs/authoring-frontend-architecture.md) · [Source-checked Atlas](architecture/README.md) · [Full system map](docs/architecture-visuals/system-architecture.md)

~~~mermaid
flowchart TB
  WORDPRESS["WordPress REST + signed plugin"] --> WP["Normalized events + native CMS sync"]
  PANELS["cPanel / InterWorx / Plesk exports"] --> BRIEF["Cited hosting inventory + migration exceptions"]
  BOOK["Bank / Excel XLSX"] --> WORKBOOK["Books review + account workbook revisions"]
  PAPER["Scanned paper / PDF"] --> OCR["On-device OCR + page coordinates"]
  AUTHOR["Puck + GrapesJS + Editor.js"] --> SITE["Native CMS pages + C/MD4C/Mustach rendering"]
  CHAT["ChatGPT / Codex / Claude clients"] --> MCP["Scoped Project MCP + revision-checked Workspace MCP"]
  CREATORS["YouTube / Patreon / Spotify"] --> CONNECT["Public account links / provider-auth plans / owner-bound grammar"]
  SITE --> EXPORT["Standalone HTML / WordPress WXR / Frappe records"]
  OCR & WORKBOOK & BRIEF --> REVIEW["Source-grounded review / document / evidence"]
  REVIEW --> EMIT["HTML5 / PDF / EPUB3 / TXT / DOCX via formatter"]
  MCP --> SITE
  MCP --> WORKBOOK
  WP --> SITE
  CONNECT -. "Only with verified authorization" .-> SITE
~~~

### Real operations with real outputs

| Starting point | Native operation and preserved detail | What a person receives |
|---|---|---|
| **WordPress blog or legacy CMS** | HMAC-SHA256 signed event; `site + post ID + version` as idempotency key; public REST-route discovery; external-ID CMS sync | Draft/source entry that can be inspected without silently publishing back to WordPress |
| **Existing native site** | `bonfyre site export SITE --format wordpress|frappe`; `--out` standalone HTML with local CSS inlined | WXR 1.2 XML, Frappe document exchange, or an offline script-free HTML copy |
| **30-site legacy agency portfolio** | `bonfyre-brief inventory` reads authorized cPanel/InterWorx/Plesk exported listings; detects mismatches and avoids secret fields | Cited inventory and migration questions, not a false “migration complete” state |
| **Spreadsheet/accounting review** | `bonfyre books inspect --workbook file.xlsx`; native XLSX read with sheet/cell identity; `workbook_review`, `workbook_preview` and `workbook_update` | New review workbook and the *same* signed-in account draft, subject to revision and permission checks |
| **Paper/scan/image** | `bonfyre-ocr lines file.pdf` uses on-device Vision/PDFKit and returns page, box, confidence and text | Text and page positions for review; never presents OCR confidence as proof |
| **Report/letter/publication** | `bonfyre-emit` and PandocFormatter produce targeted files via actual format conversion | HTML, PDF, EPUB, plain text, RSS or DOCX where the formatter/dependencies support it |
| **Puck, GrapesJS, Editor.js** | One nested authoring tree retains IDs, sections, responsive roles, source/binding metadata through editors and native CMS | Editable rich experience rendered through the C/MD4C/Mustach + WebAddress path |
| **Project or account automation** | Project MCP tools/resources/prompts; `mcp_read_bundle`; `workspace_list/records/views/preview/update` with exact revision and readback | Real project operations or edits to existing installed-product records, not shadow copies |
| **Creator platform** | YouTube/Patreon public profile locator; Spotify/YouTube/Patreon provider-auth choices; 20 studied interaction grammars | A clearly labeled link, candidate connection or authorized use *only where the provider has been verified* |
| **Restaurant Toast POS** | Commercial target is listed; no verified Toast-specific native API owner | No claim of synced restaurant menus, orders, checkout or settlement |

**The gaps are as important as the outputs:** WordPress runtime deployment is not proven by bridge tests; WXR round-trip locally does not mean a remote WordPress accepted it; React is used in the editor but a general React/Svelte conversion compiler is not verified; Spotify/YouTube/Patreon connection choices do not establish OAuth tokens; Toast has no proved native connection. See each row's source/tests in the [workflow ledger](docs/real-workflows-and-interoperability.md).

### MCP is a family of precise access paths, not a monolithic AI assistant

[Project MCP](integrations/mcp/project_node.mjs) dispatches specific project and native owners through authenticated tools, resource URIs, prompts, provenance and measured toolset-schema size. `mcp_read_bundle` runs up to eight already-authorized **read-only** calls with schema checks and duplicate-read coalescing. [Account Workspace MCP](integrations/mcp/workspace_mcp.mjs) opens real installed-product records by binding/record/revision: its preview and update must recheck the same owner's permissions. [Workbook MCP](integrations/mcp/workbook_mcp.mjs) calls the financial workbook calculation/review owner rather than performing generic unchecked edits. An individual [WebAddress site](cmd/BonfyreWebAddress/tests/site_product_test.sh) can separately answer MCP on its own host and path.

This is why a universal “matrix” description is inaccurate: the work is in **per-product operations, transport constraints, record identity, source fidelity, review, guards and downstream readback**. Matrix is one address/presence/participation concern—not a replacement for those systems.

### Formats are outputs, not screenshots

The [site-product test](cmd/BonfyreWebAddress/tests/site_product_test.sh) verifies WordPress WXR and Frappe export/re-import text parity and offline HTML asset closure. [Books](cmd/BonfyreDecide/src/books_xlsx.c) preserves source XLSX sheet/cell positions for a review instead of converting it to generic cards. [BonfyreOCR](cmd/BonfyreOCR/src/main.swift) handles paper/PDF positional text locally. [BonfyreEmit](cmd/BonfyreEmit/src/main.c) and [PandocFormatter](components/PandocFormatter/README.md) generate deliverables with format-specific dependencies. These do not claim complete lossless conversion of all documents or framework applications.

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

## Building blocks

Bonfÿre spans core native systems and interoperable application families. The repository exposes source and integration boundaries for each:

| Area | What it does | Where it lives |
|---|---|---|
| Data | Local SQL and vector search, with the source of every record kept | [`cmd/BonfyreVec`](cmd/BonfyreVec/), [`cmd/BonfyreEmbed`](cmd/BonfyreEmbed/), [`cmd/BonfyreCMS`](cmd/BonfyreCMS/) |
| Contracts | Typed schemas and the code generated from them | `schemas/control` (in release review) |
| Models | Serving, quantization and embeddings | [`cmd/BonfyreQuant`](cmd/BonfyreQuant/), [`cmd/BonfyreModel`](cmd/BonfyreModel/) |
| Work | Requests, commitments and who may act on them | [`cmd/BonfyreQueue`](cmd/BonfyreQueue/), [`cmd/BonfyreAuth`](cmd/BonfyreAuth/), [`cmd/BonfyreWorkflow`](cmd/BonfyreWorkflow/) |
| Apps | Frappe and WordPress integration | [`integrations/`](integrations/) |
| Media | Preparing and transcribing recordings | [`cmd/BonfyreMediaPrep`](cmd/BonfyreMediaPrep/), [`cmd/BonfyreTranscribe`](cmd/BonfyreTranscribe/) |
| Commerce | Moving records between systems and checking them on arrival | `cmd/BonfyreBridge` (in release review) |
| Finance | Meters, invoices and recorded payments | [`cmd/BonfyreFinance`](cmd/BonfyreFinance/), [`cmd/BonfyrePay`](cmd/BonfyrePay/), [`cmd/BonfyreMeter`](cmd/BonfyreMeter/) |
| Web | Sites, pages and the assistant endpoint for each | `cmd/BonfyreWebAddress` (in release review), `components/BonfyreServiceSite` (in release review) |

The [building blocks guide](https://docs.aurekai.work/building-blocks) says
where to start with each, and the
[local-first guide](https://docs.aurekai.work/local-first-applications)
walks through the data area. Known limits are written down rather than hidden:
a vector store is made at one width (`bonfyre-vec init store.db --dims 768`),
and changing embedding model means making a new store.

## How it is put together

Underneath, the **Matrix** represents each subject once and connects its
addresses, public presence, relationships, institutions, state, economics,
compute, external participation and interfaces. Identity, authority,
capability, work, effects, evidence, resources and commitments each have a
separate owner.

Bonfÿre also supplies execution history, simulation and optimization capabilities for Aurekai's operating programs. These sit alongside the application, media, commerce, finance and public experience systems.

The Generation-10/V8.1 system map is the architectural basis. The architecture Atlas records component ownership, implementations, measurements and development work across the native estate.

## System architecture

```text
intent / observation / foreign state
                 │
                 ▼
 Subject · AddressConstellation · Presence · Authority
                 │
                 ▼
 Bonfÿre Matrix · CapabilityClosure · WorkGraph
                 │
       ProviderGraph · Context · Resources · MoneyGraph
                 │
                 ▼
 EffectKernel · AgentSession · native/local/external realization
                 │
                 ▼
 ReceiptEnvelope · WitnessDAG · EvidenceGraph
                 │
                 ▼
 replay · shadow · simulation · backtesting
                 │
                 ▼
       Aurekai: apps · networks · programs · commerce
```

Each part has a defined responsibility:

| Organ | What it owns |
|---|---|
| **Bonfÿre Matrix** | Stable semantic subjects, address constellations, publication presences, and recursively delegated institutional topology. |
| **WorkGraph** | Durable missions, dependencies, leases, cooling, and completion contracts. |
| **CapabilityClosure** | The gap between a named capability, a callable realization, and a workload-proven one. |
| **Authority + EffectKernel** | Who may do what, with idempotency and effect ceilings checked at execution time. |
| **Contract Projection Kernel v2** | ActiveClosure-demanded projections into native ABIs, data/query forms, APIs, WIT/Wasm, Frappe surfaces, evidence, evolution, tests, and packs. |
| **Evidence and replay** | Receipts, witness DAGs, trace divergence, proof bundles, deterministic replay, shadow execution, simulation, and institutional backtesting. |
| **Optimization & learning** | Discover improvements, run simulations, backtest, evaluate, and update reusable execution strategies. Aurekai is the operating business that consumes these capabilities. |

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
