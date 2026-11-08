<p align="center">
  <img src="https://github.com/user-attachments/assets/7785a8d6-6e28-4892-8958-3daf85414f05" alt="Bonfÿre knight" width="520" />
</p>
<h1 align="center">Bonfÿre</h1>

<p align="center">
  <strong>Native operating infrastructure for Aurekai: applications, identity, data, agents, models, finance and execution.</strong>
</p>

<p align="center">
  <a href="#aurekai-products-and-experiences">Aurekai products</a> ·
  <a href="#building-blocks">Building blocks</a> ·
  <a href="#federated-workspace">Federated workspace</a> ·
  <a href="https://nickgonzales76017.github.io/bonfyre/modules/">Source atlas (70)</a> ·
  <a href="#start-here">Start here</a> ·
  <a href="#the-whole-machine">Whole machine</a> ·
  <a href="docs/client-operations-portfolio.md">Client work</a> ·
  <a href="#whats-available-today">Available today</a> ·
  <a href="https://docs.aurekai.work/">Guides</a> ·
  <a href="https://aurekai-work-samples.pages.dev/">Work samples</a> ·
  <a href="docs/architecture.md">Architecture</a> ·
  <a href="CONTRIBUTING.md">Contributing</a>
</p>

**Bonfÿre is the native operating platform behind Aurekai.** It connects applications, sources, institutional identity, agents, models, finance, and execution into a reusable system.

**Aurekai** develops and operates digital products across [Work](https://aurekai.work), [Life](https://aurekai.life), [Live](https://aurekai.live), and [Shop](https://aurekai.shop). It invests engineering, compute, software, distribution and operating capacity into new products and opportunities. Bonfÿre is the shared technology that lets one project strengthen others.

| Aurekai business | Bonfÿre platform |
|---|---|
| People, organizations, communities, creators, marketplaces and businesses | Matrix identities, addresses, institutions, relationships and source graph |
| Account workspaces, commercial programs and applications | CRM/ERPNext/HRMS/Helpdesk/LMS/Insights/Wiki/Drive integration and native application composition |
| Publishing, media, product catalogues, commerce, spatial experiences | Media pipelines, source libraries, Model Commons, spatial and web systems |
| Investment of development capacity into digital opportunities | WorkGraph, capability composition, agent orchestration, native runtime, resource and finance systems |

The underlying shift is economic: AI can build software quickly, while conventional planning and fragmented organizational knowledge limit what actually reaches users. Aurekai uses software and engineering as productive capital; Bonfÿre makes that capital reusable. [Business and technical model](docs/aurekai-bonfyre-model.md).

## Aurekai products and experiences

| Product area | What lives there |
|---|---|
| [Aurekai Work](https://aurekai.work) | Organizations, jobs, operations, relationships, professional services, applications and opportunities |
| [Aurekai Life](https://aurekai.life) | Profiles, publishing, collections, people, organizations and communities |
| [Aurekai Live](https://aurekai.live) | Creators, channels, media, recording, narration and publishing |
| [Aurekai Shop](https://aurekai.shop) | Products, catalogues, offers, sourcing, room planning, commerce and markets |
| [Bonfÿre technical guide](https://nickgonzales76017.github.io/bonfyre/docs.html) | Native system components, models, WorkGraph, applications, resource management and interfaces |

The [application gallery](https://nickgonzales76017.github.io/bonfyre/#experiences) and [work samples](https://aurekai-work-samples.pages.dev/) include interactive examples for catalogue work, spatial product design, media editing, acquisition, research, document analysis, workflow planning and connected application use. The [product guide](https://nickgonzales76017.github.io/bonfyre/explore.html) describes the broader product families.

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

**[Explore the complete, searchable source atlas →](https://nickgonzales76017.github.io/bonfyre/modules/)**
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

## The whole machine

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

### Public capabilities

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
