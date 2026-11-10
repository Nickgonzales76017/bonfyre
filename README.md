<p align="center">
  <img src="https://github.com/user-attachments/assets/7785a8d6-6e28-4892-8958-3daf85414f05" alt="Bonfÿre knight" width="520" />
</p>
<h1 align="center">Bonfÿre</h1>

<p align="center">
  <strong>Software for tools, sites and apps that keep track of who may do what, and what was done.</strong>
</p>

<p align="center">
  <a href="#see-it-running">See it running</a> ·
  <a href="#building-blocks">Building blocks</a> ·
  <a href="#start-here">Start here</a> ·
  <a href="#the-whole-machine">Whole machine</a> ·
  <a href="#what-is-live">What is live</a> ·
  <a href="https://docs.aurekai.work/">Guides</a> ·
  <a href="https://aurekai-work-samples.pages.dev/">Work samples</a> ·
  <a href="docs/architecture.md">Architecture</a> ·
  <a href="CONTRIBUTING.md">Contributing</a>
</p>

Bonfÿre is the software underneath. Aurekai is what gets built and run on it:
tools for a team, sites that carry a customer's own name, apps their customers
keep using, and the links between systems that already exist.

## See it running

| What | Where |
|---|---|
| Team tools you can use in the browser: a fixture planner, a product editor, a shipment-file check | [Work samples](https://aurekai-work-samples.pages.dev/) |
| Three shop sites with their own looks, run from one installation | [Three businesses](https://docs.aurekai.work/three-businesses) |
| A training app for a studio, for members and coaches | [Walk-through](https://aurekai-work-samples.pages.dev/training/) · [`components/BonfyreServiceSite`](components/BonfyreServiceSite/) |
| Search over your own documents, on your own computer | [Walk-through](https://aurekai-work-samples.pages.dev/documents/) · [`components/BonfyreServiceSite`](components/BonfyreServiceSite/README.md) |
| Step-by-step guides: white-label delivery, recordings, requests to books, migration | [Guides](https://docs.aurekai.work/) · [`docs/experiences`](docs/experiences/) |
| What this looks like for a service network, a media business, a community project and others | [Product families](https://docs.aurekai.work/operating-products) |

The samples use public or made-up data. Paid memberships and card payments are
not part of the training sample.

## Building blocks

Nine areas of the codebase are being cut into libraries a developer can use
without the rest. Each one works from source today. None is published as a
package yet, so there is no version to pin and no support promise.

| Area | What it does | Where it lives |
|---|---|---|
| Data | Local SQL and vector search, with the source of every record kept | [`cmd/BonfyreVec`](cmd/BonfyreVec/), [`cmd/BonfyreEmbed`](cmd/BonfyreEmbed/), [`cmd/BonfyreCMS`](cmd/BonfyreCMS/) |
| Contracts | Typed schemas and the code generated from them | [`schemas/control`](schemas/control/) |
| Models | Serving, quantization and embeddings | [`cmd/BonfyreQuant`](cmd/BonfyreQuant/), [`cmd/BonfyreModel`](cmd/BonfyreModel/) |
| Work | Requests, commitments and who may act on them | [`cmd/BonfyreQueue`](cmd/BonfyreQueue/), [`cmd/BonfyreAuth`](cmd/BonfyreAuth/), [`cmd/BonfyreWorkflow`](cmd/BonfyreWorkflow/) |
| Apps | Frappe and WordPress integration | [`integrations/`](integrations/) |
| Media | Preparing and transcribing recordings | [`cmd/BonfyreMediaPrep`](cmd/BonfyreMediaPrep/), [`cmd/BonfyreTranscribe`](cmd/BonfyreTranscribe/) |
| Commerce | Moving records between systems and checking them on arrival | [`cmd/BonfyreBridge`](cmd/BonfyreBridge/) |
| Finance | Meters, invoices and recorded payments | [`cmd/BonfyreFinance`](cmd/BonfyreFinance/), [`cmd/BonfyrePay`](cmd/BonfyrePay/), [`cmd/BonfyreMeter`](cmd/BonfyreMeter/) |
| Web | Sites, pages and the assistant endpoint for each | [`cmd/BonfyreWebAddress`](cmd/BonfyreWebAddress/), [`components/BonfyreServiceSite`](components/BonfyreServiceSite/) |

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

Aurekai uses execution traces and proof bundles to propose better factors and
routes. Those proposals must pass the system's validation and authorization
checks before they can change how it runs.

The Generation-10/V8.1 system map is the architectural basis. The live
repository is the implementation truth. When they differ, the
[Architecture Atlas](architecture/README.md) records the live maturity and
witnesses; a declared or architectural organ is never presented as proven.

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
       Aurekai: learn → test → promote/cool/thaw
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
| **Aurekai** | Evidence-bound learning: discover candidates, backtest, run gyms, shadow, promote or cool, and thaw invalid factors. |

### Public capabilities

Bonfÿre preserves **91 public Power identities**. Each identity moves through a
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

## What is live

The repository contains native C/Zig/Rust/Python/TypeScript surfaces, a frozen
Python semantic reference and conformance oracle, installed Frappe products,
model/runtime work, schemas, generated inventories, and an executable
Architecture Atlas. Maturity is deliberately mixed:

- WorkGraph, Occurrence, capability composition, authority, and the nine Frappe
  product grammars have implemented witnesses.
- CapabilityClosure, transport, proof-frontier, ForeignTwin, and receipt paths
  have measured witnesses in the live atlas.
- Aurekai's full trace-to-candidate promotion loop and SurfaceIR renderer remain
  architectural; partial learning organs do not justify claiming the loop is
  complete.
- The Generation-10 projection estate is frozen and validated as the semantic
  destination while live absorption continues.

Inspect the truth directly:

```bash
bun architecture/atlas.mjs validate
bun architecture/atlas.mjs expand ExecutionView
bun architecture/atlas.mjs get work-graph
bun architecture/atlas.mjs loss
```

## Start here

### 1. Inspect the machine without building it

```bash
git clone https://github.com/Nickgonzales76017/bonfyre-oss.git
cd bonfyre-oss
bun architecture/atlas.mjs validate
bun architecture/atlas.mjs expand CapabilityView
bun architecture/atlas.mjs expand EvidenceView
```

### 2. Run the semantic control-plane evidence

```bash
cd components/BonfyreControlPlane
python3 -m pytest tests/ -q
```

This is the frozen reference and conformance oracle, not a second runtime.

### 3. Build and inspect native capabilities

```bash
make
./cmd/BonfyreCLI/bonfyre doctor sync-subcommands
./cmd/BonfyreCLI/bonfyre list --health
./cmd/BonfyreWorkflow/build/bonfyre-workflow list
./cmd/BonfyreRecipe/build/bonfyre-recipe list
```

### 4. Run a practical observation-to-recipe path

```bash
./cmd/BonfyreWire/bonfyre-wire ingest-pcap capture.pcap --dumb-device --root layeros/state
./cmd/BonfyreWire/bonfyre-wire probe <capture_id> --root layeros/state
./cmd/BonfyreWire/bonfyre-wire artifacts <capture_id> --root layeros/state
./cmd/BonfyreWire/bonfyre-wire recipe <capture_id> --root layeros/state > recipe.json
./cmd/BonfyreStitch/bonfyre-stitch compile recipe.json --output recipe-program
```

Only use captures you own or are authorized to inspect.

For a guided version, see [QUICKSTART.md](QUICKSTART.md).

## Repository map

| Path | Purpose |
|---|---|
| [`architecture/`](architecture/) | Canonical live architecture registry, generated views, maturity, witnesses, and loss reports. |
| [`engine/`](engine/) | Native semantic kernels and conformance work. |
| [`cmd/`](cmd/) | Native public command realizations. |
| [`lib/`](lib/) | Shared native runtime and specialized libraries. |
| [`schemas/`](schemas/) | Semantic and boundary contracts. |
| [`components/BonfyreControlPlane/`](components/BonfyreControlPlane/) | Frozen Python reference/conformance oracle. |
| [`integrations/`](integrations/) | Frappe and external-system realization work. |
| [`docs/`](docs/) | Operator, architecture, product, model, media, and API guides. |
| [`site/`](site/) | Public Pages projections. |

## Read next

- [Quickstart](QUICKSTART.md)
- [Guides](https://docs.aurekai.work/): the pages in `docs/experiences`, published
- [Work samples](https://aurekai-work-samples.pages.dev/): tools to try, and where to send a project brief
- [Architecture](docs/architecture.md)
- [Architecture Atlas](architecture/README.md)
- [Operator status and drift](docs/bonfyre_status_and_drift.md)
- [Pages runtime](docs/pages-runtime.md)
- [Contributing](CONTRIBUTING.md)

## License

[MIT](LICENSE)
