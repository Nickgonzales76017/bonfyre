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
  <a href="#federated-workspace">Federated workspace</a> ·
  <a href="#start-here">Start here</a> ·
  <a href="#the-whole-machine">Whole machine</a> ·
  <a href="docs/client-operations-portfolio.md">Client work</a> ·
  <a href="#whats-available-today">Available today</a> ·
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
| A training app for a studio, for members and coaches | [Walk-through](https://aurekai-work-samples.pages.dev/training/) |
| Search over your own documents, on your own computer | [Walk-through](https://aurekai-work-samples.pages.dev/documents/) |
| Step-by-step guides: white-label delivery, recordings, requests to books, migration | [Guides](https://docs.aurekai.work/) |
| What this looks like for a service network, a media business, a community project and others | [Product families](https://docs.aurekai.work/operating-products) |

The samples use public or made-up data. Paid memberships and card payments are
not part of the training sample.

## Use a smaller piece

You can also start without building the whole system:

| Independently scoped source | The job it does | What to inspect |
|---|---|---|
| **[Statement Import](https://github.com/Nickgonzales76017/bonfyre-statement-import)** | Convert normalized bank-statement text into CSV and refuse a result when running balances fail to reconcile | [C source and pack fixtures](https://github.com/Nickgonzales76017/bonfyre-statement-import) |
| **[Agent Trace Verifier](https://github.com/Nickgonzales76017/bonfyre-agent-trace-verifier)** | Evaluate agent-execution evidence through a deterministic, source-addressed receipt | [Standalone verifier and examples](https://github.com/Nickgonzales76017/bonfyre-agent-trace-verifier) |

Those tools have their own repositories and licenses. For more involved deployments, [the client operations portfolio](docs/client-operations-portfolio.md) shows how local software and public applications fit WordPress, ERPNext, source documents, creator publishing, agency work and commerce. The [operating-products guide](https://docs.aurekai.work/operating-products) describes broader customer and partner applications.

## Federated workspace

The integrated native engine stays here. **47 public companion repositories are now
pinned as real Git submodules**, alongside the existing HVM4 dependency. They
are grouped under [`integrations/federated/`](integrations/federated/) and
[`examples/federated/`](examples/federated/), not added as thirty more root
directories.

| Profile | Modules | What lives there |
|---|---:|---|
| `products` | 2 | Independent Statement Import and Agent Trace Verifier |
| `extractions` | 7 | Earlier native library/CLI extractions, including Lambda Tensors |
| `examples` | 7 | Demonstrations, creator scaffold and the real recipe collection |
| `aurekai` | 12 | The earlier Aurekai platform, native runtime, CI, IDE, data and workflow adapters |
| `frappe` | 9 | Pinned source of Frappe, ERPNext, LMS, HRMS, CRM, Helpdesk, Insights, Wiki and archived Drive |
| `interfaces` | 8 | OpenTUI, MCP Apps, A2UI, json-render, Bubble Tea, Lip Gloss, Ratatui and Ghost design/source references |
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
The `frappe`, `interfaces`, and `all` groups can pull **gigabytes** of source
history and require `BONFYRE_ALLOW_LARGE_MODULES=1` before initializing.
Neither the optional donors nor historical lineage is cloned by default. The script verifies the exact gitlink
commit and public URL before fetching, **never advances to a remote branch
without review**, and does not recurse into older nested repositories.

The Frappe pins are source references, **not a tested nine-app compatible bench**;
`frappe/drive` is archived upstream. The interface repos are source/design
references, **not bundled components or an implicit right to reuse assets**.

These are *references to independently owned source histories*, not a second
owner for Bonfÿre facts or an assertion that old extracted code matches the
live `cmd/` implementation. No private client repository is included.
Review the license in each companion repo separately. The three unusable
archived gitlink coordinates remain documented in their evidence history
rather than blocking normal submodule initialization.

## Building blocks

Nine areas of the development codebase are being cut into independently usable
libraries. Some sources are not yet on public `main`; none is a versioned,
supported package release.

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

Aurekai uses execution traces and proof bundles to propose better factors and
routes. Those proposals must pass the system's validation and authorization
checks before they can change how it runs.

The Generation-10/V8.1 system map is the architectural basis. The live
repository is the implementation truth. When they differ, the
development Architecture Atlas records the live maturity and
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

The public [work samples](https://aurekai-work-samples.pages.dev/) and
[guides](https://docs.aurekai.work/) demonstrate current browser, publishing,
document, integration and training workflows. Samples use fictional or public
data. They are not proof of paid customer deployment, card processing or
completed private-service contracts.

**Source release boundary:** public `main` contains an earlier source snapshot
alongside recently reviewed documentation. Newer native components, the
Architecture Atlas, white-label ServiceSite source and cross-owner
implementations are undergoing separate privacy, licensing and
independent-install review. This README describes the design and available
demonstrations; it does not imply every referenced development source
directory is already in `main`.

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
