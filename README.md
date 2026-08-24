<p align="center">
  <img src="https://github.com/user-attachments/assets/7785a8d6-6e28-4892-8958-3daf85414f05" alt="Bonfÿre knight" width="520" />
</p>
<h1 align="center">Bonfÿre</h1>

<p align="center">
  <strong>A typed operating machine for turning intent into authorized work, effects, evidence, and learning.</strong>
</p>

<p align="center">
  <a href="#start-here">Start here</a> ·
  <a href="#the-whole-machine">Whole machine</a> ·
  <a href="#what-is-live">What is live</a> ·
  <a href="docs/architecture.md">Architecture</a> ·
  <a href="CONTRIBUTING.md">Contributing</a>
</p>

Bonfÿre is not a fixed collection of binaries and Aurekai is not a generic
agent wrapper. Bonfÿre owns the semantic machine: identity, authority,
capability, work, effects, evidence, resources, commitments, and institutional
state. Aurekai learns from traces and proof bundles, proposes better factors and
routes, and must pass the same validation and authority boundaries before a
proposal can become part of the machine.

The Generation-10/V8.1 system map is the architectural basis. The live
repository is the implementation truth. When they differ, the
[Architecture Atlas](architecture/README.md) records the live maturity and
witnesses; a declared or architectural organ is never presented as proven.

## The whole machine

```text
intent / observation / foreign state
                 │
                 ▼
 Identity · Authority · CapabilityClosure · WorkGraph
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

The machine keeps these concerns connected without collapsing them:

| Organ | What it owns |
|---|---|
| **WorkGraph** | Durable missions, dependencies, leases, cooling, and completion contracts. |
| **CapabilityClosure** | The gap between a named capability, a callable realization, and a workload-proven one. |
| **Authority + EffectKernel** | Who may do what, with idempotency and effect ceilings checked at execution time. |
| **Contract Projection Kernel v2** | ActiveClosure-demanded projections into native ABIs, data/query forms, APIs, WIT/Wasm, Frappe surfaces, evidence, evolution, tests, and packs. |
| **Evidence and replay** | Receipts, witness DAGs, trace divergence, proof bundles, deterministic replay, shadow execution, simulation, and institutional backtesting. |
| **Aurekai** | Evidence-bound learning: discover candidates, backtest, run gyms, shadow, promote or cool, and thaw invalid factors. |

### 91 powers, not one generic tool

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

Operational metabolism turns provider readiness, price, credit, quota,
freshness, schedules, external tools, artifact packets, and autonomous worker
dependencies into explicit WorkGraph and ResourceGraph inputs. Financial
metabolism keeps agreement, billing account, spend intent, funding source,
payment instrument, authorization, capture, settlement, refund, allocation, and
ERPNext accounting as distinct states. A price is not a payment, a provider
credit is not cash, and a dashboard is not accounting truth.

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
python3 architecture/atlas.py validate
python3 architecture/atlas.py expand ExecutionView
python3 architecture/atlas.py get work-graph
python3 architecture/atlas.py loss
```

## Start here

### 1. Inspect the machine without building it

```bash
git clone https://github.com/Nickgonzales76017/bonfyre-oss.git
cd bonfyre-oss
python3 architecture/atlas.py validate
python3 architecture/atlas.py expand CapabilityView
python3 architecture/atlas.py expand EvidenceView
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
- [Architecture](docs/architecture.md)
- [Architecture Atlas](architecture/README.md)
- [Operator status and drift](docs/bonfyre_status_and_drift.md)
- [Pages runtime](docs/pages-runtime.md)
- [Contributing](CONTRIBUTING.md)

## License

[MIT](LICENSE)
