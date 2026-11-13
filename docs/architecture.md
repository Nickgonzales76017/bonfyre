# Bonfÿre / Aurekai architecture

**Bonfÿre is the native, typed institutional operating engine. Aurekai is
the digital investment and operating business that builds public experiences,
applications, services and opportunities on that engine.** Aurekai also owns a
feedback-and-promotion loop informed by Bonfÿre evidence; that loop is *one
function of the business*, not its full definition.

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

## Whole-machine flow

```text
foreign / digital / physical / human worlds
                         │
              boundary + contract fabric
                         │
                         ▼
 Identity · Occurrence · Capability · Authority · Commitment
                         │
                         ▼
           WorkGraph + CapabilityClosure
                         │
      ┌──────────────────┼───────────────────┐
      ▼                  ▼                   ▼
 ProviderGraph      ContextCompiler      Money/Resource graphs
      └──────────────────┼───────────────────┘
                         ▼
         EffectKernel + execution phenotype
                         │
       native · local model · agent · distributed · human
                         │
                         ▼
       ReceiptEnvelope + WitnessDAG + EvidenceGraph
                         │
                         ▼
        replay · shadow · simulation · backtesting
                         │
                         ▼
       reviewed Aurekai product / partner use
           + learning and promotion
```

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

### Projection kernel

Contract Projection Kernel v2 materializes only the outputs demanded by an
ActiveClosure. Its projection families include:

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
| SurfaceIR renderer | architectural |
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

- [Architecture Atlas](../architecture/README.md)
- [Quickstart](../QUICKSTART.md)
- [Status and drift](bonfyre_status_and_drift.md)
- [Pages runtime](pages-runtime.md)
- [Client surfaces](client-surfaces.md)
- [Ontology surfaces](ontology_surfaces.md)
