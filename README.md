<div align="center">
<img width="105" height="108" alt="Bonfÿre" src="https://github.com/user-attachments/assets/7785a8d6-6e28-4892-8958-3daf85414f05">
<h1>Bonfÿre</h1>
<strong>A semantic operating engine for compiling work across software, data, models, people, devices, evidence, and economics.</strong>
<br><br>
<em>Bonfÿre is the engine. Aurekai is the larger umbrella.</em>
<br><br>
<a href="#the-machine">The machine</a> ·
<a href="#core-laws">Core laws</a> ·
<a href="#operator-model">Operator model</a> ·
<a href="#native-powers">Native Powers</a> ·
<a href="#repository-map">Repository map</a> ·
<a href="#quick-start">Quick start</a> ·
<a href="#licensing">Licensing</a>
</div>

---

## What Bonfÿre is

Bonfÿre is not an AI router, an ERP, an agent framework, a workflow builder, or a collection of unrelated command-line tools.

It is the engine underneath those kinds of systems.

Bonfÿre gives heterogeneous reality a shared semantic coordinate system, turns differences between addressed states into durable work, selects lawful realizations across native code and external providers, records what physically happened, attaches evidence and economics, and projects the result into whatever surface is appropriate.

The shortest useful description is:

```text
reality
  → address
  → derive
  → work
  → route
  → realize
  → observe
  → prove
  → reconcile
  → project
```

A file, Frappe record, model call, database row, provider capability, human action, device, artifact, economic event, or external API should not need a separate architectural universe merely because its carrier is different.

Bonfÿre tries to preserve the distinctions that matter while factoring the machinery that repeats.

> **Preserve what is meaningfully different. Factor what is merely repeated.**

## Bonfÿre and Aurekai

**Aurekai** is the larger company/project/learning umbrella.

**Bonfÿre** is the engine: the semantic, execution, evidence, model, financial, artifact, provider, and projection machine that Aurekai can operate and optimize.

That distinction is intentional. Aurekai may learn from repeated execution and improve how work is presented or realized; it does not get to silently rewrite deterministic semantic truth.

## One machine, not a bag of products

Bonfÿre is designed to run as one institutional machine.

CRM, ERP, helpdesk, knowledge, model execution, media, artifact handling, provider routing, financial reasoning, evidence, local runtimes, APIs, MCP surfaces, filesystems, and operator tooling are not meant to become separate mini-platforms glued together after the fact.

They are different projections and organs over shared semantic state.

That means a useful fact discovered in one part of the machine can lawfully participate in another without being copied into a second shadow architecture.

Examples:

```text
housing observation
  → exact source evidence
  → AddressPlane subject
  → geographic / financial derivation
  → WorkGraph obligation
  → provider realization
  → usage + cost
  → evidence closure
  → CLI / Frappe / report projection
```

```text
model/provider attempt
  → physical occurrence
  → UsageOccurrence
  → EconomicMeasure
  → CostAllocation
  → work/result/evidence lineage
```

```text
external schema or document
  → BoundaryCompiler / format semantics
  → canonical meaning
  → reusable capability
  → target-native projection
```

## The machine

The mature architecture is easiest to understand as a set of connected fabrics rather than a product menu.

```text
                            EXTERNAL REALITY
        files · APIs · DBs · Frappe · models · people · devices · events
                                      │
                                      ▼
                ┌──────────────────────────────────────────┐
                │ BoundaryCompiler / Format / Partner      │
                │ Commons / foreign-system grammars        │
                └───────────────────┬──────────────────────┘
                                    ▼
                         ┌────────────────────┐
                         │    AddressPlane    │
                         │ identity · time    │
                         │ relation · world   │
                         │ projection · proof │
                         └─────────┬──────────┘
                                   │
                     FactDelta     │     Authority / Evidence
                         ┌─────────▼──────────┐
                         │ SemanticDerivation │
                         │ exact read closure │
                         └─────────┬──────────┘
                                   ▼
                    ┌───────────────────────────┐
                    │ WorkGraph + TargetObligation│
                    │ semantic difference due    │
                    └─────────────┬─────────────┘
                                  │
              learned priors ─────┤──── operator algebra
              never authority     │
                    ┌─────────────▼─────────────┐
                    │ Provider / Placement /    │
                    │ ResourceWeather / Env     │
                    └─────────────┬─────────────┘
                                  ▼
     ┌─────────────────────────────────────────────────────────────┐
     │ native Powers · Frappe ×9 · local models · remote models   │
     │ APIs · MCP · services · humans · devices · other nodes     │
     └────────────────────────────┬────────────────────────────────┘
                                  ▼
                    ┌──────────────────────────┐
                    │ PhysicalDerivation      │
                    │ manifest · frame        │
                    │ observation · receipt   │
                    │ body availability/reuse │
                    └─────────────┬────────────┘
                                  │
                   ┌──────────────┼──────────────┐
                   ▼              ▼              ▼
             EvidenceGraph   UsageOccurrence   result/body
             WHY / PROVE          │              │
                                  ▼              │
                         FinancialMetabolism    │
                         basis-explicit money   │
                                  │              │
                   └──────────────┼──────────────┘
                                  ▼
                   Projection / Surface / Namespace
                  CLI · API · MCP · BonfÿreFS · apps
                         reports · external systems
```

Aurekai sits above this machine as a learning and optimization layer. It can identify repeated motifs, candidate locality, or cheaper equivalent realizations, but lowering is proof-gated and must lift again when its validity envelope changes.

## Core laws

The project becomes much easier to understand once a few invariants are kept explicit:

- **Address is not location.** `AddressPlane` is a semantic coordinate system spanning identity, relation, time, world, projection, evidence, and closure.
- **Work is not a queue entry.** `WorkGraph` represents an executable semantic difference between addressed states. A queue/process/provider call is only one possible realization of that work.
- **Carrier failure is not semantic failure.** A `TargetObligation` survives failed providers, builds, sessions, waits, and retries until proof closes it or a replacement explicitly supersedes it.
- **Provider is not capability.** Native code, a local model, a remote model, an API, MCP, a human, or a device can compete to realize the same capability without redefining the capability.
- **Similarity is not identity.** Latent/embedding spaces can rank or narrow candidates; they do not establish semantic identity, authority, evidence, relation, or financial truth.
- **Execution is not proof.** A zero exit code or successful network response does not by itself prove the semantic target.
- **Physical usage is not booked money.** Accrued, billed, paid, booked, credit, avoided work, and projected value remain different bases.
- **Projection is not ownership.** A Frappe row, API response, filesystem path, report, or UI surface may expose a semantic object without becoming its canonical owner.
- **Representation is not identity.** Compression, quantization, serialization, caching, or a different file format may change physical realization without changing the semantic subject.
- **Named specialization stays named.** Domain-specific Powers, runtimes, graphs, and mathematical kernels are not flattened into generic nouns just to make the diagram tidy.

## Operator model

Bonfÿre is converging on a small semantic instruction set that composes across domains:

```text
ADDRESS
SELECT
DIFF
CLOSE
PROJECT
ROUTE
ATTACH
DETACH
MOVE
RECONCILE
PROVE
WORK
EXECUTE
COMPENSATE
```

Compound operations such as retire, offboard, migrate, reconcile, fulfill, or publish are built from these semantics rather than each receiving an unrelated orchestration system.

The operator surface also distinguishes three useful modes:

```text
@   semantic address / cursor / join
?   deterministic inspection and derived queries
!   governed raw process occurrence
```

Raw shell execution is therefore not an escape hatch from the machine. When used through Bonfÿre it is still a physical occurrence with authority, evidence, and usage consequences.

## AddressPlane, WorkGraph, and WHY

Three ideas tie a large amount of the system together.

### AddressPlane

A semantic address names **what** something is in context, not merely where its bytes live.

The same underlying object can participate in different time, world, projection, evidence, or relation contexts without creating unrelated shadow identities.

### WorkGraph

Work is the difference between an addressed current state and an addressed required state.

That allows work to shrink or disappear when reality changes, survive a failed carrier, branch into alternative realizations, or create new work when proof discovers a genuine residual.

### WHY / PROVE

`WHY` is a causal/evidence slice explaining how a result, route, decision, or state was derived.

`PROVE` builds an evidence closure sufficient for a target's proof requirements. Evidence remains source-addressed; confidence or model output does not silently become ground truth.

## Semantic and physical derivation

Bonfÿre separates *what must be true* from *how bytes/processes make it true*.

```text
FactDelta
   │
   ▼
SemanticDerivationGraph
   │ exact dependency/read closure
   ▼
required postcondition
   │
   ▼
RealizationManifest
   │
   ▼
PhysicalExecutionFrame
   │
   ▼
RealizationObservation
   │
   ▼
PhysicalDerivationReceipt
```

That separation allows repeated work to be reused safely. If the semantic postcondition and derivation identity are still valid and a lawful body already exists, Bonfÿre can reuse the body rather than recompute it.

The same mechanism lets cost or provider conditions change without pretending the semantic result or bytes changed.

## Models and latent spaces

Models are first-class participants, not the center of the architecture.

Bonfÿre supports local, remote, prepared, quantized, embedded, multimodal, and specialized model realizations behind the same capability/evidence/usage laws as other providers.

The governed latent layer attaches replayable learned coordinates to exact semantic subjects with explicit passports, encoder identity, feature closure, causal frontier, metric, allowed uses, and comparability rules.

Examples of model/representation work in the estate include:

- `BonfyreQwenFPQ` and FPQ/quantized model execution;
- Lambda Tensors and specialized tensor/math paths;
- local embeddings/vector search;
- speech, vision, media, and transcription runtimes;
- remote frontier-model providers;
- learned candidate ranking and trajectory/watch signals.

The rule remains:

```text
latent prior → candidate narrowing
latent prior ↛ authority / identity / evidence / booked truth
```

## Financial metabolism

Economics are part of execution rather than an afterthought.

A physical or model attempt can create a `UsageOccurrence`; admitted pricing/allocation evidence can derive economics; financial position preserves basis instead of collapsing every number into "cost" or "savings."

```text
physical occurrence
   ↓
UsageOccurrence
   ↓
EconomicMeasure
   ↓
CostAllocation
   ↓
FinancialPosition / evidence / work
```

Retries remain distinct occurrences. Shared/cache allocations must be explicit. Unknown token/resource dimensions remain unknown rather than becoming zero. Avoided work is not automatically cash savings.

This same machinery is intended to reason over compute, model, storage, provider, human, facility, and institutional resource use.

## Native Powers

Bonfÿre has a large native command estate. Those commands are not legacy names that should all disappear into one generic runtime.

A Power keeps its domain identity and canonical owner while sharing common fabric underneath it.

Representative areas include:

- `BonfyreLedger` — ledger/economic and evidence-oriented operations;
- `BonfyreQwenFPQ` — specialized local model execution;
- `BonfyreWire` — observed external/device/network material;
- `BonfyreStitch` — composition/planning;
- `BonfyreFrappeCompiler` — Frappe schema/projection compilation;
- `BonfyreCLI` — operator front door and command registry;
- workflow, recipe, layer, API, CMS, media, inference, distribution, model, artifact, and control Powers;
- shared low-level libraries including `libbonfyre`, `liblambda-tensors`, and QUIC transport.

The important architectural distinction is:

```text
semantic shell surface ≠ implementation owner
```

A shell can invoke a Power without reimplementing the Power's domain logic.

## Frappe ×9

Bonfÿre uses the Frappe ecosystem as a major institutional projection surface rather than as nine disconnected products.

The mature lineage spans the shared Frappe/ERP base plus CRM, HRMS, Helpdesk, LMS, Insights, Wiki, and Drive surfaces. Their DocTypes and application grammars can project common Bonfÿre semantics while keeping each app's domain distinctions intact.

The goal is not to create a special "Bonfÿre food app," "Bonfÿre housing app," or one-off app for every workload. New workloads should reuse the shared semantic fabric and project through existing institutional surfaces when lawful.

## Formats, artifacts, and BonfÿreFS

Bonfÿre treats representations as meaningful execution choices.

PDF, DOCX, XLSX, SQLite, Parquet, Arrow, media containers, model artifacts, WASM, packages, 3D formats, and other representations have different object models, access patterns, streaming behavior, canonicalization, finalization, loss, and validation rules.

The common artifact/fabric layer factors storage, addressing, provenance, transport, and reuse while format-specific semantics remain where needed.

The namespace side of the machine projects files, records, queries, artifacts, maintained sets, and foreign objects through one addressable fabric. A path resolving does not itself grant read authority, and a virtual query directory is not pretending to be stored state.

## Partner Commons and the open world

Bonfÿre is intentionally open-world: useful outside libraries, protocols, services, models, repositories, and methods can participate without becoming canonical truth or being rewritten as Bonfÿre code.

Partner Commons captures reusable specialties and roles such as:

```text
schema / contract tooling
columnar and vector data
incremental computation
transport and storage
model runtimes
benchmarking / evaluation
finance / quant methods
policy / authorization
artifact and format tooling
agent/runtime hosts
```

The lifecycle is:

```text
discover specialty
  → understand exact shape
  → bind lawful role/boundary
  → route real work through it
  → measure consequences
  → retain external owner or absorb only where justified
```

External does not mean temporary, and native absorption is not automatically the goal.

## Evidence and proof

Bonfÿre is built to keep the difference between claims and observations visible.

Execution receipts, source coordinates, causal fronts, provider/model identity, body digests, pricing evidence, authority decisions, test results, and external observations can all participate in proof.

The system should be able to answer questions such as:

```text
WHY did this route win?
WHY is this work still open?
WHY is this result considered current?
WHAT physically produced these bytes?
WHAT evidence closed this obligation?
WHAT cost basis produced this number?
WHAT changed since the previous frontier?
```

That is why evidence is a fabric, not merely a log directory.

## Repository map

The repository is actively converging several historical source eras into stable owners. **Do not infer the current architecture from the age or number of top-level folders.** Some directories are lineage and evidence; others are canonical runtime owners.

The important public areas today are:

```text
engine/core/       low-level Bonfÿre execution kernel
lib/               shared native libraries and mathematical/runtime substrate
cmd/               named native Powers and operator binaries
estate/            registries, provider/operator inventories, compatibility data
integrations/      external system, MCP, ERP/Frappe and host integrations
layeros/           artifact/layer/queue/runtime state machinery
docs/              subsystem documentation and historical implementation notes
tests/             conformance, requirements, integration and regression tests
scripts/           operational, build and workload helpers
site/              public/demo surfaces
10-Code/           transitional historical source lineage; not the desired final owner
01-Ideas/ ...       research/project lineage; useful history, not the runtime architecture
```

The canonical direction is toward a smaller set of explicit owners rather than keeping duplicate implementations forever:

```text
engine/core       native kernel
lib               shared low-level libraries
cmd / services    named Powers and specialized runtimes
semantic/control  shared semantic, work, proof, provider and economic fabric
schemas/contracts source-of-truth machine contracts
generated         reconstructable projections, not authority
evidence          proof/history, not runtime ownership
```

Old source is removed only when current-owner parity and evidence make deletion lawful. Duplicate-looking paths therefore do not imply that Bonfÿre intends to preserve multiple competing architectures.

## Quick start

### Build

```bash
git clone https://github.com/Nickgonzales76017/bonfyre.git
cd bonfyre
make
```

The top-level build discovers command directories with Makefiles and builds the shared libraries plus native command estate.

For a portable build without `-march=native`:

```bash
make portable
```

### Install

```bash
make install
```

The default prefix is `~/.local`. Override it with:

```bash
PREFIX=/usr/local make install
```

### Inspect the operator surface

```bash
./cmd/BonfyreCLI/bonfyre doctor sync-subcommands
./cmd/BonfyreCLI/bonfyre list --health
```

After installation:

```bash
bonfyre list --health
```

### Run the test surface

```bash
make test
```

### Try a concrete external-material path

The existing Wire → artifact → recipe → stitch path is a useful narrow entry point into the larger machine:

```bash
./cmd/BonfyreWire/bonfyre-wire ingest-pcap capture.pcap --dumb-device --root layeros/state
./cmd/BonfyreWire/bonfyre-wire probe <capture_id> --root layeros/state
./cmd/BonfyreWire/bonfyre-wire artifacts <capture_id> --root layeros/state
./cmd/BonfyreWire/bonfyre-wire recipe <capture_id> --root layeros/state > recipe.json
./cmd/BonfyreStitch/bonfyre-stitch plan recipe.json
```

This path is an example, **not the definition of Bonfÿre**.

## Where to read next

- [`QUICKSTART.md`](QUICKSTART.md) — concrete build/operator path.
- [`docs/architecture.md`](docs/architecture.md) — earlier public architecture material.
- [`docs/bonfyre_status_and_drift.md`](docs/bonfyre_status_and_drift.md) — runtime/operator drift notes.
- [`docs/bonfyre_wire.md`](docs/bonfyre_wire.md) — external/device observation path.
- [`docs/FPQx-Algebra-Reference.md`](docs/FPQx-Algebra-Reference.md) — FPQ algebra/reference work.
- [`docs/lambda-tensors.md`](docs/lambda-tensors.md) — Lambda Tensor work.
- [`docs/api.md`](docs/api.md) — API surface.
- [`docs/cms.md`](docs/cms.md) — CMS surface.
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — contribution guidance.

Some older documents describe earlier phases of the machine. Treat the repository and current canonical owner/evidence work as higher authority than an old phase-completion note.

## Development principles

Bonfÿre development follows a few recurring rules:

1. **Extend the current owner before creating a parallel subsystem.**
2. **Make the semantic target explicit before optimizing its realization.**
3. **Keep authority, evidence, identity, economics, and provider availability separate.**
4. **Prefer real workloads to decorative demos.** A new path should eventually carry actual work.
5. **Measure before promotion.** External tools, learned paths, compression, caching, lowerings, and provider substitutions need receipts or equivalence evidence.
6. **Delete only after parity.** Historical code is lineage until a current owner and proof make it safely reconstructable or obsolete.
7. **Keep domain-specific machinery first-class.** BonfyreLedger remains BonfyreLedger; BonfyreQwenFPQ remains BonfyreQwenFPQ; Lambda Tensors remain Lambda Tensors.
8. **Use one semantic machine across domains.** Food, housing, finance, software delivery, media, institutional apps, and external partner work should stress the same fabrics rather than creating one-off architectures.

## What Bonfÿre is trying to become

The end state is not "more integrations."

It is a machine where a new domain or external system increasingly contributes only the information that is genuinely new:

```text
new identity / grammar / constraints / capability / evidence / representation
                              +
             an already existing semantic machine
                              =
                    much less bespoke software
```

As more work passes through the same address, derivation, evidence, provider, execution, finance, format, and projection fabrics, the system should compound instead of merely grow.

That is the point of Bonfÿre.

## Licensing

Bonfÿre is **source-available**, not permissively licensed open source.

Covered source in this repository is distributed under the **PolyForm Shield License 1.0.0** with the Bonfÿre licensing notice in [`LICENSE`](LICENSE). Prior versions released under MIT remain under the rights granted with those versions. Third-party or separately licensed components retain their own terms.

Commercial and partner licensing may be available separately.

---

<div align="center">
<strong>Bonfÿre engine · Aurekai umbrella</strong><br>
<sub>Meaning before transport. Evidence before closure. Capability before provider. One machine across domains.</sub>
</div>
