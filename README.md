<div align="center">
<img width="105" height="108" alt="Bonfÿre" src="https://github.com/user-attachments/assets/7785a8d6-6e28-4892-8958-3daf85414f05"><h1>Bonfÿre</h1>
<strong>Semantic routing and compilation for heterogeneous software, data, capabilities, and external systems.</strong>
<br><br>
<a href="#what-bonfÿre-is">What it is</a> ·
<a href="#how-it-works">How it works</a> ·
<a href="#quick-start">Quick start</a> ·
<a href="#architecture">Architecture</a> ·
<a href="#use-cases">Use cases</a> ·
<a href="#project-layout">Project layout</a> ·
<a href="#contributing">Contributing</a>
</div>

---

## What Bonfÿre is

Most integration software starts after a request already has a known shape.

Bonfÿre starts earlier.

It is a **semantic routing and compilation engine** designed to accept increasingly heterogeneous inputs—documents, databases, APIs, policies, application state, media, models, events, and other software surfaces—and turn them into typed, reusable structure before deciding how the work should be realized.

A compact view:

```text
heterogeneous input
        │
        ▼
 institutionalize
        │
        ▼
 typed semantic fabric
        │
        ▼
      compile
        │
        ▼
   resolve / route
        │
        ▼
 local · model · human · external
        │
        ▼
 externalize / project
```

Traditional routing looks like:

```text
known request → choose destination
```

Bonfÿre is built for:

```text
heterogeneous reality
→ understood meaning
→ legal operation
→ selected realization
→ target-compatible result
```

AI can participate in that process, but Bonfÿre is not an AI router. Models are one class of provider alongside local code, services, humans, devices, and external systems.

The larger goal is to make repeated integration work increasingly reusable:

```text
many bespoke solutions
        │
        ▼
shared semantic structure
+ shared capability structure
+ shared boundary/compiler machinery
+ small customer-specific differences
```

## Why

Software teams repeatedly solve variations of the same problem:

- ingest a customer's data or documents;
- understand an existing application's shape;
- map business rules and policies;
- connect one or more providers;
- transform the result into another system's required form;
- build a new API, adapter, workflow, or application surface around it.

The implementations differ, but much of the underlying structure repeats.

Bonfÿre is designed to capture that reusable structure instead of forcing every project to remain a one-off integration.

## How it works

### 1. Institutionalize

Bonfÿre converts incoming material into typed semantic participation.

Depending on the source, that may include:

```text
identity
kind / type
authority
provenance
state
relations
evidence
representations
capabilities
work
```

This is intentionally broader than parsing or ETL.

A PDF is not only converted into text. A database is not only converted into rows. An external application is not only reduced to an API schema.

The useful parts become addressable according to what they **mean** and what they are allowed to participate in.

### 2. Compile

A constellation of compiler responsibilities lowers semantic meaning into increasingly executable structures.

Examples include:

```text
source / boundary understanding
domain grammar
capability requirements
evidence and context
addressability
placement
execution
surfaces
target-specific externalization
```

The compiler model lets Bonfÿre preserve high-level meaning while changing how that meaning is physically realized.

### 3. Resolve and route

A capability describes **what can be done**, independently of whichever implementation currently performs it.

One capability may be realized by:

```text
local native code
local model
remote model
external API
MCP service
human
device
another Bonfÿre node
```

Routing therefore happens over semantic intent, current reachability, policy, authority, representations, provider health, resources, and other execution constraints.

### 4. Externalize

External systems have their own schemas, protocols, permissions, object models, and accepted representations.

Bonfÿre treats those boundaries explicitly.

```text
foreign/native form
       │
       ▼
 ingress grammar
       │
       ▼
 Bonfÿre semantics
       │
       ▼
 egress grammar
       │
       ▼
target-native form
```

An API is a carrier. An adapter is an implementation detail. The boundary includes the meaning required to participate correctly in the target system.

This allows the same internal semantic result to be lowered into different outward forms without treating each output as a separate one-off pipeline.

## Architecture

The public architecture can be understood as four large operations:

```text
┌──────────────────┐
│ INSTITUTIONALIZE │
└────────┬─────────┘
         ▼
╔══════════════════════════╗
║  TYPED SEMANTIC FABRIC  ║
╚════════════╤═════════════╝
             ▼
      ┌─────────────┐
      │   COMPILE   │
      └──────┬──────┘
             ▼
      ┌─────────────┐
      │ ROUTE /     │
      │ RESOLVE     │
      └──────┬──────┘
             ▼
   local · model · human
        · external
             │
             ▼
      ┌─────────────┐
      │ EXTERNALIZE │
      │ / PROJECT   │
      └─────────────┘
```

Several deeper parts of the repository support those operations:

- **semantic and domain grammars** — typed structure instead of application-specific glue;
- **capabilities and providers** — stable intent separated from implementation;
- **boundary compilation** — reusable understanding of external systems;
- **execution, receipts, and metering** — distinguish intent from what actually happened;
- **work and durable continuation** — represent causal work separately from queues and processes;
- **context, evidence, and source coordinates** — retain where information came from;
- **representation and format machinery** — preserve format-specific semantics rather than flattening everything into generic blobs;
- **local model/runtime machinery** — models participate as providers and representations inside the same execution fabric;
- **projection surfaces** — expose underlying semantics through APIs, MCP, filesystems, applications, reports, and other hosts.

Bonfÿre keeps specialized machinery specialized where the distinction matters.

The guiding rule is:

> **Preserve what is meaningfully different. Factor what is merely repeated.**

## Reusable boundaries

A major part of Bonfÿre is making external integrations reusable without pretending the external system belongs to Bonfÿre.

A reusable boundary can describe:

```text
external shape
identity mappings
kind mappings
authority constraints
ingress grammar
egress grammar
capability bindings
event profiles
representation rules
validation
loss / round-trip behavior
```

A live customer or environment can then bind that reusable definition to its own endpoint, credentials, scopes, policy, and runtime state.

Conceptually:

```text
             reusable boundary
                    │
        ┌───────────┼───────────┐
        ▼           ▼           ▼
   customer A  customer B  customer C
   connection  connection  connection
```

This is one of the ways Bonfÿre aims to reduce repeated bespoke integration engineering.

## Use cases

Bonfÿre is intentionally not tied to one input or output class.

### Document and knowledge systems

```text
PDF / DOCX / transcript / spreadsheet
                │
                ▼
        semantic material
                │
       ┌────────┼────────┐
       ▼        ▼        ▼
      API      MCP     application
       │
       ├→ evidence
       ├→ model context
       ├→ database
       └→ downstream provider
```

### Existing application integration

Understand an application's schema, capabilities, events, and authority boundary once, then reuse that compiled understanding across connections instead of rebuilding the integration for every customer.

### Provider-independent capabilities

Express the required capability first and resolve among local code, local models, remote models, APIs, humans, or other providers according to the current environment.

### Process-shaped software

Existing product/domain semantics can be reused without forcing users to navigate the historical application boundaries that originally contained them.

### Local and hybrid AI

Local model runtimes, embeddings, vectors, quantized representations, prepared state, and remote model providers participate in the same capability and routing model rather than living in a separate AI architecture.

### Media and artifact pipelines

Documents, audio, video, structured data, model artifacts, packages, and other formats retain their own representation semantics while still participating in common routing and compilation machinery.

## Current repository

This repository contains the public Bonfÿre runtime and command estate, including native C components, shared libraries, operator tooling, registries, artifact and pipeline machinery, local model integrations, media tooling, and integration/runtime surfaces.

The repository has evolved beyond the original fixed-binary framing. Command names remain useful public/operator identities, but the architecture increasingly factors repeated behavior into shared capability, representation, compiler, and runtime machinery.

Use the repository itself as the source of truth for the currently built command surface:

```bash
make
./cmd/BonfyreCLI/bonfyre list --health
```

For operator/catalog drift after pulling new runtime code:

```bash
./cmd/BonfyreCLI/bonfyre doctor sync-subcommands
./cmd/BonfyreIndex/bonfyre-index layers --root layeros/state
./cmd/BonfyreWorkflow/bonfyre-workflow list
./cmd/BonfyreRecipe/bonfyre-recipe list
./cmd/BonfyreLayer/bonfyre-layer registry --root layeros/state
```

## Quick start

### Build from source

```bash
git clone https://github.com/Nickgonzales76017/bonfyre-oss.git
cd bonfyre-oss
make
```

### Install

```bash
make install
```

By default this installs into the configured local prefix. You can override it where supported:

```bash
PREFIX=/usr/local make install
```

### Inspect the system

```bash
bonfyre list --health
bonfyre workflow list
bonfyre recipe list
bonfyre layer registry --root layeros/state
```

### Explore a concrete path

The Wire → artifact → recipe path is a useful way to see how observed external material becomes something Bonfÿre can reason about and execute against:

```bash
bonfyre wire ingest-pcap capture.pcap --dumb-device --root layeros/state
bonfyre wire probe <capture_id> --root layeros/state
bonfyre wire artifacts <capture_id> --root layeros/state
bonfyre wire recipe <capture_id> --root layeros/state > recipe.json
bonfyre stitch plan recipe.json
```

## Native runtime

Bonfÿre remains aggressively native and local-first where that makes sense.

The public estate includes work across:

```text
C11 runtime and shared libraries
SQLite-backed state and registries
content-addressed artifacts
local embeddings and vector search
speech and media processing
realtime communications
workflow and orchestration
model/provider integration
artifact and format handling
distribution and deployment
```

The architecture does **not** require every capability to be native C. Native implementations, external providers, models, services, humans, and devices can all participate behind capability contracts.

That lets the engine optimize for locality and performance without making implementation language part of the semantic contract.

## Representations and formats

Bonfÿre does not treat every format as a disposable encoding of the same generic blob.

PDF, XLSX, SQLite, Parquet, MP4, GLB, WASM, model formats, packages, and other representations have different:

```text
logical object models
validation rules
access patterns
editability
streaming behavior
canonicalization
finalization
source coordinates
loss characteristics
```

Common artifact machinery is shared where possible, but format-specific semantics are retained when required.

This same rule appears throughout the project:

```text
representation ≠ identity
provider       ≠ capability
projection     ≠ ownership
adapter        ≠ boundary
candidate      ≠ actual
compression    ≠ semantic deletion
```

## AI and models

Bonfÿre can use AI without requiring AI for every execution.

AI may help:

- understand unfamiliar external systems;
- inspect documentation and schemas;
- propose transformations;
- reason over difficult inputs;
- provide generation, vision, speech, embedding, or other capabilities.

Once useful structure is understood and validated, that structure can be represented explicitly and reused without asking a frontier model to rediscover the same solution every time.

The model estate therefore participates as part of the larger engine:

```text
semantic capability
       │
       ▼
model / provider / representation choices
       │
       ▼
local · prepared · remote · specialized
```

## Open-world integrations

Bonfÿre is designed to make useful external systems first-class participants without requiring them to become native Bonfÿre code.

External libraries, services, protocols, repositories, models, and platforms can contribute:

```text
capabilities
representations
events
foreign-owned objects
identity
authority
distribution
host surfaces
```

The intended lifecycle is closer to:

```text
discover useful external specialty
        │
        ▼
understand its shape
        │
        ▼
compile a reusable boundary
        │
        ▼
bind a live connection
        │
        ▼
route real work through it
        │
        ▼
measure actual behavior
```

External does not mean temporary, and native absorption is not automatically the goal.

## Project layout

The exact tree evolves, but the major public areas include:

```text
cmd/              native command/operator surfaces
lib/              shared native libraries
docs/             architecture and subsystem documentation
integrations/     external/runtime integration surfaces
layeros/          state, layer, and registry material
examples/         runnable examples and demonstrations
scripts/          development and operational helpers
```

Inspect the current repository rather than relying on a fixed command count.

## Design principles

Bonfÿre is built around a few recurring constraints:

**Meaning before transport.**  
A valid API call is not useful if it represents the wrong thing.

**Authority travels with semantics.**  
Having a local copy does not make Bonfÿre the owner of foreign truth.

**Capabilities outlive providers.**  
Provider selection can change without changing what the caller asked to accomplish.

**Boundaries are bidirectional.**  
Understanding what enters a system is only half of integration; Bonfÿre also needs to compile semantic intent into the target's valid form.

**Specialization is preserved.**  
Shared machinery should be factored, but genuinely different state regimes, graphs, formats, runtimes, and mathematical kernels should not be flattened for architectural neatness.

**Repeated engineering should become reusable structure.**  
The long-term target is not a larger collection of one-off adapters. It is progressively more reusable semantic, capability, boundary, and execution machinery.

## Documentation

Start with the documentation that matches what you are trying to understand:

| Area | Where to start |
|---|---|
| Architecture | [`docs/architecture.md`](docs/architecture.md) |
| Current runtime/operator status | [`docs/bonfyre_status_and_drift.md`](docs/bonfyre_status_and_drift.md) |
| Wire / observed external systems | [`docs/bonfyre_wire.md`](docs/bonfyre_wire.md) |
| Pipeline | [`docs/pipeline.md`](docs/pipeline.md) |
| API | [`docs/api.md`](docs/api.md) |
| CMS | [`docs/cms.md`](docs/cms.md) |
| Orchestration | [`docs/orchestrate.md`](docs/orchestrate.md) |
| Lambda Tensors | [`docs/lambda-tensors.md`](docs/lambda-tensors.md) |
| Benchmarks | [`docs/benchmarks.md`](docs/benchmarks.md) |

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md).

Useful contributions include:

- bug fixes;
- performance work;
- new capability implementations;
- provider and protocol integrations;
- boundary/schema improvements;
- tests and validation;
- representation/format support;
- local model/runtime work;
- documentation;
- examples;
- language and host bindings.

When adding a new external integration, prefer preserving its real semantics over forcing it into an existing abstraction that does not fit.

When adding a new native capability, prefer sharing existing runtime/compiler machinery where the semantics are genuinely the same.

## License

Bonfÿre is source-available under the
**Bonfÿre Shield License 1.0.0**.

You may inspect, use, modify, and distribute the source subject to the
license terms. The public license does not permit using Bonfÿre to provide
a product or service that competes with Bonfÿre or an Aurekai product built
with Bonfÿre.

Commercial, OEM, hosted, embedded, white-label, resale, and strategic partner
licenses are available separately from Aurekai.

Individual third-party components and selected SDKs may carry their own
licenses. See the applicable source directory and `THIRD_PARTY_NOTICES.md`.

"Bonfÿre", "Aurekai", their logos, and associated branding are not granted
for unrestricted use by the software license.

Made by [Nick Gonzales](https://github.com/Nickgonzales76017).
