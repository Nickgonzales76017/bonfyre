# Bonfÿre

**Native software engine, compilers, developer tools and runtime infrastructure.**

Bonfÿre is a software project. It provides reusable native components for building, operating and publishing typed applications and working with local data, models, sources, media, documents, accounts and external systems. Its software and technical releases are independent of any one company's commercial identity.

**Aurekai is a separate operating business and one deployment/consumer of Bonfÿre.** Aurekai's brands, customers, WORK/LIVE/SHOP/LIFE websites, contracts, provider accounts, commercial work and published content are not Bonfÿre assets by virtue of using this engine. Other institutions can use Bonfÿre without becoming part of Aurekai. The [enforced ownership boundary](docs/OWNERSHIP-BOUNDARY.md) sets out how that separation works and what legacy migration remains.

[Quickstart](QUICKSTART.md) · [Technical architecture](docs/architecture.md) · [Source registry](architecture/README.md) · [Native data and frontend](docs/native-data-compute-and-presentation.md) · [Real workflows and exports](docs/real-workflows-and-interoperability.md) · [System diagrams](docs/architecture-visuals/system-architecture.md) · [Contributing](CONTRIBUTING.md)

## Engineering scope

The repository is the **Bonfÿre technical source**, not a combined company website, client portfolio or offer catalogue. Native implementations, source contracts, technical evidence and reproduction procedures live here. Case-specific or institution-owned content appears only as a separately scoped integration, fixture or historically retained source pack.

| Engineering area | Source and implementation | Boundary |
|---|---|---|
| Native engine, identity and permissions | [C engine](engine/core/), [libbonfyre](lib/libbonfyre/), [Matrix](engine/core/include/bf_matrix.h) | a host, binary or source record does not grant institution, customer or effect authority |
| Typed contracts, architecture and compilation | [YAFF source registry](architecture/README.md), [CUE page composition](schemas/control/page-composition.cue), [Zig-to-C application lowerers](cmd/BonfyreWebAddress/tools/compile_site_application_lowerers.zig) | declared capabilities and layouts do not imply live effects |
| Storage, query and analytics | SQLite-first owners, Arrow/Parquet, [BonfyreVec](cmd/BonfyreVec/README.md), [vGPU numeric kernels](cmd/BonfyreSurfaceScene/runtime/README.md), DuckDB-Wasm | backends produce bounded numeric/data results, not customer relationships or factual endorsements |
| Model and representation | [BonfyreEmbed](cmd/BonfyreEmbed/README.md), [BonfyreFPQ](cmd/BonfyreFPQ/README.md), [BonfyreBitNet](cmd/BonfyreBitNet/src/main.c), native model passport and ResourceAdmission owners | reconstruction, learned ranking and model execution have separate quality gates |
| Authoring and public output | [SurfaceScene](cmd/BonfyreSurfaceScene/), [WebAddress](cmd/BonfyreWebAddress/), MD4C/Mustach/C, typed editor adapters, presentation tokens | the actual publication/site owner still controls revision, audience and source rights |
| Source and external systems | [ForeignTwin and ExternalEstate](cmd/BonfyreExternalEstate/), native source/connector owners, Git and Forgejo/Josh | a discovered source or named provider is not a verified installation or credential |
| Work, approvals, proof and economy | WorkGraph, receipts, capabilities, exact machine-readable contracts and authority-gated effects | a work item, proposal or benchmark is not a completed customer action or financial transaction |

The source architecture registry is **not a feature checklist**. Its YAFF records identify what has been implemented, measured or merely declared and what each architecture cannot infer. A native command's presence alone does not verify a deployed application.

## Two technical paths, kept independent and reusable

### 1. Native application and frontend composition

Bonfÿre uses **YAFF** for lossless architectural identities and interaction contracts, **CUE** for typed inputs/layouts, and **C/Zig** compilers/readers for native application behavior. Puck, GrapesJS and Editor.js provide complementary editor interactions over retained typed authoring state. MD4C/Mustach/C produce final server-rendered article, site and application material.

The newer **PostCSS `.pcss` / Tailwind / closed design-token** pipeline unifies component styling across compiled markup, forms, account workspaces and editorial pages. The [published Tailwind configuration](deploy/tailwind/tailwind.config.js) scans real class values in C-produced markup and authoring templates. Some newer `.pcss` inputs and typed authoring files still exist in the local development tree but **have not been synchronized to public `main`**; their presence is not a published release claim.

[Native frontend guide](docs/authoring-frontend-architecture.md) · [Source and maturity details](docs/native-data-compute-and-presentation.md)

### 2. Large-scale local data and numerical processing

[BonfyreEmbed](cmd/BonfyreEmbed/README.md) computes pinned-model ONNX text embeddings. [BonfyreVec](cmd/BonfyreVec/README.md) stores and searches them with SQLite and exact/extension retrieval. The [vGPU kin and spatial kernels](cmd/BonfyreSurfaceScene/runtime/README.md) compute pairwise numeric relationships in bounded tiles, with reported GPU/CPU backend and persisted result rows. DuckDB-Wasm/Parquet is a separate browser-side SQL/query lane.

The source Atlas records a **6,810-item vGPU kin workload comparing 46,376,100 pairs in 1.1 seconds**, with 710 candidates above its stated threshold. A separate browser field used DuckDB-Wasm to read **109,761 Parquet source-channel rows**, displayed through WebGPU. These are different computations; the field does not prove DuckDB SQL executes on vGPU.

**BitNet** can reorder eligible candidate handles only within its recorded rank/admission constraints; **FPQ** measures a different class of compact neural-weight representation and still requires full-model execution/task parity. They cannot inherit each other's scores, source rights or authority.

[Data and frontend architecture](docs/native-data-compute-and-presentation.md) · [FPQ evidence](cmd/BonfyreFPQ/README.md)

## Software boundary: no implicit business tenant

~~~mermaid
flowchart LR
  SDK["Bonfÿre: engine / ABI / source / tests / binary release"] --> CONTRACT["Versioned interface and explicit deployment binding"]
  CONTRACT --> CUSTOMER["Any named institution or operator"]
  CUSTOMER --> DATA["That institution's accounts, content, customers and credentials"]
  CUSTOMER --> RELEASE["Its own public/product release and effect authority"]
  AUREKAI["Aurekai: one independently owned operator"] --> CUSTOMER
  DATA -. "No implicit transfer of ownership" .-> SDK
~~~

**Aurekai is an example of a separate operator**, not a synonym, subsidiary namespace, compatibility spelling, or automatically entitled child of the engine. Engine grants do not give Bonfÿre Aurekai customer records; a business installation does not grant Aurekai the right to alter engine releases. Every effect, publication and provider access has its own institution-owned grant.

The historical source includes some Aurekai-specific hosts, pages, site presentations and example packs in higher-level modules. They remain **known migration debt**. Do not interpret this monorepo's past layout as a single business/legal/permission boundary. The [ownership policy](docs/OWNERSHIP-BOUNDARY.md) explicitly separates source ownership and defines the code/release tests needed before physical migration.

The core C library and runtime shipping paths are protected by a [product-boundary check](scripts/check-product-boundary.py) that refuses new hardcoded Aurekai identifiers or domains. Compatibility and legacy public packages are treated as versioned interfaces, never as authority or brand identity.

## Build and inspect the native software

The project contains different native owners, tests and toolchains. Build the **specific command you intend to exercise**, inspect its source and keep any model/provider/credential requirements explicit. The workspace's local source may be ahead of public `main`.

~~~bash
# Inspect the native architecture registry and its refusal/unknown states.
bun architecture/atlas.mjs summary
bun architecture/atlas.mjs get institutional-root
bun architecture/atlas.mjs search vgpu
bun architecture/atlas.mjs loss

# Exercise the separation gate without external accounts.
python3 scripts/check-product-boundary.py --self-test
python3 scripts/check-product-boundary.py

# Build/test selected owners from their own native Makefiles.
make -C lib/libbonfyre
make -C cmd/BonfyreVec
~~~

These commands require the relevant toolchains and local library dependencies. An uninstalled CUE, ONNX model, sqlite-vec extension or Dawn GPU adapter must be reported as a missing dependency/fallback, not passed off as full feature evidence.

[Quickstart](QUICKSTART.md) · [Architecture commands](architecture/README.md) · [Native ratchet](scripts/native-ratchet.sh)

## Interoperability without product ownership

Bonfÿre includes mechanisms for source and file access, CMS publishing, document generation, WordPress exchange, XLSX review, scoped MCP tools, Git/Forgejo/Josh workflows and external account connections. An **interface** means that the software can exchange a particular declared type of information; it does not imply that any publisher, platform or client is installed, contracted or authorized.

The correct path for application-specific behavior is an **optional bound adapter**: actual customer/deployment configuration and credentials stay with their named owner; Bonfÿre accepts only the resulting narrowed capability and exact data/effect scope. [Real workflows and interoperability](docs/real-workflows-and-interoperability.md) documents the concrete paths and refused inferences.

## Developer navigation

- **[Technical architecture](docs/architecture.md):** engine, command families, explicit bridges, evidence and runtime boundaries.
- **[Architecture Atlas](architecture/README.md):** canonical YAFF source, 25 families, 557 registered architectures and 562 interactions at the recorded November 13, 2026 source cut.
- **[Full architecture guide](docs/architecture-visuals/system-architecture.md):** source-backed diagrams and component map; not a commercial portfolio.
- **[Authoring/frontend](docs/authoring-frontend-architecture.md):** typed page composition, MD4C, editor adapters, presentation and responsive quality.
- **[Local data computation](docs/native-data-compute-and-presentation.md):** YAFF/CUE, Embed/Vec, vGPU, DuckDB/Parquet, BitNet, FPQ and actual provenance.
- **[Ownership and brand separation](docs/OWNERSHIP-BOUNDARY.md):** institutional principal, source, release, business and credential boundaries.
- **[Quickstart](QUICKSTART.md), [Contributing](CONTRIBUTING.md), [Licensing](LICENSE):** development and project terms.

**Aurekai business information belongs to Aurekai's own sites and operating repositories**, not the Bonfÿre engine introduction. [Aurekai's public business destination](https://aurekai.work/) is an example of a business using software; its commercial content, customers and site releases are not asserted here as engine properties.
