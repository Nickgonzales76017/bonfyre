# Architecture Atlas

A **lossless registry** of Bonfÿre's architectures whose simplified diagrams are
*generated views*, never canonical definitions. At Bonfÿre's size, synthesis has
become lossy: each new umbrella term ("Model", "Provider", "Evidence") quietly
swallows a dozen real systems. The atlas is the opposite discipline — preserve
every architecture, and let collapse be a **zoom**, not a deletion.

This directory is machine-readable and self-enforcing. `atlas.mjs` is the engine;
the `.yaff` files are the data; the constitution below is checked in CI.

**Full-system visual companion:** [Bonfÿre native software architecture](../docs/architecture-visuals/system-architecture.md) is the broad entry point (13 GitHub-rendered Mermaid views and a source-linked coverage index). The original [six-diagram gallery](../docs/architecture-visuals/README.md) is a collection of specific examples, not a substitute for the mapping, temporal, Git/Josh, representation, group/connection, model, provider, media, finance, work and protocol systems. **No visual supersedes this registry or its witnesses.**

**Frontend and authoring are system infrastructure, not a commercial demo.** [Unified authoring and native frontend](../docs/authoring-frontend-architecture.md) maps Puck, GrapesJS, Editor.js, MD4C, Mustach, typed components, native C output, responsive design, interaction grammars, scene compilation and source/permission continuity across communities, research, education, publishing, profiles and installed applications. Local development source and public GitHub source are on separate release cuts; follow the guide's verified-link distinctions.

**Real-workflow companion:** [Operational workflows and exchange boundaries](../docs/real-workflows-and-interoperability.md) documents real WordPress bridge operations, WXR/Frappe/HTML export, cPanel/InterWorx/Plesk inventories, database comparison, XLSX financial reviews, scanned-PDF OCR, Pandoc deliveries, Project/Workspace/Workbook/Site MCP, React/Puck versus unverified Svelte conversion, and YouTube/Patreon/Spotify/Toast maturity. It also preserves group/community, creator-audio, training, spatial, Git, procurement, publication and model work as **different owners**, not as a list of Matrix capabilities.

**Three distinct external inventories:** [200 browser enrollment targets](../integrations/browser/onboarding/targets.tsv) (provider/account acquisition), [104 provider-estate prospects](../cmd/BonfyreProviderEstate/providers.tsv) (downstream tenancy/partner routes), and [20 native interaction-grammar references](../cmd/BonfyreInteractionGrammar/src/main.c) (studied UI operations). Their counts describe different things. Do not add them together or call every entry a verified integration.

**Required user-facing explanation:** for any bridge or operation, name **source bytes/coordinates → exact native owner → transformation → exported/readable output → intended consumer → known test → unverified external acceptance**. An Atlas ID is a lookup handle, not an explanation of innovation. A diagram that sends everything through Matrix, WorkGraph, MCP or a generic capability service is *wrong* when the source contains independent adapters/owners.

**Do not collapse a specific offer into an engine family.** A room-planning offer is not the SpatialObservation/BonfyreSpatial architecture; a narrated feed is not the media engine; a GitHub Pages site is not the entire authoring/publication system. The registry's domain → architecture → internal calculi and interaction/witness layers are the authoritative levels. A compiled or implemented declaration is not proof that its whole end-user journey has been deployed.

## Operating rule — for every call, mine and codex

1. **Consult the atlas before architecture work.** `bun atlas.mjs get <id>` /
   `search <q>` / `summary` / `expand <View>`. Do not re-derive the system from
   memory, and do not read `atlas.index.json` into a model context: it is 258 KB
   against ~460 bytes for the same answer via `get`.
2. **Every new architecture gets an entry**, at three levels: DOMAIN →
   ARCHITECTURE → INTERNAL CALCULI. Never stop at a domain label.
3. **Update the atlas when you build or change an architecture** — add real
   `source_path` and `witness` lines, set honest `maturity`, and add an
   `interaction` for any new bridge. Then `bun atlas.mjs export`.
4. **Collapse only through a `view`.** A name like `Provider` is legal only as a
   view expanding to ≥2 real architectures. The view never replaces its children.
5. **No maturity laundering.** `measured`/`proven` requires a real witness or
   `validate` fails. A parent label cannot lend its maturity to a child.
6. **State what each architecture cannot infer** (`forbidden_inference`). The
   FPQ lesson — *low reconstruction error is not good generation* — is a field,
   not a footnote.

## Native contracts, data computation and the user-facing result

The [native computation and presentation guide](../docs/native-data-compute-and-presentation.md) follows **distinct** systems through to actual readers: YAFF architecture/interactions, CUE page and lowerer validation, Zig/C native frontend descriptors, local PostCSS/Tailwind styling, Puck/GrapesJS/Editor.js, ONNX Embed, SQLite Vec, vGPU numerical neighbor/kin kernels, DuckDB-Wasm/Parquet source tables, and BitNet/FPQ representation gates.

The Atlas should describe the **relation** between these operations, preserving exact input fields, reader/backends and their refusal rules. A source vector's model/dimensions and a vGPU similarity cutoff remain separate facts; a source table's candidate state is not permission to publish it; a measured FPQ tensor is not a completed model; an authored CUE layout is not a signed-in CRM write.

**Concrete witnesses:** [ItemArchive](surface/item-archive.yaff) records 6,810 items and 46,376,100 kin pair comparisons in one vGPU run; [the estate field](surface/client-organs.yaff) records 109,761 Parquet rows read by DuckDB-Wasm and visualized by WebGPU. [BitNet](../cmd/BonfyreBitNet/src/main.c) remains bounded to AddressPlane survivors; [FPQ's tensor receipt](../cmd/BonfyreFPQ/results/2026-09-11-qwen2.5-coder-0.5b-real-tensor-v9.json) is explicitly one reconstruction trial. New local PCSS and authoring-tree sources remain ahead of GitHub `main`.

## Commands

```
bun atlas.mjs summary         # shape of the whole atlas in ~800 bytes
bun atlas.mjs get <id>        # one architecture record, bounded
bun atlas.mjs search <query>  # content search (values, never field names)
bun atlas.mjs cannot-infer <id>  # what it may NOT be read to imply
bun atlas.mjs validate        # enforce the constitution (CI gate; exit 1 on violation)
bun atlas.mjs loss            # what the atlas knows it is missing
bun atlas.mjs maturity        # honest maturity rollup by family
bun atlas.mjs expand <View>   # reversible expansion (zoom)
bun atlas.mjs view <View>     # render a collapsed, expandable diagram
bun atlas.mjs export          # write atlas.index.json for MCP / BonfyreFS
bun atlas.mjs fs [dir]        # generate the /Bonfyre/Actual/Graphs introspection tree
```

## Consumers

- **CI** — `.github/workflows/architecture-atlas.yml` runs `validate` on every
  change; a laundering claim or a view that swallows its children fails the build.
- **MCP** — the project MCP node's `architecture_atlas` tool calls
  the native `cmd/BonfyreAtlas/bonfyre-atlas query`, so the MCP transport and the CLI cannot drift into
  two different answers (ties into the `native_tool_*` work).
- **BonfyreFS** — the introspection namespace `/Bonfyre/Actual/Graphs/…` is a
  projection of this registry, so the architecture itself becomes inspectable.
- **Repo cleanup** — an old artifact may only move to `origins/research` once its
  architectural content (a unique node type, edge, invariant, algorithm, command,
  interaction, failure mode) is captured here. Cleanup becomes semantic harvesting.

## Layout

```
architecture/
├── atlas.mjs           shim: runs the native atlas (cmd/BonfyreAtlas, C over YAFF + CUE)
├── atlas-native.sh     builds the native atlas when stale and runs it
├── atlas-fields.cue    field and maturity schema
├── schema.yaff         the meta-schema (human contract)
├── views.yaff          collapsed views → real children (reversible)
├── interactions.yaff   cross-architecture contracts (preserve / drop / forbid)
├── atlas.index.json    generated consumable (export)
├── <family>/*.yaff     one file per family, one block per architecture
└── (vectors) cmd/BonfyreAtlas/tests/parity.sh, recorded from the retired JS atlas
```

The three recovered design documents (recursive organ architecture, its second
snapshot, native-format architecture) are **seed material** for mining, not the
canonical end-state. The canonical end-state is generated from this atlas.

## Status

Seeded from the code that actually exists — the control plane, the FPQ estate,
transport, ForeignTwin/ServiceLifecycle, forms, actors, the bernstein receipt
bundle. Architectures with no implementation yet are entered honestly at
`architectural` maturity and surface in `atlas.mjs loss` as the next mining
targets, rather than being pretended into existence.
