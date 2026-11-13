# Architecture Atlas

A **lossless registry** of Bonfÿre's architectures whose simplified diagrams are
*generated views*, never canonical definitions. At Bonfÿre's size, synthesis has
become lossy: each new umbrella term ("Model", "Provider", "Evidence") quietly
swallows a dozen real systems. The atlas is the opposite discipline — preserve
every architecture, and let collapse be a **zoom**, not a deletion.

This directory is machine-readable and self-enforcing. `atlas.mjs` is the engine;
the `.yaff` files are the data; the constitution below is checked in CI.

**Visual companion:** [Architecture in pictures](../docs/architecture-visuals/README.md) provides native SVG and Mermaid views of site addressing, installed applications, publishing, execution, business grammars and spatial commerce. The visuals never supersede this registry or its witnesses.

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
