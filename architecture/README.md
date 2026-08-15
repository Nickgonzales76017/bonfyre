# Architecture Atlas

A **lossless registry** of Bonfyre's architectures whose simplified diagrams are
*generated views*, never canonical definitions. At Bonfyre's size, synthesis has
become lossy: each new umbrella term ("Model", "Provider", "Evidence") quietly
swallows a dozen real systems. The atlas is the opposite discipline — preserve
every architecture, and let collapse be a **zoom**, not a deletion.

This directory is machine-readable and self-enforcing. `atlas.py` is the engine;
the `.yaff` files are the data; the constitution below is checked in CI.

## Operating rule — for every call, mine and codex

1. **Consult the atlas before architecture work.** `python3 atlas.py expand <View>`
   or read `atlas.index.json`. Do not re-derive the system from memory.
2. **Every new architecture gets an entry**, at three levels: DOMAIN →
   ARCHITECTURE → INTERNAL CALCULI. Never stop at a domain label.
3. **Update the atlas when you build or change an architecture** — add real
   `source_path` and `witness` lines, set honest `maturity`, and add an
   `interaction` for any new bridge. Then `python3 atlas.py export`.
4. **Collapse only through a `view`.** A name like `Provider` is legal only as a
   view expanding to ≥2 real architectures. The view never replaces its children.
5. **No maturity laundering.** `measured`/`proven` requires a real witness or
   `validate` fails. A parent label cannot lend its maturity to a child.
6. **State what each architecture cannot infer** (`forbidden_inference`). The
   FPQ lesson — *low reconstruction error is not good generation* — is a field,
   not a footnote.

## Commands

```
python3 atlas.py validate        # enforce the constitution (CI gate; exit 1 on violation)
python3 atlas.py loss            # what the atlas knows it is missing
python3 atlas.py maturity        # honest maturity rollup by family
python3 atlas.py expand <View>   # reversible expansion (zoom)
python3 atlas.py view <View>     # render a collapsed, expandable diagram
python3 atlas.py export          # write atlas.index.json for MCP / BonfyreFS
```

## Consumers

- **CI** — `.github/workflows/architecture-atlas.yml` runs `validate` on every
  change; a laundering claim or a view that swallows its children fails the build.
- **MCP** — `atlas.index.json` is the queryable surface; the project MCP node
  serves atlas queries from it (ties into the `native_tool_*` work).
- **BonfyreFS** — the introspection namespace `/Bonfyre/Actual/Graphs/…` is a
  projection of this registry, so the architecture itself becomes inspectable.
- **Repo cleanup** — an old artifact may only move to `origins/research` once its
  architectural content (a unique node type, edge, invariant, algorithm, command,
  interaction, failure mode) is captured here. Cleanup becomes semantic harvesting.

## Layout

```
architecture/
├── atlas.py            engine (stdlib only; parser + constitution + views + loss)
├── schema.yaff         the meta-schema (human contract)
├── views.yaff          collapsed views → real children (reversible)
├── interactions.yaff   cross-architecture contracts (preserve / drop / forbid)
├── atlas.index.json    generated consumable (export)
├── <family>/*.yaff     one file per family, one block per architecture
└── tests/test_atlas.py the constitution's own tests
```

The three recovered design documents (recursive organ architecture, its second
snapshot, native-format architecture) are **seed material** for mining, not the
canonical end-state. The canonical end-state is generated from this atlas.

## Status

Seeded from the code that actually exists — the control plane, the FPQ estate,
transport, ForeignTwin/ServiceLifecycle, forms, actors, the bernstein receipt
bundle. Architectures with no implementation yet are entered honestly at
`architectural` maturity and surface in `atlas.py loss` as the next mining
targets, rather than being pretended into existence.
