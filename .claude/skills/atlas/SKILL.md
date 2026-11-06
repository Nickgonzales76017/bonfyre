---
name: atlas
description: The canonical architecture registry — consult before any architecture work, update in the same change that builds something. Use when asked what the system is, what an organ can or cannot do, what is unbuilt, or when adding or changing an architecture.
allowed-tools: Bash, Read, Edit
---

# The atlas

`architecture/` is the lossless, machine-readable registry of what this system
actually is. Its diagrams are generated views, never canonical.

## Read it cheaply

```bash
bun architecture/atlas.mjs summary
bun architecture/atlas.mjs search <query>
bun architecture/atlas.mjs get <id>
bun architecture/atlas.mjs cannot-infer <id>
bun architecture/atlas.mjs expand <View>
bun architecture/atlas.mjs loss        # what is unbuilt, unwitnessed, uncontracted
```

Or the `architecture_atlas` MCP tool, same ops.

**Never read `atlas.index.json` into context.** It is ~426 KB. `get <id>` answers
the same question in about 460 bytes — measured at 560× less context per lookup.
The registry is right to be lossless; the path a model reads it through is what
has to be cheap. If the tool reports `atlas_query_absent`, the read path is not
installed under `BONFYRE_ROOT` — say so rather than falling back to grep and
calling it equivalent, because source search matches text while the atlas answers
about declared maturity, witnesses and forbidden inferences.

## Write it in the same change

Building or changing an architecture means updating its `.yaff` entry in the same
commit: a real `source_path`, a real `witness`, honest `maturity`, an
`interaction` for any new bridge, and a `forbidden_inference` naming what it must
not be read to imply. Then `bun architecture/atlas.mjs export`.

## The four laws

1. **No maturity laundering.** `measured` or `proven` requires a real witness.
   A parent never lends maturity to a child.
2. **Collapse only through a view.** A broad name (`Provider`, `Model`) is legal
   only as a view expanding to two or more real architectures. The view never
   replaces its children.
3. **State what each architecture cannot infer.** The FPQ lesson — low
   reconstruction error is not good generation — is a field, not a footnote.
4. **Reachability stops at the human line.** The opportunity engine may compute
   what is reachable; it never auto-commits past a human or an external submit.

A `PostToolUse` hook validates on every `.yaff` edit and blocks on failure, so a
violation surfaces while the reasoning is still in hand rather than at commit.

## When unguided

`atlas.mjs loss` names the gaps. Build toward closing them.
