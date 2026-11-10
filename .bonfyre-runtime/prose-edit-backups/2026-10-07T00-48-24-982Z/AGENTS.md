# Bonfyre — agent working agreement (Codex, Claude, any coding agent)

Read automatically at session start. Same rules as `CLAUDE.md`; this is the
Codex-facing copy so both agents share one agreement.

## Use the Architecture Atlas every session — without being asked

`architecture/` is the canonical, lossless architecture registry; simplified
diagrams are generated views, never canonical. Full rule: `architecture/README.md`.

- **Before architecture work:** consult the atlas, don't re-derive from memory —
  `bun architecture/atlas.mjs summary` / `search <q>` / `get <id>` /
  `cannot-infer <id>` / `expand <View>` / `loss`, or MCP tool
  `architecture_atlas` (same semantics, one implementation). Don't read
  `architecture/atlas.index.json` into context: 258 KB vs ~460 bytes for `get`.
- **When you build/change an architecture:** update its `architecture/**.yaff`
  entry in the same change (real `source_path` + `witness`, honest `maturity`, an
  `interaction` for any new bridge, a `forbidden_inference`), then
  `bun architecture/atlas.mjs export`.
- **When unguided:** `bun architecture/atlas.mjs loss` names the next targets.

## CI-enforced invariants (`bun architecture/atlas.mjs validate`)

- No maturity laundering — `measured`/`proven` needs a witness.
- Collapse only through a view (≥2 real children; reversible).
- Every architecture states its `forbidden_inference`.
- Reachability (opportunity engine) never auto-commits past the human line.

## Before committing

- `bun architecture/atlas.mjs validate` and `bun test components/BonfyreControlPlane/tests`.
- Install hooks once: `git config core.hooksPath .githooks`.
- No `Co-Authored-By` commit trailer; no "Generated with Claude Code" PR footer.

## Client work and sales copy

For prospect research, client tools, demo work and outreach, read
`.agents/skills/client-sales/SKILL.md` and use its shared Humanizer editing skill.
The current review batch is 15 drafts for Nick's approval; intro calls are free.
