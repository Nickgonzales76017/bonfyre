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

## New behaviour is C over YAFF/CUE — the native ratchet

The JS/script estate is being retired onto C (`docs/engineering/mjs-retirement-20261022/PLAN.md`).
`scripts/native-ratchet.sh check` holds it to `architecture/native-ratchet.cue`:
authored `.mjs` files and lines, other scripts (`.js .ts .py`), `bun build --compile`
sites and JS-runtime programs in `~/.bonfyre/bin` may only go down. It runs in the
pre-commit hook, in CI, and on every script write by Claude.

- Build new organs and features in C over `lib/libbonfyre`, with shapes declared
  in YAFF and constrained in CUE. Emit JSON with `bf_jsonw` (byte-identical to
  `JSON.stringify`); never hand-print JSON.
- Browser code, tests of JS still in use, and thin edges may stay JS, but a net
  addition must be paid for: port or retire an equal amount.
- Retire replaced files with `scripts/retire.sh NAME REASON PATH...` (verified
  zstd archive, then removal). After a win, `scripts/native-ratchet.sh tighten`.
- An installed program is never a packed JS runtime; `bun build --compile` is refused.

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

## Writing for people

Use `.agents/skills/humanizer/SKILL.md` for all authored human-readable prose,
including UI text, docs, release notes, support replies, design prompts and agent
updates, not only sales. Follow `CONTRIBUTING.md#writing`; use embedded mode for
finished text. Preserve formal contracts, code, commands, identifiers, quotations
and historical evidence. Edit the shared source rather than generated copies.
