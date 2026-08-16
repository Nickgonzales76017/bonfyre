# Bonfyre — agent working agreement (Codex, Claude, any coding agent)

Read automatically at session start. Same rules as `CLAUDE.md`; this is the
Codex-facing copy so both agents share one agreement.

## Use the Architecture Atlas every session — without being asked

`architecture/` is the canonical, lossless architecture registry; simplified
diagrams are generated views, never canonical. Full rule: `architecture/README.md`.

- **Before architecture work:** consult the atlas, don't re-derive from memory —
  `python3 architecture/atlas.py expand <View>` / `get <id>` / `loss`, or read
  `architecture/atlas.index.json`, or MCP tool `architecture_atlas`.
- **When you build/change an architecture:** update its `architecture/**.yaff`
  entry in the same change (real `source_path` + `witness`, honest `maturity`, an
  `interaction` for any new bridge, a `forbidden_inference`), then
  `python3 architecture/atlas.py export`.
- **When unguided:** `python3 architecture/atlas.py loss` names the next targets.

## CI-enforced invariants (`python3 architecture/atlas.py validate`)

- No maturity laundering — `measured`/`proven` needs a witness.
- Collapse only through a view (≥2 real children; reversible).
- Every architecture states its `forbidden_inference`.
- Reachability (opportunity engine) never auto-commits past the human line.

## Before committing

- `python3 architecture/atlas.py validate` and `cd 10-Code/BonfyreControlPlane && python3 -m pytest tests/ -q`.
- Install hooks once: `git config core.hooksPath .githooks`.
- No `Co-Authored-By` commit trailer; no "Generated with Claude Code" PR footer.
