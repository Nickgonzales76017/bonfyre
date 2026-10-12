# Bonfyre — working agreement (Claude Code & Codex)

This file is read automatically at the start of every session in this repo. It
applies to **Claude and Codex alike**, without being asked.

## The Architecture Atlas is canonical — use it every session

`architecture/` is the lossless, machine-readable registry of Bonfyre's
architectures. Its simplified diagrams are *generated views*, never canonical.
Full operating rule: `architecture/README.md`.

**Do this automatically, not only when asked:**

1. **Before any architecture work** — consult the atlas instead of re-deriving
   the system from memory:
   - `python3 architecture/atlas.py expand <View>` or read `architecture/atlas.index.json`
   - or the MCP tool `architecture_atlas` (ops: summary/expand/get/cannot_infer/search/loss)
2. **When you build or change an architecture** — update its `architecture/**.yaff`
   entry in the same change: real `source_path` + `witness`, honest `maturity`,
   an `interaction` for any new bridge, and a `forbidden_inference` for what it
   must not be read to imply. Then `python3 architecture/atlas.py export`.
3. **Let the atlas pick the next move when unguided** — `python3 architecture/atlas.py loss`
   names the gaps (unbuilt, unwitnessed, no-interaction-contract). Build toward
   closing them.

## Non-negotiable rules (CI-enforced by `atlas.py validate`)

- **No maturity laundering.** `measured`/`proven` requires a real witness. Never
  let a parent label lend maturity to a child.
- **Collapse only through a view.** A broad name (`Provider`, `Model`) is legal
  only as a view expanding to ≥2 real architectures. The view never replaces its
  children.
- **State what each architecture cannot infer.** The FPQ lesson — low
  reconstruction error is not good generation — is a field, not a footnote.
- **Reachability stops at the human line.** The opportunity engine may compute
  what is reachable; it never auto-commits past a human or external submit.

## Hygiene

- Run `python3 architecture/atlas.py validate` and the control-plane tests
  (`cd 10-Code/BonfyreControlPlane && python3 -m pytest tests/ -q`) before
  committing changes that touch either.
- Install the repo hooks once so validation/export happen on every commit:
  `git config core.hooksPath .githooks` (or `bash scripts/install-hooks.sh`).
- Commit messages: no `Co-Authored-By` trailer; PR bodies: no "Generated with
  Claude Code" footer.
