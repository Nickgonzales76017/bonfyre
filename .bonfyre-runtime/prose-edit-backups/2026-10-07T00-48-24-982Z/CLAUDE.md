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
   - `bun architecture/atlas.mjs summary | search <q> | get <id> | cannot-infer <id> | expand <View>`
   - or the MCP tool `architecture_atlas` (same ops, same semantics — both call
     `architecture/atlas-query.mjs`)
   - **Do not read `architecture/atlas.index.json` into context.** It is 258 KB
     (~64k tokens) and `get <id>` answers the same question in ~460 bytes
     (measured: 560× less context per lookup). The registry stays lossless; the
     read path is what has to be cheap.
2. **When you build or change an architecture** — update its `architecture/**.yaff`
   entry in the same change: real `source_path` + `witness`, honest `maturity`,
   an `interaction` for any new bridge, and a `forbidden_inference` for what it
   must not be read to imply. Then `bun architecture/atlas.mjs export`.
3. **Let the atlas pick the next move when unguided** — `bun architecture/atlas.mjs loss`
   names the gaps (unbuilt, unwitnessed, no-interaction-contract). Build toward
   closing them.

## Non-negotiable rules (CI-enforced by `bun architecture/atlas.mjs validate`)

- **No maturity laundering.** `measured`/`proven` requires a real witness. Never
  let a parent label lend maturity to a child.
- **Collapse only through a view.** A broad name (`Provider`, `Model`) is legal
  only as a view expanding to ≥2 real architectures. The view never replaces its
  children.
- **State what each architecture cannot infer.** The FPQ lesson — low
  reconstruction error is not good generation — is a field, not a footnote.
- **Reachability stops at the human line.** The opportunity engine may compute
  what is reachable; it never auto-commits past a human or external submit.

## Claude Code is wired into this repo

`.claude/` makes the tooling operate the institution rather than rediscover it.
Full map: `.claude/README.md`.

- **Skills** carry the operating knowledge — `atlas`, `estate`, `carriers`,
  `density`, `desk`. Reach for one before deriving the same facts by hand.
- **Commands** `/desk` `/carriers` `/atlas` `/estate` start from live state.
- **Subagents** `atlas-keeper`, `carrier-auditor`, `density-analyst` audit real
  organs; each is told to report nothing rather than manufacture findings.
- **Hooks** enforce what CI would otherwise catch hours later: every `.yaff`
  edit is validated and **blocked** on failure, destructive commands against
  live trees are denied with the reason, and each session opens with the atlas
  counts, the desk's open queues and disk already in hand.

Personal permission grants belong in `.claude/settings.local.json` (gitignored).
`.claude/settings.json` is shared and must never carry a secret or a machine path.

## Hygiene

- Run `bun architecture/atlas.mjs validate` and the control-plane tests
  (`bun test components/BonfyreControlPlane/tests`) before
  committing changes that touch either.
- Install the repo hooks once so validation/export happen on every commit:
  `git config core.hooksPath .githooks` (or `bash scripts/install-hooks.sh`).
- Commit messages: no `Co-Authored-By` trailer; PR bodies: no "Generated with
  Claude Code" footer.

## Client work and sales copy

For prospect research, client tools, demo work and outreach, read
`.agents/skills/client-sales/SKILL.md` and use its shared Humanizer editing skill.
The current review batch is 15 drafts for Nick's approval; intro calls are free.
