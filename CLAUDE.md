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
   - or the MCP tool `architecture_atlas` (same ops, same semantics — both run
     the native `cmd/BonfyreAtlas` program)
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

## See a page change in seconds — `make -C cmd/BonfyreWebAddress preview`

Builds the tree's `bonfyre-web-address` and serves it on `:28911` against
copies of the live stores in `~/.bonfyre/preview` (never the live ones; the
live service and its release are untouched). About 15 s from edit to page.
`preview-refresh` re-copies the stores; `preview-stop` stops it.
`curl -H 'Host: aurekai.work' -H 'Accept: text/html' http://127.0.0.1:28911/<route>`,
or Chrome with `--host-resolver-rules="MAP aurekai.work:80 127.0.0.1:28911"`
and `http://aurekai.work/<route>` (assets only answer for estate hosts).
Check pages here before staging a release.

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
