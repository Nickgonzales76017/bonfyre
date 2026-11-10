# Claude Code, wired into Aurekai

This directory connects Claude Code to the project's native tools, current
state and architecture registry.

## Hooks

| hook | event | what it does |
|---|---|---|
| `institution-state.mjs` | SessionStart, **PreCompact** | injects live state: atlas counts and maturity spread, the desk's open queues, disk here, working-tree shape |
| `atlas-recall.mjs` | **UserPromptSubmit** | when a prompt names a declared architecture or canonical name, hands over that atlas entry before the answer is written |
| `live-tree-guard.mjs` | PreToolUse (Bash) | denies destructive commands this project has actually been bitten by, with the reason |
| `atlas-guard.mjs` | PostToolUse (Edit/Write) | validates the atlas on every `.yaff` edit and **blocks** on failure |
| `atlas-export-guard.mjs` | **Stop** | if the turn changed an entry, re-validates and re-exports the index |
| `agent-session.mjs` | **SessionStart, Stop, SubagentStop, PreCompact, SessionEnd** | writes the session as an AgentSession event stream |
| `statusline.mjs` | statusLine | `[AUREKAI]` badge, surface up/down, branch, dirty count, atlas counts and drift, open desk queues, disk free — and renders the caveman badge rather than replacing it |

`atlas-recall` looks up declared identifiers mentioned in a prompt before the
agent answers. Each `get` response costs a few hundred bytes. The hook matches
identifiers, not general topic words, and returns at most three entries.

`atlas-export-guard` runs after the turn ends. It validates the registry before
exporting the index, so readers can use the updated declarations. It skips
export on validation failure and does not block the turn.

`institution-state` also runs on `PreCompact` to retain the current state
when the conversation is compacted.

## The session is an observable event stream

`agent-session.mjs` writes what AgentSession already models and Claude Code
already knows: a **model turn** with provider and model identity, a **context
compaction** with distinct before/after references and a stated reason. Events
land in `.bonfyre-runtime/agent-sessions/` (gitignored), appended never
rewritten, with `sequence` monotonic and contiguous per session.

What it deliberately does **not** write is a `ReceiptEnvelope`. A receipt
claiming verified producer fidelity needs a `producer_verifier_ref`, and nothing
here verifies the producer. Every record therefore carries
`"truth": "host_origin_observation_not_receipt_evidence"` and states what it
cannot infer:

- a provider session reference is not an AgentSession identity — the session id
  is minted and stable, and Claude's own id is carried beside it as
  `provider_session_ref`
- host evidence cannot satisfy subject provider fidelity
- provider attribution is not authority, effect safety, quality, ownership or
  evidence sufficiency
- the minted root work node is a coordinate this stream owns; it binds no
  institutional work

## Installing it as one thing

`.claude-plugin/plugin.json` packages the whole surface — hooks, skills,
commands, subagents — as the `aurekai` plugin, so the wiring travels with the
repo instead of living only in one machine's settings.

`atlas-guard` matters because `atlas.mjs validate` otherwise runs at commit — a
maturity claim written at 10am is caught at 5pm, after the reasoning is gone.

`live-tree-guard` encodes real incidents, not hypotheticals: force-unmounting
BonfyreFS wedges the mount; `releases/` holds trees that `*-current` symlinks and
a running service point into; this working tree carries deletions whose only
other copy is an untracked `.archive` file.

## Skills

`atlas`, `estate`, `carriers`, `density` and `desk` can be invoked by name or
matched from their descriptions. They cover hot, shadow and cold material;
the finance book and executable catalogue; the `--long` reader contract;
and the distinction between an `in_progress` record and a running process.

The shared `humanizer` skill in `.agents/skills/humanizer/` applies to all
authored prose. Its `.claude/skills/humanizer` entry links to that same source.
Use `client-sales` as well for commercial work; it links to Humanizer instead
of keeping a second editing policy. See [Writing](../CONTRIBUTING.md#writing).

## Commands

`/desk` `/carriers` `/atlas` `/estate` — the high-frequency reads, each running
its probe up front so the answer starts from live state.

## Subagents

`atlas-keeper`, `carrier-auditor`, `density-analyst`. Each is bound to real
organs and instructed to report nothing rather than manufacture findings.

## MCP

`.mcp.json` defines `aurekai` over HTTP with `${AUREKAI_MCP_TOKEN}` expansion, so
no credential is committed. It is deliberately **not** named `bonfyre`: a
user-scoped `bonfyre` server already works, and shadowing it with a
project-scoped definition whose env var may be unset would break a working
connection.

Note: `settings.local.json` enables eight `.mcp.json` servers — `bonfyre`,
`codex-code`, `bonfyre-dev-fabric`, `bonfyre-agent-hypervisor`,
`bonfyre-fixture-canary-r4`, `bonfyre-native-spines-r7`,
`bonfyre-recursive-substrate-v5`, `bonfyre-recursive-hypervisor-v6` — and none of
them were ever defined in a project `.mcp.json`. They are enabled claims against
nothing. Define the ones that are real; drop the enablement for the ones that are
not, rather than leaving the file asserting servers that do not exist.

## Settings

`settings.json` is shared and carries no secrets or machine paths. It denies
reads of secret material outright, and asks before `sudo rm`, `git push` and
`systemctl disable`. Personal grants stay in `settings.local.json`, which is
gitignored — it currently holds 419 accumulated allow rules.
