---
slug: two-coding-agents-one-worktree
title: What happens when two coding agents disagree?
description: Parallel implementation is valuable when each worker has an independent role and a known source revision.
category: Engineering
icon: network
published: 2026-11-07
status: edited-public
topics: agents, parallelism, code review
origin: Codex and Claude architecture reviews, July–November 2026
---

We tried running several agents against Bonfÿre changes. One proposed an implementation, another reviewed it, and a third challenged the review against the actual repository. The experiment made something clear: running more agents is not the same as getting more reliable work.

More workers create more ideas and more code. They can also produce conflicting assumptions and overlapping changes.

## Give each worker a real role

The useful split was between planning, implementation, criticism and final verification. The planner inspected existing owners and dependencies. The implementer used an isolated worktree. The reviewer looked for bugs and missing tests without changing the code being judged.

A reviewer can be mistaken too. We wanted valid criticisms fixed and invalid ones answered with exact source evidence. Having two models agree is not enough.

## Know what each agent actually changed

When two sessions edit the same file, one can invalidate the other's starting assumptions. The system needs to know which revision a worker saw, which files were in scope and whether the resulting patch still applies.

Bonfÿre's [Project and MCP work](https://github.com/Nickgonzales76017/bonfyre/blob/main/mcp/README.md) reflects those lessons. Agents can work in parallel without allowing one to silently approve another's result.

The goal is productive disagreement. A good review leaves the change clearer and safer than any one agent could make it.
