---
description: Query the canonical registry, or name what it says is unbuilt
allowed-tools: Bash(bun:*), Read, Skill
argument-hint: "[search term | architecture id | 'loss']"
---

Query the atlas for: $ARGUMENTS

Run the matching bounded query with `bun architecture/atlas.mjs` from the
project root (`summary`, `search <query>`, `get <id>`, or `loss`). Use `summary`
when no argument was provided.

Answer from the registry rather than from memory of the codebase. Do not read
atlas.index.json. If the query was `loss`, pick the gap whose closure would make
the most difference and say why that one.
