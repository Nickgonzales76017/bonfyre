---
description: Query the 6.5M-observation receipted estate
allowed-tools: Bash(ssh:*), Bash(python3:*), Skill
argument-hint: "<question about parcels, owners, values or permits>"
---

Answer from the estate: $ARGUMENTS

Use the `estate` skill for the composed hot-shadows-cold view and the column
names. Compose one SELECT, run it through `estate_query`, and report the result
as a person would want it — thousands grouped, money as money.

State what the answer cannot support: these are observations of a published
record, not title, not appraisal, and not permission to contact anyone.
