---
slug: metr-study-and-the-missing-counterfactual
title: The METR productivity studies are about counterfactuals, not one eternal percentage
description: How task selection and changing agent workflows complicate developer-productivity estimates.
category: Mathematics
icon: file
published: 2026-11-08
status: edited-public
topics: statistics, METR, agents, experiments
origin: Mathematical research notes from archived experiments and external literature
source_url: https://metr.org/blog/2026-02-24-uplift-update/
---

The headline from [METR's July 2025 randomized study](https://metr.org/blog/2025-07-10-early-2025-ai-experienced-os-dev-study/) was surprising: the experienced open-source developers in the study took about 19% longer when AI tools were available. That result deserves to be taken seriously. It does not settle whether AI helps every developer on every task.

In [February 2026](https://metr.org/blog/2026-02-24-uplift-update/), METR explained that a newer experiment had become difficult to interpret. Developers who expected especially large benefits were more reluctant to accept no-AI tasks, and some declined participation altogether.

## What are we estimating?

Let Y(1) be the time a task would take with AI and Y(0) without it. The quantity of interest is the average difference across a defined set of tasks. Randomization helps make the comparison fair only if the tasks in the experiment represent the work we care about.

But an agent changes the tasks a team chooses. A person may attempt a larger refactor, more experiments or unfamiliar work that was not economical before. The task distribution itself moves.

Imagine one familiar bug that takes 100 minutes unaided but 120 with AI because the developer must check a large diff. Another task takes 80 unaided and 20 with AI. Over an equal mix, AI helps on average; on the first task alone it hurts. Neither observation is false.

## Keep value separate from time

Time saved is not identical to value created. A two-hour new feature may be more valuable than six minor fixes, or it may create expensive support work. [METR's May 2026 analysis](https://metr.org/blog/2026-05-08-task-substitution-and-uplift/) distinguishes productivity on old tasks, new tasks and value.

We should do the same when measuring agent work: define the task population, include review time and failed attempts, and record the outcome in the receiving application. The interesting question is how much more useful work the organization can deliver, not whether a benchmark screenshot contains a large percentage.
