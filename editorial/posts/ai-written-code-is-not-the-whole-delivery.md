---
slug: ai-written-code-is-not-the-whole-delivery
title: A hundred percent AI-written code tells us who typed it, not what shipped.
description: What Boris Cherny's extraordinary Claude Code output measures, and what it leaves unanswered.
category: Debates
icon: briefcase
published: 2026-11-08
status: edited-public
topics: X, Claude Code, productivity, software economics
origin: External-source commentary with primary X post and related market research
source_url: https://x.com/bcherny/status/2004897269674639461
---

Boris Cherny described an exceptional month in a widely shared [December 2025 post](https://x.com/bcherny/status/2004897269674639461): 259 pull requests, with his contributed code written by Claude Code. There is no need to minimize the achievement. An experienced engineer coordinating agents at that scale shows how much the economics of implementation have changed.

But **percentage of code written by AI** answers only the authorship question. It does not tell us how much a product improved, how much review time was required or what happened after a customer began using the new features.

## Count the right unit

A pull request can be a meaningful subsystem, a documentation correction or a tiny refactor. Lines added and removed are similarly poor units of economic value. A company could double merged lines while creating more defects and support work.

A more useful accounting identity is: *net value of a release = customer outcome − integration effort − future maintenance and risk*. All terms are uncertain, but that uncertainty is precisely why a large number of commits is not enough to calculate productivity.

The [METR 2025 developer study](https://metr.org/blog/2025-07-10-early-2025-ai-experienced-os-dev-study/) found a slowdown under one set of conditions, while the [February 2026 update](https://metr.org/blog/2026-02-24-uplift-update/) suggested newer agents may have improved productivity but emphasized serious selection problems. Those results and Cherny's experience can all be real: they study different tasks and arrangements.

## What the new capacity changes

We are interested in the larger opportunity. A small company can attempt complete business systems rather than single isolated components. But the faster implementation becomes, the more important it is to coordinate applications, source data, permissions, finances and release processes.

Bonfÿre's WorkGraph grew around that idea. Agents can contribute code and carry out bounded tasks; the operating system must still connect their work to actual destinations.

The achievement is not that a model can type everything. It is the possibility of building and operating more useful software with a smaller team.
