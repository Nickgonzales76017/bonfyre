---
slug: why-the-cheapest-token-isnt-the-cheapest-answer
title: The cheapest AI token is not necessarily the cheapest completed task
description: A simple expected-cost model for selecting models, agents and native execution backends.
category: Mathematics
icon: cpu
published: 2026-11-08
status: edited-public
topics: cost optimization, models, compute, expected loss
origin: Mathematical research notes from archived experiments and external literature
source_url: https://developers.cloudflare.com/queues/reference/delivery-guarantees/
---

When comparing model providers, the first number people often find is cost per million tokens. It is easy to place those prices in a table and choose the lowest. But a business pays for useful outcomes, retries, review and failures, not just generated tokens.

The correct decision may favor a more expensive model or even a non-model program when the task is predictable.

## Put the outcome in the equation

A rough expected cost is: **C = inference + latency cost + review cost + probability of failure × failure cost**. Each term depends on the task and the environment.

Consider two hypothetical routes for processing a transaction. Route A costs $0.01 per call but fails 10% of the time; each failure incurs $2 in rework. Its simplified expected cost is $0.21. Route B costs $0.05 but fails 2% of the time, yielding $0.09 expected cost. The 'cheap' call becomes the more expensive workflow.

Those probabilities are invented to explain the model, not performance claims about any provider.

## Native tools change the comparison

If the task is validating a known schema or calculating a tax-independent subtotal, a deterministic native command may avoid inference entirely. Models make more sense for ambiguity, language or reasoning where a fixed parser cannot handle the variety.

Bonfÿre's capability and resource placement work is intended to compare available routes under their actual constraints, including memory, latency, permissions and accounts.

Reliability is also affected by transport. [Cloudflare's at-least-once queue documentation](https://developers.cloudflare.com/queues/reference/delivery-guarantees/) explains why consumers must be prepared for duplicate deliveries; a retry-safe application has different cost and risk from a naive one.

Choosing a model should begin with the task, its failure cost and available deterministic alternatives. The token price comes after that.
