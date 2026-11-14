---
slug: differential-privacy-is-not-a-substitute-for-data-governance
title: Differential privacy helps with a query. It doesn't decide who may own the data.
description: What epsilon protects and what still needs source identity, consent and access rules.
category: Standards
icon: archive
published: 2026-11-08
status: edited-public
topics: OpenDP, differential privacy, data governance
origin: External standards, primary documentation and Aurekai integration research
source_url: https://docs.opendp.org/en/stable/api/index.html
---

Differential privacy has an appealing promise: release useful aggregate statistics while limiting how much the result reveals about an individual. The mathematics is important, and projects such as [OpenDP](https://docs.opendp.org/en/stable/api/index.html) make these mechanisms easier to work with.

A mistake is to treat a differential-privacy mechanism as a general permission to collect, reuse or republish personal information. Those are different decisions.

## What epsilon means

In an idealized epsilon-differentially private mechanism, the probability of an output changes by at most a factor of exp(epsilon) between neighboring datasets that differ in one protected individual's contribution.

Smaller epsilon generally means a stronger mathematical privacy guarantee, at the cost of more noise or less accurate output. The exact guarantee depends on the neighboring relation, mechanism and assumptions.

When several queries are released, their privacy losses compose. Ten individually protected releases are not automatically equivalent to one release with the same epsilon.

## What the mathematics cannot supply

A publisher still needs a lawful source and a reason to process the data. A customer database still needs controls on who may query it. A private individual's records should not become public simply because an aggregate visualization is available.

For Aurekai, the source and institutional owners need to decide permissible transformations. A differential-privacy mechanism is one possible implementation when an aggregate is justified.

This matters for civic analytics, community statistics, workplace reporting and medical or financial research. The proper question is whether a specific release serves a legitimate purpose under a suitable privacy definition.

Privacy mathematics can be powerful. It works best as part of governance, not as a replacement for it.
