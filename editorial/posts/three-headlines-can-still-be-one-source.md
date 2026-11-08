---
slug: three-headlines-can-still-be-one-source
title: Three headlines repeating a claim can still be one observation
description: Why dependent evidence fools simple Bayesian confidence scoring.
category: Mathematics
icon: archive
published: 2026-11-08
status: edited-public
topics: Bayes, research, evidence correlation
origin: Mathematical research notes from archived experiments and external literature
source_url: https://plato.stanford.edu/entries/bayes-theorem/
---

A research feed finds three articles saying a supplier is shutting down. A simple system sees three confirmations and reports high confidence. A more careful system notices that all three articles copied the same notice.

The arithmetic behind the mistake is straightforward. Bayes' rule is powerful, but multiplying likelihood ratios as though every observation were independent is not justified when the observations share an origin.

## A worked example

Suppose the prior odds that a rumor is true are 1:9. A genuinely independent positive signal has likelihood ratio 4. After one signal the odds become 4:9, which is about 30.8% probability.

If three such signals were independent, the odds would be 64:9, or about 87.7%. But if all three headlines copied the first report, we may still have only one piece of evidence. Treating them as three can inflate the estimate by nearly 57 percentage points in this illustrative calculation.

The exact prior and likelihood ratio would have to be justified for a real question. The structure of the error is general.

## Source lineage is part of the statistical model

A useful research system should record the original URL, publisher, observation time, citations and whether a document is a reprint or independent observation. Only then can it decide how much additional information a new article supplies.

This is why Source Library and external-estate work need relationships rather than a flat count of hits. A thousand mirrors of a manufacturer press release may be helpful for discoverability while contributing almost no new evidence.

For a mathematical starting point see the [Stanford Encyclopedia's treatment of Bayes' theorem](https://plato.stanford.edu/entries/bayes-theorem/). For Aurekai, the lesson is practical: an explanation is only as strong as the independent observations behind it.

Confidence should increase when we learn something new, not simply when the same statement appears on more screens.
