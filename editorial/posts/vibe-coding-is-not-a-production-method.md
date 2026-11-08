---
slug: vibe-coding-is-not-a-production-method
title: Karpathy described a weekend experiment. The internet heard a production method.
description: The original vibe-coding post has a boundary that its most enthusiastic interpretations forget.
category: Debates
icon: cpu
published: 2026-11-08
status: edited-public
topics: X, Karpathy, AI programming, software engineering
origin: External-source commentary with primary X post and related market research
source_url: https://x.com/karpathy/status/1886192184808149383
---

In his [February 2025 X post](https://x.com/karpathy/status/1886192184808149383), Andrej Karpathy gave the name *vibe coding* to a delightful experiment: describe the result, let a model write the code, accept changes quickly and keep going even when the implementation becomes hard to follow. It made the experience of making a little application feel remarkably different.

A detail in his description matters: he presented it as something amusing for disposable weekend projects. He did not claim that a company should stop understanding the software on which employees and customers depend.

## Where that method is excellent

For a prototype, the cost of exploring an idea can drop from days to an afternoon. Someone without a conventional programming background can build a personal utility. A designer can try a new interaction in the browser. An engineer can discover what they want before investing in a permanent architecture.

The risk arrives when a prototype inherits responsibilities its author never planned for. A billing system can lose money. A support form can expose customer details. An impressive demo can fail when a second person uses it under a different account. At that point the software needs tests, clear ownership and a way to repair it without guessing what the code means.

## A useful distinction the debate lost

[Simon Willison's 2025 essay](https://simonwillison.net/2025/Mar/19/vibe-coding/) makes a distinction between using AI to write code and *vibe coding* as deliberately not reviewing the implementation. The former can include responsible engineering. The latter is an unusually accessible form of experimentation.

We have encountered that difference throughout Aurekai. A room planner becomes a real shopping application only when it preserves the selected product and price. A generated CRM screen becomes useful only when it opens the customer's actual records under the correct account.

The response is not to reject Karpathy's idea. It is to keep his original context intact. Experiment freely, then do the additional work the product requires when somebody else begins depending on it.
