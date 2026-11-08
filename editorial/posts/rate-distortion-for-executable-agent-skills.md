---
slug: rate-distortion-for-executable-agent-skills
title: A compressed agent skill matters only if it can still do the job
description: Task-conditioned rate–distortion explains why elegant memory summaries can fail at execution.
category: Mathematics
icon: cpu
published: 2026-11-08
status: edited-public
topics: information theory, rate distortion, agent memory
origin: Mathematical research notes from archived experiments and external literature
source_url: https://doi.org/10.1002/j.1538-7305.1948.tb01338.x
---

Several of our older experiments asked whether a long interaction could be reduced to a compact reusable operator. We could compress a task description, store a vector and retrieve something semantically close. That still left the important question unanswered: could a different worker actually carry out the task from the compressed material?

[Shannon's information-theory work](https://doi.org/10.1002/j.1538-7305.1948.tb01338.x) offers a useful mathematical language. Rate–distortion theory studies how much information must be retained to reproduce a source within a chosen error.

## Choose a task-aware error function

Let S be the original skill and Z its compressed representation. A generic compressor might minimize the distance between the original text and its reconstructed summary. For a skill, we care about what happens when it is executed.

Define a task loss D(Z) = E[ L(run(decode(Z), task), correct outcome) ]. We seek a low-information representation Z subject to D(Z) not exceeding an acceptable threshold.

This is a proposed research objective, not a demonstrated theorem about Bonfÿre. Its value is that it names the failure mode clearly. A summary can preserve every broad idea in a database migration and still omit the one condition that prevents an invoice from being imported twice.

## An experiment someone else could reproduce

Take 100 tasks with stable test environments. Record successful implementations, including tools, inputs, preconditions and exception cases. Produce compressed representations at several token budgets. Give each to a fresh worker without the original context, and measure completion on held-out cases.

Plot task success against bits retained. Include tasks on which the correct action is to decline because required authority or data is missing. Measure interference when the same compressed skill is reused in a different account.

The [Gray–Neuhoff survey of quantization](https://doi.org/10.1109/18.720541) is a useful foundation for distinguishing representation size from reconstruction quality. What changes here is the distortion: our system cares about executable outcomes.

The smallest skill is not the one with the fewest words. It is the smallest representation that preserves the distinctions necessary for real work.
