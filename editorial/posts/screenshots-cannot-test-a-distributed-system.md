---
slug: screenshots-cannot-test-a-distributed-system
title: A green screenshot can't tell you whether a database lost a write
description: Antithesis and SQLancer offer different ways to challenge software beyond happy-path tests.
category: Standards
icon: network
published: 2026-11-08
status: edited-public
topics: Antithesis, SQLancer, property testing, reliability
origin: External standards, primary documentation and Aurekai integration research
source_url: https://antithesis.com/docs/introduction/how_antithesis_works/
---

A website can look finished and still fail when the network drops between two writes. A database can return the wrong row while every page loads. These are ordinary software problems, but screenshot-oriented acceptance gives them very little attention.

Our research into Antithesis and SQLancer was about broadening what a release test can discover.

## Two complementary approaches

[Antithesis](https://antithesis.com/docs/introduction/how_antithesis_works/) uses deterministic simulation and property-based exploration to exercise many possible sequences of system events. Its [debugging tools](https://antithesis.com/docs/product/debugging/) let a team replay a failure from a particular state.

[SQLancer](https://github.com/sqlancer/sqlancer) generates SQL and uses test oracles to expose incorrect database results, including discrepancies between equivalent query forms. It targets errors in the database engine itself, rather than merely checking an application's known queries.

## The property is the product requirement

For a payment workflow, a useful property might be that a retry cannot charge the customer twice. For a queue, a redelivered item must not create two unrelated business records. For an account workspace, a user from one customer must not gain access to another customer's records.

A conventional browser test can exercise the visible workflow. A fault-injection test can challenge its behavior during interruption. A SQL oracle can question the underlying data processing.

Bonfÿre's WorkGraph, queues, data systems and account applications are suitable places to combine these levels. The aim isn't to collect more green test badges. It is to find the conditions under which a real obligation fails.

A screenshot is useful evidence of presentation. The rest of the system needs its own questions.
