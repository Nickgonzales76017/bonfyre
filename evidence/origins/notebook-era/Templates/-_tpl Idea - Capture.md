<%*
const title = tp.file.title;
const created = tp.date.now("YYYY-MM-DD");
const project_link = `[[Project - ${title}]]`;
-%>
---
type: idea
title: <%= title %>
created: <%= created %>
updated: <%= created %>
status: draft
stage: exploration
system_role: Enabler
verdict: ACTIVE
confidence: low
energy_to_test: low
project_created: no
project_link: <%= project_link %>
project_status: not-started
review_date: <%= tp.date.now("YYYY-MM-DD", 7) %>
tags:
  - idea
  - inbox
aliases:
  - <%= title %>
---

# Idea: <%= title %>

## Summary
What is the shortest useful description of this idea?

## Core Insight
What makes this interesting, different, or profitable?

## First Buyer
- who wants this first?
- what moment makes them care?

## First Use Case
- concrete use case:
- input required:
- output delivered:

## Why It Might Work
- inefficiency:
- arbitrage:
- trend:

## Fast Validation
- cheapest proof:
- fastest outreach:
- what would count as traction:

## Execution Filter
- clear problem: no
- clear buyer: no
- clear outcome: no
- can test in 48 hours: no
- likely to make money in 30 days: no

## Project Bridge
- project created: no
- project link: <%= project_link %>
- project status: not-started

## Links

### Concepts
- [[04-Systems/03-Concepts/Coordination]]
- [[04-Systems/03-Concepts/Arbitrage]]
- [[04-Systems/03-Concepts/Monetization]]

### Related Ideas
- 

## Notes
- 
