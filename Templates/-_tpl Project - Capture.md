<%*
const title = tp.file.title;
const created = tp.date.now("YYYY-MM-DD");
const source_idea = await tp.system.prompt("Source idea?");
-%>
---
type: project
title: <%= title %>
created: <%= created %>
updated: <%= created %>
status: planned
stage: setup
priority: high
review_cadence: weekly
source_idea: <%= source_idea %>
tags:
  - project
---

# Project: <%= title %>

## Origin
[[<%= source_idea %>]]

## Objective
<% await tp.system.prompt("What does this project aim to achieve?") %>

## Success Definition
- outcome:
- evidence:
- by when:

## Tooling
### Project Tooling
- primary tools:
- supporting tools:

### Meta Tooling
- shared tooling or infrastructure:
- linked meta system:

## First Deliverable
- 

## First Action
- [ ] 

## Constraints
- time:
- money:
- dependencies:

## Notes
- 
