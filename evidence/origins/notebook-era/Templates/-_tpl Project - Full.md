<%*
const title = tp.file.title;
const created = tp.date.now("YYYY-MM-DD");

const status = await tp.system.suggester(
  ["planned","active","blocked","complete","killed"],
  ["planned","active","blocked","complete","killed"]
);

const stage = await tp.system.suggester(
  ["setup","build","launch","iterate","scale"],
  ["setup","build","launch","iterate","scale"]
);

const priority = await tp.system.suggester(
  ["low","medium","high","critical"],
  ["low","medium","high","critical"]
);

const source_idea = await tp.system.prompt("Source idea note?");
-%>
---
type: project
title: <%= title %>
created: <%= created %>
updated: <%= created %>
status: <%= status %>
stage: <%= stage %>
priority: <%= priority %>
review_cadence: weekly
source_idea: <%= source_idea %>
tags:
  - project
aliases:
  - <%= title %>
---

# Project: <%= title %>

## Origin
Linked Idea: [[<%= source_idea %>]]

## Objective
<% await tp.system.prompt("What does success look like?") %>

## Success Definition
- deliverable:
- proof:
- deadline:

## Tooling
### Project Tooling
- primary tools:
- supporting tools:

### Meta Tooling
- shared tooling or infrastructure:
- linked meta system:

## Scope
### In Scope
- 

### Out of Scope
- 

---

## Execution Plan
### Phase 1 - Setup
- 

### Phase 2 - Build
- 

### Phase 3 - Launch
- 

### Phase 4 - Iterate
- 

---

## Tasks
### Now
- [ ] 

### Next
- [ ] 

### Later
- [ ] 

---

## Constraints
- time:
- money:
- technical:
- dependencies:

---

## Risks
- 
- 
- 

---

## Metrics
- success metric:
- leading indicator:
- failure condition:

---

## Dependencies
### Requires
- 

### Blocks
- 

---

## Execution Log
### <%= created %>
- project created

---

## Decisions
- 

---

## Links
### Related Idea
- [[<%= source_idea %>]]

### Systems
- [[04-Systems/03-Concepts/Infrastructure]]
- [[04-Systems/03-Concepts/Automation]]
- [[04-Systems/03-Concepts/Distribution]]
- [[04-Systems/04-Meta/Meta-System]]
- [[04-Systems/04-Meta/Vault Operating System]]

---

## Status
<%= status %>
