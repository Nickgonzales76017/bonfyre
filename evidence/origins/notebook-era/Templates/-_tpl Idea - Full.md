<%*
const title = tp.file.title;
const created = tp.date.now("YYYY-MM-DD");

const status = await tp.system.suggester(
  ["draft","active","parked","archived"],
  ["draft","active","parked","archived"]
);

const stage = await tp.system.suggester(
  ["exploration","validation","build","live","scaled","parked"],
  ["exploration","validation","build","live","scaled","parked"]
);

const system_role = await tp.system.suggester(
  ["Driver","Enabler","Dependent"],
  ["Driver","Enabler","Dependent"]
);

const verdict = await tp.system.suggester(
  ["ACTIVE","PARK","KILL"],
  ["ACTIVE","PARK","KILL"]
);

const confidence = await tp.system.suggester(
  ["low","medium","high"],
  ["low","medium","high"]
);

const summary = await tp.system.prompt("One-sentence summary?");
const core_insight = await tp.system.prompt("Core insight?");
const first_use_case = await tp.system.prompt("First concrete use case?");
const first_buyer = await tp.system.prompt("First buyer?");
const project_link = `[[Project - ${title}]]`;
-%>
---
type: idea
title: <%= title %>
created: <%= created %>
updated: <%= created %>
status: <%= status %>
stage: <%= stage %>
system_role: <%= system_role %>
verdict: <%= verdict %>
confidence: <%= confidence %>
energy_to_test: low
project_created: no
project_link: <%= project_link %>
project_status: not-started
review_date: <%= tp.date.now("YYYY-MM-DD", 7) %>
tags:
  - idea
aliases:
  - <%= title %>
---

# Idea: <%= title %>

## Summary
<%= summary %>

## Core Insight
<%= core_insight %>

## Buyer
- first buyer: <%= first_buyer %>
- trigger moment:
- why they pay:

## Philosophy
- 
- 
- 

## Why It Works
- inefficiency:
- arbitrage:
- trend:

## Inputs
- 

## Output
- 

## Execution Path
1. 
2. 
3. 

## First Use Case
<%= first_use_case %>

## Offer Hypothesis
- promise:
- deliverable:
- turnaround:
- price:

## Monetization
- revenue model:
- pricing hypothesis:
- first buyer:
- why they would pay:

## Constraints
- 
- 
- 

## Leverage Type
- 

## System Role
<%= system_role %>

## Stage
<%= stage %>

## Project Bridge
- project created: no
- project link: <%= project_link %>
- project status: not-started

## System Flows

### Inputs From
- 

### Outputs To
- 

### Enables
- 

## Execution Filter
- clear first use case: no
- clear monetization path: no
- executable in 48 hours: no
- depends on <= 2 other systems: no
- can manually fulfill before automation: no

## Validation Questions
- Who wants this first?
- What painful problem does it solve?
- What is the cheapest proof?
- Can I test demand before building?
- What part is manual first?

## Links

### Concepts
- [[04-Systems/03-Concepts/Coordination]]
- [[04-Systems/03-Concepts/Arbitrage]]
- [[04-Systems/03-Concepts/Distribution]]
- [[04-Systems/03-Concepts/Monetization]]
- [[04-Systems/03-Concepts/Infrastructure]]
- [[04-Systems/03-Concepts/Automation]]
- [[04-Systems/03-Concepts/Data]]

### Related Ideas
- 

## Notes
- 

## Tags
#idea

## Verdict
<%= verdict %>
