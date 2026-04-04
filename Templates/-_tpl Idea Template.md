---
type: idea
title: <% tp.file.title %>
created: <% tp.date.now("YYYY-MM-DD") %>
updated: <% tp.date.now("YYYY-MM-DD") %>
status: <% await tp.system.prompt("Status?", "draft") %>
stage: <% await tp.system.suggester(["exploration","validation","build","live","scaled","parked"], ["exploration","validation","build","live","scaled","parked"]) %>
system_role: <% await tp.system.suggester(["Driver","Enabler","Dependent"], ["Driver","Enabler","Dependent"]) %>
verdict: <% await tp.system.suggester(["ACTIVE","PARK","KILL"], ["ACTIVE","PARK","KILL"]) %>
confidence: <% await tp.system.suggester(["low","medium","high"], ["low","medium","high"]) %>
review_date: <% tp.date.now("YYYY-MM-DD", 7) %>
tags:
  - idea
  - <% await tp.system.prompt("status?", "draft") %>
aliases:
  - <% tp.file.title %>
---


# Idea: <% tp.file.title %>

## Summary
<% await tp.system.prompt("One-sentence summary?") %>

## Core Insight
<% await tp.system.prompt("Core insight?") %>

## Buyer
- first buyer: <% await tp.system.prompt("First buyer?") %>
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
<% await tp.system.prompt("First concrete use case?") %>

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
<% tp.frontmatter.system_role %>

## Stage
<% tp.frontmatter.stage %>

## System Flows

### Inputs From
- 

### Outputs To
- 

### Enables
- 

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

## Execution Filter
- clear first use case: no
- clear monetization path: no
- executable in 48 hours: no
- depends on <= 2 other systems: no

## Validation Questions
- Who wants this first?
- What painful problem does it solve?
- What is the cheapest proof?
- Can I test demand before building?
- What part is manual first?

## Notes
- 

## Tags
#idea

## Verdict
<% tp.frontmatter.verdict %>
