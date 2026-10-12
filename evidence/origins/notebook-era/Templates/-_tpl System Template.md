<%*
const title = tp.file.title;
const created = tp.date.now("YYYY-MM-DD");

tR += `---
type: system
title: ${title}
created: ${created}
updated: ${created}
status: active
stage: operating
system_role: Core
source_project: [[Project - ${title}]]
review_cadence: weekly
tags:
  - system
  - active
---

# System: ${title}

## Purpose
What does this system do?

---

## Outcome
- value created:
- customer / user:
- measurable result:

---

## Core Mechanism
How does it actually work?

---

## Tooling
- operating tools:
- automation tools:
- supporting infrastructure:
- meta tooling note:

---

## Inputs
- trigger:
- raw materials:
- tools:

---

## Outputs
- artifact:
- decision:
- downstream effect:

---

## Flow
1. intake
2. processing
3. delivery

---

## Control Layer
- owner:
- quality check:
- manual override:

---

## Current State
- what exists?
- what is manual vs automated?
- what is still fragile?

---

## Bottlenecks
- 

---

## Metrics
- throughput:
- quality:
- revenue or time saved:

---

## Next Improvement
👉 one upgrade to make this system better

---

## Links
### Source Project
- [[Project - ${title}]]

### Upstream
- 

### Downstream
- 

### Related Pipelines
- 

---

## Tags
#system #ACTIVE
`;
%>
