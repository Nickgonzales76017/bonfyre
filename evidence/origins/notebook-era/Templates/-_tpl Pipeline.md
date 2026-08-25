<%*
const title = tp.file.title;
const created = tp.date.now("YYYY-MM-DD");

tR += `---
type: pipeline
title: ${title}
created: ${created}
updated: ${created}
status: active
stage: design
system_role: Connector
review_cadence: weekly
tags:
  - pipeline
  - active
---

# Pipeline: ${title}

## Purpose
What does this pipeline move, coordinate, or transform?

---

## Outcome
- desired result:
- primary metric:
- handoff point:

---

## Core Mechanism
How does this pipeline operate?

---

## Inputs
- trigger:
- raw materials:
- required context:

---

## Outputs
- artifact:
- status update:
- downstream action:

---

## Flow
1. capture / trigger
2. transform / decide
3. deliver / record

---

## Systems Involved
### Upstream
- 

### Downstream
- 

### Supporting
- 

---

## Control Points
- quality check:
- manual approval:
- failure fallback:

---

## Current State
- what exists?
- what is manual vs automated?
- what still breaks?

---

## Bottlenecks
- 

---

## Metrics
- throughput:
- conversion:
- margin / time saved:

---

## Next Improvement
👉 one upgrade to improve flow or automation

---

## Notes
- 

---

## Tags
#pipeline #ACTIVE
`;
%>
