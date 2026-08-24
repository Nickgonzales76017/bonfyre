<%*
const title = tp.file.title;
const created = tp.date.now("YYYY-MM-DD");

tR += `---
type: concept
title: ${title}
created: ${created}
updated: ${created}
status: active
scope: core
tags:
  - concept
  - active
aliases:
  - ${title}
---

# Concept: ${title}

## Summary
What does this concept mean in one sentence?

---

## Definition
Describe the concept clearly enough that future-you can apply it fast.

---

## Why It Matters
- what problem does this concept help solve?
- what gets easier when this is understood?
- where does this create leverage?

---

## Signals
### Positive Signals
- what does this look like when it is working?

### Failure Signals
- what does this look like when it is missing or breaking?

---

## Where It Applies
- projects:
- systems:
- pipelines:
- daily execution:

---

## Decisions This Concept Improves
- 
- 
- 

---

## Examples
### In This Vault
- 

### In The Real World
- 

---

## Tradeoffs
- upside:
- downside:
- common misuse:

---

## Linked Systems
- 

---

## Related Concepts
- 

---

## Questions
- 

---

## Notes
- 

---

## Tags
#concept #ACTIVE
`;
%>
