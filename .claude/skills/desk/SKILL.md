---
name: desk
description: What the institution is holding open for decision — unpriced capabilities, unpublished drafts, offers at zero, unfinished work. Use when asked what needs attention, what to work on, or the state of open decisions.
allowed-tools: Bash, Read
---

# The desk

`/studio/` is not a feature index. It states what is open for decision right now,
computed from the stores that hold it, and gives each open thing the one act that
closes it.

```
desk_open_decisions {}          # through MCP
curl -s https://aurekai.life/studio/ -H "Accept: text/html"
```

Five queues, each counted against its store rather than inferred from a page
size — a count that stops at the page limit reports the limit and understates
what is open:

| queue | what closes it |
|---|---|
| capabilities with no asking price | `finance_price_set`, then every commerce grammar quotes it |
| written but not published | publish, or leave it a draft deliberately |
| offers in the book at zero | price it, or accept it is not for sale |
| work started and not finished | follow the mission, or reclaim it as orphaned |
| messages written and held | nothing yet: the desk counts them and names them to a write-scoped account; no act hands one over or drops it |

## Reading it honestly

An empty queue is a **fact about the institution**, not a gap in the page. It
says what would fill it.

The act offered depends on the viewer: a write-scoped account sees *Price it*, a
stranger sees *See it* and is told why. Being signed in is never authority to
mutate.

**Unfinished work is the queue that lies most.** `in_progress` is a durable row
state, not a live process. The operator store has four writers, and the STOP file
pauses only one of them, so a large unfinished count may be stranded rows rather
than running work. Check for open runs (`completed_at IS NULL`) before believing
either reading. Reclaim only by staleness — a startup sweep hands live goals to
two runners at once.
