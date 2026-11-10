---
name: foreign
description: Read the machine-readable internet — discover how a source publishes to machines, choose the cheapest carrier, receipt what you read. Use when asked to watch, ingest, monitor or observe an external site, feed, agency or data source.
allowed-tools: Bash, Read
---

# Reading the foreign machine web

The instinct is to fetch the page and hand it to a model. That is the last rung
of a twelve-rung ladder, not the first move.

```
scripts/foreign-discover.py [--observe] <https://root> [...]
```

It probes robots, the document head, the conventional addresses and the
`.well-known` space, then says which carrier to use and why.

## The ladder

```
 1 authenticated push / event      7 JSON-LD / structured metadata
 2 WebSub  (publisher pushes)      8 machine-readable files
 3 ActivityPub (actor delivers)    9 conditional HTTP (ETag / 304)
 4 first-party API                10 HTML extraction
 5 RSS / Atom / JSON Feed         11 browser rendering
 6 sitemap lastmod delta          12 model interpretation
```

Descend only when the rung above cannot answer. Measured on the Dallas
constellation: dallasculture.org exposes five feeds, dallascityhall.com two
sitemaps with validators, dallasnews.com three sitemaps plus typed JSON-LD.
Only dart.org forces HTML extraction — and that cost is recorded in its
passport rather than treated as normal.

## Push, and what to do when it is absent

```
bonfyre-sitemap from-robots <host>    read the sitemaps robots declares
bonfyre-sitemap check <sitemap-url>   fetch, diff, record added/changed/removed
bonfyre-websub discover <topic>       does this publisher offer push?
bonfyre-websub subscribe <topic>      hub verifies the callback, then delivers
bonfyre-websub reconcile              renew leases before they lapse (hourly timer)
scripts/feed-parse.py <receipt>       receipted body -> candidates
```

A delivery is a **notification, never content**: verify the signature, then
re-read the topic through the admission law. A lapsed lease is silent, which is
why renewal runs on a timer — a topic that stops delivering looks exactly like a
source with no news.

## Two rules that are not negotiable

**A 200 is not a representation.** Sites behind a WAF answer 200 with an HTML
rejection page for every unusual path; `dallascityhall.com` does this for all of
`.well-known`. Trusting the code records affordances the source does not have,
which is worse than missing them — it routes every later read into a carrier
that will never answer. Probes check the body's shape.

**A 200 is not the representation you asked for.** dallasnews.com declares three
sitemaps in robots and serves a JavaScript `Client Challenge` for all three;
dallascityhall.com publishes under a host whose robots is unreachable, so the
law declines it. Both are facts about those sources, recorded as `challenged`
and `robots_preflight_unavailable`. A challenge is a boundary, not an obstacle
to route around.

**A withdrawal is evidence.** `removed` in a sitemap delta means the publisher
stopped listing an address. Every copy that was never told keeps asserting what
it said, so withdrawals matter more than additions.

**Parse only what still hashes to its receipt.** `feed-parse.py` checks the body
against `content_sha256` before reading one element, and refuses outright if it
has drifted. Every item comes out as `source_declares` with its receipt attached
— evidence that a publisher said something at an observed time, not evidence
that it is true.

**Discovery never reads on its own authority.** `--observe` routes the chosen
document through `bonfyre-project foreign observe`: robots preflight, exact
retained bytes, digest, receipt. A parser works from a body that still hashes to
its receipt or it does not work at all. The passport records how an institution
wishes to be consumed and what our probes saw — never what its documents claim
is true.
