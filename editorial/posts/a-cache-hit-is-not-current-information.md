---
slug: a-cache-hit-is-not-current-information
title: The server was right yesterday. The cache is right by its own rules.
description: Caching makes a promise about reuse, not truth; freshness, invalidation, variation and privacy need explicit policy.
category: Standards
icon: network
published: 2026-11-09
status: edited-public
topics: caching, HTTP, CDN, freshness, invalidation, privacy, public publication
origin: Original Aurekai editorial investigation informed by primary standards and technical documentation
source_url: https://www.rfc-editor.org/rfc/rfc9111.html
---

A visitor reads a public notice that has already been corrected by the publisher. The browser receives a fast response from a shared cache and displays the earlier wording. The server is healthy; the cache may even be following its declared rules. A quick load time has been mistaken for evidence that a page is current.

[HTTP Caching in RFC 9111](https://www.rfc-editor.org/rfc/rfc9111.html) defines how caches store, reuse and validate responses. Freshness is a relationship between a response's age and its freshness lifetime; it is not a declaration that the information remains correct in the real world.

## Three different clocks

The origin may publish a correction at 10:00. A cached response may be considered fresh until 10:30. A visitor's browser may also retain navigation history or service-worker state under different rules. Those clocks are related but not identical.

A cache can be permitted to serve a stale response under particular conditions. That behavior may be desirable for availability, but a public-safety notice or financial price needs a more deliberate policy than an evergreen magazine illustration.

## What invalidation actually promises

A versioned asset URL can let old CSS and new CSS coexist without ambiguity. An unversioned HTML address may need a revalidation policy. A conditional request can confirm whether a stored representation still matches its origin. But changing headers now cannot retroactively undo an old cache that was instructed to retain content for longer.

The right solution depends on the resource. A public article can be immutable within a named edition while its canonical page points to a corrected version. A signed-in account page must not leak data through shared caching.

## The privacy boundary

A cache key that ignores authorization or other response-varying context can reveal one person's content to another. Conversely, disabling all caching globally can make a legitimate public publishing system slow and unnecessarily expensive.

A mature architecture defines which projections may be shared, what their dependencies are, and how they are invalidated when source or rights change. Private bearer-bearing responses should not be treated as public reusable artifacts merely because they share a URL.

## Acceptance should test what users actually get

The [Aurekai Journal publication pipeline](https://nickgonzales76017.github.io/bonfyre/blog/the-deployment-was-green-but-the-site-was-old/) now checks exact live bytes for the published HTML and presentation assets. That is stronger than assuming successful upload means freshness, while still not proving every conceivable cache or device state.

The wider question is institutional: who has the authority to say content is current, and which systems are allowed to keep serving it after that authority changes? Caching is a performance feature built upon those choices.
