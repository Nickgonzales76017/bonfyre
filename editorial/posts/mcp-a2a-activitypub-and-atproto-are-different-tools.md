---
slug: mcp-a2a-activitypub-and-atproto-are-different-tools
title: MCP, A2A, ActivityPub and AT Protocol solve different problems
description: Why an interoperability strategy needs to preserve the meaning of each protocol.
category: Standards
icon: network
published: 2026-11-08
status: edited-public
topics: MCP, A2A, ActivityPub, ATProto, protocols
origin: External standards, primary documentation and Aurekai integration research
source_url: https://atproto.com/guides/overview
---

It is tempting to treat every new protocol as another universal connector. A system advertises tools over MCP, agents communicate over A2A, people publish with ActivityPub, and AT Protocol hosts portable social data. If they all move information between software, why not pick one and use it for everything?

Because they disagree about the unit being moved, the identity behind it and who may act on it.

## Four different responsibilities

The [Model Context Protocol](https://modelcontextprotocol.io/specification/2025-06-18) describes ways to present tools, resources and prompts to applications using models. [A2A](https://a2aproject.github.io/A2A/latest/specification/) focuses on communication and tasks between independent agent systems.

[ActivityPub](https://www.w3.org/TR/activitypub/) describes social activities and federation between servers. [AT Protocol](https://atproto.com/guides/overview) uses signed repositories, decentralized identifiers, Lexicon schemas and separate indexing services to support social applications.

A tool call, a delegated task, a public post and a signed repository record can refer to the same organization. That does not make them interchangeable.

## Build bridges around stable subjects

Suppose a publisher posts an event. ActivityPub may distribute the social notice. An AT Protocol repository could represent related publication records. An agent could use MCP to inspect an event source and A2A to coordinate work with another service. The account's own application still decides what the event means and what actions it permits.

Bonfÿre's Matrix and capability system are designed to retain that shared subject while letting different carriers do their specific jobs.

A good protocol strategy asks what information survives conversion, what gets lost and who owns a resulting action. We want interoperability through explicit adapters, not a universal wrapper that quietly discards meaning.
