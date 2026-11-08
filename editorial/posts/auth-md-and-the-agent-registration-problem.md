---
slug: auth-md-and-the-agent-registration-problem
title: An agent can discover an API. Who gives it an account?
description: What WorkOS auth.md contributes to machine registration, and why the destination still matters.
category: Standards
icon: plug
published: 2026-11-08
status: edited-public
topics: AUTH.md, OAuth, account registration, agents
origin: External standards, primary documentation and Aurekai integration research
source_url: https://github.com/workos/auth.md
---

We spent time researching machine-readable instructions that a service could publish for software agents: AGENTS.md in repositories, OpenAPI for endpoints, MCP descriptions for tools, and well-known metadata for authentication.

The [WorkOS auth.md project](https://github.com/workos/auth.md) addresses another missing step: how does an agent discover the registration and authorization procedure for a service on behalf of a user?

That is not the same question as whether the API supports a particular method.

## Discovery, registration and use

An agent can learn that a service has an API, then learn how to identify itself, register or claim an account and obtain a credential through an approved flow. The published [AUTH.md example](https://github.com/workos/auth.md/blob/main/AUTH.md) describes discovery, registration, token exchange and revocation.

The sequence is useful because agent tools often assume credentials already exist. But a person using a new service may never have registered before, and an organization may want the registration attached to a specific workspace.

## Where the authority belongs

In Aurekai, the account destination is central. A customer connecting GitHub or a payment provider should not accidentally reuse an installation from another company merely because the same human appears in both contexts.

A credential is held by its credential owner. The application stores the installation and available capability. The actual operation is separately checked against the account and the action being requested.

Machine-readable authentication instructions can make onboarding smoother. They do not turn an external service into an unlimited tool in every agent's hands.

The interesting opportunity is to make registration, installation, permission and revocation as composable as the API call that follows them.
