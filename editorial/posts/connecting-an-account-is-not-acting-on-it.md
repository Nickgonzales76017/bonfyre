---
slug: connecting-an-account-is-not-acting-on-it
title: Connecting an account is different from using its permissions
description: Why external service installs, credentials and authorized actions belong to separate systems.
category: Engineering
icon: plug
published: 2026-11-07
status: edited-public
topics: integrations, account security, OAuth
origin: Connection Studio, OAuth and provider discussions, September–November 2026
---

A business may connect GitHub, Cloudflare, a database, a CRM and a payment provider to the same workspace. At first it looks like a settings problem: save a token, test the connection and show a green checkmark.

But having a connection and deciding who may act through it are different things.

## The installation belongs somewhere

When someone starts a GitHub installation from an Aurekai account, the returning installation should be attached to that exact account. An older installation in the same browser or organization isn't automatically the one they chose.

The account keeps the installation reference and available capabilities. The credential owner keeps the secret. When work needs to happen, the receiving application asks for the appropriate scoped access.

## Technical possibility isn't business permission

An external API may support deleting a resource or spending money. That says the provider can perform the operation, not that this account approved it. A model can suggest an action; the account and institutional owners still decide whether to execute it.

We separate external provider descriptions, installed capabilities, account context, financial admission and the execution boundary for that reason. MCP makes tools easier to discover and use, while preserving those distinctions.

The [Bonfÿre MCP interface guide](https://github.com/Nickgonzales76017/bonfyre/blob/main/mcp/README.md) describes the system. It matters most for agencies and organizations that manage several customers whose permissions must never get mixed together.
