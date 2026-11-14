---
slug: source-available-is-not-the-same-as-open-source
title: A public GitHub repository is not automatically open source
description: Why licence rights matter when evaluating external software as productive capital.
category: Standards
icon: file
published: 2026-11-08
status: edited-public
topics: licensing, OSI, Elastic License, source available
origin: External standards, primary documentation and Aurekai integration research
source_url: https://opensource.org/osd
---

Aurekai researches and integrates a large number of external tools. Some are permissively licensed. Some are copyleft. Some publish readable source while reserving rights that make them unsuitable for a hosted service or commercial derivative.

That distinction matters before software becomes part of a shared operating platform.

## Source access and reuse are different permissions

The [Open Source Initiative's definition](https://opensource.org/osd) requires rights that include free redistribution and use across fields of endeavor. Being able to read or fork code on GitHub does not establish that the licence grants those rights.

A source-available project may prohibit offering a competing hosted service. A business-source licence may impose a change date or other conditions. A copyleft licence can require particular source distribution or other compliance when software is conveyed.

None of these models is inherently the right choice for every business. But they lead to different decisions when an external component is embedded in a product, modified or offered to customers.

## Retain the exact obligation

A project inventory should record the repository, version, licence text and how the component is actually used. Copying a feature description into a source atlas is not the same as having permission to embed its implementation.

The [OSI licence directory](https://opensource.org/licenses) is a starting point for recognized open-source licences. The contract governing the exact version remains decisive.

Our federated source work tries to keep this distinction visible: a donor or reference identity is different from a packaged and licensed application.

Software becomes productive capital when the rights and maintenance obligations are understood. A public URL on its own is not enough.
