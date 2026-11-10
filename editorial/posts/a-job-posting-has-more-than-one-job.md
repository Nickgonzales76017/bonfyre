---
slug: a-job-posting-has-more-than-one-job
title: A job posting has more than one job
description: How one source can support discovery, company profiles, CRM and narration without becoming four unrelated copies.
category: Applications
icon: briefcase
published: 2026-11-07
status: edited-public
topics: feeds, CRM, source identity, applications
origin: Feed-to-account-app and Lithic acceptance discussions, October 2026
---

Consider a job posting from a company. On a job board it is an opportunity to apply. To a business-development team it may be a sign that a company is growing. To someone using audio or accessibility tools, it could become a short narrated update. Each use is reasonable. The trouble begins when each application creates its own company, source and version of the posting.

The feed-to-application work in Aurekai came out of exactly that problem. Our example was a Lithic job listing: how do we make it useful in CRM and media workflows without inventing special handling for Lithic or copying the whole feed into every app?

## Keep the original listing

The source has its own identity, original publisher and time. Bonfÿre can retain that source and its relationship to the company. A feed can display it, while installed account applications declare what sort of incoming material they understand.

A CRM might accept an opportunity derived from a job listing. A narration app might accept the text and a source link. A recruitment workflow may need to preserve the application destination. Those are different continuations from one item.

## Let the application own its work

The CRM should decide what an opportunity means, how it relates to existing contacts and whether a person needs to review it. The media application owns its editing and publication steps. Source Library owns the original material. The feed supplies a route between them.

This is why simply adding a button to a feed wasn't enough. We needed typed intake declarations on both sides, account-owned installations and a way to match information to the capabilities actually available.

The architectural direction is described in the [Bonfÿre technical guide](https://nickgonzales76017.github.io/bonfyre/docs.html), while [Aurekai Work](https://aurekai.work) is where the business-facing continuations belong.

It is a small example with a large reach. A product listing, a council document, a supplier notice or a research paper can all have more than one useful destination. The key is to move the relationship and the relevant information, not make every application start its own disconnected world.
