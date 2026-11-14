---
slug: a-job-posting-is-not-a-job-opening
title: The job advertisement is not proof that someone is hiring
description: A listing's discoverability, vacancy, employment terms and authority to recruit are different states.
category: Business
icon: briefcase
published: 2026-11-09
status: edited-public
topics: employment, job postings, vacancy, recruiters, job data, privacy, lifecycle
origin: Original Aurekai editorial analysis of primary institutions and operating systems
source_url: https://developers.google.com/search/docs/appearance/structured-data/job-posting
---

A candidate prepares an application for a job advertised across six websites. Three listings are cached duplicates, one is a recruiter repost, another has expired, and the employer's original page no longer accepts applications. The candidate counted six opportunities; there may not be one open vacancy.

A search engine can expose the same job through multiple sources. A CRM can count applications. Neither is a substitute for the employer's current authority to hire for a particular position.

## A public listing has a lifecycle

Google's [JobPosting structured-data guidance](https://developers.google.com/search/docs/appearance/structured-data/job-posting) distinguishes the original posting date, relevant job details, hiring organization, possible expiration, location and ways to apply. It also warns against fictitious jobs and expired listings misrepresented as current.

Those are useful publishing constraints, not proof that any third-party scraper has the full truth. A job may be paused, filled internally, opened at several locations or subject to approval before an offer can be made.

## Repetition and the false market signal

Suppose five aggregators copy one employer announcement. A naive labor dashboard counts five vacancies. A more careful model retains the original posting identity, employer, role, location, source lineage and observation times. It may show five discovery channels but only one independently supported opportunity.

The opposite error is possible too: a company could genuinely be recruiting for multiple openings under one public requisition. Deduplication must not silently collapse distinct headcount. The correct answer often depends on an employer-owned record unavailable to an outsider.

## What the candidate experiences

Job postings can omit pay ranges, working arrangements, location constraints or essential qualifications. Some information is legally required in specific jurisdictions; other attributes are negotiable or unknown. An app should show which details came from the employer and which are inferred, without inventing certainty for a prettier card.

A remote label is especially misleading without the permitted geography, schedule and legal employment arrangements. 'Remote' describes work location policy; it does not automatically mean hireable from every country.

## The CRM continuation

Aurekai's feed-to-CRM concept depends on keeping the source item intact while allowing an account to open a distinct opportunity or candidate workflow. Saving a listing is not submitting an application, and preparing an outreach message is not authorization to contact a person.

A responsible job market does more than index the labor supply. It respects the candidate's time by separating observed advertisements from verified openings and by making source changes visible.
