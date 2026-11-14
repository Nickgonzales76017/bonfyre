---
slug: the-page-number-that-moved-while-you-were-reading
title: Page three did not mean the same thing twice
description: Pagination is a consistency decision about evolving records, not simply an offset and a limit.
category: Engineering
icon: database
published: 2026-11-09
status: edited-public
topics: pagination, database isolation, concurrent writes, cursor, ordering, APIs
origin: Original Aurekai editorial investigation informed by primary standards and technical documentation
source_url: https://www.postgresql.org/docs/current/queries-limit.html
---

An analyst reviews a list of fifty transactions. After reading page one, another worker inserts a new transaction near the top. The analyst opens page two and sees an item already reviewed. A later deletion causes another item to vanish from the sequence entirely. Each query was correct when executed; together the pages did not form one consistent inventory.

The [PostgreSQL LIMIT and OFFSET documentation](https://www.postgresql.org/docs/current/queries-limit.html) warns that paginated queries need a deterministic ordering. It also notes that skipped rows still incur work. Even an explicit order does not freeze the underlying dataset between separate requests.

## Offset is a position in a particular result

A query like 'skip twenty, take twenty' refers to the result as it exists under that query's visibility rules. Insertions and deletions can shift positions. Sorting only by a nonunique field such as created date can add instability when many records share the same value.

A stable total order usually needs a tiebreaker such as a durable record identifier. That protects against unpredictable ties but not against concurrent changes to the dataset.

## Cursor pagination changes the contract

An alternative is keyset pagination: 'return the next twenty records after the last seen ordered key.' This often reduces shifting duplicates and large-offset work. But new records inserted before the cursor may not appear in a continued walk, and records whose sort keys change can still move.

If the task requires an exact historical inventory, the correct model may be a snapshot or query generation held across the read, with its own retention and expiry rules. A social feed may deliberately prefer a changing stream. Neither behavior is universal.

## The audit use case

An accountant reviewing a batch of exported transactions cannot safely treat 'page 3 of 17' as a durable business reference. The reviewer needs a stable batch or snapshot identity, source revision, filters, ordering and a continuation token that cannot silently widen the query.

If the application saves 'approved through page three,' it has not described which records were examined. Save decisions against exact record IDs and revisions instead.

## Why this matters for composable apps

Aurekai's reading feeds, workboards, supplier listings and CRM screens have different expectations about freshness. A message feed can stream new items; a financial close requires reconciled membership. Reusing one generic pagination widget without describing the consistency model creates hidden process errors.

The simple design question—'Which page is next?'—is subordinate to a much harder one: 'Which set of records does the user believe they are walking through?'
