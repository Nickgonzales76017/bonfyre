---
slug: the-spreadsheet-import-that-was-not-just-data
title: The spreadsheet import was also a program
description: CSV serialization, spreadsheets, field types and downstream execution reveal why a simple export is a trust boundary.
category: Engineering
icon: database
published: 2026-11-09
status: edited-public
topics: CSV, spreadsheets, formula injection, imports, types, data provenance
origin: Original Aurekai editorial investigation informed by primary standards and technical documentation
source_url: https://www.rfc-editor.org/rfc/rfc4180.html
---

An office exports customer names to a CSV file and opens it in a spreadsheet. A row that was harmless plain text inside the database is interpreted as a formula by the receiving program. Another row begins with zeros that disappear from an account identifier. A date string is interpreted in the wrong locale. The file was described as data, but the reader gave parts of it executable or type-changing meaning.

[ RFC 4180 ](https://www.rfc-editor.org/rfc/rfc4180.html) documents a common CSV format; it does not define the business schema of the columns or guarantee every application interprets them as inert text. Format validity is not the same as safe consumption.

## A container without a type system

CSV can separate rows and fields, quote delimiters and represent text. It does not inherently distinguish postal codes from integers, currency amounts from display strings, or timestamps from dates. Two systems may parse the same cell differently and both believe they succeeded.

A value such as 00127 might be a quantity, an employee identifier or a ZIP code. Stripping leading zeros may be sensible for a quantity and destructive for the other two. The right type must come from the contract around the file, not from a guess about its appearance.

## When text turns into instructions

The [OWASP guidance on CSV Injection](https://community.owasp.org/attacks/CSV_Injection) explains why spreadsheets may treat formula-looking cell values as executable expressions. Quoting CSV fields correctly does not by itself remove the downstream spreadsheet semantics. Some mitigations can change the underlying data or behave differently between readers.

That is a security boundary: untrusted user-supplied values should not gain execution privileges because an administrator downloaded a report. A human-friendly spreadsheet export may need different handling from a machine-to-machine archival export, even when both start from the same source rows.

## Round trips are often not reversible

A report is opened, edited, saved and imported again. The spreadsheet may have normalized numbers, dates, Unicode or empty fields without preserving the original representation. The next import cannot reliably reconstruct a value that was discarded.

A safer workflow retains an immutable raw import, an explicit schema and parsing decisions, validation errors, and source-to-destination mappings. Human correction should create a reviewed change rather than overwrite evidence of what the partner actually sent.

## The wider institutional consequence

Financial systems, voter lists, medical offices and supplier catalogs all use flat files because they are convenient. That convenience does not justify trusting every reader's interpretation.

The best importer does not merely reject bad CSV. It exposes the contract: where values came from, what type each column claims, which transformations were applied, and whether exported content is allowed to be interpreted as a formula. A humble file can carry the entire organization's assumptions.
