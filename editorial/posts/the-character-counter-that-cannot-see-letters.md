---
slug: the-character-counter-that-cannot-see-letters
title: The character counter that cannot see what you typed
description: Unicode graphemes, code points, bytes, scripts and fonts expose different meanings of a single word: character.
category: Science
icon: file
published: 2026-11-09
status: edited-public
topics: Unicode, text, emoji, grapheme clusters, language, accessibility, typography
origin: Original Aurekai editorial analysis using primary institutional documentation
source_url: https://www.unicode.org/reports/tr29/
---

A signup form says a name may contain forty characters. A user types a short name with an accented letter and an emoji. The counter reports more characters than the person sees. Another editor deletes the accent but leaves the underlying letter. A third stores a visually identical name as a different database key.

These are not merely irritating details about emoji. They show how many layers a text system compresses into the word 'character.'

## What a person sees is not what a program counts

Unicode describes code points, encoded sequences and higher-level textual units. The [Unicode Text Segmentation standard](https://www.unicode.org/reports/tr29/) provides rules for extended grapheme clusters, a practical approximation of user-perceived characters. A visible symbol may consist of multiple code points, and UTF-8 may use multiple bytes to encode each one.

A byte-count limit can be necessary for storage or transport. A grapheme-count limit can be appropriate for a visible field. They answer different questions. The Unicode standard explicitly notes that a grapheme-based user limit may need a separate byte-based safety bound.

## The search problem is different again

Two strings can render identically but contain different Unicode sequences. Normalization can make selected representations comparable, yet normalization does not answer all questions about linguistic equivalence, accent significance, case folding or local ordering.

A search for a person's name might reasonably match accents or transliterations in some contexts; a legal name registry may need to preserve the exact authored bytes. A password validator must not quietly normalize and reveal unexpected equivalence. One universal 'clean text' operation is not adequate.

## Fonts are part of meaning, not just decoration

A renderer selects glyphs from fonts, shapes scripts and chooses line breaks. Grapheme clusters are not glyphs: two grapheme clusters may form a ligature, while a single grapheme can require several glyphs. A character-counting cursor and a typographic layout therefore need different kinds of knowledge.

Consider a multilingual publication that stores a quotation correctly but cannot display a script, or a scientific note whose symbols are replaced by fallback boxes. Its underlying bytes may be intact, yet its public meaning is not accessible.

## Which kind of text contract are we making?

A mature text component declares whether it is counting bytes, code points, grapheme clusters, words or layout units. It tests combining marks, right-to-left scripts, composed and decomposed accents, emoji sequences and copy-paste behavior, without treating any of those users as exceptions.

For Aurekai's editorial tools and account software, original authored text must survive. Search and presentation may derive alternate forms, but the system should never silently replace the source with a guessed equivalent. The serious design problem is making an application understand when a human is asking to edit language and when a machine is asking to compare bytes.
