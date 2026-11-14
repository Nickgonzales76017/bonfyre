---
slug: stable-identifiers-are-equivalence-classes
title: A stable identifier is a decision about what counts as the same thing
description: Unicode, line endings, schema changes and the mathematical meaning of identity continuity.
category: Mathematics
icon: network
published: 2026-11-08
status: edited-public
topics: equivalence relation, Unicode, hashing, SARIF
origin: Mathematical research notes from archived experiments and external literature
source_url: https://unicode.org/reports/tr15/
---

Two scanners can examine the same source file and disagree about the identity of a finding. One saw CRLF line endings; another saw LF. One received a composed Unicode character; another received an equivalent sequence of code points. Hashing the raw inputs gives different IDs even when the human believes the finding did not change.

The real design question comes before the hash.

## Decide the equivalence relation

Write x ~ y when two representations should count as the same underlying subject. A canonicalization C should satisfy: if x ~ y, then C(x) = C(y). But we also need to avoid collapsing meaningful differences.

Absent data is not always an empty string. A missing customer address and a blank address entered by the customer can mean different things. Reordering fields in a map may be harmless; reordering an ordered workflow is not.

[Unicode Standard Annex #15](https://unicode.org/reports/tr15/) defines forms of textual normalization. [MDN's normalization examples](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String/normalize) demonstrate how visually equivalent strings can have different underlying bytes.

## Version the contract

An identity function might hash the canonical representation alongside a schema version. That makes changes in interpretation visible. It does not remove the need to map old IDs to new ones when records are migrated.

Consider a security finding that moves five lines down after a comment is inserted. If its identity is tied to the line number, the issue can disappear and reappear as a new one without being fixed. If identity ignores too much context, two genuinely different findings can collapse.

Our earlier SARIF and Matrix discussions pointed toward testing these cases explicitly across Unicode, paths, null values, ordering and schema versions.

A digest is a useful implementation mechanism. The institutional decision is which changes preserve identity and which create a new subject. That decision has to be written down before the checksum can help.
