---
slug: a-person-is-not-their-name-field
title: The form that quietly changed a person's name
description: Names change across context and time; an identity system should preserve people without declaring one spelling their essence.
category: Research
icon: people
published: 2026-11-09
status: edited-public
topics: human identity, names, health records, transliteration, public profiles
origin: Original Aurekai editorial analysis developed from primary standards and institutional operating cases
source_url: https://www.hl7.org/fhir/R4/datatypes.html
---

Imagine a clinic merging two registrations because the date of birth and surname match. One person is mistakenly joined to someone else's history. Imagine the inverse: a patient is issued a second record because an official name changed after marriage. Neither error is fundamentally a string-validation bug. Both confuse how somebody is addressed with who the person is.

Software commonly starts with 'first name' and 'last name' because the fields appear ordinary. But a name is a social, legal and linguistic representation with an audience, a period of use and a reason for being recorded.

## A name is evidence of presentation, not an identity key

The [HL7 FHIR HumanName model](https://www.hl7.org/fhir/R4/datatypes.html) recognizes an entire textual representation, given and family components, prefix and suffix, usage such as official or nickname, and a period. Its documentation explicitly allows names to change or be repudiated. That is a richer model than a universally mandatory two-piece name.

Even that structure is not a universal theory of identity. Different cultures partition names differently; some records use a single field or have no reliable division at all. Transliteration creates multiple spellings of the same underlying name. Historical documents may record an alias without proving it is the preferred or current name.

## A realistic failure chain

Suppose a person has one professional publication credit, another legal spelling on a contract, and a different display name in a community. A platform that rewrites all three when one profile field changes can destroy citations, break contract search, and misrepresent the original publication. A platform that refuses to update any historical name can misaddress the person indefinitely.

The resolution is not to choose a universal canonical spelling. Maintain a stable internal subject reference and separate name assertions: value, script, usage, provenance, validity interval, and visibility policy. A source document can retain its recorded byline while the owner's current public profile shows an updated name.

## Matching is a decision with consequences

Two textually equal names do not prove that two records refer to one person. Two unequal names do not prove they refer to different people. A system proposing a merge must explain the independently supporting evidence, request the required review, and make reversal practical.

For a civic register, a mistaken merge can mix legal rights. For healthcare, it can contaminate clinical history. For a publishing platform, it can steal credit or make a person's past work disappear. The same data-design shortcut harms different people in different ways.

## Why this belongs in the discussion of digital profiles

Aurekai's idea of a durable profile is strongest when the profile is a presentation attached to an independently governed identity, rather than a string copied into every app. Identity, authority to edit, author credit, community membership and public appearance are distinct relationships.

A mature interface should offer a respectful free-form display name, ask for structured components only when an operation actually needs them, and never let a normalization routine decide personhood. The fundamental question is not 'How many name fields should the form have?' It is 'What claim is this field making, and who may correct it?'
