---
slug: a-phone-number-is-not-a-person
title: The number changed hands. The account did not.
description: Telephone numbering is a routing system, not a durable human identity or a permanent authorization to act.
category: Research
icon: people
published: 2026-11-09
status: edited-public
topics: telecom, E.164, account recovery, phone numbers, identity, fraud
origin: Original Aurekai editorial analysis using primary institutional documentation
source_url: https://www.itu.int/rec/T-REC-E.164/en
---

A service sends a one-time code to the phone number stored on an old account. The message arrives. The person who receives it may no longer be the person who opened the account. The software has proven current access to a telecommunications endpoint and silently interpreted it as the continuity of a person.

The [ITU E.164 numbering plan](https://www.itu.int/rec/T-REC-E.164/en) governs international public telecommunications numbering. It does not claim that a number permanently denotes one individual, one device, one household or even one kind of device.

## The number is a route through a changing network

Numbers can be ported between carriers, reassigned after disconnection, attached to business phone systems, forwarded, or used by shared family devices. A number formatted with a country code and national digits can be a valid route while revealing little about who currently controls it.

This is why phone-number parsing and account security are distinct disciplines. A numbering library may tell an application how to parse, display or assess possible numbers for a region; it cannot certify the present human controller or the future deliverability of every number.

## A failure at the recovery boundary

Suppose an account was opened years earlier with a mobile number and a recovery flow promises password reset through SMS. The user changes providers and loses access. The number is eventually assigned elsewhere. A new holder enters the number in a login page and is invited to take over the account.

The vulnerability is not the mathematical improbability of matching digits. It is the policy that possession of a reusable endpoint permanently authorizes access to the account. A correct control requires a separate account identity, recovery safeguards, detection of change or risk, and more than one trusted recovery route.

## Contact, consent and evidence are different

A delivery receipt means the network accepted or delivered a message according to its reporting rules. It does not mean an intended recipient agreed to receive marketing, that a purchase was approved, or that an account's owner was identified. A business sending appointment reminders must also respect preference, jurisdiction and revocation requirements.

For CRM systems, multiple people may share a reception line; one person may maintain several numbers; a company can have a switchboard, direct extension and temporary campaign number. Those are relations with purposes and validity periods, not duplicate people to be automatically merged.

## The architecture behind a small field

Retain the entered number, its validated display representation, verified use at a particular time and the kind of consent or proof actually obtained. Keep identity and communication channels separately addressable. If a number is rotated, the account should survive without inventing a new person.

The moment a mobile phone becomes the default identity card for every business workflow, a routing decision becomes a social decision. That is too much responsibility for a string of digits.
