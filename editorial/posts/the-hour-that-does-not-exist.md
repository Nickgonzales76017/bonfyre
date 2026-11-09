---
slug: the-hour-that-does-not-exist
title: The appointment booked for an hour that never happens
description: Civil time is a political rule, an instant is a measurement, and a recurring appointment needs both.
category: Standards
icon: globe
published: 2026-11-09
status: edited-public
topics: civil time, time zones, DST, recurrence, healthcare scheduling
origin: Original Aurekai editorial analysis developed from primary standards and institutional operating cases
source_url: https://www.rfc-editor.org/rfc/rfc9557.html
---

A dental office promises that an appointment is always at 9:00 on Monday. Its booking system stores a UTC timestamp and congratulates itself for solving time zones. Six months later the appointment moves on the patient's calendar. The database faithfully preserved an instant. It lost the promise.

A useful system must distinguish three objects that look similar in a calendar: a particular instant, a local wall-clock reading, and a rule for producing future local readings. A recurring appointment is not an array of already-known instants. It is an instruction to interpret changing civil-time rules in a named location.

## The missing hour and the repeated hour

When clocks advance, a local interval can disappear. When clocks move backward, the same wall-clock label can occur twice. A form accepting 02:30 may be accepting a time that never occurs on a particular date; a fall-back 01:30 may refer to two possible instants. Both cases require an explicit policy: reject, move to the next valid local time, or ask which occurrence the person intended.

The distinction appears in [RFC 9557](https://www.rfc-editor.org/rfc/rfc9557.html), which extends Internet timestamps to include time-zone information. A numeric UTC offset fixes a relationship for one timestamp. An IANA zone identifies a rule set whose offsets can change.

## A worked calendar example

Consider a hypothetical weekly consultation at 09:00 in New York, with the first session in January and another in July. The intended local time remains 09:00, yet the UTC instants differ by an hour because the civil offset changes seasonally. If the system repeats the first UTC time every seven days, summer sessions can appear at 10:00 local.

A different contract—run a machine every 168 elapsed hours—really does call for repeated instants separated by a fixed duration. Neither contract is inherently wrong. The failure is substituting one for the other without asking.

## A calendar record needs an interpretation policy

An operator should retain the named zone, intended wall time, recurrence rule, start date, exceptions, and a policy for gaps and overlaps. The resolved instant for each occurrence is then a projection, not the canonical recurring instruction. A notification system may also need to remember the rule-set version used to produce a reminder and recompute future instances when legislation changes.

The [IANA Time Zone Database](https://www.iana.org/time-zones) changes because governments change clocks. The rules are not an implementation nuisance; they represent current civil authority. Historical events can also acquire different displayed labels when timezone data is corrected.

## The institutional problem underneath

Healthcare scheduling, payroll shifts, prayer times, train timetables, and retail opening hours do not all mean the same thing by 'at nine.' A payroll system may need elapsed labor duration and statutory local dates. A patient expects the clinic's local schedule. A transit operator may need a service day that outlives midnight. Moving all of these into one timestamp field erases the distinctions that matter.

The design consequence for an institutional software platform is simple but expensive: treat local intent as source material, show the conversion policy to the person making the promise, and retain enough context to explain every derived instant. Storing UTC remains useful. It is not a substitute for understanding what the event is.
