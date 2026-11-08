---
slug: why-base-rates-change-camera-matching-claims
title: A 95% accurate detector can still be wrong most of the time
description: The base-rate calculation that belongs in civic surveillance and anomaly-detection discussions.
category: Mathematics
icon: file
published: 2026-11-08
status: edited-public
topics: Bayes, base rates, false positives, civic technology
origin: Mathematical research notes from archived experiments and external literature
source_url: https://www.nist.gov/itl/iad/image-group/face-recognition-vendor-test-frvt
---

Suppose a system claims to recognize a rare event correctly 95% of the time. That sounds strong. It tells us little about how likely an alert is to be right unless we also know how often the event actually happens and how often the system raises false alarms.

This matters in civic surveillance, fraud detection, industrial inspection and many other applications.

## Work through the numbers

Imagine 10,000 observations. The event occurs in 1%, so 100 are real cases. A detector with 95% sensitivity catches 95 of them. If its false-positive rate is 5%, it incorrectly flags 5% of the 9,900 ordinary observations: 495 false alerts.

Among the 590 alerts, only 95 are true. The probability that a flagged case is actually positive is 95 / 590, or about **16.1%**.

The detector's reported sensitivity remains 95%. The problem is the low base rate combined with a nontrivial false-positive rate.

## Why this changes product design

A product that shows an alert as a fact can cause real harm when an operator is rushed or the evidence is difficult to challenge. A more responsible system preserves uncertainty, records the original observation and allows independent checks before an important decision.

NIST's [face-recognition evaluation program](https://www.nist.gov/itl/iad/image-group/face-recognition-vendor-test-frvt) is a useful example of careful measurement across operational conditions. Our civic-transparency discussions similarly distinguish public infrastructure information from personal movements or sensitive search histories.

The practical question is not whether a model sounds accurate. It is what happens to the people represented by the false alerts, and whether the system knows enough to treat them differently from verified outcomes.
