---
slug: temperature-is-not-a-single-number
title: The weather dashboard that added two temperatures
description: A measured level and a difference in temperature are different quantities even when the units look familiar.
category: Science
icon: trending
published: 2026-11-09
status: edited-public
topics: measurement, temperature, units, sensors, HVAC, localization, uncertainty
origin: Original Aurekai editorial analysis of primary institutions and operating systems
source_url: https://www.nist.gov/pml/owm/si-units-temperature
---

A monitoring system receives a reading of 20 degrees Celsius and a forecasted rise of 5 degrees. It uses the same conversion function for both, then displays nonsense after switching the dashboard to Fahrenheit. Its mistake is not arithmetic ability. It has confused a level on a temperature scale with a temperature interval.

The [NIST explanation of SI temperature](https://www.nist.gov/pml/owm/si-units-temperature) notes that one Celsius-degree interval equals one kelvin interval, while the zero points differ by 273.15. Converting a temperature level includes an offset; converting a difference does not.

## A worked conversion

An absolute reading of 20 °C corresponds to 68 °F. An increase of 5 °C corresponds to an increase of 9 °F, not 41 °F. If the system converts both values as though they were absolute points, it adds the offset twice and produces an impossible interpretation of a perfectly reasonable measurement.

The distinction is common in engineering: position versus displacement, timestamp versus elapsed duration, mass versus weight, rate versus total. The same visible unit can accompany different kinds of quantities.

## Sensors add their own uncertainty

A temperature reading may reflect sensor calibration, response time, environmental placement and transmission delay. A warehouse probe near a door can register a local excursion that does not describe the entire storage volume. A medical measurement requires the specific measurement method, context and device semantics.

Reporting extra decimal places does not increase confidence. A reading of 20.000 °C from a poorly calibrated device is not automatically more informative than a rounded reading from a well-characterized sensor.

## Localization cannot repair a wrong type

A user might prefer Fahrenheit, Celsius or Kelvin. Changing the display requires knowing whether the value is a point, interval, rate of change or threshold. The [NIST SI conversion guidance](https://www.nist.gov/pml/special-publication-811/nist-guide-si-appendix-b-conversion-factors/nist-guide-si-appendix-b9) gives distinct treatments for temperature and temperature intervals.

A localization library may perform a correct conversion and still produce the wrong business result if the caller failed to identify the quantity. In safety-sensitive systems this is a correctness problem, not a cosmetic preference.

## The institutional repair

A durable measurement record includes quantity kind, unit, observation time, sensor or source, method, uncertainty where known, and applicable decision thresholds. An alert is a separate judgment applying a rule to that observation.

This is one reason scientific and industrial tools should not be designed as generic dashboards. A good interface exposes what was observed, what was inferred and what action is actually authorized.
