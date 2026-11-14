---
slug: a-coordinate-is-not-a-map-of-reality
title: The point was correct. The map was wrong.
description: Coordinates depend on reference systems, transformations, accuracy and a definition of what feature is being measured.
category: Science
icon: globe
published: 2026-11-09
status: edited-public
topics: geography, GIS, spatial data, datums, uncertainty, public maps
origin: Original Aurekai editorial analysis developed from primary standards and institutional operating cases
source_url: https://epsg.org/home.html
---

Two teams map the same facility. One records a location from a handheld receiver; another imports a survey from a legacy engineering system. Both files contain two numbers and both look plausible. Their points disagree when rendered together. The immediate temptation is to accuse somebody of bad measurement. The deeper possibility is that the numbers refer to different coordinate reference systems.

Latitude and longitude are not free-standing physical facts. A coordinate is meaningful only relative to a model of Earth, axes, units and a particular realization of the reference frame.

## The reference frame hides inside the number

A projected coordinate measured in meters is not interchangeable with geographic latitude and longitude measured in angular degrees. Even two geographical datasets can use different datums or coordinate epochs. Transforming between them may require grid corrections, known parameters or a declaration that some precision is unavailable.

The [EPSG Geodetic Parameter Dataset](https://epsg.org/home.html) exists to identify coordinate reference systems and coordinate operations explicitly. It helps explain why a mapping application should not guess the CRS from a range of plausible-looking values.

## A worked location problem

Imagine a municipal map that places a monitoring point at the center of a large property because only the parcel centroid is known. A user interprets the pin as the precise equipment position. The displayed precision—many decimal digits—can exceed the underlying observation by orders of magnitude.

The issue is not simply rounding. There are distinct kinds of uncertainty: measurement error, estimated geocoding, changed road geometry, an imprecise source description, and a deliberately generalized public position. Each has different implications for a map's credibility.

A [coordinate operation](https://www.ogc.org/standards/) can transform one reference system into another; it cannot recover a precise original observation that nobody recorded.

## Boundaries are claims about authority

A cadastral parcel, a school district, a floodplain and a neighborhood polygon may overlap without sharing one legal meaning. Drawing a boundary does not tell a user whether it controls taxation, service eligibility, enforcement or simply a community's preferred description.

For civic research, map layers should disclose their owner, date, version, representation and confidence. A community correction should improve a record without silently changing a government's original source. Public-safety information may also need deliberate geographic generalization to avoid exposing sensitive infrastructure.

## A richer kind of public map

The best map offers not only a pin but an answer to four questions: what feature is represented, how its location was determined, how accurate the result is, and who can update the claim. A position becomes useful when its uncertainty and provenance travel with it.

The larger lesson extends to search, identity and product catalogs. Every number becomes dangerous when the interface hides the frame in which it means something. A neat map is not evidence that the world fits neatly in its coordinates.
