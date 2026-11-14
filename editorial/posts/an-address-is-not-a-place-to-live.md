---
slug: an-address-is-not-a-place-to-live
title: A delivery address is not the same thing as a home
description: Postal delivery, legal residence, property, geography and care services need different representations of the same location.
category: Business
icon: building
published: 2026-11-09
status: edited-public
topics: postal addressing, property, housing, logistics, identity, deliveries
origin: Original Aurekai editorial analysis developed from primary standards and institutional operating cases
source_url: https://pe.usps.com/text/pub28/
---

A warehouse wants to know where to deliver a package. A school wants to know where a child resides. An insurer wants to know which property a contract covers. A mutual-aid organizer wants to know which neighbors could reach a person after a storm. Each asks for an address, but none is asking the same question.

A single 'street, city, state, ZIP' tuple can appear to answer all four. In practice it may identify a delivery point, a mailing convention, a building, a unit, a named landmark, a person-to-place relationship, or an administrative boundary. Conflating these is how supposedly clean databases produce unserviceable shipments and unfair eligibility decisions.

## Delivery is a route through an institution

The [USPS Publication 28](https://pe.usps.com/text/pub28/) covers urban delivery lines, rural routes, general delivery, post-office boxes, Puerto Rico conventions and other cases. These are not merely eccentric spellings of the same street grid. They are ways a delivery service reaches its intended recipient.

A post-office box can be a valid mailing address without describing where someone sleeps. A rural route may support delivery even when a mapping service cannot resolve a familiar house-number coordinate. Standardizing a postal line can make sorting reliable without establishing ownership, occupancy or geographic precision.

## The boundary between a building and a person

Consider an apartment block with many units and a commercial tenant. The parcel belongs to one landowner; the postal service serves multiple delivery points; occupants may change; the building may span more than one entrance. An app that uses formatted address text as its permanent entity ID risks merging distinct tenants or creating a new property every time a unit label changes.

A better model distinguishes site, structure, unit, delivery point and current occupant. Each relationship has evidence and time. An address is then an assertion that a service recognizes some way to reach an entity, not proof that all systems recognize the same place.

## Geography adds another layer

Coordinates help dispatchers, but the coordinates of a parcel centroid may not identify the entrance for an ambulance or the loading dock for freight. Geocoding precision must be recorded. A point inferred from a street segment should not be quietly represented as the verified doorstep.

In public services, the consequences exceed inconvenience. A person without conventional mail delivery can still be resident somewhere, eligible for services, and part of a community. Forcing the person into a false street-address format turns the application's assumptions into a barrier.

## What an operating platform should ask

Ask what the operation needs: deliver, verify eligibility, obtain consent, identify a property, plan a route or preserve a historical record. Preserve the supplied address verbatim, the relevant normalized representation, the authority that validated it, and whether its position was observed or inferred.

The expensive part of an address system is not text formatting. It is carrying the different responsibilities of postal operators, people, land records and service providers without pretending that one field can settle them.
