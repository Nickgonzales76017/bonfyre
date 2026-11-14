---
slug: the-product-code-is-not-the-inventory
title: One product code cannot tell you what is on the shelf
description: A GTIN identifies a trade item, but stock, packaging, batches, reservations and rights to sell live elsewhere.
category: Commerce
icon: cart
published: 2026-11-09
status: edited-public
topics: GTIN, GS1, SKU, inventory, warehouse, batch, fulfillment
origin: Original Aurekai editorial analysis of primary institutions and operating systems
source_url: https://support.gs1.org/support/solutions/articles/43000734283-what-is-a-global-trade-item-number-gtin-
---

A seller scans a barcode and the system finds a product. It then reports thirty available units. A customer orders twenty. The next morning the warehouse has twenty cartons, not twenty units, and several cartons are reserved for a different customer. The scan worked; the inventory model did not.

The [GS1 description of a Global Trade Item Number](https://support.gs1.org/support/solutions/articles/43000734283-what-is-a-global-trade-item-number-gtin-) defines an identifier for a trade item that may be priced, ordered or invoiced. That is not a promise that a barcode knows how many physical objects are present, which warehouse owns them, or whether an offer can be fulfilled.

## Product identity and stock identity

A trade item identifies a defined commercial unit. A retailer's internal SKU may encode its own catalog distinctions. A batch or lot can identify production lineage. A physical serial number may distinguish an individual item. A warehouse location describes where something is stored. An inventory balance is a computed quantity over receipts, transfers, reservations, adjustments and shipments.

Confusing these identities makes it hard to reconcile stock during a recall, to sell a multipack correctly, or to explain why an item that appears available cannot be picked.

## A worked quantity mistake

Suppose a supplier prices a case of twelve bottles, but the storefront's quantity selector says '1'. If '1' represents a bottle in one subsystem and a case in another, the monetary and physical errors multiply together. The system might charge for one bottle and attempt to ship twelve, or charge for twelve and ship one.

A robust quote names the product, packaging level, requested quantity and unit of measure. An inventory transaction names the physical stock, location, reservation and source event. Conversion rules are declarations, not guesses from the barcode's length.

## Inventory is a time-sensitive claim

An observed count is not a reservation. A reserved quantity is not a shipment. A carrier pickup does not prove a customer received the goods. The more channels a merchant operates, the more important these transitions become.

The system needs idempotent event processing, conflict handling and a way to reconcile unexpected physical counts. A beautiful catalogue can show a real product, yet it should not claim ready-to-ship availability when the inventory owner has not closed that question.

## The larger commerce lesson

Aurekai can reuse a product's source identity across a catalogue, room plan, quote and purchase preparation without creating duplicate objects. But each application has a different responsibility and cannot manufacture inventory certainty or buying authority from a product code.

The visible barcode is the entrance to a chain of commercial obligations, not the chain itself.
