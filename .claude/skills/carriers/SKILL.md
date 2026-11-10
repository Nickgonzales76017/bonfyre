---
name: carriers
description: Speak or verify the eighteen live carrier protocols — x402, MPP, UCP, ACP, Beckn, OGC, RESO, MISMO, ActivityPub, WebFinger, CloudEvents, WoT, CAP, EPCIS, OpenRTB, A2A, MCP. Use for payment rails, agent commerce, geospatial or real-estate feeds, event envelopes, or any question about what Aurekai exposes to machines.
allowed-tools: Bash, Read
---

# The carriers

Eighteen protocols, one semantic state. A carrier is a **projection** of the
institution into a grammar, never a second owner of the data. That is the whole
design: two carriers cannot disagree about a price or a parcel, because neither
holds one.

## Reach any of them

```
protocol_fetch { "carrier": "x402", "name": "parcel-underwrite" }
```

Carriers: `x402 mpp a2a cloudevents ogc ogc_items reso mismo activitypub
activitypub_outbox webfinger beckn ucp acp wot cap epcis openrtb`.

## The distinction that catches people

**The book and the executable catalogue are different sets.** Every service
with a price is in the finance book. Only what an organ can actually produce is
in the x402 catalogue, which is compiled into the address organ rather than read
from the book. Pricing a book entry through `finance_price_set` does **not** make
it x402-sellable — x402 settles only what can be delivered, while UCP, ACP and
Beckn may carry merchant inventory no organ delivers. `finance_book` returns both
sets for exactly this reason.

An unpriced capability answers `not_for_sale`. That is not the same as free.

## Settlement never pretends

x402 answers 402 with `accepts[]` and refuses to serve on an unverified payment.
With no facilitator configured it says `payment_unverifiable` rather than
treating a claim of payment as revenue. A rail that pretends is worse than one
that refuses.

## Empty by design is a result

`cap`, `epcis` and `openrtb` return valid, empty documents **with the reason** —
no hazard source admitted, no GS1-identified product, no inventory and no consent
basis. An empty collection that states why is a finding. Never fill one with a
sample record to make it look alive.

## Verify

`tests/circuits/cross_protocol_circuit.py` asserts per-capability price agreement
between x402 and MPP, and that unpriced capabilities refuse. Run it after
anything that touches pricing or the catalogue.
