# Desk capability pricing — internal options for approval

2026-11-10. The live desk holds one open queue: 8 capabilities answering
`not_for_sale` to every commerce grammar. Each room below is verified live;
each quote below is an option for Nick's approval, not a standing price.

Two steps unlock revenue, not one: (1) `finance_price_set {name, buy_usd,
sell_usd}` writes the service book every grammar quotes; (2) without a
facilitator the room still answers 402 `payment_unverifiable`. The
facilitator is the `BONFYRE_X402_FACILITATOR` env var on the WebAddress
service (`x402_facilitator_for` in `x402_capability_and_aperture_forms.h`;
demo institutions are never facilitators).

Cost basis (verified): Meter applies only operator-admitted per-dimension
rates and never implies one; marginal serve cost for all eight is local
compute over owned stores (SQLite/Parquet), i.e. cents per call at most.
The estate harvesting behind 1–3 and 6 is sunk. Pricing below is therefore
value-based. No demand counts were observed; no priced capability comps
exist locally (no finance.db in tree; /market renders client-side).

## 1. parcel-underwrite — per-city land and exit anchors from the DFW estate

Buyer: investors, builders, site selectors pricing land and exit. Dearest
signal on the desk; every answer cites receipted parcels.

- Seed: buy 0.10 / sell 2.00 per city report. Assumption: land one design
  partner, learn what they rerun.
- Value: buy 0.10 / sell 12.00 per city report. Assumption: the anchor
  replaces a morning of comps work.
- Hold: leave unpriced until a facilitator exists, since no sale can
  settle anyway.

## 2. owner-concentration — parcel counts by taxpayer name, disclosure floor

Buyer: journalists, researchers, funds mapping concentration. Sensitive:
the floor is the product (counts above it, silence below).

- Seed: buy 0.05 / sell 1.00 per name lookup.
- Value: buy 0.05 / sell 5.00 per lookup, buyers who resell analysis.
- Hold: if the disclosure floor has not been counsel-reviewed, do not
  price this one yet — ask first.

## 3. estate-coverage — row/page coverage per observed source, digests

Buyer: data teams auditing what the estate actually holds. Meta-product;
sells the estate's honesty more than answers.

- Seed: buy 0.02 / sell 0.25 per source report.
- Value: buy 0.02 / sell 1.00 per report with freshness verdict.
- Bundle: include free with any parcel-underwrite sale (buy 0.02 /
  sell 0.00 is refused — prices cannot be negative but zero sell is
  allowed; confirm in book).

## 4. frappe-schema — compiled schema IR for one installed lineage

Buyer: integrators and agencies building on the nine Frappe products.
Saves a day of schema spelunking per lineage.

- Seed: buy 0.05 / sell 3.00 per lineage pull.
- Value: buy 0.05 / sell 15.00, agencies white-labeling under their own
  identity (see white-label-operating-products-20261011).
- Coverage: price per lineage (nine entries), not one price for all.

## 5. route-frontier — cheapest proven-readable source for a need

Buyer: anyone paying for data they could read cheaper. The freshness
verdict is the trust anchor; the avoided list is the story.

- Seed: buy 0.05 / sell 0.50 per need.
- Value: buy 0.05 / sell 4.00, buyers with recurring sourcing spend.
- Note: sell shares the verdict, never the underlying harvester.

## 6. entity-resolve — name or identifier to one canonical actor

Buyer: CRM hygiene, record linkage, fraud review. High volume, low
patience; "does not resolve" must be as crisp as a hit.

- Seed: buy 0.01 / sell 0.10 per resolution.
- Value: buy 0.01 / sell 0.50 with volume tiers in the book.
- Floor: never below marginal compute; this one gets hammered.

## 7. artifact-lineage — lineage/dependency layers from the Merkle-DAG

Buyer: release engineering, audit, provenance checks. Niche but deep;
buyers pay for certainty, not bulk.

- Seed: buy 0.05 / sell 2.00 per artifact trace.
- Value: buy 0.05 / sell 10.00 with retained evidence refs.
- Pair: quote beside content-hash as one provenance bundle.

## 8. content-hash — content-addressed digest and lookup

Buyer: builders checksumming artifacts, caches, dedup pipelines.
Commodity surface; price is a filter, not the margin.

- Seed: buy 0.01 / sell 0.05 per digest.
- Value: buy 0.01 / sell 0.25 with lookup included.
- Do not sell below compute; volume is the only lever here.

## Unlock order and open questions

1. Approve per-capability options above (or name your own numbers).
2. `finance_price_set` each approved name; verify its room shows the
   quote. Note the book/catalogue split: pricing puts it in the book
   every grammar quotes, but x402 settles only what the executable
   catalogue (compiled into the address organ) can deliver — check
   `finance_book` for both sets.
3. Set `BONFYRE_X402_FACILITATOR` on the service; verify a presented
   payment verifies instead of 402 `payment_unverifiable`.
4. Run `tests/circuits/cross_protocol_circuit.mjs` to prove cross-carrier
   price agreement and that unpriced capabilities still refuse.
4. Open: facilitator choice (self vs third party) decides custody and
   disputes before the first sale. Open: whether demo/estate
   institutions should ever carry prices. Open: counsel review of the
   owner-concentration disclosure floor.
