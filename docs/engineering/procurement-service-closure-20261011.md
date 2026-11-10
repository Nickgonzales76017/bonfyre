# Procurement and service closure — native continuation

## Actual supplier facts and corrected source records

Supplier WARP corrected an earlier freight quote by Gmail `1a122594a6cbad21` on 2026-10-09 at 20:27:19 UTC. Houston 77041 to Dallas 75208 is **$630**; Houston to Stafford 77477 is **$250**, both before any unissued first-shipment credit. These are full cargo-van rates conditional on the load fitting, and the supplier requested freight photographs. This was not a booking, accepted purchase, or authorization to ship.

Native CMS source: `~/.bonfyre/estate-local/cms.db`, namespace `craigslist-zero-close`, type `frappe_erpnext_supplier_quotation` and projected 68-field schema `packs/wholesale-20k/schemas/supplier-quotation.json`. The earlier aggregate of $880 concealed `total` fields reversed across the two route records, even though their `grand_total` fields already matched the corrected source.

| Source record | Route | Previous total | Corrected total | Grand total |
|---|---|---:|---:|---:|
| #1 | Houston → Dallas | $250 | **$630** | $630 |
| #2 | Houston → Stafford | $630 | **$250** | $250 |

The first guard refused an attempted correction of the wrong field, with no mutation. Both later updates used CMS `--if-field terms --if-value <exact existing source terms>`, after checking supplier, route, opportunity and draft state. Readback verified that terms were preserved, both amounts agreed and both quotations remained drafts. Update receipts: `b23a2b29-74d0-4326-8ab9-6197ae72dd70` and `2823e787-53d5-474e-85e3-22c5cf92a914`. Readbacks: `cd511f56-7335-4609-8434-7321f27eb402` and `10e80118-e6fe-48e8-81bd-a309f101cbab`.

These records have no demonstrated purchase order, actual payment, delivered freight, line-item tax assessment or real ERPNext controller execution. Several Frappe-shaped monetary fields remain unset or zero; totals alone cannot supply the remaining terms.

## Separate procurement from buyer sales evidence

The original already-SENT supplier enquiries were backfilled without dispatch: Outreach **#40 WARP**, source `gmail:1a11e86d55573537` and **#41 IMM**, source `gmail:1a11ea4ad402f437`. Both use `procurement:` relationship IDs so the Finance buyer/agency join does not mistake vendor replies for buyer interest.

Four supplier replies were read from Gmail and linked as neutral, retained observations:

- #40 WARP's initial superseded prices `gmail:1a120fdee1520f1c` and corrected conditional quote `gmail:1a122594a6cbad21`.
- #41 IMM estimator assignment `gmail:1a1215117d582033`, and requested dimensions, access conditions, site contact and photos plus a *potential no-charge site survey* `gmail:1a12196acf9f0afa`.

IMM did not quote rigging at $0 or receive instructions to visit; WARP did not receive a freight booking. Original reply rows remain distinct from the latest classification. Native Outreach reports **41 locally logged entries, seven human classifications and three automated acknowledgments**; this is neither provider-confirmed delivery nor revenue.

## Native finance code now has two additional protections

Source `cmd/BonfyreFinance/src/books.inc` now projects the draft supplier-quote count and `supplier_quotations_unexplained_amount_difference` as off-books observations. The anomaly checks a material difference between `total` and `grand_total` only if declared tax, additional discount and rounding adjustments are zero. Legitimately taxed/discounted quotes do not become false anomalies. This flag alone does not prove the quote correct or that it can be accepted.

Source `cmd/BonfyreFinance/src/relationships.inc` now:

- Counts as closed only requirements whose state is **satisfied**. The states `available` and `blocked` remain open.
- Interprets a cost-basis witness as `material/cost_basis_confirmed`, state satisfied, with nonempty evidence, rather than querying unsupported `kind=cost`.
- Treats `positive|negative|neutral` as human replies for the sales relationship view. An automated acknowledgment never promotes an opportunity to human `replied`.

The Finance Makefile now names `src/relationships.inc` as a build dependency, after an isolated test exposed the stale checkout executable when this include changed. The Makefile's `smoke` and `test` recipes include `supplier_quote_memo.test.mjs` and `relationship_states.test.mjs`.

**Execution witness:** a targeted native `cmd-make` for BonfyreFinance rebuilt the checkout binary and passed **6 tests, 152 assertions and 0 failures**, including finance pipeline tests, amount-reversal detection, legitimate tax/discount cases, requirement states and human-vs-automated response separation. These are isolated regression tests. The managed `~/.bonfyre/bin/bonfyre-finance` has **not** been synchronized; the checkout and installed runtime may still report different projections until an explicitly authorized promotion.

## Live commercial closure is unchanged

The declared Finance store (`packs/wholesale-20k/coordinates.json`) contains 21 opportunities, $6,150 in stated proposed compensation, 111 qualification requirements, and $0 actual revenue. The cross-owner Finance/CRM/Outreach/Commerce view has 20 CRM matches, 16 buyer/agency outreach matches, nine physical lot listings and furthest states of 5 specified, 14 sent, and 2 replied. Pay reports **zero invoices and $0 collected**.

An actual canonical Finance `opportunity queue-batch` request returned **queueable 0**, with `human_line outreach_not_authorized` (receipt `a81b922a-b241-4b91-8e04-c11ab18a364d`). Neither supplier quote, positive human correspondence nor stated proposal price establishes purchase authority, verified cost basis, customer acceptance or cash.

## Finish these two different native transaction paths

**Buyer-side procurement** is not CommerceChain's seller-side progression. A supplier quote may become a purchase obligation only after current revision, quantities and fulfillment, actual terms, supplier ability, valid buyer authorization, counterparty, and institutional acceptance are established. Bind the native Actor/AuthorityGraph *before* recording a buyer-side `CommitmentGraph` event; its current `authority` property is a recorded string, not an enforced grant. Reuse native CommitmentGraph and ProtocolContract, not another procurement database. A signed purchase order, supplier acceptance, receipt of goods/services, payable, payment and settlement must remain distinct. The two WARP quotes and IMM enquiry must currently refuse acceptance.

**Service engagement** takes an approved customer proposal and source-linked client consent through authenticated commitment, WorkGraph execution and verified delivery before an invoice and eventual external payment confirmation. Finance already has separate `proposal`, `authority`, `customer`, `execution`, `verification`, `delivery`, `payment`, and `retention` attestations. Source test fixtures cannot replace actual counterparty consent or Pay/bank readback. The existing WebAddress `site accept` path records local commitment and potentially an invoice, but it neither notifies the customer nor proves a remote service agreement or settlement.

Ship next as two minimal execution chains with refusals first: valid quote/actor/terms produces one buyer-side commitment (on an isolated fixture), and accepted service scope produces exactly one source-linked work and billing chain. Then attach externally authorized counterparties. Preserve separate client identity, revenue recognition, return/refund and accountant export. No supplier purchase, recipient notification, payment, public deployment, or managed-runtime promotion was performed in this pass.
