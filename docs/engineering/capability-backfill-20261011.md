# Frappe/ERPNext behavioral backfill — source census and operating closure

## The census, not a parity claim

The original donor inventory contains **1,152 DocTypes** across **44 modules and nine apps**. The Oct 10 baseline ties **301** to Bonfÿre presentation views; **851** have no such tie. Presentation ownership does not prove the source application's mutation, validation, authorization, accounting or settlement behavior. The baseline checked only **18 broad capabilities** (6 proven in a bounded sense, 5 partial, 3 native verbs unproven against real effects and 4 absent), not the full application scope.

This pass re-read the actual donor JSON definitions and enumerated every DocType with \`"is_submittable": 1\`: **129 distinct documents across 16 modules**. These are *submittable source DocTypes*, not necessarily 129 independently payable financial transactions, and not 129 proven native capabilities. The new source-linked inventory is in the three adjacent TSV files:

- \`capability-backfill-20261011-erpnext.tsv\`: 74 ERPNext source DocTypes.
- \`capability-backfill-20261011-hrms.tsv\`: 53 HRMS source DocTypes.
- \`capability-backfill-20261011-other.tsv\`: 2 source DocTypes from Insights and Wiki.

Each row contains application, module, exact DocType, \`is_submittable=1\`, source JSON path, a preexisting baseline capability ID only when a named match exists, what that comparison actually proves, related owner family names for *navigation*, and \`doctype_effect_proven=false\`. Related owners are **not** assumed to support the donor's effect. The verified census totals are 74+53+2=129, with 129 unique paths and zero positive DocType-effect claims.

The architecture atlas is a different population: at the time of this read it reports **499 architecture entries** (260 implemented, 129 measured, 75 declared, 35 architectural) and the native inventory reports **178 available binaries**. Neither equals an enterprise transaction count. The atlas, binaries, DocTypes, active application programs, and 18 behavior probes are not comparable denominators.

**Controller behavior census:** A separate read of each submittable DocType's matching Python controller found **340 named lifecycle hooks on 123 of the 129 DocTypes**: 118 `validate`, 26 `before_submit`, 94 `on_submit`, 85 `on_cancel` and 17 `on_update_after_submit` definitions. See `capability-controller-hooks-20261011.tsv` for all 16 modules, and the 129 source rows for their JSON identities. Six DocTypes had no matching named hook under this scan; this does not prove that they lack inherited or other behavior.
These 340 are **source-level controller method definitions**, not 340 independent customer functions and not 340 Bonfÿre effects. Prove each relevant precondition, state change, readback, refusal and downstream obligation before marking a native replacement equivalent.


## Source module distribution

| Source application/module | Submittable DocTypes |
|---|---:|
| ERPNext Accounts | 27 |
| ERPNext Stock | 15 |
| ERPNext Setup | 1 |
| ERPNext Manufacturing | 7 |
| ERPNext CRM | 2 |
| ERPNext Assets | 8 |
| ERPNext Buying | 4 |
| ERPNext Selling | 3 |
| ERPNext Projects | 2 |
| ERPNext Subcontracting | 2 |
| ERPNext Telephony | 1 |
| ERPNext Maintenance | 2 |
| HRMS HR | 36 |
| HRMS Payroll | 17 |
| Insights | 1 |
| Wiki | 1 |
| **Total** | **129** |

The nine exact-name baseline pointers in the 129-row census are \`Budget\`, \`Journal Entry\`, \`Payment Entry\`, \`Sales Invoice\`, \`Request for Quotation\`, \`Supplier Quotation\`, \`Quotation\`, \`Project Update\`, and \`Telephony Call Type\`. These pointers are *comparison anchors*, not acceptance. For example, Pay invoicing admitted usage is not ERPNext Sales Invoice; Finance's read-only derived journal is not an editable posted Journal Entry; FreeSWITCH loopback calling is not every behavior of Telephony Call Type.

## Backfill executable behaviors, not tables

Each Donor DocType must ultimately bind to:

1. An actual business **act** with actor, institution, source contract, inputs, preconditions and reversible/refusable effect.
2. Its existing Bonfÿre **owner**, or an explicit absent owner with named integration work.
3. A **real invocation** against identified source records, not a prewritten sample pretending the act occurred.
4. A **result and readback** from the owner or counterparty, with stable release/contract identity and receipts.
5. Negative checks for wrong actor/customer, duplicate/replayed event, stale source, unknown price/availability and missing approval.
6. A customer/operator/native/MCP **projection** of the same event only after the action and evidence exist.

A DocType can be represented faithfully as source schema and still fail items 1–5. A browser page can look excellent and still have no transactional owner. A simulated provider callback can test code without proving a bank settlement.

## Priority native closure chains

| Chain and source DocTypes | Existing candidate owners | Correct next witness |
|---|---|---|
| **Sales and cash:** Quotation → Sales Order → Delivery Note → Sales Invoice → Payment Entry → Journal Entry | BonfyreOffer, Commitment, CommerceChain, Pay, Finance | Source-bound buyer acceptance, delivery evidence, correctly numbered/invoiced lines, externally acknowledged payment, reconciliation and refund |
| **Buying and vendor obligations:** Material Request → Request for Quotation → Supplier Quotation → Purchase Order → Purchase Receipt → Purchase Invoice → Payment Entry | CMS, Project/Work, Finance, Commerce, offer/contracts | A vendor quote becomes *buyer-side* commitment, received goods are checked, payable and actual settlement reconcile. Buying is not a seller's CommerceChain. |
| **Stock and logistics:** Stock Reservation Entry → Pick List → Stock Entry → Shipment → Delivery Trip → Stock Reconciliation | CommerceChain, resource/capacity owners, Bridge | Real quantities, custody, reservations, discrepancy, refusal of double allocation and updated stock balance; a listing does not establish inventory. |
| **Manufacturing:** BOM → Production Plan → Work Order → Job Card → Quality Inspection → Stock Entry | WorkGraph/Project, program/compiler, material resource owners | Materials/operations are scheduled and consumed with lot and quantity readback; designating a BOM is not manufacturing goods. |
| **Workforce:** Job Offer → Employee Onboarding → Attendance/Shift Assignment → Expense Claim → Timesheet | Actor/Authority, HR-facing CMS, WorkGraph, Time, Finance | Explicit employment/contract terms, authorized allocation of time and claims, leave and attendance; employee data isolated from customer/member data. |
| **Payroll:** Salary Structure Assignment → Payroll Entry → Salary Slip → Payment Entry | Finance, Pay, external regulated payroll/provider | Computed employer/employee taxes, withholding, earned amounts, provider acknowledgement, payouts, corrected payroll and period close; presently absent parity. |
| **Assets and field service:** Asset → Asset Movement → Maintenance Schedule → Maintenance Visit → Asset Repair | CMS/source/space, WorkGraph, Commitment, Finance | An actual owner accepts a work order, source equipment/parcel is identified, repair and acceptance are read back, bills and payments stay separate. |
| **Creator and membership:** Process Subscription, creator content/program, client request, attended call | CreatorChannel, Gate, Meter, Pay, Finance, Auth, WorkGraph, MediaProgram | Renew/cancel periods, scoped member entitlement, separate coach consent, service credit consumption on completed work and processor-verified settlement. |
| **Bank and accounting:** Bank Transaction → Payment Reconciliation → Journal Entry → Period Closing Voucher | Statement Import, Pay, Finance, CloseConsequence | Reconcile actual bank/source transactions with booked obligations, not a bare payment reference; present unmatched sources explicitly. |
| **Knowledge and analytics:** Wiki Page Patch, Insights Table Import | CMS/Wiki projections, Insights/browser analytic readers | Source-changed article/history or imported table survives edit, validation and readback under correct author and institutional visibility. |

These chains are chosen because they reuse broad estate organs and solve observable buyer work. They do **not** claim all ten end-to-end transactions are already implemented. Fixing one owner should close several DocTypes and product journeys, rather than writing a bespoke enterprise application for each donor form.

## Live commercial backfill readback

Use the declared working coordinates (not Finance's unrelated home default):

- Finance and CMS: \`packs/wholesale-20k/coordinates.json\` → \`~/.bonfyre/estate-local/finance.db\` and \`cms.db\`.
- Outreach: \`~/.local/share/bonfyre/outreach.db\`.
- Commerce: \`~/.bonfyre/estate-local/commerce.db\`.
- Pay: its independently declared owner store. Do not infer a collection from Finance or Outreach.

A native \`bonfyre-finance --db ... relationships --cms-db ... --outreach-db ... --commerce-db ...\` readback reported **21 Finance opportunities**, **20 CRM-linked**, **16 directly Outreach-linked**, **9 Commerce listings**, **4 stated prices** and **zero accepted/paid stages**. The 21 Finance opportunities total **615,000 cents in stated compensation** and zero actual revenue.

In this pass, seven selected existing buyer emails were inspected against SENT evidence and recipients. Six unique originals were backfilled as Outreach **#34–39** (Vader/Conesco, Conroe/BigCash4Toners, CAMaster/Resell CNC, Lewisville/MFO, Houston/2nd Life and Hurst/Capilleh). No message was sent, and the 39-row total is the local outreach population, not delivered mail. A TonerG email covering **both** Canon and Dell lots was already linked to a different source/timestamp and was **refused** as a second Covina row. It must remain a multi-opportunity source relationship, not two independent buyer responses.

Eight existing Gmail-origin opportunity records (#8–15) were read back against exact original SENT message IDs, recipients and UTC timestamps. Running the idempotent Outreach backfill returned those same existing row IDs; no duplicates or recipient messages were created. Those entries still use legacy \`dm\` channel labels even though their medium was email. Channel correction needs owner-level evidence and a migration, not a hidden rewrite in a report.

The five without direct Outreach joins: toner exit business listing (no confirmed buyer outreach), Covina (shared TonerG source, not a standalone send), San Antonio Laguna (no verified buyer send), India direct exporter (no named buyer), and CAMWS RFP (no submitted proposal). Closing the denominator by creating synthetic sends would destroy the utility of the join.

## Work to turn the backlog into demonstrated applications

**Step 1 — binding:** use the three source inventories as the remaining-coverage frontier and assign native owner identities to each transaction and grouped chain. Do not claim \`applied\` merely because a donor schema compiles to a list/form. The \`packs/capability-bar/crosswalk.yaff\` baseline remains historical truth and should be advanced only through its owner and recorded tests.

**Step 2 — priority operations:** first close supplier quotation → accepted purchase and service quote → accepted engagement. These bridge the real agency proposals, creator coaching commitments and physical wholesale workflows while keeping Commerce seller stages distinct from buying.

**Step 3 — money:** close billed/collected/refunded states with real provider acknowledgment, reconciled Finance books and precisely scoped creator/member entitlements. Enable tax reporting export of provider facts without pretending to remit taxes or file returns.

**Step 4 — native and visual distribution:** project witnessed events into partner/client sites, the creator studio, operator desks, generated documentation, native SDKs, CLI and MCP clients. Let white-label logos/favicons and typography change without changing the seller, payment, consent or grant coordinates.

**Step 5 — certification:** one successful event is not 129 successful DocTypes. Record the release/binary, counterparty/actor, input/state, effect, readback, refusal and unresolved downstream dependency for each closure. Re-evaluate the 129-row manifest after every demonstrated chain.

The repo guard permits additions under \`docs/engineering\` but rejects changes to \`packs/capability-bar\` through the autonomous low-risk patch path. The three files are therefore a source-linked engineering backfill, **not a hidden edit to the authoritative capability catalog**. The historical baseline and other lanes' dirty code were left untouched.

## Finance qualification requirements restored to the declared store

At the start of this pass, the explicitly declared `~/.bonfyre/estate-local/finance.db` contained all nine wholesale opportunities but **zero of their original qualification requirements**, while a separate home-default Finance database held eight per opportunity. The original source TSV `packs/wholesale-20k/analysis/eligibility-requirements-20261009.tsv` was re-read: 72 unique keys, nine existing Finance IDs, eight requirements per ID, all `unresolved` with empty evidence.

The native transaction-safe `bonfyre-finance --db ~/.bonfyre/estate-local/finance.db opportunity requirements-import --file packs/wholesale-20k/analysis/eligibility-requirements-20261009.tsv` imported **72**, with **0 duplicates** (receipt `d66e3136-a2d8-408b-8b91-c8527e9cf98d`). Immediate replay returned **0 imported / 72 duplicates** (receipt `b801dcd0-92f1-4669-a957-f46fd7c33069`). A subsequent rank readback showed all nine wholesale IDs at **0/8 closed**, retaining their unresolved seller control, inventory, specifications, actual buyer bid, fulfillment, settlement and authority conditions.

Across the entire canonical working Finance book the latest readback shows **21 opportunities / 111 requirements / 0 closed**: 72 wholesale requirements and 39 other opportunity requirements. No stage was promoted, compensation recognized or cash collected. This corrects *store-bound missing qualification*, not the external facts those requirements still seek. Future requirements and proof must be entered through the same Finance owner with source evidence rather than copied from a different default database.
