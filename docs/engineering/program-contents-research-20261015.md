# Product contents, source updates and native continuation

The previous cycle-date release implemented one focused action. The researched products support a larger unit of work: a chosen order or bundle, its line items, the recurring agreement, neighboring orders, changes affecting existing subscribers, and a continuing account. Rescheduling alone did not meet that level.

## Native merchant operation is first-class

Nick's direction is to support merchants whose business starts and runs in Aurekai. Shopify, Pipe17 and similar systems are optional connections, not prerequisites for operating the native product. This supersedes an adapter-first delivery order; it does not remove integration support.

Support native, connected and hybrid businesses through the same operation contracts. Native operations are authoritative for Aurekai-owned records. Connected operations retain the external owner. In hybrid mode, choose one authoritative writer per business fact and operation; an unavailable external account must never silently transfer ownership to Aurekai.

The native product must own merchant-authored catalogues, pricing and quote revisions, quantity-bearing inventory and reservations, order changes, recurring agreements, fulfillment, returns and financial obligations. Payment processing, banking, carrier execution and jurisdiction-specific services remain explicit replaceable boundaries. A missing optional provider may block its own effect, not the entire merchant workspace.

### Product behavior to build

A reviewed change should show affected products, quantities, stock or capacity, dates, future cycles, invoices and payment differences together. Confirmation must recheck the same dependencies that produced the preview. Preserve unaffected materializations and completed history. Previewing an outcome does not execute it.

Support goods, scheduled services and access entitlements in one commercial agreement while preserving their different completion rules. A kit can reserve components; a service can hold a time window; a digital item can grant bounded access. Do not turn all three into a generic delivered checkbox.

A supplier or partner should be able to participate through its own Aurekai account without buying another commerce platform. Explicit counterparty acceptance, permissions, allocated quantities and obligations remain necessary. A shared project does not merge sellers, stock ownership, banking or customer authority.

### First native acceptance target

Start with a merchant and customer using Aurekai accounts, with Shopify and Pipe17 absent. Publish an authored offer, calculate a revision-bound quote, accept under current permissions, allocate actual native stock or capacity, complete part of the obligation, revise only the outstanding part, and carry the resulting invoice, return or credit and verified payment reference through the existing financial owners. Use explicit test accounts and test rails in automated acceptance, never treat them as real sales.

Exercise a second service or net-terms agreement through the same owners. Fulfillment and billing dependencies must follow the agreement; settlement before fulfillment is not universal. Preserve distinct partial shipments, returns, captures and refunds instead of replacing a single exchange-stage row.

Fresh source inspection found that CommerceChain still requires the immediately previous enumerated stage and uses one replaceable row per exchange and stage. ResourceReservation provides temporal holds, not a demonstrated quantity-bearing stock ledger. The atlas describes Frappe extraction as metadata grammar, with runtime controllers and effect admission separate. These are implementation gaps, not features proven by a compiler or a new label.

The existing site acceptance path in cmd/BonfyreWebAddress/src/site_product.c already reaches commitment, Meter and Pay. Extend verified existing owners and business operations; do not create a parallel commerce database, scheduler, or provider-shaped core. Native C/domain operations, current typed contracts and native representations remain the target; required provider JSON stays at the interoperability boundary.

### Research references and release status

Medusa's inventory reservation lifecycle separates native stock allocation from fulfillment-provider shipping work: https://docs.medusajs.com/resources/commerce-modules/inventory/reservations-lifecycle

ERPNext's Sales Order documentation separates commitment, partial fulfillment and billing and makes downstream actions conditional: https://docs.frappe.io/erpnext/sales-order

Medusa's versioned order edit model keeps confirmed changes and outstanding payments explicit: https://docs.medusajs.com/resources/commerce-modules/order/edit

These references inform behavior and tests; neither product is a required runtime. This section records the revised product requirement. It does not certify or publish a completed native commerce engine.

## Product behaviors examined

Recharge's [Affinity 2.0 next-order documentation](https://support.getrecharge.com/hc/en-us/articles/22179636783895-Understanding-the-Affinity-2-0-next-order-page) describes a focused charge or order with state-dependent actions, product-level operations, other orders on the same date, products scheduled elsewhere, and an itemized summary. This implementation carries the focused-cycle and separate-program behavior into the existing Aurekai editor. It does not emulate processing, payment failures, delivery or completed-order states without an execution owner.

[Fixed-price customizable bundles](https://support.getrecharge.com/hc/en-us/articles/6977360362647-Getting-started-with-fixed-price-customizable-bundles) retain a price independent of the selected contents. [Dynamically priced customizable bundles](https://support.getrecharge.com/hc/en-us/articles/11808240171287-Getting-started-with-dynamically-priced-customizable-bundles) support content quantities within configured ranges. The Aurekai compiler now makes that distinction explicit. Existing prepaid and try-on agreements retain their own arithmetic. Authored selection rows contain product references and quantities; observed unit prices are resolved from their retained source.

Recharge's [bundle layout documentation](https://support.getrecharge.com/hc/en-us/articles/6978428049303-Configuring-customizable-bundles-layouts) describes guided selection with a live summary. Its [Bundles Widget 2.0 customization guide](https://support.getrecharge.com/hc/en-us/articles/40557120935575-Customizing-the-Bundles-Widget-2-0) states that the widget is storefront-only and that its customization interface is in Early Adopter status. Those are distinct from the Affinity customer-portal surface; this work does not claim they are one universal interface. The Aurekai selection workspace provides a product catalogue, scoped selection, quantity rules and a per-cycle consequence table within the existing editor.

[Managing existing bundles](https://support.getrecharge.com/hc/en-us/articles/6978928485911-How-to-manage-existing-bundles-in-Recharge) distinguishes changes to bundle definitions and existing subscribers. The corresponding Aurekai operation reviews a later capture against the saved programs using the old source. Explicitly selected programs can adopt it together; other programs keep their version. Stable SKU and product/offer addresses join the versions. Missing or ambiguous matches require manual replacement. This is source-version adoption, not a Recharge subscription update.

The sources above are public primary documentation. No authenticated competitor account, competitor billing operation or full product-parity exercise was performed.

## Existing owners extended

The canonical recurring-program compiler remains in scripts/recurring-commerce. The browser compiler and contents module are exact generated copies. Product selection belongs to the existing program's terms; each override addresses its original cycle date. The occurrence, source version and product identity remain distinct. A date move does not create another selected product or change later cycles.

The public editor uses the existing browser record store. A native package binds the same editor to the existing Auth, Pack, WebAddress and CMS workspace record service. The native path uses MessagePack and the existing record and blob representation; it does not add a store, backend service, JSON state file or alternative authorization mechanism. The immutable package is retained by the existing workspace package materializer. Available configuration definitions are projected and published through the existing PackGraph setup publisher.

Source adoption checks the selected programs and retained source revisions in one transaction. A failure refuses the whole selected batch. It does not overwrite the currently open editor. Reopening the saved version restores the adopted product references and calculated contents. Fixed-price programs do not acquire an itemized price just because their source prices change.

The combined upcoming view derives from the current program and the saved programs, replacing the current program's saved copy rather than counting both. Programs sharing one date are kept separate. Unknown prices and invalid contents remain unresolved rather than yielding zero-cost totals.

## Verification scope

The contents browser journey imports labeled fixtures through the existing capture review, builds recurring selections, swaps and adds a product for one cycle, changes defaults, undoes the edit, handles quantity errors, saves multiple programs and adopts changed source prices across selected programs. It tests a concurrent program edit, an unchanged fixed-price agreement, source-body preservation, mobile widths and a full browser restart.

The native journey uses isolated accounts and databases with the actual compiled Auth, CMS and WebAddress owners. It installs the published configuration, reads the same program as another authorized user, refuses a stale multi-program write, checks read-only and unrelated-account refusal, saves through the actual browser editor and opens the result in another browser context. The published server's WebAddress and CMS binaries were compared with those tested. A native fixture passing does not by itself establish a completed real customer installation.

Run the existing browser projection check, make -C integrations/browser test-workbench, integrations/browser/tests/workbench-interaction-browser.mjs, integrations/browser/tests/contents-browser.mjs and integrations/browser/tests/contents-native.mjs. The public browser journey uses the existing explicit WORKBENCH_TEST_ORIGIN option. Execution logs, exact source manifests, screenshots and release readbacks are retained under output/verification/workbench-contents-20261015.

## Remaining product scope

Provider-driven discovery and fresh capture synchronization, merchant price and inventory quotes, charge execution, payment recovery, shipment handling, discounts and subscription lifecycle operations are not implemented by this contents release. The native editor uses the destination selected at installation, but existing browser recurring programs do not yet transfer automatically with their complete source dependency set. The existing catalogue and room-draft adoption remains unchanged and separately verified.

Those are additional complete workflows, not claims that can be earned by adding their buttons. The public source-and-program guide documents the supported operations and exact storage destinations.
