# Three businesses. One software foundation.

Explore three independently named and configured websites built from the same Bonfÿre site and content owners. Each presents different material, business offerings and customer actions. Aurekai can deliver the application, while the finished experience belongs to the business.

## Open the businesses

| Business | Experience | Visit | Customer action |
|---|---|---|---|
| **Fernhollow Pottery** | Warm studio journal, hands-on classes | [Visit Fernhollow](http://fernhollow.localhost:8917/classes) | Ask about an $85 studio workshop |
| **Tallgrass Bike Works** | Dark workshop, service and repair notes | [Visit Tallgrass](http://tallgrass.localhost:8917/clinic) | Request a $35 flat-tire clinic |
| **Juniper Lane Bakery** | Light neighborhood bakery, seasonal writing | [Visit Juniper](http://juniper.localhost:8917/bakery) | Enquire about a custom cake |

These `.localhost` links open on the Mac hosting the local demonstration, not from an external machine.

### Fernhollow Pottery

A quiet studio design with warm ceramic tones and an editorial feel. Class information, dates and booking enquiries belong to the studio. The workshop offer is fixed at $85 per person.

[Read the classes](http://fernhollow.localhost:8917/classes) · [View the offers](http://fernhollow.localhost:8917/offers)

### Tallgrass Bike Works

A near-black shop identity with warm industrial accents. The flat-tire clinic has its own technical instructions, customer enquiries and a bookable offer.

[Read the repair clinic](http://tallgrass.localhost:8917/clinic) · [View the offers](http://tallgrass.localhost:8917/offers)

### Juniper Lane Bakery

A pale, botanical editorial presentation. The bakery publishes its own writing, and a cake enquiry remains an enquiry until terms and price are agreed.

[Read the bakery notes](http://juniper.localhost:8917/bakery) · [View the offers](http://juniper.localhost:8917/offers)

## Under the customer experience

Each business has an independent host, site subject, published content and MCP address. The operating system uses a common set of native owners.

| Customer sees | Bonfÿre provides | Operator uses |
|---|---|---|
| Pages, offers and answers | CMS, WebAddress, presentation templates | `bonfyre site page`, `offer`, `show` |
| Questions and booking requests | Site MCP, scoped credentials and lead records | `bonfyre site inbox` |
| Confirmed work | Commitments, terms and operator actions | `bonfyre site accept` |
| Invoices and settlement records | Meter, Pay, Finance and source-linked books | `bonfyre site paid`, Finance books |
| Content from other systems | Native Bridge and import/export tools | `bonfyre bridge`, `site import`, `site export` |
| Search and assistant access | Site search, scoped MCP tools | `bonfyre site search`, `mcp`, `usage` |

The assistant route is not the product. Each site works in a regular browser, with MCP as another permitted interface over the same records.

## Reuse without blending customers together

A partner can configure the same native components for a second business without copying the first business's identity, enquiries, financial history or permissions. A new website is not a new data platform.

The current native presentation owner supports per-site colors and brand tokens. The fuller design system also needs typography, templates, graphics, favicon assets and layout choices to be applied per customer. The theme attributes live in `presentation_template`; WebAddress is their renderer. It is the right owner for the remaining visual work.

## Extend the examples

Use [white-label delivery](/white-label-delivery) for customer scoping and site setup. Follow [recording to workspace](/recording-to-workspace) for audio-powered content, [request to books](/request-to-books) for commercial operations, or [one application, several interfaces](/interfaces-and-mcp) to try the MCP and terminal views.
