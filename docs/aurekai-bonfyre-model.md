# Aurekai and Bonfÿre

**Built on software. Driven by Aurekai.** The business develops distinctive digital operations; Bonfÿre supplies the shared native software. Each customer, creator, brand or partner retains its own identity and authority.

Aurekai is an operating business that builds, improves and runs digital products, services and communities. Bonfÿre is the shared software infrastructure we use to do it.

Aurekai can work with a local shop, a publisher, a property or equipment supplier, a creator and a software business. Those experiences may have different names, customers and commercial relationships. Underneath, they can share identity, information systems, applications, software development, payments, models and computing resources.

This is the idea behind Aurekai as **one company economically and many businesses experientially**. It can support independent participants and partners while operating its own products and projects. The four Aurekai domains, along with independently branded sites, give that larger system places where people can actually use it.

## Why we're building this

AI has made implementation faster. Agents can write large amounts of code, assemble applications, research unfamiliar systems and run tests in parallel. The same acceleration hasn't happened in the meetings, planning queues, departmental handoffs, vendor integrations and manual reporting that surround software projects.

A small team can now make something ambitious quickly, but it may not know enough about accounting, retail, identity, media, regulation or hosting to operate it. A large organization has that expertise, but often spreads it across departments and systems that don't communicate well. Billable planning and repeated coordination can make the problem worse.

Our answer is to keep the useful parts of every project—code, data models, connections, product knowledge, distribution, relationships and operating routines—available for the next one. In Aurekai those assets are part of the business. In Bonfÿre they're represented as software that can be combined, executed and improved.

The “digital private equity” analogy fits because Aurekai can invest engineering hours, AI compute, reusable applications, technical operations and distribution alongside financial capital. The commercial relationship depends on the project, whether that's a product we own, a service engagement, a licence, a partnership or a share in the outcome.

## How the businesses fit together

There are several ways someone might encounter Aurekai.

**A business owner** may want to move an aging WordPress site, connect their inventory and customer records, add a booking workflow or give employees a cleaner operating system. They care about their customers and their business, not what compiler we used. Our white-label work includes examples with distinct retailer brands, domains, content, tools and customer permissions.

**A creator or publisher** may want a home for articles, channels, recordings and a community. Their work can also be useful in a reading room, topical collection, personal profile or marketplace. In our publication work, the source belongs to the publisher or creator while different parts of Aurekai give it additional ways to reach people.

**A buyer or supplier** may need catalogues, product options, pricing, procurement, freight or a way to see how goods fit in a physical space. The native spatial work also handles time-aware observations and geographical queries for research, local planning, publications, community/civic material and asset contexts. 2D/3D room planning is one historical offer, not the definition of the spatial engine.

**An organization** may need CRM, finance, staff, projects, service tickets, training and documents. The Frappe-derived application work makes those established business grammars available within the native Bonfÿre system. A business can choose the applications it needs, and each one can handle its own records and workflows.

**A person or community** may have a public profile, collections, posts, members and shared projects. The plan for Aurekai profiles and organizations is broader than a feed of posts: the profile is a recognizable place for the person's contributions, publications, relationships and activities.

**An external provider or existing system** may supply inventory, data, an API, software, computing resources or distribution. Bonfÿre discovers and describes what is available; Aurekai looks for practical ways those resources can contribute to a real business or product.

The teams and products need different interfaces, but their accounts, source systems, computing tools and business applications can draw from the same infrastructure.

## The four public destinations

| Destination | What people do there |
|---|---|
| [aurekai.work](https://aurekai.work) | Find organizations and opportunities, run account workspaces and use professional applications such as CRM, projects, finance and service |
| [aurekai.life](https://aurekai.life) | Build profiles, collect publications, follow interests, participate in communities and organize personal material |
| [aurekai.live](https://aurekai.live) | Publish media, follow channels, use creator tools, manage recordings and take part in public events and experiences |
| [aurekai.shop](https://aurekai.shop) | Explore goods and offers, work with suppliers, configure products and use purchasing or sourcing tools |

These are **territories**, not the underlying architecture. A company or creator may also have a subdomain or independent host. A website, API resource, account workspace and media channel can refer to the same person or company, with different views and permissions.

## What has actually been built, and why it matters

**The innovation is not “everything flows through Matrix.”** The breakthrough being pursued is the ability to start with radically different source systems and human jobs, **preserve the original semantics and rights**, produce a useful deliverable in the format the receiving party actually needs, and leave a verifiable path to continue or correct the work. Each participating system keeps a distinct owner and protocol.

~~~mermaid
flowchart LR
  WP["WordPress posts, PHP sites, legacy panels"] --> MIG["Signed bridge, WXR exchange, migration inventory"]
  FILE["Excel, paper scans, PDF, DOCX, captions"] --> WORK["Books review, OCR, document formatter"]
  DEV["Git projects, React/Puck, GrapesJS, Frappe"] --> CODE["Typed editing, native renderer, source changes"]
  CREATOR["Channels, communities, podcasts, publisher sites"] --> MEDIA["Account-linked publication, media and participation"]
  AGENT["Claude, Codex, ChatGPT / MCP clients"] --> OP["Scoped tools, read bundles, draft preview and revisioned update"]
  MIG & WORK & CODE & MEDIA & OP --> OUT["Independent deliverables + checked next-step rights"]
~~~

### Different inputs, real outputs and evidence

| Workstream | Technical work already present | What it concretely produces |
|---|---|---|
| **Legacy publishing** | WordPress REST capability discovery, HMAC-SHA256 signed webhook, stable remote site/post/version identity, native CMS external-ID sync | Observed native content entry, separate authoring/review; not unapproved WordPress publication |
| **Content portability** | `bonfyre site export` produces standalone HTML with embedded styles, WordPress WXR 1.2 and Frappe documents; local round-trip tests check exact article text | Offline page or format-native files another system can ingest, subject to external destination tests |
| **Hosting and PHP modernization** | `BonfyreBrief` reads authorized cPanel/InterWorx/Plesk exports and cites discrepancies; `BonfyreBridge` checks migrated database row values/type differences | Inventory, known exceptions and testable staged migration plan, not a guessed successful cutover |
| **Excel and business finance** | Native bounded XLSX cell parser, retained sheets/coordinates, categorized review workbooks; domain-specific FinancialWorkbook with revisioned MCP read/preview/update | A reviewable spreadsheet and the *same* saved account record, without automatic ledger posting |
| **Printed records** | On-device BonfyreOCR uses macOS Vision/PDFKit and retains each recognized line's page, position and confidence | Source-grounded extracted text for human verification, rather than “AI knows what the paper says” |
| **Reports and published documents** | BonfyreEmit/PandocFormatter perform specific Pandoc and native RSS conversions | HTML5, PDF, EPUB3, TXT, RSS and DOCX where dependencies allow, with format-specific acceptance |
| **Frontend and applications** | Puck uses React; GrapesJS and Editor.js attach to one typed authoring tree; MD4C/Mustach/C produce native HTML; 27 ServiceSite section owners and Frappe CRM source program have local parity tests | Editable site, article or actual application grammar without copying a donor's runtime HTML |
| **Developer work** | Project MCP with schemas/resources/prompts, authorized patch/test tools, scoped read-bundle coalescing; GitHub/Forgejo/Josh source operations | Observable code changes and receipts; filtered checkout; a separate live release gate |
| **Installed account software** | Workspace MCP returns installed editor/record/revision; specific workbook calls recalculate/preview; Catalogue and Room Planning can reference one saved record and attachment | One continuous workspace record across UI and agent interaction |
| **People, groups and media** | Native community memberships, accepted posts/reactions, creator audio schedules, transcripts and channel/media grammar | Participation, articles, groups or scheduled audio tied to their actual owner and audience |
| **Spatial research** | SpatialObservation includes observed/effective/expiry/recorded times; BonfyreSpatial has indexed reconstruction/query owners | Historical/geo/source investigation and maps; **room design is only one application** |
| **Outside services** | 200 account-enrollment targets, 104 downstream-provider prospects and 20 studied platform interaction grammars, with separately scoped credentials | A truthful pipeline from candidate to admission to verified provider effect, not 324 fictitious integrations |

See the [operational source/output ledger](real-workflows-and-interoperability.md), [MCP tool guide](../integrations/mcp/README.md), [WordPress bridge](../integrations/wordpress/README.md), [provider admission](../integrations/browser/onboarding/README.md), [frontend architecture](authoring-frontend-architecture.md) and [native Atlas](../architecture/README.md). Each includes named sources and what the current tests do **not** establish.

## Why the work connects without becoming one system

**A job posting** retained through the source frontier may be presented in a reading experience, *optionally* offered as a CRM opportunity to an installed app, or turned into a reviewed narrated edition. No posted source becomes a financial transaction just because the CRM sees it.

**A publication or community** can use a rich authoring document, membership-aware forums, a publisher page, an account-owned podcast, a shared source library and an interactive geographical feature. Those pieces do not share one database or one right-to-publish; the continued identity and the documented handoffs are what make the experience coherent.

**A small organization** can review a real XLSX, import files, consult a compiled Frappe CRM form, draft an offer, produce a PDF, and let a colleague or an authorized MCP client continue the exact saved version. Excel cell arithmetic, organization membership, office-document conversion and payments stay with their original owners.

**A developer or agency** can read Git source, compare C/React/legacy PHP components, test a proposed patch, export a WordPress interchange file and arrange a real hosting cutover. There is **no universal React/Svelte/PHP converter** proved by the current source: frontend behavior, server effects and migration must be independently validated.

**A creator** can declare YouTube/Patreon/Spotify public profiles and compose their own Aurekai channel, program or recordings. A public profile locator is not a working OAuth grant; a studied YouTube/Spotify interface grammar is not an authenticated account or licensed playback. **Toast** remains a commercial POS target, not a proven restaurant transaction integration.

The company-level value comes from making these **specific, reusable, independently accountable operations** available across many engagements and noncommercial contexts—not from reducing education, creative work, communities, research, commerce and old business systems to a handful of website demos.

## Why the shared technology matters economically

The same engine can help with very different types of capacity:

- **Software and compute:** existing applications, models, processors, APIs and idle machine capacity
- **Goods and physical resources:** inventory, rooms, property, equipment, storage and services
- **People and organizations:** skills, service providers, teams, publishers, businesses and community groups
- **Audience and distribution:** channels, publications, customer relationships, discovery and media
- **Commercial operations:** quotations, agreements, work, invoicing, payments, fulfillment and reporting

Aurekai can organize these resources into products or business relationships. Sometimes that means developing software. Sometimes it means improving a company that already operates. Sometimes it means providing a service, arranging distribution or making an underused capability available to someone who needs it.

The shared software makes that range possible. A piece of source handling, a new account application, a finance integration or a creator tool can become useful in more than one part of the business.

## Research informing the product portfolio

The research helps us choose where software investment could matter most. In its [October 2026 report](https://www.uschamber.com/technology/2026-empowering-small-business-report), the U.S. Chamber found that 41% of surveyed small businesses use six or more technology platforms. A company with CRM, inventory, staff, finance and online sales already has plenty of software; the opportunity is making those systems useful together.

Existing websites matter, too. [W3Techs reported that WordPress powered 39.4% of measured websites on November 7, 2026](https://w3techs.com/technologies/comparison/cm-wordpress). That helps explain why our legacy PHP, WordPress, database and hosting modernization work sits alongside the newer native web platform. Content, reports and business relationships built over years have real value.

For creators, the [IAB's September 2026 CreatorFronts recap](https://www.iab.com/blog/2026-iab-creatorfronts-event-recap/) projects around $44 billion in U.S. creator advertising during 2026 and describes the need for <q>reducing friction in the Creator Economy</q>. Channels, profiles, publishing, source libraries, customer relationships and offers are parts of that practical infrastructure.

And [Stanford HAI's 2026 AI Index](https://hai.stanford.edu/ai-index/2026-ai-index-report/economy) reports 88% of surveyed organizations used AI in at least one function in 2025, while agent deployment in particular functions remained much earlier. Bonfÿre's native agent and application work addresses what happens after a model produces output: the receiving systems, data, resources and people involved.

These are dated third-party observations, not measurements of Aurekai revenue or adoption. The [Aurekai market research page](https://nickgonzales76017.github.io/bonfyre/market.html) links the original sources and the product examples that led us to them.

## Current development estate

The local Bonfÿre V6.1.0 runtime and architecture registry reported these figures on November 7, 2026:

| Area | Current recorded inventory |
|---|---:|
| Native command families | 93 |
| Frappe-derived DocTypes | 1,840 across 51 modules |
| WorkGraph | 20,487 nodes and 45,412 edges |
| Active backend adapters | 14 |
| Architecture Atlas | 556 entries (346 implemented, 152 measured, 24 declared, 34 architectural) |

These figures describe the installed development system. The public repository and the four public websites have their own release histories, which may be at different revisions.

## Read more

- [Aurekai products](https://nickgonzales76017.github.io/bonfyre/explore.html) and [public applications](https://nickgonzales76017.github.io/bonfyre/#experiences)
- [Bonfÿre technical guide](https://nickgonzales76017.github.io/bonfyre/docs.html), [source architecture](architecture.md) and [native repository](https://github.com/Nickgonzales76017/bonfyre)
- [White-label businesses](https://docs.aurekai.work/three-businesses), [operating-products guide](https://docs.aurekai.work/operating-products) and [work samples](https://aurekai-work-samples.pages.dev/)
- [Quickstart](../QUICKSTART.md), [contributing](../CONTRIBUTING.md) and [source atlas](https://nickgonzales76017.github.io/bonfyre/modules/)

The public pages introduce the products. The technical documentation explains what developers can use and how it works. Developers can find version-specific tests, operating requirements and limits in the documents for each component.
