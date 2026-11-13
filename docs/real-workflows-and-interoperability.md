# Bonfÿre and Aurekai: real workflows, interfaces, exports, and interoperability

**Read this before substituting “Matrix,” “capability,” “integration,” or “MCP” for an implementation.** Aurekai does not ingest every product into one registry and then magically serve it. Different systems retain their own data, commands, rights, procedures, format requirements and users. Bonfÿre implements **specific bridges, format converters, account-bound editors, first-party command organs, native web renderers, and transport protocols**, with exact handoffs between them.

[Main README](../README.md) · [Native authoring](authoring-frontend-architecture.md) · [Architecture](architecture.md) · [Atlas of detailed contracts](../architecture/README.md)

## The operating model: many independent paths, not one central funnel

~~~mermaid
flowchart LR
  HOST["WordPress / cPanel / InterWorx / Plesk"] --> MIG["Import, inventory, cited migration brief"]
  EDIT["Puck / GrapesJS / Editor.js / Frappe surfaces"] --> CMS["Site pages, drafts, compiled app sections"]
  FILE["XLSX / CSV / PDF / image / Markdown / captions"] --> DOC["Books, OCR, Ingest, Emit, retained source records"]
  CREATOR["YouTube / Spotify / Patreon / other accounts"] --> CAND["Profile reference, provider review, optional admitted connector"]
  AGENT["Assistant / Codex / Claude / MCP client"] --> MCP["Project MCP, product tools, resources, prompts, account workspace"]
  MIG --> DEL["Migration plan / separate cutover and verification"]
  CMS --> WEB["WebAddress native HTML + site-specific MCP"]
  CMS --> EX["Standalone HTML / WordPress WXR / Frappe exchange"]
  DOC --> REVIEW["Source-grounded review, revised document, exported deliverable"]
  CAND --> OWN["CreatorChannel, MediaLibrary, membership or external-account owner ONLY if bound"]
  MCP --> AUTH["Exact native owner tools and authorization checks"]
  AUTH --> REVIEW
  AUTH --> CMS
  AUTH --> DEL
~~~

**Why these arrows do not all pass through Matrix:** site addressing resolves *where* a subject appears and for *whom*. It is not the format translator for DOCX, the execution engine for a Svelte app, the WordPress event verifier, the Books calculation engine, a payment processor, or a grant for Spotify. Equally, MCP is not a replacement for live website delivery or native CLI operations: it exposes selected real owners through a protocol with a limited and permission-checked tool/resource surface.

| User asks to… | Actual owner of the transformation | Usable artifact / interaction | What still must be proven |
|---|---|---|---|
| Connect a WordPress site | WordPress bridge, signed plugin, CMS client | verified bridge-control proof; normalized post event; replay-safe native CMS entry | WP runtime integration, publication authority, migration/cutover |
| Move authored site content | `bonfyre site` import/export | HTML file; WordPress WXR 1.2; Frappe documents | actual WP/Frappe import, DNS, content/media parity |
| Modernize an old hosting estate | BonfyreBrief inventory/cited + DB bridge + review | cited inventory/brief, discrepancies, row-value comparison | actual server access, PHP/plugin compatibility, staged cutover |
| Work in Excel | Books/XLSX, Workbook owner, workbook-specific MCP | preserved source cells + review workbook + account draft | workbook formula coverage, approval/posting, UI/remote deployment |
| Work with a paper document | BonfyreOCR on macOS, Ingest, cited review | OCR lines with page/geometry/confidence; text source | recognition accuracy, validation, lawful retention, signed deliverable |
| Emit a readable deliverable | BonfyreEmit/PandocFormatter | standalone HTML, PDF, EPUB3, plain text, RSS; DOCX via formatter | runtime dependencies, typography and output-specific acceptance |
| Build a custom frontend | SurfaceScene / SitePresentation / WebAddress + specific client framework | semantic HTML/HTMX/native C or typed frontend extension | React/Svelte adapter parity and deployable bundle when claimed |
| Operate a workspace through AI | Project MCP + installed Workspace MCP | record list, read, preview, update, revision conflict/readback | account grant and product-specific effect verification |
| Connect YouTube/Patreon/Spotify | profile reference, provider-auth catalog, InteractionGrammar | public link or planned OAuth; declared media scene grammar | actual OAuth token/scope, media/membership read or action |
| Connect Toast POS | *No verified Toast-specific native owner found* | commercial target only | provider admission, POS API, data and effect contract |

## 1. WordPress: a bridge, an export format, and a migration are three different jobs

~~~mermaid
flowchart TB
  WP["Selected WordPress site / plugin"] --> DISC["Public REST route discovery: post, page, media, taxonomy, Woo, ActivityPub"]
  WP --> SIG["HMAC SHA-256 webhook with site, post, version"]
  WP --> CLAIM["Shared-secret bridge-control challenge"]
  SIG --> EV["Normalized idempotent event"]
  EV --> CMS["Native CMS external-ID create/update sync"]
  CLAIM --> HOLD["Bridge control only — not institutional/publish authority"]
  CMS --> LOCAL["Source record, review and optional native generation receipt"]
  OTHER["Native authored Bonfÿre site"] --> EXPORT["bonfyre site export"]
  EXPORT --> WXR["WordPress WXR 1.2 XML"]
  EXPORT --> FRAPPE["Frappe documents"]
  EXPORT --> HTML["Offline standalone HTML with CSS inlined"]
  WXR --> TEST["Re-import to Bonfÿre and compare exact text"]
~~~

- **Read a remote post:** `createWordPressBridge().fetchPost(id)` calls `/wp-json/wp/v2/posts/{id}` with bounded LRU/TTL cache and request coalescing.
- **Inspect before assuming:** `discoverPublicCapabilities()` examines the *advertised public REST routes*. It does not silently assign WooCommerce, ActivityPub or editor/write scope.
- **Ingest changes safely:** the plugin and `normalizeWebhook` use HMAC verification, remote site/post/version identity and an idempotency key. `toNativeCmsEntry` preserves site/post IDs as the external identity. `createNativeCmsClient` upserts by that identity, not by matching a title or copying a public page.
- **Keep model output separate:** the native-generation client can project a hashed `bonfyre.native.generation.result.v1` receipt into `cms_generation`. That receipt is not permission to post to WordPress.
- **Export a different system's own interchange:** `bonfyre site export notes --format wordpress --out notes.xml` writes WordPress WXR; `--format frappe` writes Frappe documents; default HTML includes local CSS but no scripts. The [native site-product test](../cmd/BonfyreWebAddress/tests/site_product_test.sh) reimports WXR/Frappe locally and checks text, including angle brackets/quotes. **The test does not prove a real WordPress or Frappe server accepted the export.**
- **Real migration has more stages:** inventory accounts, PHP versions, WordPress/plugin compatibility, forms, media, database, mail, redirects, host ownership, TLS, client approvals, staging, and rollback. No successful export is proof of those stages.

Sources: [WordPress bridge and contract](../integrations/wordpress/README.md), [bridge implementation](../integrations/wordpress/bridge.mjs), [plugin](../integrations/wordpress/plugin/aurekai-bridge.php), [site exporter](../cmd/BonfyreWebAddress/src/site_product.c), [hosting inventory](../cmd/BonfyreBrief/src/inventory.c), [database value bridge](../cmd/BonfyreBridge/src/main.c).

**Current boundary:** the registry reports plugin/bridge static tests, *not* a verified WordPress runtime installation. The plugin's site-claim proof establishes installed bridge control, not ownership of the publishing institution.

## 2. Hosting and old PHP estates: source inventory → cited decisions → migration checks

The source contains a concrete importer rather than a generic “modernize websites” offer.

~~~mermaid
flowchart LR
  OWNER["Customer-intake answers + express grant"] --> BRIEF["BonfyreBrief inventory"]
  PANEL["Customer-supplied exports: cPanel / InterWorx / Plesk"] --> BRIEF
  BRIEF --> DIFF["Domains, sites, mailboxes, databases; statement/export discrepancies"]
  DIFF --> REPORT["Cited migration brief; unknowns and refusal"]
  REPORT --> STAGE["Proposed backups, staging, PHP/DB/WordPress checks"]
  STAGE --> DB["BonfyreBridge: row/value/type comparison"]
  DB --> HAND["Customer-authorized cutover and live readback"]
~~~

`bonfyre-brief inventory` parses **read-only exported listings** from cPanel WHM/API2, InterWorx NodeWorx/SiteWorx and Plesk XML API. It requires an express grant to use the panel exports, normalizes domains/mailboxes, counts secret-bearing fields without copying their values, distinguishes account limits from real counts, and records discrepancies between owner statements and exports. It **does not log into the panel** or infer the account remains live.

`BonfyreBridge` compares selected database rows after transfer and reports type differences; hash aggregation is *not* a transactional or cryptographic proof. An old PHP/WordPress site requires separate runtime testing. Sources: [inventory implementation](../cmd/BonfyreBrief/src/inventory.c), [inventory test](../cmd/BonfyreBrief/tests/hosting_inventory_test.sh), [database bridge](../cmd/BonfyreBridge/src/main.c), [bridge tests](../cmd/BonfyreBridge/tests/db_bridge_test.sh).

## 3. React and Svelte: rendering targets must not be confused with proven converters

**React is used in a real way**: Puck's editor mount creates a React root using `ReactDOM.createRoot` inside the common authoring workspace. GrapesJS supplies another frontend interaction mode. The retained authoring tree, CMS and C-rendered public result remain separate from that React runtime.

**Svelte is not yet a proven full import/export lane.** Repository search found a Threlte/Svelte grid *reference*, but no live native Svelte compiler/converter, generated SvelteKit app, or Svelte/React one-to-one migration test. The architecture must not advertise a universal framework translator.

For a real React/Svelte/legacy PHP request, the needed chain is **source tree and license → component/state/route extraction → source owner and action mapping → responsive/interactions acceptance → generated target/runtime → test build → browser parity → release**. JSX, React hooks, Svelte stores/actions, PHP sessions, CSS selectors, JS DOM side effects and authenticated server logic cannot be “converted” by wrapping them in a typed card. The existing [authoring frontend contract](authoring-frontend-architecture.md) demonstrates some of the independent steps (typed editing, native C rendering and browser tests), not a complete cross-framework compiler.

This limitation is part of the product quality standard: custom code must not silently become inert HTML or a disconnected island.

## 4. Office, Excel and paper: source cell and page provenance survive the operation

~~~mermaid
flowchart TB
  BANK["Bank/source XLSX: sheet + cell + formula/cache"] --> XLSX["BonfyreBooks native bounded ZIP / XLSX"]
  XLSX --> CLASS["On-device categorization + unresolved review lines"]
  CLASS --> OUT["New review XLSX, original bytes/hash retained"]
  OUT --> W["FinancialWorkbook owner: assumptions, recalculation, revisions"]
  W --> MCP["workbook_review / preview / update through account MCP"]
  PAPER["Printed page / scan / PDF"] --> OCR["BonfyreOCR: Apple Vision + page locations + confidence"]
  OCR --> READ["Human-verifiable extracted text and source link"]
  MARKDOWN["Authored Markdown or brief"] --> EMIT["BonfyreEmit / PandocFormatter"]
  EMIT --> PDF["PDF / HTML / EPUB3 / TXT / RSS"]
  EMIT --> DOCX["DOCX via PandocFormatter convert"]
~~~

**Excel is not a CSV masquerading as a workbook.** `books_xlsx.c` reads real XLSX ZIP/XML within bounds (8 MiB compressed file; 32 MiB expanded, up to 5,000 rows/128 columns), refuses macros, unsafe archives, ambiguous mapped columns and unsupported formula-derived values, and retains actual sheet/cell coordinates and source SHA-256. `bonfyre books inspect --workbook bank.xlsx` and `bonfyre books review --report report.json --out review.xlsx` are concrete CLI entry points. Its review output is a new workbook, *not* an assertion that its content has been posted to an accounting ledger. [XLSX C owner](../cmd/BonfyreDecide/src/books_xlsx.c), [Books entry/usage](../cmd/BonfyreDecide/src/usage.h).

**A signed-in reviewer and an assistant can inspect the same financial draft**: `workbook_review` reads a bounded slice, recalculates supported formulas and exposes source positions; `workbook_preview` checks edits against revision and digest; `workbook_update` saves through the account owner with an exact preview fingerprint. Original file references survive. The generic `workspace_update` intentionally does *not* substitute for product-specific financial calculations. [Workbook MCP](../integrations/mcp/workbook_mcp.mjs), [Workspace MCP](../integrations/mcp/workspace_mcp.mjs).

**Paper is not born digitally structured.** `bonfyre-ocr lines input.pdf` runs macOS Vision/PDFKit, returning page number, normalized x/y/width/height, recognizer confidence and line text; `bonfyre-ocr text input.pdf` returns ordered lines separated by page. It uses the machine's installed recognizer, not hosted inference; OCR confidence is not proof. Physical originals, scans, text-bearing PDFs and DOCX should remain distinguishable. [OCR implementation](../cmd/BonfyreOCR/src/main.swift).

**Document delivery is a format-specific operation.** [BonfyreEmit](../cmd/BonfyreEmit/src/main.c) drives Pandoc to HTML5, PDF (wkhtmltopdf fallback weasyprint), EPUB3 and plain text and generates RSS in process; dependency availability matters. [PandocFormatter](../components/PandocFormatter/README.md) separately documents a generic `convert transcript.md --to docx` path. There is no demonstrated universal round-trip preserving all Word/Excel/PDF layout, formulas, OCR geometry, tracked changes and signatures.

## 5. MCP is not one assistant wrapper: project, source, service, and installed-workspace paths

~~~mermaid
flowchart TB
  CLIENT["ChatGPT / Codex / Claude / native MCP client"] --> ENTRY["Project MCP, authenticated Bun endpoint"]
  ENTRY --> TOOL["Curated product toolset or exact tool schema"]
  ENTRY --> RES["Resources: atlas, native inventory, model status, service profiles"]
  ENTRY --> PROMPT["Native-delivery / evidence intake / addressed-participation prompts"]
  TOOL --> RO["Read-only bundles: up to 8, schema checked, coalesced"]
  TOOL --> WR["Effects: owner-specific guarded call"]
  WR --> EDIT["Project patch preview, apply, tests, receipts"]
  WR --> SPACE["Workspace list, read, preview, update"]
  SPACE --> RECORD["Same account-owned saved record, not shadow copy"]
  ENTRY --> SITE["Another MCP: site-specific WebAddress path/host"]
~~~

**Project MCP** in [`project_node.mjs`](../integrations/mcp/project_node.mjs) has its own authenticated transport, source identity checks, rich tool/resource/prompt enumeration, owner routing, compact product toolsets and service-profile listings. Its `mcp_read_bundle` only admits already-listed, read-only tools, validates each schema, limits concurrency to eight and coalesces duplicate in-flight reads. It cannot use that path to smuggle an effectful call.

**Account workspace MCP** in [`workspace_mcp.mjs`](../integrations/mcp/workspace_mcp.mjs) is a different product: `workspace_list` finds installed product editors within the current account destination; `workspace_records` reads revisions and hashes; `workspace_views` deep-links the same saved record; `workspace_preview` checks scalar edits; `workspace_update` must recheck exact grants/revisions and reconcile uncertain writes. The native product/record owner stays authoritative. The registry's contract test covers **eleven installed workspace packages across ten families**, including Catalogue/Room Planning reading the *same destination-owned record*, not duplicating its data.

**Workbook MCP** is a specialized layer on top, not a generic patch disguised as financial verification. **Site MCP** lives at a selected site's own route/host and is tested separately by [`site_product_test.sh`](../cmd/BonfyreWebAddress/tests/site_product_test.sh); it is not the global project operator.

**Recursive MCP**, native tool inventory and process orchestration remain separate mechanisms: [recursive overlay](../integrations/mcp/recursive_overlay.mjs), [bounded pipe](../integrations/mcp/bounded_pipe.mjs), [native project transport](../integrations/mcp/project_node.mjs), [signed-in workspace tools](../integrations/mcp/workspace_mcp.mjs). Its registry reports preserved resources/prompts/trace and auth gates, while the deployed edge was **not certified** by the same tests.

**What the innovation is here:** an assistant can participate in the *same real revisioned work* the person sees, across different product editors, without duplicating canonical records or granting its own authority. Typed, narrow toolsets and read bundles also avoid presenting an undifferentiated thousands-of-tools wall. Model-visible schema-byte reduction is measurable; it is not automatically a measured inference cost reduction or higher model correctness.

## 6. YouTube, Patreon, Spotify, and Toast: four different stages of integration

| Service | Actual work present in source | What is *not* verified |
|---|---|---|
| **YouTube** | Public channel URLs recognized as `external_account_link`; OAuth/API-key methods declared in provider-auth selection; native `youtube-creator-channel` interaction grammar; `BonfyreRepurpose` creates descriptions and declines invented chapters | A verified signed-in YouTube Data API token, uploaded video, working creator-account mutation, complete chapter timing |
| **Spotify** | PKCE/confidential OAuth options declared, with separate app-only token candidate; 20-grammar native catalog contains `spotify-audio-habitat`; CreatorChannel/MediaLibrary binding rules in InteractionGrammar | User playback authorization, full playlist read/write, DRM/media rights, active Listen scene or every provider effect |
| **Patreon** | Public creator-link recognition and an explicit confidential OAuth proposal; webhook branch declared unavailable until exact client/scopes/delivery; source/participation contract covers public carrier types | A functioning signed-in membership sync, verified tier/charge, webhook processing, paywall, settlement |
| **Toast** | Listed as a commercial POS candidate in the white-label product notes | No Toast API integration, credentials, imported orders/menu, webhook, checkout, payments or POS effects established |

**Provider enrollment is not provider activation.** The account enrollment system has 200 target services × two identity paths with queue and credential-stage tests; status, verified fields, consent and scopes determine whether anything can run. [Provider auth selection](../integrations/browser/provider-auth-capability.mjs), [external account recognition](../integrations/browser/account-choices.mjs), [InteractionGrammar C](../cmd/BonfyreInteractionGrammar/src/main.c), [provider estate](../cmd/BonfyreProviderEstate/src/main.c).

**Interaction grammar is not a donor product clone.** Native [`BonfyreInteractionGrammar`](../cmd/BonfyreInteractionGrammar/src/main.c) declares **20 studied patterns**—YouTube channel, Spotify library, Discord community, GitHub code review, Figma design review, Shopify commerce, Slack/Teams work, and others. Each grammar names an exact owner family, verifier and intended native lowerer. It does not carry donor messages, subscribers, money, catalog, playback or private access; missing lowerers are explicitly pending.

A creator may have a public YouTube link, an authored Aurekai channel, an imported transcript, a podcast RSS feed and a separate Patreon membership agreement. Those are **five different records**, not “one social capability.”

## 7. The native experience must be able to leave Aurekai

Three kinds of portability must be kept separate.

1. **Portable documents/media:** WordPress WXR, Frappe documents, XLSX review workbook, PDF, EPUB, DOCX via PandocFormatter, VTT text, Markdown, HTML, RSS and records are governed by format-specific source/output contracts. Some are readers, some writers; very few are complete bidirectional conversions.
2. **Portable site frontends:** The standalone site HTML export inlines style and removes scripts for an offline snapshot. That is not a working JS application. Svelte/React target code generation needs its own component/state/form/interaction tests. Current typed authoring/C/HTMX work solves *some* necessary contracts but not all framework semantics.
3. **Portable account workflows:** MCP resources/prompts/tools, native workspace records, source IDs, Git/Forgejo/Josh checkout, CMS revisions and explicit owner APIs let a new interface continue work without copying state. The account's actual permission and downstream service are still required.

~~~mermaid
flowchart LR
  SRC["Exact source bytes, identities, editorial structures and records"] --> X["Selected format/product adapter"]
  X --> OUT["Typed deliverable: WXR, XLSX, PDF, HTML, workspace record, MCP result"]
  OUT --> VERIFY["Format parse / semantic fields / account scope / browser output"]
  VERIFY --> PUBLISH["Explicit destination acceptance / staged activation"]
  VERIFY --> FAIL["Gap/exception if semantics are missing"]
~~~

The important distinction is *round-trip evidence versus advertised output*. For WordPress/Frappe, local export/re-import text parity is tested; real destination acceptance is not. For XLSX, an original-cell and review-source comparison is tested; complete Excel formula semantics and accounting posting are not. For PDF, writing a file does not preserve semantic reading order, signatures or correspondence with paper. A public provider channel name does not create its API installation.

## 8. How to document a real workstream—required template for all READMEs

Every named platform, file format, application or protocol claim should contain:

| Field | What a useful README says |
|---|---|
| **Operation** | The *actual verb*, e.g. `site export --format wordpress`, `workbook_preview`, `fetchPost`, `bonfyre-ocr lines` |
| **Input** | Exact source and type: signed webhook, exported WHM file, WordPress REST route, XLSX bytes, local PDF scan, account revision |
| **Canonical owner** | The real product/domain responsible; not a universal “Matrix/capability” bucket |
| **Transformation** | Which exact fields, IDs, formulas, layout, source citations and authorization posture survive or change |
| **Output and consuming app** | WXR/XML, XLSX, native CMS draft, PDF, rendered site, MCP tool response, account editor deep link |
| **Test and acceptance** | Named source test and what it checks; distinguish fixture, local, published and actual remote acceptance |
| **Refusal / what it does not prove** | No provider OAuth, no external post, no PDF fidelity, no migration cutover, no payment, etc., unless witnessed |
| **Commercial and noncommercial paths** | Show personal, group, educational, civic/creative, organizational and client uses where the operation applies |

**Do not replace all of these with the sentence “flows through the capability matrix.”** An architectural diagram should show the interfaces, formats, state owners, edits, transports, rights and *actual resulting work*. New features should change the native source and tests first, then update this index.

## 9. Current source-by-source acceptance ledger

| Path | Evidence currently observed | Release classification |
|---|---|---|
| WordPress signed bridge and CMS upsert | `integrations/wordpress/bridge.test.mjs`; plugin static tests | Implemented locally, WordPress runtime not proven |
| Offline HTML / WordPress WXR / Frappe interchange | `cmd/BonfyreWebAddress/tests/site_product_test.sh` | Local parse/re-import parity; external imports unproven |
| cPanel / InterWorx / Plesk read-only inventory | `cmd/BonfyreBrief/tests/hosting_inventory_test.sh` | Exported files only, not server migrations |
| SQLite/database row comparison | `cmd/BonfyreBridge/tests/db_bridge_test.sh` | Comparison, not transactional move guarantee |
| XLSX read/review and native calculations | `cmd/BonfyreDecide/tests/parity.sh`, workbook-specific tests | Partial formula/format envelope, live accounts separately checked |
| Scanned PDF/image OCR | `cmd/BonfyreOCR/tests/ocr_test.sh` | Local machine OCR, no accuracy guarantee |
| Markdown/PDF/EPUB/DOCX outputs | BonfyreEmit and PandocFormatter source | Dependency/format-specific verification |
| Project and recursive MCP | `integrations/mcp/tests/project_node.test.mjs`, `recursive_overlay.test.mjs` | Source fixture tests; live edge requires release receipt |
| Shared installed workspace with revisioned edits | `scripts/tests/installed-workspace-contract.test.mjs` | Owner tests, not proof every browser app is live |
| YouTube/Spotify native interaction grammar | `cmd/BonfyreInteractionGrammar/tests/interaction_grammar_test.sh` | Grammar binding/test, not provider API or playback |
| Patreon | Browser/public-link and provider-auth declarations | Credential/remote effect unverified |
| Toast | Commercial candidate in docs | Not implemented |
| React/Puck visual editor | Local SurfaceScene authoring and browser fixtures | Local source newer than public GitHub; public code sync unresolved |
| Svelte | Threlte reference in vendor only | No verified conversion/runtime integration |

*The source state referenced here is the live local Bonfÿre project inspected on 2026-11-13. GitHub `main` may lag local editor sources. Only link to actual publicly existing files; describe local-only source as local-only. No line here implies the public domains were deployed by editing these READMEs.*
