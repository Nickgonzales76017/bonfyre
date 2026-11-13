# Bonfÿre clients and frontends — real surface types and boundaries

A “client surface” is **not** a generic dashboard consuming one central Matrix. Bonfÿre has native web routes, account-owned installed applications, rich composition/editing clients, CLI/MCP tools, media/playback, source and geographic research views, external provider controls, and optional device/graphics clients. They **do different jobs** and should not be described as interchangeable.

[User-visible source/output workflows](real-workflows-and-interoperability.md) · [Unified Puck/GrapesJS/Editor.js and native frontend](authoring-frontend-architecture.md) · [WebAddress and site interchange](../cmd/BonfyreWebAddress/tests/site_product_test.sh) · [MCP operations](../integrations/mcp/README.md)

## What the surface is, what it reads, what it writes

| Experience | Native source/reader | Interaction and write owner | What the surface does not do |
|---|---|---|---|
| **Public profile / journal / publication** | WebAddress, CMS `site_page` + `site_fragment`, SceneCompiler/PageComposition | reviewed CMS authoring, article/content release | not an owner-independent copied GitHub Pages demo |
| **Editor canvas / design / article** | Puck (React canvas), GrapesJS, Editor.js over one retained authoring tree | native CMS revision, owner/site/route checks, publication review | no arbitrary JavaScript/CSS injection or right to change source bindings |
| **Business application** | native Frappe DocType/CRM compiler + CMS/application declarations | respective CRM/ERP/workspace operation | a compiled form is not a running Frappe controller or valid customer mutation |
| **Account workbench** | native destination-bound Workspace record store | read/list/preview/update through owner and exact revision | not a second MCP-private database |
| **Finance review** | Books XLSX with sheet/cell provenance, FinancialWorkbook model | workbook-specific calculation/review and native account save | not financial approval, accounting posting, invoice or settlement |
| **Community/groups** | community body, membership, current presence, posts/reactions and moderation state | authenticated community owner | public presence is not attendance or membership access |
| **Media/listening/station** | CreatorChannel/MediaLibrary, AccountWorldAudio CMS and WebAddress player | owner-specific recordings, schedule, transcript marks, allowed playback | no unverified Spotify playback, YouTube publishing or private audio rights |
| **Spatial/map research** | spatial observations with observed/effective/recorded/expiry time and native queries | original subject/source owner and spatial observation reviewer | not a room-planner-first or all-geography precision guarantee |
| **Document/source research** | retained material, source digests, exact passages; local OCR for paper/PDF | source/intake and human review | a PDF text read or OCR confidence is not source truth |
| **Project source and agents** | Git/Forgejo/Josh + Project MCP state/resources and native command inventory | project patch preview, guarded apply, tests and release | an agent transcript or displayed tool is not source mutation authority |
| **Connected providers** | account enrollment/ProviderEstate/Connection Studio | provider-specific OAuth, credentials, actual API client and receipt | a public profile link or provider candidate is not a live connection |
| **Offline exports** | `bonfyre site export`, Books XLSX, BonfyreEmit/PandocFormatter | format-specific reader/writer and receiving system | HTML snapshot is not a React/Svelte app; DOCX/PDF is not a live site |

### Native browser lowering is a deliberate choice

~~~mermaid
flowchart LR
  OWN["Existing source and app owner"] --> SCENE["Typed SurfaceScene / PageComposition"]
  SCENE --> HTML["C-native server HTML, safe roles and content"]
  HTML --> CSS["Responsive templates / typography / glyphs"]
  HTML --> HTMX["Owner-scoped HTMX fragment when needed"]
  HTML --> JS["Small interaction runtime for dialog, tabs, carousel, grids"]
  HTML --> MACHINE["Machine carrier at permitted audience"]
  JS --> EFFECT["Specific native action with permission recheck"]
  HTMX --> EFFECT
~~~

[`WebLowering`](../cmd/BonfyreWebAddress/src/main.c) asks whether a browser already knows how to do something in HTML/CSS before allocating more JavaScript. The same source may have both human HTML and a machine response, with **audience/permission differences explicitly retained**. Visual components can reuse interactions, keyboard behavior and responsive tokens; their material and actions still belong to the right domain owner.

The shared authoring tree has local source tests for **27 ServiceSite section owners** and nested source-bound content through Puck/GrapesJS/Editor.js, plus C and browser-parity fixtures. Some of those latest editor files are **local-only** and not yet on public GitHub `main`; see [frontend evidence distinctions](authoring-frontend-architecture.md). The 91 Stitch design references form 14 native pattern families, not 91 independently launched apps. The 238 Ghost design fingerprints are candidates, not approved complete site themes.

## External UI is not automatically an external system

A site can use [MCP on its own WebAddress host](../cmd/BonfyreWebAddress/tests/site_product_test.sh). The authenticated [Project MCP](../integrations/mcp/project_node.mjs) and [Workspace MCP](../integrations/mcp/workspace_mcp.mjs) expose different operations, grants and scopes. YouTube/Spotify/Patreon interface grammars are studied **interaction shapes** and must be bound to an actual native owner; they do not import donor content, OAuth or playback.

**Frameworks:** Puck legitimately mounts in React. The retained content model and native C renderer do **not** prove complete React/Next.js or Svelte/SvelteKit source-conversion parity, nor can an arbitrary PHP application's server-side authorization be flattened to frontend components. External runtimes need their own source/state/effect extraction and browser tests.

## Optional graphical, headless and device clients

The historical `clients/raylib` experiment is an *optional graphical adapter*, not the only client supported by Bonfÿre. Terminal-native, browser, HTMX, WebGPU/spatial, offline document and MCP/headless paths each have separate readers and delivery envelopes. A client's animation or cached view cannot become a canonical WorkGraph lease, source fact, payment, publication or authorization.

## Acceptance rules for a new client

- **Read the real owner:** name source/record/route/format, revision and policy instead of saying “fetch capability state.”
- **State the interaction:** e.g. tab selection vs workbook edit vs provider authorization vs CMS publication.
- **Prove the effect boundary:** no renderer, model, client or MCP tool fabricates write rights.
- **Test the returned artifact:** browser responsive/keyboard behavior, WXR/Frappe content parity, XLSX cell lineage, OCR page positions or exact account revision as applicable.
- **Show gaps:** offline snapshots, donor reference libraries, local fixtures and unverified external integrations are labeled as such.

[See the actual source-to-output workflows](real-workflows-and-interoperability.md) before asserting that an experience is finished.
