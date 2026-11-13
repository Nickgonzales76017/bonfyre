# Aurekai native authoring, frontend and experience architecture

> **This is a platform capability, not a website-builder integration.** The editor work is the meeting point of source identity, presentation grammar, institutional records, rich content, spatial/media/interaction programs, native computation, and authorized publication. Puck, GrapesJS, Editor.js, MD4C and Mustach are valuable parts of the path; **none is the canonical document owner**.
>
> **Source/release split (2026-11-13):** This guide is committed to public GitHub `main`, but several of the exact authoring C/JS files and tests inspected through the current **local Bonfÿre project** are **not yet on GitHub `main`**. Those files are identified by inline source paths rather than dead GitHub links. Only links verified on public `main` are clickable. Local implementation/test evidence does **not** certify public source sync, production activation, or a live-site behavior.

> **Evidence basis:** Bonfÿre architecture-registry entries `cmd-surface-scene`, `site-presentation-composition`, `surface-ir`, `surface-renderer`, `scene-compiler`, `presentation-template`, `web-lowering`, `design-fingerprint`, `site-fragment-migration` and `native-interaction-components`, plus the exact referenced source/tests (inspected 2026-11-13). `implemented` is not synonymous with live release. MD4C is the library actually included in C; the earlier "M4D" shorthand in conversation is not a separate verified implementation.

[Whole-system architecture](architecture-visuals/system-architecture.md) · [Main README](../README.md) · [Architecture registry](../architecture/README.md) · [Native SurfaceScene](../cmd/BonfyreSurfaceScene/) · [CMS](../cmd/BonfyreCMS/) · [WebAddress](../cmd/BonfyreWebAddress/)

## 1. The idea: one subject, many *real* ways to experience and change it

~~~mermaid
flowchart TB
  S["Subject / article / group / institution / physical place / application"]
  S --> O["Canonical identity, owners, sources, relationships and audience"]
  O --> C["Capabilities and authorized action apertures"]
  O --> D["Authoring tree: typed nested components + editorial + responsive roles"]
  C --> D
  D --> R["SurfaceIR + native Scene + PageComposition"]
  R --> P["Study-derived visual grammar + design tokens + interaction patterns"]
  P --> L["C native rendering / WebAddress + selected browser enhancements"]
  L --> A["Person, group, journal, account app, public site or machine view"]
  A --> V["Review / version / authority / effect / release readback"]
  V -. "Approved changes only" .-> O
~~~

A person is not a template. A publication is not a post card. A CRM Lead is not a demonstration of a form. A place is not a furniture layout. One semantic owner may appear as an editorial story, a comparative research interface, a private form, a public collection, a map, a program, or an audio edition *without discarding what the owner means*. The reusable part is not a copied page; it is a **typed projection with a declared source, purpose, permission and renderer**.

The system must keep these things independent:

| Independent plane | Owner and purpose | What it must never imply |
|---|---|---|
| **Source / semantic record** | CMS, Frappe, ForeignTwin, community, work, spatial, media or other domain owner | A visually appealing scene grants rights or canonicalizes a foreign record |
| **Authoring document** | Shared nested tree, stable coordinates, controlled responsive/style roles | An editor can mint a different source identity or write to another account |
| **Presentation and interaction** | SurfaceIR, SurfaceScene, PageComposition, SceneCompiler, templates, UI vocabulary | A card, map or tab entails data that the source does not hold |
| **Address and audience** | Matrix / WebAddress / InformationPolicy | A route that resolves is publicly readable or editable |
| **Actions and publication** | Canonical CMS revisions, declared effect endpoints, separate release process | Preview, local success, a source import or a saved draft is live |

## 2. Puck + GrapesJS + Editor.js — a shared structure, not three islands

~~~mermaid
flowchart TB
  USER["One selected account / site / page"]
  USER --> TREE["bonfyre.authoring-tree.v1"]
  PUCK["Puck: component / structural canvas"] <--> ADAPT["Typed adapters and coordinate normalization"]
  GRAPES["GrapesJS: visual layout, nested composition"] <--> ADAPT
  EDITOR["Editor.js: eight typed editorial tools"] --> ED["Retained editorial blocks"]
  ADAPT <--> TREE
  ED --> TREE
  TREE --> VALID["Tree validation + source continuity + responsive roles"]
  VALID --> OPS["Insert / move / remove / update / style / duplicate / undo / redo"]
  OPS --> DIGEST["Exact-source revision digest and proposed edit gate"]
  DIGEST --> CMS["BonfyreCMS site_page + site_fragment aggregate"]
  CMS --> READ["Reopen by exact namespace / route / revision"]
  READ --> TREE
~~~

**Grounded implementation:**

- Shared tree (`cmd/BonfyreSurfaceScene/runtime/authoring-tree.mjs`) records `site_id`, `page`, explicit child hierarchy, stable IDs, typed fields, source/binding metadata, responsive styles, and per-editor round trips. Puck and GrapesJS document adapters are symmetrical for supported typed content.
- Existing editing operations include insert, remove, move with cycle refusal, update, device-specific style, duplicate, bounded undo/redo, and digest-checked proposals. A proposal cannot silently retarget the selected site, fabricate a source reference, replace capability bindings, or remove source-bound nodes.
- Editor.js editorial material (`cmd/BonfyreSurfaceScene/src/authoring_editorial.c`) has eight known block types: paragraph, header, quote, list, code, delimiter, checklist and table. The native renderer admits only compiled, reviewed templates; arbitrary editor HTML does not become a public page.
- Native CMS authoring bridge (`components/BonfyreServiceSite/authoring/native-cms.mjs`) performs namespace- and revision-checked writes, keeps unrelated fragments, refuses silent edits to published content, and **re-reads after saving** instead of treating an HTTP success as proof of persistence.

The center of gravity is the *document contract*, not the browser library. Editors can evolve or be replaced without making a page's canonical content depend on a proprietary component tree. A typed application field retains its source and authorization even when a designer rearranges its presentation.

## 3. Not a seven-block editor: full section and application composition

~~~mermaid
flowchart LR
  A["Blank canvas / shared tree"] --> V["Typed component vocabulary"]
  F["27 ServiceSite section owners"] --> V
  UI["Native interactive UI patterns"] --> V
  ERP["Frappe phase-12 compiled surface program"] --> V
  SCENE["Native subject Scene + slots and fragments"] --> V
  V --> DOC["Nested authoring document"]
  DOC --> EDIT["Puck / GrapesJS / editorial tools"]
  EDIT --> CMS["CMS fragments, preserved bindings"]
  CMS --> C["C native component and editorial renderer"]
  C --> B["Browser, account and public site"]
~~~

The earlier seven-element proof-of-concept lives in authoring-prototype.mjs (`cmd/BonfyreSurfaceScene/runtime/authoring-prototype.mjs`); it is **not** the current acceptance target. The richer implementation retains the 27 canonical ServiceSite section types (`components/BonfyreServiceSite/authoring/site-section-catalogue.generated.mjs`) from their real owners. Section parity tests (`cmd/BonfyreSurfaceScene/tests/authoring-section-parity.test.mjs`) verify their nested domain payloads, required fields, source references, responsive data and CMS readback through both editor projections.

The blank-canvas test (`cmd/BonfyreSurfaceScene/tests/blank-canvas-authoring.test.mjs`) verifies a first-party component palette available to both editors, nested authoring from an empty page, native CMS-compatible output, and actual interaction semantics. The native component registry originally covered 43 compiled templates; a later registry witness reports 46 component kinds, including fields and choices. These are **component contracts**—not 46 fully launched customer applications.

The real CRM Lead integration test (`cmd/BonfyreSurfaceScene/tests/experience_authoring_native_test.mjs`) compiles an actual Frappe CRM source program, retains its tabs and more than 25 declared fields, produces one authored tree, round-trips it through Puck and GrapesJS, and writes/reopens its native fragments. **No Frappe controller or foreign record is executed by that test.** This matters: the same editor is capable of composing an *application grammar* rather than just a marketing page.

## 4. MD4C, Mustach, C renderer and browser-native interactions

~~~mermaid
flowchart TB
  DOC["CMS-authored typed document"]
  DOC --> MARK["MD4C: GitHub-flavored Markdown, raw HTML off"]
  DOC --> ED["Eight Editor.js type templates"]
  DOC --> COMP["Native component catalogue / typed data"]
  MARK & ED & COMP --> M["Mustach / C output templates"]
  M --> SAFE["Safe link, media, property, size, source and form constraints"]
  SAFE --> HTML["Native initial HTML in WebAddress"]
  HTML --> B["Browser: semantic HTML/CSS first"]
  B --> HX["HTMX fragments where owner-scoped refresh is needed"]
  B --> JS["Small scoped interaction runtime for tabs, carousel, dialog, menus"]
  HTML --> MACHINE["Machine carrier from the same owner, distinct permitted disclosure"]
~~~

This is not a new handwritten Markdown parser. Native Markdown (`cmd/BonfyreSurfaceScene/src/authoring_markdown.c`) uses vendored MD4C (`cmd/BonfyreSurfaceScene/vendor/md4c/`; local-only source), GitHub-flavored syntax and `MD_FLAG_NOHTML`, then reviews generated destinations and rejects unapproved embedded images. Mustach templates (`cmd/BonfyreSurfaceScene/vendor/mustach/`; local-only source) form the fixed rendering vocabulary. The C component implementation (`cmd/BonfyreSurfaceScene/src/authoring_native_components.c`) admits typed content, limits output, rejects executable markup and unsafe form destinations, and does not consume arbitrary GrapesJS/Puck HTML, scripts or CSS.

The browser parity test (`cmd/BonfyreSurfaceScene/tests/native_browser_authoring_parity.test.mjs`) creates from a **blank page**, writes fixture CMS rows, compiles the actual C projection, and checks browser interaction in two account namespaces. It exercises responsive layouts, ARIA tabs, carousel, dialogs and review-required actions. **That is a controlled Chromium fixture, not evidence that every published domain already renders every component.**

The broader [WebLowering contract](../cmd/BonfyreWebAddress/src/main.c) asks what HTML, CSS and the browser can already do before loading more script. Native initial HTML, owner-scoped fragments, standards-based behavior and optional browser interaction can coexist. A native C backbone is not an argument for removing all client-side interaction.

## 5. Frontend maturity: tokens, density, accessibility, rhythm, icons and interaction

~~~mermaid
flowchart TB
  CONTENT["Actual content volume, kind, relationships, actions"] --> GRAM["SceneCompiler: lead, feed, index, table, rail, media, map"]
  GRAM --> REG["Semantic regions and reading order"]
  DESIGN["Reviewed design fingerprints / native template"] --> TOK["Closed presentation roles: color, typography, mode, width, spacing"]
  TOK --> REG
  DEV["Desktop / tablet / phone responsive roles"] --> REG
  GLYPH["Licensed compiled icon vocabulary"] --> REG
  REG --> HTML["Native SurfaceIR / PageComposition output"]
  HTML --> UX["Keyboard + screen-reader + interaction + dense readable information"]
  UX --> CHECK["Browser + accessibility + real device / route readback"]
~~~

**Visual styling is not just swapping themes.** [PresentationTemplate](../cmd/BonfyreCMS/content-types/presentation_template.json) and [site presentation](../cmd/BonfyreWebAddress/src/site_presentation.c) lower closed tokens so a client changes the expression of a page without writing arbitrary CSS into the owner's application. Responsive tree roles (`cmd/BonfyreSurfaceScene/runtime/authoring-tree.mjs`) support desktop/tablet/phone with layout modes such as stack, grid, columns, split, carousel, masonry and full-bleed. Component metadata, source identity and audience rules survive presentation changes.

[SceneCompiler](../schemas/control/page-composition.cue) composes reading rhythms from available material, omitting unsupported empty regions. That is different from a fixed gallery of generic placeholder cards. The [native glyph vocabulary](../cmd/BonfyreWebAddress/src/glyph_set.tsv) carries explicit identity/license checks instead of fetching arbitrary icon assets at runtime, although browser-module glyph convergence remains incomplete. Shared [interaction components](../cmd/BonfyreSurfaceScene/runtime/renderers/) include edited-history, review and record-grid behavior with stable selection, keyboard/focus and permission checks.

### Design references are evidence, not copied runtime

The [design fingerprint work](../cmd/BonfyreCMS/content-types/design_fingerprint.json) and [Ghost reference compiler](../cmd/BonfyreCMS/tools/ghost_marketplace_templates.mjs) maintain **238 candidate visual vectors**. They are *not* 238 fully analyzed site themes: masthead, hero, section rhythm, mobile collapse, motion, media geometry and typed semantic-slot acceptance need individual measurement. The [Stitch grammar](../cmd/BonfyreSurfaceScene/runtime/stitch-grammar.mjs) maps **91 retained design references to 14 reusable experience patterns**; [its test](../cmd/BonfyreSurfaceScene/tests/stitch_composition_test.mjs) verifies that applying a pattern preserves the underlying Scene's owners and bindings. These are two different inputs: presentation evidence and native interaction grammar.

The frontend upgrade should increase actual information density and affordance quality—search, navigation, readable rich media, legitimate actions, group/connection tools, responsive continuity—rather than adding collapsible demo islands or progressively more decorative widgets.

## 6. A compiled experience works across noncommercial and commercial life

~~~mermaid
flowchart TB
  CORE["One authoring + native experience compiler"]
  CORE --> PEOPLE["Personal profile / life journal / portfolio"]
  CORE --> COMMUNITY["Group, club, community, discussion and participation"]
  CORE --> PRESS["Publisher / investigative article / research library"]
  CORE --> CIVIC["Public records / historical and safe aggregate maps"]
  CORE --> LEARN["Learning / community knowledge / guides"]
  CORE --> MEDIA["Recording / listening room / creator channel"]
  CORE --> SCI["Scientific / technical evidence and spatial exploration"]
  CORE --> WORK["Professional application / CRM / workflow"]
  CORE --> SHOP["Sourcing / catalogue / procurement / commerce"]
  PEOPLE & COMMUNITY & PRESS & CIVIC & LEARN & MEDIA & SCI & WORK & SHOP --> RIGHTS["Same source, rights, institution, revision and release disciplines"]
~~~

For a **group**, authoring can compose a profile, membership-aware discussion, event, information page and relevant map without duplicating the community owner. For a **research/public-interest journal**, the same tooling can combine prose, citations, data, timeline, source comparison, interactive table, map and an optional audio edition; safety and rights remain independent. For a **student or maker**, a portfolio can become an actual learning resource, lab notebook and live project surface. For an **organization**, a real DocType can become a usable account-native form or review workflow. **The innovation is in retaining the semantic continuity as the presentation and audience change**, not in promising that every one of these workflows has shipped.

## 7. What is innovative — and what still needs proof

| Architectural proposition | Why it is useful | Evidence vs current limitation |
|---|---|---|
| **Several full editing modes, one retained grammar** | Content and bindings survive moving among structural, visual and editorial authoring | Puck/GrapesJS round-trip and nested CMS tests; production editor completeness still requires route-by-route verification |
| **Native render + typed action seams** | Browser interaction is an enhancement over meaningful owner-built HTML, not the entire application database | C rendering + Chromium fixtures; not every legacy donor route has migrated |
| **Application semantics inside an editor** | A Frappe form, community component or source-bound tool can be composed without being converted to empty HTML | Real compiled CRM Lead test; controller/write-back not implied |
| **Reversible design and material separation** | Brand, content density and reading rhythm can change while canonical identities stay stable | Template/Scene tests; 238 Ghost candidates not fully observed or approved |
| **Authored views beyond marketing** | The same visual/interaction language supports public-interest, personal, cultural, academic and operational contexts | Source contracts and tested pattern composition; domain journeys vary in maturity |
| **Safe computational editing** | A model can propose typed operations against an exact revision without receiving publication or source-forging powers | `applyAuthoringProposal` digest and binding gates; no blanket autonomous publication |
| **Real cross-device, cross-audience integrity** | A phone view, public profile and private account view can refer to one underlying subject | Responsive roles, Matrix and account-scoped tests; deployed site parity remains an explicit acceptance |
| **Source-preserving migration** | GitHub Pages, ServiceSite and legacy copied sections can be staged as CMS drafts without pretending a copied screenshot is an application | [fragment migration tests](../cmd/BonfyreSurface/tests/fragment_migration_native.test.mjs); exact slot mapping and public release are separate |

**The libraries themselves are not novel inventions.** GrapesJS, Puck, Editor.js, MD4C, Mustach, HTMX and Markdown exist independently. The distinctive Bonfÿre/Aurekai approach, insofar as its cross-owner implementation is verified, is **one compiled experience model** that can retain institution, document, source, action and visual identity while lowering across editors, C-native web output, account apps, creator surfaces, and machine-readable views. Claims about performance superiority, full 90-journal live parity or complete donor-site fidelity require measurements and release readback.

## 8. Acceptance: a real editor and a real experience, not another demo

1. **Create, don't import.** Start with a blank account-owned document; author nested content, interactions and responsive variants through the installed editor. Puck/GrapesJS/Editor.js must not secretly create three competing documents.
2. **Retain the original meaning.** Reopen after CMS save and compare exact child structure, section payloads, binding IDs, read-only source identity, citation/media provenance, and revision digest.
3. **Render the same semantic scene.** Native C/WebAddress and browser interactions must show the designed density, keyboard semantics, responsive behavior, usable navigation and real material; no empty filler, synthetic source or disconnected demos.
4. **Respect changes and effects.** Every mutation goes through its owning application's validation and explicit account authority; review-held drafts remain findable, but cannot become public or overwrite existing published material through a preview.
5. **Verify publication independently.** Inspect source, tests, approved release and live target for exact domain, route, site owner, article count, media player, responsive layout and public/private audience.
6. **Broaden beyond a single customer's website.** Exercise a group/community, profile/journal, document/knowledge page, data/spatial experience and a compiled application grammar against the same editor/renderer contracts.

In particular, the **90-journal parity target and rich one-to-one personal site reproduction** are acceptance goals, not proofs established by this architecture document. Tests confirm important internal paths, but their existence must not be mistaken for final live readback.

## 9. Source and verification map

| Work | Owner/source | Test/witness |
|---|---|---|
| Shared typed tree and editors | authoring-tree.mjs (`cmd/BonfyreSurfaceScene/runtime/authoring-tree.mjs`), unified editor (`components/BonfyreServiceSite/authoring/unified-editor.mjs`) | authoring-ids (`cmd/BonfyreSurfaceScene/tests/authoring-ids.test.mjs`), blank canvas (`cmd/BonfyreSurfaceScene/tests/blank-canvas-authoring.test.mjs`) |
| Complete domain sections | 27 section catalogue (`components/BonfyreServiceSite/authoring/site-section-catalogue.generated.mjs`) | section parity (`cmd/BonfyreSurfaceScene/tests/authoring-section-parity.test.mjs`) |
| Browser parity and C-native output | native components (`cmd/BonfyreSurfaceScene/src/authoring_native_components.c`) | Chromium test (`cmd/BonfyreSurfaceScene/tests/native_browser_authoring_parity.test.mjs`) |
| Markdown and editorial | MD4C compiler (`cmd/BonfyreSurfaceScene/src/authoring_markdown.c`), editorial C (`cmd/BonfyreSurfaceScene/src/authoring_editorial.c`) | Markdown C test (`cmd/BonfyreSurfaceScene/tests/native_authoring_markdown_test.c`), component C test (`cmd/BonfyreSurfaceScene/tests/native_authoring_components_test.c`) |
| CMS revisions/rights | CMS bridge (`components/BonfyreServiceSite/authoring/native-cms.mjs`) | CMS tests (`cmd/BonfyreSurfaceScene/tests/authoring-cms.test.mjs`) |
| Compiled business experiences | [experience-programs](../cmd/BonfyreSurfaceScene/runtime/experience-programs.mjs) | real Frappe test (`cmd/BonfyreSurfaceScene/tests/experience_authoring_native_test.mjs`) |
| Research-derived visual layouts | [design fingerprints](../cmd/BonfyreCMS/content-types/design_fingerprint.json), [Stitch grammar](../cmd/BonfyreSurfaceScene/runtime/stitch-grammar.mjs) | [Stitch test](../cmd/BonfyreSurfaceScene/tests/stitch_composition_test.mjs) |
| Shared interaction components | [edit history/review/record grid](../cmd/BonfyreSurfaceScene/runtime/renderers/) | [interaction tests](../cmd/BonfyreSurfaceScene/tests/reusable-interactions.test.mjs) |
| Component migration | [fragment staging](../cmd/BonfyreSurface/tools/stage_fragment_migration.mjs) | [draft-only test](../cmd/BonfyreSurface/tests/fragment_migration_native.test.mjs) |

This is a zoom into one fundamental **cross-cutting system family** of Aurekai, not an attempt to replace or shrink the broader registry. See the [whole-system architecture](architecture-visuals/system-architecture.md) for the other families and the boundary of the editor's authority.
