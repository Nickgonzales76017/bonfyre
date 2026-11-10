# Native documentation and white-label presentation closeout

Owner: existing Bonfÿre Fabric, CMS and WebAddress. Do not introduce a second catalog, state store, documentation API, site server or provider-facing renderer. This work is a refinement of existing source.

## Current working slice

`docs/experiences/` contains business, native and integration guides. WebAddress runs the isolated documentation site at `docs.localhost:8916` through its existing `site new`, `site page` and `site mcp` commands. A separate local estate on port 8917 holds Fernhollow, Tallgrass and Juniper with independent site identities, CMS namespaces, offerings and presentation templates.

The existing `site/matrix/own-site.css` is already an editorial manual layout with sticky sidebar, page search, outline, code tabs, copy controls and responsive reflow. Preserve it as a foundation. The needed improvement is richer application-specific page composition and a reusable brand projection, not a CSS-framework transplant.

The current bridge result is useful: a site exported to WordPress WXR, Frappe Blog Post documents and self-contained HTML; each structured export restored one post into an independent local site. WordPress and Frappe import added three trailing newline characters to the body. The native `bonfyre bridge db` copied `community_post` and `site_presentation` into a separate SQLite database and `db-verify` read back 5/5 rows per table with matching values. No deployment or external database was involved.

## Native design owner — WebAddress

Exact entry points:

- `cmd/BonfyreWebAddress/src/site_product.c`, `site_create`, `site_set`, `site_check`, `site_page`.
- `cmd/BonfyreWebAddress/src/site_mcp.c`, `McpSite`, `site_icon_metadata`, `site_browser_icon`, `manual_page`, `emit_tokens`.
- `site/matrix/own-site.css` and `own-site-reading.js`.
- Existing CMS types `presentation_template`, `site_presentation`, `site_page`, `site_fragment`, `site_program`, `application_binding`, `design_fingerprint`.

### Customer-specific composition

`presentation_template` already records `hero_mode`, `navigation_mode`, `media_mode`, `typography_mode`, `card_mode`, `chrome_mode` and `layout_tokens`. The independent-site loader in `site_mcp.c` currently loads only six color tokens, while `emit_tokens` projects only those colors. The other template fields are stored but not realized by that renderer.

Expand the existing presentation projection to carry those declared attributes into meaningful semantic layout modes. Let the CSS choose distinct editorial, industrial, service and community treatments from named template traits, not from demo site IDs or hard-coded brand colors. Build one common set of responsive primitives with variant typography, hero structure, navigation, media and cards. The local sample should visibly separate three businesses when all accents are forced to the same color.

Do not replace content owners: a site page is still a `site_page` with `site_fragment` sections; the customer-facing view lowers the same records. The CMS and WebAddress changes are the owner change, not a new design CMS.

### Favicons, logos and brand separation

`site_browser_icon` and `site_icon_metadata` currently emit the knight only for `site->reading`; independent sites emit no favicon metadata. For a customer deployment, resolve an optional logo and favicon from its approved presentation/media program. For Aurekai's own docs, project the existing knight. Keep the independent client's own mark off Aurekai asset URLs.

Do not infer ownership from whether a site is a feed-reading view. Use the institutional/site subject and approved presentation profile. Preserve the existing safe fallback for websites without supplied assets.

`site_check` currently fails `own-title` when the title contains the word “Aurekai” (`site_product.c` around line 732). That conflates Aurekai-owned docs with independent white-label customers. Check title identity against the site's declared brand and subject. Preserve tests that refuse platform branding on independent sites; also add a positive test that permits Aurekai branding on an Aurekai-owned site.

### Better documentation home and navigation

The site manual layout begins on grouped `site_page` records. The homepage is currently a standard community page with all standing pages as ungrouped navigation links. Give docs a genuine docs-home composition: a strong outcome-led hero, four primary journeys, a visible real example gallery, a developer/library entrance, and search. Reuse `site_program`, `semantic_slot`, `presentation_template` and `site_fragment`. The existing manual remains the reference and article reading experience.

Support code/preview splits in the documentation renderer. A visitor should be able to inspect one customer website, its operator view, native command and MCP tool without navigating to four different products. The selected subject and authority remain the same.

## Source- and release-bound reference — Fabric

Exact entry points:

- `engine/core/src/fabric_exec.c`, `operator_reference`, `reference_usage`, `reference_excerpt`.
- `estate/operator-contracts.yaff` and `operator-contract-bindings.tsv`.
- `packs/no-simulation/command-evidence-20261010.tsv` (recorded execution matrix).
- WebAddress `site page --file` for the consumer presentation.

The default managed Fabric state currently reports 96 bound catalog items, three events and a `command.transcribe` reference bound to `blocked_requires_service_v1` at generation `8244d56caf14`, with no last run. The retained October 10 evidence matrix records a different generation `bfc22d2cfda2d2838c6a7926d445b8ce154894416930ee85399bd83912ab16d9`, with a broader list of quality-proven commands. They are independent states and generations, not contradictory results to merge.

Select the Fabric state and release explicitly for a docs build. Generate command reference from the selected catalog and bound operator contract. Attach a run result only where the event, receipt, compiled contract generation and source release agree. If there is no matching run, render invocation and limits without an execution result.

Do not treat a blocked contract with the generic `service_dependent` family as related to all other blocked commands. `operator_reference` currently lists a large cluster of unrelated commands as “In the same family” for exactly this reason. Related links should follow actual functional families or explicitly declared composition relationships.

Reference outputs should use native source/publication policy before emitting sample artifact contents. The current `reference_showable` local-path check does not establish that arbitrary excerpt content belongs on a public customer page. Keep the publisher's asset visibility and release authorization in the same owner as publication.

The native CLI help, Fabric reference, customer guide and MCP tool help need one stable capability identity. The README links to the appropriate guide; the guides link to release-bound reference; neither should be another source of capability truth.

## Concrete acceptance

1. Three local sites are visually distinct in layout and typography as well as palettes, and their independent customer logos/favicons resolve on their own hosts. No Aurekai knight leaks into an independently branded client.
2. One Aurekai-owned documentation site uses the knight and is accepted by `site check`; the independent customer checks still reject accidental platform identity.
3. The documentation landing page has an outcome-led composition, client/partner/native entrances, and working internal navigation. Manual pages retain find, outline, copy, previous/next, code tabs and accessible responsive layout.
4. The `three-businesses` and `export-import-readback` examples remain linkable to actual local sites. Their core actions run through Bonfÿre Site, Bridge and CMS owners, not scripted stand-ins.
5. A reference build for the current managed Fabric state does **not** claim that the October 10 matrix events happened in that state. A separately selected historical state can show its own generation and evidence.
6. A second database readback verifies the moved sample records; content-format import is checked separately from relational bridge equivalence.
7. Test WebAddress and Fabric changes with their existing native tests, local host readback and independent-site MCP credential checks. Render both desktop and phone views and inspect their results.
8. Preserve the existing dirty working tree and staged work from other lanes. Limit source edits to their owners and use targeted builds and deployment; do not invoke global sync or publish the profiles.

No new document-compiler store is necessary. The artifact of this work is a better projection of existing native contracts and customer records, and a release-bound documentation experience served through the existing WebAddress deployment machinery.
