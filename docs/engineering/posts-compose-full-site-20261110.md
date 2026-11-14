# Posts compose the full site

2026-11-10. Direction: profile posts mature to handle full GitHub-Pages
composition. No separate pages track, no HTML importing, no byte-mirroring.

## Current state (verified)

- Post sources: `editorial/posts/*.md` (frontmatter + markdown).
- Intake exists and is tested: `journal-import.mjs` parses posts into
  authoring trees and saves native isolated editable profile drafts via
  `native-cms` page-write. Sixteen posts round-trip; intake cannot publish.
- Composition machinery exists and is tested: md4c markdown, mustach
  templates, 43 native components, editorial blocks, Puck/GrapesJS
  adapters, site fragments, release/export with staging verification.
- Served output exists twice, separately: `site/blog/*/index.html`
  (hand-composed styled journal articles, deployed to GitHub Pages) and
  the profile tabs on `nick.aurekai.work`, which link OUT to github.io
  instead of composing.
- Profile rendering lives in `profile_home.h`, `profile_gallery.h`,
  `community_journey.h`. Styling assets (`aurekai-field-guide.css`,
  `aurekai-research.css`, `blog.css`, `product-icons.svg`) are served.

## The gap

Nothing composes a native post draft into the full styled article shape
(hero, breadcrumb, icon, nav shell, CSS) on the estate. The intake tests
stop at drafts; the release tests compose synthetic documents, not posts;
the site HTML is authored beside the pipeline, not through it.

## Target

`nick.aurekai.work` serves journal posts composed 1:1 with the GitHub
Pages rendering — same blocks, same components, same copy, styled —
through: post markdown → authoring tree → native CMS draft → composed
styled page. Block-and-component parity, not byte mirroring: the Pages
HTML is never an input.

## Style travels with the composition, not beside it

The mature style (field-guide/research/blog CSS, product icons, theme
tokens) enters the same way: the release compiler's asset closure
(`checkedAssets`) admits only exact reviewed sources with SHA receipts,
and the composed pages reference the served stylesheets. A composed
article that renders unstyled is a failed composition, not a follow-up.
Parity covers style identity: same stylesheets, same icon set, same
shell contracts (hero, breadcrumb, nav). The estate serves these from
its asset path; GitHub Pages output is the reference rendering, never
the source.

## Build slices

1. **Draft-to-article composition root (native).** Extend the native
   component renderer with journal-article assembly: frontmatter to hero
   (title, lead, category, icon, read time), markdown through md4c to
   sections, mustach page shell with nav + CSS refs. Reuse the 43
   components; add none without a failing case first.
2. **Profile route binding.** Serve composed posts under the profile
   territory (`profile_home.h` family), reusing its scope and privacy
   rules. No new track, no parallel router.
3. **Styled output.** Reference the served CSS/icon assets; add no new
   style system. Visual check in `preview` before any release claim.
4. **Parity proof.** A test that composes every post in `editorial/posts/`
   and asserts per-block/component equivalence with the `site/blog/`
   reference: same block sequence, same component instances, same text,
   same shell contracts. Fails on drift in either direction.

## Decided

1. Route shape: `/blog/:slug`, mirroring the Pages addresses (Nick,
   2026-11-10).
2. Publish mechanics: explicit promote, reusing the CMS conditional
   update — `entry update <type> <id> '{"status":"published"}' --if-field
   status --if-value draft` (verified: promotes once, refuses re-promote;
   no new verb). Related selection for keep-reading stays an explicit
   caller input until a recommender exists.

## Implementation (2026-11-10, uncommitted worktree)

Native path, no second renderer. `profile-story` bodies render through
the reviewed md4c stage inside `authoring_native_components.c`, between
the escaped prefix and suffix — the same slot the editorial stage uses;
triple-mustache stays forbidden. The serving binary links the shared
units (`site_authored_document`, native components, editorial,
template, markdown, md4c, mustach). `/blog/:slug[/]` is served by the
profile family (`profile_home.h`) under the apex site scope; the
projector needs no new track.

Intake retargeted from `/journal/` to `/blog/` (importer, launcher,
fixtures). Frontmatter `category`/`icon` now parse; category rides to
the page `eyebrow`. `site_page` gained the six editorial fields the
intake already wrote — they were silently dropped before, so no
published date ever persisted. Deploy note: run `bonfyre-cms schema
migrate` against the live CMS store; the served route tolerates
unmigrated stores (dateless hero, route-ordered siblings).

Shell contracts, served form: estate-absolute asset refs
(`/assets/*`, `/blog/blog.css`), four sheets (field-guide, research,
blog, authored-reader — the last dresses component sections), estate
canonical, hero (breadcrumb, eyebrow category · read time, h1, lead,
long date), rail (section titles as text — md4c emits no anchors, so no
TOC links rather than dead ones), keep-reading (newest published
siblings, deterministic fallback), footer. No per-article glyph: icons
parse but have no CMS field. Title-fold matches the JS composer: an
opening section headed exactly like the page renders body-only.

Verified: reference post 200 from preview with both section headings
in order, `2 min read`, `November 7, 2026`, no origin leak;
unpublished slug 404; all five style assets 200. Native suites
(components, document, markdown, fragment, contract) and intake suites
(journal 16-post, narration, static-candidate, cms) green; control
plane 1131 pass; ratchet ok and tightened (1611 files / 178526 lines,
paid by retiring the dead component-verification script cluster).
Pre-existing, untouched: sandboxed browser/socket tests fail at launch;
SurfaceScene Makefile names two absent files
(`cms_advanced_composer.test.mjs`, `audit-native-library-candidates.mjs`).

## Intake registry (2026-11-10, same worktree)

One registry, every kind: `intake-registry.mjs` normalizes WordPress
(receipted `normalized.txt`), RSS/Atom, GeoJSON feeds, Frappe AppPack
surfaces and Ghost Content API posts to `aurekai.journal-intake.v1`
journals, which ride the unchanged merge → page-write → promote →
`/blog` path. Detection is by shape with explicit-kind override; raw
WordPress block HTML is detected but refused with direction to the
normalized form. Feeds digest to a declared top-N (USGS 560 → 24 by
magnitude, AppPack 37 → 24 doctypes) rather than refusing large
ordinary inputs; Frappe field codes render through the compiler's own
`BfFieldType` order. Slugs namespace per kind (`wp-`, `rss-`, `usgs-`,
`frappe-`, native Ghost slugs).

Verified: all 8 ingested WP posts, 12-item Dallas RSS, 560-feature
USGS feed and 37-doctype insights pack parse; one article per kind
served 200 from preview with kind-correct eyebrows, populated
keep-reading, estate canonicals and no origin leaks. Committed tests
use trimmed receipted fixtures (`tests/fixtures/intake/` + SHASUMS);
no Ghost dump is ingested, so its parser is proven against a
hand-built API-shape fixture. Ratchet paid by retiring five verified
dead ops scripts (808 lines) and tightened to 1608 files / 177982
lines. Remaining: live dashboard data feeds, raw block-HTML parsing,
per-article product-icon CMS field.
