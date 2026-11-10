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

## Open decisions (Nick)

1. Route shape for composed posts on `nick.aurekai.work`
   (`/blog/:slug` mirroring Pages, or `/posts/:slug` under the profile?).
2. Draft-to-published mechanics: which approval/publish path promotes a
   composed post to the public surface?
