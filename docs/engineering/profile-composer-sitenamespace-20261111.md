# Profile publishing through the shared page composer (2026-11-11)

Decision: profile pages, posts and site design open the existing visual
authoring tools and publish through the one shared page model
(site_page + site_fragment), instead of growing another import queue or
review screen.

## What already existed (verified, not built)

- One page model: site_page editorial fields + site_fragment relational
  blocks, status-gated publication, native projector serving.
- One composer: /cms/compose (+/page, +/pages) with digest-guarded
  read-modify-write via `bonfyre-cms entry page-read/page-write`.
- Visual editors: Puck/GrapesJS/Editor.js adapters converging on
  authoring-tree.v1, round-tripping through the same snapshot contract
  (native-cms.mjs treeFromNativeSnapshot/toNativePageWrite).

## The two gaps, and the change

1. Namespace: the composer always opened the actor-<user> workspace;
   profile pages live under the site namespace and the `site` parameter
   was ignored server-side. serve_cms_composer now honors `?site=` only
   with the exact profile ownership proof (write session, bound actor,
   published member site, Matrix presence) and passes it to
   page-read/page-write --ns. No sharing model implied.
2. Entry points: the profile edit tab links into
   /cms/compose?site=<site-id>, and the served composer threads the
   site through read, save, listing and history so a site edit can
   never land in the wrong namespace.

## Proof

- cms_page_composer.test.mjs: owner opens/saves/lists in the site
  namespace; another account and unknown site get 403; actor-namespace
  isolation holds. (The file's browser test fails in this environment
  on the pristine tree too — pre-existing, puppeteer/Chrome.)
- Live: /cms/compose/page?route=/&site=person-nick-work returned the
  real site snapshot (namespace person-nick-work, page id 42) for its
  owner; the edit tab renders the entry link.

## Left open

- The unified-editor bundle was never built and no route serves it;
  serving it needs no backend change (same endpoint contract).
- Free-tier owners hit the composer's existing write-grant 403.
