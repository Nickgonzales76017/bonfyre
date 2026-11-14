# Visual editor served + estate CSS pipeline (2026-11-11)

## Visual workspace: built, served, mounted

- `bun run build` in components/BonfyreServiceSite/authoring installs the
  pinned GrapesJS/Puck/Editor.js toolchain and bundles entry.mjs to a
  stamped, gitignored site/matrix/authoring-editor.js (3.18 MB) plus the
  GrapesJS and shell stylesheets.
- Served at /cms/compose/advanced behind the composer write grant, with
  ?site= ownership enforced; the bundle, shells and route are allowlisted
  assets. Narration handoff (article-narration + Audio Studio entry) and
  the section arranger ship inside the mounted workspace.
- Proof: headless Chrome mounted with zero console errors and listed the
  site's pages. (Chrome must resolve an estate host: assets 404 on bare
  127.0.0.1 by design; use --host-resolver-rules.)

## Select purge: radio pills, identical contracts

- Companions territory/sort filters, report reason and quiz
  choice/importance selects became radio-pill groups (cp-pills, ph-pills)
  in the same forms; POST/GET field contracts unchanged. Live pages show
  pills with zero <select>; quiz answers record 303 end to end.
- Remaining estate selects (account worlds et al.) follow the same
  pattern; none are load-bearing as selects.

## Tailwind pipeline established (system CSS, phase 1)

- The shipped tailwind.css was unreproducible: its content scan corpus
  never existed in the repo. deploy/tailwind now pins the toolchain
  (tailwindcss 3.4.19 + forms + typography) and scans the served markup
  itself (C sources behind escapes, templates, shells) with a
  class-attribute-only transform, so C keywords never become utilities.
- The semantic @layer components ship whole via safelist (proven
  necessary: CMS-held and release-tree usage is invisible to repo
  scans); utilities follow real usage. Rebuild kept every shipped
  selector except provably dead `static`; serves live at full size.
- `bun run check` + the estate-css workflow hold served bytes identical
  to a fresh build. Next: token-layer unification (--w-/--n-), then
  surface-by-surface extraction of the 42 embedded style blocks.
