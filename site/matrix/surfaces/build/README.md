# How these pages are built

The pages one directory up are authored HTML, with shared assets and native
surface programs supplying their dynamic material. Edit those pages directly.
The former Python wrappers embedded entire pages as string literals; their
archived versions are historical snapshots, not build inputs.

    ../world.html     authored source    the shelf and every character
    ../console.html   authored source    the subject instrument
    ../packs.html     authored source    the pack closure
    ../home.html      authored source    the four territory landing pages
    ../link.html      authored source    the foreign link desk
    build_surface.py.archive  retired helper with no live caller; clone
                              grammars are compiled by BonfyreWebAddress

Publish the authored page to the host's `site/matrix/surfaces/`. A page that
loads `/assets/bonfyre-kit.js` also needs `site/matrix/bonfyre-kit.js` republished
from `surfaces/_kit.js` — `test_clone_surfaces.py` fails when those two drift.

The four former `author_*.py` scripts were one-time SSH writers. Their authored
CMS rows now live in JSONL and are checked or imported locally through
`install_authored_rows.mjs`; no active tool embeds a private key, host or
production database path. A dry run is the default. Explicit `--apply`, `--type`,
`--db`, and `--ns` are required to write a local CMS database. The rows do not establish that a
remote institution or host adopted anything.

## What does not belong in a page

The kit holds the shared reading of the census extraction contract
(`K.readSource`) and the date vocabulary (`K.dateToken`). A page that fetches
`/foreign/body` and maps the fields itself has copied that contract out of the
census, where the two can then disagree — which is what these three pages were
doing before they were consolidated, and they had already drifted on which
sensitive flags they honour.
