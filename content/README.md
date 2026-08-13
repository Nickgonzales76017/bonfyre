# content/

Filesystem source-material vault for the Aurekai Research & Relations
Institution — the model where every experiment, maintainer interaction, and
piece of content traces back to one connected evidence packet instead of
living as disconnected marketing output.

This is raw source material — the receipts and working notes an entry gets
written from, not the entry itself. **The real entry lives natively in
Bonfyre's own CMS runtime**, not in a live Frappe/MariaDB site: see
`cmd/BonfyreCMS` (`bonfyre-cms entry create <type> --ns <ns> '<json>'`,
backed by `bonfyre-zig/bonfyre_cms.db`) and `cmd/BonfyreFrappeCompiler`
(compiles a real Frappe app's doctype JSON into a schema graph without
needing that app's site to actually be running). The nine Frappe apps under
`integrations/frappe-bench/apps/` are the *schema source* — real doctype
JSON, e.g. `wiki/wiki/wiki/doctype/wiki_page/wiki_page.json` — that gets
hand-derived (or eventually compiler-derived) into a `BonfyreCMS`
content-type schema under `cmd/BonfyreCMS/content-types/`, then written as
real entries. The Frappe *site* itself was never initialized (no
`site_config.json`, no database) and isn't needed for this path — that's
only required for the Frappe web UI, not for the native content substrate.

As of 2026-08-13: `wiki_page` and `course_lesson` content-type schemas exist
in `cmd/BonfyreCMS/content-types/`, derived from the real Wiki/LMS doctype
fields. The Bernstein research-episode content and the Identity Continuity
lesson below are written as real entries under namespace `aurekai` in
`bonfyre_cms.db` (`entry get wiki_page 1` / `entry get course_lesson 1`) —
this directory is their source material, not a stand-in for them.

## Layout

```
content/
  research-episodes/
    <slug>/
      README.md          -- what this episode is, status, what's missing
      notes/              -- exports, working notes
      receipts/           -- verbatim primary evidence (reviews, logs, etc.)
```

Each episode's source material should trace back to a row in
`~/Library/Application Support/Bonfyre/CapitalGym/capital.db`, table
`content_genomes` — that row is the canonical thesis/claims/evidence/gate
state; this directory is supporting material for it, not a second source
of truth.

## Rule

Nothing gets fabricated to fill out the shape. If a category (screenshots,
video, clips) doesn't have real material yet, its directory doesn't exist
yet either.
