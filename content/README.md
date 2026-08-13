# content/

Filesystem source-material vault for the Aurekai Research & Relations
Institution — the model where every experiment, maintainer interaction, and
piece of content traces back to one connected evidence packet instead of
living as disconnected marketing output.

This is a stand-in, not the real thing. The actual design puts this on
`Drive` (one of nine Frappe apps in `integrations/frappe-bench/apps/`) with
`BonfyreFS`/`ArtifactGraph` projecting research-episode folders
automatically. As of 2026-08-13 that bench has all nine apps
(`frappe`/`crm`/`erpnext`/`hrms`/`helpdesk`/`lms`/`wiki`/`drive`/`insights`)
cloned as code but the site was never initialized — no `site_config.json`,
no database, nothing running. Spinning that up is real infrastructure work
(MariaDB, Redis, `bench new-site`) that needs an explicit decision, not
something to do silently. Until then, this directory holds the same shape
by hand.

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
