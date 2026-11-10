# ServiceSite source ownership

Do not put all site behavior in site.mjs or build.mjs. The canonical boundaries are:

| Concern | Authoring owner |
|---|---|
| Command routing | site.mjs |
| CMS push, pull and publication | site-publication.mjs |
| New-site drafts | site-draft.mjs |
| Lead reconciliation and hook safety | site-leads.mjs |
| BonfyreCMS invocation and records | site-cms.mjs |
| Record validation and page shape | site-contract.mjs |
| Individual section rendering | sections/<kind>.mjs |
| Section registration | section-registry.mjs |
| HTML page composition | site-renderer.mjs and shell.mjs |
| Material source resolution | site-material.mjs |
| CSS pipeline | site-styles.mjs and looks/ |
| Build output and cloud publication | build.mjs and cloud.mjs |
| Catalogue, documents, training, planners and workbook | The existing matching feature modules and tests |

build.mjs reexports the old stable API; new logic belongs in its dedicated owner.
Run bun run test:compiler and bun test tests/site-cms.test.mjs tests/site-leads.test.mjs for focused witnesses.
Do not treat syntax checks as these functional tests having passed.
Read-only five-root file classification: scripts/analysis/repository_maturity.mjs.
Never delete installed donor/runtime sources without independent parity, rollback and DeletionProof.
