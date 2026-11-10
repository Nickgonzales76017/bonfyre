# Source workspace interaction research and release

The previous public workbench exposed compiler objects and a flat configuration form. The October 15 revision adds operations on retained sources and proposed cycles while reusing the existing browser store, observation parser, recurring-program compiler and ServiceSite publisher.

## Primary-source patterns

| Product documentation | Interaction studied | Applied behavior and boundary |
|---|---|---|
| [Recharge Affinity next-order page](https://support.getrecharge.com/hc/en-us/articles/22179636783895-Understanding-the-Affinity-2-0-next-order-page) | Focus one order; put actions on that order; change available actions with its state. | Dated cycle cards have skip, restore and single-cycle rescheduling, with explicit confirmation and recalculated modeled costs. These are local proposal edits, not changes to a Recharge order, charge or customer account. |
| [Recharge reschedule customization](https://docs.getrecharge.com/docs/customizing-reschedule-options-in-affinity-20) | Offer quick delays and a calendar in the focused action, with bounded available dates. | One-week and original-date shortcuts accompany the date picker; the preview explains exactly which cycle moves and the monthly-cost changes. Neighboring cycles and the original date bound the proposal. Supplier availability is not inferred. |
| [Airtable CSV import extension](https://support.airtable.com/articles/3067164948-csv-import-extension) | Preview identity matches and distinguish new, changed and unchanged records before committing. | Existing capture files receive an explicit import review with unchanged, changed-version, new-source and invalid rows. The selected writes are atomic. This release does not add general CSV mapping or live URL discovery. |
| [Shopify inventory bulk editing](https://help.shopify.com/en/manual/inventory-and-locations/adjusting-inventory/bulk-editing-inventory) | Work on selected records, select relevant properties and handle concurrent changes before overwriting inventory. | Source selection, context-specific cycle actions, saved-versus-proposed review, read dependencies and stale-tab refusal. No Shopify account or inventory mutation is connected. |

These references were reviewed as published product documentation. No authenticated competitor account was exercised and no claim of full product parity is made.

The single-cycle scope is intentional. Changing one proposed delivery is not changing the recurring series, and moving a skipped prepaid delivery cannot imply a refund or another charge. Recharge also distinguishes a click from a completed action in its [Affinity event model](https://docs.getrecharge.com/docs/leveraging-events-in-affinity); this workspace likewise separates preview, confirmed local change, reviewed save and any future external effect.

## Released functionality

The workspace opens into a searchable source library. Import review keeps exact source versions and suppresses content-identical timestamp-only refreshes. Comparisons show source addresses, captured terms and exact supported prices. Applying a price changes only the proposed price after confirmation. Cycle actions use the same compiler as ordinary editing; a skipped prepaid gift delivery still costs the full prepaid term. Undo and redo restore editing-session values and source selections.

Draft saves check selected source revisions in the same transaction. Source removal scans saved draft references inside its own write transaction, so a concurrent admission cannot leave a dangling reference. Conflicts preserve the open proposal. Both the main workspace and full studio render readable proposals instead of raw compiler dumps. Existing interoperable exports remain separate from normal review.

The cycle-date continuation extends the existing recurring-program terms rather than adding a scheduler. Original cycle identity survives repeated moves and skips, later cycles retain their dates, collisions and stale series references refuse, and an open confirmation cannot overwrite intervening edits. Cancel changes nothing; undo and redo include the date change. The saved proposal, readable export and full studio preserve the same original-to-proposed date relationship.

## Verification and publication

Run `make -C integrations/browser test-workbench`, then run `bun integrations/browser/tests/program-editor-browser.test.mjs` and `bun integrations/browser/tests/workbench-interaction-browser.mjs` from the checkout. Each browser witness accepts the existing explicit `WORKBENCH_TEST_ORIGIN` and `WORKBENCH_TEST_PROOF` settings. The public origin is restricted to the owned browser-workbench branch. Publisher operations remain `make -C integrations/browser publish-workbench` and `verify-workbench`.

The live user guide is [Sources, cycles and a reviewed proposal](https://docs.aurekai.work/source-program-workspace); the installed compiler and existing Cloudflare bridge are described in [Native source reuse](https://docs.aurekai.work/native-source-reuse). Native documentation records own the rendered pages. The public docs release preserves unrelated files from the exact prior deployment rather than replacing the estate with an older local export. Initial interaction evidence is retained in `output/verification/workbench-interaction-20261015/`; cycle-date implementation, local and public browser journeys, source hashes and deployment readbacks are retained in `output/verification/workbench-cycle-continuation-20261015/`.

The continuation's regression has 107 passing tests and 332 assertions. The docs publication changes only the source-program guide and retains the other 25 deployed files; all 26 served digests and seven routes at desktop and phone widths were checked. The Work Samples card and carousel use the actual successful public-browser rescheduling screenshot. Canonical architecture-atlas publication and Git commit remain separate gated work; these product and documentation deployments do not establish them.
