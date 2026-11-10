# Live retained-source navigation — October 22, 2026

## Deployed account application

The existing Connected research and planning installation for `user:19` is active at revision 22, release `d8b2d86be17aed532bf84cffb9fb6c75efe1b8c4e3ca1375e4c2730a72b0c25a`. Its binding remains `ws-bd08c6776460a11c6d1e970b01f53cc15b65a37a`; the saved-work route remains `/apps/ws-bd08c6776460a11c6d1e970b01f53cc15b65a37a/_work`.

The change upgrades the installed research reader. It adds no recovery studies, task rows, provider grants or completed-application claims. All 98 pre-existing saved record rows and all other workspace bindings were unchanged. The public service configuration and operator STOP marker were unchanged. No client application was connected or operated.

## What changed for the reader

Machine-local contract references now open the complete retained contract inside the current account workspace. The reader loads every part through the existing native record owner, verifies each part and its original offsets, and verifies the combined source digest before displaying a successful result. Search, retained-part navigation and return to the original requirement are available in the source reader.

Partner-profile references open the exact profile text already embedded in that requirement's saved source. The reader reopens the originating native record, checks its current source identity and digest, and extracts only the exact retained profile coordinate. It never reads a host file, follows a file URL, searches for a similarly named replacement, or imports another copy.

The initial view separates the actual requirement from the imported agent handoff. Show original handoff exposes the complete historical text through the same reference-aware rendering. Original source strings, source digests and citation offsets remain unchanged in storage. Selecting evidence translates the visible text back to its original offsets; selection across a reference control refuses rather than producing a false quotation.

Two local release-directory coordinates in the historical contract are displayed as historical deployment identifiers. They are not presented as readable source files or links.

## Exact coverage and acceptance

The scan covered all 98 imported studies. It found 1,516 local coordinates across 92 records: 1,412 contract references, 102 partner-profile references and two historical release-directory labels. All 1,514 file references resolved to their exact retained bodies, representing 104 distinct source targets. The seven contract records reassemble into the two complete original contracts. The 102 profile bodies match the separately retained original source text byte for byte.

Nine tests passed with 6,221 assertions immediately before activation. They cover the complete imported set, unchanged package members, missing/altered/duplicate contract parts, embedded-source mismatches, foreign-account refusal, desktop reading/search/selection/save/reopen, phone source reading, account-change clearance, denied reads and cancellation of late responses. The UI tests use isolated native Auth/CMS/WebAddress services and actual Chrome, not a standalone mock page.

Test receipt: `f658fb5b-be3b-4735-a3bb-a4c9e041ac7e`. Exact deployment review: `da94a512-eae5-46b4-8196-4c0fa1b5b0cd`. Activation and unchanged-state verification: `48aa4a26-d172-4553-8c59-139d0c05261e`. A subsequent authenticated workspace-list read independently returned the new release as ready; the setup-fleet record still resolved at revision one with its unchanged body digest.

## Ownership and remaining boundaries

Implementation: `integrations/browser/retained-material.mjs`, `retained-material-view.mjs` and `retained-material.css`. `scripts/materialize_reference_repair.mjs` changes only the current package's research renderer, its current-record getter and source metadata, adding those three shared files. `scripts/release/reference_reader_rollout.mjs` uses the existing Pack lifecycle compiler and owner, rechecks the current installed release and declaration identities, reruns acceptance, and verifies unchanged saved records after activation.

The two connected remote QA browser profiles currently have no recorded Aurekai sign-in. This release has production native account/deployment readback and isolated authenticated browser interaction evidence; it does not have a fresh click-through using the user's production browser session. Canonical Atlas registration was refused by path preflight because `architecture/surface/interaction-components.yaff` requires the manual write gate; no alternate write path was used.

This closes the identified local-reference defect in the imported Research workflow. It does not complete the hundreds of underlying business, institutional and integration operations, and no historical recovery or application-completion milestone was advanced by this repair.
