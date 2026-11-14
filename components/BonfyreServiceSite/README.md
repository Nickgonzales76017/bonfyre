# ServiceSite

The kit behind the [work samples](https://aurekai-work-samples.pages.dev/). **Built on software. Driven by Aurekai.** This signature applies to Aurekai-owned examples only; independent customer sites retain their own names and branding.
Each site is owned by a canonical `service_site` record in BonfyreCMS;
its folder's `content.json` is an export for review, build and publication,
not an independently authoritative source. `site.mjs publish` pulls the
saved CMS record before building and deploying. Public interactive demos,
installed native workflows and descriptive walk-throughs are different outputs.

| What | Try it | In this file |
|---|---|---|
| Fixture planner | [Open](https://aurekai-work-samples.pages.dev/red3-planner/) | [Fixture planning](#fixture-planning) |
| Audio mixer | [Open](https://aurekai-work-samples.pages.dev/audio/) | [Audio mixing](#audio-mixing) |
| Product editor | [Open](https://aurekai-work-samples.pages.dev/catalogue/) | [Catalogue editing](#catalogue-editing) |
| Document search | [Walk-through](https://aurekai-work-samples.pages.dev/documents/) | [Local document workspace](#local-document-workspace) |
| Training app | [Walk-through](https://aurekai-work-samples.pages.dev/training/) | `bun components/BonfyreServiceSite/site.mjs training packs/aurekai-fitness-studio --port 4178` |

Guides for the rest of Bonfÿre are at [docs.aurekai.work](https://docs.aurekai.work/).

Check the published sample pages in a fresh browser:

```sh
rtk proxy bun components/BonfyreServiceSite/tests/public-samples-browser.mjs --live
```

This checks headings, images, loaded resource responses, links to page anchors,
and settled desktop and phone layouts. It records screenshots and failures in
`packs/no-simulation/live-samples-20261013/pages/` by default; set
`SAMPLE_BROWSER_PROOF` for another evidence directory. It submits no forms.
Catalogue editing, planner persistence and the planner's live lead handoff have
separate browser witnesses; a page audit does not establish those workflows.

## Local document workspace

Open the synthetic household sample on this Mac:

```sh
rtk proxy bun components/BonfyreServiceSite/site.mjs documents packs/aurekai-work-samples/household --port 4177
```

Visit `http://127.0.0.1:4177`. Search by meaning or exact phrase, open the original paragraph and save a cited note. Notes use the existing browser workbench store, with revision checks to prevent a stale tab from overwriting a newer save. They belong to that browser profile and origin; keep the same port when returning.

The command requires the installed `bonfyre-embed` and `bonfyre-vec` binaries and the local `nomic-embed-text-onnx` model. It indexes bounded `service-site.documents.v1` JSON collections with document IDs, titles and plain text. The sample contains eight fictional documents and 24 passages. The index stays in the collection's ignored `.documents` directory. Queries and indexing run through the installed native commands; the page loads no remote assets.

Removing a document deletes every indexed passage and keeps it removed after restart. Original collection text, saved notes, embedding caches and backups remain. A changed source collection is refused until a reviewed index is prepared in a new collection folder. This version has no PDF import, OCR, cloud handoff, n8n integration or automatic answer generation. Similarity results are passages for review and can include unrelated material.

The local HTTP service binds to `127.0.0.1`, checks origin and host, and requires its page's session token for document operations. These checks do not isolate documents from other software or people using the same Mac account. Avoid exposing the service through a public proxy.

Checks from the repository root:

```sh
rtk proxy bun test components/BonfyreServiceSite/tests/documents.test.mjs
rtk proxy bun components/BonfyreServiceSite/tests/documents-browser.test.mjs
```

The browser check uses a temporary collection and dedicated Chrome profile. It exercises the installed native model, citations, invalid input, save conflicts, failed removal, desktop/mobile layouts and full process/browser restart. Its screenshots and receipt are in `packs/aurekai-work-samples/site/design/verification-documents-local`. Google Stitch generation remains pending; the authored implementation is not a Stitch result.

### Prepared n8n example

`packs/aurekai-work-samples/household/n8n/household-source-review.json` is a prepared, unpublished workflow for n8n 2.42.5. A manual trigger reads a question, calls the existing local workspace, then retrieves the original text and source digest for review. An exact-phrase search with no match has a separate branch. It contains no cloud AI, email, calendar, Notion or deletion nodes.

This export has **not run successfully in n8n**. The temporary npm installation exhausted disk space on October 8; a 512 MB heap retry also ran out of memory. The failed installation was removed. Do not present the export as a working n8n integration or an installed client workflow.

The generator is `packs/aurekai-work-samples/household/n8n/workflow.mjs`. Once the pinned runtime is available with sufficient disk space, run `components/BonfyreServiceSite/tests/documents-n8n-witness.mjs` with Bun and `BONFYRE_N8N_INSTALL` pointing at its installation folder. That pending witness uses a temporary collection and n8n database; it checks source identity, the empty-result branch, rejected input, persistent removal and unavailable-service failure. It imports no credentials and publishes no workflow.

### Source package

Prepare a source preview in a new directory:

```sh
rtk proxy bun components/BonfyreServiceSite/document-package.mjs /tmp/aurekai-document-source
```

The package copies the existing workspace, browser store, native Embed/Vec sources and required runtime-library slice, plus the fictional sample. An explicit allowlist excludes private records, installed binaries, model weights and local indexes. Each copied source has a SHA-256 entry. Existing output directories are refused.

Follow the package's `README.txt`, then run `bun run.mjs build` and `bun run.mjs serve 4177` there. Its launcher uses the newly compiled commands and checks the pinned Nomic model and vocabulary. It requires separately installed Bun, Homebrew SQLite and ONNX Runtime; it downloads nothing. The current build recipe targets Apple Silicon macOS.

`tests/documents-package.test.mjs` compiles and serves the exported sources in a temporary directory, checks the original source passage and persistent removal after restart, and refuses to fall back to the user's installed command when the package binary is missing. The October 8 run passed 44 assertions. This is a fresh-directory test on the development Mac; a clean-machine installation and public release remain pending.

## Catalogue editing

The site record supplies the layout and client copy. `catalogue-material.mjs` packages retained products for the editor and generates the intake contract. `catalogue-baseline.mjs` uses the pinned parse5 dependency to prepare description text once, then hashes each original editable value. The browser reads that prepared description; the intake worker and native lead hook check submitted originals against the same field digests.

Install the build dependency from this directory:

```sh
rtk proxy bun install --frozen-lockfile --ignore-scripts
rtk proxy bun run test:catalogue
```

The parser runs during material preparation and native review. It is not loaded by the visitor's browser or the deployed intake worker. Original-field hashes stay out of the page's product index. A matching original value establishes agreement with the retained catalogue, not current merchant data or approval of the proposed edit.

From the repository root, build and publish the work-samples site through its existing owner:

```sh
rtk proxy bun components/BonfyreServiceSite/site.mjs build packs/aurekai-work-samples/site
rtk proxy bun components/BonfyreServiceSite/site.mjs push packs/aurekai-work-samples/site
rtk proxy bun components/BonfyreServiceSite/site.mjs publish packs/aurekai-work-samples/site
```

After reviewing local edits, `push` saves them to the native CMS. `publish` pulls that saved record before rebuilding and deploying; local edits that have not been pushed are replaced by the CMS copy.

After publication, run `rtk proxy bun run verify:catalogue-live` from this directory. This opt-in check compares three deployed assets against the local build and submits one marked draft with a deliberately wrong original title. It expects HTTP 400 and no receipt. It contacts only `aurekai-work-samples.pages.dev`; it does not send outreach or publish merchant changes. It is an API check, not a browser walkthrough.

The catalogue review tests cover valid and forged originals through generated intake and the native lead hook, retries, failed storage and the complete CMS handoff. `tests/leads.test.mjs` covers recovery after partial sync failures. `tests/routing.test.mjs` checks that only lead endpoints invoke the intake worker.

## Review receipts

After a successful submission, the workspace saves its receipt beside the draft in the existing browser store. The Products filter has a **Sent for review** option. Reopening the same saved version shows its reference without sending another request. A later saved version is ready for a new review.

Receipts are tied to the form, source and full saved changes, and contain no sender contact details. They record receipt of a submission, not a review decision. If the server receives the draft but local storage fails, the page keeps the reference visible and says it could not save it. Retrying that saved version recovers the server's existing receipt.

## Fixture planning

A dedicated workspace page can use a `planner` section and a named material function returning `plannerFiles`. The site record supplies its title, merchant presentation and preset labels; the retained collection supplies product options, prices, dimensions, sources and starting layouts. The RED3 example is `packs/aurekai-work-samples/site/fixture-planner.json`.

Visitors can arrange up to twelve fixtures, compare starting layouts, set a budget, save locally, share a copy and download or print the brief. Saved projects keep separate spaces and alternative arrangements. Plan, fixture selection, project library and comparison have their own reloadable views. Comparisons show exact quantity and listed-cost differences, budget problems and supplier questions; their download includes both source-bearing project briefs. Copies have fresh identities and no inherited review receipt. Older-source plans remain stored and are excluded from current comparisons. Missing dimensions stay visible as supplier questions. The review action sends a saved copy to the site's existing intake and native review queue. The work-samples preview explicitly routes to Aurekai. It does not contact RED3 or place an order.

Build and check the example:

```sh
bun components/BonfyreServiceSite/build.mjs packs/aurekai-work-samples/site --out /tmp/aurekai-fixture-planner-build
bun test components/BonfyreServiceSite/tests/planner.test.mjs
bun components/BonfyreServiceSite/tests/planner-browser.test.mjs
```

The browser check uses a fresh Chrome profile, the built intake worker and temporary local storage. It verifies reload, restart, shared links, conflicting saves, invalid input, exports, failed-send recovery and responsive layouts. It sends no external requests to intake. Screenshots and results go to the site's `design/verification-planner-local` directory. Source observations and the pending Google Stitch brief are retained beside it.

To check accepted intake on the published work-samples site, run:

```sh
PLANNER_VERIFY_LIVE=1 bun test components/BonfyreServiceSite/tests/planner-live-intake.test.mjs
BONFYRE_QUIET=1 bun components/BonfyreServiceSite/site.mjs leads packs/aurekai-work-samples/site
```

The live test compares deployed assets with the local build, submits one marked synthetic plan and retries it to verify the receipt. It writes to Aurekai's intake and retains evidence in `design/verification-planner-live/intake`. The request stays identical on repeat runs against the same catalogue. Check the matching native lead's full plan and brief, run the sync again to check for duplicates, then mark that synthetic lead handled and its held message abandoned. The October 8 run completed these checks with lead 13 and message 64; no supplier was contacted.


## Audio mixing

The `/audio/` workspace has a generated sample mix and an editor for local WAV recordings. Both use the shared native C kernel compiled to WASM in a module worker. Visitors can play, edit, save in their browser and download a 16-bit stereo WAV. The existing browser workbench store protects saves against stale tabs.

A `creator` workspace section uses named material from `creatorFiles(directory)`. Its `sample.json` names bounded, hashed PCM files and the initial native output digest. Packaging refuses changed sources or a changed initial mix. The public package contains only selected source frames, the verified kernel and browser modules; it includes no private filesystem paths or recorded audio. The worker verifies the same sources before enabling playback. Each package has a content-addressed asset directory so browser caches keep its modules together.

```sh
bun test components/BonfyreServiceSite/tests/creator.test.mjs
bun components/BonfyreServiceSite/build.mjs packs/aurekai-work-samples/site --out packs/no-simulation/creator-site-20261013/dist
bun components/BonfyreServiceSite/tests/creator-browser.mjs
```

The browser witness checks saved-level reload and conflicts, invalid gains, source failure, playback and a real downloaded WAV on desktop and phone layouts. Add `--live` to test the public route without submitting anything; only the disposable browser profile is written. The sample mix uses generated tones. The Your recordings workspace also imports PCM 16-bit and float32 WAV files, up to eight tracks, 24 MB combined and 30 seconds per track. All tracks in one session must share a sample rate. Mono is duplicated to stereo. Users can trim clips, place them on a 30-second timeline, change levels, mute, preview and export a stereo WAV with source-bearing session notes. Saved imported sessions retain the original WAV buffers and their hashes in the existing revision-checked browser store. Restoring changed source bytes fails. Recording permission choices record the visitor’s statement; they do not grant publication rights. No files are uploaded. Effects, live recording, account sync and a streaming decoder remain separate work.
