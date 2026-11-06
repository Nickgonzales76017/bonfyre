# Native documentation and white-label presentation closeout

Owner: existing Bonfÿre Fabric, CMS and WebAddress. Do not introduce a second catalog, state store, documentation API, site server or provider-facing renderer. This work is a refinement of existing source.

## Current working slice

`docs/experiences/` contains business, native and integration guides. WebAddress runs the isolated documentation site at `docs.localhost:8916` through its existing `site new`, `site page` and `site mcp` commands. A separate local estate on port 8917 holds Fernhollow, Tallgrass and Juniper with independent site identities, CMS namespaces, offerings and presentation templates.

The existing `site/matrix/own-site.css` is already an editorial manual layout with sticky sidebar, page search, outline, code tabs, copy controls and responsive reflow. Preserve it as a foundation. The needed improvement is richer application-specific page composition and a reusable brand projection, not a CSS-framework transplant.

The current bridge result is useful: a site exported to WordPress WXR, Frappe Blog Post documents and self-contained HTML; each structured export restored one post into an independent local site. WordPress and Frappe import added three trailing newline characters to the body. The native `bonfyre bridge db` copied `community_post` and `site_presentation` into a separate SQLite database and `db-verify` read back 5/5 rows per table with matching values. No deployment or external database was involved.

## Native design owner — WebAddress

Exact entry points:

- `cmd/BonfyreWebAddress/src/site_product.c`, `site_create`, `site_set`, `site_check`, `site_page`.
- `cmd/BonfyreWebAddress/src/site_mcp.c`, `McpSite`, `site_icon_metadata`, `site_browser_icon`, `manual_page`, `emit_tokens`.
- `site/matrix/own-site.css` and `own-site-reading.js`.
- Existing CMS types `presentation_template`, `site_presentation`, `site_page`, `site_fragment`, `site_program`, `application_binding`, `design_fingerprint`.

### Customer-specific composition

`presentation_template` already records `hero_mode`, `navigation_mode`, `media_mode`, `typography_mode`, `card_mode`, `chrome_mode` and `layout_tokens`. The independent-site loader in `site_mcp.c` currently loads only six color tokens, while `emit_tokens` projects only those colors. The other template fields are stored but not realized by that renderer.

Expand the existing presentation projection to carry those declared attributes into meaningful semantic layout modes. Let the CSS choose distinct editorial, industrial, service and community treatments from named template traits, not from demo site IDs or hard-coded brand colors. Build one common set of responsive primitives with variant typography, hero structure, navigation, media and cards. The local sample should visibly separate three businesses when all accents are forced to the same color.

Do not replace content owners: a site page is still a `site_page` with `site_fragment` sections; the customer-facing view lowers the same records. The CMS and WebAddress changes are the owner change, not a new design CMS.

### Favicons, logos and brand separation

`site_browser_icon` and `site_icon_metadata` currently emit the knight only for `site->reading`; independent sites emit no favicon metadata. For a customer deployment, resolve an optional logo and favicon from its approved presentation/media program. For Aurekai's own docs, project the existing knight. Keep the independent client's own mark off Aurekai asset URLs.

Do not infer ownership from whether a site is a feed-reading view. Use the institutional/site subject and approved presentation profile. Preserve the existing safe fallback for websites without supplied assets.

`site_check` currently fails `own-title` when the title contains the word “Aurekai” (`site_product.c` around line 732). That conflates Aurekai-owned docs with independent white-label customers. Check title identity against the site's declared brand and subject. Preserve tests that refuse platform branding on independent sites; also add a positive test that permits Aurekai branding on an Aurekai-owned site.

### Better documentation home and navigation

The site manual layout begins on grouped `site_page` records. The homepage is currently a standard community page with all standing pages as ungrouped navigation links. Give docs a genuine docs-home composition: a strong outcome-led hero, four primary journeys, a visible real example gallery, a developer/library entrance, and search. Reuse `site_program`, `semantic_slot`, `presentation_template` and `site_fragment`. The existing manual remains the reference and article reading experience.

Support code/preview splits in the documentation renderer. A visitor should be able to inspect one customer website, its operator view, native command and MCP tool without navigating to four different products. The selected subject and authority remain the same.

## Source- and release-bound reference — Fabric

Exact entry points:

- `engine/core/src/fabric_exec.c`, `operator_reference`, `reference_usage`, `reference_excerpt`.
- `estate/operator-contracts.yaff` and `operator-contract-bindings.tsv`.
- `packs/no-simulation/command-evidence-20261010.tsv` (recorded execution matrix).
- WebAddress `site page --file` for the consumer presentation.

The default managed Fabric state currently reports 96 bound catalog items, three events and a `command.transcribe` reference bound to `blocked_requires_service_v1` at generation `8244d56caf14`, with no last run. The retained October 10 evidence matrix records a different generation `bfc22d2cfda2d2838c6a7926d445b8ce154894416930ee85399bd83912ab16d9`, with a broader list of quality-proven commands. They are independent states and generations, not contradictory results to merge.

Select the Fabric state and release explicitly for a docs build. Generate command reference from the selected catalog and bound operator contract. Attach a run result only where the event, receipt, compiled contract generation and source release agree. If there is no matching run, render invocation and limits without an execution result.

Do not treat a blocked contract with the generic `service_dependent` family as related to all other blocked commands. `operator_reference` currently lists a large cluster of unrelated commands as “In the same family” for exactly this reason. Related links should follow actual functional families or explicitly declared composition relationships.

Reference outputs should use native source/publication policy before emitting sample artifact contents. The current `reference_showable` local-path check does not establish that arbitrary excerpt content belongs on a public customer page. Keep the publisher's asset visibility and release authorization in the same owner as publication.

The native CLI help, Fabric reference, customer guide and MCP tool help need one stable capability identity. The README links to the appropriate guide; the guides link to release-bound reference; neither should be another source of capability truth.

## Concrete acceptance

1. Three local sites are visually distinct in layout and typography as well as palettes, and their independent customer logos/favicons resolve on their own hosts. No Aurekai knight leaks into an independently branded client.
2. One Aurekai-owned documentation site uses the knight and is accepted by `site check`; the independent customer checks still reject accidental platform identity.
3. The documentation landing page has an outcome-led composition, client/partner/native entrances, and working internal navigation. Manual pages retain find, outline, copy, previous/next, code tabs and accessible responsive layout.
4. The `three-businesses` and `export-import-readback` examples remain linkable to actual local sites. Their core actions run through Bonfÿre Site, Bridge and CMS owners, not scripted stand-ins.
5. A reference build for the current managed Fabric state does **not** claim that the October 10 matrix events happened in that state. A separately selected historical state can show its own generation and evidence.
6. A second database readback verifies the moved sample records; content-format import is checked separately from relational bridge equivalence.
7. Test WebAddress and Fabric changes with their existing native tests, local host readback and independent-site MCP credential checks. Render both desktop and phone views and inspect their results.
8. Preserve the existing dirty working tree and staged work from other lanes. Limit source edits to their owners and use targeted builds and deployment; do not invoke global sync or publish the profiles.

No new document-compiler store is necessary. The artifact of this work is a better projection of existing native contracts and customer records, and a release-bound documentation experience served through the existing WebAddress deployment machinery.

## Page replacement safety verified 2026-10-13

The native `site page --replace` path deleted the page and its fragments before attempting to read the replacement file. An isolated regression test reproduced the loss with a missing file. Valid replacements also received new page IDs, changing their position in navigation.

WebAddress now reads and checks replacement input before deleting fragments. Missing files, directories, empty or whitespace-only text, embedded NUL bytes and combined `--replace --remove` flags are refused without changing the existing page records or served content. A valid replacement updates the existing CMS page ID, keeps its creation time and navigation group, and changes the group only when explicitly requested. CMS deletion and update calls name the site's namespace.

The three regression tests pass 35 assertions through both the checkout resolver and installed product command. The existing site-product workflow suite passes; its optional reference MCP client was unavailable and recording import was explicitly skipped to avoid loading a model. The fixed resolver is installed, and the installed `bonfyre-site` is now a relative link to it. Previous binaries are retained for rollback. Live MCP receipt `85a840c4-9a47-4811-be71-7ccfc6fa73a2` confirms matching installed and checkout hashes. Evidence is retained in `packs/no-simulation/site-page-replacement-20261013/`.

This protects input refusals and page identity. Replacement still uses separate CMS operations, so a storage failure after input validation does not yet have an atomic rollback. The atlas and refreshed ExperienceMap corpus retain that limitation.


## Atomic page writes installed 2026-10-13

The storage-failure gap above is closed. WebAddress prepares the full page and its fragments, then calls native CMS `entry page-write` once. CMS validates the namespace and expected page ID under one transaction, applies the existing typed writes, and commits the page, fragments and operation history together. A replacement retains the page ID and navigation group. Creation and removal use the same transaction boundary.

Injected failures in fragment insertion, page deletion and operation-history insertion preserve the previous records. The served guide remains unchanged after a refused replacement or removal. Stale page identities, wrong namespaces, duplicate JSON keys, cross-page fragments, decoded NULs and oversized fields are refused. Concurrent same-page replacements serialize as complete generations; this does not compare content revisions.

Hooks are deferred until commit, and rejected writes emit no publication notifications. A notification delivery failure reports `committed:true` with `DELIVERY_FAILED`, so callers know the content was saved. An interrupted command without a commit receipt asks the operator to check the saved page. Hooks and notifications still have no durable outbox; a process failure after commit can interrupt delivery.

Both native owners are installed with previous binaries retained for rollback. The installed commands pass eight tests and 122 assertions. Full CMS, site-product and ExperienceMap suites pass; the site suite skips its unavailable optional MCP client and the explicitly disabled recording import. Atlas validation passes with 504 architectures and 442 interactions. The refreshed installed ExperienceMap resolves the CMS owner and its new page-write bridge without a checkout override.

Live MCP receipts `0707a8f6-e391-41f2-b306-f9557a2196ae` (CMS) and `bd07aa8c-cf8d-4984-8f09-02623cff4011` (WebAddress) match the checkout binary digests. Evidence, installation hashes and rollback locations are in `packs/no-simulation/site-page-atomic-20261013/`. The broader control-plane run reports 864 passing, 259 skipped and seven failed homepage assertions in files outside this change; those results are retained separately in `control-plane-witness.log`.


## Public URL review repaired 2026-10-13

Following the fallback homepage's “Prepare route” and “Review in Studio” buttons exposed a broken handoff. A plain pasted URL reached a handler that required an ExternalEstate occurrence ID, so the next page refused every URL entered through that form.

The native Studio handler now presents a URL review when no occurrence coordinate was supplied. It displays the escaped address and performs no fetch. Anonymous visitors can continue through sign-in with the full address preserved; read-only accounts see the review without a read control. Write-authorized accounts can submit the existing ForeignTwin observation action. The generated source identity derives from the exact URL. An explicitly supplied candidate coordinate, including an empty one, still follows the original exact-pair validation and cannot fall back to this path.

Sign-in now preserves bounded review continuations up to 4,095 bytes, including long addresses and their query parameters. A failed sign-in percent-encodes the continuation before returning it to the login page, so a nested ampersand does not split it into unrelated login parameters. The native observer test reads an isolated local HTTP source and verifies its retained body and receipt; no live external source was fetched by this test.

Four installed regression tests pass 42 assertions. Native authentication, A2A and ExperienceMap checks pass. The control-plane suite reports 871 passing and 259 skipped tests after stale homepage wording and CSS-selector assertions were reconciled with the current fallback template. These assertions now distinguish native territory identity from visual styling; they do not establish four distinct fallback layouts.

The browser witness opens the four declared native territory addresses, follows intake into Studio, and preserves the sign-in continuation at 1440 and 390 pixels. It runs the existing four-worker server configuration. Single-worker runs timed out when navigating between territory pages after the review; that concurrency behavior remains to be isolated. The public apex currently serves PageComposition, not this fallback template, and no public deployment of the new URL review is claimed.

The checked WebAddress binary is installed with its predecessor retained for rollback. MCP receipt `1f0b64d0-6d2e-46de-b9c2-24bca4466eae` matches its checkout digest. Evidence and screenshots are retained in `packs/no-simulation/home-contract-20261013/`. The architecture registry and installed ExperienceMap corpus carry the new handoff and its limits.


## Browser connection stalls fixed 2026-10-13

The single-worker browser timeout above came from accepting a socket and immediately blocking in `read()`. A browser's unused preconnection could occupy the only worker indefinitely. The same path dispatched some requests before their headers were complete. Seven direct socket tests run against the previous installed build reproduced six failures, including idle-connection starvation, premature dispatch and body text supplying a missing Host header.

WebAddress now polls a bounded pool of incomplete requests before dispatch. Each worker holds at most 64 connections, allocates buffers as bytes arrive, and preserves the existing 256 KiB whole-request limit. Idle connections expire after ten seconds; incomplete requests expire after thirty seconds. At the queue limit, the oldest incomplete connection is refused before another is admitted. Complete requests return to the existing synchronous handlers with blocking response sockets. The queue does not parallelize owner writes or cache authorization state.

Fifteen regression tests pass 135 assertions across transport readiness, URL review and atomic page replacement. The previously stalled four-territory browser flow passes twelve checks at desktop and phone widths using one worker. Native authentication, Matrix address, A2A, prefork concurrency and ExperienceMap suites pass. Control-plane validation remains at 871 passing and 259 skipped tests, and atlas validation passes.

The unrelated weekly-plan browser test reaches the native CMS save and is refused with both the preceding installed server and this build. Its passing request-size checks and failing save are retained in separate logs; this change does not claim that the weekly-plan workflow is complete. Response-write latency and long-running owner handlers also remain outside the readiness queue's bounds.

The checked binary is installed with the previous build retained for rollback. MCP receipt `7475c44c-e988-4b5b-85a1-a40da753877e` matches the checkout digest. Evidence, before/after socket results, screenshots and installation hashes are retained in `packs/no-simulation/http-readiness-20261013/`. The architecture registry and installed ExperienceMap corpus include the new transport contract.

### 2026-10-13: private planning saves preserve table text

The weekly preparation form reached the native CMS with literal tabs inside JSON strings, so the store rejected the complete plan as invalid JSON. The WebAddress string writer now uses the shared Lex JSON emitter and checks the escaped size before appending a value. Tabs, carriage returns, newlines, quotes, backslashes and UTF-8 survive the form-to-store round trip. Inputs whose escaping exceeds the carrier bound are refused before the store runs.

The real Chrome flow passed 41 checks against both the checkout and installed WebAddress: retained recipes become a complete private draft, the existing Desk lists it for its owner, other accounts and guests cannot see it, and source records stay unchanged. Auth surface and URL review checks also passed. The atlas was updated, exported and validated, and the ExperienceMap checks passed. Evidence and the installed-binary rollback receipt are in `packs/no-simulation/planning-save-20261013/`. This is a local native installation; existing public processes were not restarted.

### 2026-10-13: saved notes open without truncation

The note room copied saved content into a 32 KB display buffer and collapsed its line breaks. It now allocates the response from the selected CMS body and its possible HTML expansion. The text keeps its line breaks and wraps long values at phone width. If the complete response cannot be allocated, the request fails explicitly instead of presenting a partial note.

The expanded Recipe-to-CMS browser witness passes 47 checks against the installed WebAddress. It compares the reader text with the complete saved preparation plan, reads a roughly 64 KB quote-heavy field whose HTML exceeds 380 KB, verifies phone layout, and denies direct private-note reads by another account and a guest. The previous installed build fails both the whitespace and long-content checks. Native auth and ExperienceMap checks pass, and the atlas validates. Evidence is in `packs/no-simulation/note-readback-20261013/`; MCP receipt `9db29853-190a-4e43-9453-335c2a3addff` confirms the installed binary matches the checkout. The predecessor and corpus release remain available for rollback. This change has not restarted public services.

### 2026-10-13: command references use evidence from their compiled generation

Fabric's reference writer selected the newest complete event without checking its contract generation. A real native hash workload reproduced the problem: after a changed declaration was compiled, the new reference still displayed the previous generation's receipt.

The reference now requires the selected state's catalog binding, contract binding and contract to agree before rendering. Its last verified run must have matching generation metrics and an operator-execution receipt with a valid content digest. The receipt must agree with the event's operator, mission, node and output, name the selected contract and generation, and record both checks as passed. A damaged newest receipt does not hide an earlier valid run. Historical events remain retained. References show the full generation and artifact addresses.

The installed native Fabric passes 61 regression assertions, including actual hash execution and recompilation, stale bindings, invalid JSON, mismatched receipt fields, damaged digests and absent or stale metrics. All native core conformance tests pass. Atlas validation and ExperienceMap checks pass, and the installed corpus was refreshed. MCP receipt `29be51bd-5c0f-4fbd-afdf-a61570824b47` confirms the installed Fabric digest; the managed transcribe reference names generation `8244d56caf144889e133fe1d98d50c2d6dc575df2290fd505cacc9f2cbf1713d` and reports no verified run for that generation.

Evidence and rollback information are in `packs/no-simulation/reference-generation-20261013/`. This check establishes contract-generation agreement; it does not attest the executable currently installed or authorize publication of artifact excerpts. The existing excerpt publication-policy gap and generic service-family links remain open. No public reference pages were deployed in this change.

### 2026-10-13: reference excerpts require a publisher declaration

Fabric references now withhold artifact excerpts by default. A publisher can supply `--disclosure FILE` with schema `bonfyre.reference-disclosure.v1`, the exact operator and compiled contract generation, and InformationPolicy fields naming each permitted `bonfyre://artifact/<sha256>#excerpt`. Only a field whose audiences include `public` admits that artifact's excerpt. A `publish-local` event alone does not disclose its contents.

The declaration is trusted publisher input, not an authentication credential or publication grant. For example, after reviewing the exact artifact and replacing both placeholders with retained identities:

```json
{
  "schema": "bonfyre.reference-disclosure.v1",
  "operator": "command.hash",
  "contract_generation": "<64 lowercase hexadecimal characters from the selected contract>",
  "fields": [{
    "field_name": "bonfyre://artifact/<64 lowercase hexadecimal characters from the reviewed artifact>#excerpt",
    "audiences": ["public"]
  }]
}
```

```sh
~/.bonfyre/bin/bonfyre-fabric operator reference command.hash --disclosure reference-disclosure.json
```

Declarations must be regular files of at most 64 KiB. Unknown or duplicate keys, malformed JSON, duplicate audiences, mismatched scope and wildcard artifact fields are refused before output. The reference records the declaration digest. Admitted excerpts are read within the same file bound and checked against both the stored digest and artifact address; changed bytes are withheld. Existing text and path-redaction checks still apply.

Literal backticks in an excerpt now receive a longer surrounding code fence. WebAddress preserves that fence width through section import and HTML rendering, so embedded headings and script text remain code. UTF-8 validation uses the already-read, digest-verified bytes through the shared native validator; it does not reopen the artifact. The old builds reproduced both unintended disclosure and a split code example. Installed regression tests pass 149 Fabric assertions and 61 site assertions, including Unicode edge cases and real Chrome checks at 1440 and 390 pixels. Native core conformance, schema validation, atlas validation and ExperienceMap checks pass.

Both binaries are installed with rollback copies, and the installed ExperienceMap corpus was refreshed. MCP receipts `6a8030e6-d883-4eba-a55a-d0a005ef0018` and `dffca26f-f7ca-494a-a22c-8de63e2d347e` report the installed Fabric and WebAddress digests recorded in `packs/no-simulation/reference-disclosure-20261013/installation.json`. Full receipts, regression logs and browser screenshots are retained alongside that file. The broader control-plane suite passes 871 tests, with 259 skipped and no failures. Public services were not restarted or deployed. Generic service-family links and executable attestation remain separate gaps.

### 2026-10-13: compression savings measures one archive stream

REA's Ghidra analysis of installed Compress exposed an archive-read loop followed by another tar-to-zstd pass. The native savings command now feeds one tar stream into the existing libzstd dependency, counts its input and completed frame output, and discards compressed chunks. It avoids a second archive read, a zstd subprocess and a temporary compressed file. Level 19, checksums and one worker match the current CLI's frame construction. Builds without libzstd headers retain the existing fallback.

Five alternating comparisons preserved raw, archive-stream and compressed-byte counts. A synthetic 2,000-file directory improved from a median 2.481 seconds to 1.179 seconds. A 64 MiB repeated-block file stayed near 0.56 seconds, while reported maximum resident size fell from 230.7 MiB to 147.3 MiB. These are local fixture measurements, not a production-throughput claim.

The full Compress suite passes, including four installed stream tests with 23 assertions and a separate fallback-build check. Real tar and zstd establish the expected frame size; tests cover empty and larger streams, one archive invocation, absent subprocess compression, and failed producers. Atlas validation passes. An ExperienceMap definition-test timeout during benchmarking passed on an isolated rerun, and the installed corpus was refreshed. MCP receipt `ba96d472-d355-45f6-8251-6d6a947a277b` confirms the installed binary matches the tested checkout. Evidence, benchmark runs and rollback information are in `packs/no-simulation/compress-stream-20261013/`. Family archive creation and the directory's separate metadata walk remain unchanged.

### 2026-10-13: clip receipts hash large media without whole-file allocation

The earlier Creator discussion identified a whole-file read in clip export receipt hashing. That code was still present in the installed exporter. It now hashes through a reusable 1 MiB buffer using the existing crypto implementation, checks file metadata and total bytes, and refuses detected changes or read errors. Descriptors close on both success and failure. The source must still remain stable throughout the earlier render; a successful hash is not a snapshot of the full workflow.

Five alternating hash-stage measurements on a 256 MiB fixture preserved the independent SHA-256 digest. Median reported maximum resident size fell from 285.4 MiB to 32.8 MiB, and elapsed time including startup fell from 0.390 to 0.247 seconds. This does not measure overall rendering throughput.

Hash and export tests pass 50 assertions. The tests cover partial buffers, interrupted and failed reads, file truncation and replacement, and a real decoded portrait export whose source and output digests match `shasum`. The installed module passes the hash checks. MCP receipt `1556ad1f-0fa8-40a7-8197-12f5db1123b8` records a second real export through the installed native launcher. Atlas and ExperienceMap checks pass, and the managed corpus was refreshed. `packs/no-simulation/clip-hash-20261013/` retains the benchmark, synthetic preview, rendered video, receipts and module rollback information.

### 2026-10-13: public samples verified through the native handoff

Fresh-browser checks on the published work-samples site pass 37 catalogue and 26 planner checks. They cover edits, invalid values, persistence, stale-tab protection and phone layouts. A separate marked planner enquiry passes 12 handoff checks: the cloud retains its complete selection, native reconciliation prepares one lead and held message, and a repeated sync creates neither again. No email was sent. Independent cleanup readback confirms the test lead is handled, its message is abandoned and the exact remote test record is absent.

The root and six sample routes also pass a reusable browser audit for loaded resources, images, headings, known-route anchors and settled desktop/phone layouts. Training and document pages remain walk-throughs; their runtime workflows were not exercised by this audit.

The audit exposed a cleanup defect in the shared browser helper: completed protocol requests left timeout timers alive after Chrome had closed. Replies and failures now clear those timers, and connection closure rejects pending requests. Lifecycle tests pass across replies, protocol errors, caller errors and disconnection; the existing PDF round trip still passes. The final public page audit exits in 10.47 seconds. Evidence is in `packs/no-simulation/live-samples-20261013/`, including the old helper completing its work but failing to exit. Atlas validation and the final isolated ExperienceMap build pass, and the installed corpus was refreshed. No public site content or service deployment changed.

### 2026-10-13: browser navigation waits for the requested document

The shared browser helper accepted a retained load event from an earlier navigation. A local delayed-script fixture reproduced a second page returning while still loading, and an unsafe-port navigation returning success. Navigation now requires a fresh load event matching its frame and document loader. Fragment navigation requires a fresh frame-and-URL event; browser navigation errors and download responses reject.

The regression tests pass 12 assertions, including redirects, repeated fragment navigation and attachment failure. Existing lifecycle and PDF round-trip tests pass 18 assertions. All seven published sample routes pass desktop and phone audits with the corrected helper. Atlas validation and the full ExperienceMap build pass, and the installed corpus was refreshed. Evidence is in `packs/no-simulation/browser-navigation-20261013/`.

The attachment fixture produced both browser connection closure and `net::ERR_ABORTED`; both are rejected, while the separate `isDownload` response branch remains unexercised by this browser. Document load still does not establish application readiness or successful HTTP status. No public content or service deployment changed.

### 2026-10-13: native file hashing refuses partial reads

The shared file hasher treated a stream error as end-of-file and finalized a digest of whatever bytes it had read. Hash duplicated that loop in its file and dedup commands. The old primitive fails nine regression checks; the old command also accepts a directory as the empty-file digest.

The shared reader now refuses open, read and close errors, leaves an empty output and preserves the error. Hash uses it for file and duplicate reads, aborting duplicate reports when a file cannot be hashed. Its build now follows shared-library changes. The reader keeps its 8 KiB buffer; callers remain responsible for keeping inputs stable.

The library suite passes, including 18 reader checks. Hash passes 26 assertions across independent digest comparisons, actual file inputs and an injected partial-read variant. Native core conformance and 149 Fabric reference assertions pass. Hash and Fabric are installed with rollback copies; native tool receipts confirm their digests and the expected directory refusal. Atlas validation and ExperienceMap checks pass, and the installed corpus was refreshed. Evidence is in `packs/no-simulation/native-hash-20261013/`. Other statically linked consumers still need rebuilding to receive the shared correction.

### 2026-10-13: native file hashes use macOS's existing crypto implementation

The shared file reader now uses CommonCrypto on macOS while retaining its portable fallback and exported incremental API. It keeps the same 8 KiB buffer and error handling. The four rebuilt consumers import SHA-256 through their existing libSystem dependency.

Five alternating local runs reduced median hashing time for 128 MiB plus 17 bytes from 0.720 to 0.119 seconds. A complete raw intake of 64 MiB plus 17 bytes, including copying and both hashes, improved from 1.067 to 0.525 seconds. Independent digests agree in every run. These are synthetic local measurements that may benefit from filesystem caching.

Ingest also now honors source and normalized hash failures before writing ready manifests; the earlier binary falsely accepted an absent source. The library suite passes 18 reader checks on each backend, Hash passes 58 assertions, and Ingest passes 47. Native core conformance, 149 Fabric reference assertions and the full Compress suite pass. Hash, Fabric, Ingest and Compress are installed with rollback copies. Native tool receipts and independent readback verify the installed binaries, a synthetic intake and a four-passport registry.

Atlas validation and ExperienceMap checks pass, and the installed corpus was refreshed. Evidence and benchmark scripts are in `packs/no-simulation/native-hash-acceleration-20261013/`. Other static consumers still need rebuilding; the portable branch has not been executed on Linux in this change. Intake's broader write-error and atomic-publication gaps remain separate. No public service deployment changed.

### 2026-10-13: upstream Creator work reaches the local native surface

A GitHub review found the landed Creator compiler and mixer were absent from this checkout. Eight source and test files were imported from commit `1187f3736ebbdeba3b70edb39f04de15c752cd5c`, with exact provenance retained. This repository's CLI now exposes timeline compilation, revision-bound candidate edits and PCM invocation planning. Metadata reads are bounded, and planning neither reads media nor executes the mixer. The global `akai` command points to another checkout and was not changed.

`bonfyre-creator-mix` is now built and installed, reachable through native MCP. It mixes raw float32 stems in bounded blocks, preserves stem order and offers sparse silence without changing logical audio bytes. A six-second synthetic stereo demo matches an independent reference sample by sample. Dense and sparse files have the same 2,304,000 logical bytes; local allocation falls from 2,306,048 to 1,191,936 bytes for this fixture. The installed MCP render also matches exactly, with receipt `5801d660-f6f9-4cc2-a4f6-62823c8063b5` retained.

The browser preview decodes and plays the WAV and fits desktop and phone widths. The full Aurekai suite passes 73 tests, with one WASM check skipped for the absent wasm32/lld toolchain. Atlas validation and ExperienceMap checks pass, and the installed corpus was refreshed. Source provenance, native receipts, listening copy and screenshots are in `packs/no-simulation/creator-upstream-20261013/`.

The mixer reports materialized but unverified output; the synthetic driver supplies separate source and output verification. It does not supply media rights, codecs, effects, recording or a full editor. This preview is local; no public sample site was deployed.


### 2026-10-13: Creator renders through the shared kernel in the browser

The installed Zig 0.13.0 compiler builds the unchanged native mixer kernel into a 259-byte WASM artifact. Source and artifact hashes are pinned, and rebuilding produces identical bytes. The previous optional clang test now verifies the artifact without skipping. The browser adapter accepts bounded 1024-frame blocks, up to eight channels and 64 ordered stems, and rejects invalid bounds and nonfinite output. WASM memory is fixed at 128 KiB; caller-owned media and returned blocks use additional JavaScript memory.

An interactive local sample remixes three generated tones when gains change and plays the result through Web Audio. Chrome matches every byte of the native six-second stereo render: 576,000 float32 values, SHA-256 `068bb4b051fbe6de2fc1936cdcf6d31e5ea518d0e84a0d972075cf4f8448f26f`. The first observed browser render took 11.2 ms; this is one offline observation, not a real-time latency guarantee. Mute, reset, playback and stop pass, along with desktop and phone layouts and zero script exceptions.

All 77 Aurekai tests pass with no skips. New parity coverage compares native and WASM across one, two and eight channels with 64 overlapping stems and verifies refusal/recovery and fixed memory. Atlas and ExperienceMap checks pass; the installed corpus is `eee0a96ab77dae2e180d9d286fcda21eed693b8deb8882166f937dd30fbaa804`. Evidence, source hashes and screenshots are in `packs/no-simulation/creator-wasm-20261013/`. The native binary and public sites were not changed. This sample holds its media in memory and does not supply AudioWorklet integration, streaming decoding, effects or recording. Final available disk space was 5.14 GiB.


### 2026-10-13: model-file verification follows the accelerated shared hasher

The installed model manager still contained the old shared file hasher because its build did not depend on the static library. Its Makefile now rebuilds that dependency and links the archive explicitly. The rebuilt executable refuses a directory that the old binary accepted as the empty-file digest. Five alternating native verification runs on 128 MiB plus 17 synthetic bytes reduced median time from 0.718 to 0.127 seconds, about 5.6 times faster on this Mac. Every digest agrees with an independent reference, and the source bytes remain unchanged. No model was downloaded or executed; filesystem caching may affect these timings.

Verification also mishandled an unknown hash: manifest registration stores SQL NULL, but verification treated it as the string `(null)`. NULL and empty hashes now use the existing compute-and-store path. A failed database update can no longer report a stored hash. The installed binary passes nine file-verification checks, including tampering, absence, directory errors, unknown-hash persistence and uniqueness conflicts. Existing manifest and representation tests pass. Its SHA-256 is `10090fdf15cdcc497d1bfaedc46db07256db50d33869bc094716ae276863caf4`, independently read back through native MCP receipt `093386cd-2c4f-4438-92b9-3814cb68d08c`. The previous executable is retained for rollback.

The same dependency audit found stale Spines source pins for the shared SHA implementation and header. A pinned Zig rebuild produces exactly the existing 30,454-byte WASM artifact, so only those source pins changed. All 505 MCP tests pass with 6,788 assertions. Atlas validation and ExperienceMap checks pass, and the installed corpus was refreshed. Model evidence is in `packs/no-simulation/model-hash-20261013/`; Spines rebuild evidence is in `packs/no-simulation/spines-source-pin-20261013/`. Final available disk space was 5.16 GiB. Other statically linked consumers still need individual rebuild and verification.


### 2026-10-13: the public work-samples catalogue includes an audio mixer

The [audio workspace](https://aurekai-work-samples.pages.dev/audio/) is published through the existing ServiceSite CMS and Cloudflare project and linked from the home page. Visitors can adjust three generated-tone tracks, play and stop the mix, save levels in their browser and download a 16-bit WAV. Mixing uses the shared native C kernel in a module worker; newer gain requests replace stale queued results. Playback draws a cached waveform instead of scanning all samples on every animation frame.

Packaging verifies each source and the initial native output, then publishes only the selected source frames: 1,536,000 PCM bytes instead of 2,304,000. The worker verifies source hashes and the initial native digest before enabling playback. Audio modules and files share a content-addressed directory. Saves use the existing browser workbench store and reject stale-tab overwrites. Browser Back clears stale readiness and recreates the worker; this correction followed a failure in the first public witness.

The final route passes sixteen live browser checks; local tests add corrupted and absent source checks for eighteen total. A real downloaded file decodes as six-second, 48 kHz stereo PCM. All thirteen deployed page, style and audio-package files match the local build exactly. All eight public routes pass desktop/phone page audits with no script failures or server writes. Existing catalogue and planner browser workflows pass 57 and 28 local checks, and 30 focused tests pass 198 assertions. Atlas and ExperienceMap checks pass, and the local installed registry is refreshed.

Evidence and previous CMS/deployment state are retained in `packs/no-simulation/creator-site-20261013/`. The public sample uses only generated audio and stores levels on the visitor's browser. Imported media, effects, recording, account sync and a real-time AudioWorklet engine remain separate work. No outreach or live lead submission was made.

### October 15: Cloudflare bridge shared verification

Codex contributed independent edge, HTTP ingress and native queue recovery tests alongside the concurrent ChatGPT bridge implementation. The joint local suite passes 16 tests. The edge consumer now verifies the receipt before acknowledging a delivery. A fixed Project MCP `cloudflare-bridge` profile builds and tests the bridge; isolated HTTP MCP verification passed, while the active service still needs a safe reload and tool catalog refresh. No cloud deployment was performed. See `integrations/cloudflare/CODEX-REVIEW.md` for ownership and remaining work.

### October 15: Accounts and team access in the training workspace

The creator workspace now has separate URLs for each studio's requests, programs, team access, appearance and account access. Navigation is derived from the current native permissions; the routing helper accepts workspace and view definitions rather than assuming one client or workflow. Requests have search and personal/status views, and the account screen shows different access across studios. Headers and request descriptions occupy less space.

Scoped administrators can review and change an existing account's access to requests, programs or appearance. BonfyreAuth owns the list and mutation commands. It checks administrator authority inside the write transaction, bounds delegation by the administrator's expiry and refuses self-changes. The browser never chooses an arbitrary institution. Direct grant removal does not imply removal of inherited access.

Evidence: eight creator API tests, native team tests, the existing Auth tests, and 16 new full-browser checks in `packs/no-simulation/product-maturity-20261015/workspace/verification.json`. Desktop and phone screenshots are alongside that receipt. The server remains local; this work does not establish public deployment, invitations or browser account provisioning. `cmd/BonfyreAuth/TEAM-ACCESS-HANDOFF.md` records coordination and the exact native contract.


### October 15: Catalogue drafts become a navigable workspace

The public product workspace now separates products, saved drafts and submissions received for review. Editing has independent views for details, options and prices, search and collections, and the final change review. Store, workspace and editing view survive reload and browser navigation. Back between editing views preserves unsaved changes. Saved work stays scoped to its store and retained source; only a receipt for the exact saved version appears under Sent for review.

The existing intake and browser store remain the owners. The final public deployment is `4e2828c1-85b7-42ef-9fb7-d50d72fd6469`; all four checked page/module hashes match the build. Eighteen live workspace checks and all eight public route audits pass without submissions. The original catalogue workflow passes 59 local checks, including actual generated-worker receipt recovery. Focused tests pass 24 tests with 161 assertions; ControlPlane passes 871 with 259 skipped. Evidence is in `packs/no-simulation/product-maturity-20261015/catalogue/`. Merchant publication, shared editing and authenticated remote review status remain unconnected.


### October 15: Retail planning across spaces and alternatives

The live fixture planner has a project library and separate layout, fixture selection and comparison views. A visitor can keep multiple spaces, copy a plan into an independently saved alternative, compare exact fixture quantities and costs, review budget and layout questions, and download both source-bearing briefs together. Search, reloadable URLs and Back navigation make saved work accessible. The same plan contract, revision-checked browser store and native lead intake remain in use. A copy cannot inherit the original's review receipt.

The published deployment `892f07e6-1c23-4d72-9aba-140dac13ac14` passes 21 new browser checks on desktop and phone widths. The page and three planner modules match the build. The existing 28 browser checks pass locally, including stale-tab refusal, share links, printing, failed-send recovery and actual generated-worker receipts. Eleven focused tests pass with 78 assertions. Atlas validation and ExperienceMap checks pass, and the installed corpus is refreshed. Evidence is in `packs/no-simulation/product-maturity-20261015/planner/`. Comparisons use observed fixture prices; quotes, confirmed site clearances, procurement authority and shared account storage remain separate work.


### October 15: Local recording sessions in the public audio workspace

The audio workspace now accepts up to eight PCM16 or float32 WAV recordings, with 24 MB of encoded input, 32 MB of decoded PCM and a 30-second timeline. It supports mono and stereo recordings at one shared sample rate. Users can trim and place clips, inspect waveform envelopes, change levels, mute, listen, save the original recordings with their edits, and download a stereo WAV with source-bearing session notes. Source, timeline and export views have reloadable URLs. The native C/WASM mixer remains the rendering owner, working in bounded blocks inside the existing module worker.

Imported sessions use the existing browser workbench store and expected revisions. Source bytes are hashed again on restore. Refused imports preserve the earlier session, invalid trims block output, and stale tabs cannot overwrite newer work. Imported render requests carry a worker session reference so a replaced source set cannot silently receive an old editor's clips. Permission choices record the visitor's statement and reset for replacement recordings. The workspace uploads no media and grants no publication authority.

The final deployment is `198ed242-d813-424f-b756-58f833aca2ad`. Twenty-eight live browser checks pass, covering two sample rates, fractional source endings, an actual exported WAV checked against independent sample calculations, reload, conflicting saves, invalid input recovery, changed-source refusal and 320 to 1440 pixel layouts. The original sample mixer passes eighteen local checks, including exact native parity and missing/corrupt packaged sources. Ten focused tests pass with 56 assertions. Seventeen published page, package and preview-image hashes match the build. All eight public routes pass desktop and phone audits with no broken images, exceptions or submissions.

The homepage now shows actual planning-comparison and imported-audio previews, with audio also in the preview reel. Atlas and ExperienceMap checks pass; the installed corpus is `3f61e6421a61a2ae81bdbea2da03da186047f7fe32f198537235caaa9b742e11`. Evidence is in `packs/no-simulation/product-maturity-20261015/audio/`. Long recordings, streaming decoding, effects, live recording, multiple imported-session libraries, account sync and collaborative editing remain separate work.


### 2026-10-15 — catalogue interaction upgrade and frontend research

The public catalogue now opens a populated AG Grid Community 36.2.0 workspace. Product and option layouts share the retained catalogue and existing draft owner. A visitor can filter or search products and SKUs, edit titles and option fields, select products or options, preview exact bulk price adjustments or tags, apply changes, undo/redo, inspect source details, and save multiple product drafts in one revision-checked transaction. A stale draft aborts every write. Saved work continues into the existing draft review and native intake flow. The library is served from our own deployment with its license and checked npm provenance; no Enterprise features are used.

The interface uses the site's dark palette and opens directly into the working data. The frontend benchmark is Shopify's bulk editor and Airtable's record review: useful selection, editing, and inspection with little explanatory copy. Research sources and differentiated proposed interactions for all 30 programs are retained in `packs/no-simulation/product-maturity-20261015/frontend/interaction-research.tsv`. Those rows are design hypotheses; they do not represent implemented demos. Additional candidates include Konva for spatial editing, WaveSurfer for media, Tiptap for documents, React Flow for processes, and MapLibre for geographic work. React-based libraries remain candidates for appropriate product islands, not an instruction to rewrite every native surface. tldraw requires a production license and FullCalendar resource timelines are Premium; neither was installed or represented as a free production dependency.

Verification: catalogue model/review/grid tests pass 29 tests and 184 assertions; the new browser journey passes 29 local and 29 live checks, including exact option changes, undo/redo, atomic save, restart, stale-tab rejection, store switching, review continuation, and widths 320, 390 and 1440. The original catalogue browser regression passes all 59 checks, including generated-worker receipt recovery and native review contracts. ControlPlane passes 871 tests with 259 skipped and no failures. Atlas validation and ExperienceMap corpus checks pass. Deployment `c0e35176-1ff6-4207-95ae-467320fae4c6` matches all seven checked page/library/module assets. Evidence is retained under `packs/no-simulation/product-maturity-20261015/frontend/`.

This is a frontend building block, not completion of the 30-program goal. Shared authenticated editing, organization/role-specific journeys, merchant publication, and the remaining domain interfaces are still open. Nick explicitly raised the standard to a coherent product suite with deep actions, polished states, accessibility, keyboard operation and dependable continuation. The private Upper90 workroom investigation made no source changes; frontend work took priority after that correction. Claude was sent the revised scope and catalogue ownership; successful delivery had not established that Claude read or reviewed the work.

## 2026-10-15 — audio arrangement editor

The public audio workspace now opens into three labeled generated tracks on a shared timeline. Clips move and trim by pointer or keyboard; undo and redo restore the native render, the source waveform has a draggable trim selection, and playback starts at the chosen cursor. Track zoom and horizontal scrolling stay synchronized with the ruler. Precise numeric controls remain available for the selected track. The existing recording and export routes, original tone mixer, source fingerprints and revision-checked browser saves remain in use.

WaveSurfer 8.0.2 and its Regions plugin are self-hosted in the content-addressed audio package with their BSD-3-Clause license. The official npm archive was checked against its registry SHA-512 integrity before extraction; file hashes and source URL are retained in the vendor provenance file. The library receives validated source peaks and handles waveform selection. The existing native WASM worker produces the mixed PCM. A failed waveform module leaves the native controls usable, and Retry loads a fresh module URL without losing the session. No audio is uploaded.

The interaction reference is Descript's [timeline work](https://feedback.descript.com/changelog/the-new-timeline-is-on); the library integration follows [WaveSurfer's documentation](https://wavesurfer.xyz/docs/). The page has less introductory text, and the homepage now shows actual catalogue-grid and audio-arrangement screenshots with short action descriptions. This is an implemented audio interaction upgrade. Account synchronization, multiple saved session libraries, long recordings, effects, collaborative editing and the other demo programs remain open.

Verification: 12 focused tests pass with 69 assertions. The new browser journey passes 24 local and 24 live checks, including pointer movement and trims, keyboard movement and undo, source waveform resizing, zoom, synchronized scroll, cursor playback, restart, widths 320 to 1440, and waveform failure/retry. Every sample in the downloaded gesture-edited WAV matches an independent calculation from the retained source WAVs and clip placements. The existing imported-audio journey passes 28 checks and the original tone mixer passes 18. The public eight-route audit reports no broken images, overflow, script exceptions or writes. Atlas validation and ExperienceMap checks pass; the installed eight-file corpus is `c47c4fd2ab4fe18aa1687a4a891ab435bc1d88941cec3070b05cbf950b828779`.

Deployment `35f84626-3358-420a-888c-354985d25f66` is live at <https://aurekai-work-samples.pages.dev/audio/>. All 22 checked HTML, screenshot, module, source, kernel and license files match the published build. Evidence is retained in `packs/no-simulation/product-maturity-20261015/audio/`, including `local-arrangement/`, `live-arrangement/`, `arrangement-regression/`, `tone-regression/`, `public-arrangement-site/`, and `arrangement-deployment.json`. Claude received the file-ownership note and review handoff in its inbox; neither message has been reported as read. No commit was made.

## 2026-10-15 — spatial retail planner

The public retail planner now has a Konva canvas with snapped fixture dragging, Shift selection, selection boxes, group movement, alignment, distribution, duplication, rotation, keyboard movement, pan, zoom, undo and redo. A phone touch drag updates the same quarter-foot coordinates as desktop input. The editor exports a PNG of the current view. Quick-add controls sit beside the working layout, and the homepage preview shows the actual canvas. Introductory and repeated instructional text was shortened.

The new spatial modules project into the existing fixture-plan contract. They do not persist Konva scene JSON or introduce another project store. Duplicates receive new fixture keys and retain the exact supplier product and option identities; the existing assessment recomputes prices and layout questions. Group movement bounds one delta for the entire selection, preserving relative positions. Known footprints, dashed planning allowances and countertop markers remain distinct. The existing numeric/SVG editor remains usable if the optional canvas library fails, and Retry restores the canvas.

Konva 10.9.0 is self-hosted with its MIT license. Its official npm archive was checked against registry SHA-512 integrity; hashes and source URL are retained under `vendor/konva-10.9.0/`. The interaction research uses [Figma's design canvas](https://www.figma.com/design/) and [Konva's selection and transformation documentation](https://konvajs.org/docs/select_and_transform/Basic_demo.html). Fixture dimensions cannot be freely resized in this editor because they refer to retained products.

The first public check caught a rollout mismatch: the new page and a new module did not arrive together at the canonical address. The immutable deployment was intact, and a later read matched both files. The planner now places its modules, store adapter and vendor assets in one content-addressed package, keeping the former flat addresses available for already-open tabs. UI package versions do not change the catalogue digest or saved plan identities. The initial failed read and browser run remain in `spatial-initial-propagation.json` and `live-spatial/initial-propagation-failure.json`.

Verification: 14 focused tests pass with 93 assertions across the plan, project and spatial models, including package identity. The spatial journey passes 26 local and 26 public checks: pointer and touch movement, group offsets, alignment, exact duplicate costs, undo/redo, keyboard editing, pan/zoom, reload, browser Back, PNG export, refusal to delete all fixtures, source-bearing review, 320 to 1440 pixel layouts and library failure/retry. The original generated-worker intake journey passes 28 checks; the saved-project and comparison journey passes 21. Those regression runs include revision conflicts, shared copies, actual local intake, receipt recovery, source-generation isolation and multiple saved projects. No public intake was submitted by this pass.

Deployment `f9ad11eb-6f99-4730-a307-533444332535` is live at <https://aurekai-work-samples.pages.dev/red3-planner/>. All 13 checked public files match the build. Atlas validation and ExperienceMap checks pass; the installed eight-file corpus is `583c987746e4fc5ee768c8ce7874a0ae67cfbf5e22b208576890885956100089`. Evidence is under `packs/no-simulation/product-maturity-20261015/planner/`, including `local-spatial/`, `live-spatial/`, `spatial-project-regression/`, `spatial-intake-regression/`, `spatial-deployment.json` and `public-spatial-site/`.

A requested read-only Claude review could not start: the dispatcher returned `fabric is stopped`, and the existing `~/.bonfyre/operator/STOP` admission flag is present. The flag was not removed and no alternate dispatcher was launched. The existing Claude session received the implementation handoff in its inbox but has not reported reading it. Shared organization/account journeys, merchant approval, measured site geometry and the other demo programs remain separate unfinished work. No commit was made.

The final eight-route public audit passes with no visible broken images, layout overflow, script exceptions or writes. Its readiness check now waits for the catalogue, planner and audio workspaces to finish loading, and image checks inspect rendered elements. The initial audit had captured the catalogue during loading and counted 223 lazy images inside its hidden legacy editor as failures; that original result is retained in `public-spatial-site/initial-readiness-audit.json`. The corrected audit checks the populated grid and its visible images before taking screenshots.


## 2026-10-15 — training request queue and team directory

The authenticated creator workspace now has list and board views over the native request queue. Coaches can open a brief in a detail panel, accept it as their signed-in actor, prepare its routine, renew their assignment and inspect the delivered exercises. Search includes brief text and delivered routine names. Personal, unassigned, in-progress, expired and delivered views show current counts. Request selection, filters, sort order and layout are stored in the URL and survive reload and browser Back. Keyboard users can move between rows and open them with Enter. On phones, the selected request replaces the queue and offers Refresh and Queue controls above the brief. Expired or another coach's assignments expose no false recovery or edit action.

Team access now has name/email search, role filters, active/expired status and compact member rows. Counts explicitly cover loaded direct grants; pagination does not masquerade as a complete membership census. Adding an existing account is an expandable action. Review shows the current and proposed direct role. The account overview links directly to authorized studio tasks. Studio appearance still supplies the interface's colors and name.

The independent implementation review identified stale access reviews and expiration extension in native team mutation. The browser now sends the expected direct grant role and expiry. BonfyreAuth compares those fields inside its existing immediate write transaction and rejects a stale change with 409. Existing grant expiry is retained or shortened by the caller's own authority, never extended by a role edit. A null expectation requires no active direct grant; expired active rows count as present and revoked rows count as absent. Older callers may omit the expectation. This is a comparison of current fields, not an audit revision, so an intervening change that restores identical fields is not detected.

The interaction references are [Linear triage](https://linear.app/docs/triage) and [Clerk organization management](https://clerk.com/docs/guides/organizations/create-and-manage). The implementation uses the existing native Auth and WorkGraph owners. Clerk was not installed and no second identity or membership store was added. The small queue-view module retains presentation state only. Google Stitch design generation remains pending; these screens are implemented and browser-reviewed, not represented as Stitch output.

Verification: the new queue/directory browser journey passes 35 checks, including two studios, admin and reader roles, actual native claims, selected-request links, output search, reload/Back, keyboard selection, 320/390/1440px layouts, stale-review refusal and revocation. The prior workspace journey passes 16 checks. The creator-to-member journey passes 12 checks, including native delivery and member adoption. Queue and creator HTTP tests pass 11 tests with 89 assertions. The full native Auth test target passes institution grants, member administration, locked concurrency, expiration, actor closures and external account links. Atlas validation and the ExperienceMap corpus checks pass. The installed eight-file corpus is `a5712a3ab0d8aeb07360c4f8303700c210295d89bcee8aa1e444565e2f4a539d`.

Screenshots, verification and source hashes are retained in `packs/no-simulation/product-maturity-20261015/workspace/queue/`. This is a local authenticated workflow: the server remains bound to loopback and refuses public hosts. Public deployment, invitations, account provisioning, actor binding, inherited-membership inspection, scheduling, attendance and assignment recovery remain open. Any permission denial currently clears the frontend session view, even when other studio grants remain valid. The public Work Samples deployment was not changed by this slice. No commit was made. Claude received the ownership/review handoff in its inbox; delivery still does not establish that the peer read it. The separate native dispatcher remains stopped and was not bypassed.


## 2026-10-15 — acquisition brief workspace and route comparison

The Brief Studio now opens into an opportunity list and focused brief, checks, routes, source and recipient-preview views. Equipment, property, supplier and civic cases retain different proof requirements. Each case has its own memo, recipient, recommendation, budget, comparison mode and section selection. URLs retain the selected domain, case and view. The previous long introductory form has been replaced by the working views.

Route comparison shows a check-by-provider matrix, exact base/labor/travel costs and the current selected plan. Reviewers can exclude a provider, edit its price and quote reference, or add a route by choosing eligible checks. The same bounded solver supplies the comparison and recipient output; repeated memo input does not rerun the route search. Coverage remains a proposed method, and the count of locally reviewed checks is shown separately. Recorded-ready quotes need a reference; excluded offers remain in the workspace and are omitted from selected-route YAFF output.

Tiptap 3.32.0 supplies the memo's formatting, undo/redo and linked source insertion. Its bundle, license collection, package lock and provenance are self-hosted. The integration follows [Tiptap's plain-JavaScript documentation](https://tiptap.dev/docs/editor/getting-started/install/vanilla-javascript). The record-list and focused-review reference is [Airtable's record review](https://www.airtable.com/guides/collaborate/interface-designer-record-review). The small bounded coverage matrix uses native HTML. No AG Grid or identity provider was added to this repository.

A second agent reviewed and extended the core migration and export contract. Version-two editorial snapshots preserve legacy data while separating each opportunity's values. New imports do not copy another case's notes. Memo documents have bounded depth, node count and text size; only supported formatting and HTTPS links survive validation. Ordinary Markdown prose is escaped. Autosave compares the stored workspace revision in one IndexedDB transaction and preserves edits made during a pending save. A stale tab keeps its edits for current-brief export and requires explicit reload of the saved workspace. If the optional rich editor fails, the plain text field can still save; Retry retains those edits.

Verification: 20 brief-core tests and 7 existing ITAD/property room tests pass. The final code review also found overlong generated headings, overlong route IDs, discarded unfinished quotes, edited-route resurrection, and failed-load state mismatches. Candidate routes now validate before mutation, derived headings are bounded, unfinished quote forms require save or cancel before navigation, deleted editors reset, and workspace identity changes only after successful hydration. Failed conflict reload keeps the dirty state and unload protection. The browser journey passes 55 checks locally and repeats all 55 at the public preview, including formatted memo edits, undo/redo, source insertion, per-case budget and section isolation, quote changes, invalid budget, TSV import/refusal, new routes, reload, browser Back, two-tab conflicts, library failure/retry, print and every view at 320/390 pixels. No browser exceptions or remote writes were observed. All 14 published Brief Studio files match their local hashes. The publisher's preview-branch check passes 11 assertions. Atlas validation and the ExperienceMap corpus checks pass; the installed eight-file corpus is `2024e71fb1b3e21233a6ae448ed14c557ca1304bcd82aa8fee1816a848e2d536`.

Preview deployment `e8875e2b-6dc2-49cf-8f59-3ab119d8c9b6` is available at <https://brief-studio-upgrade.aurekai-work-samples.pages.dev/brief-studio/>. The existing ServiceSite publisher now accepts an explicit preview branch and refuses the production branch as a preview target. The production deployment stayed `f9ad11eb-6f99-4730-a307-533444332535`. Source changes live in the sibling `pages-procurement-memory` checkout; its GitHub Pages workflow needs a commit, so that canonical site remains unchanged. No commit was made. Evidence, screenshots, source patch, hashes and deployment receipt are retained in `packs/no-simulation/product-maturity-20261015/brief/`.

Drafts remain specific to the browser origin; the new preview does not import a visitor's GitHub-origin storage. Shared accounts and organization persistence, collaborative review, full workspace backup, native approval/dispatch and Google Stitch design generation remain open. This work advances program 17; it does not complete the thirty-program scope. Claude's interactive session received the source and integration handoff; message delivery is not confirmation that the peer read it. The stopped native dispatcher was not bypassed.


## 2026-10-15 — ITAD acquisition desk, manifest editing and valuation scenarios

The equipment demo now has separate inventory, valuation, lot comparison, verification, source and buyer-inquiry views. Its manifest uses AG Grid Community 36.2.0 for cell editing, sorting, search, condition filtering and multi-row selection. Bulk changes show each selected line's old and proposed values before applying; removal uses the same review step. A separate Open action exposes the row inspector without shifting the grid during double-click editing. Per-lot undo/redo restores the record and its review state without changing other lots.

New lots begin with empty inventory and no provider offers. Buyer identity, requirements, verification budget, mode and prepared status now belong to each lot. A versioned migration preserves existing settings. Valuation shows the cost allocation, target maximum purchase price and five recovery scenarios. Each unit's scenario value rounds to cents before aggregation; the reserve rounds upward and allowable acquisition proceeds round downward. Unpriced inventory has no numerical net or bid conclusion. The comparison and exported inquiry retain the basis of illustrative assumptions, including after a user imports a replacement manifest while retaining example costs.

The interaction references are [Shopify's bulk editor](https://help.shopify.com/en/manual/shopify-admin/productivity-tools/bulk-editing) and [AG Grid's cell editing contract](https://www.ag-grid.com/javascript-data-grid/cell-editing/). The pinned Community bundle, MIT license and original provenance are self-hosted; no paid grid module or map library was added. Native SVG supplies the bounded cost allocation. Google Stitch design generation remains pending.

All manifest mutations validate a separate candidate before changing the workspace. Existing IndexedDB revision storage remains the draft owner. Saves use immutable snapshots and compare the expected revision. A stale tab retains its current draft and offers export or explicit reload. Initial loading blocks mutation; failed reads and edits arriving during a delayed reload preserve the draft. Uncommitted grid and inspector edits retain unload protection. If the grid library fails, a list editor can modify and save the same rows; Retry loads the grid with those edits intact. Error notifications no longer intercept clicks on the fallback editor.

Verification: 12 new equipment-core tests, 7 existing operating-room tests and 20 Brief Studio tests pass. The browser journey passes 54 local checks and repeats all 54 on the exact public preview. It exercises real cell editing, bulk preview/apply/remove, undo/redo, filters, invalid quantities, entire-file import refusal, custom lots, exact valuation totals, independent settings, reload, source exports, startup and delayed-read guards, two-tab conflicts, fallback/retry, print and all six views at 320/390 pixels. No browser exceptions or remote writes were observed. All 13 published assets checked against the source match. Atlas validation and ExperienceMap corpus checks pass; the installed eight-file corpus is `a94c90bde7da8a1ec61a79351379ba00381cb5833bd41314dd9505b70974cccc`.

Preview deployment `e5f2d6ea-40f9-4086-9482-0e20ae6ab558` is live at <https://itad-desk-upgrade.aurekai-work-samples.pages.dev/itad-exchange/>. Work Samples production stayed `f9ad11eb-6f99-4730-a307-533444332535`. Source remains in the sibling `pages-procurement-memory` checkout; GitHub Pages still serves its committed version. No commit was made. Evidence, screenshots, source hashes, patch, public asset hashes and deployment receipts are retained under `packs/no-simulation/product-maturity-20261015/itad/`.

The account integration review identified the existing training session, actor-binding, institutional-grant and native revision owners as the reuse path. Public organization work still needs a same-origin authenticated API, explicit destination-scoped draft admission and cross-organization tests. The training server remains loopback-only. This pass does not add shared accounts, purchase approval, buyer assent, custody receipts, settlement or a full-workspace backup. The thirty-program scope remains active. Claude's interactive session received the source and account-boundary handoff; inbox delivery does not establish that the peer read it. The stopped native dispatcher was not bypassed.


## 2026-10-17 — MCP discovery, source reads and early write preflight

The shared Architecture Optimization Plan chat contains 408 nodes and 17 visible messages. Tool-result nodes include 125 connector results and 45 container results, but arguments and results are redacted. Those counts cannot establish distinct call counts, retries or latency. The visible account reports a decision-consumer patch that could not land and a later workspace-material patch that landed while its Atlas change remained outside write scope. These are historical reports pending current-source verification.

The existing Project MCP tools now support targeted inventory summaries, pages of results, related source reads in one call, exact whole-file hashes and conditional refresh. Discovery loads family and capability metadata once per request. Source batches accept at most sixteen ranges and share source/output budgets. Each file retains its own read check; the batch is not an atomic checkout snapshot.

The existing patch-preview tool also accepts paths before a patch exists. It reports hashes, dirty state, configured path restrictions and manual-write availability without creating an operator store or exposing approval material. Dirty-state Git reads are limited to the named paths. Existing apply authorization, policy, receipts and source checks remain authoritative. The delivery prompt and initialization contract describe this sequence.

Verification: 82 focused tests pass with 846 assertions, including actual loopback HTTP and gateway-forwarding checks. The read-only measurement records 217,426 response bytes for the 177-tool legacy inventory, 6,162 for its first twenty summary rows and 1,334 for a targeted CMS summary. Eight source reads use one request; unchanged refresh drops their combined response from 146,676 to 3,135 bytes. These are measured response sizes, not an end-to-end latency claim. Evidence is in packs/no-simulation/mcp-workflow-20261017/. The idle project MCP service was reloaded from PID 934 to 79957. Its authenticated endpoint now advertises and executes all four workflow changes. The plist and operator STOP state stayed byte-identical. No approval policy or production content changed. Verification through the ChatGPT Aurekai app returned UNAUTHORIZED and requests reauthentication; that connector path remains unverified.

Direct MCP readback also passes filtered inventory, a three-file source batch and path preflight. The current Books and Manifest sources already contain some repairs described as pending in the shared chat; the retained review records their hashes without claiming full patch equivalence. ControlPlane verification passes 871 tests with 259 skips and zero failures. Atlas validation and the refreshed ExperienceMap corpus pass.


## 2026-10-17 — Agent relay admission, independent review and compact task status

The existing relay checks the candidate against the Project patch owner before spending review calls. A clean structured review skips the repair pass. Failed executable tests allow one bounded repair in the same worktree; repeated failure stops before final review. Codex and Claude final reviews run independently against the same candidate and test evidence. The reviewed patch is checked again before application.

The broker routes Project operations directly to the configured Project MCP owner. Fabric status reports current admission and process-owned work separately from historical task states. Compact task status omits full prompts and results while retaining run counts, recent receipt references and a full-read continuation. A historical running task without current process ownership is identified for reconciliation without resuming or rewriting it.

Verification: 62 focused tests pass with 381 assertions, and 11 related estate/hook tests pass. The retained HTTP fixture uses authenticated requests, a real Git worktree, a real Bun test and the real guarded Project apply operation. Agent responses are injected deterministic fixtures. One submission produces five agent invocations and a completed application; compact status is 2,900 bytes versus 49,716 for the full result. This does not measure paid model latency or output quality.

The paused live broker was reloaded to PID 96330 and then PID 8869 after the final source check. Its Project endpoint is reachable; its separately configured mesh endpoint remains unavailable. All six durable tables and 52 receipt files match the before-reload snapshot. The policy and both STOP files are unchanged, and no live agent task was dispatched. The final constructor guard rejects non-boolean repair policy. A follow-up reload initially refused a non-quiescent socket check; a later idle check passed, the tested source hashes were verified, and the final guard is now loaded. Durable tables and receipts still match the original snapshot. Evidence is retained in `packs/no-simulation/mcp-workflow-20261017/`.

Isolated relays still start from the configured Git commit and can miss uncommitted or untracked source. The retained `relay-source-coverage.json` confirms all four current broker files are absent from the selected local HEAD. Source snapshot admission remains the next workflow gap. The ChatGPT app connector still needs its separate authenticated readback.


## 2026-10-17 — Public demo directory updates

Work Samples now links to the full GitHub directory from its navigation and sample cards. Its home introduction is shorter. The current ITAD and Brief Studio previews were already linked and remain intact. The canonical CMS record was updated through `site.mjs push` and published through `site.mjs publish`, deployment `b4040f63-4f8b-42c7-a46e-c4a589c276f6`. A build comparison finds seven changed HTML pages and no changed application modules or data files. All nine public pages pass the existing fresh-browser audit at 1440 and 390 pixels, with no broken images, overflow, browser exceptions or writes. The home page matches the published build byte for byte.

The GitHub showcase adds Sources and recurring programs, Retail space planner, Catalogue workspace and Audio arrangement. It now contains thirteen unique application links. Six additional published screenshots feed its existing carousel from the authored links. The older `explore.html#apps` list links directly to the current directory and Work Samples. Existing legacy links remain available. Changes were made against the current remote files with source checks; the differing local site and profile checkouts were not overwritten.

The first Pages run stopped on three directory assertions that still expected nine entries, two screenshots and no screenshot on the first card. Those expectations now cover the thirteen entries and eight images. The twelve directory tests pass locally; URL filtering, failed-image behavior, keyboard controls and plain-link fallback checks remain in place. HTTP checks passed for all thirteen demo destinations and eight screenshot URLs. GitHub commits are `20420af215996afc216be524df73b72ea52fae7c` and `9ef69c00cf69f1269f7f46ff378625dc6b8d7e93`. The follow-up deployment is live. Both GitHub pages match the current remote source byte for byte. Fresh 1440/390 browser checks confirm thirteen authored entries and thirteen derived carousel cards, the legacy-list continuation, no broken images, no overflow and no browser exceptions. GitHub Pages run `39070152184` completed successfully, including its local and public product/room and composed-workspace checks.

For later demo releases, update the canonical Work Samples CMS record and the current GitHub `site/index.html` authored directory together. The carousel derives from that directory. Keep walkthroughs and case studies labeled separately, and read back the exact public route before linking it. Evidence, source snapshots and deployment receipts are retained in `packs/no-simulation/demo-discovery-20261017/`.


## 2026-10-17 — Explicit current-source inputs for isolated relays

`implementation_relay` now accepts optional `source_files` entries containing a path and the whole-file SHA-256 returned by `project_read`. A null hash requires the path to be absent. The existing worktree owner admits up to 64 explicit files and 8 MiB total, records the request, captures the selected bytes and modes, and creates an isolated baseline over the configured Git commit. It preserves the main checkout's HEAD and staged index. Unselected paths stay at that Git base; callers must include the current dependencies needed for the task.

Wrong or stale hashes, symlink paths, ignored untracked inputs, oversized sets and cancellation stop before model calls. Preparation writes all admitted Git objects in one batch without clean filters, checks their identities against the captured bytes, and updates the isolated index in one batch. Repository attributes that would reinterpret those bytes refuse the baseline. Source and snapshot receipts retain the coverage and baseline identity. A partially prepared worktree requires reconciliation rather than being silently reseeded.

Tests and review use the pinned baseline. Supplied source edits do not appear as agent changes, and an agent committing inside its isolated worktree cannot hide its changes from the patch. The owner checks selected input hashes and modes again before application; detected changes retain the candidate for review. These are bounded observations, not an atomic snapshot of every repository dependency or a distributed transaction. Project policy still decides whether a dirty path may be changed. The relay prompt and tool description explain the input contract; nullable hashes are checked by the shared transport without coercing other JSON types.

Verification: 124 tests pass with 1,163 assertions across twelve owner and transport suites. Real HTTP fixtures cover the original Git-base route, the selected-current-source route and refusal under clean-only Project policy. With an explicit dirty-path policy in the temporary fixture, the relay reads a current value of 10, imports an untracked dependency, runs the real Bun test and applies only the value change to 11 through the real Project owner. The input snapshot stays out of the patch and the original index stays byte-identical. Agent responses are deterministic fixtures; no model-quality or paid-provider result is inferred. A 64-file fixture verifies one object batch and one index batch. Additional checks cover source changes during preparation and final review, binary and empty files, deletions, unsafe destinations, cancellation, receipt corruption and pinned diffs after an agent commit.

The idle broker was reloaded from PID 8869 to 29366. Its authenticated live catalogue now exposes the source-file schema while admission remains paused. All six durable tables and 52 receipt files are unchanged. Configuration, policy and STOP state are unchanged; no live task was submitted. Atlas validation and the ExperienceMap build pass. The installed eight-file corpus is `d3d788cf6ade4e7ede22171fb8fd88079269f6e27fa1e5d9e4746c44f9f32ad7`.

Evidence is in `packs/no-simulation/mcp-workflow-20261017/`: `relay-source-http-fixture.json`, `relay-source-tests.log`, `relay-source-input-hashes.json`, `relay-source-live.json`, both `relay-source-state-*` snapshots and `relay-source-corpus.json`. A live model-backed mission remains unverified. The operator remains stopped.

The subsequent ChatGPT app readback authenticated successfully and returned the current relay catalogue source with its whole-file hash. The app still exposes the earlier input schemas: a filtered inventory request returned all 180 tools, and connector validation removed the batch `files` argument before rejecting the missing `path`. Authentication has recovered; batched reads and targeted discovery remain unavailable through this app schema. `chatgpt-connector-readback.json` retains the bounded results without connector session metadata. The recursive overlay source also caches Project schemas at startup; this is a possible stale-catalogue layer, not proof of the running remote route. A refreshed end-to-end catalogue remains to be verified.

## 2026-10-17 — Delivery relay through the public Project owner

The public OAuth services point directly to Project MCP on port 3051. Project now forwards five existing Fabric operations: `fabric_status`, `implementation_relay`, `task_status`, `task_cancel` and `recent_receipts`. The binding imports the owner's schemas and uses its existing queue, source snapshots, tests, review and patch receipts. The delivery instructions describe one submission followed by compact status reads. Other Fabric task-launch operations are not added to this binding.

Project write admission and STOP still precede relay submission, and Fabric checks its own admission again. Cancellation remains available while stopped, with the Project write gate still required. A remote Fabric deployment requires its own configured credential; callers cannot select the endpoint. OAuth exposes this path only through the existing full administrative closure. Personal and institution sessions cannot read developer task history or submit work. Forwarded owner errors now retain a bounded, credential-redacted reason while preserving the uncertain-effect classification and prohibition on automatic retries.

Verification: 142 tests pass with 1,249 assertions across thirteen owner and transport suites. The separate OAuth institution-slice fixture passes. The HTTP delivery fixture now enters through Project, forwards to Fabric, runs real Git isolation and a Bun test, and applies through the real Project patch owner. Both Git-base and selected-current-source cases pass; the default clean-path policy still refuses the dirty candidate before review expense. Agent responses remain deterministic fixtures, with no paid model requests.

The live Project and both local OAuth entrypoints were refreshed after source/configuration checks. OAuth had no incoming connections or child processes; its remaining fixed upstream keepalive socket closed during reload. Project had no active connections or child processes when reloaded. The broker stayed running and paused. All six Fabric durable tables, 52 receipt files, service configuration and STOP controls are unchanged. Project lists 86 tools, including exact schemas for all five delivery operations. Live status, receipt and historical-task reads pass through the new binding; the compact task response is 958 bytes and still marks the unowned historical run for reconciliation. No live task was submitted.

Evidence is retained under `packs/no-simulation/mcp-workflow-20261017/` in the `project-fabric-*` files. The ChatGPT app still authenticates and reads the new bridge source after the service refresh, but its exposed tool catalogue remains the earlier version. Refreshing that catalogue is still necessary before the app can use batched reads, inventory filters or relay submission. The Atlas and installed ExperienceMap corpus include the new owner interaction.
