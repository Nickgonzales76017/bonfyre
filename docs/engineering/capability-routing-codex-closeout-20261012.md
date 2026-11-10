# Cross-owner capability routing: Codex reconciliation and MCP closeout

## Starting point

The native estate is already broad. The live architecture atlas readback on October 12 shows **504 architectures**, **440 interactions**, **11 views**, **264 implemented** and **129 measured** entries. Those numbers are registry populations, not 504 independent runnable programs. The native binary inventory read 178 families before exclusion of backup executable artifacts. The Capability registry had 134 entries and gained 43 installed entries from its native `index` operation, reaching **177**.

The operational problem is that those authorities do not reach the user objective together. `bonfyre-capability match` is a single-capability lexical ranking; it cannot by itself plan or certify an application operation. It returned generic Control/Upper90 entries for creator subscriptions and supplier purchase requests. The saved Finance and Pay entries described their behavior as `Usage:`, invented a zero-dollar price and advertised `layer` as a command. ExperienceMap’s installed binary returned `cannot open manifest.json` from MCP even though the checkout generated corpus exists.

## Codex checkpoint examined

The Codex conversation snapshot `Pasted text(20261012-040344).txt` finishes during fitness studio device/browser work. Its last complete Bun unit run passed **76 tests across 15 files / 509 expectations**. That includes program publication, source-versioned creator operations, actor-scoped requests, native WorkGraph claim/delivery, program authoring, source isolation, device import and a member-owned local history. These are real repository tests, not paid subscription or merchant settlement evidence.

After that passing run, Codex tightened TCX validation, heart-rate samples and calendar selection, and the device unit tests passed **6 cases / 46 expectations**. Its last logged browser run passed 12 checks, then failed the added `phone calendar action downloads dated training events` assertion. The current test (`components/BonfyreServiceSite/tests/training-devices-browser.test.mjs`, lines 34–36) first sets Puppeteer offline, reloads, adopts a 2027 plan, and expects a downloaded `training-plan.ics`. The current app may use Web Share (`navigator.canShare`/`navigator.share`) instead of the download fallback; and the test does not inspect the `#notice` error path or distinguish those branches. Exact root cause remains unverified. Check that the plan still contains an upcoming scheduled session, disable or stub Web Share to test the download route, test the share route separately, and check the generated `DTSTART` contents. The production date calculation in `training-plan.mjs` uses `startDay + (session.day-1) + 7*week`, so the fixture with `day:1` and `startDay:2027-01-10` correctly expects January 10. No edits to the Codex-owned ServiceSite components were made in this MCP lane.

### Existing fitness owners worth composing

- `architecture/apps/training.yaff` declares the active MemberTraining, TrainingCreatorAccess, TrainingCreatorRequest, TrainingProgramAuthoring/Publication, TrainingWeeklyPlans, StudioAppearance and DeviceConnections systems.
- `components/BonfyreServiceSite/training-requests.mjs` and native WorkGraph own requests/claims/delivery; `training-publishing.mjs` retains CMS releases, actor scopes and versioned distribution.
- `training-device-files.mjs`, `training-devices.mjs` and member training models keep TCX, sensor and calendar operations in the member's own state. These are not permissions to view another person's health data.
- Finance, Pay, Meter, subscriptions and merchant settlement remain separate business owners. Passing creator publication tests do not establish Paddle-equivalent recurring billing, tax remittance, payouts or provider settlements.

## Applied source changes in this lane

1. **Native Capability** (`cmd/BonfyreCapability/src/main.c`): common-language stopwords no longer rank unrelated capabilities, and `index` can reconcile old source=`probed` help fragments using existing `architecture/atlas.organs.json` owner descriptors. Reconciled rows carry `source=atlas-projection`, no invented executable command and an unmeasured cost. Curated/manual rows are not rewritten. `tests/discovery_test.sh` covers stale-probe repair, idempotence, source identity and compound supplier-owner search.
2. **MCP native boundary** (`integrations/mcp/project_native.mjs`): read-only exact binary fingerprinting now happens on *selected execution*, where the receipt reports both managed and checkout SHA-256 values. The broad inventory uses file metadata and labels equal-sized binaries `not_compared` instead of hashing hundreds of artifacts or claiming equality. Different-sized files are detected; missing or checkout-only binaries remain explicit. Backup executables are excluded from family discovery.
3. **ExperienceMap resource binding**: the existing MCP `native_tool_invoke` now passes `BONFYRE_EXPERIENCE_MAP_ROOT` to the installed ExperienceMap binary when the checkout contains `manifest.json` and `all_bindings.tsv` and no operator override exists. The receipt retains corpus provenance and `release_match=not_attested`; there is no new registry or service.
4. **Regression fixtures** in `integrations/mcp/tests/project_native.test.mjs` cover binary parity, drift, checkout-only owner, missing counterpart, unrelated backup artifacts, ExperienceMap corpus selection and explicit environment override.

Verification so far: `project_test_profile({profile:'cmd-make',target:'BonfyreCapability'})` passes the native discovery and owner-contract suites. `project_test_profile({profile:'node-syntax',paths:[project_native.mjs,project_native.test.mjs]})` passes on both sources. The new Bun MCP integration cases were added but have NOT been executed; do not mark them passing. The global `git diff --check` currently encounters a separate pre-existing `cmd/BonfyreKVCache/src/main.c` blank line at EOF, so it is not an unqualified success report.

### Runtime coordinate gap

The connected Aurekai MCP currently selects `/Users/nickgonzales/.bonfyre/bin/bonfyre-capability` and other managed binaries, not the recently built checkout objects. The live managed Capability registry now has 177 entries after reindex, but Finance/Pay still show the old `Usage:` entry and match ranking; the new C reconciliation only executes when the checkout binary is installed and its index run. The new JS MCP code also requires a controlled owner reload. Neither install nor reload has been performed. Do not claim these source improvements are already available to every connected assistant.

## Integration acceptance, ordered by leverage

**1. Finish the bounded MCP test and release boundary.** Execute `bun test integrations/mcp/tests/project_native.test.mjs` in the owning checkout and repair any fixture failures. Check that inventory remains responsive with 178 or more binaries; `native_tool_invoke` must report digest parity for exactly one selected family. Verify ExperienceMap `stats`, `find` and one `chain-def` through the current managed endpoint after controlled MCP reload. A result should name the corpus revision as well as the executing binary; no separate store.

**2. Use an objective to select a complete native chain.** Combine existing atlas app/work entries, ExperienceMap human/protocol chains, Capability realization and Fabric operator contract through an ephemeral read-only resolution. Keep raw owner references, required authority and evidence, callable/current binary and unknown prerequisites separate. The first fitness request should find creator program source, member request, WorkGraph, CMS publication and entitled member delivery; it should identify paid subscription/settlement as a separate unsatisfied business chain.

**3. Publish one reusable occurrence into its real carriers.** Reuse existing SurfaceIR, WebAddress/site, native CLI, MCP Apps-compatible views and Fabric operator reference to show the *same observed work result*. A creator's program release should render as a member-facing program and a creator WorkGraph/finance handoff without copying private training history across tenants. Correcting the source should invalidate the affected projection, not all unrelated work.

**4. Select the next external commercial action by a source-backed requirement.** Supplier WARP's Dallas/Stafford quotation amounts are now corrected in CMS, but the quotes are still drafts. North Dallas Gazette, 8848 Digital and other service proposals are not accepted contracts. Continue buyer-side procurement and accepted-service chains only from actual authorizations, observed prices, delivery and Pay/provider evidence. No automatic purchase or customer message was authorized.

**5. Measure reuse.** Count independently finished effects, source corrections propagated, next recipient readbacks and time to a usable result. Exclude synthetic requests, unaccepted offers, successful builds alone, invented model costs and unpaid invoices from realized revenue or customer completion.

### Named ExperienceMap reuse checked against the retained compiler output

The generated human-chain source (`cmd/BonfyreExperienceMap/generated/chains.jsonl`) already names **52** chain definitions. For the creator studio, inspect `identity-access` (actor/session/permission), `learning` (program/practice/progress), `creator-publish` (reviewed media/release/audience), and `enterprise-gtm` (quote/approval/payment/support/renewal) as separately owned composable segments. For the WARP freight case, `logistics` covers inventory, carrier quote, booking, delivery and settlement, while its distinct buyer-side authority still belongs to Finance/Commitment. These chain definitions are experience patterns; they cannot certify the native effect or grant the merchant permission. This check does not add another training-only flow or replace the training application.


Do not modify Codex's active fitness/browser component files from this lane while its current session is running. The operator is paused; the two staged GitHub profiles remain unpublished; no commit, deploy, server restart, purchase, send or payment occurred during this routing repair.

## Executed 2026-10-12 (later the same day)

`bun test integrations/mcp/tests/project_native.test.mjs` was run from the checkout: 27 tests pass, no fixture repairs needed. The checkout `bonfyre-capability` (discovery and owner-contract suites passing) was installed over the managed binary and `index` run: 177 known, 0 newly discovered, 51 legacy help probes refreshed from the atlas; `show pay` now carries the atlas description instead of `Usage:`. The previous binary is kept as `bonfyre-capability.before-reindex` in the rollback folder. `bonfyre-experience-map stats` answers through the managed binary when `BONFYRE_EXPERIENCE_MAP_ROOT` names the checkout's `generated` directory and still fails without it. Not done: items 2 to 5 of the acceptance list (objective-to-chain resolution, one occurrence in its real carriers, the next commercial action, reuse measurement). Match ranking remains lexical.

## Calendar handoff verified 2026-10-13

The device browser failure reproduced: Chrome reported `canShare` for the calendar file, but `share` rejected it with `NotAllowedError` and the page showed "Permission denied". The member app now downloads the calendar after a sharing failure and also provides a direct download button. Cancelling the share sheet does not download anything.

The existing device browser witness now passes 23 checks, including separate share, cancellation, refused-permission, failed-probe and offline-download paths. It verifies the exported session date and that all handoffs preserve member history and plan progress. The calendar fixture now uses a future date derived from the test day. Device and plan unit suites pass 11 tests with 95 assertions; the offline browser witness passes its ten checks, including process restart and update retention. Physical sensors, native companions and import into actual calendar apps remain unverified.

The [public training sample](https://aurekai-work-samples.pages.dev/training/#devices) now explains watch-file review, offline calendar handoff and sensor pairing limits. The change was saved through `site.mjs push` to native CMS service-site record 3 and published through its existing Pages owner. Desktop (1440 px) and mobile (390 px) browser readback found all three sections visible with no overflow or runtime errors; the deployed HTML matches the reviewed build byte for byte. Evidence: `packs/no-simulation/fitness-studio-20261013/sample-live.json` and adjacent screenshots. This publishes the sample description; the training application remains locally hosted.

## Native definition lookup repaired 2026-10-13

The native ExperienceMap command returned the wrong definition for nine of 52 workflow IDs and three of 108 protocol IDs. It searched entire JSON lines, so `chain-def community` returned `listen`, and `protocol-def websocket` returned `nostr`. The same failure was reproduced through the managed MCP connection before editing.

Definition lookup now reads exact top-level IDs through SQLite's JSON reader in memory. It preserves the original record, refuses ambiguous or malformed input before emitting a result, and handles escaped IDs without matching nested fields or description text. `url-def` remains a term search. The six native tests pass 420 assertions; the existing 27 MCP integration tests pass 165 assertions. Corpus-location checks also pass.

The repaired binary is installed, with its predecessor retained for rollback. A content-addressed copy of the six runtime corpus files now lives under the existing installed share path, so CLI use from `/tmp` no longer needs a checkout path or environment override. All 177 workflow, protocol and home definitions were read back from that installed copy successfully. Live MCP readback reports matching checkout and managed binary digests and returns the correct community and WebSocket definitions. Installation, corpus hashes and MCP receipts are retained in `packs/no-simulation/experience-map-20261013/`.

The objective-to-chain resolver remains open. The next observed discovery gap is that the current name-based compiler assigns training program and plan schemas only to `knowledge`; native `find training` exposes those retained records, but they are absent from the learning and creator-publication chains. That needs source-backed classification through the existing compiler before these references can support reliable cross-owner resolution.

## Training discovery classification repaired 2026-10-13

The atlas now declares workflow IDs for six training architectures. ExperienceMap joins their exact source paths to the existing CMS and surface records, so training programme and weekly-plan schemas appear in `learning` and `creator-publish`. Request records retain the `service-case` and `project` associations, including their WorkGraph interaction references. Access records retain `identity-access`. A neighbouring studio-style schema does not inherit those declarations.

The compiler verifies the atlas declaration digest and refuses unknown workflow IDs, duplicate declarations and missing declared source paths. Each classified record names the owning architecture and classification basis. Six architecture records also retain their source paths, witnesses, known gaps, interaction references and forbidden inferences. The new native `record-def` command reads an exact record ID, including `cms_type:training-program-release` and `architecture:training-creator-request`.

The complete ExperienceMap build passed. Its native lookup suite checked 747 compiled definitions and records with 1,572 assertions. Atlas validation passed with 504 architectures and 441 interactions; the atlas freshness tests passed. The rebuilt binary and seven-file corpus are installed with the prior binary and corpus retained for rollback. Live MCP returned the new schema classifications and request-to-WorkGraph references with matching managed and checkout binary digests. Standalone readback from `/private/tmp`, without a corpus environment override, returned the same records from the installed corpus. Evidence is in `packs/no-simulation/experience-map-20261013/classification/`.

This resolves the classification gap above. Objective-to-chain resolution remains open: these declarations identify candidates and their owners, but do not establish execution authority, paid entitlement or settlement.

## Native objective discovery installed 2026-10-13

`bonfyre-experience-map resolve 'member training creator program request publication'` now returns candidate owners and their declared connections in one read. The compiled owner graph retains all 504 exported architecture contracts, 441 interactions and 52 workflow definitions. Lexical ranking selects up to five anchors by default; expansion includes their immediate neighbours while retaining each interaction's direction. The response reports the atlas declaration digest, source paths, evidence references, known gaps and forbidden inferences. It uses in-memory SQLite and writes no routing database.

The training objective selects request, publication, creator access, member training and authoring as anchors. Their connections bring in 15 owners and 19 interactions, including WorkGraph, CMS and authentication. Standalone installed readback and live MCP agree. MCP receipt `3d7679ea-57c4-4a9e-9238-203e867ab43e` reports matching installed and checkout binary hashes. The prior binary and corpus remain available for rollback.

The complete ExperienceMap build passed, including six resolver tests with 93 assertions and the existing six native definition tests with 1,572 assertions. The resolver tests cover source correction, edge removal, incoming connections, unknown objectives, malformed graphs, duplicate keys and identities, and the retained training objective. The compiler also rejects a damaged owner graph during its freshness check. Atlas validation passed. Build, install and readback evidence is retained in `packs/no-simulation/experience-map-20261013/resolution/`.

This advances objective discovery; it does not finish the acceptance requirement for a complete native chain. Current callable binaries and Fabric contracts, actor/effect authority, evidence readback, provider admission, economic commitment and ordered fact flow remain unresolved in the response. The interface guide now documents the command in source; that guide change has not yet been republished to the public documentation site.

## Public discovery guide verified 2026-10-13

The [interface guide](https://docs.aurekai.work/interfaces-and-mcp) now includes the installed `resolve` command, exact-owner lookup and the unresolved execution checks. The page was saved through native `site page` into the documentation CMS and rendered by WebAddress. The local CMS was also missing the existing Building blocks page; its retained Markdown source is now registered there so the guide keeps that navigation link.

Publication used the existing `aurekai` Pages project. The prior deployment supplied the unchanged files, and the native-rendered guide replaced only `/interfaces-and-mcp.html`. Deployment `e4fc643c-4823-4a9d-9617-6e64356e54e5` succeeded. All 23 public files were read back: the guide matches the reviewed native output, and the other 22 files retain their previous bytes. Fresh browser checks passed at 1440 and 390 pixels with no page overflow or runtime errors; section-outline navigation and the Building blocks link work. Evidence, screenshots and the repeatable browser witness are in `packs/no-simulation/docs-discovery-20261013/`.

## Recovery readback 2026-10-18 UTC

The October 12 pending states above are historical. The October 13 sections already close the calendar handoff, exact definition lookup, training classification, installed corpus lookup and lexical objective discovery. The connected `training-mcp-smoke` profile now passes all 54 tests, including the current selected-binary, corpus-override, filtered discovery and HTTP source-workflow cases. The fixture environment correction is documented in `training-public-mcp-closeout-20261012.md`.

A fresh managed `resolve 'member training creator program request publication'` returned the intended request, publication, creator-access, member-training and authoring anchors in 105 ms. Native receipt `e4e2f731-7963-434b-9570-1f0b6d61c809` reports matching managed and checkout binary SHA-256 `69d837e66ea4520181a2035313025c42286163f5975ae2d4633bfce6ff85c9b6`. Its corpus binding still says `release_match=not_attested` and its result explicitly refuses execution authorization and proof of a complete executable chain. Do not reopen the old missing-resolver problem or promote lexical discovery into the unresolved ordered fact-flow and effect checks.
