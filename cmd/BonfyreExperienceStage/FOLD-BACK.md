# Aurekai product adoption and fold-back

User direction: October 15, 2026. This extends the existing thirty-program objective; it does not replace `experience-maturities.yaff`, add a product registry, or certify any new implementation.

## Required destination

Every experience must become both a substantial standalone product and an adoptable part of the live Aurekai ecosystem. A person, household, group, community or organization must be able to enter through a demo, feed item, profile, shared object or live site; establish their identity and context; configure the workflow; use its frontend and backend; and produce a useful result without developer-side account seeding or a separate code fork.

The relevant territories are Aurekai WORK, LIFE, LIVE and SHOP. A territory is an entrance and presentation context, not a new copy of the account, source, work or commerce records. A separately branded customer site remains an addressed site with its own identity and disclosure rules.

The existing editors must retain their specialized interaction depth. A grid, canvas, timeline, publishing editor, training session and acquisition workbench are distinct products. Shared machinery must not flatten them into a generic dashboard, checklist or chat transcript.

## Existing owners to compose

- `BonfyreAuth` and the existing WebAddress signup/login/account boundary own session proof. Actor binding, institution admission, product roles and object authority remain distinct.
- Matrix, institution and relationship owners supply the actual subject and context. Self, household, group, community, public, market and institution must not be reduced to one organization identifier.
- PackGraph owns reusable behavior, sparse overlays, installation and activation. Forking, installation, activation and authorization are separate acts. Preserve the official, institution, community, household and scenario composition order.
- Existing `CapabilityProgram`, `ExperienceStage`, `ApplicationBinding` and `SemanticSlot` records describe selected operations and surfaces. Extend their owners instead of creating an application-installer database.
- `SitePresentation`, `SiteProgram`, `PresentationTemplate`, SurfaceScene and WebAddress compose addressed experiences. `cmd/BonfyreWebAddress/src/site_product.c` already coordinates site records through CMS and address owners; its local operator verbs are not automatically a public multi-tenant provisioning API.
- `components/BonfyreServiceSite` owns the current catalogue, planner, creator and training implementations. The sibling `pages-procurement-memory` checkout owns the acquisition editors. Preserve those owners and their existing tests while adding bindings.
- Domain owners retain domain objects. CMS, WorkGraph, source/frontier, media, commerce, finance and publication operations do not become one generic workspace-snapshot store.
- Publication, profile, community, feed, channel, media-library and delivery owners expose selected released results. An internal work occurrence is not automatically a public post.

These are composition references, not a new umbrella architecture. Consult each live atlas entry and its source before implementation. No parent maturity is inherited by a missing bridge.

## Adoption lifecycle

1. Discover or try: open a useful product directly, with an honest empty state or explicitly chosen example. Do not require adopting the whole platform to try one workflow.
2. Continue: preserve the intended workflow and recoverable draft across signup, login and failed authentication. Never carry credentials or private payloads in a return URL.
3. Choose context: self, an admitted household, group, community or organization. Creating a new context and joining an existing one have different authority requirements. Verify identity, membership and role through their owners.
4. Configure: select the released behavior, appearance, data connections, optional features and resource/budget limits. Preview the actual requested configuration and any unavailable capability.
5. Prepare: create destination-scoped domain state, required bindings, private storage references, routes and jobs through existing owners. Pin the selected product release and input/configuration identity. Do not copy demo permissions, people, approvals, payments or fulfilled work.
6. Activate: expose ready capabilities only after their owners have accepted the setup. Installation is recoverable, idempotent work; a partial failure resumes rather than duplicating the household, organization, job or site.
7. Use and collaborate: keep product-specific views, exact object/version addresses, private working state, explicit sharing and current role checks. A second authorized actor must reopen and continue the actual work.
8. Publish or exchange: deliberately select the result, representation, audience and destination. Release into the current profile/feed/channel/shop/community/site system through its owners; preserve the private source workspace.
9. Continue elsewhere: an eligible recipient opens, adopts, adapts, requests, books or purchases through the same operation and source identity. Receiving context and authority are checked again.
10. Maintain: upgrade the reusable base without losing local configuration; surface migration conflicts; support withdrawal, revoked access, backup/export and recovery without inventing external rollback or erasing valid history.

This lifecycle is a requirement, not a claim that all ten steps are implemented. The UI should describe the person's task and show concrete setup progress, not these internal compiler terms.

## What gets shared

Distinguish a live object reference, a released result, an independently editable copy, a reusable configuration and an executable workflow request. The sharing control must say which is being offered.

A reusable configuration carries the behavior version and permitted defaults, not its creator's customer records, credentials, private household data, approval history, provider entitlement or money. A recipient resolves new destination-specific bindings.

A live shared reference remains governed by current source access. An adopted copy has its own identity and retained lineage. Revocation must stop future protected reads, but it must not be presented as erasing already exported or lawfully copied bytes.

A household member can separately operate a business or participate in a community. Context switching must not migrate data, change its audience, discard an unsaved edit or import another context's spending authority.

## Public sites, feeds, profiles and templates

Keep public discovery, private operation and deliberately shared delivery as different projections over the same underlying objects. Public projections contain only released fields, never private records hidden by CSS or a client-side filter.

A feed item can expose an appropriate named action: open the source, watch a released clip, adopt a training program, customize an allowed layout, request a supplier quote, or open an offer. Its enabled actions come from the current source and receiving-context capability/authority checks.

Profiles are participation and publication entrances. A profile can show released work, offers, programs and permitted ways to collaborate without becoming a database clone of that person's private workspace.

Ghost-derived presentation belongs in the existing native presentation system. A reference candidate is not a measured, production-ready template. Appearance changes must preserve source identity, operation semantics, access and responsive interaction. Do not add a Ghost backend or force an interactive editor into a reading-page layout.

Public reading-page enhancement and active workbench editing are different frontend responsibilities. Native composition selects lawful material and operation apertures; the editor may maintain uncommitted working state and perform local canvas/audio/grid interaction, without becoming an authorization or publication owner.

Across the four top-level territories, use the existing identity system and an explicit destination-bound session/authorization exchange. Do not attempt a cookie shared across unrelated top-level domains, put bearer tokens in URLs, or treat a destination link as permission to enter it.

## First six product folds

| Product | Standalone depth to preserve and extend | Integration acceptance |
|---|---|---|
| Catalogue | Imports, products/options, bulk review, publishing canvas, channel differences, history and rollback | Adopt into a personal or merchant context; publish released catalogue material to profile/SHOP/site; continue a selected item in planning or a creative workflow; verify receiving-store changes through the merchant owner. |
| Retail planner | Multiple spaces, spatial manipulation, accurate constraints, alternatives and costing | A household or team configures a space; shared members review the same layout; selected products reach supplier/procurement work; accepted delivery updates the appropriate project or inventory, not an invented purchase. |
| Creator audio | Imported multitrack work, trims, automation, comparison, rights review and release | Reuse authorized source/library material; share a project with collaborators; release selected media to channels, feeds or a branded site; reuse a released cut in a campaign without treating it as campaign publication. |
| Training | Member sessions, plans, devices, offline continuation, creator program authoring and team controls | A person adopts a released program; a studio configures its branded frontend and backend; coaches and members retain different records and permissions; scheduling, enrollment and commercial membership use their respective owners. |
| Brief Studio | Multiple opportunities, editable source-linked writing, quotes, comparison, recipient views and recovery | A selected lot, project or event produces a brief from the same source objects; recipient-specific releases preserve disclosure; review and verification requests return outcomes into the originating work. |
| ITAD | Inventory, reviewed batch changes, valuation, verification, buyer comparison, custody and settlement | Personal disposal and organizational disposition share reusable behavior with different context; verified offers can enter SHOP/procurement; agreements, pickup, custody and settlement remain actual owner operations. |

These six are an initial cross-product acceptance set. All thirty declared programs remain in scope, including ForeignTwin import/clip, civic media, research, SEAR, MHS, process twins and enterprise workflows.

## Cross-product acceptance journeys

- Household: signup, create or join an authorized household, configure a room, bring in products, share a layout, request a comparison/brief, and continue a permitted procurement operation. Private address, budget and household records do not enter public discovery.
- Creator or studio: signup, configure the studio, import authorized material, author a program with media, publish a selected release, let a second person adopt it, and preserve that person's independent history and privacy.
- Business or cooperative: signup, establish the organization and roles, import an equipment lot, create a verification/brief workflow, release an offer, and carry accepted work into procurement and custody without rekeying the original objects.
- Community reuse: share a permitted configuration or released result; a second household/group adopts it with new bindings, independent private state and retained lineage. A later base upgrade does not replace their local work.
- Media and advertising: use a released clip or product selection to prepare creative material; publish or spend only through an actual admitted campaign owner. Creative export, impression, inquiry, order and settled payment remain different outcomes. Advertising owner availability is not established by this contract.

## Native integration gaps observed on October 15

The current source separates two carriers: native SiteProgram composition and independently deployed ServiceSite workbenches. Hosting or linking both does not connect their state or authority.

`acquisitionHandler` is tested with native Auth and an in-memory store; `acquisitionStore` is tested with native WorkGraph and a reduced fixture. Their separate success does not establish the actual editor-to-native persistence-to-collaborator journey.

`acquisition-store.mjs` currently keys a whole ITAD workspace by organization. Replacing a file extension would not create shared domain objects or source-scoped incremental reuse. Preserve native revision ownership and each editor's migration/recovery behavior when replacing avoidable materialization. No new canonical JSON store is authorized here.

WebAddress signup/login continuation is implemented locally in `src/auth_continuation.h` and `src/main.c`. It retains the selected path, query and fragment through both fallback forms and CMS-authored action fragments. The native form owner supplies the fallback behavior instead of the old browser redirect script. Forty native checks and thirty-three Chrome checks pass, including fresh accounts and independent native saved items. The public service has not been replaced; this does not configure a household, organization, product installation or shared editor.

The live atlas records incomplete scoped PackGraph/World enrichment in ActorPresentationCell, unresolved pack-demand scheduling, and incomplete measured Ghost profile realization. Do not describe an aggregate presentation class, declared dependency, installed binary or reference theme as a completed product binding.

## Release proof

Evaluate standalone depth and ecosystem adoption independently. Neither score substitutes for the other, and neither is a new registry of record.

For an integrated release, exercise a fresh account with no manual grants or seeded customer configuration; one household and one organization; two independent actors; desktop and phone; a second product and a second live territory where applicable; a real saved object and a useful output.

Include interrupted setup, repeated activation, expired session, revoked membership, unauthorized context, conflicting edits, unavailable provider, upgrade failure, withdrawal and private-to-public disclosure checks. Return to the original work with the actual downstream state.

A source edit, contract, test scaffold, successful build, directory link, draft pack or queued goal is not a delivered demo. A plan must not count an unexecuted operation as complete. Retain exact product source, release, binding, deployment and browser-witness identities when reporting progress.

## October 15 native continuation checkpoint

`make -C cmd/BonfyreWebAddress auth-continuation-witness` builds and tests the current native return-address and form logic against isolated Auth, CMS, Matrix and retained publication material. `auth-continuation-browser` is an explicit Chrome test using a local HTTPS carrier and three independent browser contexts. It launches no model and refuses when available memory is below eighteen percent.

The tested user path is a selected published item, account creation, a failed sign-in, successful sign-in back to the item, explicit private saving, and a second person's independent continuation from the same public address. Registration itself creates neither an institutional grant nor a saved item. Withdrawing the source prevents anonymous access while the existing saved-item owner preserves the permitted private copy.

The existing `external_account_surface_test.sh` and `page_saved_test.sh` regressions pass. The latter now checks the exact parsed retry destination and parameters instead of requiring one percent-encoding spelling.

Native binary SHA-256: `8213c3e6020a1ff0efb00c9aa5dac02ba05d064d9b045f45825fb279c123cc41`.

Browser witness: `cmd/BonfyreWebAddress/tests/artifacts/auth-continuation/1792085005031-8213c3e6020a/verification.tsv` with desktop and phone captures alongside it. Native execution receipt: `bbb3cb93-c636-45d8-8302-c9395405f611`; browser receipt: `6cef5a98-8965-4b94-84fe-2d834240ecbe`; existing regressions: `9b4f6d81-3654-4af5-88d7-0a5c7f05a43a`.

The current MCP patch policy refused changes under `components/BonfyreServiceSite` and `architecture/`. The training-release proposal and intended atlas fields are retained in this directory's `proposals/` folder for their authorized owners. Neither proposal is applied, and no atlas maturity has been raised. Complete client/server wiring and browser verification are still required for training sharing.

No public deployment, managed binary installation, live server restart, automatic-operator resume or end-to-end product setup is claimed. All thirty full product lifecycles remain open at their existing scope.

## October 15 native configuration adoption checkpoint

The checkout now has an implemented native PackGraph installation lifecycle. `cmd/BonfyreCMS/src/pack_lifecycle.h` mutates the existing `pack_installation` and `pack_activation` records through CMS, with relational revision and dependency-pin metadata in the same database. `cmd/BonfyreWebAddress/src/pack_resolution.h` resolves the selected published definitions and fingerprints their relevant fields. No product workspace snapshot, copied pack definition, credential or account grant is created by installation.

The native `/packs/setup` route renders public review, current context selection, installed/proposed dependency versions, explicit installation and activation, source-change refusal, deactivation, removal and recipient reuse links. It uses the native signup/login continuation and current account permissions. A fresh account can install personally without seeded institution grants. Institutional setup requires an existing exact institution grant; the tests provision those grants separately. Household, group, community and organization creation are not implemented by this change.

The scoped `/packs/closure?pack=ID&context=self` reader supplies retained versions to the existing PackGraph renderer. An authorized institution context is also supported. Private installations are excluded from the legacy public actor query, public installation counts and the old head-based ExternalEstate advertisement emitter. Private lock-aware forwarding beyond this scoped reader is still an integration gap.

Installation, pins and revision commit together under an immediate transaction. Repeating a just-committed operation preserves its identity. Competing reviewers cannot overwrite the same revision. An upstream head advance does not change an installed dependency version; an explicitly reviewed upgrade replaces the pins and requires activation again. A changed selected definition invalidates use of the old closure, while unrelated definitions do not. Missing dependencies, cycles, incompatible pins, ambiguous heads and authority-granting overlays refuse setup. Overlay ordering identity is included because the existing renderer orders same-layer operations by row ID.

Executed against isolated native stores with fictional public configurations: twenty-eight lifecycle checks, forty-one HTTP lifecycle checks, the public-advertisement isolation witness, forty existing auth-continuation checks, existing saved-item and account regressions, and two CMS command-dispatch tests. The CMS, WebAddress and ExternalEstate binaries compiled with warnings treated as errors. This is not a complete repository certification.

The attempt to add the new Chrome configuration-lifecycle test was blocked by the tool's safety check. The file was not applied or executed. The new interface has no fresh visual, phone or browser-interaction certification; the earlier auth-only Chrome witness does not certify it. No alternate mechanism was used to run the blocked test.

Current binary identities and executed receipt references are retained in `verification/pack-lifecycle-20261015.tsv`. The earlier continuation binary and browser references above remain historical evidence. The canonical atlas has not been updated because its path is outside the current patch authority; intended additions are retained in `proposals/pack-lifecycle-atlas.proposal.txt` without changing canonical maturity.

This checkpoint configures behavior references. It does not provision the six workbench backends, a customer site, resource hydration, provider accounts, paid operations or shared editor state. Bind those actual owners into this reviewed lifecycle, preserve their specialized interaction depth, and verify a useful cross-product result before claiming zero-to-hero product adoption.

No managed binary installation, live restart, public deployment, commit, outreach, payment or autonomous-operator resume occurred in this checkpoint.


## October 15 public configuration release

The native configuration lifecycle is deployed at `/packs/setup` on aurekai.work, aurekai.life, aurekai.live and aurekai.shop. The release installed the tested CMS binary, set an explicit `BONFYRE_CMS_BINARY` in the installed public-WebAddress launchd definition, and restarted that service. A consistent CMS backup, previous managed binaries and the prior installed service definition were retained before mutation. The repository deployment plist has not been changed.

`scripts/publish_experience_setups.mjs` projects selected entries from the existing thirty-program portfolio into the existing PackGraph owners. Six selected products and their four prerequisites produce ten configurations and seventy-one CMS rows in total. The final dry-run readback finds zero missing rows and refuses conflicting or duplicate identities. This is public workflow-configuration material, not ten completed customer application installations. The actual catalogue, planner, creator, training guide and acquisition preview links are declared through existing pack demand records.

The publication integration passed sixteen checks with isolated native Auth, CMS and WebAddress, including fresh personal setup, explicit activation, independent recipient state, accurate guide/demo labels and rejection of credential-bearing preview links. The existing forty-one native HTTP lifecycle checks and the ExperienceStage portfolio/build suites pass. The portfolio keeps its original 8+8+14 program classifications while exposing the two already-published acquisition previews as previews.

Work Samples was republished through its existing CMS-backed ServiceSite owner. Production deployment `7326617c-b2d9-4505-9c0e-fc802371ce2b` succeeded. Its homepage navigation includes Aurekai setup. Dedicated catalogue and planner headers do not inherit that menu; the proposed shared workspace navigation was refused because the builder is outside the allowed patch roots. No alternate file-write mechanism was used.

GitHub PR 47 merged as `fd80b9d32ea4e2e2a8a344fed8c9ccfdec285996`; the public carousel script contains its live setup link. Concurrent Spaces releases were preserved. The twelve directory checks pass. A subsequent release hit a non-fast-forward push after successful Spaces browser checks; PR 48 merged as `ef287ea1c8eded1432732cfd38d63b76a3517aa6` to add bounded reconciliation with complete revalidation before retry. Its nine real-Git regression tests passed locally. A merged workflow fix is not itself a passed production workflow run.

The new live account-creation browser-test file was blocked by a safety check and was not applied or executed. No new live verification accounts were created. Public readback verifies the routes, declared configuration, signup destinations, demo links, homepage navigation and deployed GitHub script; it does not certify public account creation, shared editors or backend provisioning. The thirty-program end-to-end objective remains open. No outreach, purchases, payments or autonomous-operator resume occurred.

GitHub Pages run `38810193140` subsequently completed successfully at 2026-10-15T21:35:36Z. Its directory and retention tests, local studio browser checks, generated-release retention, Pages deployment, and actual public studio/carousel verification all passed. The concurrent Spaces lane corrected the lazy-capture browser check in `cc049fdb5fb56ed365150d6e4d13cd80a448720b`. This browser result covers the published catalogue-to-room journey, not Aurekai account creation or native backend provisioning.

## October 15 connected catalogue and room release

Installation now commits actual destination-specific application bindings alongside installation and dependency pins. Activation opens the existing specialized catalogue, product inspection and room editors through native WebAddress. CMS owns the revision-checked draft records, atomic batches, replay receipts, tombstones and verified model bodies over bounded MessagePack. Native save failures do not fall back to a different IndexedDB record. Separately installed catalogue and room products share one destination's Spaces record family; different organizations, households and personal workspaces remain isolated. Private bindings are excluded from existing public site-program advertisements.

Explicit browser adoption now includes source-side selection and destination-side review, unchanged original records/revisions, unselected conflicting drafts, identical-content recognition and stale-review refusal. Source model files retain their bytes and metadata. Room plan and fixture identities are preserved, but the receiving copy clears the original submission receipt so it does not appear already sent. The final pinned package is `8552281c1ac7aa0b0fc53ae7e92bce49770c823c445f1623ffbe96a249fe8d96`, derived from reviewed Spaces commit `d184c72986f6443f027e6228f33da2fdfa079c7d`.

The final release passed 32 native HTTP/publication checks, 13 real two-origin adoption checks and 16 specialized-editor browser checks with isolated native accounts. The browser journey placed catalogue products in a room, reopened the same plan through a second authorized member and a separately installed room product, saved and reloaded another revision, and retained the supplier SKU/subtotal in the existing brief. The native owner passed 31 checks; existing 28 lifecycle and 41 setup HTTP regressions also passed during development. No production verification account was created. Live readback verified both connected setup pages on WORK, LIFE, LIVE and SHOP, not a private production account walkthrough.

The connected application coordinates are `experience.multi-merchant-catalogue.workspace` and `experience.retail-procurement.workspace`: two new releases and eighteen CMS rows derived from the existing portfolio, without rewriting the original workflow-only definitions or enrolling existing users. The live service now uses versioned CMS/WebAddress binaries, an explicit CMS binding and a pinned browser package. A consistent CMS backup and the prior installed service definition were retained. Release fingerprints and execution receipts are in `verification/connected-workspace-release-20261015.tsv`.

The repository guide `docs/experiences/connected-workspaces.md` and current experience index were updated without replacing concurrent guide work. Work Samples deployment `adffb8bd-a4db-443b-8555-186dea263002` published the CMS-authored `/connected-workspaces/` guide, its navigation and the integration-owned source consent controls; all were read back publicly. GitHub PR 49 merged as `6e97440416a16a33118cd85fb7011c19df26e59f`; its `/bonfyre/workspace-transfer/` page is publicly readable. The consent projection matches the tested integration source and leaves the retained Spaces editors/store unchanged. No denied component source patch was bypassed.

## October 15 household membership and audio follow-up

The public native service now uses a separate versioned household/audio release. CMS SHA-256 is `2a3d100d9a25c38b664efe224cc6b861faf085404762be48be9bc0040b9b1f2f`; WebAddress SHA-256 is `3756b5db39749ca468fef0b4b5002869bda74c57e3a009366cdc436b47db6926`. Its consistent CMS backup passed quick_check, the prior service definition is retained, and existing catalogue/room packages were not replaced.

Household access is no longer steward-only for installed applications. The current steward can review an existing account and grant read, write or configuration-admin access through `/households/access`. Only the steward manages membership. Revision and authority-generation checks prevent stale grants and prevent stewardship or publication cycling from reviving old access. Native draft transactions check access again. The 25 household checks and actual browser grant/revoke forms pass; no production membership was created.

`experience.native-creator-studio.workspace` installs the existing audio editor with family `creator-audio` and package `0049afa318eb159a43d05cdb5bf3eae637de43f09b03fbea926d2a74536e49c4`. Its seven public definition rows are present with zero missing rows on final readback. Twenty browser checks pass for original WAV buffers, shared arrangements, second-member reopening, stale-save refusal, decoded WAV export and phone layout. Eight adoption checks pass both with an isolated source and with the actual public Work Samples audio source. The destination remained an isolated native account, and adopted work reopened in the real waveform editor.

Work Samples deployment `37af402d-1f74-40eb-a575-9e0e9a7075d4` publishes the household/audio guide and updated source consent. Live audio consent bytes match the integration owner. Original source browser records remain unchanged; recipient recording permission resets to unreviewed and training records are excluded. GitHub's Spaces-only transfer helper was not changed in this continuation. Release receipts and boundaries are in `verification/household-audio-release-20261015.tsv`.

Training, Brief Studio, ITAD, creator-to-channel delivery, supplier orders and merchant publication remain separate adaptations. RetentionGC core and atlas patches were refused by the connector's writable-root policy; neither was bypassed or replaced with another registry or collector. `docs/engineering/remaining-workspace-adaptations-20261015.md` records the existing owners and required retention transaction, liveness, historical-reference and quota tests. No automatic upload deletion is enabled.

## October 16 public-to-business lifecycle requirements

The user extended the refactor beyond linking and installing existing demonstrations: useful logged-out public participation, optional guest work, first-time account and organization creation, meaningful business-rule configuration, customer-facing publication, people and invitation management, payments, fulfillment, ongoing operation and resource replacement must form one usable product lifecycle. A newcomer must be able to start at a normal Aurekai apex without seeing a demo and build the equivalent working operation from empty customer state.

`docs/engineering/public-value-business-lifecycle-20261016.md` records the recovered textbook/SHOUTOUT/setup-fleet basis, ten primary-source market benchmarks, concrete current source limitations and sixteen unexecuted release requirements. Public discovery, free membership, paid customer access and organizational authority remain different relationships. Configuration must change supported transaction and fulfillment behavior rather than only names, colors or source fixtures.

The current workspace fixture directly creates institution grants and a household; its editing checks are not evidence of self-service organizational bootstrap. The current public setup already groups Applications separately from Workflow configurations. Preserve these working pieces while replacing per-business publisher and material-binding branches through the existing product/program and surface contracts.

SHOUTOUT is a required generality test spanning venues, sponsors, creators, time/place, audience, media delivery and money. Do not reduce it to a generic post, creator directory, testimonial widget or another hard-coded demo. Preserve the broader original business fleet and specialized editors while connecting visitor, customer, participant and operator journeys.

This checkpoint applied research and acceptance documentation only. No new business onboarding, transaction model, payment integration, runtime build, live account, campaign, deployment or atlas maturity was created. Guides and samples must advance alongside tested implementations, not present these requirements as shipped functionality.
