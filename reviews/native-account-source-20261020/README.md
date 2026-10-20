# Native account and source-workbench recovery

Status: reviewed candidate with passing isolated tests; not installed or publicly deployed. Related release blocker: [Issue 56](https://github.com/Nickgonzales76017/bonfyre/issues/56).

This directory recovers the six candidate files left in Git tree `633ee6fb4d0367b038d692ca2d53cefda91dbf7f` when the previous ChatGPT turn failed. Commit `8988fff47063eef092ca92f851b321c1ca4b36a7` made that work reachable. The following commits add the preimage-checked patch builder, native/browser tests and the draft-restoration repair discovered by those tests.

The native Project preflight on October 20 still returned `path outside allowed roots` for the named `site/matrix` consumers, `manual_approval_required: true`, and `operator_paused: true`. Nick's authorization is explicit, but the local token-backed write gate has not accepted a patch. This review does not change that policy, find an approval secret, resume dispatch, write the protected files through a different command, or restart a service.

## What the candidate changes

The resulting patch targets the existing owners:

| Native consumer | Candidate behavior |
|---|---|
| `site/matrix/surfaces/account-native.html` | Identity, connections, profile controls and installed-tool sections have separate failure outcomes. Hidden private sections really disappear. Link writes use native-compatible form encoding. Logout reconciles the current session instead of claiming an unconfirmed revocation. |
| `site/matrix/surfaces/_kit.js` and its current `site/matrix/bonfyre-kit.js` projection | Remove permanent session caching. Coalesce only an in-flight read, parse the existing `user` envelope, bound the read, distinguish unavailable from anonymous, and render the resolved name as text. |
| `cmd/BonfyreWebAddress/src/account_navigation_markup.h` | Refresh the existing private native navigation when the account client reports an identity change. Keep the existing Auth actor / Matrix public LIFE profile resolution and checked continuation. |
| `site/matrix/surfaces/link.html` | Cancel or disregard obsolete searches; pin later pages to their source release; retain failed-page cursors for retry; compare up to twelve exact source identities across searches; keep destination controls reachable on phones; stop scanning the receipt directory on page opening. |
| `site/matrix/aurekai-bar.css` | Remove the suffix that forcibly flattens donor grids, backgrounds, imagery and typography. Keep the shared navigation rules and scope reduced-motion treatment to that navigation. |

The account form can survive a history/visibility recheck in bounded memory and restore only after the same native account is verified. A different account never receives the previous account's draft. This covers the four declared-account-link fields; it does not claim preservation of every form embedded in the profile authoring section.

The source comparison is transient presentation state. It does not own source admission, saved discovery criteria, acceptance verdicts, external credentials or publication authority. Its exported TSV contains exact references and a release coordinate, not a copied source database. The old persistent JSON distribution-plan sidecar is no longer written. Existing sidecars are not automatically deleted; future reviewed migration must decide how to preserve deliberate user choices.

The existing SourceCollection/World operation captures the selected source before awaiting anything and refuses a second concurrent submission. Its separate CMS writes and uncertain-response reconciliation remain unfinished. A comparison, source selection, public profile link or green upstream status is not a completed publication or provider connection.

## Reproduce without changing the native checkout

The pure source tests run with Node or Bun:

```sh
node --test reviews/native-account-source-20261020/source-cut.test.mjs
```

The integration tests use the existing native fixture, Bun and the Mac's installed Chrome/Puppeteer. They create disposable Auth/CMS/Matrix records, use a loopback server and close their browser/server afterward. They do not use a customer session or a provider credential.

```sh
BONFYRE_PROJECT_ROOT=/Users/nickgonzales/Developer/Bonfyre \
  bun test reviews/native-account-source-20261020/source-cut.test.mjs \
           reviews/native-account-source-20261020/native.test.mjs
```

The test intercepts only the candidate frontend responses and deliberately specified failures. Native account creation/login, link add/revoke and successful logout use actual isolated owners. Source-list inputs and the minimal donor-CSS page are explicitly synthetic. History lifecycle events are explicitly dispatched, not represented as a real browser-managed BFCache traversal.

To prepare the exact six-file patch, not apply it:

```sh
bun reviews/native-account-source-20261020/build-candidate.mjs \
  /Users/nickgonzales/Developer/Bonfyre
```

The builder verifies all six whole-file preimages first, writes only a fresh directory outside the project, and emits `candidate.patch`, before/candidate files and `manifest.tsv`. It refuses changed inputs. It is review tooling, not a production string-rewrite generator or another authoring runtime. Adoption means applying reviewed changes to the original consumers and retiring the temporary review assembly, not making the deployed site load this directory.

## Verification actually performed

The final local run passed **31 tests**, zero failures, 53 Bun expectations: 22 pure source-selection cases and nine native/browser tests. The source-browser test covers both 1440- and 390-pixel widths. It checks actual control visibility and whole-page width, and confirms no initial receipt read and no publication POST.

Native invocation receipt: `fc0a0361-5003-4b17-b887-8e68a56b1da5`.

Native WebAddress executable used by the isolated fixture:
`49d2d32ba92508efb6ac5d7e375c93aa5dee34ccae03c19daa6527ff6b5befff`.

The suite reproduced the old false-sign-out behavior, then verified the candidate against the same isolated identity owner. It also verified native URL-encoded add/revoke with `+` and `&`, unsuccessful and successful logout, same-account draft restoration and different-account isolation, unavailable identity without a signup prompt, safe display-name rendering, phone profile-menu linkage through explicit Auth/Matrix state, cross-query source comparison, and preservation of locally defined grid/image/font styles.

Earlier runs are retained as failures: `f0e63192-39f8-49af-8c1c-564a93080a32` and `fb87a448-0510-43e2-8eb5-acc040944834`. The first exposed actual form-draft loss. Two later test corrections made the hostile name compatible with the current native response parser and made the search wait tolerate the intentionally empty debounce interval; the expected final results were preserved.

`git apply --check` succeeds against the observed current checkout. Readback/applicability receipt: `17d49fa6-1f0b-4ccf-afb6-d957e647fd3a`. This is not write approval and does not apply the patch.

The generated page and asset postimages are recorded in [candidate-manifest.tsv](candidate-manifest.tsv). Account and source screenshots, verification details and the generated patch remain in the successful native review output:
`/var/folders/xn/01qdv38d6mx4jspqwjg95b000000gq/T/bonfyre-candidate-ObmrzZ/`.

The screenshot paths are local evidence, not public screenshot URLs. This test run did not establish a customer-account journey, a deployed fix, full gallery visual parity, native crawl execution, source publication, or completion of the Firecrawl/Exa recovery.

## Recover the larger source product, not a filter box

The October 19 source pair and the October 20 Codex log retain a larger target. The receiving contract should be carried with this repair into the existing ForeignTwin, SourceFrontier, ItemArchive, CMS/Studio, Pack/application, WorkGraph and SurfaceScene owners.

| Receiving area | What still needs to reach the product |
|---|---|
| Find / Exa-derived discovery | Maintained criteria, candidate membership and reasons, reviewable enrichment, accepted/rejected decisions preserved across reruns, useful saved views and comparison. Literal catalogue filtering is not semantic discovery or verification. |
| Understand / Firecrawl-derived inspection | Inspect source pages, structure, extracted fields and media beside original material; choose the actual read/crawl/map operation, test mappings, review scope/limits/refresh behavior, compare meaningful changes and follow nested source/item/version addresses. An observed endpoint or retained body is not a configured crawl. |
| Source profile and reverse uses | Resolve the source/publisher through its existing identity owner. Show representative permitted material, available operations, coverage, freshness, attribution, quota and public or authorized uses. Do not invent a profile URL from a label or leak private-use counts. |
| Prepared production | Bind the selected sources into the existing installed Production Studio and domain editors. Keep source, behavior and appearance separately inspectable. Populate receiving work instead of opening an empty editor or requiring export/import. |
| Batch and granular work | Defaults, effective values, explicit precedence, overrides, locks, reset, exact affected selection, real previews and independently continuing outputs. Independent transformation, many-to-one assembly and coupled resource allocation have different semantics. |
| Distribute and maintain | Destination-specific versions, reviewed release, separately observed delivery, changes, corrections and withdrawal. Preserve partial results and reconcile uncertain external responses before retrying. |
| Presentation and account continuity | Same reviewed product/release imagery in the demo, full list, setup and installed shelf, with action labels determined by the actual installation and role. Preserve selected subject, parts, draft, variant and return position through login, profile inspection, expansion and destination adoption. |

These requirements are source-derived receiving obligations, not new owners or newly completed capabilities. The original lists remain independent: thirty product programs, 56 historical list groups, 41 surface patterns, 60 actor presentation cells, Ghost references, source families, 388 skill paths and operation-specific platform integrations. One source workbench cannot stand in for all of them.

Source-reading entry points already retained on the native machine:
- `docs/engineering/aurekai-experience-cohesion-20261015.md`: "Complete productions inside feeds and addressed details", "Deep source reading: production relationships in the October 19 pair", full authoring and historical recovery sections.
- `docs/engineering/handoff-backfill-20261019/README.md` and `skills-conversion-contract.md`: original scope paths and the full-production overlay, not a substitute for the original texts.
- User files `Pasted text(20261019-194002).txt`, `Pasted text (2)(20261019-194114).txt`, and `Pasted text(20261020-155823).txt`: evolving implementation reports and later requirements must remain distinguishable.

A corresponding source change can affect a caption, selected item, report cell, product estimate, publication or operating plan without invalidating unrelated work. The selected source version, contextual inclusion and receiving operation must remain identifiable. Cache by what an operation consumes; compare its output before propagating further. Rights and account changes require independent admission checks even when content bytes are unchanged.

## Native adoption and release requirements still open

Do not merge this review directory and call Issue 56 fixed. Re-read the native preimages and obtain the actual local patch admission. Integrate the changes into the original consumers, update the `foreign-link-desk`/shared-surface contracts and replace obsolete string-presence tests with behavioral ones. The current `link_workspace_contract.mjs` still requires tokens from the retired JSON plan export; its old expectation must be reconciled, not skipped.

Review remaining lifecycle cases: nested profile scripts and unsaved profile forms across identity changes, permission loss during processing, exact once-only behavior/reconciliation of the two CMS publication steps, bounded server-side receipt lookup, selected source-version correction, and the complete source-to-installed-production handoff. The scoped candidate tests do not discharge those obligations.

Run the existing account-navigation, Auth, workspace, source-review and world-material suites on the exact candidate native build. Preserve the installed six bindings and current editor versions unless a separately reviewed upgrade requires changing them. Code rollback must not restore old user records. Then promote through the existing release path and verify the actual anonymous/login/profile/installed-work and source-processing journeys on the supported live territories, including mobile, keyboard, reduced motion and current account changes. Update release-associated previews, app lists and guides from the verified release, not from this review's test counts.
