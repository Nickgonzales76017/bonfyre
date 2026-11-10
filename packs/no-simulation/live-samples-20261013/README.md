# Public samples and browser cleanup

The published [work samples](https://aurekai-work-samples.pages.dev/) were checked in fresh Chrome profiles on October 13, 2026.

| Scope | Result | Evidence |
| --- | --- | --- |
| Catalogue editing | 37 checks pass, including invalid values, save/reload, browser restart, stale tabs, store selection and phone layout | `catalogue/browser-verification.json` |
| Fixture planner | 26 checks pass, including layout edits, invalid dimensions, saved state, stale tabs, a fresh shared-link visitor, download and print | `planner/browser-verification.json` |
| Planner intake and native handoff | 12 checks pass, including cloud receipt readback and idempotent native reconciliation | `planner-handoff/receipt.json` |
| Seven public routes | Headings, loaded resources, images, known-route anchors and settled 1440/390-pixel layouts pass | `pages/browser-verification.json` |

The catalogue, planner and page-audit runs submitted no forms. A separate handoff witness submitted exactly one marked enquiry using `verification@example.invalid`. Native reconciliation retained one service lead and one held message; repeating it created neither again. No email, supplier contact, quote or order was issued.

Cleanup was independently read back after the witness: lead 14 is handled, message 139 is abandoned, and the exact remote test record is absent. The receipt is `lead:planner:2cb31f442a761c5cd38d08c92a32c26b85c3703a69b9b26cebdb207a877956a7`. See `planner-handoff/cleanup-readback.json`. No unrelated request was included in the scoped sync.

The training and document pages are walk-throughs. This audit does not establish a working public training session, document search, catalogue submission, external destination, download not exercised above, or physical-device connection.

## Reusable page audit

`components/BonfyreServiceSite/tests/public-samples-browser.mjs --live` inspects the root and every page in the existing site record. It records partial progress and distinguishes a failed or incomplete browser run from a complete result. Screenshots and network observations are retained with the result.

The initial audit measured a brief overflow during a desktop-to-phone CSS transition. `resize-timing.json` shows the layout settling back to the viewport width. The audit now waits for fonts and active CSS transitions before measuring, without stopping the continuous sample reel. No website CSS or public deployment changed.

## Shared browser helper

Successful DevTools replies previously left their timeout timers scheduled. The helper could finish its work, close Chrome and remove its profile while the caller process stayed alive until those deadlines. `session-completed-but-held.json` records `WORK_DONE` from the old helper followed by a six-second process timeout. Its normal request deadline was 45 seconds; the page audit used 120 seconds.

The shared helper now clears timers on replies, protocol errors and send failures. Connection closure and final cleanup reject pending requests and release their timers. The lifecycle witness passes six assertions across successful replies, protocol errors, caller errors and a browser disconnect with a pending command; it also checks profile removal. The old helper fails the process-exit witness. The existing PDF round-trip test passes 12 assertions with the changed helper.

The final seven-route audit completed and exited in 10.47 seconds (`pages-timing.log`). This is one observed audit duration, not a general browser-performance benchmark. The helper is used directly from the checkout by existing browser operations; no launcher binary or public service was replaced.

Atlas validation passes. An initial ExperienceMap definition test hit its short timeout during browser work; the later complete, isolated build passes. Both logs are retained, and the installed corpus was refreshed from the final atlas.
