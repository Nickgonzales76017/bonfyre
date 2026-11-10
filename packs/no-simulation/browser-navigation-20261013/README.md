# Browser navigation verification, 2026-10-13

The shared Chrome helper previously accepted any retained load event from the tab. A second navigation could finish while its document was still loading; a failed navigation could borrow the earlier successful load. `before.log` reproduces both failures with `chrome-session-before.mjs`.

`integrations/browser/capture/chrome-session.mjs` now enables lifecycle events and waits for a fresh load matching the requested frame and loader. Same-document navigation waits for a fresh frame-and-URL event. Navigation errors and download responses reject. Events received before the navigation response remain eligible, because Chrome may deliver them in that order.

`after.log` records two passing browser tests with 12 assertions: a delayed script on a reused tab, a redirect, repeated fragment navigation, an attachment failure and an unsafe-port refusal. The attachment produced connection closure in two exploratory runs and `net::ERR_ABORTED` in the final run. Both outcomes reject; this browser did not exercise the separate `isDownload` response branch. The exploratory logs are retained.

The existing lifecycle test passes six assertions across four subprocess cases; the PDF writer-to-reader round trip passes 12 assertions. `pages/browser-verification.json` and its screenshots verify all seven public sample routes at desktop and phone widths. That audit found no failed route, broken image or external write.

The protocol fields are recorded in `protocol-excerpt.json`, from the [official Chrome DevTools protocol source](https://github.com/ChromeDevTools/devtools-protocol/blob/master/json/browser_protocol.json). A document load does not establish asynchronous application readiness or successful HTTP status; callers still check those separately. Concurrent navigation calls on the same tab are not covered here.

The helper is used from the repository; no native binary or public site deployment is part of this change. Atlas export/validation and the ExperienceMap build logs are retained here. `corpus-installation.json` records the previous and refreshed installed corpus, with per-file digests.
