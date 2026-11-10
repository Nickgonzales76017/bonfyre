# Browser checks of the 2026-10-17 builds (Chrome, real clicks)

## Financial review MCP App: fixed
- The built page (integrations/mcp/dist/workbook-review.html) threw two `SyntaxError: missing ) after argument list` on load, so it never connected to a host. Cause: components/BonfyreServiceSite/workbook-mcp-build.mjs inserted the bundles with `template.replace(marker, string)`, and `$'`, `` $` ``, `$&` sequences in the minified AG Grid and app code were treated as substitution patterns.
- Fixed with a function replacement; dist rebuilt (2,480,321 bytes, sha256 6dd826b3...). In Chrome the page now loads with no console errors, renders the grid, and says "Could not connect to the host: Method not found" when opened outside a host.
- Added a test in tests/workbook-mcp.test.mjs, "every inline script in the built financial review page parses". It fails on the old line and passes on the fix. All 10 tests in the file pass. The existing 9 never executed the page, which is why they passed on the broken build.
- Still unverified: rendering inside ChatGPT. That needs the gateway reload, then an app refresh by Nick.

## Catalogue Releases (packs/no-simulation/catalogue-release-20261017/public, served locally)
Archive note, 2026-10-23: this historical generated build now lives in `~/.bonfyre/archive/retired/catalogue-release-output-20261023.tar.zst`, with restore verification and a per-file SHA-256 manifest. The observations below still refer to those historical bytes. `sources.json`, `build.log` and `tests.log` remain in the original evidence directory. See `docs/engineering/mjs-retirement-20261022/PLAN.md` for restoration and current-source regeneration; a new build does not reproduce this dated browser observation.

Desktop 1456 px, real clicks, no console errors: select one product, "Adjust price by %" -5, the preview shows 16 option prices 199.00 -> 189.05 with "Store unchanged", apply to drafts, Review changes, save the draft, Releases, name it, stage it ("1 products · 16 changes · 0 affected rooms"), room impact "0 saved rooms checked", review note, Finish review, status "Review complete". Merchant publication is honestly stated as not connected.
Problems:
1. Withdrawn. The phone findings came from headless Chrome screenshots, which don't render below about 500 px and crop the result. Measured inside a real 390 px frame, the document is exactly 390 px wide; only the grid's own horizontal scroll and the scrollable tab bar are wider, as they should be. `?workspace=releases` does open Releases.
2. (withdrawn with 1)
3. "Stage selected drafts" is disabled with nothing selected (correct), but looks almost the same as enabled.
4. Copy: "Save 1 product drafts", "1 products · 16 changes", "1 product drafts saved" all need singular forms.
5. Product rows can't have their price edited directly; Enter then cmd+a selected all 223 rows. Price changes go through bulk adjust or the option rows. Worth a hint in the UI.
6. Mouse-wheel over the changes table scrolls only the table, so Finish review (below it) is hard to reach on a trackpad.

## Blocking the Work Samples build (2026-10-17, from the other Claude session)
- components/BonfyreServiceSite/planner-material.mjs lists 'planner-source-review.mjs' in its file list, but that file doesn't exist, so `site.mjs build packs/aurekai-work-samples/site` fails with ENOENT. The new shipment check page (sections/shipments.mjs, shipment-*.mjs, content.json archive page) is ready to publish behind it.
- The live web-address now runs ~/.bonfyre/static-search-release-20261017 (rollback: account-workspace-release-20261017-787c5456cc61, plist in service.before.plist). It adds /_search per CMS site; build any future release from the repo, which includes it.
