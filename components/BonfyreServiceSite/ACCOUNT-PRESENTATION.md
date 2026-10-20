# Account-aware application presentation

This increment extracts the existing public gallery card renderer and adds a native-session presentation component. It does not add an account store or a new product registry. The public gallery's authored `a[data-demo]` records remain its input; the same createCard/mountCollection functions accept owner-supplied installed, setup and workflow records without replacing their editor destinations with demo links.

## Release boundary

The public gallery uses the shared preview implementation as `site/assets/showcase.js`, and upgrades its full list after the scoped stylesheet loads. Without that stylesheet or JavaScript, the original ordinary links remain. Actual described screenshots are reused. No fabricated screenshots or hidden installations are created. Search and visual/compact views use the same membership. The scripts do not mount heavy editor engines or call native account APIs on GitHub Pages.

The `account-session.mjs` and `account-navigation.mjs` files are published reusable source, **not an installed native fix**. Issue 56 remains open. During this work, the native connector's project_read, project_status and native_tool_inventory returned HTTP 502. Do not report these modules as present in the native WebAddress process merely because they are public assets.

## Required native caller migration

Re-read the current versions before integrating: `cmd/BonfyreWebAddress/src/main.c` (estate bar, page_emit_nav, login/signup handlers), `site/matrix/surfaces/account-native.html`, `site/matrix/surfaces/_kit.js`, `cmd/BonfyreWebAddress/src/pack_setup.h`, `cmd/BonfyreWebAddress/src/application_continuation.h` and `site/matrix/aurekai-bar.css`.

The native host should create one same-origin account-session observation, bind its lifecycle once, and mount each navigation consumer against it. Remove replaced duplicate identity probes. Keep links-load failure separate from identity failure. Text-only link mutations must match the owner's URL-encoded decoder. Preserve the existing Auth/CMS mutation checks and current publication permissions.

Mounting navigation requires an explicit root and any existing registration nodes that it must hide. Canonical profile and selected-destination links must come from the existing owner and include `userId` matching the currently verified account; stale actor links are ignored. Without a verified public-profile link, the component offers the existing `/account#presence` controls, labeled Profile & publications, not a guessed public-profile URL. The native producer still owes the actual profile-link binding.

Unknown/unavailable state is distinct from confirmed anonymity. The component uses the nested `user` response, bounded reads, no-store same-origin requests, current observations on history restoration, immutable identity summaries, and no persistent browser identity. Logout is sent once and reconciled by reading the owner. A failed provider, listener, or network request cannot certify logout. Owner permission checks remain required for every write.

Private profile/HTMX regions must independently be cleared and rebound on account changes. The navigation component alone does not control all nested private content. Native login/signup redirects still require the checked continuation helper; do not replace it with an unchecked next URL. Bind external session changes to the lifecycle's changed() notification; messages contain no identity or credential material.

Native discovery should supply exact release previewIdentity/previewRevision plus its current permitted href, state and destination label. joinPublishedPreview requires matching identity and a supplied revision match. It never rewrites href, credentials, roles or installation state. Obtain records through existing native owners, not by matching application titles or scraping unrelated pages. Remove the original global styles which flatten specialized grids and banners; do not add another blanket reset.

## Verification

`node --test scripts/tests/account-session.test.mjs` tests ephemeral observation and navigation paths with explicit test responses. `scripts/tests/account-ui-browser.mjs` uses a labeled loopback Auth-owner fixture, including actual browser navigation and CSS checks. That is not native Auth integration or the user's account. Its optional ACCOUNT_UI_ORIGIN mode is a read-only public gallery test only.

Required final native acceptance remains anonymous → actual login → exact profile/installed work; refresh/Back, account switch, link-provider failure, failed/logout/expiry, private-region clearing, native owner-derived cards, responsive and reduced-motion behavior. Prior instrument releases and all source-derived production/authoring targets remain independent unfinished obligations.
