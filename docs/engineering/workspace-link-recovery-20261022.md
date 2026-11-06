# Signed-in workspace and old-link recovery — October 22, 2026

## Failure reproduced

The public route could reach login while the signed-in workspace was unavailable. At inspection, the installed release was `d8b2d86be17aed532bf84cffb9fb6c75efe1b8c4e3ca1375e4c2730a72b0c25a`, but the root package head and workspace demand had advanced to `ebc9fc98b1b555bd5507344ec263df256249b485e4ca52a0ca2338603586626d`. That publication changed the pinned resolution without completing the account upgrade. The current account tool returned ready=false; native pack-workspace describe refused. This was independent of the earlier transient database error.

The user also retained links pinned to `c70d97e7004b69149941f1805f16d7f556792110b51977befc39bd663983e654`. Such links previously ended at a plain conflict even when the saved content was unchanged.

## Deployed repair

The existing binding `ws-bd08c6776460a11c6d1e970b01f53cc15b65a37a`, owned by user:19/self, is active at installation revision 24 with release `6f13a12c3a36da030f67b13c84a5c69ca763c82860745c2432707b837fb60b3b`. It preserves every newer production/Data checks/recovery file and folds in the exact-source reference reader. All 98 original saved records remain at revision one. Other bindings, account permissions and operator STOP were unchanged.

The ordinary `materializeAuthoredWorkspace` pipeline now composes the shared reference-reader materializer before returning its final package. Future ordinary builds therefore retain this reader rather than requiring a one-off repair afterward. This does not make non-atomic publication of changed global declarations safe; publication and matching installation still require reconciliation through the native lifecycle owner.

The WebAddress binary at `~/.bonfyre/releases/workspace-continuation-20261022-3HO0SH/bonfyre-web-address` is live. Only the service executable changed; existing service arguments, owner paths, assets and databases were retained. Old `_open` links now show an explicit current-editor continuation after rechecking current account access, exact record revision, whole-body digest and compatible view. Changed records, invalid digests and foreign accounts remain refused. No GET migrates, saves or installs anything.

## Acceptance and actual readback

The frozen candidate passed 31 tests and 6,711 assertions. The new `cmd/BonfyreWebAddress/tests/workspace_link_upgrade.test.mjs` reproduces declaration/installation divergence, upgrades through the native owner, exercises all three supplied historical links and follows the old link in Chrome into the saved setup and its complete retained contract. Other suites exercise phone source reading, source tampering, storage failures, account activity and session handoff.

Authenticated production workspace_list returned ready=true after deployment. workspace_views independently resolved the setup, QR/mobile and marketing records against the new release, with unchanged revision and content digest. Public HTTPS probes verified login continuation and private-read refusal. The browser interaction witnesses use isolated accounts; no successful production-browser click-through is claimed. Neither connected TinyFish profile currently records an Aurekai sign-in.

Receipts: acceptance `f17cc874-f84f-4486-bf7e-0028cbc6196a`; native installation `04dab527-b3b7-4803-9cd4-e15e43ff9729`; public service promotion `58973fd5-68f8-4d11-8fd0-6abacf457b7c`; final release and record verification `9df361e3-2991-4683-a849-ca805b391ba0`. Retained accepted/application-installed/promoted/final-readback packets are beside the deployed executable.

The three records remain recovered requirements, not implemented SHOUTOUT, RewardsOps, marketing or physical-world applications. This repair does not increase the underlying workflow completion count. Upper90, JRSS and AI Loss were not activated or operated. Canonical Atlas witness updates remain blocked by the protected architecture path; no bypass was attempted.
