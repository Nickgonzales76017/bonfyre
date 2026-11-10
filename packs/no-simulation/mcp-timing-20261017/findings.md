# MCP findings, 2026-10-17 (measured directly against project_node :3051)

- Latency, per tool (server side): project_status 128 ms, project_search 92-318 ms, project_diff 124 ms, architecture_atlas 66-79 ms, native_tool_inventory 404 ms, estate_recall 3,037 ms.
- Payload (before the project_read/inventory changes): native_tool_inventory 217 KB, operator_status 37 KB, tools/list 37 KB / 81 tools. project_read of a 14 KB file returned 55 KB: one JSON object per line, sent as both content text and structuredContent.
- bonfyre_moe_status (project_native.mjs moeStatus) calls /v1/models. The bonfyre-moe front treats that as work, tries to load a model and answers 503 when memory is short. GET /bonfyre/residency answers without loading (schema bonfyre.moe.residency.v2; also reports leases).
- project_test_profile bun-syntax with no paths is an error; ChatGPT hit gates like this.
- Products: bonfyre-books/-monitor/-manifest were Bun scripts that native_tool_invoke refused as interpreter launchers. They are now compiled binaries in ~/.bonfyre/bin (components/BonfyreDecision/build-products.sh). BonfyreBooks over MCP: 1.2 s end to end.
- ChatGPT connection: the gateway token endpoint returns expires_in 28800 with no refresh_token, while BonfyreAuth sessions last 7 days, so ChatGPT asks to reconnect every 8 h. A fix was proposed and denied by the permission check as an auth change; the decision is with Nick. Do not apply it without Nick.
- Gateway: 3-6 bonfyre-auth spawns plus one upper90 spawn per estate on every request, about 10 ms each.

## For the Work Samples gallery pass (2026-10-17)

- Nick, 2026-10-17: "we are done with the explainer demos masquerading as actual app demos".
- own-model.aurekai-work-samples.pages.dev and its "Your own model at work" card on the work-samples home (packs/aurekai-work-samples/site/content.json, assets/shots/own-model.jpg) are saved run output, not an app. Its manifest page still shows the old ITAD export with qty 1. Do not list it as an app demo; removing it is Nick's call. A duplicate Pages project, aurekai-own-model, also exists.
- Current and real: docs.aurekai.work/business-mentions, /books-on-your-machine and /lot-manifests (republished 2026-10-17).
- The Claude session that wrote this is not editing the gallery.

## Edit made by this session in integrations/mcp/project_native.mjs (2026-10-17 ~21:35 UTC)
- moeStatus() now reads GET /bonfyre/residency first and returns `residency` plus candidates as `models`. It falls back to /v1/models only when the server has no residency route (plain llama-server). project_node + transport_adversarial tests: 15 pass. Live only after the next :3051 reload. ChatGPT hit this 503 in a live test today and spent minutes looking for another way to read residency.

## Live ChatGPT test, 2026-10-17 21:28-21:33 UTC (Pro model, chat "Bonfyre MCP Investigation")
Task: three read-only questions (manifest held-count behaviour, model residency, atlas cannot_infer for cmd-books). Wall clock 4m 22s. Answers 1 and 3 were correct and cited source lines; answer 2 failed with HTTP 503 from bonfyre_moe_status (the live :3051 still runs the old moeStatus).
ChatGPT's own per-call timings:
- Before reconnect, calls waited 13m 30s to 24m 20s on "authentication retry required" (expired token, no refresh_token).
- After reconnect, each MCP call took 0.96-2.19 s end to end, against 3-400 ms measured at :3051. So about 1-2 s per call is ChatGPT/tunnel/gateway overhead, paid once per call (batching matters).
- native_tool_invoke (bonfyre-moe status): 70 s end to end, almost all of it waiting for the per-call approval prompt (non-read-only tool); the binary itself took 15 ms.
- 14 MCP calls plus 9 orchestration calls. Several of them went to finding another way to read residency, because no working residency tool exists.
- ChatGPT caches tool schemas, so newly added arguments are stripped until the app's actions are refreshed.

## After the :3051 reload (pid 42539), 21:36 UTC
ChatGPT, same chat, one question: bonfyre_moe_status called exactly once, status passed, 1.357 s end to end (the server reported 1 ms), 28 s total on Pro. It reported resident false, leases 0, and the last refusal verbatim. That's the question that failed after minutes of searching at 21:33.

## Final ChatGPT test after the refresh-token fix and reconnect, 22:30 UTC (Pro, new chat "Run Bonfyre Tests")
Four calls; total 47 s. bonfyre_moe_status 0.50 s (resident false, last refusal verbatim). fabric_status 4.50 s (paused, 0 active/queued, nothing dispatched). native_tool_invoke BonfyreBooks learn: 46.61 s end to end, of which 1.62 s native, the rest waiting for the approval prompt. native_tool_invoke BonfyreBooks booking: 1.93 s end to end, 1.04 s native. Results match the local run exactly: 1,447 booked, 53 held, 1,438 of 1,447 right.
Compared with the first run: 4 min 22 s and a failed residency answer, with 13-24 min auth stalls before that.

## Product operations on Project MCP (22:38 UTC, :3051 reloaded, 91 tools)
books_learn / books_profiles / books_book / monitor_mentions / manifest_lot (integrations/mcp/project_products.mjs). Local timing over MCP: books_learn on 6,000 lines 810 ms; books_book on 1,500 lines 568 ms (28 KB answer, held lines listed with reasons). ChatGPT test with the bank export attached (22:41): ChatGPT did not see the new tools (cached tool list) and fell back to native_tool_invoke --help, which cannot take an attachment. Stopped at 3 min. The app's tool list must be refreshed before ChatGPT can use them.

## After the tool-list refresh (23:28 UTC)
ChatGPT found books_profiles and books_book by itself from a plain request with the bank export attached. Its first books_book call with the whole 70 KB export was rejected: bonfyre_oauth_gateway.mjs capped every request body, MCP included, at 32 KB, and answered invalid_request. ChatGPT then split the file into batches under the cap and carried on (over 4 minutes). Fixed: /mcp bodies up to 8 MB, sign-in forms still 32 KB. Also fixed a bug that predates this work: /oauth/token was returned without await, so a body error escaped the handler and the caller got no answer. Gateways reloaded 23:3x UTC (pids 87203/87206). A public 100 KB unauthenticated /mcp POST now gets 401 instead of 400.

## Same request after the 8 MB limit and the totals fix (23:47 UTC)
One books_book call with the full 70 KB export. Total 2 min 59 s on Pro (was 4 min 7 s with batching). Answer: 1,447 booked, 53 held with suggestions and confidence (20-69%), 9 booked lines that disagree with the file's own categories called out, and booked line counts by account with dollar totals stated as unavailable (the file has no amounts). The earlier run had reported 0.00 totals.
Remaining time is ChatGPT Pro reasoning plus its own spreadsheet check of the attachment; the Bonfyre call itself is under a second.

## For the workspace-MCP session (messages to it were held, 2026-10-17)
- Mine: integrations/mcp/project_products.mjs (owner "products": books_learn, books_profiles, books_book, monitor_mentions, manifest_lot), wired in project_node.mjs and in the gateway readOnlyTools loop; tests/project_products.test.mjs; components/BonfyreDecision/*.
- Gateway changes of mine: the refresh_token grant; /mcp bodies up to 8 MB (MCP_BODY_BYTES), sign-in forms still 32 KB; /oauth/token now awaited.
- Reloads by me: :3051 at ~22:38 and ~23:45 UTC; gateways at ~23:35 UTC.
- ChatGPT keeps a cached tool list: new tools appear only after Nick refreshes the AurekaiBonfyre app.
- Books profiles belong to this machine, with no organisation binding yet. Your workspace/account ownership is the natural place to attach them.
