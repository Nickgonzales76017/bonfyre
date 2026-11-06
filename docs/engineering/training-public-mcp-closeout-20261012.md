# Fitness: one approved programme across application and MCP

## Source-backed existing execution

Codex's completed `packs/no-simulation/fitness-studio-20261011/request-browser-verification.json` shows an HTTP-submitted member request reaching native WorkGraph, a scoped creator queue, claimed work, content-addressed delivery, a member receipt, and member adoption that survives reload and offline operation. The actual witnessed request is `f2ec783f-2b1c-43a6-827a-5d1093fab60c` for the `forge` studio; its private brief stays on the request owner.

The separate `publishing-browser-verification.json` at 2026-10-12T03:09:38Z records 13 passed browser checks: request-delivery origin, a creator grant separate from coaching, review before publication, versioned native CMS release, public catalogue without private request coordinates, member adoption, withdrawal, no fallback to older versions, offline retention and no page errors. These two browser receipts use their own isolated fixtures. Do not call them one shared production occurrence or evidence of a subscriber payment.

## Connection implemented in the existing MCP owner

`integrations/mcp/training_public.mjs` implements a strict read-only public projection of `training_program_release` records. It accepts only the source CMS namespace `training-<studio>`, verifies the exact SHA-256 body digest and studio/program/version identity, rejects unexpected routine fields, returns the latest **published** version per programme, and never falls back to an older version if the latest is withdrawn. Returned attributes are the programme key, title, description, version, routine, published timestamp and source content SHA; member request IDs, publisher actors, submission hashes, payment records and private training state remain excluded.

`integrations/mcp/project_native.mjs` now implements `training_public_programs({studio})` by reading the **same** native BonfÿreCMS catalogue that `publishingStore` uses. The method does not run `schema apply`, create a second store or call a model/provider. Tool registration is in the existing `integrations/mcp/project_catalog.json`; autonomous admission classifies it as read-only in `integrations/mcp/operator_tools.mjs`. One native programme is consumable as a member app routine and an MCP structured result without copying the member's workout log.

**Opt-in is mandatory.** The MCP operator must explicitly set both `BONFYRE_TRAINING_CMS_HOME` to an absolute existing training CMS home and `BONFYRE_TRAINING_MCP_PUBLIC_STUDIOS` to the comma-separated public studio IDs. Unconfigured studios are refused, no default Finance/CMS estate store is substituted, and the API exposes only already published public programme records. Nothing here grants paid-tier entitlements or consent to personal training data.

## Isolated execution fixture

`integrations/mcp/tests/training_public_programs.test.mjs` builds one end-to-end test on disposable stores: member HTTP POST -> native Queue mission -> actor-granted creator claim/draft/delivery -> explicit programme publication through creator API -> public programme GET -> MCP view of the same routine -> member-owned `routine.save`. It includes wrong-studio refusal, a private-data exclusion assertion, read-only native invocation, later withdrawal, and a separate source-tampering/refusal fixture.

The suite uses `cmd/BonfyreQueue`, `cmd/BonfyreAuth`, `cmd/BonfyreCMS`, native CMS source readbacks and the existing ServiceSite handler. It does not start a browser, live model, external provider, public listener, payment processor or production customer operation. It contains no live member identity.

The **fixed** `training-mcp-smoke` profile is now declared in the existing `project_operator.mjs` and `project_catalog.json`, and allowed in `operator_tools.mjs`. It runs exactly five approved test modules (new cross-owner fixture, Project native MCP, Project tool registry, training requests, training publishing) with Bun. It does not accept a caller-supplied script path.

## Observed verification and precise blocker

- Native `cmd-make BonfyreCMS` passed its existing tests (including conditional updates, quotation source numeric accuracy, and publisher market owner); `cmd-make BonfyreQueue` and `cmd-make BonfyreAuth` returned successfully with existing executables.
- `node-syntax` succeeded for all six changed/new MCP JavaScript test and implementation files. `project_catalog.json` was parsed and inspected as JSON with 33 Project tools and no duplicate names, including one read-only `training_public_programs`.
- Calling `project_test_profile({profile:'training-mcp-smoke'})` through the currently connected MCP failed at the older tool-schema enum before any test execution. The new Bun suite is therefore **NOT YET EXECUTED OR PASSING**. The live connector does not yet advertise the new tool.
- The active Project MCP runtime and managed Capability executable are older than their checkout sources. A controlled reload and exact selected-binary/corpus digest readback are required; do not interpret a source patch as a deployed endpoint.

## Acceptance and Codex handoff

First use the approved fixed `training-mcp-smoke` test profile **after** the Project MCP owner reload or execute the exact isolated Bun command from the existing developer terminal. Examine and fix any failures without touching live fitness member records. Then verify the `forge` MCP tool reads precisely the same public programme identity/version/routine as `GET /api/training/programs/forge` from the configured staging site, and that another studio or unconfigured environment is refused. Withdrawal must remove the current public listing without changing an already adopted member copy.

Only after this may the docs call the public MCP fitness programme example runnable. The paid membership, private coach-consent, subscriber tax and payment-settlement chains remain separate unresolved work. Preserve the 761-file dirty checkout, 2 staged profiles, paused operator, and Codex's active ServiceSite component lane. No commit, external message, purchase, public deployment, server restart or payment was part of this implementation.

## Executed 2026-10-12 (later the same day)

The five modules of the `training-mcp-smoke` profile were run with Bun from the checkout: 45 tests across 5 files pass. One fixture defect was repaired first: the `ask` helper in `integrations/mcp/tests/training_public_programs.test.mjs` returned the handler's value directly, which is not always a promise, so `.then` failed on the public catalogue read; it is now `async`. The suite was run directly, not through `project_test_profile`, so the connected MCP runtime's older tool schema was not exercised and the live connector still does not advertise the tool. The guide `docs/experiences/interfaces-and-mcp.md` now describes the example as a test you can run, not as a hosted endpoint.

## Connected profile recovered 2026-10-18 UTC

The connected Project MCP now advertises and executes `training-mcp-smoke`. The first recovery run passed 53 tests and exposed one fixture failure: `project_node.test.mjs` inherited the running MCP's write settings through `process.env`. Patch `patch-1792285157-78b36d4184ce` confines the fixture's home, policy and approval-file coordinates, explicitly disables its write switches, and checks those settings through its HTTP health route. It changes no production permission.

The same connected profile then passed all 54 tests with 718 assertions across its five modules in 5.26 seconds. The earlier catalog-reload blocker is closed. These are isolated native Auth/Queue/CMS and MCP integration checks; production studio configuration, host UI, paid membership and settlement remain separate. Cross-session recovery and remaining product work are recorded in `docs/commercial-leverage-refinement-20261008.md`, section `Session recovery: 2026-10-18 UTC`.
