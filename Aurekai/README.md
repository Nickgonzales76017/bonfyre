# Aurekai

Aurekai is the public platform layer for the Bonfyre native runtime.

The `akai` CLI provides the Aurekai front door while preserving compatibility
with Bonfyre operators, manifests, and legacy artifact formats.

## Aurekai-first formats

```
.akmodel    model weight pack
.aksae      SAE feature dictionary
.akfpqx     cross-model FPQx alignment manifest
```

## Legacy-compatible formats

```
.bfmodel    (alias: .akmodel)
.bfsae      (alias: .aksae)
.bffpqx     (alias: .akfpqx)
```

## Dual-manifest release rule

Every Aurekai release includes both manifests:

```
aurekai.manifest.json       public distribution metadata
bonfyre.manifest.json       legacy runtime validation metadata
```

This is not a bug. It is the bridge.

Aurekai packages include a Bonfyre compatibility manifest for native runtime validation.

## Recommended release artifact set

```
akai-hyper-v0.8.0-bun-darwin-arm64
aurekai-runtime-v0.8.0-bun-darwin-arm64.tar.gz
aurekai-model-memory-qwen3-8b-20260502.tar.gz
aurekai-appliance-v0.8.0-bun-darwin-arm64.tar.gz
aurekai.manifest.json
bonfyre.manifest.json
SHA256SUMS
SBOM.spdx.json
```

## Install

```bash
bun add -g @aurekai/runtime
akai doctor --deep
akai install --user
akai dashboard
akai run recipe.json --sae-audit --semantic-cache
```

## CLI

```bash
akai doctor --deep
akai dashboard
akai run <recipe> [--input FILE] [--sae-audit] [--semantic-cache]
akai install --user|--system [--service]
akai sae:activate --dict default.aksae --residual residual.bin
akai model:inspect qwen3-8b.akmodel
akai fpqx:align-sae --from qwen3-8b.l24.aksae --to llama3-8b.l26.aksae --out qwen3-to-llama3.akfpqx
akai query:features "feature:6159 > 0.7"
```

Compatibility aliases remain in place during migration:

```bash
bonfyre       -> akai
bonfyre-hyper -> akai
bonfyre-sae   -> akai sae
```

## Registry

| Surface        | Name                        |
|----------------|-----------------------------|
| GitHub         | `aurekai/aurekai`           |
| npm            | `@aurekai/runtime`          |
| PyPI           | `aurekai`                   |
| Docker / GHCR  | `ghcr.io/aurekai/runtime`   |
| Hugging Face   | `aurekai/model-memory`      |
| Homebrew       | `aurekai/tap`               |
| Helm           | `aurekai-runtime`           |
| VS Code        | Aurekai Workbench           |
| CLI            | `akai`                      |

Integration governance registry:

- `registry/integrations.json`
- `registry/fork-surfaces.json`
- `docs/FORK_SURFACES.md`

Quick CLI access:

```bash
akai integrations --json
akai forks --json
```

Executable integration runtime:

```bash
akai integrate:list --surface hard-fork --json
akai integrate:list --surface state-machine --group execution_pipeline_continuity --json
akai integrate:run --target "LayerZero" --event-json '{"event_id":"lz-1","action":"packet_commit","risk_score":0.33}' --persist --json
akai integrate:run --target "LayerZero" --event-json '{"event_id":"lz-1","action":"packet_commit","risk_score":0.33}' --enforce-doctrine --json
akai integrate:batch --surface soft-fork --event-json '{"event_id":"shim-1","action":"loop_tick","risk_score":0.15}' --json
akai integrate:state-machine --type transport_continuity --event-json '{"event_id":"sm-1","action":"continuity_tick","risk_score":0.58}' --json
akai integrate:ingest --target "Wormhole" --input-file ./event.json --persist --json
akai integrate:ingest-batch --surface hard-fork --input-file ./event.json --strict --json
akai integrate:ingest --target "LayerZero" --use-template layerzero-prod --environment prod --strict --json
AUTOGEN_SIGNING_SECRET=dev-secret akai integrate:ingest --target "AutoGen" --use-template autogen-dev --environment dev --request-body-json '{"workspace":"lab","latest":true}' --json
akai integrate:ingest --target "TradingAgents" --event-json '{"run":{"id":"ta-1","status":"ok"},"risk":0.22}' --json
akai integrate:ingest --target "IBC packet flows" --event-json '{"packet":{"sequence":"42","action":"ack"},"risk":0.12}' --json
akai integrate:presets --target "LayerZero" --json
akai integrate:templates --target "LayerZero" --environment prod --json
akai integrate:validate-event --target "LayerZero" --input-file ./event.json --json
akai integrate:list --surface state-machine --group transport_continuity --json
akai integrate:scaffold-repos --all --out-dir ./generated/integration-repos --json
```

Connector ingestion modes:

- `--event-json` direct payload injection
- `--input-file` local event feed file (JSON or text)
- `--endpoint` HTTP endpoint fetch + normalization
- `--use-template` resolve endpoint/auth/query via registry template
- `--environment` select template environment scope (`dev`, `staging`, `prod`, etc)
- `--request-body-json` override endpoint request body (for POST/PUT/PATCH)
- `--request-body-file` load request body override from file
- `--strict` fail ingestion when parser profile validation fails

Template signing support:

- `signing.mode: hmac-sha256`
- `signing.secret_env` to source the HMAC key
- `signing.signature_header` and `signing.timestamp_header`
- `signing.required: true` enforces secret presence in endpoint mode

Protocol/app preset registry:

- `registry/integration-connector-presets.json`

Endpoint template registry:

- `registry/integration-endpoint-templates.json`

Preset-aware parser families include:

- transport: IBC/Cosmos, LayerZero, Axelar, Wormhole, Hyperlane, Stargate
- rollup pipeline: zkSync, Starknet, Arbitrum, Optimism, Taiko, Scroll, Linea
- financial topology: Uniswap v4 hooks, Aave, Morpho, Pendle, Ethena, EigenLayer, Maker/Sky
- availability: Celestia, Avail, EigenDA
- soft-fork loops: n8n, LangGraph, AutoGPT, AutoGen, Continue.dev
- observer layers: Chainlink/Pyth rounds, Safe/Gelato trails, The Graph snapshots

Runtime execution contracts:

- `schemas/aurekai.adapter.contract.v1.json`
- `schemas/aurekai.committed_state.v1.json`
- `schemas/aurekai.transition.v1.json`
- `schemas/aurekai.trajectory.v1.json`
- `schemas/aurekai.policy_decision.v1.json`
- `schemas/aurekai.lineage_edge.v1.json`
- `schemas/aurekai.functional_claim.v1.json`
- `schemas/aurekai.state_object.v1.json`
- `schemas/aurekai.export_projection.v1.json`
- `schemas/aurekai.integration_execution.v1.json`

Commerce continuity contracts:

- `schemas/aurekai.fulfillment_transition.v1.json`
- `schemas/aurekai.invoice.proof.v1.json`
- `schemas/aurekai.sla.continuity.v1.json`
- `schemas/aurekai.delivery_witness.v1.json`
- `schemas/aurekai.chargeability_claim.v1.json`

Commerce continuity runtime behavior:

- For commerce descriptors (`fulfillment_state`, `treasury_state`, commerce categories), execution emits a `commerce_continuity` bundle with:
	- `aurekai.fulfillment_transition.v1`
	- `aurekai.delivery_witness.v1`
	- `aurekai.sla.continuity.v1`
	- `aurekai.invoice.proof.v1`
	- `aurekai.chargeability_claim.v1`
- Functional claims include invoice-backing evaluation tied to chargeability result
- Chargeability and invoice continuity now evaluate policy fail-conditions directly (for example `delivery_gap`, `handoff_break`, `step_policy_break`, `unhandled_branch`) rather than only the top-level allow/halt decision

Domain semantics registries:

- `registry/chart-families.json`
- `registry/continuity-policy-families.json`
- `registry/protocol-mutation-boundaries.json`
- `registry/residual-calibration-profiles.json`

Executable protocol mutation boundaries:

- Runtime resolves protocol boundaries from `registry/protocol-mutation-boundaries.json`
- Canonical transition functions (e.g. `a->b->c`) are executed as stepwise transitions
- Boundary metadata (`canonical_state_object`, `witness_boundary`, `replay_granularity`, `lineage_edge_type`) is emitted into committed state, trajectory summary, and execution envelope

Ingress doctrine enforcement:

- `--enforce-doctrine` fails execution when integration output does not satisfy doctrine checks
- Doctrine checks enforce mapping into committed state, transitions, policy, lineage, projections, and claims
- Protocol-critical surfaces require explicit mutation boundaries

Transport semantics runtime outputs:

- `transport_semantics` includes domain-to-domain envelope, public/private projection modes, witness bundles, replay/retransmission semantics, topology route scoring, and settlement acknowledgment semantics

Runtime policy evaluator:

- `src/policy-family-evaluator.mjs` (family-specific fail conditions and thresholds)
- `src/residual-calibrator.mjs` (chart-family weighted residual calibration + taxonomy)

Execution outputs can be persisted at:

- `output/integration-runs/*.jsonl`

Standalone repo-ready integration scaffolds can be generated at:

- `generated/integration-repos/*`

Continuity core importable API:

- `src/continuity-core.mjs`
- package export: `@aurekai/runtime`

Isolated kernel package boundary:

- `kernel/` (internal package: `@aurekai/continuity-core`)
- package subpath export: `@aurekai/runtime/kernel`
- state runtime subpath export: `@aurekai/runtime/kernel/state-runtime`
- trajectory calculus subpath export: `@aurekai/runtime/kernel/trajectory-calculus`

Repo split tooling:

- split execution guide: `docs/KERNEL_REPO_SPLIT_EXECUTION.md`
- prepare standalone kernel repo scaffold: `npm run split:kernel:prepare`
- verify standalone kernel repo scaffold: `npm run split:kernel:verify`
- wire runtime to sibling local kernel package path: `npm run split:kernel:use-local`
- lock runtime to published kernel version: `bash ./scripts/set-kernel-dependency.sh --published <version>`

Example kernel import:

```js
import {
	runKernelIntegration,
	createStateSnapshot,
	mutateStateObject,
} from "@aurekai/runtime/kernel";
```

Trajectory calculus primitives:

- `buildTrajectoryCalculus`
- `buildReplaySlices`
- `buildBranchModel`
- `compressBranches`
- `runContinuityQuery`

## Brand architecture

- Aurekai Platform
- Aurekai Runtime
- Aurekai Intake
- Aurekai Intelligence
- Aurekai Memory
- Aurekai Proof
- Aurekai Wire
- Aurekai Commerce
- Aurekai Publish
- Aurekai Edge

## Public naming

- Product: Aurekai
- CLI: `akai`
- Internal codename: Bonfyre
- Main package: `@aurekai/runtime`
- Main container: `ghcr.io/aurekai/runtime`
- Manifest schema: `aurekai.deploy.v1`

## Repo split

The `Aurekai/` directory in the Bonfyre monorepo is the canonical source
for this public layer until the native C symbols are migrated. The Bonfyre
monorepo remains the implementation source.

Standalone repo target: `github.com/aurekai/aurekai`

