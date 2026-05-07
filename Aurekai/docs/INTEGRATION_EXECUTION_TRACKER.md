# Integration Execution Tracker

Status: in progress
Owner: Aurekai runtime

This tracker converts integration intent into implementation work packages.

## Phase 1: Governance + Discovery

- [x] Define hard-fork targets and depth semantics
- [x] Define soft-fork targets and shim semantics
- [x] Define ingress adapter rule (semantic projection only)
- [x] Define state-machine integration sets
- [x] Add CLI discovery commands:
  - [x] `akai integrations --json`
  - [x] `akai forks --json`

## Phase 2: Adapter Runtime Interfaces

Goal: add canonical adapter runtime contracts in hyper runtime.

- [x] Add adapter contract schema (`aurekai.adapter.contract.v1`)
- [x] Add committed-state emission contract (`aurekai.committed_state.v1`)
- [x] Add transition event contract (`aurekai.transition.v1`)
- [x] Add lineage edge contract (`aurekai.lineage_edge.v1`)
- [x] Add continuity policy decision contract (`aurekai.policy_decision.v1`)
- [x] Add export projection contract (`aurekai.export_projection.v1`)
- [x] Add executable integration runtime entry points:
  - [x] `akai integrate:list`
  - [x] `akai integrate:run`
  - [x] `akai integrate:batch`

## Phase 3: Hard-Fork Runtime Surfaces

Goal: protocol-native runtime embedding path.

### Transport
- [ ] IBC/Cosmos packet flow state + transition witness path
- [ ] LayerZero packet observer path
- [ ] Axelar GMP observer path
- [ ] Wormhole VAA observer path
- [ ] Hyperlane message observer path
- [ ] Stargate transfer observer path

### Rollup Pipeline
- [ ] zkSync proof pipeline observer path
- [ ] Starknet proof pipeline observer path
- [ ] Arbitrum/Optimism pipeline observer path
- [ ] Taiko/Scroll/Linea pipeline observer path

### Financial Topology
- [ ] Uniswap v4 hook observer path
- [ ] Aave state observer path
- [ ] Morpho state observer path
- [ ] Pendle state observer path
- [ ] Ethena state observer path
- [ ] EigenLayer operator observer path
- [ ] Maker/Sky vault observer path

### Availability
- [ ] Celestia DA root observer path
- [ ] Avail DA root observer path
- [ ] EigenDA blob/dispersal observer path

## Phase 4: Soft-Fork Runtime Shims

Goal: execution-loop mediation without replacing host runtime.

- [ ] n8n loop shim
- [ ] LangGraph loop shim
- [ ] AutoGPT/AutoGen loop shim
- [ ] Ollama route shim
- [ ] Mem0 state shim
- [ ] Continue.dev CI/editor shim
- [ ] Browser-Use shim
- [ ] OpenHands shim
- [ ] ComfyUI shim
- [ ] GitHub Actions / VS Code / MCP shim layer

## Phase 5: New Ingress Adapters

Goal: thin ingress that maps into kernel semantics only.

- [ ] TradingAgents
- [ ] LangGraph
- [ ] OpenHands
- [ ] Browser-Use
- [ ] Continue.dev
- [ ] Mem0
- [ ] n8n
- [ ] Ollama
- [ ] AutoGPT
- [ ] ComfyUI

## Current Batch Plan

Batch A: adapter contracts + schema files
Batch B: transport protocol observer layer
Batch C: rollup and availability observer layer
Batch D: financial and privacy observer layer
Batch E: soft-fork execution shims
Batch F: ingress adapters + gating + export projection conformance tests

Current implementation delta:

- [x] Protocol profile runtime semantics wired by surface (hard-fork, soft-fork, adapter, observer, state-machine)
- [x] State-machine continuity batch runner (`akai integrate:state-machine --type ...`)
- [x] Group filter in target listing (`akai integrate:list --group ...`)
- [x] Connector-driven ingestion runner (`akai integrate:ingest --target ...`)
- [x] Connector-driven ingestion batch runner (`akai integrate:ingest-batch --surface ...`)
- [x] Input modes supported: direct JSON, local input file, HTTP endpoint fetch
- [x] Protocol-specific parser presets (`registry/integration-connector-presets.json`)
- [x] Preset inspection command (`akai integrate:presets --target ...`)
- [x] Parser validation profiles (`registry/integration-parser-validation-profiles.json`)
- [x] Strict ingest enforcement (`--strict`)
- [x] Standalone event validator (`akai integrate:validate-event`)
- [x] Endpoint template registry (`registry/integration-endpoint-templates.json`)
- [x] Template-aware ingest resolver (`--use-template`, `--environment`)
- [x] Template discovery command (`akai integrate:templates`)
- [x] POST/PUT/PATCH request body template support (`body`, `--request-body-json`, `--request-body-file`)
- [x] HMAC-signed endpoint templates (`signing` block with required secret env)
- [x] Expanded integration target taxonomy for hard-fork, soft-fork, adapter, observer, and state-machine groups in `registry/integrations.json`
- [x] Added missing adapter/observer connector presets (TradingAgents, OpenHands, Browser-Use, Continue.dev, Mem0, Ollama, ComfyUI, IBC/LayerZero/Wormhole/Hyperlane packet observers, DA/storage publication flows)
- [x] Fixed state-machine listing/index behavior so groups remain discoverable even when targets also exist on other surfaces
- [x] Repo-ready integration scaffold generator (`akai integrate:scaffold-repos`)
- [x] Added explicit ingress-depth/trust/kernel-layer metadata to integration runtime descriptors and envelopes
- [x] Added runtime trajectory object + functional claims emission (`aurekai.trajectory.v1`, `aurekai.functional_claim.v1`)
- [x] Added domain registries for chart families, policy families, and protocol mutation boundaries
- [x] Added state-machine-ready scaffold contents (`src/index.mjs`, continuity pass/fail test vectors)
- [x] Added continuity core import API surface (`src/continuity-core.mjs`, package export)
- [x] Added policy-family evaluator runtime (`src/policy-family-evaluator.mjs`) and wired decisioning into `aurekai.policy_decision.v1`
- [x] Added residual calibration runtime (`src/residual-calibrator.mjs`) with chart-family weighting/taxonomy and policy integration
- [x] Added isolated kernel package boundary (`kernel/`) and routed shell execution commands through kernel APIs
- [x] Added state object runtime APIs for typed state kinds and lifecycle phases (`kernel/src/state-runtime.mjs`)
- [x] Added trajectory calculus primitives (`kernel/src/trajectory-calculus.mjs`) with replay slices, branch compression, and continuity query outputs wired into `aurekai.trajectory.v1`
- [x] Activated protocol mutation boundaries as runtime behavior (boundary resolution + canonical transition function execution + emitted witness/replay metadata)
- [x] Activated commerce continuity runtime bundle generation (fulfillment transition, delivery witness, SLA continuity, invoice proof, chargeability claim) and linked invoice-backed functional claims
- [x] Added policy-native commerce evaluation: chargeability + invoice continuity derive from triggered policy fail-conditions (delivery/handoff/workflow blockers), not only decision verdict
- [x] Added strict ingress doctrine enforcement mode (`--enforce-doctrine`) with doctrine report emission and protocol-critical boundary requirements
- [x] Added transport semantics runtime envelope (domain envelope, projection modes, witness bundles, replay/retransmission semantics, topology route scoring)
- [x] Expanded protocol mutation boundary registry saturation (LayerZero/Wormhole/Axelar/Hyperlane/Stargate + Arbitrum/Optimism/zkSync/Starknet + Avail/EigenDA + Morpho/Maker/Pendle/Ethena)
- [x] Added richer policy/claim specialization outputs (policy specializations and family-specific functional claims)
- [x] Added full repo split execution tooling and guide (`scripts/prepare-kernel-repo-split.sh`, `scripts/verify-kernel-repo-split.sh`, `docs/KERNEL_REPO_SPLIT_EXECUTION.md`)
- [x] Promoted kernel doctrine registries and constructors to first-class subdomains (`schema-registry`, `chart`, `policy`, `transition`, `trajectory`, `claim`) with package subpath exports
- [x] Updated split scaffold generation to include doctrine modules and verify expanded continuity-core API surface
- [x] Updated integration repo scaffold generator to emit kernel-aware manifests and constructor-based adapter stubs (`@aurekai/continuity-core` dependency)
