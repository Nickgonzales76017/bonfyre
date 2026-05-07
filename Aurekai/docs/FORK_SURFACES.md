# Fork Surfaces

This document defines how Aurekai classifies protocol integration depth.

## Hard Fork Depth

Hard-fork or deep-coprocessor surfaces are places where Aurekai becomes part of runtime state semantics.

Hard-fork depth means:

- protocol-native committed state objects
- transition witnesses emitted inside mutation paths
- continuity policy inside core execution semantics
- lineage/replay native to protocol runtime
- deformation-aware monitoring as a first-class primitive

Current hard-fork targets:

- Cross-chain transport
  - IBC/Cosmos-style transport
  - LayerZero
  - Axelar
  - Wormhole
  - Hyperlane
  - Stargate
- Rollup/proof pipeline
  - zkSync
  - Starknet
  - Arbitrum/Optimism style state pipelines
  - Taiko/Scroll/Linea
- Financial topology
  - Uniswap v4 hooks
  - Aave
  - Morpho
  - Pendle
  - Ethena
  - EigenLayer
  - Maker/Sky
- Availability systems
  - Celestia
  - Avail
  - EigenDA

## Soft Fork Depth

Soft-fork/deep-shim surfaces are places where Aurekai mediates execution meaningfully while host semantics remain primary.

Soft-fork depth means:

- Akai sits inside the execution loop
- host state translated into committed state transitions
- continuity policy can classify or gate transitions
- export and lineage are first-class outputs

Current soft-fork targets:

- n8n
- LangGraph
- AutoGPT / AutoGen
- Ollama routing/proxy path
- Mem0
- Continue.dev via CI/editor path
- Browser-Use
- OpenHands
- ComfyUI
- GitHub Actions / VS Code / MCP surfaces

## Adapter Rule

Adapters must remain semantic projectors, not semantic inventors.

Every adapter maps host events into:

- committed state
- transitions
- policies
- lineage edges
- export projections

## Canonical Registry

Machine-readable source of truth:

- `registry/fork-surfaces.json`
- `registry/integrations.json`
