# Cosmos/IBC

Standalone Aurekai integration scaffold for Cosmos/IBC.

## Target Profile

- Slug: cosmos-ibc
- Repo name: aurekai-integration-cosmos-ibc
- Package name: @aurekai/integration-cosmos-ibc
- Surfaces: state-machine
- Categories: transport_continuity
- Runtime depths: continuity-native
- State-machine types: transport_continuity

## Kernel Mapping Rule

This integration must map host semantics into Aurekai kernel objects only:

- committed_state
- transitions
- policies
- lineage_edges
- export_projections
- functional_claims

## Kernel Dependency

This scaffold expects @aurekai/continuity-core as the substrate contract.
Chart and policy declarations should map into kernel registries.

## Suggested Extraction

- Keep parser presets and endpoint templates aligned with the runtime registry.
- Preserve continuity policy semantics from the main Aurekai engine.
- Treat this repo as a publishable integration surface, not a semantic fork of the kernel.

## Source of Truth

Generated from the Aurekai integration registry and intended for repo extraction planning.

