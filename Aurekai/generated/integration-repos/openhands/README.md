# OpenHands

Standalone Aurekai integration scaffold for OpenHands.

## Target Profile

- Slug: openhands
- Repo name: aurekai-integration-openhands
- Package name: @aurekai/integration-openhands
- Surfaces: soft-fork
- Categories: runtime-shim
- Runtime depths: execution-loop-mediation
- State-machine types: none

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

