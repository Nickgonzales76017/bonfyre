# Kernel Repo Split Execution

This document defines the concrete split path from the monorepo package boundary to a fully independent kernel repository.

## Objective

Extract continuity core into a standalone repository and package (`@aurekai/continuity-core`) while keeping shell/runtime repos thin consumers.

## Current Stage

- In-repo kernel package boundary exists under `kernel/`
- Runtime shell (`akai`) already routes integration execution through kernel APIs
- Split scaffolding scripts now exist to generate a standalone kernel repository

## Tooling

- Prepare split repo scaffold:
  - `bash ./scripts/prepare-kernel-repo-split.sh`
- Prepare to custom location:
  - `bash ./scripts/prepare-kernel-repo-split.sh --out-dir ../aurekai-continuity-core`
- Force refresh target scaffold:
  - `bash ./scripts/prepare-kernel-repo-split.sh --force`
- Verify split scaffold:
  - `bash ./scripts/verify-kernel-repo-split.sh`

## Dependency Source Wiring

Wire runtime shell to sibling local kernel package path:

- `npm run split:kernel:use-local`

Lock runtime shell to published kernel version:

- `bash ./scripts/set-kernel-dependency.sh --published <version>`
- Example: `bash ./scripts/set-kernel-dependency.sh --published 0.1.0-alpha.1`

The published flow pins an explicit version string for `@aurekai/continuity-core`.

## First-Class Doctrine Subpaths

The split package exports first-class doctrine domains:

- `@aurekai/continuity-core/state`
- `@aurekai/continuity-core/transition`
- `@aurekai/continuity-core/trajectory`
- `@aurekai/continuity-core/claim`
- `@aurekai/continuity-core/policy`
- `@aurekai/continuity-core/chart`
- `@aurekai/continuity-core/schema-registry`

Legacy compatibility aliases remain available for `state-runtime` and `trajectory-calculus`.

## Split Sequence

1. Generate standalone kernel scaffold to sibling repo directory.
2. Run scaffold verification and module export checks.
3. Initialize target repo, wire CI, and configure independent release workflow.
4. Publish `@aurekai/continuity-core` prerelease.
5. Update runtime shell package to consume published dependency instead of local imports.
6. Lock doctrine enforcement checks in shell CI against kernel package APIs.

## Enforcement Gates

A split is considered complete only if all conditions hold:

- Kernel package builds and verifies independently from monorepo shell code.
- Shell repo imports continuity APIs from external kernel package version.
- Integration execution still passes doctrine enforcement checks.
- Protocol mutation boundaries and transport semantics behave unchanged post-split.
- Functional claim and commerce continuity outputs remain contract-compatible.

## Risks

- Import-path drift during extraction.
- Schema and registry divergence between shell and kernel repos.
- Release skew between shell and kernel package versions.

## Mitigations

- Keep extraction script deterministic and rerunnable.
- Version kernel package independently and pin from shell.
- Add CI checks for API surface and schema compatibility.
