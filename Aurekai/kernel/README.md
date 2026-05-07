# Aurekai Continuity Kernel

This package isolates the continuity substrate from shell surfaces.

Package:

- `@aurekai/continuity-core`

Entry points:

- `./src/index.mjs`
- `./src/state-runtime.mjs`

Public API categories:

- integration execution (`runKernelIntegration`, `runKernelSurfaceBatch`, `runKernelStateMachineBatch`)
- ingestion (`ingestKernelIntegration`, `ingestKernelBatch`)
- target discovery (`listKernelTargets`)
- claims evaluation (`evaluateFunctionalClaims`)
- state object runtime (`createStateSnapshot`, `createStateTransition`, `createAttachmentArtifact`, `createTrajectoryProjection`, `mutateStateObject`, `forkStateObject`, `mergeStateObjects`, `archiveStateObject`, `reopenStateObject`)

State object runtime enforces the lifecycle phases and state kinds from `aurekai.state_object.v1`:

- phases: `create`, `mutate`, `transport`, `fork`, `merge`, `archive`, `reopen`
- kinds: `state_snapshot`, `state_transition`, `attachment_artifact`, `trajectory_projection`

This package is designed as the extraction boundary for a full kernel/shell repository split.
