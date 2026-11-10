# Release scripts

Public releases stage, verify and activate with
`bun scripts/release/public_surface_rollout.mjs stage|verify|activate`, run from the
integration checkout (`~/Developer/Bonfyre-converge`), so a release never races
edits in the working checkout. Acceptance groups carry across candidates when
their inputs are unchanged.

The October private-convergence recovery driver (`converge.py` and helpers) was
retired on 2026-11-06 after the integration branch merged into `main`; it is
restorable from `~/.bonfyre/archive/retired/private-convergence-recovery-20261106.tar.zst`.
