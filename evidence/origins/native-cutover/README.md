# Native-cutover compatibility archive

`compatibility-oracles-2026-09-09.zip` is a restoreable bundle of the 393
already-retired `*.archive` files present at cutover.

- SHA-256: `5934d537ce0cc20d5734bb0aca0dd5ae9c92d8a5ebc7926d7594179dbe537626`
- Contents: archived compatibility callers, differential/oracle tests,
  one-shot corpus compilers, research/analysis scripts, and static-surface
  build residue.
- Retention: the original `*.archive` paths remain in the checkout for source
  lineage; the zip is a compact offline-restoration bundle, not a runtime
  import path.

This bundle deliberately excludes active runtime authorities:

- `components/BonfyreControlPlane/domain_profile.py`
- `components/BonfyreControlPlane/foreign_live.py`
- `components/BonfyreControlPlane/live_semantic_track.py`
- `components/BonfyreControlPlane/publication_program.py`

Each remains a differential oracle or active compatibility owner until its
native generated replacement has passed parity witnesses. A zip never proves
that a runtime was absorbed.
