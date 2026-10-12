# Bonfÿre repository layout

The repository root is a routing surface, not a notebook. Each tracked path has
one physical owner; physical placement does not grant semantic authority.

| Owner | Root paths | Meaning |
|---|---|---|
| Production code | `engine/`, `lib/`, `cmd/`, `bin/`, `completion/`, `programs/`, `services/`, `workflow/`, `frontend/`, `bonfyre-zig/`, `bonfyre-hyper/`, `aurekai-continuity-core/`, `Aurekai/`, `feldera/`, `serm/`, `layeros/`, `components/` | Native kernels, command surfaces, shared libraries, current product components, and runtimes. |
| Architecture and contracts | `architecture/`, `schemas/`, `estate/`, `recipes/`, `packs/` | Canonical architecture registry, contract sources, operator catalog, and workload data. Generated diagrams never replace atlas entries. |
| Generated projections | `generated/` | Reproducible materializations and checked-in projection witnesses. This tree is never an authoring source. |
| Evidence and fixtures | `evidence/`, `tests/`, `test-speech/`, `examples/` | Receipts, historical origins, recovery evidence, verification artifacts, fixtures, and executable proof. |
| Integrations | `integrations/`, `vendor/` | External boundaries and pinned foreign code. External profiles do not own Bonfÿre semantics. |
| Documentation and site | `docs/`, `site/`, `content/` | Human documentation, public site, and CMS-targeted content. |
| Repository operations | `.github/`, `.githooks/`, `.sdd/`, `migration/`, `scripts/`, `tools/` | CI, developer automation, migrations, and repository maintenance. These paths operate on the machine but do not own product semantics. |
| Operator state | `.bonfyre/`, `.bonfyre-runtime/`, `layeros/state/` | Run receipts, local runtime state, locks, and mutable operator data. State is not source. |

`components/` is the canonical owner for independently buildable product
components. `BonfyreControlPlane` remains a tested Python reference and atlas
witness while its semantics are absorbed by the native kernel; that maturity
boundary does not make the component tree historical or authoritative over
`engine/`, `lib/`, `cmd/`, `architecture/`, or `schemas/`.

Canonical placement is not a claim that every child is Generation-10-native.
`generated/projections/estate/transitional-root-absorption.json` records each
child's measured fate and `tools/transitional_root_absorption.py --verify`
fails if that projection drifts. A child with unabsorbed capability remains in
place until its current owner passes the capability-loss fence. Once
measurement proves `superseded_by_cmd`, the duplicate compatibility copy is
removed under a machine-readable DeletionProof; git history retains recovery
while the `cmd/` implementation remains the only live owner.

## Historical material

The numbered Obsidian vault (`01-Ideas` through `08-Transcriptions`), its
templates/configuration, root dashboards, and the old source-of-truth migration
map are preserved under `evidence/origins/notebook-era/`. They are source-backed
historical evidence, not parallel implementations.
The retired NightlyBrainstorm implementation and its run records live below
`evidence/origins/notebook-era/runtime/`; it is preserved for replay but is no
longer a production component or scheduled writer.

Opportunity-recovery captures live under `evidence/recovery/`; verification
artifacts live under `evidence/verification/`; stable inputs live under
`evidence/fixtures/`. The absorbed Run 7 principal-loop witness is now the
current acceptance test
`tests/acceptance/generation10_principal_loop.sh` rather than a campaign script
at repository root.

Legacy `--src`, `--out`, `output`, `reports`, and `tmp` peers have also been
absorbed into fixtures, generated projections, evidence, or operator state.
Reproducible caches may still exist in conventional ignored build directories;
they are governed by SafeReclaim rather than treated as source.

## Rules

- A campaign or run adds state, receipts, packs, or fixtures; it does not add a
  new implementation root.
- Author contracts and architecture only in their canonical trees. Commit a
  generated projection only when its source and regeneration path are known.
- Put mutable state under an operator-state root. Do not commit databases,
  caches, build products, or logs as source.
- Every tracked top-level directory must have exactly one owner class in the
  repository-shape conformance test. A new root therefore requires an explicit
  placement decision, not merely a new folder.
- Preserve rare history as evidence; delete only under the DeletionProof rules.
- `tests/test_repository_shape.py` prevents the notebook and campaign roots from
  becoming top-level peers again.
