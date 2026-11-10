# Reusable interaction implementation

## 2026-10-19: scoped command inputs and immutable selections

The canonical workspace composer and its existing WebAddress asset projection
now share SHA-256 `e0b274df81528b38470109bb2b69afabf33f506fb90688403ab7457ed9cbc5e6`
(14,344 bytes). Commands may declare `reads: [bindingId, ...]`; the owning
binding is included. Captured inputs contain immutable selection coordinates,
continuity-group epochs and the corresponding owner versions. An unrelated
pane selection does not invalidate an explicitly scoped preview. Omitted read
declarations retain the older conservative workspace-wide selection check.
Adapters must expose stable revision tokens for the material actually consumed.
This is not a replacement for the domain dependency graph.

`resultPolicy: 'preview'` emits `stale-result` with the original context when
those inputs changed. Owner-operation results remain results even after their
own successful write advances a revision. Callers must handle this distinction;
no effect is automatically replayed and a stale view does not undo a commit.
Nested selections are copied/frozen within explicit bounds. Listener failures
cannot wedge command cleanup; diagnostics retain at most 32 errors. Dirty-work,
source-replacement, command-aperture and disposal checks remain in force.

Native make passes with 37 context regressions, 16 existing interaction tests
and the CMS-to-C-compiler workspace witness. The public asset was read back
exactly. ServiceSite and previously published GitHub packages are not thereby
proven to adopt the new declarations. See the cohesion handoff's runtime follow-through.

The five modules in `runtime/renderers/` are the same source bytes used by the
catalogue, product inspector, room planner and tested ITAD substitution in public
repository PR 50. They implement the existing SurfaceRenderer separation:
interaction grammar takes owner-supplied data and commands; it owns no store.

The collection workbench has editable grids, product/option-like modes, search,
facets, column controls, selection, detail inspection, bulk-change review and
saved-copy continuation. The scene workbench has camera and orientation controls,
dimensions, mesh inspection, isolation, exploded views, lighting and gestures.
Record-grid, review-sheet and editable-slice history support both families.

`make` runs `tests/reusable-interactions.test.mjs` with the existing SurfaceScene
tests. The 2026-10-16 native run passed all 16 new tests and all existing scene,
runtime, neighbourhood, kin and stage checks. Component hashes match package
`222ab2b3f0cb0334bca55865dc8af5868a3a5f8379411bda8b5754120f8db9e1`.

GitHub Actions run 38832364109 verified the original 28-check catalogue-to-room
flow and 45 before/after and reuse assertions. Run 38890427161 published the
merged components and passed 30 checks against the actual public Pages site.

Native ServiceSite rebinding and the atlas update remain unapplied. The MCP patch
policy excludes `components/BonfyreServiceSite/` and `architecture/`; no policy
was changed. The existing native catalogue material still copies its original
controller. ITAD replacement is tested but its Cloudflare publication is open.
Automatic SurfaceScene composition and the other demo families are not completed
by installing these modules. The autonomous operator remains paused.

## 2026-10-16: executable workspace composition

`runtime/renderers/workspace-composition.mjs` now compiles explicit application
bindings, semantic slots, continuity keys, declared arrangements and action
apertures into a workspace session. `SurfaceRuntime.composeWorkspace` uses it;
source deltas validate both projections before changing the active revision.
Dirty editors and pending commands prevent source replacement. Selection remains
specific to the declared continuity group; current command availability is
checked again when invoked. The domain owner still authorizes every effect.

`tests/workspace_native_test.mjs` builds temporary CMS records, compiles them with
the C owner, binds the result, edits the CMS source and recompiles. It verifies
selection continuity, command refusal, retained dirty work, successor adoption
and teardown. The complete native `make` suite passes with this test included.
The test exposed a missing `source_digest` in read-only compiler output. The
checkout compiler now returns its already-calculated digest without changing
the persisted source identity. Managed-binary installation is not claimed.

The public catalogue/room composition was merged in PR 51 as e993c8e85145.
Pages deployment 38904057448 passed the original public journey (30 checks),
combined workspace operations (24 checks), and pointer docking, close/reopen,
resize and retained-input checks (6). These are separate browser observations,
not inferred from native build success. The existing public showcase routes remain.

The WebAddress asset projection now includes the workspace composition import.
Its exact /assets/renderers/workspace-composition.mjs route serves a flat,
byte-identical copy of the canonical module; the filesystem traversal guard is
unchanged. The complete WebAddress make target passed on 2026-10-16, including
page_surface_scene_test and the existing native route, source composition,
household, grammar, media, stage and discovery-review regressions. No running
