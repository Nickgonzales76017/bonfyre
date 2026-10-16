# Reusable interaction workbenches

These are implementations of the existing SurfaceRenderer boundary. The renderer provides interaction grammar; the binding provides domain records, validation, source identity, ownership and effects. They live under the existing SurfaceScene runtime, not a new application or storage framework.

## Extracted and exercised

| Implementation | Interaction owned here | Existing consumers |
| --- | --- | --- |
| `collection-workbench.mjs` | Product/option-like collection modes, search, facets, column control, selection, editable cells, detail inspection, bulk-change preview, undo/redo, reviewed save and recoverable failure | Catalogue, using its existing product-copy validator and draft store |
| `scene-workbench.mjs` | Smooth framing, orientation controls, lighting, selectable model parts, isolation, exploded inspection, projected dimensions, manipulation gestures, selection outlines and expanded viewport | Catalogue product inspection and the room planner |
| `record-grid.mjs` | AG Grid lifecycle, stable row identity, selection reconciliation, editor status, current editability, filtering and teardown | Collection workbench and ITAD equipment manifest |
| `review-sheet.mjs` | Escaped before/after review, generation-bound confirmation, pending state, cancellation and focus restoration | Catalogue bulk/save review and ITAD manifest bulk review |
| `edit-history.mjs` | Bounded editable-slice transactions, no-op handling, rejected-edit rollback, undo and redo | Catalogue, ITAD and room placement |

The current extraction does not replace the remaining demos' timelines, memo editors, source readers, schedulers, settings or publication flows. The setup workbench from the preceding session is a prepared donor, not an installed production consumer.

## Bind records without importing an application

`createCollectionWorkbench(host, {binding, presentation, loadLibrary})` takes three independent contributions.

`binding` implements `load`, `rows`, `capture`, `restore`, `edit`, `changes`, `prepareBulk`, `prepareSave`, `persist`, `acceptSave` and `discard`. It may add `canEdit`, `onState`, `onLoadFailure` and `afterSave`. Only the binding knows what a row means. `capture` selects editable values, excluding immutable source material, model bodies and permission records. `prepareBulk` returns a validated candidate and the human-readable changes. `prepareSave` supplies the original owner's write envelope. The owner must recheck permission and revision at effect time; `canEdit` is a presentation guard.

`presentation` supplies `words`, `scopeLabel`, `modes`, `filters`, `bulk`, `metricDefinitions`, `metrics`, `columns`, and optional `warning`, `gridOptions`, `resize`, `inspect` and `actions`. An inspector can return trusted markup with a `mount` hook that returns cleanup. Source strings must be escaped. The catalogue adapter demonstrates a continuation from row selection into actual 3D configuration rather than another read-only detail card.

`loadLibrary` provides the existing pinned renderer dependency. Replacing an application does not require reinstalling its storage, changing frameworks, adding a network API or reformatting its domain data.

The returned interface remains `activate(scope)`, `close()`, `dirty()`, `busy()` and `refresh()`. A new scope invalidates old asynchronous loads. Shortcuts belong to the mounted host, not the entire document. A confirmed write and a failed downstream refresh remain different states.

## Bind a scene without importing a fixture plan

`createSceneWorkbench` receives the existing Three.js engine, renderer, scene, camera, controls, root, viewport and model-object group. It does not load models or create another scene store.

The binding supplies:

- `readView()`: selected Object3D, pickable objects, selection identity, optional world-unit ground geometry, spatial posture and selection label.
- `objectKey(mesh)` and `formatDimension(length, axis, basis)`: exact domain selection and unit presentation.
- `manipulation.allowed`, `capture`, `preview`, and `commit`: gesture admission, source-bound edit basis, placement constraint and the original owner's mutation.
- `historyActions`: existing undo/redo commands with current enabled state and optional subscriptions.
- `decorateScene` and `words`: application presentation that should not leak into the generic interaction engine.

`integrations/browser/adapters/product-scene-tools.mjs` contains the retained fixture-plan conversion to metres and quarter-foot placement. The shared renderer contains no merchant identity, fixture schema, feet conversion, SKU mapping or persistence. Parts restore their prior visibility and transforms after inspection. A dimension derived from an exploded view remains labelled an inspection span.

## Replace donors through the real build

`scripts/component-release.mjs` packages the canonical modules at a content-derived address and builds new immutable catalogue and planner UI packages. It preserves old package addresses, source collections, saved-record keys and model bodies. `COMPONENT-DONORS.tsv` retains exact pre-extraction source entries so another build does not treat its own generated adapter as a fresh donor. An upstream donor change must be reconciled rather than silently patched by a broad search-and-replace.

The regular Pages workflow and its concurrent-release retry both invoke that same builder. The CI comparison tool delegates to it; it does not maintain a separate implementation. Repeating the build must produce byte-identical inventory and package addresses.

The ITAD transplant is an exact-source patch against the retained deployed advanced desk. The older `pages-procurement-memory` main branch is not that deployed donor. Its final publication through the native source owner remains a separate integration step.

## Acceptance: replace, exercise, compare

`node --test scripts/tests/component-contracts.test.mjs` tests the core selection, edit, review and cleanup contracts.

The `Reusable component replacement` workflow reconstructs the public donor code, runs the normal release builder, and executes the existing advanced catalogue-to-room scenario unchanged. It then runs identical pointer/keyboard editing, bulk review, save, reload, inspection and continuation operations before and after substitution. Saved catalogue drafts are compared without their save timestamp; domain source identities and all changed values remain in the comparison. ITAD records are compared directly.

The browser checks also mount two grids with identical row IDs but independent owners, destroy one without breaking the other, compare emitted modules against canonical bytes, and deliberately withhold the shared record renderer. Both donor applications must fail to claim a ready table when the shared dependency is absent. An import statement or component story alone does not pass replacement acceptance.

The following remain unproven by these tests: native account publication, authenticated sharing, merchant effects, phone hardware, Safari, general graph-to-component auto-composition and the rest of the thirty-experience extraction programme. Do not transfer the donor's unimplemented features into this component's maturity claim.

## Remaining donor families

Extract the media timeline and transcript alignment while leaving sampling, decoding and rights with Creator/media owners. Extract memo composition with retained source insertion and review while preserving each document contract. Extract scenario comparison from procurement, ITAD and planner alternatives without assuming common units or prices. Extract the settings workbench's inheritance, search, modified views, preview and conflict recovery without turning settings into another authority store. Extract request inboxes, reconciled imports and publication workspaces by preserving their original claims, revisions and destination acknowledgments.

Each family should replace at least two suitable consumers before being described as general-purpose. Keep the original user-facing operations and add a replacement-dependency test. Do not replace distinct products with one dashboard schema.
