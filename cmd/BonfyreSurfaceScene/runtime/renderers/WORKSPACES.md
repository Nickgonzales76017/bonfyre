# Composed workspaces

A workspace combines several live interaction components over explicit application bindings. The catalogue composes editable records, selected-record inspection and the current change review. The planner composes its 2D/3D editor, source products, quantities, project properties, saved alternatives and comparison. Both keep their original domain models, saved-record keys and downstream operations.

## What the application supplies

`workspace-composition.mjs` consumes the existing `bonfyre-surface-scene.v1` coordinates and an explicit Map of executable bindings. Each binding supplies its application kind, label, existing element, current edit status, supported selection predicate and owner commands. The scene supplies semantic slots, application bindings, continuity keys, action apertures and declared desktop arrangements. Unknown bindings, incompatible kinds and undeclared actions are refused. A material query remains an owner concern; this composition does not execute it or infer a data source.

`workspace-host.mjs` projects those bindings into Dockview 8.5.1 MIT core. Presets arrange a task such as editing, specification or comparison. The panel chooser and searchable command palette expose the current composition without requiring another application-specific navigation controller. The palette invokes the existing handler after checking current availability; it never establishes authorization itself.

Layout operations move the original content nodes. A hidden or closed pane retains its editor in the same host, including unsubmitted inputs. Reopening or redocking it restores the same node rather than creating a second grid, model renderer, history or draft. Teardown restores the original layout and removes subscriptions. Narrow viewports collapse the composition into accessible tabs over the same content. User-customized dock geometry is currently ephemeral; reload restores the declared arrangement, not a saved layout preference.

Selection is shared only within an explicit continuity group. Pending operations carry the current source revision, actor coordinate, selected record, owner version and abort signal. Replacing the source is refused while a registered editor is dirty or an operation is pending. Effects still require the domain owner's revision and permission checks. Browser-local posture is not an authenticated principal.

## Existing donor operations

The catalogue still edits through its existing source-preserving model and saves through its atomic browser store. The change pane shows the same differences as the original review dialog. Selecting a real catalogue option continues into the existing 3D configurator and room plan.

The planner retains original fixture identifiers, quarter-foot positions, supplier questions and retained source prices. A changed draft price in catalogue editing does not silently alter a fixture quote basis. Quantity changes, rotation, undo, saved alternatives and the comparison all operate on the original plan. The comparison is refreshed independently of the old page navigation, so opening it through a workspace arrangement does not show stale material.

## Build and verification

The normal `component-release.mjs` builder and its concurrent-release retry assemble these same bindings. `workspace-dependencies.mjs` verifies the pinned Dockview archive and emitted module/CSS identities, retaining its MIT licence. The browser imports local release assets; no runtime CDN or install script is required. A repeated build must retain identical bytes and addresses. A missing optional docking dependency leaves the original editor available.

`workspace-composition.test.mjs` covers composition compatibility, source-scoped selection, independent sessions, current availability, pending-command cancellation, retry and source-change refusal. `workspace-composition-browser.mjs` uses actual catalogue edits, configuration, 3D rendering, quantities, alternatives, comparison, mobile panels and saved-state reload. `workspace-docking-browser.mjs` exercises pointer docking, closing/reopening the same editor and resizing. These tests run before publication and again on the public site. They do not claim that all suite workflows have been extracted.

## Native integration

In the native checkout, `SurfaceRuntime.composeWorkspace` consumes this same composition core. The new `workspace_native_test.mjs` populates a temporary CMS, invokes the actual C compiler, binds the returned scene, changes the CMS source, recompiles and verifies retention/adoption behavior. The checkout compiler now includes its already-calculated source digest in read-only output. The native make suite passed these checks on 2026-10-16. These changes are applied in the native checkout; managed-binary installation and hosted estate-wide activation are separate.

Native ServiceSite rebinding and the atlas update remain outside the current automatic patch policy. The public Pages assembly is not misrepresented as a deployed native CMS service. No account grants, merchant operations or source storage were added here.

## Remaining extraction scope

This implementation provides executable workspace composition for two existing products. It does not yet synthesize a complete product from an arbitrary pack. Media/transcript editing, publication canvases, scenario exploration, source reconciliation, resource scheduling and inherited setup are still distinct donor families. They should join this composition through their existing owners, preserving their characteristic interactions. A successful next extraction replaces those consumers and demonstrates the same source-to-result task; a new registry entry or component gallery is insufficient.
