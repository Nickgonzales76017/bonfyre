# Client Surfaces

Bonfyre is client-agnostic.

Bonfyre core owns:

- LayerArtifact registry
- recipes
- graph
- query
- index
- queue
- sync
- MoQ event delivery
- API
- ledger and meter
- lifecycle state

Client surfaces are optional adapters that consume Bonfyre state and events, render them for a specific experience, and emit user actions back into Bonfyre.

## Examples

- CLI-only client
- web dashboard
- Raylib cozy 2D client
- VR or spatial client
- stream overlay
- headless automation client

## ClientSurface Concept

A `ClientSurface` is not canonical state.

A `ClientSurface` may:

- render local views
- cache lightweight client state
- animate events
- capture user inputs
- emit action payloads back to Bonfyre

A `ClientSurface` must not:

- own recipes
- own the graph
- own the ledger
- own the LayerArtifact registry
- own canonical world state

## Event Flow

```text
Bonfyre core
  -> emits domain events and sync patches
  -> delivers through API / Sync / MoQ
  -> optional client surface renders local state
  -> player or operator action emits input event back to Bonfyre
```

## Current Optional Surface

- `raylib_cozy_2d`

This surface is implemented as an optional adapter under `clients/raylib`.
