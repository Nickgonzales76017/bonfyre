# Raylib Client Surface

`clients/raylib` implements a cozy 2D client surface adapter for Bonfyre.

This is an optional client.
It is not the canonical Bonfyre UI.
It is not required for CLI, API, headless, or server use.

## Purpose

The Raylib adapter consumes Bonfyre events and local sync patches, renders a 2D world view, and emits lightweight player input actions back to Bonfyre.

## Event Flow

```text
Bonfyre recipe emits domain event
  -> sync/MoQ/API delivers event
  -> Raylib adapter updates local render state
  -> player action emits T_PLAYER_ACTION back to Bonfyre
```

## Reads

- `T_TILEMAP_VIEW`
- `T_SPRITE_ASSET`
- `T_SPRITE_FRAME`
- `T_UI_PANEL`
- `T_WORLD_PATCH`
- `T_LIVE_EVENT`
- `T_VALUE_EVENT`
- `T_MOQ_EVENT_STREAM`
- `T_SYNC_PATCH`

## Emits

- `T_PLAYER_ACTION`
- `T_TOUCH_INPUT`
- `T_MOUSE_INPUT`
- `T_GAMEPAD_INPUT`
- `T_LOCAL_CACHE_UPDATE`
- `T_CLIENT_HEARTBEAT`

## Rendered Domain Events

- `seed.generated`
- `crop.watered`
- `harvest.verified`
- `trade.created`
- `coop.pool.funded`

## Design Guardrails

The Raylib adapter:

- does not write to `layers.db`
- does not own recipes
- does not own graph state
- does not own ledger state
- does not own canonical world state

It only maintains local cached render state for the current client session.
