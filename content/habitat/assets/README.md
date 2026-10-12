# Habitat asset packs

Art vocabulary for the HabitatCompiler. Licences differ per pack and are
tracked in `pack-registry.json`; only `redistributable: true` sheets may be
inlined into a published artifact.

| pack | licence | how it got here |
|---|---|---|
| Kenney (10 packs, 29 sheets, 15,570 tiles) | **CC0-1.0** | downloaded directly from kenney.nl |
| LimeZu / Serene Village / Modern Exteriors | commercial, owner-supplied | owner's own `nmatter1/smallville` checkout — referenced, never vendored |
| Pixel Frog (Tiny Swords, Pixel Adventure) | **CC0-1.0** | see below |
| 0x72 (DungeonTileset II) | **CC0-1.0** | see below |

## The two itch packs

Pixel Frog and 0x72 both state CC0 on their own itch.io project pages, so
there is no licensing obstacle. The obstacle is mechanical: itch.io gates
downloads behind a browser session with a CSRF-protected POST, which is not
something to script around.

Third-party GitHub "mirrors" exist and are **deliberately not used**. The
ones found were unlicensed re-uploads inside unrelated game projects, with
the sheets sliced into hundreds of individual files. For art going into a
product, provenance from the author's own page matters more than
convenience.

**To add them:** download the official zips while signed in to itch, drop
them in `assets/packs/dropin/`, and run:

```
python3 scripts/bonfyre_habitat_assets.py assets/packs/extracted assets/packs/registry.json
```

The registry picks them up automatically — `classify()` already recognises
`pixelfrog` and `0x72` paths and carries their CC0 status through.

## Grid inference

Kenney ships each sheet twice: `*_packed.png` with no gutters, and an
unpacked variant laid out as `cols*(tile+1)-1` with 1px spacing. An earlier
version of the scanner only tested exact division and silently discarded
every unpacked sheet — roguelike-modern-city, caves-dungeons, characters
and indoors were all being thrown away. `grid_for()` now solves for
`(tile, spacing)` together, which recovered 17 sheets and 9,221 tiles.
