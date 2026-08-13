# Bonfyre Habitat — first prototype

First working slice of the "Bonfyre Habitat" vision (spatial/social
projection of real Bonfyre graph state — see memory: bonfyre-habitat-vision).
No prior Habitat code existed anywhere in this repo before 2026-08-13
(searched `site/demos/`, `frontend/`, Downloads zips — nothing found); this
is the real starting point, not a continuation of hidden prior work.

## What's here

- `scene-2026-08-13.json` — output of `scripts/bonfyre_habitat_compiler.py`,
  compiled from real `fabric.db` missions/artifacts, real `bonfyre_cms.db`
  wiki_page/course_lesson entries and `_relations`, and real `capital.db`
  content_genomes/relationship_episodes. Every node's `source` field points
  at the exact row it was compiled from.
- `floor-plan-viewer-2026-08-13.html` — first pass: a static blueprint-style
  floor plan, click a room/object/character to see its source. Superseded
  by the version below same day, kept for the iteration history.
- `town-2026-08-13.html` — second pass: a walkable 2D top-down town in the
  Smallville / Generative-Agents interaction style, static single-screen
  diorama, flat rectangle sprites. Correct interaction model, too thin a
  world and too plain a render -- superseded same day.
- `town-2026-08-13-v2.html` — third pass, current: rebuilt after actually
  reading `game-scene.ts`/`player.ts`/`agents.ts` in
  github.com/nmatter1/smallville rather than just the mechanics doc. Real
  differences this pass adds: a world larger than the viewport with a
  camera that follows the player (their `Player.update()` does exactly
  this via a camera dolly), Y-sorted draw order so sprites overlap
  correctly by vertical position (their `setDepth(this.y)`), chibi
  characters with distinct hair/shirt palettes and a directional
  walk-cycle instead of flat rectangles, peaked-roof cottage buildings
  with a window/door/sign/status pennant instead of bare rooms, seeded
  ground decoration (trees/bushes/flowers/lamps) so the world reads as a
  town instead of three boxes on grass, and a speech-bubble ambient tag
  instead of a translucent label. Their LimeZu tileset is still licensed
  art and still isn't reused -- everything is drawn procedurally. Same
  real data, same source-of-truth discipline throughout. Published live
  at: https://claude.ai/code/artifact/a0580535-e11c-4c1d-8696-e51d869be659
- `town-2026-08-13-v3.html` — fourth pass, current: sprites and item icons
  rebuilt as true pixel-grid art instead of smooth canvas primitives.
  Characters are a programmatically generated 14x22-unit chibi rig (shaped
  hair with bangs/crown/side coverage, direction-aware eyes, tapered
  shirt with a shaded far side, skin-toned arms, two-frame alternating-
  stride legs, boots) drawn pixel-by-pixel with an automatic 1px dark
  outline (the standard pixel-art outline trick), palette-swapped per
  resident. Items got distinct detailed icons per real kind instead of
  one generic box: documents are a desk with a dog-eared paper and
  visible text lines, wiki pages are a corkboard with a pinned note,
  lesson boards are a bookcase with varied book spines. Buildings gained
  shingle-course roof shading, a smoking chimney, a mullioned
  shutter-and-flower-box window, and a plank door with a real handle.
  Published live at the same URL, replacing the prior pass there.

## What this prototype does NOT implement

Per the vision doc: no `EmbodimentProfile` selection (objects get one fixed
visual form, not a chosen manifestation), no sound/acoustic grammar, no
`BonfyreNarrate` world narration, no time-of-day/decay state, no
cross-Bonfyre federation or `FederationBoundary`, no LambdaTensor-learned
spatial motifs, no drag-and-drop intent classification. It proves exactly
one thing: **a spatial scene can be compiled deterministically from real
graph state, with every object's identity traceable to its source of
truth** — the foundational claim the rest of Habitat is built on.

## Regenerating

```
python3 scripts/bonfyre_habitat_compiler.py /tmp/scene.json
```

Then re-run the injection step (see git history for the exact script used
2026-08-13) to embed the fresh scene into a copy of the viewer HTML.
