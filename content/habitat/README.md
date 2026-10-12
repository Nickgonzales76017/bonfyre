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
- `town-2026-08-13-v4.html` — fifth pass, current: rebuilt against the
  actual reference images this time, not just the code. Downloaded and
  viewed the Generative Agents paper (arxiv.org/pdf/2304.03442, Figures
  1/3/4) directly, plus read `agents.ts`'s real sprite-sheet frame-index
  math. The reference's characters are small and simple — the real detail
  is in speech-bubble callouts ("ML:", "IR: 😊") and richly furnished
  rooms (bed+nightstand, stove+counter+table, bathroom fixtures, dense
  2-tone tile floor). Replaced the plain name tag with a proper callout
  bubble (initials + a status glyph, rounded box, tail, matching Fig. 4
  exactly), tightened the floor checker, and gave every room fixed
  furniture beyond the real data objects — a wall bookshelf, a filing
  cabinet, a potted plant, a chair pair — all pixel-grid, all procedural.
  Published live at the same URL.
- `town-2026-08-13-v5.html` — sixth pass, current: fixed a real structural
  bug the last four passes shared. Buildings were a flat top-down floor
  plan with a roof triangle floating disconnected above it (roof base at
  `y-50`, door/window at `y+h` -- nothing in between actually connected
  them). That's not the 16/32-bit dimetric look real SNES-era top-down
  RPGs use (Stardew Valley, RPG Maker VX, Secret of Mana), where the roof
  and a real, tall, textured back-wall face are one continuous structure.
  Added `WALL_H`, a proper back-wall band directly under the roof eave
  (with an eave shadow cast onto it so the two visually read as one
  piece), moved the window from the floor-level "front" to the back-wall
  face where it actually belongs, added side-wall returns for depth, kept
  the door at the near/open side (unchanged gameplay collision). Published
  live at the same URL.

- `town-2026-08-13-v6.html` + `scene-2026-08-13-v6.json` — seventh pass,
  current, published as a **new** artifact:
  https://claude.ai/code/artifact/0d5dda12-fdbc-4d1d-8eea-ab5bda81b457

  Two real problems fixed and one real expansion.

  **1. The dimetric fix finally landed.** v6's `WALL_H` was 30px against a
  230px-deep room -- structurally correct, visually a hairline, which is
  why it kept reading as a floor plan with a roof over it no matter how
  correct the math was. Wall is now 88px with a 64px roof rise (~40% of
  building depth, the proportion 16-bit dimetric top-down actually uses),
  and at that size the face needed real material: horizontal siding
  courses, vertical stud shadows, a light-catch under the eave, a dark
  baseboard at the wall/floor junction, and lit/shaded corner-post
  returns. Windows became a properly-sized pair set into the wall face
  instead of one stretched to fill it. `TOP` moved to 230 so the taller
  structure clears world Y=0 instead of being clipped by the camera clamp.

  **2. Roof and wall can no longer desync.** They were two functions
  (`drawBuildingBack`/`drawBuildingFront`) run as two separate passes over
  all rooms. Now one `drawBuilding(room)` computes `wallTopY`/`peakY`/
  `baseY` once and every draw call uses those same locals, with the eave
  shadow drawn last directly over the seam.

  **3. The compiler now reads the rest of Bonfyre**, per the vision doc's
  "REAL BONFYRE STATE" list -- not just missions and content. Each table
  gets the built form its semantics already imply:

  | Real source | Rows | Spatial form |
  |---|---|---|
  | `capital.db human_gates` | 6 | Gates on Gate Road — open ones stand open, blocked ones are barred |
  | `capital.db offers` + `assets` + `commercial_experiments` | 4 | Market Row stalls — one crate per experiment actually run |
  | `capital.db evidence_ledger` | 8 | Evidence Board — verified claims pinned green |
  | `capital.db learning_ledger` | 3 | Standing-stone monuments — adopted ones lit |
  | `capital.db capital_actions` | 93 → 9 groups | Work Board — stacked bar by real status |
  | `fabric.db receipts` + `events` | 6 | Receipt-chain obelisk, one lit ring per chained entry |
  | `fabric.db roots` | 9 | Foundation Yard stones — height by durability, seal by trust level |

  25 new interactive entities, all real rows, each with its `source` in
  the inspector panel. Empty tables produce empty lists, never placeholder
  scenery. Verified by executing the render path headlessly against a
  stubbed canvas: 3 frames clean, 0 bad coordinates, all 25 inspector
  dialogs render without error.

- `town-2026-08-13-v7.html` + `scene-2026-08-13-v7.json` — eighth pass,
  current: https://claude.ai/code/artifact/77761eef-8771-4ce3-8121-884786aeb495

  Every previous pass built the *town* and skipped the *primitives*. The
  vision doc lays out roughly a dozen specific mechanisms and v6 shipped
  none of them. This pass implements them:

  **EmbodimentProfile (real, 12 of them).** An object no longer "has a
  sprite" — it has `spatial_forms[]`, `interaction_ports[]`,
  `visual_weight`, `attention_weight`, `audio_signature`,
  `narration_policy` and a `surface_fallback`. `V` cycles the embodiment;
  the record underneath never changes.

  **Five-class intent classification + Review Gate.** The load-bearing
  safety rule. Every port is `decorative` / `projection` / `attachment` /
  `work` / `authority`, shown color-coded before you act. `authority`
  **cannot execute from the world at all** — it opens a Review Gate a
  human answers, and even confirming records an intent rather than writing
  to any ledger. The intent log (`Tab`) states this on its header.

  **Acoustic grammar (`M`).** Procedural WebAudio, no assets. Fireplace
  bed = healthy idle system; proximity hum = local work running; static =
  unresolved external evidence; bell = WorkGraph completion; register tick
  = settled economic event. Continuous signatures follow real proximity to
  the object whose state they report.

  **World narration.** Entering a zone compiles "N things changed while
  you were away" from real `relationship_episodes` and `fabric.db events`
  timestamps — not a card list.

  **Time of day.** Real wall clock drives six phases with real lighting.

  **Zones + FederationBoundary (`Z`).** Four zones derived from real
  publication state, each rendering its actual boundary rules
  (discoverable / visible_to / copyable / referenceable / requires_approval).
  A private zone is visibly hatched off.

  **Latent compositions.** 6 doors and trails that exist because a
  composition is *already valid* in the real graph — a genome with
  verified evidence but no published page, a trusted relationship with no
  offer attached, a gate whose prerequisite is already recorded done.

  **Memory landmarks.** Importance computed as `evidence×2 + episodes`
  decides shelf → wall → permanent landmark, with one carved notch per
  real evidence row.

  **Role-based embodiment.** `agent ≠ model ≠ provider ≠ person` survives
  the projection: workshop operators carry a tool, visiting advisors carry
  a lantern and stand on a dashed visitor plinth, plain participants carry
  neither — all keyed off the real `role` string.

  **Embodied verbs → composition verbs.** walk/look/place/give/remember
  compile to Add / Use with / Show as / Try / Run on.

  Verified headlessly (execution, not appearance): 45 entities, 0 bad
  coordinates, 3 clean frames, and 14 feature assertions — including that
  an `authority` port provably opens a gate instead of acting, and that
  role embodiments actually differ.

## What this prototype does NOT implement

As of v7 the remaining gaps are narrower and specific:

- **Cross-Bonfyre federation is declared, not connected.** `FederationBoundary`
  rules are real and rendered, but there is no second Bonfyre to visit, no
  `AssetPassport` issuance, and no transfer semantics (copy vs delegation vs
  StitchWire vs ForeignTwin) — those need a real peer.
- **LambdaTensor-learned spatial motifs.** Layout weight comes from real
  `semantic_gravity` counts, but no `HabitatBasis` is learned across
  habitats — that requires many compiled worlds to find recurrence in.
- **Decay state.** Landmarks rise by importance; nothing yet sinks or
  weathers over time.
- **Authoritative execution.** Deliberate: confirming a Review Gate records
  an intent and does **not** write to `capital.db`/`fabric.db`. Wiring the
  real effect path is a separate, review-gated piece of work.

What it does prove, now more than the original claim: a spatial scene can
be compiled deterministically from real graph state, **every object's
identity traces to its source of truth, and the projection provably cannot
mutate what it projects.**

## Regenerating

```
python3 scripts/bonfyre_habitat_compiler.py /tmp/scene.json
```

Then re-run the injection step (see git history for the exact script used
2026-08-13) to embed the fresh scene into a copy of the viewer HTML.
