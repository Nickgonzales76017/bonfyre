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
- `floor-plan-viewer-2026-08-13.html` — a static rendering of that scene
  (blueprint-style floor plan, click a room/object/character to see its
  source). Published live at:
  https://claude.ai/code/artifact/a0580535-e11c-4c1d-8696-e51d869be659

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
