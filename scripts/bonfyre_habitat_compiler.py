#!/usr/bin/env python3
"""
bonfyre_habitat_compiler.py -- first real HabitatCompiler prototype.

Compiles a spatial scene description from REAL Bonfyre state, not invented
placeholder content:

  fabric.db (missions, artifacts, events)
  + bonfyre_cms.db (wiki_page/course_lesson entries, _relations)
  + capital.db (content_genomes, relationship_episodes)
        |
        v
  scene.json  (buildings/rooms/objects/paths/characters)

This is deliberately minimal: one building, one room per real mission, real
artifacts as room objects, real _relations as paths between rooms, real
relationship_episodes counterparties as visiting characters with a real
interaction history. No fabricated rooms, objects, or characters -- every
node in the output scene traces back to a specific row in a specific real
database, recorded in each node's `source` field.

This does not attempt embodiment-profile selection, sound, narration, time,
or cross-Bonfyre federation -- those are real, larger pieces of the Habitat
vision (see memory: bonfyre-habitat-vision) that this prototype does not
claim to implement. It proves one thing: a spatial scene can be compiled
deterministically from real graph state, with every object's identity and
status traceable to its source of truth.
"""
import json
import os
import re
import sqlite3
import sys

FABRIC_DB = os.path.expanduser("~/Library/Application Support/Bonfyre/fabric.db")
CMS_DB = "/Users/nickgonzales/Documents/Bonfyre/bonfyre-zig/bonfyre_cms.db"
CAPITAL_DB = os.path.expanduser(
    "~/Library/Application Support/Bonfyre/CapitalGym/capital.db"
)

STATUS_ROOM_STATE = {
    "held": "locked",       # real content, not yet allowed to leave the room
    "draft": "under-construction",
    "ready": "staged",
    "published": "open",
}


def connect(path):
    if not os.path.exists(path):
        print(f"warning: database not found, skipping: {path}", file=sys.stderr)
        return None
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


def compile_scene():
    fabric = connect(FABRIC_DB)
    cms = connect(CMS_DB)
    capital = connect(CAPITAL_DB)

    rooms = []
    objects = []
    paths = []
    characters = []

    genome_by_slug = {}
    if capital:
        for row in capital.execute("SELECT * FROM content_genomes"):
            genome_by_slug[row["slug"]] = dict(row)

    mission_rows = list(fabric.execute("SELECT * FROM missions")) if fabric else []
    mission_index = {}
    for i, m in enumerate(mission_rows):
        room_id = f"room-{m['id']}"
        mission_index[m["id"]] = room_id
        rooms.append(
            {
                "id": room_id,
                "kind": "research-room",
                "label": m["id"],
                "state": STATUS_ROOM_STATE.get(m["status"], m["status"]),
                "workgraph_cursor": m["workgraph_cursor"],
                "position": {"x": (i % 3) * 4, "y": 0, "z": (i // 3) * 4},
                "source": {"db": "fabric.db", "table": "missions", "id": m["id"]},
            }
        )

    if fabric:
        for e in fabric.execute(
            "SELECT * FROM events WHERE mission_id IS NOT NULL"
        ):
            mission_id = e["mission_id"]
            output_uri = e["output_uri"]
            if not output_uri or mission_id not in mission_index:
                continue
            art = fabric.execute(
                "SELECT * FROM artifacts WHERE uri=?", (output_uri,)
            ).fetchone()
            if not art:
                continue
            objects.append(
                {
                    "id": f"object-{art['digest'][:16]}",
                    "kind": "document",
                    "label": os.path.basename(art["uri"]),
                    "room": mission_index[mission_id],
                    "bytes": art["bytes"],
                    "digest": art["digest"],
                    "source": {
                        "db": "fabric.db",
                        "table": "artifacts",
                        "digest": art["digest"],
                    },
                }
            )

    # Cross-reference wiki_page id -> mission id via the real annotations
    # already written into content_genomes.update_dependencies_json tonight
    # (e.g. "real wiki_page entry id=2 ..." / "real fabric.db mission
    # agentguard-213-research-episode ..."), rather than guessing from slugs.
    mission_ids = [m["id"] for m in mission_rows]
    genome_wiki_to_mission = {}
    genome_slug_to_mission = {}
    if capital:
        for slug, g in genome_by_slug.items():
            deps = " ".join(json.loads(g["update_dependencies_json"] or "[]"))
            found_mission = next((mid for mid in mission_ids if mid in deps), None)
            if not found_mission:
                dir_match = re.search(r"research-episodes/([\w-]+)/", deps)
                if dir_match:
                    prefix = dir_match.group(1)
                    found_mission = next(
                        (mid for mid in mission_ids if mid.startswith(prefix)), None
                    )
            if found_mission:
                genome_slug_to_mission[slug] = found_mission
                wiki_match = re.search(r"wiki_page entry id=(\d+)", deps)
                if wiki_match:
                    genome_wiki_to_mission[int(wiki_match.group(1))] = found_mission

    wiki_page_room = {}
    if cms:
        for row in cms.execute(
            "SELECT id, title, route, published, status FROM wiki_page"
        ):
            mission_id = genome_wiki_to_mission.get(row["id"])
            page_room = mission_index.get(mission_id) if mission_id else None
            wiki_page_room[row["id"]] = page_room
            objects.append(
                {
                    "id": f"wiki-page-{row['id']}",
                    "kind": "wiki-page",
                    "label": row["title"],
                    "room": page_room,
                    "state": "open" if row["published"] else "held",
                    "source": {"db": "bonfyre_cms.db", "table": "wiki_page", "id": row["id"]},
                }
            )

        for row in cms.execute("SELECT id, title FROM course_lesson"):
            objects.append(
                {
                    "id": f"course-lesson-{row['id']}",
                    "kind": "lesson-board",
                    "label": row["title"],
                    "room": None,
                    "note": "shared across rooms via example_of relations, not owned by one room",
                    "source": {"db": "bonfyre_cms.db", "table": "course_lesson", "id": row["id"]},
                }
            )

        for rel in cms.execute(
            "SELECT * FROM _relations WHERE source_type='wiki_page' AND target_type='wiki_page'"
        ):
            a = wiki_page_room.get(rel["source_id"])
            b = wiki_page_room.get(rel["target_id"])
            if a and b and a != b:
                paths.append(
                    {
                        "id": f"path-{rel['id']}",
                        "from": a,
                        "to": b,
                        "relation": rel["relation"],
                        "source": {"db": "bonfyre_cms.db", "table": "_relations", "id": rel["id"]},
                    }
                )

    if capital:
        for row in capital.execute("SELECT * FROM relationships"):
            episodes = capital.execute(
                "SELECT interaction_kind, direction, occurred_at FROM relationship_episodes "
                "WHERE relationship_id=? ORDER BY occurred_at",
                (row["id"],),
            ).fetchall()
            home_room = None
            for slug, g in genome_by_slug.items():
                if g.get("relationship_id") == row["id"] and slug in genome_slug_to_mission:
                    home_room = mission_index.get(genome_slug_to_mission[slug])
            characters.append(
                {
                    "id": f"character-{row['id']}",
                    "label": row["counterparty"],
                    "role": row["role"],
                    "relationship_state": row["relationship_state"],
                    "home_room": home_room,
                    "visit_history": [
                        {"kind": e["interaction_kind"], "direction": e["direction"], "at": e["occurred_at"]}
                        for e in episodes
                    ],
                    "source": {"db": "capital.db", "table": "relationships", "id": row["id"]},
                }
            )

    for c in (fabric, cms, capital):
        if c:
            c.close()

    return {
        "schema_version": "bonfyre.habitat.scene.v0",
        "compiled_at": "compiled-at-runtime",
        "note": (
            "Prototype scene, not full HabitatCompiler. Every room/object/path/"
            "character has a `source` pointing at the real row it was compiled "
            "from -- nothing here is invented."
        ),
        "buildings": [
            {
                "id": "building-aurekai-research",
                "label": "Aurekai Research Wing",
                "rooms": [r["id"] for r in rooms],
            }
        ],
        "rooms": rooms,
        "objects": objects,
        "paths": paths,
        "characters": characters,
    }


if __name__ == "__main__":
    import datetime

    scene = compile_scene()
    scene["compiled_at"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    out_path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/bonfyre-habitat-scene.json"
    with open(out_path, "w") as f:
        json.dump(scene, f, indent=2)
    print(f"scene written: {out_path}")
    print(
        f"rooms={len(scene['rooms'])} objects={len(scene['objects'])} "
        f"paths={len(scene['paths'])} characters={len(scene['characters'])}"
    )
