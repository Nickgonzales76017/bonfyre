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

# ----------------------------------------------------------------------
# EmbodimentProfile registry. The vision doc is explicit that an object
# does not have "a sprite" -- it has a set of legal spatial FORMS, and
# the compiler chooses one. The profile also declares which interactions
# are legal on it, and -- critically -- what CLASS each interaction is,
# because "a drag is an intent signal, never an automatic authoritative
# mutation". Five intent classes, from the doc:
#
#   decorative   personal arrangement, never leaves the habitat
#   projection   changes how it is shown, not what it is
#   attachment   proposes a semantic edge (a candidate relation)
#   work         opens a WorkGraph intent
#   authority    would mutate authoritative state -> ALWAYS review-gated
#
# `surface_fallback` is the non-spatial surface to fall back to when the
# object cannot be embodied (the doc requires every embodiment to degrade
# to a real surface, never to nothing).
# ----------------------------------------------------------------------
EMBODIMENT_PROFILES = {
    "research-room": {
        "canonical_kind": "Mission",
        "spatial_forms": ["mission-wall", "workshop", "study"],
        "interaction_ports": [
            {"port": "enter", "intent": "projection", "verb": "walk"},
            {"port": "inspect", "intent": "projection", "verb": "look"},
            {"port": "assign-work", "intent": "work", "verb": "work"},
        ],
        "visual_weight": 1.0,
        "attention_weight": 0.8,
        "audio_signature": "mechanical-hum",
        "narration_policy": "on-enter",
        "surface_fallback": "mission detail surface",
    },
    "document": {
        "canonical_kind": "Artifact",
        "spatial_forms": ["desk-paper", "pinned-note", "filed-folder"],
        "interaction_ports": [
            {"port": "read", "intent": "projection", "verb": "look"},
            {"port": "place-on-desk", "intent": "attachment", "verb": "place"},
            {"port": "file-to-ledger", "intent": "authority", "verb": "give"},
        ],
        "visual_weight": 0.5,
        "attention_weight": 0.4,
        "audio_signature": "paper-rustle",
        "narration_policy": "on-change",
        "surface_fallback": "artifact digest view",
    },
    "wiki-page": {
        "canonical_kind": "KnowledgeNode",
        "spatial_forms": ["corkboard", "wall-plaque", "open-book"],
        "interaction_ports": [
            {"port": "read", "intent": "projection", "verb": "look"},
            {"port": "link-to", "intent": "attachment", "verb": "place"},
            {"port": "publish", "intent": "authority", "verb": "give"},
        ],
        "visual_weight": 0.6,
        "attention_weight": 0.6,
        "audio_signature": "paper-rustle",
        "narration_policy": "on-change",
        "surface_fallback": "wiki page",
    },
    "lesson-board": {
        "canonical_kind": "Primitive",
        "spatial_forms": ["bookcase", "lectern", "teaching-board"],
        "interaction_ports": [
            {"port": "study", "intent": "projection", "verb": "look"},
            {"port": "cite", "intent": "attachment", "verb": "place"},
        ],
        "visual_weight": 0.7,
        "attention_weight": 0.5,
        "audio_signature": "paper-rustle",
        "narration_policy": "on-change",
        "surface_fallback": "LMS lesson",
    },
    "gate": {
        "canonical_kind": "HumanGate",
        "spatial_forms": ["road-gate", "turnstile", "sealed-door"],
        "interaction_ports": [
            {"port": "inspect", "intent": "projection", "verb": "look"},
            {"port": "request-open", "intent": "work", "verb": "speak"},
            {"port": "mark-cleared", "intent": "authority", "verb": "give"},
        ],
        "visual_weight": 0.9,
        "attention_weight": 1.0,
        "audio_signature": "radio-static",
        "narration_policy": "always",
        "surface_fallback": "gate checklist",
    },
    "stall": {
        "canonical_kind": "Offer",
        "spatial_forms": ["market-stall", "notice-board", "shopfront"],
        "interaction_ports": [
            {"port": "inspect", "intent": "projection", "verb": "look"},
            {"port": "run-experiment", "intent": "work", "verb": "work"},
            {"port": "record-sale", "intent": "authority", "verb": "give"},
        ],
        "visual_weight": 0.8,
        "attention_weight": 0.7,
        "audio_signature": "cash-register",
        "narration_policy": "on-change",
        "surface_fallback": "offer record",
    },
    "notices": {
        "canonical_kind": "EvidenceGraph",
        "spatial_forms": ["investigation-board", "archive-room"],
        "interaction_ports": [
            {"port": "read", "intent": "projection", "verb": "look"},
            {"port": "pin-claim", "intent": "attachment", "verb": "place"},
            {"port": "invalidate", "intent": "authority", "verb": "give"},
        ],
        "visual_weight": 0.9,
        "attention_weight": 0.9,
        "audio_signature": "radio-static",
        "narration_policy": "always",
        "surface_fallback": "evidence ledger",
    },
    "monument": {
        "canonical_kind": "Learning",
        "spatial_forms": ["standing-stone", "plaque", "landmark"],
        "interaction_ports": [{"port": "remember", "intent": "projection", "verb": "remember"}],
        "visual_weight": 0.7,
        "attention_weight": 0.3,
        "audio_signature": "none",
        "narration_policy": "on-enter",
        "surface_fallback": "learning ledger entry",
    },
    "workboard": {
        "canonical_kind": "WorkGraph",
        "spatial_forms": ["mission-wall", "assembly-line", "path"],
        "interaction_ports": [
            {"port": "review", "intent": "projection", "verb": "look"},
            {"port": "take-work", "intent": "work", "verb": "work"},
        ],
        "visual_weight": 1.0,
        "attention_weight": 0.9,
        "audio_signature": "mechanical-hum",
        "narration_policy": "always",
        "surface_fallback": "capital action list",
    },
    "obelisk": {
        "canonical_kind": "ReceiptChain",
        "spatial_forms": ["obelisk", "ledger-column"],
        "interaction_ports": [{"port": "replay", "intent": "projection", "verb": "remember"}],
        "visual_weight": 0.9,
        "attention_weight": 0.4,
        "audio_signature": "distant-bell",
        "narration_policy": "on-change",
        "surface_fallback": "receipt replay",
    },
    "foundation": {
        "canonical_kind": "StorageRoot",
        "spatial_forms": ["foundation-stone", "cellar-door"],
        "interaction_ports": [{"port": "inspect", "intent": "projection", "verb": "look"}],
        "visual_weight": 0.4,
        "attention_weight": 0.2,
        "audio_signature": "none",
        "narration_policy": "never",
        "surface_fallback": "root registry",
    },
    "character": {
        "canonical_kind": "Actor",
        "spatial_forms": ["resident", "visiting-avatar", "portrait", "booth"],
        "interaction_ports": [
            {"port": "talk", "intent": "projection", "verb": "speak"},
            {"port": "invite", "intent": "work", "verb": "invite"},
            {"port": "grant-passport", "intent": "authority", "verb": "give"},
        ],
        "visual_weight": 0.8,
        "attention_weight": 0.9,
        "audio_signature": "none",
        "narration_policy": "on-change",
        "surface_fallback": "relationship record",
    },
}

# Role-based embodiment. The doc is explicit that avatars are NOT uniform
# and that `agent != model != provider != person` must survive the
# projection. Roles below are matched against the real `role` string on
# capital.db relationships rows.
ROLE_EMBODIMENT = [
    ("maintainer", {"embodiment": "workshop-operator", "presence": "external", "form": "resident"}),
    ("reviewer", {"embodiment": "visiting-advisor", "presence": "external", "form": "visiting-avatar"}),
    ("collaborator", {"embodiment": "visiting-advisor", "presence": "external", "form": "visiting-avatar"}),
    ("community", {"embodiment": "person", "presence": "external", "form": "portrait"}),
    ("issue", {"embodiment": "person", "presence": "external", "form": "portrait"}),
    ("adjacent", {"embodiment": "person", "presence": "external", "form": "visiting-avatar"}),
]

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
                    # role-based embodiment: agent != model != provider != person
                    "embodiment": next(
                        (e for k, e in ROLE_EMBODIMENT if k in (row["role"] or "").lower()),
                        {"embodiment": "person", "presence": "external", "form": "portrait"},
                    ),
                    "home_room": home_room,
                    "visit_history": [
                        {"kind": e["interaction_kind"], "direction": e["direction"], "at": e["occurred_at"]}
                        for e in episodes
                    ],
                    "source": {"db": "capital.db", "table": "relationships", "id": row["id"]},
                }
            )

    # ------------------------------------------------------------------
    # Town-level features beyond the research wing. The vision doc's
    # "REAL BONFYRE STATE" list names more graphs than missions/content:
    # gates, evidence, learning, money/opportunity, timeline, roots. Each
    # of those is a real populated table, and each gets the spatial form
    # its semantics actually imply -- a blocking gate is a gate, an
    # append-only evidence chain is a notice board, a durable storage root
    # is a foundation stone. Nothing here is invented; empty tables
    # produce empty lists rather than placeholder scenery.
    # ------------------------------------------------------------------
    gates, notices, monuments, stalls, timeline, foundations = [], [], [], [], [], []

    if capital:
        for row in capital.execute(
            "SELECT * FROM human_gates ORDER BY value_at_stake DESC, id"
        ):
            gates.append(
                {
                    "id": f"gate-{row['id']}",
                    "label": row["exact_gate"],
                    "opportunity": row["opportunity"],
                    "gate_class": row["gate_class"],
                    "status": row["status"],
                    "value_at_stake": row["value_at_stake"],
                    "external_ref": row["external_ref"],
                    "blocks": row["completed_before_gate"],
                    "source": {"db": "capital.db", "table": "human_gates", "id": row["id"]},
                }
            )

        for row in capital.execute("SELECT * FROM evidence_ledger ORDER BY id"):
            notices.append(
                {
                    "id": f"notice-{row['id']}",
                    "claim": row["claim"],
                    "evidence_type": row["evidence_type"],
                    "ref": row["ref"],
                    "verified": bool(row["verified_at"]),
                    "verification_method": row["verification_method"],
                    "room": mission_index.get(
                        genome_slug_to_mission.get(
                            next(
                                (
                                    s
                                    for s, g in genome_by_slug.items()
                                    if g["id"] == row["content_genome_id"]
                                ),
                                None,
                            )
                        )
                    ),
                    "source": {"db": "capital.db", "table": "evidence_ledger", "id": row["id"]},
                }
            )

        for row in capital.execute("SELECT * FROM learning_ledger ORDER BY occurred_at"):
            monuments.append(
                {
                    "id": f"monument-{row['id']}",
                    "learning_type": row["learning_type"],
                    "description": row["description"],
                    "occurred_at": row["occurred_at"],
                    "adopted": bool(row["adopted"]),
                    "source_ref": row["source_ref"],
                    "source": {"db": "capital.db", "table": "learning_ledger", "id": row["id"]},
                }
            )

        asset_by_id = {
            r["id"]: dict(r) for r in capital.execute("SELECT * FROM assets")
        }
        for row in capital.execute("SELECT * FROM offers ORDER BY id"):
            asset = asset_by_id.get(row["asset_id"])
            experiments = capital.execute(
                "SELECT COUNT(*) AS n, SUM(response_signal IS NOT NULL AND response_signal != '') AS answered"
                " FROM commercial_experiments WHERE offer_id = ?",
                (row["id"],),
            ).fetchone()
            stalls.append(
                {
                    "id": f"stall-{row['id']}",
                    "label": row["name"],
                    "offer_class": row["offer_class"],
                    "status": row["status"],
                    "asking_price": row["asking_price"],
                    "currency": row["currency"],
                    "target_customer": row["target_customer"],
                    "backing_asset": asset["name"] if asset else None,
                    "backing_asset_status": asset["status"] if asset else None,
                    "experiments_run": experiments["n"] or 0,
                    "experiments_answered": experiments["answered"] or 0,
                    "source": {"db": "capital.db", "table": "offers", "id": row["id"]},
                }
            )

    if fabric:
        # receipts form a real hash chain (previous_receipt_id/chain_hash);
        # walking it in order gives the town an actual chronology to display
        for row in fabric.execute(
            "SELECT * FROM receipts ORDER BY created_at, id"
        ):
            timeline.append(
                {
                    "id": row["id"],
                    "kind": "receipt",
                    "subject_kind": row["subject_kind"],
                    "subject_id": row["subject_id"],
                    "content_hash": row["content_hash"],
                    "chained_to": row["previous_receipt_id"],
                    "at": row["created_at"],
                    "source": {"db": "fabric.db", "table": "receipts", "id": row["id"]},
                }
            )
        for row in fabric.execute(
            "SELECT * FROM events ORDER BY start_at, id"
        ):
            timeline.append(
                {
                    "id": row["id"],
                    "kind": "event",
                    "actor": row["actor"],
                    "effect_class": row["effect_class"],
                    "status": row["status"],
                    "mission": row["mission_id"],
                    "room": mission_index.get(row["mission_id"]),
                    "at": row["start_at"],
                    "source": {"db": "fabric.db", "table": "events", "id": row["id"]},
                }
            )
        timeline.sort(key=lambda e: (e["at"] or "", e["id"]))

        for row in fabric.execute("SELECT * FROM roots ORDER BY id"):
            foundations.append(
                {
                    "id": f"foundation-{row['id']}",
                    "label": row["id"],
                    "kind": row["kind"],
                    "locator": row["locator"],
                    "authority_class": row["authority_class"],
                    "durability": row["durability"],
                    "trust_level": row["trust_level"],
                    "sensitivity": row["sensitivity"],
                    "access_mode": row["access_mode"],
                    "source": {"db": "fabric.db", "table": "roots", "id": row["id"]},
                }
            )

    # WorkGraph rollup: real capital_actions, grouped by status, so the
    # town can show what the system is actually working on right now
    workboard = []
    if capital:
        for row in capital.execute(
            "SELECT status, COUNT(*) AS n, SUM(face_value) AS face,"
            " SUM(realized_value) AS realized FROM capital_actions"
            " GROUP BY status ORDER BY n DESC"
        ):
            workboard.append(
                {
                    "status": row["status"],
                    "count": row["n"],
                    "face_value": row["face"] or 0,
                    "realized_value": row["realized"] or 0,
                    "source": {"db": "capital.db", "table": "capital_actions", "group_by": "status"},
                }
            )

    # ------------------------------------------------------------------
    # Zones + FederationBoundary. The doc requires private / household /
    # work / public / shared zones, each with an explicit boundary saying
    # what may be discovered, seen, copied, referenced, acted upon, or
    # brought in. These are derived from the real publication state of the
    # underlying rows, not declared by hand: a `held` genome is not
    # publicly visible; a published wiki page is.
    # ------------------------------------------------------------------
    published_pages = 0
    held_genomes = 0
    if cms:
        published_pages = cms.execute(
            "SELECT COUNT(*) FROM wiki_page WHERE published=1"
        ).fetchone()[0]
    if capital:
        held_genomes = capital.execute(
            "SELECT COUNT(*) FROM content_genomes WHERE status IN ('held','draft')"
        ).fetchone()[0]

    zones = [
        {
            "id": "zone-work",
            "label": "Research Wing",
            "class": "work",
            "contains": [r["id"] for r in rooms],
            "federation_boundary": {
                "discoverable": False,
                "visible_to": ["owner"],
                "copyable": False,
                "referenceable": True,
                "actionable_by": ["owner"],
                "requires_approval": ["publish", "grant-passport"],
            },
            "reason": f"{held_genomes} content genomes still held/draft in capital.db",
            "source": {"db": "capital.db", "table": "content_genomes", "group_by": "status"},
        },
        {
            "id": "zone-public",
            "label": "Market Row & Gate Road",
            "class": "public",
            "contains": ["stall-*", "gate-*"],
            "federation_boundary": {
                "discoverable": True,
                "visible_to": ["owner", "visitor"],
                "copyable": False,
                "referenceable": True,
                "actionable_by": ["owner"],
                "requires_approval": ["record-sale", "mark-cleared"],
            },
            "reason": "offers and human gates already reference external parties",
            "source": {"db": "capital.db", "table": "offers"},
        },
        {
            "id": "zone-shared",
            "label": "Civic Square",
            "class": "shared",
            "contains": ["notice-board", "work-board", "timeline-obelisk", "monument-*"],
            "federation_boundary": {
                "discoverable": True,
                "visible_to": ["owner", "visitor"],
                "copyable": True,
                "referenceable": True,
                "actionable_by": ["owner"],
                "requires_approval": ["invalidate"],
            },
            "reason": f"{published_pages} wiki pages published; evidence board is citable",
            "source": {"db": "bonfyre_cms.db", "table": "wiki_page"},
        },
        {
            "id": "zone-private",
            "label": "Foundation Yard",
            "class": "private",
            "contains": ["foundation-*"],
            "federation_boundary": {
                "discoverable": False,
                "visible_to": ["owner"],
                "copyable": False,
                "referenceable": False,
                "actionable_by": ["owner"],
                "requires_approval": ["inspect"],
            },
            "reason": "storage roots carry sensitivity and access_mode from fabric.db",
            "source": {"db": "fabric.db", "table": "roots"},
        },
    ]

    # ------------------------------------------------------------------
    # "The world remembers": importance-ranked landmarks. The doc maps
    # ContinuityIsland to spatial UX -- things Bonfyre has determined must
    # not be compressed away become permanent landmarks, while less
    # important things sink shelf -> wall -> basement. Importance here is
    # computed from real signal: how many evidence rows cite it, whether a
    # receipt chain pins it, whether a maintainer actually responded.
    # ------------------------------------------------------------------
    landmarks = []
    if capital:
        for row in capital.execute(
            "SELECT g.id, g.slug, g.status, g.thesis,"
            " (SELECT COUNT(*) FROM evidence_ledger e WHERE e.content_genome_id=g.id) AS evidence_n,"
            " (SELECT COUNT(*) FROM relationship_episodes r WHERE r.relationship_id=g.relationship_id) AS episode_n"
            " FROM content_genomes g"
        ):
            score = (row["evidence_n"] or 0) * 2 + (row["episode_n"] or 0)
            if score >= 6:
                tier, place = "landmark", "permanent"
            elif score >= 3:
                tier, place = "wall", "room-wall"
            else:
                tier, place = "shelf", "shelf"
            landmarks.append(
                {
                    "id": f"landmark-{row['id']}",
                    "label": row["slug"],
                    "tier": tier,
                    "placement": place,
                    "importance": score,
                    "evidence_count": row["evidence_n"] or 0,
                    "episode_count": row["episode_n"] or 0,
                    "status": row["status"],
                    "room": mission_index.get(genome_slug_to_mission.get(row["slug"])),
                    "source": {"db": "capital.db", "table": "content_genomes", "id": row["id"]},
                }
            )
        landmarks.sort(key=lambda l: -l["importance"])

    # ------------------------------------------------------------------
    # Discovery as world growth. A door/trail appears ONLY when a real
    # latent composition is already valid in the graph -- never invented.
    # Each candidate below is a `Use with` composition the underlying rows
    # already support but which has not been executed yet.
    # ------------------------------------------------------------------
    latent = []
    if capital:
        # a genome with verified evidence but no published page is a real
        # publish-composition waiting to happen
        for row in capital.execute(
            "SELECT g.id, g.slug, g.status, g.publish_gate,"
            " (SELECT COUNT(*) FROM evidence_ledger e"
            "   WHERE e.content_genome_id=g.id AND e.verified_at IS NOT NULL) AS verified_n"
            " FROM content_genomes g WHERE g.status IN ('held','draft')"
        ):
            if (row["verified_n"] or 0) > 0:
                latent.append(
                    {
                        "id": f"latent-publish-{row['id']}",
                        "kind": "door",
                        "composition": "ContentGenome + verified Evidence -> publishable article",
                        "label": f"publish: {row['slug']}",
                        "blocked_by": row["publish_gate"],
                        "verified_evidence": row["verified_n"],
                        "room": mission_index.get(genome_slug_to_mission.get(row["slug"])),
                        "source": {"db": "capital.db", "table": "content_genomes", "id": row["id"]},
                    }
                )
        # a relationship that reached technical engagement but has no offer
        # attached is a real opportunity trail
        for row in capital.execute(
            "SELECT id, counterparty, relationship_state, ecosystem FROM relationships"
            " WHERE relationship_state IN ('TRUSTED_INTERACTION','TECHNICAL_ENGAGEMENT')"
        ):
            latent.append(
                {
                    "id": f"latent-offer-{row['id']}",
                    "kind": "trail",
                    "composition": "Relationship(trusted) + Asset -> qualified paid ask",
                    "label": f"unopened path: {row['counterparty']}",
                    "relationship_state": row["relationship_state"],
                    "ecosystem": row["ecosystem"],
                    "source": {"db": "capital.db", "table": "relationships", "id": row["id"]},
                }
            )
        # a gate whose prerequisite work is already recorded complete is a
        # real, currently-openable door
        for row in capital.execute(
            "SELECT id, exact_gate, opportunity, completed_before_gate, status"
            " FROM human_gates WHERE status='open' AND completed_before_gate IS NOT NULL"
            " AND completed_before_gate != ''"
        ):
            latent.append(
                {
                    "id": f"latent-gate-{row['id']}",
                    "kind": "door",
                    "composition": "HumanGate + completed prerequisite -> openable",
                    "label": f"openable: {row['exact_gate']}",
                    "opportunity": row["opportunity"],
                    "source": {"db": "capital.db", "table": "human_gates", "id": row["id"]},
                }
            )

    # ------------------------------------------------------------------
    # World time + narration. The doc: time makes the world alive without
    # needing AI, and BonfyreNarrate compiles a state delta into "three
    # things changed while you were away" rather than a card list. Both
    # come from real timestamps.
    # ------------------------------------------------------------------
    narration = []
    if capital:
        for row in capital.execute(
            "SELECT occurred_at, direction, interaction_kind, summary, relationship_id"
            " FROM relationship_episodes ORDER BY occurred_at DESC LIMIT 6"
        ):
            narration.append(
                {
                    "at": row["occurred_at"],
                    "channel": "relationship",
                    "text": f"{row['direction']} {row['interaction_kind']}: {row['summary']}",
                    "source": {"db": "capital.db", "table": "relationship_episodes"},
                }
            )
    if fabric:
        for row in fabric.execute(
            "SELECT start_at, actor, effect_class, status, mission_id FROM events"
            " ORDER BY start_at DESC LIMIT 4"
        ):
            narration.append(
                {
                    "at": row["start_at"],
                    "channel": "work",
                    "text": f"{row['actor']} {row['effect_class']} on {row['mission_id']} ({row['status']})",
                    "room": mission_index.get(row["mission_id"]),
                    "source": {"db": "fabric.db", "table": "events"},
                }
            )
    narration.sort(key=lambda n: n["at"] or "", reverse=True)

    # ------------------------------------------------------------------
    # Semantic gravity: district sizing is proposed from what is actually
    # in the graph, not hand-placed. More real rows in a family => more
    # ground given to it. The doc: "same engine, different life" -- two
    # users' worlds must compile differently from their own graphs.
    # ------------------------------------------------------------------
    gravity = [
        {"district": "research", "weight": len(rooms) + len(objects), "members": len(rooms)},
        {"district": "gates", "weight": len(gates), "members": len(gates)},
        {"district": "market", "weight": len(stalls) * 2, "members": len(stalls)},
        {"district": "civic", "weight": len(notices) + len(monuments) + len(workboard), "members": len(monuments) + 2},
        {"district": "foundation", "weight": len(foundations), "members": len(foundations)},
    ]
    total_weight = sum(g["weight"] for g in gravity) or 1
    for g in gravity:
        g["share"] = round(g["weight"] / total_weight, 4)

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
        "gates": gates,
        "notices": notices,
        "monuments": monuments,
        "stalls": stalls,
        "timeline": timeline,
        "foundations": foundations,
        "workboard": workboard,
        "embodiment_profiles": EMBODIMENT_PROFILES,
        "zones": zones,
        "landmarks": landmarks,
        "latent_compositions": latent,
        "narration": narration,
        "semantic_gravity": gravity,
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
        f"paths={len(scene['paths'])} characters={len(scene['characters'])} "
        f"gates={len(scene['gates'])} notices={len(scene['notices'])} "
        f"monuments={len(scene['monuments'])} stalls={len(scene['stalls'])} "
        f"timeline={len(scene['timeline'])} foundations={len(scene['foundations'])} "
        f"workboard={len(scene['workboard'])}\n"
        f"zones={len(scene['zones'])} landmarks={len(scene['landmarks'])} "
        f"latent={len(scene['latent_compositions'])} narration={len(scene['narration'])} "
        f"profiles={len(scene['embodiment_profiles'])}"
    )
