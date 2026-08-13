#!/usr/bin/env python3
"""
bonfyre_habitat_mapgen.py -- the HabitatCompiler's map stage.

The vision is explicit that world topology is COMPILED FROM SEMANTIC
GRAVITY, not hand-designed and not borrowed: "HabitatCompiler proposes
room/building layout from what's actually in a user's graph ... same
engine, different life, so two users' worlds compile completely
differently."

So this does not place Bonfyre records onto somebody else's finished
town. It takes:

  scene.json          real Bonfyre graph state (bonfyre_habitat_compiler.py)
  room_stamps.json    real room templates lifted from the Smallville
                      Tiled map (nmatter1/smallville) -- their art and
                      their tile craftsmanship, used as vocabulary
  vocab.json          ground/floor/wall gids sampled from the same map

and compiles a NEW map whose districts are sized by
`scene.semantic_gravity`, whose building count comes from the real
number of missions/offers/roots, and whose furniture positions carry
the record they were compiled from.

Every tile drawn is a real tile from their tilesets. Every structure
placed is there because a real row exists.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ASSETS = os.environ.get(
    "HABITAT_ASSETS",
    "/private/tmp/claude-503/-Users-nickgonzales-Documents-Bonfyre/"
    "0bbdbcaa-919a-4255-b270-78fff0b8d2b2/scratchpad/assets",
)

GRID_W, GRID_H = 210, 150
MARGIN = 6


def load(path):
    with open(path) as f:
        return json.load(f)


# Room templates typed by what is ACTUALLY IN THEM (measured from the
# tileset composition of each captured stamp), so spatial form can follow
# semantic meaning instead of being handed out round-robin. That was the
# core incoherence: erpnext rendered as a grocery store because template
# choice ignored what the record meant.
#
#   0  dwelling     Room_Builder + Kitchen + Bedroom     private life
#   1  institution  Room_Builder + Classroom/Library     large domain estate
#   2  store        Room_Builder + Grocery               market, inventory
#   3  study        Room_Builder + Classroom (small)     focused research
#   4  shop         Room_Builder + Ice Cream Shop        small offer counter
STAMP_KIND = {0: "dwelling", 1: "institution", 2: "store", 3: "study", 4: "shop"}
FORM_FOR = {
    "AppPack": "institution",
    "Mission": "study",
    "Offer": "store",
    "StorageRoot": "dwelling",
    "EvidenceGraph": "institution",
    "WorkGraph": "institution",
    "Learning": "study",
    "ReceiptChain": "study",
}


def compile_map(scene):
    stamps = load(os.path.join(ASSETS, "room_stamps.json"))["rooms"]
    by_kind = {}
    for i, st in enumerate(stamps):
        by_kind.setdefault(STAMP_KIND.get(i, "study"), []).append(st)

    def stamp_for(canonical_kind, size_rank=0):
        pool = by_kind.get(FORM_FOR.get(canonical_kind, "study")) or stamps
        return pool[size_rank % len(pool)]
    vocab = load(os.path.join(ASSETS, "vocab.json"))
    ground_gid = vocab["ground"][0]
    path_gids = vocab["ground"][1:3]

    layers = {
        name: [[0] * GRID_W for _ in range(GRID_H)]
        for name in ("ground", "floors", "walls", "objects", "decor")
    }
    for y in range(GRID_H):
        for x in range(GRID_W):
            layers["ground"][y][x] = ground_gid

    markers = []
    labels = []
    blocked = [[0] * GRID_W for _ in range(GRID_H)]

    def stamp_at(stamp, ox, oy):
        """Blit a real captured room (all of its layers) at ox,oy."""
        src = stamp["layers"]
        for name, key in (
            ("floors", "interior_floors"),
            ("walls", "interior_walls"),
            ("objects", "interior_objects"),
            ("decor", "interior_decorations"),
        ):
            grid = src.get(key) or []
            for j, row in enumerate(grid):
                for i, gid in enumerate(row):
                    if not gid:
                        continue
                    x, y = ox + i, oy + j
                    if 0 <= x < GRID_W and 0 <= y < GRID_H:
                        layers[name][y][x] = gid
                        if name == "walls":
                            blocked[y][x] = 1
        # floors clear the block flag so interiors stay walkable
        for j, row in enumerate(src.get("interior_floors") or []):
            for i, gid in enumerate(row):
                if gid:
                    x, y = ox + i, oy + j
                    if 0 <= x < GRID_W and 0 <= y < GRID_H and not layers["walls"][y][x]:
                        blocked[y][x] = 0

    def on_furniture(ox, oy, st, nth=0):
        """Pick the nth furnished tile inside a placed stamp, so a marker
        stands on real furniture rather than floating over bare floor."""
        spots = []
        for j, row in enumerate(st["layers"].get("interior_objects") or []):
            for i, gid in enumerate(row):
                if gid:
                    spots.append((ox + i, oy + j))
        if not spots:
            return (ox + st["w"] // 2, oy + st["h"] // 2)
        return spots[(nth * 7) % len(spots)]

    def road(x0, x1, y, half=2):
        for x in range(max(0, x0), min(GRID_W, x1)):
            for dy in range(-half, half + 1):
                yy = y + dy
                if 0 <= yy < GRID_H and not layers["floors"][yy][x]:
                    layers["ground"][yy][x] = path_gids[(x + dy) % len(path_gids)]

    # ------------------------------------------------------------------
    # Semantic gravity decides how much ground each district gets. This
    # is the whole point: a graph with 9 storage roots and 3 missions
    # compiles a different town than one with 40 offers.
    # ------------------------------------------------------------------
    gravity = {g["district"]: g for g in scene.get("semantic_gravity", [])}
    order = ["apps", "research", "market", "civic", "foundation", "gates"]
    present = [d for d in order if gravity.get(d, {}).get("members")]
    total_share = sum(gravity[d]["share"] for d in present) or 1.0

    # Districts are horizontal BANDS spanning the full map width, with
    # vertical extent proportional to their semantic-gravity share. An
    # earlier version gave each district a narrow vertical column sized
    # by share, which left most of the map empty and wrapped buildings
    # into a single stacked file.
    district_span = {d: (MARGIN, GRID_W - MARGIN) for d in present}
    usable_h = GRID_H - MARGIN * 2
    ROW_Y = {}
    cy = MARGIN
    for d in present:
        share = gravity[d]["share"] / total_share
        band = max(20, int(usable_h * share))
        ROW_Y[d] = cy
        cy += band + 2
    ROW_Y.setdefault("gates", max(4, ROW_Y.get("research", 8) - 6))

    def place_row(district, items, canonical_kind, on_place, repeat=None):
        """Lay a district out left-to-right. `repeat(item)` may return >1 so
        that a genuinely larger record occupies genuinely more ground."""
        if district not in district_span:
            return
        x0, x1 = district_span[district]
        y = ROW_Y[district]
        cursor_x = x0
        row_y = y
        row_h = 0
        for i, item in enumerate(items):
            st = stamp_for(canonical_kind, i)
            halls = repeat(item) if repeat else 1
            width = st["w"] * halls + (halls - 1)
            if cursor_x + width > x1:
                cursor_x = x0
                row_y += row_h + 3
                row_h = 0
            if row_y + st["h"] >= GRID_H:
                break
            for k in range(halls):
                stamp_at(st, cursor_x + k * (st["w"] + 1), row_y)
            on_place(item, cursor_x, row_y, st, halls)
            cursor_x += width + 3
            row_h = max(row_h, st["h"])

    # --- app district: one building per real compiled Frappe app. The
    # room template is chosen by scale, so erpnext (502 doctypes) gets a
    # bigger structure than drive (12) -- the estate's real shape shows.
    def on_app(a, ox, oy, st, halls=1):
        labels.append({"x": ox + (st["w"] * halls) // 2, "y": oy - 1,
                       "text": a["app"],
                       "sub": f"{a['doctypes']} doctypes · {halls} hall(s) · {a['audit']}"})
        mx, my = on_furniture(ox, oy, st, 1)
        markers.append({"x": mx, "y": my,
                        "glyph": "🏛️" if a["status"] == "compiled" else "⚠️",
                        "kind": "civic", "civic": "app", "label": a["app"], "data": a})

    apps_sorted = sorted(scene.get("apps", []), key=lambda a: -a["doctypes"])
    place_row("apps", apps_sorted, "AppPack", on_app,
              repeat=lambda a: max(1, min(6, -(-a["doctypes"] // 100))))

    # --- research district: one real room per real fabric.db mission
    def on_mission(m, ox, oy, st, halls=1):
        labels.append({"x": ox + st["w"] // 2, "y": oy - 1,
                       "text": m["label"], "sub": m["workgraph_cursor"]})
        mx, my = on_furniture(ox, oy, st, 0)
        markers.append({"x": mx, "y": my, "glyph": "📋",
                        "kind": "room", "label": m["label"], "data": m})
        mine = [o for o in scene["objects"] if o.get("room") == m["id"]]
        for k, obj in enumerate(mine):
            ax, ay = on_furniture(ox, oy, st, k + 2)
            markers.append({
                "x": ax, "y": ay,
                "glyph": {"wiki-page": "📄", "lesson-board": "📚"}.get(obj["kind"], "🗎"),
                "kind": "object", "label": obj["label"], "data": obj})

    place_row("research", scene["rooms"], "Mission", on_mission)

    # --- market: one shop per real offer
    def on_offer(s, ox, oy, st, halls=1):
        labels.append({"x": ox + st["w"] // 2, "y": oy - 1,
                       "text": s["label"], "sub": s["status"]})
        sx, sy = on_furniture(ox, oy, st, 1)
        markers.append({"x": sx, "y": sy, "glyph": "🏷️",
                        "kind": "civic", "civic": "stall", "label": s["label"], "data": s})
    place_row("market", scene.get("stalls", []), "Offer", on_offer)

    # --- civic: evidence board, work board, receipt chain, monuments
    civic_items = []
    if scene.get("notices"):
        civic_items.append(("📌", "notices", "Evidence Board",
                            {"notices": scene["notices"],
                             "source": {"db": "capital.db", "table": "evidence_ledger"}}))
    if scene.get("workboard"):
        civic_items.append(("📊", "workboard", "Work Board",
                            {"workboard": scene["workboard"],
                             "source": {"db": "capital.db", "table": "capital_actions"}}))
    if scene.get("timeline"):
        civic_items.append(("⛓️", "obelisk", "Receipt Chain",
                            {"timeline": scene["timeline"],
                             "source": {"db": "fabric.db", "table": "receipts"}}))
    for m in scene.get("monuments", []):
        civic_items.append(("🗿", "monument", m["learning_type"], m))

    def on_civic(item, ox, oy, st, halls=1):
        glyph, civic, label, data = item
        cx, cy = on_furniture(ox, oy, st, 1)
        markers.append({"x": cx, "y": cy, "glyph": glyph,
                        "kind": "civic", "civic": civic, "label": label, "data": data})
    place_row("civic", civic_items, "EvidenceGraph", on_civic)

    # --- foundation yard: one vault per real fabric.db storage root
    def on_root(f, ox, oy, st, halls=1):
        fx, fy = on_furniture(ox, oy, st, 2)
        markers.append({"x": fx, "y": fy, "glyph": "🧱",
                        "kind": "civic", "civic": "foundation", "label": f["kind"], "data": f})
    place_row("foundation", scene.get("foundations", []), "StorageRoot", on_root)

    # --- gates sit on the road between districts, not in a building
    gates = scene.get("gates", [])
    if gates and "research" in district_span:
        gy = ROW_Y["gates"]
        road(MARGIN, GRID_W - MARGIN, gy, 1)
        span = (GRID_W - MARGIN * 2) // max(1, len(gates))
        for i, g in enumerate(gates):
            gx = MARGIN + i * span + span // 2
            markers.append({"x": gx, "y": gy, "kind": "civic", "civic": "gate",
                            "glyph": "🚧" if (g.get("status") or "").lower() == "open" else "⛔",
                            "label": g["label"], "data": g})
            if (g.get("status") or "").lower() != "open":
                for dy in (-1, 0, 1):
                    if 0 <= gy + dy < GRID_H:
                        blocked[gy + dy][gx] = 1

    # connecting roads between district rows
    for d in present:
        road(MARGIN, GRID_W - MARGIN, max(2, ROW_Y[d] - 2), 1)


    # District headers: the town needs to be legible at a glance, so each
    # band says what it is and which real tables produced it.
    DISTRICT_TITLE = {
        "apps": ("THE ESTATE", "nine Frappe apps · BonfyreFrappeCompiler"),
        "research": ("RESEARCH", "fabric.db missions · bonfyre_cms pages"),
        "market": ("MARKET", "capital.db offers · experiments"),
        "civic": ("CIVIC", "evidence · work · receipts · learning"),
        "foundation": ("FOUNDATIONS", "fabric.db storage roots"),
    }
    for d in present:
        t = DISTRICT_TITLE.get(d)
        if t:
            labels.append({"x": MARGIN + 16, "y": max(2, ROW_Y[d] - 4),
                           "text": t[0], "sub": t[1], "header": True})

    # residents stand in the room compiled from their own relationship
    actors = []
    for i, ch in enumerate(scene["characters"]):
        home = next((mk for mk in markers
                     if mk["kind"] == "room" and mk["data"]["id"] == ch.get("home_room")), None)
        base = (home["x"] + 4, home["y"] + 2) if home else (MARGIN + 10 + i * 6, 36)
        actors.append({"x": base[0], "y": base[1], "label": ch["label"], "data": ch})

    return {
        "w": GRID_W, "h": GRID_H, "tw": 16, "th": 16,
        "note": ("Compiled from real Bonfyre graph state by semantic gravity. "
                 "Tiles and room templates are real assets from the Smallville "
                 "Tiled map; the LAYOUT is compiled, not copied."),
        "gravity": scene.get("semantic_gravity", []),
        "districts": {d: district_span[d] for d in district_span},
        "layers": {k: v for k, v in layers.items()},
        "blocked": blocked,
        "markers": markers,
        "labels": labels,
        "actors": actors,
    }


def rle(flat):
    out, i = [], 0
    while i < len(flat):
        v, n = flat[i], 1
        while i + n < len(flat) and flat[i + n] == v:
            n += 1
        out.append(v)
        out.append(n)
        i += n
    return out


if __name__ == "__main__":
    scene = load(sys.argv[1] if len(sys.argv) > 1 else "/tmp/bonfyre-habitat-scene.json")
    out = compile_map(scene)
    packed = dict(out)
    packed["layers"] = {
        k: rle([c for row in v for c in row]) for k, v in out["layers"].items()
    }
    packed["blocked"] = rle([c for row in out["blocked"] for c in row])
    dest = sys.argv[2] if len(sys.argv) > 2 else "/tmp/habitat-map.json"
    with open(dest, "w") as f:
        json.dump(packed, f, separators=(",", ":"))
    print(f"map written: {dest}  {out['w']}x{out['h']}")
    print(f"districts: {out['districts']}")
    print(f"markers={len(out['markers'])} labels={len(out['labels'])} actors={len(out['actors'])}")
    print(f"size {os.path.getsize(dest)//1024} KB")
