#!/usr/bin/env python3
"""
bonfyre_habitat_assets.py -- asset pack registry for Habitat.

Habitat's art vocabulary should not be one borrowed tileset. This walks a
pack directory, measures every candidate spritesheet, works out its grid,
and emits a registry the HabitatCompiler can draw from.

Provenance is tracked per pack because the licences genuinely differ:

  Kenney            CC0. Downloaded from kenney.nl directly. No attribution
                    required, redistribution fine.
  LimeZu            PAID licence. Supplied by the owner via their own
                    nmatter1/smallville checkout. Not redistributable --
                    referenced from the owner's copy, never vendored.
  Pixel Frog        CC0 per the itch.io project pages, but itch gates the
  0x72              download behind a browser session. Drop the official
                    zip into assets/packs/dropin/ and re-run; this script
                    ingests it. Third-party GitHub "mirrors" are
                    deliberately NOT used -- they are unlicensed re-uploads
                    with sliced-up files and bad provenance.

Grid inference: a sheet is usable if both dimensions divide by a candidate
tile size. Kenney ships `*_packed.png` variants with no inter-tile spacing,
which are preferred because the unpacked ones carry 1px gutters that make
naive tile maths wrong.
"""
import json
import os
import struct
import sys

TILE_CANDIDATES = (16, 8, 32, 24)

PROVENANCE = {
    "kenney": {
        "author": "Kenney", "licence": "CC0-1.0", "source": "https://kenney.nl",
        "attribution_required": False, "redistributable": True,
    },
    "limezu": {
        "author": "LimeZu", "licence": "commercial (owner-supplied)",
        "source": "owner's nmatter1/smallville checkout",
        "attribution_required": True, "redistributable": False,
    },
    "pixelfrog": {
        "author": "Pixel Frog", "licence": "CC0-1.0",
        "source": "https://pixelfrog-assets.itch.io",
        "attribution_required": False, "redistributable": True,
    },
    "0x72": {
        "author": "0x72", "licence": "CC0-1.0",
        "source": "https://0x72.itch.io/dungeontileset-ii",
        "attribution_required": False, "redistributable": True,
    },
}


def png_size(path):
    """Read width/height straight from the IHDR chunk -- no dependencies."""
    with open(path, "rb") as f:
        head = f.read(24)
    if len(head) < 24 or head[:8] != b"\x89PNG\r\n\x1a\n":
        return None
    return struct.unpack(">II", head[16:24])


def classify(path):
    p = path.lower()
    for key in PROVENANCE:
        if key in p:
            return key
    return "unknown"


def _axis(size, stride):
    """Tile count along one axis, tolerating a present or trimmed gutter."""
    if size % stride == 0:
        return size // stride
    if (size + stride - (stride - 1)) % stride == 0 and (size + 1) % stride == 0:
        return (size + 1) // stride
    return None


def grid_for(w, h):
    """Best (tile, spacing) that explains both dimensions.

    Kenney ships each roguelike sheet twice: a `_packed` variant with no
    gutters, and an unpacked one laid out as cols*(tile+1)-1, i.e. 1px
    spacing between tiles. Only checking exact division silently dropped
    every unpacked sheet -- roguelike-modern-city, caves-dungeons,
    characters and indoors were all being thrown away.
    """
    for t in TILE_CANDIDATES:
        if w % t == 0 and h % t == 0 and (w // t) * (h // t) >= 12:
            return t, 0
        for sp in (1, 2):
            stride = t + sp
            # Kenney is not internally consistent: some sheets end on a
            # trailing gutter (w == cols*stride) and some trim it
            # (w == cols*stride - sp), and a single sheet can use one
            # convention horizontally and the other vertically --
            # roguelike-characters is 918x203, which is 54*17 wide but
            # 12*17-1 tall. Accept either on each axis independently.
            cols = _axis(w, stride)
            rows = _axis(h, stride)
            if cols and rows and cols * rows >= 12:
                return t, sp
    return None


def scan(root):
    packs = []
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            if not name.lower().endswith(".png"):
                continue
            full = os.path.join(dirpath, name)
            size = png_size(full)
            if not size:
                continue
            w, h = size
            if w < 32 or h < 32:
                continue          # single icons, not sheets
            found = grid_for(w, h)
            if not found:
                continue
            tile, spacing = found
            rel = os.path.relpath(full, root)
            packs.append({
                "path": rel,
                "pack": rel.split(os.sep)[0],
                "provenance": classify(rel),
                "w": w, "h": h,
                "tile": tile,
                "spacing": spacing,
                "cols": _axis(w, tile + spacing) if spacing else w // tile,
                "rows": _axis(h, tile + spacing) if spacing else h // tile,
                "tiles": ((_axis(w, tile + spacing) if spacing else w // tile)
                          * (_axis(h, tile + spacing) if spacing else h // tile)),
                # Kenney's packed variants have no gutters; prefer them
                "packed": "_packed" in name.lower(),
                "bytes": os.path.getsize(full),
            })
    return packs


if __name__ == "__main__":
    root = sys.argv[1] if len(sys.argv) > 1 else "assets/packs/extracted"
    out = sys.argv[2] if len(sys.argv) > 2 else "assets/packs/registry.json"
    sheets = scan(root)
    sheets.sort(key=lambda s: -s["tiles"])

    # only redistributable packs may be inlined into a published artifact
    for s in sheets:
        prov = PROVENANCE.get(s["provenance"], {})
        s["redistributable"] = bool(prov.get("redistributable"))
        s["licence"] = prov.get("licence", "unknown")

    registry = {
        "provenance": PROVENANCE,
        "sheet_count": len(sheets),
        "total_tiles": sum(s["tiles"] for s in sheets),
        "sheets": sheets,
    }
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w") as f:
        json.dump(registry, f, indent=2)

    print(f"registry: {out}")
    print(f"{len(sheets)} sheets, {registry['total_tiles']} tiles total")
    by_pack = {}
    for s in sheets:
        e = by_pack.setdefault(s["pack"], {"tiles": 0, "sheets": 0, "lic": s["licence"]})
        e["tiles"] += s["tiles"]
        e["sheets"] += 1
    for pack, e in sorted(by_pack.items(), key=lambda kv: -kv[1]["tiles"]):
        print(f"  {pack:<44} {e['sheets']:>3} sheets {e['tiles']:>6} tiles  [{e['lic']}]")
