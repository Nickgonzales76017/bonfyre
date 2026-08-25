"""Every Bonfyre command as a CommandAdvertisement -- the whole 91-cmd surface
addressable in one inventory (mandate SS4/SS25).

Each cmd/Bonfyre* is a capability organ. This enumerates the surface, classifies
each identity honestly (built / source-only), maps it to the Bonfyre capability it
provides (BonfyreAuth -> Authority, BonfyreLedger -> Value, BonfyreFPQ ->
FPQRepresentation, ...), and publishes the inventory into the fabric so BonfyreFS
serves the discoverable capability surface. Static only -- it never executes a
command binary (no side effects, no --help hangs); classification is by files on
disk. Silent identities are named, never wrapped to fake a 91/91 count.
"""

from __future__ import annotations

import json
import os
import stat

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
CMD = os.path.join(REPO, "cmd")

# the capability / Bonfyre fact each well-known command provides (curated; the
# rest advertise capability = their own name). This is where the command surface
# meets the wired fact graph.
CAPABILITY = {
    "BonfyreAuth": "Authority", "BonfyreCapability": "Capability",
    "BonfyreLedger": "Value", "BonfyreFinance": "Value", "BonfyreEconomy": "Value",
    "BonfyreFPQ": "FPQRepresentation", "BonfyreFPQx": "FPQRepresentation",
    "BonfyreKVCache": "KVPassport", "BonfyreGraph": "Relation", "BonfyreEntity": "Actor",
    "BonfyreHash": "Artifact", "BonfyreFS": "NamespaceProjection",
    "BonfyreEmbed": "SID", "BonfyreIndex": "AddressAdvertisement",
    "BonfyreProof": "ProofFrontier", "BonfyreGate": "Authority",
    "BonfyreInfer": "ExecutionReceipt", "BonfyreGen": "ExecutionReceipt",
    "BonfyreCMS": "Publication", "BonfyreLearn": "Capability",
    "BonfyreQueue": "WorkState", "BonfyreProject": "WorkState",
    "BonfyreEmit": "Occurrence", "BonfyreIngest": "ExternalEvent",
    "BonfyreDiscipl": "Capability", "BonfyreCompress": "Representation",
}
# effect class hint by name fragment (pure read vs mutating)
_MUTATING = ("Auth", "Gate", "Ledger", "Finance", "Emit", "Ingest", "Queue",
             "Project", "CMS", "Distribute", "Sync", "Pipeline")


def _binary_in(d: str, name: str) -> str | None:
    short = name[len("Bonfyre"):].lower() if name.startswith("Bonfyre") else name.lower()
    try:
        for f in os.listdir(d):
            p = os.path.join(d, f)
            if not os.path.isfile(p) or f.endswith((".o", ".sh", ".py", ".md")):
                continue
            if os.stat(p).st_mode & stat.S_IXUSR and (
                f == name or f.lower() == short or f.startswith("bonfyre-")):
                return f
    except OSError:
        return None
    return None


def advertise_one(name: str) -> dict:
    d = os.path.join(CMD, name)
    binary = _binary_in(d, name)
    has_src = os.path.isdir(os.path.join(d, "src"))
    if binary:
        cls = "built"
    elif has_src:
        cls = "source-only"
    else:
        cls = "declared"
    effect = "mutating" if any(m in name for m in _MUTATING) else "pure-read"
    return {
        "identity": name,
        "binary": binary or "",
        "classification": cls,
        "provides": CAPABILITY.get(name, name),
        "effect_class": effect,
        # BonfyreAuth/Gate assert Authority, so effects through them are authority-gated
        "authority_gated": name in ("BonfyreAuth", "BonfyreGate") or effect == "mutating",
    }


def inventory() -> dict:
    names = sorted(n for n in os.listdir(CMD)
                   if os.path.isdir(os.path.join(CMD, n)) and n.startswith("Bonfyre"))
    ads = [advertise_one(n) for n in names]
    by_cls: dict[str, int] = {}
    for a in ads:
        by_cls[a["classification"]] = by_cls.get(a["classification"], 0) + 1
    return {"command_count": len(ads), "by_classification": by_cls,
            "capabilities_covered": sorted({a["provides"] for a in ads}),
            "advertisements": ads}


def publish() -> dict:
    import fabric_publish as fp
    import hashlib
    import sqlite3
    from pathlib import Path
    inv = inventory()
    out = Path(fp.PROJECTIONS) / "capability-advertisements"
    out.mkdir(parents=True, exist_ok=True)
    path = out / "command-surface.json"
    path.write_text(json.dumps(inv, indent=2, sort_keys=True))
    published = False
    try:
        db = sqlite3.connect(str(fp.FABRIC), timeout=8)
        db.execute("PRAGMA busy_timeout=8000")
        fp.ensure_schema(db)
        data = path.read_bytes()
        dg = hashlib.sha256(data).hexdigest()
        uri = f"bonfyre://artifact/{dg}"
        now = fp._iso()
        db.execute("INSERT INTO namespace_objects(uri,kind,owner,source_authority,native_id,"
                   "version,locator,policy,sensitivity,freshness,evidence_state,operations,"
                   "content_contract,query_contract,effect_contract,created_at)"
                   " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                   " ON CONFLICT(uri) DO UPDATE SET freshness='current'",
                   (uri, "capability-advertisement", "local-user", fp.SOURCE_AUTHORITY,
                    "command-surface", "1", uri, "default", "standard", "current",
                    "measured", "read", "capability-advertisement.v1", "typed-lookup.v1",
                    "governed", now))
        db.execute("INSERT INTO artifacts(digest,uri,media_type,source_uri,locator,bytes,"
                   "representation,created_at) VALUES(?,?,?,?,?,?,?,?)"
                   " ON CONFLICT(digest) DO UPDATE SET locator=excluded.locator",
                   (dg, uri, "application/json", str(path), str(path), len(data),
                    "zero-copy-reference", now))
        db.commit()
        db.close()
        published = True
    except sqlite3.OperationalError:
        published = False
    return {"inventory": inv, "published": published, "path": str(path)}


if __name__ == "__main__":
    r = publish()
    inv = r["inventory"]
    print(f"command surface: {inv['command_count']} commands")
    print(f"  by classification: {inv['by_classification']}")
    print(f"  capabilities covered: {len(inv['capabilities_covered'])}")
    print(f"  published to fabric: {r['published']}")
    # spot-check the authority command
    auth = next(a for a in inv["advertisements"] if a["identity"] == "BonfyreAuth")
    print(f"  BonfyreAuth -> provides {auth['provides']}, {auth['classification']}, "
          f"authority_gated={auth['authority_gated']}")
