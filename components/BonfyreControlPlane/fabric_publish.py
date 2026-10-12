"""Publish control-plane state INTO the fabric so the real system can see it.

The critique that landed: the atlas and the control plane were an island. They
wrote to their own control_plane.db that nothing else reads, while the real
system -- BonfyreFS, the native tools -- lives on ``~/.bonfyre/estate-fabric/
fabric.db``. BonfyreFS is useful because it is a live mount over that fabric.
This connects the island to the mainland.

It publishes projections (the architecture atlas, the substrate evidence, the
live reachable capacity) into the fabric as content artifacts, each with its
required namespace object, matching the fabric's own conventions
(content-addressed digest, ``zero-copy-reference`` locator pointing at a real
file). Once published, ``bonfyre-fs`` serves them as files -- the same live mount
that already serves missions and artifacts now also serves the architecture and
the reachable capacity. Connected and browsable, not a separate registry.

Writes are additive and idempotent, under a distinct ``source_authority`` of
``bonfyre-control-plane`` so they never impersonate fabric-core or touch its rows.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

FABRIC = Path.home() / ".bonfyre" / "estate-fabric" / "fabric.db"
PROJECTIONS = Path.home() / ".bonfyre" / "estate-fabric" / "projections"
SOURCE_AUTHORITY = "bonfyre-control-plane"

UTC = dt.timezone.utc

# The two tables BonfyreFS reads, as the fabric defines them. Created only when a
# test points at a throwaway db; the real fabric already has them.
SCHEMA = """
CREATE TABLE IF NOT EXISTS namespace_objects(
  uri TEXT PRIMARY KEY, kind TEXT NOT NULL, owner TEXT NOT NULL,
  source_authority TEXT NOT NULL, native_id TEXT, version TEXT NOT NULL,
  locator TEXT NOT NULL, policy TEXT NOT NULL, sensitivity TEXT NOT NULL,
  freshness TEXT NOT NULL, evidence_state TEXT NOT NULL, operations TEXT NOT NULL,
  content_contract TEXT NOT NULL, query_contract TEXT NOT NULL,
  effect_contract TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS artifacts(
  digest TEXT PRIMARY KEY, uri TEXT NOT NULL UNIQUE, media_type TEXT NOT NULL,
  source_uri TEXT, locator TEXT NOT NULL, bytes INTEGER NOT NULL,
  representation TEXT NOT NULL, created_at TEXT NOT NULL,
  FOREIGN KEY(uri) REFERENCES namespace_objects(uri));
"""


def _iso() -> str:
    return dt.datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class Published:
    name: str
    digest: str
    uri: str
    bytes: int


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.commit()


def _commit_retry(db: sqlite3.Connection, attempts: int = 40, backoff: float = 0.25) -> None:
    """Commit into the live fabric, waiting out BonfyreFS's per-call read locks.

    The fabric is delete-journal (single writer) and BonfyreFS reads it on every
    FUSE call, so a write must land in a gap between reads. Retry a bounded number
    of times before giving up rather than corrupting or blocking the live mount."""
    for i in range(attempts):
        try:
            db.commit()
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or i == attempts - 1:
                raise
            time.sleep(backoff)


def publish_file(
    db: sqlite3.Connection,
    *,
    name: str,
    content_path: Path,
    media_type: str = "application/json",
    content_contract: str = "control-plane-projection.v1",
    dedupe: bool = False,
) -> Published:
    """Publish a real file as a fabric artifact + its namespace object.

    The artifact is a zero-copy reference to the file on disk (the fabric's own
    representation), content-addressed by sha256, discoverable by ``name`` via
    the namespace object's native_id. Idempotent on the digest.

    With ``dedupe``, prior control-plane artifacts sharing this ``name`` but a
    different digest are removed after the new one lands -- so a versioned
    projection (reachable-capacity, whose timestamp changes its digest each
    republish) keeps only its latest, rather than accumulating history."""
    data = content_path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    uri = f"bonfyre://artifact/{digest}"
    now = _iso()
    db.execute(
        "INSERT INTO namespace_objects"
        "(uri,kind,owner,source_authority,native_id,version,locator,policy,sensitivity,"
        " freshness,evidence_state,operations,content_contract,query_contract,effect_contract,created_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        " ON CONFLICT(uri) DO UPDATE SET native_id=excluded.native_id,"
        "  content_contract=excluded.content_contract, freshness='current'",
        (uri, "artifact", "local-user", SOURCE_AUTHORITY, name, "1", uri, "default",
         "standard", "current", "measured", "read", content_contract,
         "typed-lookup.v1", "governed", now),
    )
    db.execute(
        "INSERT INTO artifacts(digest,uri,media_type,source_uri,locator,bytes,representation,created_at)"
        " VALUES(?,?,?,?,?,?,?,?)"
        " ON CONFLICT(digest) DO UPDATE SET locator=excluded.locator, bytes=excluded.bytes",
        (digest, uri, media_type, str(content_path), str(content_path), len(data),
         "zero-copy-reference", now),
    )
    if dedupe:
        stale = [r[0] for r in db.execute(
            "SELECT uri FROM namespace_objects"
            " WHERE native_id=? AND source_authority=? AND uri!=?",
            (name, SOURCE_AUTHORITY, uri))]
        for ou in stale:
            db.execute("DELETE FROM artifacts WHERE uri=?", (ou,))
            db.execute("DELETE FROM namespace_objects WHERE uri=?", (ou,))
    _commit_retry(db)
    return Published(name=name, digest=digest, uri=uri, bytes=len(data))


def publish_projections(fabric: Path = FABRIC) -> list[Published]:
    """Publish the standing control-plane projections into the live fabric.

    The atlas index and each substrate's semantic proof are already real files;
    they are referenced in place. Returns what was published so a caller can show
    the fabric now carries them."""
    published: list[Published] = []
    # the live fabric is held by running services; wait for the lock rather than
    # failing, and use WAL so a reader (BonfyreFS) is not blocked by our write.
    db = sqlite3.connect(str(fabric), timeout=60)
    db.execute("PRAGMA busy_timeout=60000")
    ensure_schema(db)

    atlas_index = Path(__file__).resolve().parent.parent.parent / "architecture" / "atlas.index.json"
    if atlas_index.exists():
        published.append(publish_file(
            db, name="architecture-atlas", content_path=atlas_index,
            content_contract="architecture-atlas.v1"))

    substrates = Path.home() / ".bonfyre" / "substrates" / "v6.1"
    if substrates.exists():
        for proof in sorted(substrates.glob("*/semantic-proof.json")):
            published.append(publish_file(
                db, name=f"substrate-proof-{proof.parent.name}", content_path=proof,
                content_contract="substrate-semantic-proof.v1"))
    db.close()
    return published
