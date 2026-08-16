"""Publishing into the fabric produces exactly the rows BonfyreFS reads: a
content-addressed artifact and its namespace object, additive and idempotent."""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import fabric_publish as fp


def test_publish_creates_artifact_and_namespace_object(tmp_path):
    db = sqlite3.connect(":memory:")
    fp.ensure_schema(db)
    content = tmp_path / "atlas.index.json"
    content.write_text('{"architectures": {}}')

    pub = fp.publish_file(db, name="architecture-atlas", content_path=content)

    # the namespace object BonfyreFS resolves the artifact through
    row = db.execute(
        "SELECT kind,source_authority,native_id FROM namespace_objects WHERE uri=?",
        (pub.uri,)).fetchone()
    assert row == ("artifact", fp.SOURCE_AUTHORITY, "architecture-atlas")
    # the content-addressed artifact, zero-copy referencing the real file
    art = db.execute(
        "SELECT digest,representation,locator FROM artifacts WHERE uri=?", (pub.uri,)).fetchone()
    assert art[0] == pub.digest and art[1] == "zero-copy-reference"
    assert art[2] == str(content)


def test_publish_is_idempotent(tmp_path):
    db = sqlite3.connect(":memory:")
    fp.ensure_schema(db)
    content = tmp_path / "c.json"
    content.write_text("{}")
    a = fp.publish_file(db, name="x", content_path=content)
    b = fp.publish_file(db, name="x", content_path=content)
    assert a.digest == b.digest
    assert db.execute("SELECT count(*) FROM artifacts").fetchone()[0] == 1


def test_only_our_source_authority_is_written(tmp_path):
    # additive: a pre-existing fabric-core row is never touched.
    db = sqlite3.connect(":memory:")
    fp.ensure_schema(db)
    db.execute(
        "INSERT INTO namespace_objects VALUES"
        "('bonfyre://mission/x','mission','local-user','fabric-core','x','1','m','default',"
        "'standard','current','declared','read','mission.v1','typed-lookup.v1','governed','t')")
    db.commit()
    content = tmp_path / "c.json"
    content.write_text("{}")
    fp.publish_file(db, name="x", content_path=content)
    # the fabric-core mission is untouched; only our artifact was added
    authorities = {r[0] for r in db.execute("SELECT DISTINCT source_authority FROM namespace_objects")}
    assert authorities == {"fabric-core", fp.SOURCE_AUTHORITY}
