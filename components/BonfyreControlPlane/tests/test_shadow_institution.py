"""ShadowInstitutionGraph: observation is epistemic; conversion to active is
decided by real relationship progress, not by a richer public picture."""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import relationship as rel
import shadow_institution as si


def _db():
    db = sqlite3.connect(":memory:")
    si.ensure_schema(db)
    rel.ensure_schema(db)
    return db


def test_observed_then_shadow_needs_attachments():
    db = _db()
    si.record(db, si.ShadowInstitution("acm", name="ACM", contact_actor="acm-editor",
                                       profile="acm.editor"))
    assert si.conversion_stage(db, "acm").stage == si.OBSERVED  # no public structure yet
    si.record(db, si.ShadowInstitution("acm", name="ACM", contact_actor="acm-editor",
                                       profile="acm.editor",
                                       observed_hierarchy=("editor-in-chief", "area editors"),
                                       public_attachments=("submission-form", "editorial-board")))
    assert si.conversion_stage(db, "acm").stage == si.SHADOW  # mapped, no relationship


def test_conversion_requires_real_relationship_progress():
    db = _db()
    si.record(db, si.ShadowInstitution("acm", contact_actor="acm-editor", profile="acm.editor",
                                       public_attachments=("submission-form",)))
    assert si.conversion_stage(db, "acm").stage == si.SHADOW

    rel.record(db, rel.Relationship("r", actor="acm-editor", profile="acm.editor", stage="contacted"))
    assert si.conversion_stage(db, "acm").stage == si.SHADOW  # contacted < engaged

    rel.advance(db, "r", "engaged", evidence="reply")
    assert si.conversion_stage(db, "acm").stage == si.RELATIONSHIP_ESTABLISHED

    rel.advance(db, "r", "collaborated", evidence="co-authored")
    assert si.conversion_stage(db, "acm").stage == si.ACTIVE
    assert si.is_active(db, "acm") is True


def test_observation_is_not_authority():
    # a fully-mapped hierarchy with no relationship never reaches active.
    db = _db()
    si.record(db, si.ShadowInstitution("uni", contact_actor="dean", profile="uni.dean",
                                       observed_hierarchy=("provost", "deans", "chairs"),
                                       public_attachments=("org-chart", "grant-portal")))
    assert si.conversion_stage(db, "uni").stage == si.SHADOW
    assert si.is_active(db, "uni") is False
