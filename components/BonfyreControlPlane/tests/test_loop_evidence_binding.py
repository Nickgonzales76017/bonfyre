"""The form loop fills an evidence slot with the RIGHT fabric artifact by a
semantic test -- even when the wrong artifact is positionally first."""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import actors
import atomic_forms as af
import institution_loop as il


def _control(tmp_path):
    p = tmp_path / "control.db"
    con = sqlite3.connect(str(p))
    actors.ensure_schema(con)
    con.close()
    return p


def _fabric(tmp_path):
    p = tmp_path / "fabric.db"
    con = sqlite3.connect(str(p))
    con.execute("CREATE TABLE artifacts(digest TEXT, kind TEXT, name TEXT)")
    # the WRONG artifact is inserted first, so a positional fill would grab it.
    con.execute("INSERT INTO artifacts VALUES('wrong123','note','a random note')")
    con.execute("INSERT INTO artifacts VALUES('right456','measurement','fpq roundtrip report')")
    con.commit()
    con.close()
    return p


def test_matcher_slot_binds_the_right_artifact_not_the_first(tmp_path):
    form = af.AtomicForm(
        form_id="evidence-form", title="t",
        evidence=[af.EvidenceSlot(name="fpq_measurement", match_kind="measurement",
                                  match_name=("fpq",))],
    )
    result = il.run(form, control_db=_control(tmp_path), fabric_db=_fabric(tmp_path),
                    cms_db=tmp_path / "cms.db")
    slot = form.evidence[0]
    assert slot.artifact_digest == "right456"       # the matching artifact
    assert slot.artifact_digest != "wrong123"       # never the positional first
    assert result.form_ready is True                # slot filled -> submit-ready
