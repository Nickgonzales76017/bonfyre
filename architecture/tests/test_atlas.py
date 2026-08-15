"""The atlas guards itself: the constitution is code, so these tests are the
constitution's tests. If the real registry stops validating, CI fails here.
"""

import sys
from pathlib import Path

ATLAS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ATLAS_DIR))

import atlas  # noqa: E402


def _load():
    return atlas.Atlas.load(ATLAS_DIR)


def test_real_registry_is_valid():
    # the whole registry obeys the constitution: no laundering, every view child
    # resolves, every interaction endpoint resolves.
    errors = _load().validate()
    assert errors == [], "\n".join(errors)


def test_views_are_reversible():
    a = _load()
    for vid, view in a.views.items():
        children = view.lst("expands_to")
        assert len(children) >= 2
        for child in children:
            # a collapsed child always knows its parent view -- collapse is zoom.
            assert vid in a.views_of(child)


def test_every_architecture_states_what_it_cannot_infer_or_is_flagged():
    # forbidden_inference is the highest-value field; loss() names any architecture
    # missing it, so the gap is never silent.
    a = _load()
    missing = a.loss()["no_cannot_infer"]
    for aid in a.architectures:
        has = bool(a.architectures[aid].lst("forbidden_inference"))
        assert has == (aid not in missing)


def test_fpq_lesson_is_encoded():
    a = _load()
    fpq = a.architectures["fpq-transform"]
    forbidden = " ".join(fpq.lst("forbidden_inference")).lower()
    # the exact FPQ lesson: reconstruction error is not generation quality.
    assert "reconstruction" in forbidden and "generation" in forbidden


def test_maturity_laundering_is_rejected(tmp_path):
    bad = tmp_path / "bad.yaff"  # outside the atlas tree so load() never sees it
    bad.write_text(
        "atlas neg\n"
        "architecture bogus\n"
        "  canonical_name BogusGraph\n"
        "  family test\n"
        "  domain Test\n"
        "  semantic_question does laundering get caught?\n"
        "  maturity proven\n"
    )
    a = atlas.Atlas()
    for block in atlas.parse_file(bad):
        a.architectures[block.ident] = block
    assert any("laundering" in e for e in a.validate())


def test_measured_architectures_carry_witnesses():
    a = _load()
    for aid, arch in a.architectures.items():
        rank = atlas._MATURITY_RANK.get(arch.get("maturity"), 0)
        if rank >= atlas.WITNESS_REQUIRED_AT:
            assert arch.lst("witness"), f"{aid} claims {arch.get('maturity')} with no witness"


def test_export_is_stable_and_complete():
    a = _load()
    export = a.export()
    assert set(export["architectures"]) == set(a.architectures)
    # a measured architecture exports its witnesses
    assert export["architectures"]["fpq-transform"]["witnesses"]
