"""The ternary ranker learns ternary weights from outcomes and reorders eligible
candidates -- it never changes eligibility."""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import route_ranker as rr


def test_ternary_weights_learn_a_clear_signal():
    # feature 0 perfectly predicts success; feature 1 is balanced (pure noise).
    records = [
        rr.RouteRecord((1.0, 0.5), True),
        rr.RouteRecord((1.0, 0.5), True),
        rr.RouteRecord((0.0, 0.5), False),
        rr.RouteRecord((0.0, 0.5), False),
    ]
    model = rr.TernaryRanker.train(records)
    assert model.weights[0] == 1               # feature 0 -> +1
    assert model.weights[1] == 0               # weak signal -> 0, not laundered
    assert set(model.weights) <= {-1, 0, 1}    # genuinely ternary
    assert model.trained_on == 4


def test_negative_signal_gives_minus_one():
    records = [rr.RouteRecord((0.0,), True), rr.RouteRecord((1.0,), False)]
    assert rr.TernaryRanker.train(records).weights[0] == -1


def test_rank_orders_survivors_by_predicted_success():
    model = rr.TernaryRanker(weights=(1, 0, 1), trained_on=10)
    candidates = {
        "strong": (1.0, 0.0, 1.0),   # score 2
        "weak": (0.0, 1.0, 0.0),     # score 0
        "mid": (1.0, 0.0, 0.0),      # score 1
    }
    ranked = model.rank(candidates)
    assert [c for c, _ in ranked] == ["strong", "mid", "weak"]


def test_empty_records_is_a_zero_model():
    model = rr.TernaryRanker.train([])
    assert set(model.weights) == {0} and model.trained_on == 0


def test_rank_commands_reorders_funnel_survivors():
    # the funnel already rejected the impossible; the ranker only reorders these.
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE estate_catalog(family TEXT, estate TEXT)")
    db.execute("CREATE TABLE capability_identities(public_name TEXT, location TEXT)")
    db.executemany("INSERT INTO estate_catalog VALUES(?,?)",
                   [("BonfyreFPQ", "model"), ("BonfyreCMS", "surface_human")])
    db.executemany("INSERT INTO capability_identities VALUES(?,?)",
                   [("BonfyreFPQ", "/bin/fpq"), ("BonfyreCMS", "/bin/cms")])
    db.commit()
    # a model that favors model-estate commands
    model = rr.TernaryRanker(weights=(1, 0, 0), trained_on=10)
    ranked = rr.rank_commands(db, ["BonfyreCMS", "BonfyreFPQ"], model)
    assert ranked[0][0] == "BonfyreFPQ"   # model command ranked first


def test_records_from_real_work_items():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE work_items(id INTEGER PRIMARY KEY, priority REAL, "
               "target_plane TEXT, attempt INTEGER, state TEXT)")
    db.executemany("INSERT INTO work_items(priority,target_plane,attempt,state) VALUES(?,?,?,?)", [
        (8.0, "run4", 0, "satisfied"), (2.0, "", 3, "failed"), (5.0, "run5", 0, "effected"),
    ])
    db.commit()
    records = rr.records_from_work(db)
    assert len(records) == 3
    assert records[0].success is True and records[1].success is False
    # a model trained on these produces ternary weights
    model = rr.TernaryRanker.train(records)
    assert set(model.weights) <= {-1, 0, 1} and model.trained_on == 3
