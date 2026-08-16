"""The forwarding plane: bitwise eligibility rejects the impossible cheaply, the
ternary proof vector blackholes a disproven hypothesis, and none of it grants
authority or proves anything -- it only compiles what already exists."""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import address_plane as ap
import authority as au
import proof_frontier as pf


def test_eligibility_is_a_bitwise_reject():
    # need bits 0 and 2; a candidate must have both.
    need = 0b101
    assert ap.eligible(0b111, need) is True
    assert ap.eligible(0b101, need) is True
    assert ap.eligible(0b100, need) is False   # missing bit 0
    survivors, rejected = ap.reject_count(need, {"a": 0b111, "b": 0b100, "c": 0b101})
    assert sorted(survivors) == ["a", "c"] and rejected == 1


def test_authority_mask_compiles_from_real_grants():
    db = sqlite3.connect(":memory:")
    au.ensure_schema(db)
    au.grant(db, au.AuthorityEdge("e1", actor="nick", permission=au.PUBLISH, subject="paper"))
    au.grant(db, au.AuthorityEdge("e2", actor="nick", permission=au.REVIEW, subject="paper"))
    # holds publish+review, needs publish -> admitted; needs commit -> rejected
    assert ap.authority_admits(db, "nick", "paper", [au.PUBLISH]) is True
    assert ap.authority_admits(db, "nick", "paper", [au.PUBLISH, au.REVIEW]) is True
    assert ap.authority_admits(db, "nick", "paper", [au.COMMIT]) is False
    # a revoked grant drops out of the mask
    au.revoke(db, "e1")
    assert ap.authority_admits(db, "nick", "paper", [au.PUBLISH]) is False


def test_proof_ternary_and_blackhole():
    db = sqlite3.connect(":memory:")
    pf.ensure_schema(db)
    pf.set_layer(db, "model:x", 2, "reconstruction", "proven", subject_profile="p")
    pf.set_layer(db, "model:x", 4, "transformer_math", "open", subject_profile="p")
    pf.record_noncause(db, pf.KnownNonCause(
        noncause_id="nc", hypothesis="quantization", subject_scope="model:x",
        experiment="fp16 passthrough still garbage"))

    tern = ap.proof_ternary(db, "model:x", "p")
    assert tern["reconstruction"] == ap.PROVEN        # +1
    assert tern["transformer_math"] == ap.UNRESOLVED  # 0
    assert tern["quantization"] == ap.DISPROVEN       # -1 blackhole

    # a disproven hypothesis is a blackhole route the search never re-enters
    assert ap.is_blackholed(db, "model:x", "quantization corruption") is True
    assert ap.is_blackholed(db, "model:x", "runtime path") is False


def test_capability_mask_from_estate():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE estate_catalog(family TEXT, estate TEXT)")
    db.executemany("INSERT INTO estate_catalog VALUES(?,?)",
                   [("BonfyreFPQ", "model"), ("BonfyreSLI", "model"), ("BonfyreHash", "artifact")])
    db.commit()
    reg = ap.capability_registry(db)
    assert reg.size == 3
    need = ap.capability_mask(reg, ["BonfyreFPQ", "BonfyreSLI"])
    have_all = ap.capability_mask(reg, ["BonfyreFPQ", "BonfyreSLI", "BonfyreHash"])
    have_partial = ap.capability_mask(reg, ["BonfyreFPQ"])
    assert ap.eligible(have_all, need) is True
    assert ap.eligible(have_partial, need) is False


def _estate_and_auth_and_proof(tmp_path):
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE estate_catalog(family TEXT, estate TEXT)")
    db.executemany("INSERT INTO estate_catalog VALUES(?,?)", [
        ("BonfyreFPQ", "model"), ("BonfyreSLI", "model"), ("BonfyreKVCache", "model"),
        ("BonfyreHash", "artifact"), ("BonfyreNet", "distributed_device"),
        ("BonfyreCMS", "surface_human"),
    ])
    db.commit()
    au.ensure_schema(db)
    pf.ensure_schema(db)
    return db


def test_route_funnels_by_capability_then_authority_then_proof(tmp_path):
    db = _estate_and_auth_and_proof(tmp_path)
    # demand: a model-estate operation on model:x, actor nick needs ACT, recon proven
    au.grant(db, au.AuthorityEdge("e", actor="nick", permission=au.ACT, subject="model:x"))
    pf.set_layer(db, "model:x", 2, "reconstruction", "proven", subject_profile="p")

    d = ap.RouteDemand(estates=("model",), actor="nick", subject="model:x",
                       subject_profile="p", required_permissions=(au.ACT,),
                       required_proven_layers=("reconstruction",))
    r = ap.route(db, d)
    assert r.considered == 6
    assert r.rejected_by_capability == 3          # 3 non-model commands rejected
    assert set(r.survivors) == {"BonfyreFPQ", "BonfyreSLI", "BonfyreKVCache"}
    assert r.rejected_by_authority == 0 and r.rejected_by_proof == 0


def test_route_rejects_on_missing_authority():
    db = _estate_and_auth_and_proof(None)
    d = ap.RouteDemand(estates=("model",), actor="nobody", subject="model:x",
                       required_permissions=(au.ACT,))
    r = ap.route(db, d)
    assert r.survivors == () and r.rejected_by_authority == 3   # 3 model cmds gated out


def test_route_rejects_on_blackholed_hypothesis():
    db = _estate_and_auth_and_proof(None)
    pf.record_noncause(db, pf.KnownNonCause(
        noncause_id="nc", hypothesis="quantization", subject_scope="model:x",
        experiment="fp16 passthrough"))
    d = ap.RouteDemand(estates=("model",), subject="model:x",
                       forbidden_hypotheses=("quantization corruption",))
    r = ap.route(db, d)
    assert r.survivors == () and r.rejected_by_proof == 3   # blackhole rejects the route
