"""Fences for the public housing weak-signal -> AvailabilityCandidate projection."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "tools"))

import housing_signal_candidates as hsc  # noqa: E402


SEARCH_FIXTURE = """<html><head>
<script type="application/ld+json">
{"@context":"https://schema.org","@type":"ItemList","itemListElement":[
 {"position":"0","@type":"ListItem","item":{"@type":"Room","latitude":32.8,"longitude":-96.8,
  "numberOfBedrooms":1,"numberOfBathroomsTotal":1,
  "address":{"@type":"PostalAddress","addressLocality":"Dallas","addressRegion":"TX"}}},
 {"position":"1","@type":"ListItem","item":{"@type":"Room","latitude":32.7,"longitude":-97.1,
  "numberOfBedrooms":2,"numberOfBathroomsTotal":2,
  "address":{"@type":"PostalAddress","addressLocality":"Arlington","addressRegion":"TX"}}}
]}
</script></head><body>
<ol class="cl-static-search-results">
<li class="cl-static-hub-links"><div>see also</div></li>
<li class="cl-static-search-result" title="Lease takeover, move in today">
  <a href="https://example.invalid/view/a">
    <div class="title">Lease takeover, move in today</div>
    <div class="details"><div class="price">$1,712</div><div class="location">Dallas</div></div>
  </a>
</li>
<li class="cl-static-search-result" title="Looking for a roommate">
  <a href="https://example.invalid/view/b">
    <div class="title">Looking for a roommate</div>
    <div class="details"><div class="price">$650</div><div class="location">Arlington</div></div>
  </a>
</li>
<li class="cl-static-search-result" title="For sale by owner: 3br house">
  <a href="https://example.invalid/view/c">
    <div class="title">For sale by owner: 3br house</div>
    <div class="details"><div class="price">$310,000</div><div class="location">Dallas</div></div>
  </a>
</li>
<li class="cl-static-search-result" title="Looking for a room near campus">
  <a href="https://example.invalid/view/d">
    <div class="title">Looking for a room near campus</div>
    <div class="details"><div class="price">$700</div><div class="location">Dallas</div></div>
  </a>
</li>
</ol></body></html>
"""

DETAIL_FIXTURE = """<html><body>
<span id="titletextonly">Lease takeover, move in today</span>
<span class="price">$1,712</span>
<time datetime="2026-07-27T14:46:49-0500">posted</time>
<time datetime="2026-08-16T14:51:12-0500">updated</time>
<div class="viewposting" data-latitude="32.804700" data-longitude="-96.814200"></div>
<div class="mapaddress">Reagan near Maple</div>
<div class="attrgroup">
  <span class="attr important">1BR / 1Ba</span>
  <span class="attr important available-now">available now</span>
</div>
<div class="attrgroup">
  <div class="attr application_fee_explained"><span class="labl">application fee details:</span>
    <span class="valu">$75 Application Fee</span></div>
  <div class="attr pets_dog"><span class="valu">
    <a href="https://example.invalid/s?pets_dog=1">dogs are ok - wooof</a></span></div>
  <div class="attr"><span class="valu">
    <a href="https://example.invalid/s?housing_type=1">apartment</a></span></div>
</div>
</body></html>
"""

SURFACES = {
    "cl-fixture-search": {
        "role": "search",
        "market": "DFW",
        "category": "apartments_query_lease_takeover",
        "rental_intent": True,
        "parser": "craigslist_static",
    }
}


def write_observation(state: Path, twin_id: str, url: str, payload: str) -> dict:
    bodies = state / "bodies"
    bodies.mkdir(parents=True, exist_ok=True)
    body_path = bodies / f"{twin_id}.html"
    body_path.write_text(payload, encoding="utf-8")
    receipt = {
        "schema": "bonfyre-foreign-observation.v1",
        "twin_id": twin_id,
        "source_url": url,
        "observed_at": "2026-08-24T01:00:00Z",
        "transport_exit": 0,
        "http_status": 200,
        "body_bytes": len(payload.encode("utf-8")),
        "content_sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        "body_path": str(body_path),
        "availability_claim": False,
        "observation_kind": "source_observed",
    }
    with (state / "source-receipts.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(receipt) + "\n")
    return receipt


@pytest.fixture()
def state(tmp_path: Path) -> Path:
    target = tmp_path / "housing"
    target.mkdir()
    write_observation(target, "cl-fixture-search", "https://example.invalid/search", SEARCH_FIXTURE)
    (target / "surfaces.json").write_text(json.dumps(SURFACES), encoding="utf-8")
    return target


def run(state: Path) -> tuple[list[dict], list[dict], list[dict]]:
    hsc.main([
        "--state", str(state),
        "--surfaces", str(state / "surfaces.json"),
        "--repo-root", str(state),
    ])

    def read(name: str) -> list[dict]:
        path = state / name
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]

    return read("availability-candidates.jsonl"), read("candidate-rejections.jsonl"), read("current-listings.jsonl")


def test_ownership_transfer_post_is_never_a_target(state: Path) -> None:
    candidates, rejections, _ = run(state)
    assert "https://example.invalid/view/c" not in {row["posting_url"] for row in candidates}
    reason = next(r for r in rejections if r["posting_url"] == "https://example.invalid/view/c")
    assert reason["rejected_because"].startswith("ownership_transfer_surface:")


def test_demand_side_post_is_rejected_but_roommate_supply_is_kept(state: Path) -> None:
    """'looking for a room' is demand; 'looking for a roommate' is supply."""
    candidates, rejections, _ = run(state)
    urls = {row["posting_url"] for row in candidates}
    assert "https://example.invalid/view/d" not in urls
    assert "https://example.invalid/view/b" in urls
    reason = next(r for r in rejections if r["posting_url"] == "https://example.invalid/view/d")
    assert reason["rejected_because"].startswith("demand_side_post:")


def test_candidate_is_not_an_availability_fact(state: Path) -> None:
    candidates, _, listings = run(state)
    assert listings == []
    for row in candidates:
        assert row["availability_claim"] is False
        assert row["exactness"] == "candidate"
        assert row["corroboration"] is None


def test_search_row_carries_price_geo_and_signals(state: Path) -> None:
    candidates, _, _ = run(state)
    row = next(c for c in candidates if c["posting_url"] == "https://example.invalid/view/a")
    assert row["asking_price_usd_month"] == 1712
    assert row["latitude"] == 32.8 and row["longitude"] == -96.8
    assert set(row["signal_kinds"]) == {"lease_takeover", "immediate_move_in"}
    assert row["source_content_sha256"] == hashlib.sha256(
        SEARCH_FIXTURE.encode("utf-8")).hexdigest()


def test_corroboration_requires_an_independent_detail_observation(state: Path) -> None:
    candidates, _, _ = run(state)
    row = next(c for c in candidates if c["posting_url"] == "https://example.invalid/view/a")
    write_observation(state, f"cl-detail-{row['candidate_id']}",
                      "https://example.invalid/view/a", DETAIL_FIXTURE)
    candidates, _, listings = run(state)

    row = next(c for c in candidates if c["posting_url"] == "https://example.invalid/view/a")
    assert row["exactness"] == "corroborated_observation"
    assert row["corroboration"]["source_agreement"] == {"price_agrees": True, "title_agrees": True}

    listing = next(l for l in listings if l["posting_url"] == "https://example.invalid/view/a")
    assert listing["price_usd_month"] == 1712
    assert listing["posted_at"] == "2026-07-27T14:46:49-0500"
    assert listing["updated_at"] == "2026-08-16T14:51:12-0500"
    assert listing["housing_type"] == "apartment"
    assert listing["available_text"] == "available now"
    assert listing["application_fee_text"] == "$75 Application Fee"
    assert listing["map_cross_street"] == "Reagan near Maple"
    assert listing["dogs_stated_ok"] is True
    assert listing["cats_stated_ok"] is False
    # An exact posting reading is still not verified availability.
    assert listing["availability_claim"] is False
    assert listing["availability_verified"] is False


def test_a_body_that_no_longer_hashes_to_its_receipt_is_not_parsed(state: Path) -> None:
    (state / "bodies" / "cl-fixture-search.html").write_text("tampered", encoding="utf-8")
    candidates, rejections, listings = run(state)
    assert candidates == [] and rejections == [] and listings == []


def test_boundary_response_yields_no_candidates(tmp_path: Path) -> None:
    target = tmp_path / "housing"
    target.mkdir()
    (target / "source-receipts.jsonl").write_text(json.dumps({
        "schema": "bonfyre-foreign-observation.v1",
        "twin_id": "cl-fixture-search",
        "source_url": "https://example.invalid/search",
        "observed_at": "2026-08-24T01:00:00Z",
        "transport_exit": 0,
        "http_status": 403,
        "body_bytes": 12,
        "content_sha256": None,
        "availability_claim": False,
        "observation_kind": "boundary_response",
    }) + "\n", encoding="utf-8")
    (target / "surfaces.json").write_text(json.dumps(SURFACES), encoding="utf-8")
    candidates, _, listings = run(target)
    assert candidates == [] and listings == []


def test_policy_exclusion_yields_no_candidates_without_becoming_a_denial(tmp_path: Path) -> None:
    target = tmp_path / "housing"
    target.mkdir()
    (target / "source-receipts.jsonl").write_text(json.dumps({
        "schema": "bonfyre-foreign-observation.v1",
        "twin_id": "cl-fixture-search",
        "source_url": "https://example.invalid/search",
        "observed_at": "2026-08-24T01:00:00Z",
        "observe_right": "public",
        "policy_decision": "robots_disallow",
        "fetch_attempted": False,
        "transport_exit": 0,
        "http_status": 0,
        "body_bytes": 0,
        "content_sha256": None,
        "availability_claim": False,
        "observation_kind": "policy_excluded",
    }) + "\n", encoding="utf-8")
    (target / "surfaces.json").write_text(json.dumps(SURFACES), encoding="utf-8")
    candidates, _, listings = run(target)
    assert candidates == [] and listings == []


def test_foreign_rows_in_a_shared_durable_output_are_preserved(state: Path) -> None:
    """$STATE/household/housing is shared: never truncate another lane's rows."""
    candidates, _, _ = run(state)
    row = next(c for c in candidates if c["posting_url"] == "https://example.invalid/view/a")
    write_observation(state, f"cl-detail-{row['candidate_id']}",
                      "https://example.invalid/view/a", DETAIL_FIXTURE)

    foreign = {
        "schema": "some-other-lane.v1",
        "posting_url": "https://example.invalid/other-lane",
        "property": "written by a different housing lane",
    }
    (state / "current-listings.jsonl").write_text(json.dumps(foreign) + "\n", encoding="utf-8")

    _, _, listings = run(state)
    assert foreign in listings
    assert any(l.get("schema") == hsc.SCHEMA_LISTING for l in listings)
