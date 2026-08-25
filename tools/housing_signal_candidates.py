#!/usr/bin/env python3
"""Normalize public rental/sublet/room postings into AvailabilityCandidates.

The observation step is owned by ``bonfyre-project foreign observe`` (ForeignTwin);
this tool only reads retained bodies plus their content-addressed receipts and
projects weak public supply signals into candidate records.

Non-collapse rules enforced here:
  * a parsed posting is an observation, never an availability fact;
  * a corroborated posting is an exact reading of the posting, not proof the
    unit is still available -- that would need a contact effect, which the run
    forbids;
  * private ownership alone is never a target: a record is emitted only when the
    source surface and the posting text both carry public rental/sublet/room
    intent.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import sys
from pathlib import Path

SCHEMA_CANDIDATE = "bonfyre-availability-candidate.v1"
# `current-listings.jsonl` is a shared durable output.  This lane used the
# generic observation schema in its first revision, which made it impossible to
# distinguish its rows from another producer's rows during a read/merge/write.
# Keep the schema lane-owned going forward; the narrowly scoped legacy handling
# in `merge_jsonl` below removes only old rows whose posting URL this run owns.
SCHEMA_LISTING = "bonfyre-housing-social-observation.v1"
LEGACY_LISTING_SCHEMA = "bonfyre-housing-observation.v1"
SCHEMA_REJECTION = "bonfyre-availability-candidate-rejection.v1"

# Weak/quirky supply signals named by the work item.
SIGNAL_LEXICON: dict[str, tuple[str, ...]] = {
    "lease_takeover": (
        "lease takeover", "take over lease", "taking over lease", "lease transfer",
        "lease assignment", "assume lease", "lease break",
    ),
    "sublet": ("sublet", "sublease", "sub-lease", "sub lease", "subleasing", "subletting"),
    "roommate_departure": (
        "roommate moving out", "roommate is moving", "roommate leaving", "replace roommate",
        "roommate needed", "looking for a roommate", "seeking roommate", "female roommate",
        "male roommate",
    ),
    "room_available": (
        "room available", "room for rent", "private room", "rooms for rent",
        "bedroom available", "room 4 rent", "room available now",
    ),
    "moving_out": ("moving out", "must move", "relocating", "moving soon", "vacating"),
    "new_phase": (
        "new phase", "phase 2", "phase ii", "now preleasing", "pre-leasing", "preleasing",
        "lease up", "lease-up", "brand new community", "now open", "grand opening",
    ),
    "immediate_move_in": (
        "immediate move", "move in today", "move-in today", "available now", "available today",
        "ready now", "move in now", "move-in now", "asap", "immediately available",
    ),
    "manager_special": (
        "move in special", "move-in special", "manager special", "managers special",
        "weeks free", "week free", "month free", "months free", "free rent", "look and lease",
        "reduced rent", "rent special", "no deposit", "waived", "concession", "$99 move",
        "1st month free", "first month free",
    ),
    "private_yard": (
        "fenced yard", "fenced-in yard", "private yard", "fenced backyard", "backyard",
        "private patio", "fenced in", "large yard", "yard for dog", "dog run",
    ),
}

# Public rental/sublet/room intent. Without one of these the record is dropped.
RENTAL_INTENT = (
    "for rent", "rent", "rental", "lease", "leasing", "sublet", "sublease", "roommate",
    "room available", "room for rent", "move in", "move-in", "available now", "tenant",
    "deposit", "/mo", "per month", "monthly",
)

# Ownership-transfer surfaces are never solicitation targets for this work item.
SALE_MARKERS = (
    "for sale by owner", "fsbo", "house for sale", "home for sale", "selling my home",
    "selling my house", "owner financ", "seller financ", "rent to own", "rent-to-own",
    "lease to own", "lease purchase", "wholesale deal", "investment property for sale",
    "land for sale", "we buy houses", "cash offer for your",
)

# Demand-side posts (someone looking FOR housing) are not supply signals.
DEMAND_MARKERS = (
    "looking for a room", "need a room", "seeking a room", "in search of", "iso ",
    "wanted:", "housing wanted", "i am looking for", "we are looking for a place",
)

ROW_RE = re.compile(
    r'<li class="cl-static-search-result"[^>]*>(.*?)</li>', re.S)
HREF_RE = re.compile(r'<a href="([^"]+)"')
TITLE_RE = re.compile(r'<div class="title">(.*?)</div>', re.S)
PRICE_RE = re.compile(r'<div class="price">(.*?)</div>', re.S)
LOCATION_RE = re.compile(r'<div class="location">(.*?)</div>', re.S)
LDJSON_RE = re.compile(
    r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', re.S)
DETAIL_TITLE_RE = re.compile(r'<span id="titletextonly">(.*?)</span>', re.S)
DETAIL_PRICE_RE = re.compile(r'<span class="price">(.*?)</span>', re.S)
DETAIL_POSTED_RE = re.compile(r'datetime="([^"]+)"')
DETAIL_ATTR_RE = re.compile(r'<div class="attr(?: [^"]*)?">(.*?)</div>', re.S)
DETAIL_ATTR_CLASS_RE = re.compile(r'<div class="attr ([^"]+)">(.*?)</div>', re.S)
DETAIL_LABL_RE = re.compile(r'<span class="labl">(.*?)</span>', re.S)
DETAIL_VALU_RE = re.compile(r'<span class="valu">(.*?)</span>', re.S)
DETAIL_HOUSING_RE = re.compile(r'<span class="housing">(.*?)</span>', re.S)
DETAIL_SPAN_ATTR_RE = re.compile(r'<span class="attr ([^"]+)">(.*?)</span>', re.S)
DETAIL_HOUSING_TYPE_RE = re.compile(r'housing_type=\d+">([^<]+)<')
DETAIL_MAPADDRESS_RE = re.compile(r'<div class="mapaddress">(.*?)</div>', re.S)
DETAIL_LATLON_RE = re.compile(
    r'data-latitude="([-0-9.]+)"[^>]*data-longitude="([-0-9.]+)"')
TAG_RE = re.compile(r'<[^>]+>')


def text_of(fragment: str | None) -> str:
    if not fragment:
        return ""
    return html.unescape(TAG_RE.sub(" ", fragment)).strip()


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 16), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_receipts(receipt_path: Path) -> list[dict]:
    rows = []
    if not receipt_path.exists():
        return rows
    for line in receipt_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def verified_body(receipt: dict, root: Path) -> tuple[Path, str] | None:
    """Return the retained body only when it still hashes to the receipt."""
    body_path = receipt.get("body_path")
    if not body_path:
        return None
    path = Path(body_path)
    if not path.is_absolute():
        path = root / path
    if not path.exists():
        return None
    digest = sha256_path(path)
    if digest != receipt.get("content_sha256"):
        return None
    return path, digest


def _phrase_pattern(phrase: str) -> re.Pattern[str]:
    """Letter-bounded match so 'looking for a room' cannot fire on 'roommate'."""
    return re.compile(r"(?<![a-z])" + re.escape(phrase) + r"(?![a-z])")


_PHRASE_CACHE: dict[str, re.Pattern[str]] = {}


def phrase_hit(phrase: str, lowered: str) -> bool:
    pattern = _PHRASE_CACHE.get(phrase)
    if pattern is None:
        pattern = _PHRASE_CACHE[phrase] = _phrase_pattern(phrase)
    return pattern.search(lowered) is not None


def match_signals(text: str) -> list[str]:
    lowered = text.lower()
    return sorted(
        kind for kind, phrases in SIGNAL_LEXICON.items()
        if any(phrase_hit(phrase, lowered) for phrase in phrases)
    )


def has_rental_intent(text: str, surface_intent: bool) -> bool:
    lowered = text.lower()
    return surface_intent or any(phrase_hit(marker, lowered) for marker in RENTAL_INTENT)


def rejected_reason(text: str) -> str | None:
    lowered = text.lower()
    for marker in SALE_MARKERS:
        if phrase_hit(marker, lowered):
            return f"ownership_transfer_surface:{marker}"
    for marker in DEMAND_MARKERS:
        if phrase_hit(marker, lowered):
            return f"demand_side_post:{marker.strip()}"
    return None


def parse_craigslist_search(body: str) -> list[dict]:
    """Rows from the no-JS static result list, aligned with Schema.org geo."""
    geo: dict[int, dict] = {}
    for block in LDJSON_RE.findall(body):
        try:
            payload = json.loads(block)
        except json.JSONDecodeError:
            continue
        if payload.get("@type") != "ItemList":
            continue
        for element in payload.get("itemListElement", []):
            try:
                position = int(element.get("position"))
            except (TypeError, ValueError):
                continue
            item = element.get("item") or {}
            address = item.get("address") or {}
            geo[position] = {
                "latitude": item.get("latitude"),
                "longitude": item.get("longitude"),
                "bedrooms": item.get("numberOfBedrooms"),
                "bathrooms": item.get("numberOfBathroomsTotal"),
                "locality": address.get("addressLocality"),
                "region": address.get("addressRegion"),
                "schema_type": item.get("@type"),
            }

    rows = []
    for position, fragment in enumerate(ROW_RE.findall(body)):
        href = HREF_RE.search(fragment)
        if not href:
            continue
        row = {
            "posting_url": html.unescape(href.group(1)),
            "title": text_of(TITLE_RE.search(fragment).group(1)) if TITLE_RE.search(fragment) else "",
            "price_text": text_of(PRICE_RE.search(fragment).group(1)) if PRICE_RE.search(fragment) else "",
            "location_text": text_of(LOCATION_RE.search(fragment).group(1)) if LOCATION_RE.search(fragment) else "",
            "result_position": position,
        }
        row.update(geo.get(position, {}))
        rows.append(row)
    return rows


def parse_price(price_text: str) -> int | None:
    """Craigslist renders 'no price stated' as $0; that is absence, not a price."""
    digits = re.sub(r"[^0-9]", "", price_text or "")
    value = int(digits) if digits else None
    return value or None


def parse_craigslist_detail(body: str) -> dict:
    detail: dict = {}
    title = DETAIL_TITLE_RE.search(body)
    if title:
        detail["title"] = text_of(title.group(1))
    price = DETAIL_PRICE_RE.search(body)
    if price:
        detail["price_text"] = text_of(price.group(1))
        detail["price_usd_month"] = parse_price(detail["price_text"])
    posted = DETAIL_POSTED_RE.findall(body)
    if posted:
        detail["posted_at"] = posted[0]
        if len(posted) > 1:
            detail["updated_at"] = posted[-1]
    latlon = DETAIL_LATLON_RE.search(body)
    if latlon:
        detail["latitude"] = float(latlon.group(1))
        detail["longitude"] = float(latlon.group(2))
    housing = DETAIL_HOUSING_RE.search(body)
    if housing:
        detail["housing_line"] = re.sub(r"\s+", " ", text_of(housing.group(1))).strip(" -/")

    attributes: list[str] = []
    typed: dict[str, str] = {}
    for fragment in DETAIL_ATTR_RE.findall(body):
        label = DETAIL_LABL_RE.search(fragment)
        value = DETAIL_VALU_RE.search(fragment)
        label_text = re.sub(r"\s+", " ", text_of(label.group(1) if label else "")).strip(" :")
        value_text = re.sub(r"\s+", " ", text_of(value.group(1) if value else fragment)).strip()
        if not value_text:
            continue
        rendered = f"{label_text}: {value_text}" if label_text else value_text
        if rendered not in attributes:
            attributes.append(rendered)
    for class_name, fragment in DETAIL_ATTR_CLASS_RE.findall(body):
        value = DETAIL_VALU_RE.search(fragment)
        value_text = re.sub(r"\s+", " ", text_of(value.group(1) if value else fragment)).strip()
        if value_text:
            typed.setdefault(class_name.strip().split()[0], value_text)
    # The bed/bath, size and availability chips are spans, not divs.
    for class_name, fragment in DETAIL_SPAN_ATTR_RE.findall(body):
        value_text = re.sub(r"\s+", " ", text_of(fragment)).strip()
        if not value_text:
            continue
        if value_text not in attributes:
            attributes.append(value_text)
        for token in class_name.split():
            if token != "important":
                typed.setdefault(token, value_text)
    if attributes:
        detail["attributes"] = attributes[:40]
    if typed:
        detail["typed_attributes"] = typed

    # Pet policy is read from the posting's own typed attribute, not loose text.
    detail["dogs_stated_ok"] = "dogs are ok" in typed.get("pets_dog", "").lower()
    detail["cats_stated_ok"] = "cats are ok" in typed.get("pets_cat", "").lower()
    housing_type = DETAIL_HOUSING_TYPE_RE.search(body)
    detail["housing_type"] = housing_type.group(1).strip() if housing_type else None
    detail["available_text"] = next(
        (value for key, value in typed.items() if key.startswith("available")), None)
    detail["application_fee_text"] = typed.get("application_fee_explained")
    detail["rent_period"] = typed.get("rent_period")
    detail["furnished_stated"] = "furnished" in typed.get("is_furnished", "").lower()
    detail["no_smoking_stated"] = bool(typed.get("no_smoking"))
    mapaddress = DETAIL_MAPADDRESS_RE.search(body)
    if mapaddress:
        detail["map_cross_street"] = re.sub(r"\s+", " ", text_of(mapaddress.group(1))).strip()
    return detail


def candidate_id(source_url: str, posting_url: str) -> str:
    return hashlib.sha256(f"{source_url}\n{posting_url}".encode("utf-8")).hexdigest()[:16]


def build_candidates(receipts: list[dict], root: Path, surfaces: dict) -> tuple[list[dict], list[dict]]:
    candidates: list[dict] = []
    rejections: list[dict] = []
    for receipt in receipts:
        twin = receipt.get("twin_id", "")
        surface = surfaces.get(twin)
        if not surface or surface.get("role") != "search":
            continue
        if receipt.get("observation_kind") != "source_observed":
            continue
        resolved = verified_body(receipt, root)
        if not resolved:
            continue
        body_path, digest = resolved
        body = body_path.read_text(encoding="utf-8", errors="replace")
        for row in parse_craigslist_search(body):
            text = f"{row['title']} {row['location_text']}"
            reason = rejected_reason(text)
            if reason:
                rejections.append({
                    "schema": SCHEMA_REJECTION,
                    "posting_url": row["posting_url"],
                    "title": row["title"],
                    "rejected_because": reason,
                    "source_twin_id": twin,
                })
                continue
            if not has_rental_intent(text, surface.get("rental_intent", False)):
                rejections.append({
                    "schema": SCHEMA_REJECTION,
                    "posting_url": row["posting_url"],
                    "title": row["title"],
                    "rejected_because": "no_public_rental_intent",
                    "source_twin_id": twin,
                })
                continue
            signals = match_signals(text)
            if not signals:
                continue
            candidates.append({
                "schema": SCHEMA_CANDIDATE,
                "candidate_id": candidate_id(receipt["source_url"], row["posting_url"]),
                "market": surface.get("market"),
                "signal_kinds": signals,
                "signal_strength": len(signals),
                "title": row["title"],
                "posting_url": row["posting_url"],
                "asking_price_text": row["price_text"],
                "asking_price_usd_month": parse_price(row["price_text"]),
                "location_text": row["location_text"],
                "latitude": row.get("latitude"),
                "longitude": row.get("longitude"),
                "bedrooms": row.get("bedrooms"),
                "bathrooms": row.get("bathrooms"),
                "surface_category": surface.get("category"),
                "surface_rental_intent": surface.get("rental_intent", False),
                "source_url": receipt["source_url"],
                "source_twin_id": twin,
                "source_observed_at": receipt.get("observed_at"),
                "source_content_sha256": digest,
                "availability_claim": False,
                "exactness": "candidate",
                "corroboration": None,
            })
    return candidates, rejections


def corroborate(candidates: list[dict], receipts: list[dict], root: Path) -> list[dict]:
    """Attach an independent detail observation to candidates that have one."""
    by_url: dict[str, dict] = {}
    for receipt in receipts:
        if not str(receipt.get("twin_id", "")).startswith("cl-detail-"):
            continue
        if receipt.get("observation_kind") != "source_observed":
            continue
        resolved = verified_body(receipt, root)
        if not resolved:
            continue
        by_url[receipt["source_url"]] = {"receipt": receipt, "body": resolved[0], "sha": resolved[1]}

    listings: list[dict] = []
    for candidate in candidates:
        found = by_url.get(candidate["posting_url"])
        if not found:
            continue
        body = found["body"].read_text(encoding="utf-8", errors="replace")
        detail = parse_craigslist_detail(body)
        detail_signals = match_signals(
            f"{detail.get('title', '')} {' '.join(detail.get('attributes', []))} "
            f"{text_of(body)[:6000]}"
        )
        search_price = candidate.get("asking_price_usd_month")
        detail_price = detail.get("price_usd_month")
        agreement = {
            # null means one side stated no price -- absence, not disagreement.
            "price_agrees": (
                None if search_price is None or detail_price is None
                else search_price == detail_price
            ),
            "title_agrees": detail.get("title", "").strip() == candidate["title"].strip(),
        }
        candidate["corroboration"] = {
            "kind": "independent_detail_observation",
            "detail_twin_id": found["receipt"]["twin_id"],
            "detail_observed_at": found["receipt"].get("observed_at"),
            "detail_content_sha256": found["sha"],
            "signal_kinds_confirmed": sorted(set(candidate["signal_kinds"]) & set(detail_signals)),
            "signal_kinds_added": sorted(set(detail_signals) - set(candidate["signal_kinds"])),
            "source_agreement": agreement,
        }
        candidate["exactness"] = "corroborated_observation"
        listings.append({
            "schema": SCHEMA_LISTING,
            "candidate_id": candidate["candidate_id"],
            "market": candidate["market"],
            "posting_url": candidate["posting_url"],
            "title": detail.get("title") or candidate["title"],
            "price_usd_month": detail.get("price_usd_month"),
            "price_text": detail.get("price_text"),
            "posted_at": detail.get("posted_at"),
            "updated_at": detail.get("updated_at"),
            "latitude": detail.get("latitude", candidate.get("latitude")),
            "longitude": detail.get("longitude", candidate.get("longitude")),
            "housing_line": detail.get("housing_line"),
            "housing_type": detail.get("housing_type"),
            "available_text": detail.get("available_text"),
            "application_fee_text": detail.get("application_fee_text"),
            "rent_period": detail.get("rent_period"),
            "furnished_stated": detail.get("furnished_stated"),
            "no_smoking_stated": detail.get("no_smoking_stated"),
            "map_cross_street": detail.get("map_cross_street"),
            "attributes": detail.get("attributes", []),
            "dogs_stated_ok": detail.get("dogs_stated_ok"),
            "cats_stated_ok": detail.get("cats_stated_ok"),
            "signal_kinds": sorted(set(candidate["signal_kinds"]) | set(detail_signals)),
            "source_url": found["receipt"]["source_url"],
            "source_twin_id": found["receipt"]["twin_id"],
            "source_observed_at": found["receipt"].get("observed_at"),
            "source_content_sha256": found["sha"],
            "search_source_url": candidate["source_url"],
            "search_content_sha256": candidate["source_content_sha256"],
            "observation_kind": "posting_observed",
            "availability_claim": False,
            "availability_verified": False,
            "verification_blocked_on": "exact availability needs a contact/application effect, which this run forbids",
        })
    return listings


SHORTLIST_HEADING = "## Quirky / nonstandard supply signals (housing-social-quirky)"


def render_shortlist(candidates: list[dict], listings: list[dict], surfaces: dict) -> str:
    by_url = {row["posting_url"]: row for row in listings}
    lines = [SHORTLIST_HEADING, ""]
    lines.append(
        "Candidate-grade supply signals mined from public rental/sublet/room surfaces. "
        "A row here is an observation of a posting, never a verified vacancy: exact "
        "availability needs a contact or application effect, which this run forbids."
    )
    lines.append("")
    lines.append(
        f"- candidates: {len(candidates)} | corroborated by an independent detail "
        f"observation: {len(listings)}"
    )
    lines.append(
        "- surfaces observed: "
        + ", ".join(sorted(f"`{twin}` ({desc.get('role')})" for twin, desc in surfaces.items()))
    )
    lines.append("")

    for kind in sorted(SIGNAL_LEXICON):
        matching = [c for c in candidates if kind in c["signal_kinds"]]
        if not matching:
            continue
        corroborated = [c for c in matching if c["posting_url"] in by_url]
        lines.append(f"### {kind} ({len(matching)} candidates, {len(corroborated)} corroborated)")
        lines.append("")
        shown = corroborated or matching
        shown = sorted(shown, key=lambda c: c.get("asking_price_usd_month") or 10**9)[:8]
        for row in shown:
            listing = by_url.get(row["posting_url"])
            price = (listing or {}).get("price_usd_month") or row.get("asking_price_usd_month")
            bits = [f"**${price}/mo**" if price else "**price not stated**"]
            if listing:
                if listing.get("housing_line"):
                    bits.append(listing["housing_line"])
                if listing.get("housing_type"):
                    bits.append(listing["housing_type"])
                if listing.get("posted_at"):
                    bits.append(f"posted {listing['posted_at'][:10]}")
                if listing.get("dogs_stated_ok"):
                    bits.append("dogs stated OK")
                if listing.get("available_text"):
                    bits.append(listing["available_text"])
                bits.append("corroborated")
            else:
                bits.append(row.get("location_text") or row.get("market") or "")
                bits.append("candidate only")
            lines.append(f"- {row['title']} — " + " · ".join(b for b in bits if b))
            lines.append(f"  - signals: {', '.join(row['signal_kinds'])}")
            lines.append(f"  - source: {row['posting_url']}")
        lines.append("")

    lines.append("### Not treated as supply")
    lines.append("")
    lines.append(
        "- Private individual ownership alone is never a target; ownership-transfer "
        "postings and demand-side 'looking for a room' posts are dropped with a reason "
        "into `candidate-rejections.jsonl`."
    )
    lines.append(
        "- A `boundary_response` means a permitted source fetch was attempted and denied; "
        "a `policy_excluded` receipt means the source was not fetched because robots or "
        "the observe right disallowed it. Neither is evidence that inventory is absent."
    )
    lines.append("")
    lines.append(
        "_Other housing frontiers (effective cost, concession, park/dog geography, "
        "Pareto) append their own sections below._"
    )
    lines.append("")
    return "\n".join(lines)


def write_shortlist(path: Path, body: str) -> None:
    previous = path.read_text(encoding="utf-8") if path.exists() else ""
    head, sep, tail = previous.partition(SHORTLIST_HEADING)
    if sep:
        # Replace only this work item's section; leave other frontiers intact.
        following = tail.split("\n## ", 1)
        rest = ("\n## " + following[1]) if len(following) > 1 else ""
        path.write_text(head + body + rest, encoding="utf-8")
        return
    prefix = previous if previous.endswith("\n") or not previous else previous + "\n"
    if not prefix:
        prefix = "# Housing shortlists\n\n"
    path.write_text(prefix + body, encoding="utf-8")


def merge_jsonl(
    path: Path,
    rows: list[dict],
    owned_schema: str,
    key: str,
    legacy_owned_schema: str | None = None,
) -> int:
    """Replace only the rows this tool owns.

    ``$STATE/household/housing`` is shared by several concurrently running housing
    lanes. Truncating a shared durable output would silently delete another
    carrier's rows, so foreign rows are read back and preserved verbatim.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    mine = {row[key]: row for row in rows}
    preserved: list[dict] = []
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                existing = json.loads(line)
            except json.JSONDecodeError:
                continue
            is_legacy_row_this_run_owns = (
                legacy_owned_schema is not None
                and existing.get("schema") == legacy_owned_schema
                and existing.get(key) in mine
            )
            if existing.get("schema") != owned_schema and not is_legacy_row_this_run_owns:
                preserved.append(existing)
    preserved = [row for row in preserved if row.get(key) not in mine]
    with path.open("w", encoding="utf-8") as handle:
        for row in preserved + rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    return len(preserved)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True, help="household/housing state directory")
    parser.add_argument("--surfaces", required=True, help="JSON map of twin_id -> surface descriptor")
    parser.add_argument("--repo-root", default=".", help="root used to resolve relative body paths")
    args = parser.parse_args(argv)

    state = Path(args.state)
    root = Path(args.repo_root)
    surfaces = json.loads(Path(args.surfaces).read_text(encoding="utf-8"))
    receipts = load_receipts(state / "source-receipts.jsonl")

    candidates, rejections = build_candidates(receipts, root, surfaces)
    # One posting can appear on several search surfaces; keep the richest record.
    merged: dict[str, dict] = {}
    for candidate in candidates:
        key = candidate["posting_url"]
        previous = merged.get(key)
        if previous is None:
            merged[key] = candidate
            continue
        union = sorted(set(previous["signal_kinds"]) | set(candidate["signal_kinds"]))
        previous["signal_kinds"] = union
        previous["signal_strength"] = len(union)
        previous.setdefault("also_seen_on", []).append(candidate["source_url"])
    candidates = sorted(
        merged.values(),
        key=lambda row: (-row["signal_strength"], row.get("asking_price_usd_month") or 10**9),
    )

    listings = corroborate(candidates, receipts, root)

    merge_jsonl(state / "availability-candidates.jsonl", candidates,
                SCHEMA_CANDIDATE, "posting_url")
    merge_jsonl(state / "candidate-rejections.jsonl", rejections,
                SCHEMA_REJECTION, "posting_url")
    preserved = merge_jsonl(state / "current-listings.jsonl", listings,
                            SCHEMA_LISTING, "posting_url", LEGACY_LISTING_SCHEMA)
    write_shortlist(state / "shortlist.md", render_shortlist(candidates, listings, surfaces))

    print(json.dumps({
        "candidates": len(candidates),
        "corroborated": len(listings),
        "rejections": len(rejections),
        "receipts_read": len(receipts),
        "foreign_listing_rows_preserved": preserved,
        "signal_histogram": {
            kind: sum(1 for c in candidates if kind in c["signal_kinds"])
            for kind in sorted(SIGNAL_LEXICON)
        },
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
