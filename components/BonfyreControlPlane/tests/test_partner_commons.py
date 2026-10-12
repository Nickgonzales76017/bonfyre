import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import foreign
import partner_commons


def test_partner_profile_requires_nonownership_and_projects_read_only_twin():
    db = sqlite3.connect(":memory:")
    profile = partner_commons.PartnerProfile(
        partner_id="example",
        name="Example Partner",
        specialty="bounded specialty",
        connects_to="foreign-twin",
        does_not_own="Bonfyre semantic ownership",
        remote_url="https://example.test/",
    )
    partner_commons.record_profile(db, profile)
    partner_commons.record_observation(
        db,
        partner_commons.PartnerObservation(
            observation_id="obs-1",
            partner_id="example",
            state="observed",
            observed_at="2026-08-24T00:00:00Z",
            source_ref="https://example.test/status",
        ),
    )

    twin = partner_commons.as_foreign_twin(db, "example")
    assert twin.rights == (foreign.OBSERVE, foreign.REFERENCE)
    assert partner_commons.latest_state(db, "example") == "observed"
