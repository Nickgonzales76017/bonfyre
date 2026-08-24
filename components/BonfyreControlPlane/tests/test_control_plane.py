"""Tests written against the two Run 6 failures they exist to make impossible.

Each test names the incident it reproduces, so a future change that reintroduces
the defect fails with the history attached rather than a bare assertion.
"""

import datetime as dt
import sqlite3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import provider_state as ps  # noqa: E402
import resource_admission as ra  # noqa: E402

UTC = dt.timezone.utc


def at(day: int, hour: int = 0, minute: int = 0) -> dt.datetime:
    return dt.datetime(2026, 8, day, hour, minute, tzinfo=UTC)


class ProviderFoldTests(unittest.TestCase):
    def test_transient_failure_cannot_shorten_a_hard_capacity_window(self):
        """The Run 6 incident: Codex reported a usage limit resetting
        2026-08-19, a later unrelated transient failure overwrote the circuit,
        and the guardian relaunched Run 5 straight into the exhausted provider.
        """
        observations = [
            ps.Observation("codex", ps.HARD_CAPACITY, at(13, 22, 23), reset_at=at(19, 22, 53)),
            ps.Observation("codex", ps.TRANSIENT_FAILURE, at(13, 22, 30), detail="502"),
        ]
        state = ps.derive_state("codex", observations, now=at(13, 22, 43))
        self.assertEqual(state.status, "capacity_exhausted")
        self.assertEqual(state.circuit_until, at(19, 22, 53))
        self.assertFalse(state.available_at(at(13, 22, 43)))
        self.assertFalse(state.available_at(at(18, 0)))

    def test_success_does_not_clear_an_unexpired_hard_capacity(self):
        """One cached response served during a capacity window is not evidence
        that capacity returned."""
        observations = [
            ps.Observation("codex", ps.HARD_CAPACITY, at(13, 22), reset_at=at(19, 22)),
            ps.Observation("codex", ps.SUCCESS, at(13, 23)),
        ]
        state = ps.derive_state("codex", observations, now=at(14, 0))
        self.assertEqual(state.status, "capacity_exhausted")

    def test_hard_capacity_expires_on_its_own_reset_time(self):
        observations = [
            ps.Observation("codex", ps.HARD_CAPACITY, at(13, 22), reset_at=at(19, 22)),
        ]
        self.assertFalse(ps.derive_state("codex", observations, at(19, 21)).available_at(at(19, 21)))
        after = ps.derive_state("codex", observations, at(19, 23))
        self.assertEqual(after.status, "ready")
        self.assertTrue(after.available_at(at(19, 23)))

    def test_later_hard_capacity_extends_but_never_shortens(self):
        observations = [
            ps.Observation("codex", ps.HARD_CAPACITY, at(13), reset_at=at(19)),
            ps.Observation("codex", ps.HARD_CAPACITY, at(14), reset_at=at(15)),
        ]
        state = ps.derive_state("codex", observations, now=at(14, 1))
        self.assertEqual(state.circuit_until, at(19))

    def test_transient_failures_back_off_and_recover(self):
        observations = [
            ps.Observation("claude", ps.TRANSIENT_FAILURE, at(13, 10), detail="429"),
        ]
        cooling = ps.derive_state("claude", observations, now=at(13, 10))
        self.assertEqual(cooling.status, "cooling")
        recovered = ps.derive_state("claude", observations, now=at(13, 11))
        self.assertEqual(recovered.status, "ready")

    def test_manual_pause_holds_until_explicit_resume(self):
        observations = [
            ps.Observation("codex", ps.MANUAL_PAUSE, at(13, 22, 46), detail="Run6 freeze"),
        ]
        paused = ps.derive_state("codex", observations, now=at(20, 0))
        self.assertEqual(paused.status, "manual_pause")
        self.assertFalse(paused.available_at(at(20, 0)))

        resumed = ps.derive_state(
            "codex",
            observations + [ps.Observation("codex", ps.MANUAL_RESUME, at(20, 1))],
            now=at(20, 2),
        )
        self.assertEqual(resumed.status, "ready")

    def test_observations_are_append_only_through_the_api(self):
        db = sqlite3.connect(":memory:")
        ps.ensure_schema(db)
        ps.record(db, "codex", ps.HARD_CAPACITY, observed_at=at(13, 22), reset_at=at(19, 22))
        ps.record(db, "codex", ps.TRANSIENT_FAILURE, observed_at=at(13, 23), detail="503")
        self.assertEqual(len(ps.load_observations(db, "codex")), 2)
        state = ps.current_state(db, "codex", now=at(14, 0))
        self.assertEqual(state.status, "capacity_exhausted")
        self.assertEqual(state.circuit_until, at(19, 22))

    def test_rejects_unknown_event_kinds(self):
        with self.assertRaises(ValueError):
            ps.Observation("codex", "made_up", at(13))


# Verbatim from run6-evidence-freeze-20260813-174623. Paraphrasing these is how
# the original matcher ended up looking for "usage limit reached" while Codex
# was actually saying "You've hit your usage limit".
CODEX_LIMIT = (
    "You've hit your usage limit. Upgrade to Pro "
    "(https://chatgpt.com/explore/pro), visit "
    "https://chatgpt.com/codex/settings/usage to purchase more credits or "
    "try again at Aug 19th, 2026 10:53 PM."
)
CLAUDE_LIMIT = "session limit · resets 7:20pm (America/Chicago)"


class FailureClassificationTests(unittest.TestCase):
    def test_parses_the_real_codex_message_including_its_ordinal_date(self):
        """'Aug 19th' -- the ordinal suffix broke a naive month/day regex."""
        kind, reset = ps.classify_failure(CODEX_LIMIT, now=at(13, 22, 23))
        self.assertEqual(kind, ps.HARD_CAPACITY)
        self.assertEqual(reset, dt.datetime(2026, 8, 19, 22, 53, tzinfo=UTC))

    def test_parses_the_real_claude_wall_clock_reset(self):
        """Claude states a local time with no date; the reset is the next
        occurrence of it. 7:20pm America/Chicago on 2026-08-13 is CDT (UTC-5),
        so 00:20Z the following day."""
        kind, reset = ps.classify_failure(CLAUDE_LIMIT, now=at(13, 22, 23))
        self.assertEqual(kind, ps.HARD_CAPACITY)
        self.assertEqual(reset, dt.datetime(2026, 8, 14, 0, 20, tzinfo=UTC))

    def test_wall_clock_reset_later_today_does_not_roll_forward_a_day(self):
        kind, reset = ps.classify_failure(CLAUDE_LIMIT, now=at(13, 18, 0))
        self.assertEqual(kind, ps.HARD_CAPACITY)
        self.assertEqual(reset, dt.datetime(2026, 8, 14, 0, 20, tzinfo=UTC))

    def test_recovers_an_iso_reset(self):
        kind, reset = ps.classify_failure("session limit; resets 2026-08-19T22:53:00Z")
        self.assertEqual(kind, ps.HARD_CAPACITY)
        self.assertEqual(reset, dt.datetime(2026, 8, 19, 22, 53, tzinfo=UTC))

    def test_hard_capacity_without_a_reset_still_classifies_hard(self):
        kind, reset = ps.classify_failure("quota exhausted")
        self.assertEqual(kind, ps.HARD_CAPACITY)
        self.assertIsNone(reset)

    def test_transient_markers_stay_transient(self):
        kind, _ = ps.classify_failure("503 service unavailable")
        self.assertEqual(kind, ps.TRANSIENT_FAILURE)


class Run6IncidentReplayTests(unittest.TestCase):
    """End-to-end replay of the relaunch, on the frozen timestamps.

    From run6_provider_state at freeze:
        codex last_success_at  2026-08-13T22:23:23+00:00
        codex last_started_at  2026-08-13T22:43:48+00:00
    Between those, Codex reported the hard limit resetting Aug 19th. The
    supervisor started it again 20 minutes later anyway.
    """

    LIMIT_OBSERVED = dt.datetime(2026, 8, 13, 22, 23, 23, tzinfo=UTC)
    RELAUNCH_ATTEMPT = dt.datetime(2026, 8, 13, 22, 43, 48, tzinfo=UTC)

    def test_the_relaunch_is_refused(self):
        db = sqlite3.connect(":memory:")
        ps.ensure_schema(db)

        kind, reset = ps.classify_failure(CODEX_LIMIT, now=self.LIMIT_OBSERVED)
        ps.record(db, "codex", kind, observed_at=self.LIMIT_OBSERVED, reset_at=reset)

        # The unrelated transient failure that overwrote the circuit in Run 6.
        ps.record(
            db,
            "codex",
            ps.TRANSIENT_FAILURE,
            observed_at=dt.datetime(2026, 8, 13, 22, 30, tzinfo=UTC),
            detail="local pipe failure",
        )

        state = ps.current_state(db, "codex", now=self.RELAUNCH_ATTEMPT)
        self.assertEqual(state.status, "capacity_exhausted")
        self.assertEqual(state.circuit_until, dt.datetime(2026, 8, 19, 22, 53, tzinfo=UTC))
        self.assertFalse(
            state.available_at(self.RELAUNCH_ATTEMPT),
            "Run 5 must not be launchable into an exhausted provider",
        )

    def test_the_provider_returns_only_after_its_stated_reset(self):
        db = sqlite3.connect(":memory:")
        ps.ensure_schema(db)
        kind, reset = ps.classify_failure(CODEX_LIMIT, now=self.LIMIT_OBSERVED)
        ps.record(db, "codex", kind, observed_at=self.LIMIT_OBSERVED, reset_at=reset)

        before = ps.current_state(db, "codex", now=dt.datetime(2026, 8, 19, 22, 52, tzinfo=UTC))
        self.assertFalse(before.available_at(dt.datetime(2026, 8, 19, 22, 52, tzinfo=UTC)))

        after = dt.datetime(2026, 8, 19, 22, 54, tzinfo=UTC)
        self.assertTrue(ps.current_state(db, "codex", now=after).available_at(after))


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.policy = ra.AdmissionPolicy(
            protected_floor_bytes=10 * ra.GIB,
            per_plane_quota_bytes=20 * ra.GIB,
            max_grant_bytes=40 * ra.GIB,
        )

    def test_refuses_the_celld_clone_that_caused_enospc(self):
        """Run 5 began building the 316-crate celld workspace with far less
        headroom than it needed and hit ENOSPC mid-build."""
        request = ra.ResourceRequest("run5", "cargo_workspace", 15 * ra.GIB)
        decision = ra.decide(request, self.policy, free_bytes=18 * ra.GIB, committed_bytes=0)
        self.assertEqual(decision.verdict, ra.REJECT)
        self.assertIn("protected floor", decision.reason)

    def test_admits_the_same_request_with_real_headroom(self):
        request = ra.ResourceRequest("run5", "cargo_workspace", 15 * ra.GIB)
        decision = ra.decide(request, self.policy, free_bytes=40 * ra.GIB, committed_bytes=0)
        self.assertEqual(decision.verdict, ra.ADMIT)

    def test_concurrent_planes_cannot_all_spend_the_same_free_space(self):
        """Five planes ran at once in Run 6. A bare free-space check admits all
        of them against the same bytes."""
        db = sqlite3.connect(":memory:")
        ra.ensure_schema(db)
        probe = lambda _volume: 40 * ra.GIB  # noqa: E731

        admitted = 0
        for plane in ("run1", "run2", "run3", "run4", "run5"):
            request = ra.ResourceRequest(plane, "cargo_workspace", 12 * ra.GIB)
            decision, grant = ra.request_grant(db, request, self.policy, probe=probe)
            if decision.admitted:
                admitted += 1
                self.assertIsNotNone(grant)

        # 40 GiB free, 10 GiB floor => 30 GiB spendable => two 12 GiB grants.
        self.assertEqual(admitted, 2)
        self.assertEqual(ra.committed_bytes(db, "/"), 24 * ra.GIB)

    def test_releasing_a_grant_returns_the_space(self):
        db = sqlite3.connect(":memory:")
        ra.ensure_schema(db)
        probe = lambda _volume: 40 * ra.GIB  # noqa: E731

        first, grant_id = ra.request_grant(
            db, ra.ResourceRequest("run5", "build", 20 * ra.GIB), self.policy, probe=probe
        )
        self.assertTrue(first.admitted)
        blocked, _ = ra.request_grant(
            db, ra.ResourceRequest("run2", "build", 20 * ra.GIB), self.policy, probe=probe
        )
        self.assertEqual(blocked.verdict, ra.DEFER)

        ra.release_grant(db, grant_id)
        unblocked, _ = ra.request_grant(
            db, ra.ResourceRequest("run2", "build", 20 * ra.GIB), self.policy, probe=probe
        )
        self.assertTrue(unblocked.admitted)

    def test_per_plane_quota_defers_a_greedy_plane(self):
        """Run 5 produced 53% of receipts and consumed the most of everything.
        One plane must not be able to take the whole machine."""
        db = sqlite3.connect(":memory:")
        ra.ensure_schema(db)
        probe = lambda _volume: 200 * ra.GIB  # noqa: E731

        for _ in range(2):
            decision, _ = ra.request_grant(
                db, ra.ResourceRequest("run5", "build", 10 * ra.GIB), self.policy, probe=probe
            )
            self.assertTrue(decision.admitted)

        over, _ = ra.request_grant(
            db, ra.ResourceRequest("run5", "build", 10 * ra.GIB), self.policy, probe=probe
        )
        self.assertEqual(over.verdict, ra.DEFER)
        self.assertIn("per-plane quota", over.reason)

        # A different plane is unaffected by run5's quota.
        other, _ = ra.request_grant(
            db, ra.ResourceRequest("run2", "build", 10 * ra.GIB), self.policy, probe=probe
        )
        self.assertTrue(other.admitted)

    def test_defer_and_reject_are_distinguished(self):
        request = ra.ResourceRequest("run5", "build", 15 * ra.GIB)
        # Space exists but is committed elsewhere -> waiting could help.
        deferred = ra.decide(
            request, self.policy, free_bytes=40 * ra.GIB, committed_bytes=20 * ra.GIB
        )
        self.assertEqual(deferred.verdict, ra.DEFER)
        # Space does not exist at all -> waiting cannot help.
        rejected = ra.decide(
            request, self.policy, free_bytes=20 * ra.GIB, committed_bytes=0
        )
        self.assertEqual(rejected.verdict, ra.REJECT)

    def test_stale_grants_from_killed_planes_age_out(self):
        """Run 6 lost planes to pipe deadlock, rc137 and ENOSPC. Grants that
        only release on a clean exit would ratchet toward starvation."""
        db = sqlite3.connect(":memory:")
        ra.ensure_schema(db)
        probe = lambda _volume: 40 * ra.GIB  # noqa: E731
        ra.request_grant(
            db,
            ra.ResourceRequest("run5", "build", 20 * ra.GIB),
            self.policy,
            probe=probe,
            now=at(13, 20),
        )
        self.assertEqual(ra.committed_bytes(db, "/"), 20 * ra.GIB)

        reaped = ra.reap_expired(db, dt.timedelta(hours=6), now=at(14, 6))
        self.assertEqual(reaped, 1)
        self.assertEqual(ra.committed_bytes(db, "/"), 0)

    def test_a_sizeless_request_is_refused(self):
        decision = ra.decide(
            ra.ResourceRequest("run5", "build", 0), self.policy, 100 * ra.GIB, 0
        )
        self.assertEqual(decision.verdict, ra.REJECT)


if __name__ == "__main__":
    unittest.main(verbosity=2)
