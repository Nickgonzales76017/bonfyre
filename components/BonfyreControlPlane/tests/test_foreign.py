"""ForeignTwins stay remote, are rights-gated, and materialize into the fabric."""
import sys, unittest, urllib.request
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import foreign as fg

API = "http://127.0.0.1:8092/api/health"


def _api_up() -> bool:
    try:
        op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        return op.open(API, timeout=3).status == 200
    except Exception:
        return False


class RightsTests(unittest.TestCase):
    def test_a_twin_without_observe_is_not_read(self):
        twin = fg.ForeignTwin("x/y", "inst", API, rights=(fg.REFERENCE,))
        r = fg.fetch(twin, timeout=3)
        self.assertFalse(r.ok)
        self.assertIn("observe right", r.reason)

    def test_external_identity_is_not_authority(self):
        # A twin from an institution with only observe cannot act.
        twin = fg.ForeignTwin("x/y", "some-external-org", API, rights=(fg.OBSERVE,))
        self.assertFalse(twin.may(fg.ACT))
        self.assertTrue(twin.may(fg.OBSERVE))

    def test_a_boundary_failure_is_data_not_a_crash(self):
        twin = fg.ForeignTwin("x/y", "inst", "http://127.0.0.1:9/dead",
                              rights=(fg.OBSERVE,))
        r = fg.fetch(twin, timeout=2)
        self.assertFalse(r.ok)
        self.assertIn("boundary error", r.reason)


@unittest.skipUnless(_api_up(), "local BonfyreAPI not running")
class LiveExternalTransportTests(unittest.TestCase):
    def test_observe_fetches_the_projection_over_http(self):
        twin = fg.ForeignTwin("svc/health", "local-api", API, rights=(fg.OBSERVE,))
        r = fg.fetch(twin, timeout=5)
        self.assertTrue(r.ok)
        self.assertEqual(r.status, 200)
        self.assertIn("bonfyre-api", r.projection)


if __name__ == "__main__":
    unittest.main(verbosity=2)
