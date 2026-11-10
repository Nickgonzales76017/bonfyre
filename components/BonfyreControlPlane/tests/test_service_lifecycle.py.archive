"""The provisionable-service lifecycle over a real service, rights-gated."""
import sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import service_lifecycle as sl
import foreign as fg

API = Path.home() / ".bonfyre" / "bin" / "bonfyre-api"


class RightsAndStateTests(unittest.TestCase):
    def test_bind_requires_the_bind_right(self):
        p = sl.ServiceProfile("x", "y", "/nonexistent", 9099, rights=(fg.OBSERVE,))
        p.state = sl.PROVISIONED
        self.assertFalse(sl.bind(p).ok)

    def test_cannot_bind_before_provision(self):
        p = sl.ServiceProfile("x", "y", "/nonexistent", 9099,
                             rights=(fg.OBSERVE, fg.BIND))
        self.assertFalse(sl.bind(p).ok)  # state is CATALOGED

    def test_cannot_execute_unbound(self):
        p = sl.ServiceProfile("x", "y", "/nonexistent", 9099)
        self.assertFalse(sl.execute(p).ok)

    def test_release_is_safe_with_no_process(self):
        p = sl.ServiceProfile("x", "y", "/nonexistent", 9099)
        r = sl.release(p)
        self.assertTrue(r.ok)
        self.assertEqual(p.state, sl.RELEASED)


@unittest.skipUnless(API.exists(), "bonfyre-api not installed")
class LiveLifecycleTests(unittest.TestCase):
    def test_full_cycle_over_a_real_service(self):
        p = sl.ServiceProfile("test-api", "bonfyre", str(API), 8097,
                             rights=(fg.OBSERVE, fg.BIND))
        try:
            self.assertTrue(sl.provision(p).ok)
            self.assertEqual(p.state, sl.PROVISIONED)
            self.assertTrue(sl.bind(p).ok)
            e = sl.execute(p)
            self.assertTrue(e.ok)
            self.assertIn("bonfyre-api", e.detail)
        finally:
            self.assertTrue(sl.release(p).ok)
        # after release, the service is gone
        self.assertFalse(sl.execute(p).ok)


if __name__ == "__main__":
    unittest.main(verbosity=2)
