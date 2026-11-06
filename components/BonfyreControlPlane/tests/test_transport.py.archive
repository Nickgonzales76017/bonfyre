"""Same operation, different transport phenotype -- and they must agree."""
import sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import transport as tp

HASH_BIN = Path.home() / ".bonfyre" / "bin" / "bonfyre-hash"
SAMPLE = Path("/tmp/pipeline-input.txt")


def _sample() -> Path:
    if not SAMPLE.exists():
        SAMPLE.write_text("transport phenotype test\n")
    return SAMPLE


@unittest.skipUnless(HASH_BIN.exists(), "bonfyre-hash not installed")
class DirectTransportTests(unittest.TestCase):
    def test_direct_runs_and_carries_no_receipt(self):
        r = tp.run_hop(family="BonfyreHash", binary=str(HASH_BIN),
                       args=["file", str(_sample())], transport=tp.DIRECT)
        self.assertEqual(r.exit_code, 0)
        self.assertIsNone(r.receipt_id)


@unittest.skipUnless(HASH_BIN.exists() and tp.mcp_available(),
                     "MCP server not present")
class TransportEquivalenceTests(unittest.TestCase):
    def test_direct_and_mcp_produce_the_same_result(self):
        args = ["file", str(_sample())]
        d = tp.run_hop(family="BonfyreHash", binary=str(HASH_BIN), args=args, transport=tp.DIRECT)
        m = tp.run_hop(family="BonfyreHash", binary=str(HASH_BIN), args=args, transport=tp.MCP)
        self.assertEqual(d.stdout.split()[0], m.stdout.split()[0])

    def test_only_the_mcp_carrier_provides_provenance(self):
        args = ["file", str(_sample())]
        d = tp.run_hop(family="BonfyreHash", binary=str(HASH_BIN), args=args, transport=tp.DIRECT)
        m = tp.run_hop(family="BonfyreHash", binary=str(HASH_BIN), args=args, transport=tp.MCP)
        self.assertIsNone(d.receipt_id)
        self.assertIsNotNone(m.receipt_id)
        self.assertIsNotNone(m.stdout_sha256)

    def test_the_mcp_path_exercises_the_real_server(self):
        # The module imported is the actual running MCP server, not a mock.
        self.assertTrue(tp.MCP_SERVER.exists())
        self.assertTrue(str(tp.MCP_SERVER).endswith("project_node.py"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
