"""CID prefix routing (placement, not meaning) and SID SimHash (locality, not
truth). The SimHash mechanism is checked on controlled vectors; the integration
over real Bonfyre embeddings runs when bonfyre-embed is present."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import address_plane as ap
import bonfyre_sid as sid


# --------------------------------------------------------------- CID routing

def test_cid_prefix_is_the_top_bits():
    d = "b0" + "0" * 62   # 0xb0... -> top 4 bits = 1011 = 11
    assert ap.cid_prefix(d, 4) == 0xB


def test_cid_longest_prefix_match():
    d = "a" * 64  # top bits: 1010 1010 ...
    table = [
        ap.PrefixRoute(prefix=0b1, nbits=1, route="lan-replica"),
        ap.PrefixRoute(prefix=0b1010, nbits=4, route="model-objects"),
    ]
    # both prefixes match; the longer (4-bit) wins
    assert ap.route_cid(d, table) == "model-objects"
    # a digest matching neither falls to default
    assert ap.route_cid("0" * 64, table) == "fabric-primary"


# --------------------------------------------------------------- SID SimHash

def test_simhash_preserves_proximity():
    v = [((i * 37) % 11) - 5 for i in range(64)]
    same = sid.simhash(v)
    assert sid.hamming(same, sid.simhash(v)) == 0            # deterministic
    # a nearly-identical vector stays close; the negation flips every bit
    near = [x + 0.001 for x in v]
    opp = [-x for x in v]
    assert sid.hamming(same, sid.simhash(near)) < sid.hamming(same, sid.simhash(opp))
    assert sid.hamming(same, sid.simhash(opp)) == sid._SID_BITS


def test_neighborhood_ranks_by_hamming():
    q = 0b1010
    cands = {"far": 0b0101, "near": 0b1011, "exact": 0b1010}
    ranked = sid.neighborhood(q, cands, top=2)
    assert ranked[0] == ("exact", 0) and ranked[1][0] == "near"


@pytest.mark.skipif(not sid.embed_available(), reason="requires bonfyre-embed")
def test_sid_pipeline_over_real_bonfyre_embeddings():
    # uses Bonfyre's real embedding substrate end to end: text -> vector -> SID.
    # Asserts the pipeline runs and compiles a valid, deterministic SID. (Neighbor-
    # hood *quality* depends on the embedding backend; short-text hash embeddings
    # are near-orthogonal, so the ranking is exercised on controlled vectors above,
    # not on these weak embeddings.)
    v = sid.embed("FPQ functional polar quantization compresses model weights")
    assert len(v) > 0
    s1 = sid.simhash(v)
    assert s1 != 0
    assert sid.hamming(s1, sid.simhash(v)) == 0   # deterministic over the real vector
