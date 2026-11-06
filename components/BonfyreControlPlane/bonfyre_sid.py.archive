"""SID: semantic-neighborhood locality, via SimHash over real Bonfyre embeddings.

CID answers "is this the same object, and where does it live"; SID answers "what
is approximately like this one." Identity is cryptographic and random with
respect to meaning; locality must come from a semantic representation instead.

This uses Bonfyre's own embedding substrate -- bonfyre-embed (onnx/MiniLM or the
hash backend) -- for the vector, then compiles it to a binary SID with SimHash:
each bit is the sign of the vector's projection onto a fixed random hyperplane,
so Hamming distance between two SIDs tracks the angle between their vectors. Two
objects with similar embeddings land in the same SID neighborhood; a Hamming
lookup returns candidates a slot might be satisfied by, which the exact
EvidenceGraph / ActorGraph then verifies.

SID is a locality index, never truth: a small Hamming distance suggests a
candidate, it does not establish identity, evidence, or authority.
"""

from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path
from typing import Optional, Sequence

BONFYRE_EMBED = Path.home() / ".bonfyre" / "bin" / "bonfyre-embed"

# Fixed hyperplane basis. Deterministic pseudo-random signs from a seed, so the
# same vector always compiles to the same SID -- a stable compiled index.
_SEED = 1337
_SID_BITS = 256


def embed_available() -> bool:
    return BONFYRE_EMBED.exists()


def embed(text: str, *, backend: str = "onnx", dims: int = 0, timeout: int = 60) -> list[float]:
    """Embed text with the real Bonfyre embedding substrate."""
    if not BONFYRE_EMBED.exists():
        raise RuntimeError("bonfyre-embed not installed")
    with tempfile.TemporaryDirectory() as d:
        tin = Path(d) / "in.txt"
        tout = Path(d) / "out.json"
        tin.write_text(text)
        argv = [str(BONFYRE_EMBED), "--text", str(tin), "--out", str(tout),
                "--backend", backend, "--output-format", "json"]
        if dims:
            argv += ["--dims", str(dims)]
        subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
        d = json.loads(tout.read_text())
    return list(d.get("vector", []))


def _planes(dim: int, nbits: int = _SID_BITS, seed: int = _SEED) -> list[list[float]]:
    """nbits fixed random hyperplanes over a dim-dimensional space. A cheap,
    deterministic LCG keeps this dependency-free and reproducible."""
    state = seed & 0xFFFFFFFF
    planes: list[list[float]] = []
    for _ in range(nbits):
        row: list[float] = []
        for _ in range(dim):
            state = (1103515245 * state + 12345) & 0x7FFFFFFF
            row.append((state / 0x7FFFFFFF) - 0.5)  # in [-0.5, 0.5)
        planes.append(row)
    return planes


def simhash(vector: Sequence[float], nbits: int = _SID_BITS, seed: int = _SEED) -> int:
    """Compile a vector to a binary SID: bit i is the sign of the projection onto
    hyperplane i. Hamming distance between SIDs tracks the angle between vectors."""
    planes = _planes(len(vector), nbits, seed)
    sid = 0
    for i, plane in enumerate(planes):
        dot = sum(v * p for v, p in zip(vector, plane))
        if dot >= 0.0:
            sid |= 1 << i
    return sid


def hamming(a: int, b: int) -> int:
    return bin(a ^ b).count("1")


def sid_of(text: str, *, backend: str = "onnx") -> int:
    return simhash(embed(text, backend=backend))


def neighborhood(query: int, candidates: dict[str, int], top: int = 5) -> list[tuple[str, int]]:
    """The nearest SIDs to the query, by Hamming distance -- the semantic
    neighborhood a slot searches before exact verification."""
    ranked = sorted(((cid, hamming(query, s)) for cid, s in candidates.items()), key=lambda t: t[1])
    return ranked[:top]
