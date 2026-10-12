"""ForeignTwins: remote resources that stay remote, accessed over a boundary.

The essay's §21 abstraction. A ForeignTwin is a reference to something owned by
another institution -- an API, a service, a remote artifact -- with a cached
projection, not a copy that pretends to be local truth. Two disciplines it
enforces, both from the archive:

  external identity is not authority   a twin carries explicit rights. A hop
                                       that would act on a twin without the
                                       right is refused, the same way the proof
                                       frontier refuses a proven-layer mutation.
  the projection is cached, not truth  fetching a twin materializes a cached
                                       projection into the fabric, which
                                       BonfyreFS can then mount as a file --
                                       remote reality made locally addressable
                                       without becoming canonical.

This gives the closure a third kind of hop beyond local binaries and the
direct/MCP transports: a foreign hop over an external (HTTP) carrier, gated by
rights and materialized into the fabric for atomic-form / BonfyreFS use.
"""

from __future__ import annotations

import json
import subprocess
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Sequence

# Rights a twin can grant. Observe is the weakest; act mutates the remote.
OBSERVE = "observe"
REFERENCE = "reference"
BIND = "bind"
ACT = "act"

RIGHTS = (OBSERVE, REFERENCE, BIND, ACT)


@dataclass(frozen=True)
class ForeignTwin:
    remote_resource_id: str
    source_institution: str
    url: str
    profile: str = "http/v1"
    rights: Sequence[str] = field(default_factory=lambda: (OBSERVE,))
    cached_projection: Optional[str] = None

    def may(self, right: str) -> bool:
        return right in self.rights


@dataclass(frozen=True)
class ForeignResult:
    twin_id: str
    ok: bool
    status: int
    projection: Optional[str]
    reason: str


def fetch(twin: ForeignTwin, *, timeout: int = 8) -> ForeignResult:
    """Fetch a twin's projection over its boundary, gated by the observe right.

    Observation is the minimum. A twin the operator did not grant observe on is
    not read -- external presence is not permission.
    """
    if not twin.may(OBSERVE):
        return ForeignResult(twin.remote_resource_id, False, 0, None,
                             "no observe right on this twin")
    try:
        with urllib.request.urlopen(twin.url, timeout=timeout) as response:
            body = response.read().decode("utf-8", "replace")
            return ForeignResult(twin.remote_resource_id, True, response.status,
                                body, f"observed {len(body)} bytes")
    except Exception as error:  # noqa: BLE001 - a boundary failure is data
        return ForeignResult(twin.remote_resource_id, False, -1, None,
                            f"boundary error: {type(error).__name__}")


def materialize(
    twin: ForeignTwin,
    result: ForeignResult,
    *,
    bonfyre: Path,
    state_dir: Path,
) -> Optional[str]:
    """Ingest a twin's cached projection into the fabric.

    The projection becomes a content-addressed artifact BonfyreFS can mount --
    remote reality made a local file without becoming canonical truth. This is
    the tie to BonfyreFS, which is laid out to work with atomic forms: a foreign
    resource materialized here can back an atomic form's evidence slot.
    """
    if not result.ok or result.projection is None:
        return None
    scratch = Path("/tmp") / f"twin-{twin.remote_resource_id.replace('/', '_')}.json"
    scratch.write_text(json.dumps({
        "remote_resource_id": twin.remote_resource_id,
        "source_institution": twin.source_institution,
        "profile": twin.profile,
        "observed_status": result.status,
        "projection": result.projection,
    }))
    done = subprocess.run(
        [str(bonfyre), "artifact", "ingest", str(scratch), "application/json"],
        capture_output=True, text=True, timeout=30,
        env={"BONFYRE_STATE_DIR": str(state_dir), "PATH": "/usr/bin:/bin"},
    )
    if done.returncode != 0:
        return None
    for line in done.stdout.splitlines():
        if line.startswith("digest="):
            return line.split("=", 1)[1].strip()
    return None
