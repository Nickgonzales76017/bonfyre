"""ServiceLifecycle: the provisionable-service boundary (the OSB shape).

The essay's §18 point -- some capabilities do not exist yet and must be
provisioned before use: catalog, provision, bind, execute, release. This is the
Open Service Broker lifecycle expressed over the real boundary commands, gated
by ForeignTwin rights. A provisioned service is a ForeignTwin with a lifecycle:
it can be stood up, bound (requires the bind right), used, and released.

Proven against a real provisionable service -- a BonfyreAPI instance this
actually starts, binds, calls over HTTP, and stops. Not a mock lifecycle: the
process exists between provision and release, and does not before or after.

The disciplines carried forward: bind requires the bind right (external
identity is not authority), and release is idempotent and always safe, because a
service that cannot be reliably torn down leaks -- the Run 6 lesson applied to
provisioned capability.
"""

from __future__ import annotations

import subprocess
import time
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Sequence

import foreign as fg

CATALOGED = "cataloged"
PROVISIONED = "provisioned"
BOUND = "bound"
RELEASED = "released"


@dataclass
class ServiceProfile:
    service_id: str
    provider_institution: str
    binary: str
    port: int
    rights: Sequence[str] = field(default_factory=lambda: (fg.OBSERVE, fg.BIND))
    state: str = CATALOGED
    _process: Optional[subprocess.Popen] = None
    _log: Optional[Path] = None


@dataclass(frozen=True)
class LifecycleResult:
    service_id: str
    state: str
    ok: bool
    detail: str


def provision(profile: ServiceProfile, *, wait: float = 3.0) -> LifecycleResult:
    """Stand the service up. It exists as a real process after this returns."""
    binary = Path(profile.binary)
    if not binary.exists():
        return LifecycleResult(profile.service_id, profile.state, False,
                              f"binary {binary} absent")
    log = Path("/tmp") / f"svc-{profile.service_id}.log"
    db = Path("/tmp") / f"svc-{profile.service_id}.db"
    db.unlink(missing_ok=True)
    profile._log = log
    profile._process = subprocess.Popen(
        [str(binary), "serve", "--port", str(profile.port), "--db", str(db)],
        stdout=log.open("w"), stderr=subprocess.STDOUT, start_new_session=True,
    )
    time.sleep(wait)
    profile.state = PROVISIONED
    return LifecycleResult(profile.service_id, PROVISIONED, True,
                          f"pid {profile._process.pid} on :{profile.port}")


def bind(profile: ServiceProfile) -> LifecycleResult:
    """Bind to the provisioned service. Requires the bind right."""
    if fg.BIND not in profile.rights:
        return LifecycleResult(profile.service_id, profile.state, False,
                              "no bind right -- external presence is not authority")
    if profile.state != PROVISIONED:
        return LifecycleResult(profile.service_id, profile.state, False,
                              "cannot bind a service that is not provisioned")
    profile.state = BOUND
    return LifecycleResult(profile.service_id, BOUND, True, "bound")


def execute(profile: ServiceProfile, path: str = "/api/health",
            *, timeout: int = 5) -> LifecycleResult:
    """Call the bound service over its HTTP boundary."""
    if profile.state != BOUND:
        return LifecycleResult(profile.service_id, profile.state, False,
                              "service is not bound")
    url = f"http://127.0.0.1:{profile.port}{path}"
    try:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        body = opener.open(url, timeout=timeout).read().decode("utf-8", "replace")
        return LifecycleResult(profile.service_id, BOUND, True, body[:120])
    except Exception as error:  # noqa: BLE001
        return LifecycleResult(profile.service_id, BOUND, False,
                              f"boundary error: {type(error).__name__}")


def release(profile: ServiceProfile) -> LifecycleResult:
    """Tear the service down. Idempotent and always safe -- no leak."""
    proc = profile._process
    if proc is not None:
        try:
            import os, signal
            os.killpg(proc.pid, signal.SIGTERM)
            proc.wait(timeout=3)
        except Exception:  # noqa: BLE001
            try:
                proc.kill()
            except Exception:  # noqa: BLE001
                pass
        profile._process = None
    profile.state = RELEASED
    return LifecycleResult(profile.service_id, RELEASED, True, "released")
