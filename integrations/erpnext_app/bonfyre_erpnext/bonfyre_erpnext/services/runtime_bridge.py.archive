"""Bridge to the local Bonfyre dev-fabric broker (bonfyre-dual-agent-fabric).

Talks to the broker's HTTP API on localhost. Token file location and default
port match the broker's own conventions (~/.bonfyre/mcp/token,
BONFYRE_MCP_TOKEN_FILE) so this doesn't need separate credentials.
"""

import os
import urllib.error
import urllib.request
from pathlib import Path

DEFAULT_BROKER_URL = "http://127.0.0.1:3053"
DEFAULT_TOKEN_FILE = Path.home() / ".bonfyre" / "mcp" / "token"


def _token() -> str:
    token_file = Path(os.environ.get("BONFYRE_MCP_TOKEN_FILE", str(DEFAULT_TOKEN_FILE)))
    if token_file.exists():
        return token_file.read_text().strip()
    return os.environ.get("BONFYRE_MCP_TOKEN", "")


def _broker_url() -> str:
    return os.environ.get("BONFYRE_BROKER_URL", DEFAULT_BROKER_URL)


def is_available(timeout: float = 2.0) -> bool:
    """True if the local dev-fabric broker responds to /health."""
    req = urllib.request.Request(f"{_broker_url()}/health")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status == 200
    except (urllib.error.URLError, OSError):
        return False


def status(timeout: float = 5.0) -> dict | None:
    """Authenticated broker /status snapshot, or None if unreachable/unauthorized."""
    req = urllib.request.Request(f"{_broker_url()}/status")
    token = _token()
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            import json

            return json.loads(resp.read())
    except (urllib.error.URLError, OSError):
        return None
