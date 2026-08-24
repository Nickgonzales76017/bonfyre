"""Execute a command hop over a chosen transport phenotype.

The essay's transport point: the same semantic operation runs over different
carriers, and the carrier is not the meaning. A hop can execute by direct
subprocess or through the MCP server -- same binary, same result -- but the MCP
path returns a signed receipt (receipt_id, stdout_sha256, status) the raw call
does not. Provenance is a property of the transport, not the operation.

This makes transport selectable and proves the two agree: a BonfyreHash of the
same file returns the same digest whether run directly or through the MCP
server's own native_tool_invoke, which this imports and calls so the test
exercises the real MCP execution path rather than a mock.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

MCP_SERVER = Path.home() / ".bonfyre" / "mcp" / "project_node.py"

DIRECT = "direct"
MCP = "mcp"


@dataclass(frozen=True)
class TransportResult:
    transport: str
    exit_code: int
    stdout: str
    receipt_id: Optional[str]      # only the MCP carrier provides one
    stdout_sha256: Optional[str]


def _run_direct(binary: str, args: list[str], timeout: int) -> TransportResult:
    done = subprocess.run([binary, *args], capture_output=True, text=True, timeout=timeout)
    return TransportResult(DIRECT, done.returncode, done.stdout, None, None)


_MCP_MODULE = None


def _mcp_module():
    """Import the running MCP server so the test hits its real execution path."""
    global _MCP_MODULE
    if _MCP_MODULE is None:
        if not MCP_SERVER.exists():
            raise RuntimeError("MCP server not present")
        spec = importlib.util.spec_from_file_location("bf_mcp", str(MCP_SERVER))
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except SystemExit:
            pass
        _MCP_MODULE = module
    return _MCP_MODULE


def _run_mcp(family: str, args: list[str], timeout: int) -> TransportResult:
    module = _mcp_module()
    result = module.native_tool_invoke({"family": family, "args": args,
                                        "timeout_seconds": timeout})
    return TransportResult(
        MCP, result.get("exit_code", -1), result.get("stdout", ""),
        result.get("receipt_id"), result.get("stdout_sha256"),
    )


def run_hop(
    *, family: str, binary: str, args: list[str],
    transport: str = DIRECT, timeout: int = 30,
) -> TransportResult:
    """Run one hop over the chosen carrier. Same operation, different phenotype."""
    if transport == MCP:
        return _run_mcp(family, args, timeout)
    return _run_direct(binary, args, timeout)


def mcp_available() -> bool:
    return MCP_SERVER.exists()
