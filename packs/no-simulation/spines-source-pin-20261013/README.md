# Spines source pin repair

The shared SHA implementation and header changed while Spines retained their old source hashes. Its source verifier refused to run. `previous.lock.json` preserves the earlier lock; `source-pin-changes.json` records both updates.

`rebuild.json` records a fresh Zig 0.13.0 build against current sources. Its 30,454-byte artifact is byte-identical to the pinned reducer. The normal build and verification command succeeds with the corrected source pins. No reducer artifact or running service was changed.

`mcp-tests.log` records all 505 MCP tests passing with 6,788 assertions. A separate isolated depth probe also passed valid terms up to 12,000 nested negations and a follow-up reduction on each instance; it found no failure requiring a parser change.
