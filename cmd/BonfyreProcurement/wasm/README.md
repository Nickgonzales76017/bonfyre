# Procurement browser kernel

This directory exposes the same pure C procurement derivations used by
`bonfyre-procurement` to a WebAssembly host.

The module owns no persistence or external effect. The browser writes a bounded
JSON edge representation into exported memory, calls one of the three pure
operations, copies the result out, and saves that result through the existing
native Workspace record owner.

Operation numbers:

- `1` — prepare supplier request
- `2` — retain/validate supplier quote observation
- `3` — compare quote observations

The wrapper deliberately exports no supplier-send, approval, order, payment,
settlement, or shipment operation.
