# Bonfyre estate generation fabric

The estate fabric is the shared boundary for native model and media tooling.
It discovers executable `bonfyre-*` binaries under `bin/` and `cmd/Bonfyre*/`,
invokes them with bounded time/output limits, and emits hashed receipts that
can be consumed by ledger, analytics, CMS, WordPress, and agent workflows.

Generation profiles are configuration-driven so the estate is not coupled to
the Qwen 0.5B profile. Register any compatible native generation backend with
`BONFYRE_MODEL_PROFILES` as a JSON array:

```json
[
  {
    "id": "coder-small",
    "tool_id": "bonfyre-qwen-fpq",
    "binary": "/path/to/bonfyre-qwen-fpq",
    "modelPack": "/models/coder.fpq-pack.json",
    "tokenizer": "/models/coder/tokenizer.json"
  }
]
```

The MCP surface exposes inventory, profile discovery, bounded native invocation,
single generation, and bounded batch generation. A new MCP connection is
required after changing server tool schemas because MCP tool lists are fixed at
connection time.
