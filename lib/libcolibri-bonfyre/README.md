# Colibri Expert-Streaming in Bonfyre

This directory contains Bonfyre's integration of [Colibri's](https://github.com/JustVugg/colibri) expert-streaming architecture, enabling massive MoE model inference (744B GLM-5.2, 671B DeepSeek-V3) with distributed memory tiers.

## What's Here

```
lib/libcolibri-bonfyre/       Core expert-streaming engine
├── include/
│   └── colibri_bonfyre.h     API: engine, placement, metrics, distributed
├── src/
│   ├── cbf_engine.c          Main engine (I/O pool, lookahead, LRU caches)
│   ├── cbf_quic_expert.c     QUIC-based expert push/pull/rebalance
│   └── cbf_fragment_cache.c  Fragment-based router learning cache
└── Makefile

cmd/BonfyreMoE/               Distributed MoE inference binary
├── src/main.c                Modes: plan/doctor/chat/serve/peer/rebalance
└── Makefile

cmd/BonfyreInfer/             Enhanced architecture planning
├── src/
│   ├── bonfyre_infer.h       Model shape + graph plan API
│   ├── graph_plan.c          Arch builders (qwen2, glm52, deepseek-v3)
│   └── main.c                CLI: plan, compare
└── Makefile

docs/
├── COLIBRI_INTEGRATION.md    Full architecture & design doc
└── COLIBRI_QUICKREF.md       Command reference & troubleshooting
```

## Quick Start

### Build
```bash
# Library
cd lib/libcolibri-bonfyre && make

# Binaries
cd cmd/BonfyreMoE && make
cd cmd/BonfyreInfer && make
```

### Run
```bash
# Plan expert placement (read-only, fast)
bonfyre-moe plan --model /nvme/glm52 --vram 80 --ram 128

# Validate readiness
bonfyre-moe doctor --model /nvme/glm52

# Interactive chat with workload-specific cache
bonfyre-moe chat --model /nvme/glm52 --workload code_review

# Distributed inference with 3-node fleet
bonfyre-moe serve --model /nvme/glm52 --port 8080 \
  --peers node-2:7000,node-3:7000
```

## Key Capabilities

### Expert Streaming (from Colibri)
- Dense components (~9.9GB) stay resident
- 19,456 experts (19MB each) stream on-demand
- Per-layer LRU cache + async I/O pool
- Router lookahead prefetch (71.6% predictable)
- Learning cache (pins hot experts, gets faster over time)

### Distributed Extensions (Bonfyre)
- **Network tier**: QUIC-stream experts from fleet peers
- **Fragment-based cache**: Router heat as perspectives + confidence
- **Latency-aware pinning**: Replicate hot experts to low-latency nodes
- **Lambda-tensor KV**: 57× compressed persistent state
- **Swarm inference**: Batch positions across hosts (TODO)

## Memory Tiers

| Tier | Latency | Use Case |
|------|---------|----------|
| VRAM | 0.1ms | Hot experts (high frequency) |
| RAM | 1ms | Warm experts (medium frequency) |
| Disk | 10ms | Cold experts (low frequency) |
| Network | 20-50ms | Distributed hot pool |

**Placement affects speed only, never precision or routing semantics.**

## Performance

| Hardware | Experts Resident | Cold tok/s | Warm tok/s |
|----------|------------------|------------|------------|
| 6× RTX 5090 | All (VRAM) | 5.8 | 6.8 |
| 128GB desktop | 5K (RAM) | 1.5 | 2.5 |
| 25GB laptop | 0 (disk) | 0.05 | 0.1 |
| 3-node fleet | 15K (distributed) | 2.0 | 4.0 |

**Based on Colibri benchmarks + Bonfyre network tier estimates.**

## Integration Points

- **Fragment System**: Router cache stored as `kind="expert_routing_heat"` fragments
- **QUIC Transport**: Layer-prioritized expert streams, 0-RTT reconnect
- **Lambda-Tensors**: KV cache compressed 57×, byte-identical restore
- **Layer Artifacts**: MoE topology awareness (`T_MOE_ROUTER`, `T_MOE_EXPERT`)

## Documentation

- **[Full Architecture](docs/COLIBRI_INTEGRATION.md)**: Design, API reference, roadmap
- **[Quick Reference](docs/COLIBRI_QUICKREF.md)**: Commands, configuration, troubleshooting

## Status

**Phase**: Implementation (prototype)  
**Core engine**: Structured, stubbed inference math (TODO: complete forward pass)  
**QUIC streaming**: Protocol defined, integration pending  
**Fragment cache**: API complete, update/load/export implemented  
**Binaries**: CLI complete, modes stubbed (TODO: wire up)

## Next Steps

1. Complete `cbf_forward()` inference math
2. Wire up QUIC expert streaming
3. Add tokenizer for chat mode
4. Implement API server (OpenAI-compatible)
5. CUDA/Metal backend ports
6. Distributed speculative decoding

## License

Apache 2.0 (matches Colibri and Bonfyre)

## References

- **Colibri upstream**: https://github.com/JustVugg/colibri
- **GLM-5.2 model**: https://huggingface.co/mateogrgic/GLM-5.2-colibri-int4-with-int8-mtp
- **Bonfyre repo**: (current workspace)
