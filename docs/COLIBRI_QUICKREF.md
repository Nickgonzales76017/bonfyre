# Colibri-Bonfyre Quick Reference

## What Changed

Bonfyre can now run massive MoE models (744B GLM-5.2, 671B DeepSeek-V3) with expert streaming across memory tiers and distributed nodes.

## New Components

### Libraries
- **`lib/libcolibri-bonfyre/`** — Expert-streaming engine with QUIC + fragment integration

### Binaries
- **`cmd/BonfyreMoE/`** — Distributed MoE inference (plan/chat/serve/peer/rebalance)
- Enhanced **`cmd/BonfyreInfer/`** — Now handles MoE architecture planning

## Quick Start

### 1. Plan Expert Placement
```bash
bonfyre-moe plan --model /nvme/glm52 --vram 80 --ram 128

# Output shows: experts_vram, experts_ram, experts_disk, tok/s estimates
```

### 2. Run Distributed Inference
```bash
# Node 1: Main inference
bonfyre-moe chat --model /nvme/glm52 --workload code_review --peers node-2:7000,node-3:7000

# Node 2, 3: Expert servers
bonfyre-moe peer --model /nvme/glm52 --port 7000
```

### 3. Compare Architectures
```bash
bonfyre-infer compare --arch qwen2 --arch glm52

# Shows: total params, active params/token, sparsity %
```

## Memory Tiers

| Tier | Latency | Capacity | Use Case |
|------|---------|----------|----------|
| VRAM | 0.1ms | 24-80GB | Hot experts (high routing frequency) |
| RAM | 1ms | 128-512GB | Warm experts (medium frequency) |
| Disk | 10ms | 1-4TB | Cold experts (low frequency) |
| Network | 20-50ms | Fleet capacity | Distributed hot expert pool |

## Router Learning Cache

**Storage**: Fragment-based, perspective-aware  
**Heat metric**: Exponential moving average (α=0.1)  
**Pin threshold**: heat > 0.5 → promote to faster tier  
**Evict threshold**: heat < 0.1 → demote to slower tier  
**Convergence**: 50-100 turns typical

```bash
# Load learned patterns for a workload
bonfyre-moe chat --model /nvme/glm52 --workload sql_optimization

# Export atlas visualization
bonfyre-moe export-atlas --model /nvme/glm52 --out atlas.json
```

## Expert Replication

```bash
# Analyze cache and replicate hot experts
bonfyre-moe rebalance --model /nvme/glm52 --peers node-2:7000,node-3:7000

# Manual replication (advanced)
# Use libcolibri-bonfyre API: cbf_expert_replicate(engine, layer, expert, "node-2")
```

## Configuration Knobs

### Memory Budget
```bash
--vram <GB>           # GPU memory (0 = auto-detect)
--ram <GB>            # System RAM (0 = auto-detect)
--disk <path>         # Expert storage directory
```

### Pinning Policy
```bash
--pin-experts <N>     # Pin N hot experts (0 = auto)
--pin-threshold <F>   # Heat threshold for pinning (0.0-1.0)
```

### Performance
```bash
--io-threads <N>      # Async I/O pool size (default: 4)
--prefetch <N>        # Router lookahead layers (default: 1)
--numa                # NUMA-aware weight placement
--no-spec             # Disable speculative decoding
```

### Network
```bash
--peers <list>        # host1:port1,host2:port2,...
--port <port>         # QUIC listen port (default: 7000)
--no-network          # Disable network tier
```

## Integration Points

### Fragment System
- Router cache entries: `kind="expert_routing_heat"`
- Perspective tag: `perspective="workload:<tag>"`
- Confidence = routing heat (0.0-1.0)
- Query hot experts: `bf_fragment_query(store, kind, perspective, min_confidence=0.5)`

### QUIC Transport
- Expert streams: `family_key = FNV-1a-64(layer, expert)`
- Priority: `layer_idx % 4` (maps to BF_LAYER_*)
- Transfer: header (64 bytes) + int4 blob (~19MB for GLM-5.2)

### Lambda-Tensors
- KV cache compression: 57× smaller than dense
- Persistent across sessions: `cbf_kv_cache_save()` / `load()`
- Byte-identical on restore (zero re-prefill)

## Performance Estimates

| Hardware | Experts Resident | Cold tok/s | Warm tok/s |
|----------|------------------|------------|------------|
| 6× RTX 5090 | All (VRAM) | 5.8 | 6.8 |
| 128GB desktop | 5K (RAM) | 1.5 | 2.5 |
| 25GB laptop | 0 (disk) | 0.05 | 0.1 |
| 3-node fleet | 15K (distributed) | 2.0 | 4.0 |

**Cold** = no experts cached (worst case)  
**Warm** = learning cache converged (typical after 50-100 turns)

## Common Workflows

### Development
```bash
# Plan + validate before loading
bonfyre-moe plan --model /nvme/glm52 --vram 40
bonfyre-moe doctor --model /nvme/glm52

# Interactive testing
bonfyre-moe chat --model /nvme/glm52 --workload test
```

### Production
```bash
# API server with fleet
bonfyre-moe serve --model /nvme/glm52 --port 8080 \
  --peers node-2:7000,node-3:7000 \
  --workload production \
  --io-threads 8 --prefetch 2

# Periodic rebalancing (cron)
bonfyre-moe rebalance --model /nvme/glm52 --peers <list>
```

### Research
```bash
# Compare model architectures
bonfyre-infer compare --arch glm52 --arch deepseek-v3

# Export expert routing patterns
bonfyre-moe export-atlas --model /nvme/glm52 --out atlas.json

# Analyze workload differences
# Use fragment diff API: bf_fragment_diff(store, kind, "workload:A", "workload:B")
```

## Troubleshooting

### Low throughput
1. Check cache temperature: `bonfyre-moe metrics --model /nvme/glm52`
2. Increase pinned experts: `--pin-experts 1000`
3. Add more RAM: `--ram 256`
4. Reduce network hops: co-locate hot experts

### High latency
1. Disable prefetch if causing thrash: `--prefetch 0`
2. Reduce I/O threads if CPU-bound: `--io-threads 2`
3. Pin more experts: `--pin-threshold 0.3` (lower = more aggressive)

### Network issues
1. Check peer connectivity: `bonfyre-moe doctor --model <path>`
2. Verify QUIC port open: `nc -zv <peer> 7000`
3. Measure latency: `ping <peer>`
4. Fallback to local-only: `--no-network`

## See Also

- **Architecture docs**: `docs/COLIBRI_INTEGRATION.md`
- **Fragment system**: `lib/libfragment/include/fragment.h`
- **QUIC transport**: `lib/libquic-transport/include/bf_quic.h`
- **Lambda-tensors**: `lib/liblambda-tensors/include/lambda_tensors.h`
- **Colibri upstream**: https://github.com/JustVugg/colibri
