# Multi-Model Support for Colibri-Bonfyre

Complete guide for preparing and running inference on multiple MoE architectures beyond GLM-5.2.

## Supported Architectures

| Architecture | Params | Experts/Layer | Top-K | Context | Status |
|--------------|--------|---------------|-------|---------|--------|
| **Mixtral 8x7B** | 47B | 8 | 2 | 32K | ✅ Ready |
| **Mixtral 8x22B** | 141B | 8 | 2 | 64K | ✅ Ready |
| **Qwen2-MoE 2.7B** | 2.7B | 60 | 4 | 32K | ✅ Ready |
| **Qwen2-MoE 57B** | 57B | 64 | 8 | 32K | ✅ Ready |
| **DeepSeek-V2** | 236B | 160 | 6 | 164K | ✅ Ready |
| **DeepSeek-V3** | 671B | 256 | 8 | 164K | ✅ Ready |
| **GLM-5.2** | 744B | 256 | 2 | 8K | ✅ Ready |

## Quick Start

### Option 1: List Available Architectures

```bash
./cmd/BonfyreMoE/bonfyre-moe list-archs
```

Output:
```
Known MoE Architectures:

mixtral-8x7b          47.0 B params, 32 layers, 32x8 experts
                      Aliases: mixtral, mixtral-8x7, Mixtral-8x7B-v0.1

qwen-moe-2.7b         2.7 B params, 24 layers, 24x60 experts
                      Aliases: qwen-moe, Qwen1.5-MoE-A2.7B, qwen2-moe

deepseek-v3           671.0 B params, 61 layers, 61x256 experts
                      Aliases: DeepSeek-V3, deepseek-671b
...
```

### Option 2: Prepare a Model from HuggingFace

#### A. Single Model
```bash
# Mixtral 8x7B (~90GB download + conversion)
./scripts/prepare_moe_model.py mistralai/Mixtral-8x7B-v0.1 ./models/mixtral-8x7b/

# Qwen2-MoE 2.7B (~20GB download + conversion)
./scripts/prepare_moe_model.py Qwen/Qwen1.5-MoE-A2.7B ./models/qwen-moe-2.7b/

# DeepSeek-V3 (~1.5TB download + conversion, requires significant resources)
./scripts/prepare_moe_model.py deepseek-ai/DeepSeek-V3 ./models/deepseek-v3/
```

#### B. Batch Preparation
```bash
# Prepare all small models (< 50GB)
./scripts/batch_prepare_moe.sh --quick

# Prepare specific model family
./scripts/batch_prepare_moe.sh mixtral

# Prepare all models (requires ~2TB storage)
./scripts/batch_prepare_moe.sh
```

## Model Preparation Details

### What the Scripts Do

1. **Download** — Fetches model from HuggingFace (requires `huggingface_hub`)
2. **Extract** — Separates weights into components:
   - `embeddings.bin` — Token embeddings (float32)
   - `attention_dense.bin` — Q/K/V/O projections (float32)
   - `experts_L{layer}_E{expert}.bin` — Per-expert FFN weights (int4 quantized)
   - `output_proj.bin` — LM head (float32)
3. **Quantize** — Experts quantized to int4 by default (use `--no-quantize` to skip)
4. **Configure** — Generates `model_config.json` with architecture metadata
5. **Tokenizer** — Copies tokenizer files if available

### Storage Requirements

| Model | Download | Converted | Total |
|-------|----------|-----------|-------|
| Qwen2-MoE 2.7B | 10GB | 8GB | ~20GB |
| Mixtral 8x7B | 50GB | 40GB | ~90GB |
| Qwen2-MoE 57B | 120GB | 110GB | ~240GB |
| Mixtral 8x22B | 150GB | 130GB | ~280GB |
| DeepSeek-V2 | 250GB | 240GB | ~500GB |
| DeepSeek-V3 | 750GB | 720GB | ~1.5TB |
| GLM-5.2 | 800GB | 750GB | ~1.6TB |

### Dependencies

```bash
# Python packages
pip install torch transformers safetensors huggingface_hub

# Optional: BonfyreFPQ for advanced compression
make -C cmd/BonfyreFPQ
```

## Running Inference

### 1. Validate Model

```bash
./cmd/BonfyreMoE/bonfyre-moe doctor \
  --model ./models/mixtral-8x7b/ \
  --arch mixtral
```

Output:
```
[cbf_doctor] Running model validation checks...

1. Model files
  ✓ embeddings.bin (512 MB)
  ✓ All 256 expert files found

2. Memory budgets
  Model footprint:
    Embeddings: 512 MB
    Attention: 2048 MB
    All experts: 38400 MB
    Total (dense + all experts): 40960 MB
  Configured budgets:
    VRAM: 24000 MB
    RAM: 64000 MB
    Disk: 0 MB
    Network: 0 MB
  ✓ Dense components fit in memory

3. Network configuration
  • Network tier disabled

4. Fragment store
  • Fragment store disabled (router cache uses temp file)

✓ All checks passed - model ready for inference
```

### 2. Interactive Chat

```bash
./cmd/BonfyreMoE/bonfyre-moe chat \
  --model ./models/mixtral-8x7b/ \
  --arch mixtral \
  --vram 24000 \
  --ram 64000
```

Output:
```
BonfyreMoE Interactive Chat
===========================
Model: ./models/mixtral-8x7b/
Architecture: mixtral-8x7b (47.0B params, 32 layers, 8 experts/layer)

Memory configuration:
  VRAM: 24000 MB
  RAM: 64000 MB
  Disk: 0 MB
  Network: 0 MB

Estimated performance:
  Cold start: 1.5-2.5 tok/s
  Warm (80% cache hit): 5.8-6.8 tok/s

Enter your messages (type 'quit' to exit, 'save' to persist cache):

You: What are mixture of experts models?
Assistant: Mixture of Experts (MoE) models are a type of neural network architecture...
```

### 3. API Server

```bash
./cmd/BonfyreMoE/bonfyre-moe serve \
  --model ./models/qwen-moe-2.7b/ \
  --arch qwen-moe-2.7b \
  --port 8080 \
  --vram 12000 \
  --ram 32000
```

OpenAI-compatible endpoints:
- `POST /v1/chat/completions`
- `POST /v1/completions`
- `GET /health`

### 4. Expert Server (Distributed Mode)

```bash
# Node 1: Start expert server
./cmd/BonfyreMoE/bonfyre-moe peer \
  --model ./models/deepseek-v3/ \
  --arch deepseek-v3 \
  --port 9090 \
  --vram 48000 \
  --ram 128000

# Node 2: Connect and run inference
./cmd/BonfyreMoE/bonfyre-moe chat \
  --model ./models/deepseek-v3/ \
  --arch deepseek-v3 \
  --peer node1.local:9090 \
  --vram 24000 \
  --ram 64000 \
  --network 10000  # 10GB network tier budget
```

## Architecture Auto-Detection

The system can auto-detect architectures in three ways:

### 1. From `model_config.json`
```json
{
  "architecture": "mixtral-8x7b",
  "config": {
    "n_vocab": 32000,
    "d_model": 4096,
    ...
  }
}
```

### 2. From Directory Name
```bash
./models/mixtral-8x7b/  # Auto-detects as "mixtral-8x7b"
./models/Mixtral-8x7B-v0.1/  # Also works with HF naming
```

### 3. Manual Override
```bash
--arch mixtral  # Use any alias from cbf_arch_list_all()
```

## Advanced: Custom Architectures

### Define New Architecture

Add to `lib/libcolibri-bonfyre/src/cbf_architectures.c`:

```c
{
    .name = "my-custom-moe",
    .aliases = {"custom", NULL},
    .shape = {
        .n_vocab = 50000,
        .d_model = 2048,
        .n_layers = 24,
        .n_heads = 16,
        .n_kv_heads = 16,
        .head_dim = 128,
        .d_ffn = 8192,
        .n_experts_per_layer = 32,
        .n_experts_active = 4,
        .max_seq_len = 4096,
        .rope_theta = 10000.0f,
        .expert_size_mb = 50,
    }
},
```

### Or Create `model_config.json` Manually

```bash
cd ./models/my-custom-model/
cat > model_config.json <<EOF
{
  "architecture": "custom-moe",
  "config": {
    "n_vocab": 50000,
    "d_model": 2048,
    "n_layers": 24,
    "n_heads": 16,
    "n_kv_heads": 16,
    "head_dim": 128,
    "d_ffn": 8192,
    "n_experts_per_layer": 32,
    "n_experts_active": 4,
    "max_seq_len": 4096,
    "rope_theta": 10000.0,
    "expert_size_mb": 50
  }
}
EOF
```

## Integration with Existing Tooling

### BonfyreFPQ Compression

```bash
# After preparing model, compress experts further
cd ./models/mixtral-8x7b/

for expert in experts_L*_E*.bin; do
    ../../../cmd/BonfyreFPQ/bonfyre-fpq compress \
        "$expert" "${expert%.bin}.fpq" --bits 3
done
```

### GGUF Export (for llama.cpp compatibility)

```bash
# Use existing BonfyreFPQ GGUF export
./cmd/BonfyreFPQ/bonfyre-fpq export-gguf \
    ./models/mixtral-8x7b/ \
    ./models/mixtral-8x7b.gguf
```

### Ollama Integration

```bash
# Export to GGUF first, then create Ollama model
./cmd/BonfyreFPQ/bonfyre-fpq export-gguf \
    ./models/qwen-moe-2.7b/ \
    ./qwen-moe-2.7b.gguf

# Create Modelfile
cat > Modelfile <<EOF
FROM ./qwen-moe-2.7b.gguf
EOF

# Import to Ollama
ollama create bonfyre-qwen-moe -f Modelfile
ollama run bonfyre-qwen-moe
```

## Performance Tuning

### Memory Tier Configuration

```bash
# VRAM-heavy (RTX 4090 24GB)
--vram 22000 --ram 32000 --disk 0 --network 0

# RAM-heavy (M3 Max 128GB unified memory)
--vram 0 --ram 120000 --disk 0 --network 0

# Disk streaming (Colibri baseline)
--vram 0 --ram 4000 --disk 50000 --network 0

# Distributed (3-node fleet)
--vram 24000 --ram 64000 --disk 0 --network 20000 \
  --peer node1:9090 --peer node2:9090 --peer node3:9090
```

### Router Cache Optimization

```bash
# Save router cache after first run
# (automatically done in chat mode with 'save' command)

# Reuse cache for same workload
./cmd/BonfyreMoE/bonfyre-moe chat \
  --model ./models/mixtral-8x7b/ \
  --workload coding  # Loads coding-specific router patterns
```

### Expert Rebalancing

```bash
# Analyze expert usage and replicate hot experts across fleet
./cmd/BonfyreMoE/bonfyre-moe rebalance \
  --model ./models/deepseek-v3/ \
  --workload coding \
  --peers node1:9090,node2:9090,node3:9090
```

## Troubleshooting

### "Architecture not found"
- Use `./cmd/BonfyreMoE/bonfyre-moe list-archs` to see available names
- Check that `model_config.json` exists and is valid
- Use `--arch` flag to manually specify architecture

### "Expert file not found"
- Verify all `experts_L*_E*.bin` files exist
- Re-run `prepare_moe_model.py` if preparation was interrupted
- Check storage space (may have run out mid-conversion)

### Out of Memory
- Reduce VRAM/RAM budgets
- Enable disk/network tiers
- Use smaller model or distributed mode

### Slow Inference
- Check router cache hit rate (should be >70%)
- Increase VRAM/RAM budgets
- Run `rebalance` to optimize expert placement
- Enable router lookahead (enabled by default)

## Examples

### Example 1: Mixtral 8x7B on RTX 4090 24GB

```bash
# Prepare
./scripts/prepare_moe_model.py mistralai/Mixtral-8x7B-v0.1 ./models/mixtral/

# Validate
./cmd/BonfyreMoE/bonfyre-moe doctor --model ./models/mixtral/

# Run
./cmd/BonfyreMoE/bonfyre-moe chat \
  --model ./models/mixtral/ \
  --vram 22000 \
  --ram 64000
```

Expected performance: 5-7 tok/s warm, 2-3 tok/s cold

### Example 2: Qwen2-MoE 2.7B on MacBook Pro M3

```bash
# Prepare
./scripts/prepare_moe_model.py Qwen/Qwen1.5-MoE-A2.7B ./models/qwen-moe/

# Run (unified memory)
./cmd/BonfyreMoE/bonfyre-moe chat \
  --model ./models/qwen-moe/ \
  --vram 0 \
  --ram 32000
```

Expected performance: 8-12 tok/s warm, 4-6 tok/s cold

### Example 3: DeepSeek-V3 Distributed (3 nodes)

```bash
# Node 1 (48GB VRAM + 256GB RAM)
./cmd/BonfyreMoE/bonfyre-moe peer \
  --model ./models/deepseek-v3/ \
  --port 9090 \
  --vram 46000 \
  --ram 240000

# Node 2 (24GB VRAM + 128GB RAM)
./cmd/BonfyreMoE/bonfyre-moe peer \
  --model ./models/deepseek-v3/ \
  --port 9090 \
  --vram 22000 \
  --ram 120000

# Node 3 (Client + inference)
./cmd/BonfyreMoE/bonfyre-moe chat \
  --model ./models/deepseek-v3/ \
  --vram 22000 \
  --ram 120000 \
  --network 20000 \
  --peer node1.local:9090 \
  --peer node2.local:9090
```

Expected performance: 2-4 tok/s distributed

## Next Steps

1. **Benchmark**: Use `bonfyre-infer compare` to compare architectures
2. **Optimize**: Run `rebalance` after initial workload to improve cache hit rates
3. **Scale**: Add more nodes in peer mode for larger models
4. **Experiment**: Try different memory configurations for your hardware

## References

- [COLIBRI_INTEGRATION.md](./COLIBRI_INTEGRATION.md) — Full technical architecture
- [COLIBRI_QUICKREF.md](./COLIBRI_QUICKREF.md) — Configuration reference
- [COLIBRI_IMPLEMENTATION_SUMMARY.md](./COLIBRI_IMPLEMENTATION_SUMMARY.md) — Implementation details
- [BonfyreFPQ docs](../10-Code/BonfyreFPQ/INFERENCE_ARCHITECTURE.md) — Weight compression
- [Bonfyre repo conventions](../AGENTS.md) — Project structure
