# BonfyreFPQ — Status Report (April 13, 2026)

## What We've Built

BonfyreFPQ is a C-native weight compression codec that encodes neural network weights using E8 lattice quantization, FWHT spectral transforms, and rANS entropy coding into a native `.fpq` file format. It's real, it compresses, and the math works.

## What Works Today

### Compression Pipeline (Solid)
- **`convert-fpq`**: Reads any safetensors model → writes `.fpq` v12 native format
- **`decode`**: Reads `.fpq` → writes BF16 safetensors (lossless roundtrip to quantization fidelity)
- Tested on 5 models across 3 architectures (LLM, diffusion, speech):

| Model | Params | FP32 Size | .fpq v12 | Ratio | bpw | Avg Cosine |
|-------|--------|-----------|----------|-------|-----|------------|
| Qwen 2.5 3B | 3.09B | 6.17 GB | 3.1 GB | 2.0× | 8.43 | 0.9997 |
| Wan 2.1 T2V 1.3B | 1.42B | 5.3 GB | 1.3 GB | 4.2× | 7.53 | 0.9999 |
| Whisper v3-turbo | 809M | 3.0 GB | 684 MB | 4.5× | 7.09 | — |
| SmolLM2 1.7B | 1.71B | 6.4 GB | 1.9 GB | 3.5× | 9.05 | — |
| TinyLlama 1.1B | 1.10B | 4.1 GB | 1.1 GB | 3.8× | 8.43 | — |

### Inference Validation (Proven)
- **Qwen 2.5 3B**: Decoded v12 → loaded in transformers → logit cosine **0.9896**, top-1 agreement **100%**
- **Wan 2.1 T2V**: Decoded v12 → loaded in diffusers WanPipeline → **generated 480×832 video, 17 frames, 30 denoising steps** — prompt-accurate, high quality
- Perplexity (Qwen 0.5B, WikiText-2): baseline 11.95 → v8 **12.07** (+0.9% — negligible)

### SLI — Spectral Lattice Inference (Kernel Works)
- Computes `y = Wx` without decompressing weights — first quant method to do this
- 8× bandwidth reduction (64 bytes per 256-element block vs 512 BF16)
- Cosine **1.0000000000** vs dense decode path
- Validated on real Qwen 3B tensors via `sli-bench`

### HuggingFace Models (30 repos on NICKO/)
- 5 new v12 native repos uploaded today
- 25 prior repos (algebra-compressed, v9, various models)

## What Doesn't Work Yet

### The Honest Gap: .fpq → Inference Is Not Direct

The current path to inference is:

```
.fpq file → decode to safetensors → load in PyTorch/diffusers → normal inference
```

This works, but it's a **decode-then-infer** workflow. The `.fpq` file is essentially a compressed archive that you unpack before use. The user gets smaller storage/transfer, but **no inference speedup**.

The SLI kernel (`fpqx_sli_matvec`) proves the math for computing directly on compressed weights, but it's a standalone C function tested via `sli-bench`. It is **not wired into**:
- A PyTorch custom op
- A transformer forward pass
- Any end-to-end inference runtime
- Any serving framework (vLLM, TGI, llama.cpp)

### What's Needed for Production

1. **PyTorch SLI Op** — Wrap `fpqx_sli_matvec` as a `torch.autograd.Function` that replaces `nn.Linear.forward()`. This is the critical bridge.

2. **Model loader** — Read `.fpq` directly into SLI-backed tensors without decompressing. Currently `fpqx_read_model()` does full decompress to fp32.

3. **Integration with a serving framework** — Either a llama.cpp backend, or a vLLM quantization config, or at minimum a standalone inference script that loads `.fpq` and runs generation without ever materializing full-precision weights.

4. **Key remapping** — The Wan video demo required manual key remapping (original names → diffusers names) and shape reshaping (1D modulation → 3D scale_shift_table, 2D patch_embedding → 5D conv). This needs to be automated or the `.fpq` format needs to store original metadata.

5. **Multi-format support** — Currently only reads safetensors. GGUF support exists for reading but not for `.fpq` output.

## Architecture Versions

| Version | Core Innovation | Quality (cos) | bpw |
|---------|----------------|---------------|-----|
| v3 | FWHT + Lloyd-Max | 0.984 | 3.50 |
| v4 | Chaos codebook + Ghost head | 0.985 | 3.57 |
| v7 | E8 lattice + μ-law + trellis | 0.996 | 4.19 |
| v8 | E8 + 16D RVQ + Viterbi | 0.9999 | 5.06 |
| v9 | + truncated SVD + speed opts | 0.9999 | 4.21 |
| v12 | + rANS entropy + native format | 0.9997 | 7.53–9.05 |

## The Conversion Pipeline

The pipeline is efficient. A single `convert-fpq` process handles any safetensors model:
- Reads tensor-by-tensor (streaming, not all at once)
- Encodes each 2D weight through: FWHT → E8 lattice snap → μ-law warp → RVQ tile → ghost correction → rANS entropy
- Passes through 1D tensors (biases, norms) as-is in BF16
- Writes native `.fpq` with full metadata

Conversion speed is ~1 GB/min on a single CPU core. A $0.17/hr RunPod A4000 converted 5 models in under an hour.

## Repo/Data Locations

- **Code**: https://github.com/Nickgonzales76017/bonfyre (MIT)
- **Models**: https://huggingface.co/NICKO (30 repos)
- **Binary**: `bonfyre-fpq` (convert-fpq, decode), `bonfyre-fpqx` (sli-bench, algebra-compress)

## Next Steps

1. Kill RunPod pod after confirming all data is on HF/GitHub
2. Set up cheap persistent server for batch conversions (Hetzner/OVH, ~$50/mo)
3. Build the PyTorch SLI bridge (the real production gap)
4. Convert top-50 HuggingFace models to v12 as a library
5. Write proper documentation and usage examples
