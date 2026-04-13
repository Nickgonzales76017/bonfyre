# BonfyreFPQ: Inference Architecture

## What the data says (11 models, 8 domains, April 2026)

### The headline: E8 quantization is architecture-agnostic

| Domain | Models | Avg Cos | Worst Cos | Ratio |
|--------|--------|---------|-----------|-------|
| LLM (Qwen/TinyLlama/SmolLM2) | 3 | 0.999826 | 0.999588 | 2.0–3.7× |
| Video diffusion (Wan 2.1) | 1 | 0.999874 | 0.999590 | 4.1× |
| Speech ASR (Whisper/Wav2Vec2) | 2 | 0.999851 | 0.998678 | 4.0–4.5× |
| Vision transformer (ViT) | 1 | 0.999733 | 0.998490 | 4.2× |
| CNN (ResNet-50) | 1 | 0.999658 | 0.998306 | 4.0× |
| Segmentation (SAM) | 1 | 0.999497 | 0.992606 | 4.3× |
| Depth estimation (DPT) | 1 | 0.999742 | 0.998460 | 4.0× |
| Multimodal (CLIP) | 1 | 0.999876 | 0.998477 | 3.5× |

### Five takeaways

1. **Quality is not domain-specific.** Same binary, same flags, same code path — 0.999+ avg cosine on everything from 25M-param CNNs to 3B-param LLMs. No per-architecture tuning. The E8 lattice works because it optimizes geometry, not statistics.

2. **Larger models compress better.** Models ≥500M params: worst-case floor 0.9996. Models <500M params: worst-case floor 0.9926. Larger weight matrices have more redundancy for the low-rank SVD to capture and more blocks for rANS entropy to exploit. The codec gets *more* effective as models scale up.

3. **The only quality issue is tiny positional tables.** SAM's 8 bad tensors are all `rel_pos_h/w` at shape [27×64] = 1,728 elements. That's 6.75 blocks — barely enough for the E8 lattice to stabilize. Fix: pass tensors <4096 elements through as raw BF16 (already done for <512). Every other tensor in every model is 0.998+.

4. **Compression ratio is remarkably stable.** Median 4.0× from safetensors (BF16). Range 3.5–4.5×. The outlier is Qwen 3B at 2.0× — but that's because Qwen uses tied embeddings and GQA, so proportionally more of the model is in "hard to compress" embedding tables. Still 0.9998 quality.

5. **This is a universal model distribution format.** One format handles text, vision, speech, video, segmentation, depth, and multimodal. No special modes. No configuration. `convert-fpq model.safetensors model.fpq` just works. This means the inference runtime only needs one loader, one matmul kernel, one decode path.

### What this means for the runtime

The data says we don't need per-domain codecs, per-architecture encoders, or special handling. We need exactly one thing: **a fast way to go from .fpq file → inference output with zero friction.**

---

## The UX: One command

Everything should feel like this:

```bash
# Text generation
fpq run qwen2.5-3b.fpq "What is the capital of France?"

# Speech transcription
fpq run whisper-v3-turbo.fpq audio.wav

# Image classification
fpq run vit-base.fpq photo.jpg

# Pull from HuggingFace + run (like ollama)
fpq pull NICKO/qwen2.5-3b-v12-fpq
fpq run qwen2.5-3b "Explain quantum computing"

# Decode to safetensors (escape hatch for PyTorch/diffusers)
fpq decode model.fpq model.safetensors

# Convert any model
fpq convert model.safetensors model.fpq

# Serve as API
fpq serve qwen2.5-3b.fpq --port 8080
# → POST /v1/chat/completions (OpenAI-compatible)
```

One binary. Auto-detects model type from the .fpq metadata. No config files. No Python. No framework installation.

---

## Architecture: Two layers, not three

Previous plan had 3 tiers and 8 files. That's too many seams. Flatten it.

### Layer 1 — `libfpq` (the C engine)

Single shared library. This is what everything links against.

```c
// === Loading ===
fpq_model_t  *fpq_open(const char *path);           // mmap + build SLI contexts
void          fpq_close(fpq_model_t *m);             // cleanup

// === The only math call that matters ===
void          fpq_matmul(fpq_model_t *m,             // y = W[name] @ x
                         const char *tensor_name,     // via SLI (no decode)
                         const float *x, float *y);

// === Escape hatches ===
void          fpq_decode_tensor(fpq_model_t *m,      // decode one tensor → FP32
                                const char *name,
                                float *out);
void          fpq_decode_all(const char *fpq_path,    // decode entire model → safetensors
                             const char *out_path);

// === Info ===
fpq_info_t    fpq_info(fpq_model_t *m);              // name, tensors, params, version
```

That's 5 functions. `fpq_open`, `fpq_matmul`, `fpq_close` for inference. `fpq_decode_*` for interop. `fpq_info` for tooling.

`fpq_matmul` auto-selects: NEON on ARM, AVX2 on x86, scalar fallback. No configuration.

### Layer 2 — `fpq` (the single binary)

One CLI binary (~300 KB) that handles every use case. Subcommands:

| Command | What it does |
|---------|-------------|
| `fpq run <model> [input]` | Infer: auto-detects modality from model metadata |
| `fpq pull <hf-repo>` | Download .fpq from HuggingFace |
| `fpq convert <in> <out>` | safetensors/GGUF → .fpq |
| `fpq decode <in> <out>` | .fpq → safetensors/GGUF |
| `fpq serve <model> [--port]` | OpenAI-compatible API server |
| `fpq info <model>` | Print model metadata |
| `fpq bench <model>` | SLI benchmark (existing sli-bench) |
| `fpq export-gguf <model>` | Convert .fpq → GGUF for Ollama/llama.cpp |

Model type detection via stored metadata in .fpq header:
- `arch: "llama"` → text generation (transformer decode loop)
- `arch: "whisper"` → speech (encoder-decoder + mel frontend)
- `arch: "vit"` → vision (patch embed + encoder)
- `arch: "clip"` → multimodal (dual encoder)
- `arch: "dit"` → video/image diffusion (denoise loop — requires external scheduler)

The `fpq run` command inspects the arch tag and dispatches to the right runtime. The user never specifies this.

### What lives inside `fpq run`

For text (the primary target):

```
fpq_open("model.fpq")
  → mmap file
  → parse tensor table
  → build SLI contexts for each weight tensor (no decompression)
  → load tokenizer from embedded metadata or sidecar file

for each token:
  embed = lookup(token_id)
  for each layer:
    h = rmsnorm(h)
    q, k, v = fpq_matmul(q_proj, h), fpq_matmul(k_proj, h), fpq_matmul(v_proj, h)
    h += attention(q, k, v, kv_cache)
    h = rmsnorm(h)
    h += fpq_matmul(down_proj, silu(fpq_matmul(gate_proj, h)) * fpq_matmul(up_proj, h))
  logits = fpq_matmul(lm_head, rmsnorm(h))
  next_token = sample(logits, temperature, top_k, top_p)
```

Non-linear ops (rmsnorm, silu, softmax, rope, sampling) are <200 lines of C total. All the heavy compute goes through `fpq_matmul` → SLI kernel.

For speech: mel spectrogram → encoder transformer → decoder transformer → greedy/beam decode.
For vision: patch flatten → encoder transformer → classification head.

Each runtime is ~500 lines of C. Not libraries. Just switch cases inside the `fpq run` dispatcher.

---

## Ollama integration path

Ollama = Go wrapper around llama.cpp. llama.cpp = GGML tensors. Two paths to get there:

### Path A — GGUF export (works today, after decode)

```bash
fpq decode model.fpq model.safetensors
# then use ollama's safetensors import:
ollama create mymodel -f Modelfile
# Modelfile: FROM ./model.safetensors
```

This already works. Decode → safetensors → Ollama import. Adds a step but zero new code needed.

### Path B — Native GGUF export (next step)

```bash
fpq export-gguf qwen2.5-3b.fpq qwen2.5-3b-fpq.gguf
```

Write a GGUF v3 file with the decoded BF16 weights + all metadata (vocab, config, chat template). Then:

```
# Modelfile
FROM ./qwen2.5-3b-fpq.gguf
```

`ollama create bonfyre-qwen3b -f Modelfile && ollama run bonfyre-qwen3b`

This is the clean handoff. The GGUF writer already exists in the codebase (`ggml_reader.c` + `serialize.c` handle GGUF read/write). We just need to wire it into the `fpq export-gguf` subcommand.

### Path C — GGML backend (future, optional)

Register `GGML_TYPE_FPQ_V12` as a custom tensor type in llama.cpp. Then llama.cpp loads .fpq weights directly, dispatches matmul to our SLI kernel — no decode step, 8× bandwidth reduction during inference.

This is the dream but requires an upstream PR to llama.cpp/GGML. Path B gets us into the Ollama ecosystem immediately with no external dependencies.

### Ollama model registry

Once we have Path B working, publishing models becomes:

```bash
# Convert and package
fpq export-gguf model.fpq model.gguf
# Create Ollama model
ollama create NICKO/qwen2.5-3b-fpq -f Modelfile
# Push to registry
ollama push NICKO/qwen2.5-3b-fpq
```

Then anyone runs: `ollama run NICKO/qwen2.5-3b-fpq`

---

## Implementation priority

| # | What | Why | Size |
|---|------|-----|------|
| 1 | `.fpq → SLI direct load` | Everything else blocks on this. Currently must encode from float to get SLI context. Need: read .fpq tensor → populate SLI struct directly. | ~300 lines C |
| 2 | `libfpq.h` + 5-function API | The public interface. Wraps existing `fpqx_sli_prepare` + `fpqx_sli_matvec`. | ~200 lines C |
| 3 | NEON inner kernel | E8+tile scoring loop is 80% of compute. NEON gets ~4× on Apple Silicon. | ~150 lines C |
| 4 | `fpq run` for text | Transformer forward pass: rmsnorm + rope + attention + ffn + sampling. All matmuls via `fpq_matmul`. | ~800 lines C |
| 5 | `fpq export-gguf` | Write decoded weights as GGUF v3. Gets us into Ollama. | ~400 lines C |
| 6 | `fpq serve` | HTTP server with OpenAI-compatible `/v1/chat/completions`. | ~300 lines C |
| 7 | `fpq pull` | Download .fpq from HuggingFace repos. | ~100 lines C |
| 8 | `fpq run` for speech | Whisper runtime: mel + encoder + decoder. | ~600 lines C |
| 9 | `fpq run` for vision | ViT runtime: patch + encoder + head. | ~400 lines C |
| 10 | Python bindings (`pip install bonfyre-fpq`) | ctypes wrapper around libfpq.so. `from bonfyre_fpq import FPQModel`. | ~100 lines Python |

**Steps 1–5 are the critical path.** After step 5, someone can `ollama run` an FPQ model. After step 6, any OpenAI-compatible app works. After step 10, PyTorch users can drop in.

---

## File layout

```
10-Code/BonfyreFPQ/
├── include/
│   ├── fpq.h           (existing — codec internals)
│   ├── fpqx.h          (existing — SLI + algebra)
│   └── libfpq.h        (NEW — 5-function public API, ~60 lines)
├── src/
│   ├── libfpq.c        (NEW — API impl: open/matmul/close/decode/info)
│   ├── fpq_neon.c      (NEW — NEON E8+tile scoring kernel)
│   ├── fpq_run.c       (NEW — `fpq run` dispatcher + text/speech/vision runtimes)
│   ├── fpq_serve.c     (NEW — HTTP server, OpenAI-compatible)
│   ├── fpq_gguf.c      (NEW — GGUF v3 export from .fpq)
│   ├── fpq_pull.c      (NEW — HuggingFace downloader)
│   └── ... (existing: fpq_native.c, fpqx_ops.c, v4_optimizations.c, etc.)
├── python/
│   └── bonfyre_fpq.py  (NEW — ctypes wrapper, ~100 lines)
├── Makefile            (updated: `make fpq` builds the unified binary)
└── fpq.1              (NEW — man page)
```

The unified `fpq` binary replaces both `bonfyre-fpq` and `bonfyre-fpqx`. One tool.

---

## Design rules

1. **One binary.** `fpq` does everything. No `bonfyre-fpq` + `bonfyre-fpqx` + `libfpq` confusion.
2. **Zero config.** Model arch is in the .fpq header. Tokenizer is embedded or sidecar. No JSON configs.
3. **No Python in the critical path.** Python bindings exist for interop, but `fpq run` is pure C.
4. **Ollama-shaped.** `fpq pull`, `fpq run`, `fpq serve` mirror Ollama's UX so the mental model transfers.
5. **Escape hatches everywhere.** `fpq decode` gets you back to safetensors. `fpq export-gguf` gets you into llama.cpp/Ollama. Never trapped in the format.
