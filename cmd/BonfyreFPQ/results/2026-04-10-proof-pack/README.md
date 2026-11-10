# 2026-04-10 Proof Pack

This folder captures a reproducible local replication set for `v8 RLF` using the binaries and scripts already present in this repo.

## Raw artifacts

- `whisper_base_roundtrip_v8_bits3_limit2.txt`
- `qwen_ppl_v8_bits3_max1000_len512_stride256.txt`
- `qwen_ppl_v4_bits3_max1000_len512_stride256_skipbase.txt`
- `qwen_hqq_bits3_g64_axis1_cpu.txt`
- `hqq_qwen_benchmark.py`
- `qwen_comparison.csv`
- `qwen_comparison.png`
- `plot_qwen_comparison.py`

Two additional files are partial/aborted exploratory runs and should not be treated as final benchmark artifacts:

- `whisper_base_roundtrip_v8_bits3_limit3.txt`
- `qwen_ppl_v8_bits3_max5000.txt`

## Commands run

### 1. Native Whisper roundtrip

```bash
cd /Users/nickgonzales/Documents/Bonfyre/cmd/BonfyreFPQ
./bonfyre-fpq roundtrip-v8 /Users/nickgonzales/.local/share/whisper/ggml-base.en.bin --limit 2 --bits 3 \
  2>&1 | tee results/2026-04-10-proof-pack/whisper_base_roundtrip_v8_bits3_limit2.txt
```

### 2. Qwen perplexity, `v8`

```bash
cd /Users/nickgonzales/Documents/Bonfyre/cmd/BonfyreFPQ
PYTHONUNBUFFERED=1 python3 -u perplexity_benchmark.py \
  --model Qwen/Qwen2.5-0.5B \
  --bits 3 \
  --device mps \
  --mode v8 \
  --max-tokens 1000 \
  --max-length 512 \
  --stride 256 \
  2>&1 | tee results/2026-04-10-proof-pack/qwen_ppl_v8_bits3_max1000_len512_stride256.txt
```

### 3. Qwen perplexity, `v4`

```bash
cd /Users/nickgonzales/Documents/Bonfyre/cmd/BonfyreFPQ
PYTHONUNBUFFERED=1 python3 -u perplexity_benchmark.py \
  --model Qwen/Qwen2.5-0.5B \
  --bits 3 \
  --device mps \
  --mode v4 \
  --max-tokens 1000 \
  --max-length 512 \
  --stride 256 \
  --skip-baseline \
  2>&1 | tee results/2026-04-10-proof-pack/qwen_ppl_v4_bits3_max1000_len512_stride256_skipbase.txt
```

### 4. Qwen perplexity, `HQQ`

```bash
cd /Users/nickgonzales/Documents/Bonfyre/cmd/BonfyreFPQ
PYTHONUNBUFFERED=1 python3 -u results/2026-04-10-proof-pack/hqq_qwen_benchmark.py \
  --model Qwen/Qwen2.5-0.5B \
  --bits 3 \
  --group-size 64 \
  --axis 1 \
  --max-tokens 1000 \
  --max-length 512 \
  --stride 256 \
  --device cpu \
  2>&1 | tee results/2026-04-10-proof-pack/qwen_hqq_bits3_g64_axis1_cpu.txt
```

## Reproduced results

### Whisper base GGML, native `roundtrip-v8`, 3-bit, first 2 tensors

- `v4` worst `0.984447`, avg `0.985678`
- `v7` worst `0.996096`, avg `0.996143`
- `v8` worst `0.999763`, avg `0.999808`
- `Δavg v8-v7 = +0.003665`
- Realized `bpw` on the sampled tensors:
  - tensor 1: `v4 3.56`, `v7 4.18`, `v8 4.73`
  - tensor 2: `v4 3.55`, `v7 4.15`, `v8 4.32`
- Native diagnostic note:
  - `E8 Viterbi RMSE` improvement printed as `0.0%`
  - `RVQ post RMSE` improved by `27.2%` and `25.1%`

Conclusion: `v8` clearly wins on reconstruction quality, but the sampled native path does not yet show a true `~3.0-3.5 bpw` realized rate.

### Qwen 2.5 0.5B, WikiText-2 slice, 3-bit, 994 tokens

Baseline from the `v8` run:

- Baseline PPL: `14.2033`

`v8 RLF`:

- PPL: `14.4829`
- PPL degradation: `+1.97%`
- Average cosine: `0.999783`
- Worst cosine: `0.999588`
- Tensors quantized: `169`

`v4 COORD` on the same slice/settings:

- PPL: `35.5890`
- PPL degradation vs same baseline: `+150.57%`
- Average cosine: `0.982761`
- Worst cosine: `0.982327`
- Tensors quantized: `169`

Conclusion: on this directly reproduced Qwen slice, `v8` is near-lossless while `v4` is decisively broken.

### Qwen 2.5 0.5B, external baseline: `HQQ`, 3-bit, group size 64, CPU

From the standalone HQQ run:

- Baseline PPL: `14.2019`
- `HQQ@3` PPL: `32.3814`
- PPL degradation: `+128.01%`
- Quantized layer type: `HQQLinear`

Conclusion: on the same 994-token Qwen slice, this first external 3-bit HQQ baseline is dramatically worse than `v8 RLF@3` and much closer to the failure regime than the near-lossless regime.

### Visual artifact

- `qwen_comparison.csv` captures the machine-readable comparison table.
- `qwen_comparison.png` is the rendered chart.
- `plot_qwen_comparison.py` regenerates the chart from the CSV.

The chart summarizes the current local frontier on the Qwen slice:

- `FP32`: `14.20`
- `v8 RLF@3`: `14.48`
- `HQQ@3`: `32.38`
- `v4 COORD@3`: `35.59`

## What this proof pack supports

- `v8 RLF` materially outperforms `v4 COORD` on both weight cosine and task-level perplexity.
- `v8 RLF` materially outperforms a real external 3-bit HQQ baseline on the same Qwen slice:
  - `v8`: `14.4829` PPL
  - `HQQ`: `32.3814` PPL
- The current visual frontier is now captured in `qwen_comparison.png`, not just prose.
- The Qwen result is strong enough to support a claim like:
  - "`v8 RLF` preserves Qwen 0.5B quality at 3-bit-mode quantization with low single-digit perplexity degradation in our local reproduction."
- The native Whisper artifact supports the quality story but also forces honest wording about realized rate:
  - sampled `v8` quality is excellent
  - sampled native `bpw` is still above the headline `3-bit` target

## What this proof pack does not yet prove

- It does not establish an apples-to-apples frontier against TurboQuant or other external baselines.
- It does not yet cover GPTQ, AWQ, QuIP#, or TurboQuant directly.
- On this Apple Silicon machine:
  - `auto-gptq` imports, but `Qwen2` is not supported in the installed package (`qwen2 isn't supported yet`)
  - even an `OPT` smoke test reports `Load pretrained model to do quantization requires CUDA available`
  - `autoawq` installation is blocked by Triton / Intel-extension dependency constraints
- It does not yet reproduce the full `BENCHMARKS.md` Qwen run configuration.
- It does not include a finished KV-cache reproduction.
- It does not convert the `3-bit` mode into proven `3.0 bpw` packed reality.
