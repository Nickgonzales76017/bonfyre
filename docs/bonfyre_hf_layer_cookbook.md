# Bonfyre HF Layer Cookbook

This cookbook is the structured companion to the generated extraction artifacts under:

- `recipes/google/*.yaml`
- `recipes/topology/*.yaml`
- `recipes/cross_fusion/*.yaml`
- `recipes/families/family_index.json`
- `workflow/family_bindings.json`

It is intentionally downstream of those files. The repo artifacts are the source of truth; this document explains how to use them.

## Directory Layout

`recipes/google`
- One recipe per Google family or extract target.
- Covers Gemma, Gemma 2, Gemma 3, Gemma 3n, Gemma 4, CodeGemma, T5Gemma, PaliGemma, PaliGemma 2, SigLIP, SigLIP2, EmbeddingGemma, FunctionGemma, ShieldGemma, MedGemma, TxGemma, TranslateGemma, MetricX, AlphaGenome, and Gemma Scope.

`recipes/topology`
- HF collection-derived topology recipes:
  - leaderboard MoE
  - DeepSeek routing
  - Qwen multimodal + quant
  - diffusion / CLIP / LLaVA bridge
  - GGUF / Unsloth runtime pack

`recipes/cross_fusion`
- Derived Bonfyre recipes that combine Google family structure with collection-mined topology structure.

`recipes/families/family_index.json`
- Expanded `T_*` taxonomy with tensor patterns, source repo patterns, capabilities, workflow steps, and subfamily decomposition.

`workflow/family_bindings.json`
- Normalized workflow attachment map for:
  - `A1.s01` ingress
  - `A2.s02` transform/projection
  - `A3.s03` reasoning/routing
  - `A4.s04` memory/cache
  - `A5.s05` safety/eval/output

## Tooling

### `tools/hf_tensor_scan.py`

Purpose:
- scan HF repo metadata
- inspect available tensor/config/tokenizer surfaces
- classify them into Bonfyre `T_*` families using `family_index.json`
- emit a candidate recipe YAML

Example:

```bash
python tools/hf_tensor_scan.py \
  --repo google/gemma-4-31b-it \
  --include "model.layers.*" \
  --emit recipes/generated/gemma4_scan.yaml \
  --collection google/gemma-4
```

Notes:
- uses `BONFYRE_HF_SCAN_FIXTURE_DIR` when set for offline fixture-based scanning
- otherwise uses `huggingface_hub`
- preserves optional vs required tensors in emitted validation blocks

### `tools/hf_layer_pull.py`

Purpose:
- resolve a Bonfyre `T_*` family to tensor patterns from `family_index.json`
- emit a recipe artifact focused on that family

Example:

```bash
python tools/hf_layer_pull.py \
  --repo google/paligemma2-3b-mix-224 \
  --family T_PROJECTOR_BRIDGE \
  --out recipes/generated/paligemma2_projector.yaml
```

## Common Extraction Patterns

Dense decoder core:

```bash
python tools/hf_tensor_scan.py \
  --repo google/gemma-3-12b-it \
  --include "model.layers.*" \
  --emit recipes/generated/gemma3_decoder.yaml
```

Projector / multimodal bridge:

```bash
python tools/hf_layer_pull.py \
  --repo google/paligemma2-3b-mix-224 \
  --family T_PROJECTOR_BRIDGE \
  --out recipes/generated/paligemma2_bridge.yaml
```

Embedding / memory path:

```bash
python tools/hf_layer_pull.py \
  --repo google/embeddinggemma-300m \
  --family T_EMBED_POOL \
  --out recipes/generated/embeddinggemma_memory.yaml
```

MoE routing:

```bash
python tools/hf_layer_pull.py \
  --repo deepseek-ai/DeepSeek-V3 \
  --family T_HIER_ROUTER \
  --out recipes/generated/deepseek_router.yaml
```

Runtime / quant pack:

```bash
python tools/hf_layer_pull.py \
  --repo unsloth/Qwen3-32B-unsloth-bnb-4bit \
  --family T_LAYER_PACK \
  --out recipes/generated/qwen3_runtime_pack.yaml
```

## Validation Philosophy

Every recipe in this cookbook keeps:
- `required_tensors`
- `optional_tensors`
- `missing_behavior`
- gap flags when tensor names are inferred, repo-dependent, or conversion-dependent

This is deliberate. Bonfyre should preserve implementation uncertainty as structured metadata rather than drop entire families.

## Gap Classes

Common gap classes represented in the generated files:
- raw Google export vs HF conversion naming differences
- projector namespace differences (`vision_tower.*` vs `vision_model.*`)
- config-driven structure that is not a standalone tensor
- downstream quant/GGUF metadata that may not live in original source repos
- cross-fusion recipes that combine verified Google families with topology families not co-located in the same upstream repo

## Practical Bonfyre Flow

1. Choose a family recipe from `recipes/google`
2. Choose a topology recipe from `recipes/topology`
3. If needed, choose a fused candidate from `recipes/cross_fusion`
4. Resolve relevant `T_*` families in `recipes/families/family_index.json`
5. Attach them to the workflow spine using `workflow/family_bindings.json`
6. Use the tools to generate repo-specific variants or fresh scans

## Coverage Intent

This cookbook keeps the following explicitly represented:
- Google open model families
- MoE routing
- quantization as structure
- multimodal fusion
- diffusion graphs
- evaluation heads
- runtime/cache layout
- safety heads
- domain heads
- embedding heads
- speculative cross-fusion recipes

The goal is not to reduce these into a few “best model” suggestions. The goal is to keep Bonfyre’s tensor extraction surface broad enough that no high-value architectural seam gets dropped on the floor.
