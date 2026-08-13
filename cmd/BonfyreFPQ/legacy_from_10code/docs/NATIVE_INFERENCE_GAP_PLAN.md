# BonfyreFPQ Native Inference Gap Plan

## Problem

The current `.fpq` path wins on storage but loses on usability.

Today the pipeline is:

1. store seed programs in `.fpq`
2. fully decode offline
3. write compatibility safetensors
4. run normal Transformers inference

That means the compression result is real, but the product result is incomplete.

If native `.fpq` cannot participate in inference directly, then the core claim is still open.

## Current Technical Reality

The format stores reconstruction programs, not ready-to-run dense weights.

- `src/fpq_native.c` reads and writes the native compact format
- `src/serialize.c` defines the serialized tensor and block structure
- `src/v4_optimizations.c` contains the full decode path for v8/v9 tensors
- `src/fpqx_ops.c` contains the unified FPQ-X decode machinery

The blocker is not file I/O. The blocker is that Transformers matmul expects contiguous weight tensors, while `.fpq` currently requires a seed-expansion decode pipeline before a usable tensor exists.

## What Does Not Count

These are not enough on their own:

- better storage ratios without direct inference
- faster offline decode into safetensors
- a separate runtime that still requires a full pre-expand step before inference begins

The bar is stricter:

- native `.fpq` must participate in inference directly
- no offline decompression step
- no material quality regression versus the compatibility path

## Three Viable Directions

### 1. FPQ Lazy Tensor Loader

Wrap `.fpq` tensors as lazily decoded tensors inside PyTorch.

Idea:

- memory-map the `.fpq` file
- keep tensor metadata resident
- decode only the blocks touched by each layer matmul
- cache decoded blocks per layer/device
- expose this through a Python extension so `from_pretrained()` can attach an FPQ-backed parameter object

Why this is good:

- keeps the existing format
- reuses the existing C decode logic
- gets to direct inference without changing model weights on disk

Why this is not enough by itself:

- if every forward pass repeatedly reconstructs whole tensors, latency will still be bad
- it needs block cache discipline and prefetching to avoid turning decompression into the new bottleneck

### 2. FPQ Fused Matmul Runtime

Do not decode full tensors. Decode blocks directly into the GEMM path.

Idea:

- pack `.fpq` tensors as block programs
- during `linear(x, W)`, reconstruct one block of `W` at a time inside the kernel path
- multiply immediately and discard temporary dense block data
- keep only hot blocks cached

This is the most novel path because it changes the question from:

"How do we recreate dense weights fast enough?"

to:

"How do we consume compressed programs directly inside matmul?"

Why this is the strongest target:

- closes the real runtime gap
- avoids full weight materialization
- makes `.fpq` a serving format instead of only an archive format

Why this is hard:

- requires a custom CPU and probably CUDA matmul backend
- requires scheduler work for block ordering, cache reuse, and prefetching
- requires proving that decode-plus-matmul beats decode-then-matmul in practice

### 3. Hybrid Native Serving Format

Split each tensor into an inference-critical dense path and a compressed correction path.

Idea:

- keep a small dense low-rank or structured base matrix
- store the residual in `.fpq`
- run inference on the base immediately
- apply residual reconstruction only where it matters most

Why this matters:

- likely the fastest route to usable native inference
- lowers the amount of runtime reconstruction needed
- creates a practical bridge between storage innovation and inference innovation

Why this is a trade-off:

- less pure than full native `.fpq` inference
- sacrifices some compression to gain real usability
- needs careful quality accounting so it does not quietly become another compatibility path under a different name

## Recommended Path

The best path for BonfyreFPQ is:

### Phase 1. Build a truthful native runtime target

Ship a Python extension plus C runtime that can load `.fpq` directly and run inference without an offline conversion step.

Target architecture:

- `libbonfyrefpq_runtime`
- Python module: `bonfyrefpq_runtime`
- API shape:
  - `load_fpq_tensor(path, tensor_name, device)`
  - `load_fpq_model(repo_or_dir, device)`
  - `fpq_linear(input, fpq_weight, bias=None)`

This phase proves direct inference is possible.

### Phase 2. Make it novel

Upgrade from lazy full-tensor decode to fused blockwise decode-plus-matmul.

This is the actual innovation target.

Concretely:

- decode one FPQ block into a small scratch buffer
- multiply against the input tile immediately
- accumulate output tile
- prefetch next block while compute runs
- cache only high-reuse blocks and hot layers

Success means `.fpq` is no longer just compressed storage. It becomes an executable weight format.

### Phase 3. Add a hybrid fast path where needed

For the heaviest tensors, allow:

- dense low-rank base
- FPQ residual correction

This gives a production escape hatch if pure fused FPQ runtime is still too slow on some layers.

## Concrete Build Order

### Step 1. Extract a standalone runtime library

Refactor decode entry points out of the CLI path into a runtime API.

Needed exports:

- open native file
- inspect tensor metadata
- decode one tensor block
- decode one output tile contribution

Probable sources:

- `src/fpq_native.c`
- `src/v4_optimizations.c`
- `src/fpqx_ops.c`

### Step 2. Add block-level decode API

Current code is oriented around reconstructing full tensors.

Add APIs shaped like:

```c
int fpq_open_model(const char *path, fpq_model_handle **out);
int fpq_get_tensor_info(fpq_model_handle *model, const char *name, fpq_tensor_info *out);
int fpq_decode_block(fpq_model_handle *model, const char *name, uint32_t block_idx, float *scratch);
int fpq_linear_blocked(const float *input, fpq_tensor_handle *weight, const float *bias, float *output);
```

The main shift is from full reconstruction to block reconstruction.

### Step 3. Build Python bindings

Use a minimal extension layer so Hugging Face models can attach FPQ-backed parameters.

Goals:

- load a config and tokenizer normally
- intercept weight loading
- map selected parameters to FPQ handles instead of dense tensors
- override `Linear` calls with `fpq_linear_blocked`

### Step 4. Benchmark the right thing

The new benchmark is not file size.

The new benchmark table must include:

- first-token latency
- tokens/sec
- peak memory
- perplexity delta vs compatibility safetensors
- quality delta vs full BF16

### Step 5. Only then update public claims

The public story changes only after:

- native `.fpq` inference works end-to-end
- the runtime path does not require offline decompression
- the quality delta is acceptable and measured

## Novelty Thesis

The strongest novel claim available here is not:

"We compressed model files more."

It is:

"We turned weight files into executable reconstruction programs that can be consumed directly by inference kernels without ever materializing the full dense tensor offline."

That is the difference between an archive format and a runtime format.

## Immediate Next Milestone

The next milestone should be narrow and testable:

### Milestone A

Run one `Linear` layer directly from `.fpq` in Python without generating safetensors first.

Definition of done:

- input activations enter Python
- output activations come from native `.fpq` weight execution
- no offline conversion file is created
- cosine similarity and max error are measured against BF16 output

If this milestone fails, the format is still storage-only.

If this milestone succeeds, BonfyreFPQ has the beginning of a real native inference story.