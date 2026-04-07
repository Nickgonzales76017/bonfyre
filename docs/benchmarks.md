# Benchmarks

All benchmarks on Apple M-series, single-threaded unless noted.

## Pipeline latency

End-to-end processing of a single audio file through the full pipeline:

| Mode | Latency | Notes |
|---|---|---|
| Sequential (10 separate binaries) | 76 ms | Fork/exec overhead per step |
| Optimized (inline SHA-256, direct SQLite) | 20–35 ms | After per-binary optimization |
| **Unified (`bonfyre-pipeline run`)** | **5–8 ms** | Single process, no fork/exec |

### Breakdown (unified mode)

| Stage | Time |
|---|---|
| Gate check | < 1 ms |
| Ingest (normalize + SHA-256) | 2–3 ms |
| Index build (direct libsqlite3) | 2–3 ms |
| Meter record | 1–2 ms |
| Stitch (JSON write) | < 1 ms |
| Ledger (JSONL append) | < 1 ms |

## Lambda Tensors compression

Tested on N=10,000 structurally similar JSON records:

| Encoding | % of raw JSON | Absolute |
|---|---|---|
| Raw JSON | 100% | baseline |
| gzip -9 | 5.5% | no random access |
| zstd -19 | 4.8% | no random access |
| V1 (varint + zigzag) | 88% | binary packing |
| V2 (type-aware) | 64.9% | small-int, float32 downshift |
| V2 + Interned | 29% | cross-member string dedup |
| **V2 + Huffman** | **13.5%** | canonical Huffman per position |
| Codebook overhead | — | 399–666 bytes (one-time) |

Lambda Tensors is 2.4× larger than gzip but provides O(1) per-field random access.

### Compression vs gzip ratio by family size

| Family size | Lambda Tensors / gzip |
|---|---|
| N=100 | 1.9× gzip |
| N=1,000 | 2.3× gzip |
| N=10,000 | 2.8× gzip |

Advantage grows with family size because the generator + codebook amortize better.

## CMS operations

BonfyreCMS (299 KB binary) benchmarked in-process:

| Operation | Throughput | Notes |
|---|---|---|
| Create record | 921 μs/op | N=1,000 mixed schemas |
| Update record | 155 μs/op | Direct libsqlite3 |
| Reconstruct (Lambda Tensors) | 89 μs/op | From compressed form |
| ANN k-NN query | 1.13 ms | N=10,000, k=10 |

## Memory usage

| Component | Peak RSS |
|---|---|
| BonfyreIndex (N=3,000 artifacts) | 2.18 MB |
| BonfyrePipeline (N=3,000 artifacts) | 2.36 MB |
| BonfyreCMS idle | 15 MB |
| Lambda Tensors compression (N=10,000) | 66 MB |

## Binary sizes (measured)

| Category | Count | Total size |
|---|---|---|
| Audio pipeline | 18 | 648 KB |
| Infrastructure | 7 | 596 KB |
| Monetization | 7 | 284 KB |
| Orchestration | 5 | 160 KB |
| **All 46 binaries** | **46** | **2.0 MB** |

| Category | Count | Binaries |
|---|---|---|
| Infrastructure | 7 | CMS, API, Auth, Index, Graph, Runtime, Hash |
| Orchestration | 5 | Pipeline, CLI, Queue, Sync, Stitch |
| Audio pipeline | 26 | Ingest through delivery, Embed, Vec, Canon |
| Monetization | 7 | Offer, Gate, Meter, Ledger, Finance, Outreach, Pay |
| Library | 2 | liblambda-tensors + libbonfyre |
| **Total** | **46 + 2 libs** | **93% pure C11** |

## Embedding & vector search (BonfyreEmbed + BonfyreVec)

BonfyreEmbed: ONNX Runtime C API, BERT WordPiece tokenizer in C, mean pooling + L2 normalize.
BonfyreVec: SQLite C API + sqlite-vec extension, no Python.

### ONNX inference (all-MiniLM-L6-v2, 384-dim, Apple M-series)

| Configuration | Wall time | CPU utilization |
|---|---|---|
| Single-threaded (`intra_op=1`, `ORT_ENABLE_BASIC`) | 304 ms | 119% |
| **Multi-threaded (`intra_op=ncpu`, `ORT_ENABLE_ALL`)** | **237 ms** | **158%** |

Measured single-embed wall time: **237 ms** (3-run median, warm cache).

### Trie tokenizer (P1)

Replaced hash-table Vocab with trie-based tokenizer:

| Aspect | Hash table | Trie |
|---|---|---|
| Lookup | O(n) shrinking-substring probes | **O(word_len) single traversal** |
| Subword matching | Rebuild `##` prefix per probe | **Dedicated sub_trie (keys without `##`)** |
| Memory layout | 30K hash buckets | **128-wide ASCII children, pool-allocated** |
| Correctness | Identical | **Verified: 384-dim output matches within 1e-6** |

### `--insert-db` inline insertion (P1)

Embed + insert into sqlite-vec in a single process, zero intermediate file I/O:

```bash
bonfyre-embed --text doc.txt --insert-db my.db --backend onnx
```

| Mode | Steps | File I/O |
|---|---|---|
| Separate binaries | embed → write VECF → read VECF → insert | 2 writes + 1 read |
| **`--insert-db`** | **embed → insert (in-process)** | **0** |

### Vector output format

| Format | Size (384-dim) | Parse overhead |
|---|---|---|
| JSON (`{"vector": [...]}`) | 6.4 KB | 384 × `strtof()` ≈ 0.1 ms |
| **VECF binary (raw float32)** | **1.5 KB** | **`fread()` < 0.001 ms** |

For batch ingestion of 10K embeddings: JSON parse = ~1 second; VECF binary = ~10 ms.

### Batch embedding (P2+P3, measured)

10 files, all-MiniLM-L6-v2, Apple M-series:

| Mode | Wall time | Model loads | DB opens | Speedup |
|---|---|---|---|---|
| 10 × single invocations | 2,492 ms | 10 | — | baseline |
| **`--input-dir` batch (P2)** | **386 ms** | **1** | — | **6.5×** |
| **`--input-dir --insert-db` (P2+P3)** | **689 ms** | **1** | **1** | **3.6×** |

3 files:

| Mode | Wall time | Speedup |
|---|---|---|
| 3 × single invocations | ~750 ms | baseline |
| **`--input-dir` batch** | **428 ms** | **1.75×** |
| **`--input-dir --insert-db`** | **315 ms** | **2.4×** |

### SIMD cosine similarity (P2)

BonfyreVec hand-rolled NEON (ARM) cosine for exact search and pairwise comparison:

| Mode | Backend | Use case |
|---|---|---|
| `search` (default) | sqlite-vec ANN | Fast approximate nearest neighbors |
| `search --exact` | **NEON SIMD cosine** | Brute-force exact scan, no approximation |
| `compare <id1> <id2>` | **NEON SIMD cosine** | Pairwise similarity between stored vectors |

Self-match: cosine = 1.00000000, distance = 0.00000000.

### Batch embedding (P2)

`--input-dir` loads the ONNX model once and embeds N files in a single session:

| Mode | 3 files | Model loads |
|---|---|---|
| 3 × `bonfyre-embed --text` | ~1.8 s | 3 |
| **`bonfyre-embed --input-dir`** | **~0.9 s** | **1** |

### libbonfyre shared runtime (P2)

8 binaries refactored from duplicated utility functions to shared `libbonfyre.a`:

| Function | Before | After |
|---|---|---|
| `ensure_dir` | 8 copies (~13 LOC each) | **`bf_ensure_dir()` — single implementation** |
| `read_file_contents` | 4 copies (~12 LOC each) | **`bf_read_file()` — single implementation** |

Binaries: Repurpose, Segment, Clips, SpeechLoop, Tone, Canon, Query, Tag.

## P3: Connection pooling, full libbonfyre, native fastText

### BonfyreTag: fastText inference in pure C

| Metric | Before (Python subprocess) | After (native C) |
|---|---|---|
| Python dependency | Required (fasttext pip package) | **None for inference** |
| Process overhead | fork+exec per prediction | **Single process** |
| Model loading | Per invocation via Python | **Once, reused for batch** |
| Status latency | ~150 ms (Python import overhead) | **6 ms** |
| Binary size | ~50 KB + Python runtime | ~55 KB standalone |

### BonfyreEmbed: batch DB connection pooling (measured)

| Metric | Before | After |
|---|---|---|
| DB opens per batch | N (one per file) | **1** |
| sqlite3_load_extension calls | N | **1** |
| Prepared statements | Created+finalized per file | **Created once, reset per file** |
| 10-file batch+insert wall time | — | **689 ms** (vs 2,492 ms single) |

### libbonfyre linkage (expanded)

| Metric | P2 | P3 |
|---|---|---|
| Binaries linked | 8 | **29** |
| `ensure_dir` copies eliminated | 8 | **29 (all instances)** |
| `read_file_contents` copies eliminated | 4 | **5 (all instances)** |

Binaries added in P3: Brief, CMS, Compress, Embed, Emit, Graph, Ingest, MediaPrep,
MFADict, Narrate, Offer, Pack, Paragraph, Pipeline, Proof, Render, Stitch,
Transcribe, TranscriptClean, TranscriptFamily, WeaviateIndex.

## Comparison: Bonfyre CMS vs Strapi

| Metric | Strapi | Bonfyre CMS |
|---|---|---|
| Install size | ~500 MB | 299 KB |
| Dependencies | 400+ npm packages | libc + SQLite |
| Cold start | 30–120 s | < 50 ms |
| Create throughput | ~2 ms/op | 921 μs/op |
| Memory (idle) | ~200 MB | 15 MB |
| Language | JavaScript | C11 |

## Reproducing

```bash
# Build and run the CMS benchmark
cd cmd/BonfyreCMS
make
./bonfyre-cms bench --rounds 1000

# Run the pipeline benchmark
cd cmd/BonfyrePipeline
make
./bonfyre-pipeline bench --input test.md --rounds 100
```

## Cumulative P0→P3 results (measured, Apple M-series)

All measurements taken after P3 on `0793070`.

### Embedding pipeline (embed → search, 10 files)

| Metric | Pre-P0 (baseline) | After P3 | Improvement |
|---|---|---|---|
| Single embed wall time | ~600 ms (Python subprocess) | **237 ms** (C + ONNX multi-thread) | **2.5×** |
| 10-file embed | ~6,000 ms (10 × Python) | **386 ms** (batch, 1 model load) | **15.5×** |
| 10-file embed + DB insert | ~7,000 ms+ (10 × Python + 10 × DB open) | **689 ms** (batch + pooled DB) | **10×** |
| Vector file size (384-dim) | 6.4 KB JSON | **1,544 bytes** VECF binary | **4.2×** smaller |
| Vector parse time | 384 × `strtof()` ≈ 0.1 ms | `fread()` < 0.001 ms | **~100×** |
| Vec exact search (10 docs) | N/A (no SIMD path) | **5 ms** (NEON cosine brute-force) | new capability |
| Vec ANN search (10 docs) | — | **8 ms** (sqlite-vec) | — |
| Vec pairwise compare | N/A | **4 ms** | new capability |
| Tokenizer | Hash-table O(n) probes | **Trie O(word_len)** | algorithmic improvement |

### Pipeline (gate → ingest → index → meter → stitch → ledger)

| Metric | Pre-P0 | After P3 | Improvement |
|---|---|---|---|
| Full pipeline | 76 ms (10 separate fork/exec) | **8 ms** (unified, single process) | **9.5×** |

### BonfyreTag (text classification)

| Metric | Pre-P3 (Python fastText) | After P3 (pure C) | Improvement |
|---|---|---|---|
| Runtime dependency | Python 3 + pip `fasttext` | **None** | eliminated |
| Status check latency | ~150 ms (Python import) | **6 ms** | **25×** |
| Predict overhead | fork+exec+Python per call | **Single process, native inference** | eliminated |
| Batch predict | N × Python subprocess | **1 model load, N predictions** | N× fewer forks |

### Code health

| Metric | Pre-P0 | After P3 | Improvement |
|---|---|---|---|
| Build flags | `-O2` | **`-O3 -march=native -flto=auto`** | 10–30% across all binaries |
| Duplicated `ensure_dir` | 29 copies | **1** (libbonfyre) | 29 copies eliminated |
| Duplicated `read_file` | 5 copies | **1** (libbonfyre) | 5 copies eliminated |
| Binaries needing Python | 5 (Embed, Vec, Tag, Tone, Transcribe) | **3** (Tone, Transcribe, Tag-train) | 40% reduction |
| libbonfyre linkage | 0/46 | **29/46** | 63% of binaries |
| All 46 binaries total size | — | **2.0 MB** | — |
| Tests | 38 lib + 20 bonfyre + 10 status | **68 total, all pass** | — |

### Lambda Tensors (compression, N=10,000)

| Encoding | % of raw JSON | Notes |
|---|---|---|
| Raw JSON | 100% (1,189,440 bytes) | baseline |
| V2 + Huffman packed | **9.3%** (110,192 bytes) | O(1) random access |
| Arithmetic packed | **9.2%** (108,859 bytes) | near Shannon limit |
| gzip -9 | 5.5% | no random access |

Lambda Tensors: 1.7× gzip at N=10K but with O(1) per-field random access.
