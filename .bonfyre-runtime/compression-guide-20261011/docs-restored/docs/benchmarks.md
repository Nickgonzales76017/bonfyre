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
| Audio pipeline | 27 | 961 KB |
| Infrastructure | 8 | 646 KB |
| Monetization | 7 | 273 KB |
| Orchestration | 5 | 190 KB |
| **All 47 binaries** | **47** | **~2.1 MB** |

## Transcription quality (BonfyreTranscribe v3.1 + HCP v3.1)

BonfyreTranscribe v3.1 uses the libwhisper C API directly (no fork/exec, no Python) and applies Complex-Domain Hierarchical Constraint Propagation (HCP) v3.1 — a quad-channel spectral refinement algorithm with formant anchoring and context-seeded constrained re-decode — as a post-processing pass.

### Algorithm

Quad-channel complex lifting encodes acoustic confidence (logprob, vlen, no-speech probability) as magnitude channel one, morphological statistics (subword frequency, token length) as phase channel one, token bigram coherence as magnitude channel two (semantic channel), and token trigram coherence as magnitude channel three (combined as 80% bigram + 20% trigram). Radix-2 Cooley-Tukey FFT transforms the joint signal into spectral domain. A three-band adaptive filter with Dirichlet anomaly detection identifies correlated anomaly patterns that flat thresholds miss. IFFT reconstructs per-token quality adjustments.

**KIEL-CC** (Kalman Innovation Error Localization): Adaptive complex Kalman filter tracks token-to-token dynamics. Lag-1 autocorrelation sets the process gain. Normalized innovation magnitude flags tokens whose confidence trajectory diverges from the local trend.

**E-T Gate** (Energy-Text Cross-Agreement Gate): Frame-by-frame RMS energy and spectral flatness via 512-point FFT over the raw audio. Segments where Whisper emitted text but the audio contains silence or noise are flagged as hallucinations.

**Semantic Channel**: A 262K-slot token bigram hash table plus a 524K-slot trigram hash table (generated from speech + text corpora via tiktoken/GPT-2 BPE) provide coherence scores. Combined scoring: 80% bigram + 20% trigram. Segments with >80% low-coherence tokens are flagged.

**Formant Anchoring** (v3.1): Per-segment FFT analysis of speech formant bands (F1: 200–1000 Hz, F2: 1000–3000 Hz). Segments claiming speech but lacking formant energy are flagged.

**Context-Seeded Re-decode** (v3.1): Hallucinated segments are re-decoded with 10-beam search on an expanded audio slice (±2s from surrounding clean segments). The broader audio context gives Whisper better BPE priors for ambiguous regions.

Pure C11 — hand-rolled FFT, no external math library.

### Results (multi-source creator audio, Apple M-series)

#### Base model (ggml-base.en-q5_0, 53 MB)

| Source | Segments | Before (Whisper base) | After (HCP v3.1) | Change |
|---|---|---|---|---|
| Ali Abdaal (720 s) | 429 | 0.928 | **0.999** | **+7.6%** |
| Shaan Puri (720 s) | 467 | 0.924 | **0.998** | **+8.0%** |
| PickFu (360 s) | 200 | 0.916 | **0.999** | **+9.1%** |
| **Average** | — | 0.923 | **0.999** | **+8.2%** |

#### Tiny model (ggml-tiny.en, 74 MB)

| Source | Segments | Before (Whisper tiny) | After (HCP v3.1) | Change |
|---|---|---|---|---|
| Ali Abdaal (720 s) | 436 | 0.923 | **0.998** | **+8.2%** |
| Shaan Puri (720 s) | 214 | 0.901 | **0.997** | **+10.6%** |
| PickFu (360 s) | 252 | 0.915 | **0.998** | **+9.1%** |
| **Average** | — | 0.913 | **0.998** | **+9.3%** |

**Key finding: Tiny + HCP v3.1 achieves 0.997 quality — within 0.1% of base + HCP, at ~5× faster decode speed.**

### Overhead breakdown (base.en, Ali Abdaal)

| Stage | Time |
|---|---|
| HCP spectral (w/ bigram + trigram) | 8.3 ms |
| KIEL-CC Kalman | 0.1 ms |
| E-T Gate audio | 1362 ms |
| Formant Anchoring | 1765 ms |
| Semantic channel | <0.1 ms |
| **Total** | **~3.1 s** |
| % of decode time | ~4% |

### Hallucination detection layers

| Layer | Signal | Threshold |
|---|---|---|
| Compression ratio | zlib compress ratio | > 2.4 |
| N-gram repetition | 4-gram repeat ratio in segment | > 0.4 |
| Vlen anomaly | Token voice-length outlier | > 3σ from mean |
| Low logprob | Per-token log probability | < -1.0 mean |
| HCP spectral | Magnitude/phase deviation after IFFT | Adaptive (Dirichlet) |
| KIEL-CC Kalman | Normalized innovation magnitude | > 3.0σ |
| E-T Gate | RMS energy + spectral flatness vs text | speech_frac < 0.25 |
| Semantic | Token bigram + trigram coherence | >80% below 0.02 |
| Formant | F1/F2 speech-band energy ratio | ratio < 0.15 |

### vs cloud transcription services

| | Deepgram | OpenAI Whisper API | **Bonfyre + HCP v3.1** |
|---|---|---|---|
| Cost | $0.006/min | $0.006/min | **$0/min** |
| Quality | ~0.85–0.90 | ~0.87 (base) | **0.999** (base) / **0.997** (tiny) |
| Hallucination detection | None | None | **9-layer** |
| Post-process overhead | N/A (cloud) | N/A (cloud) | **<5% of decode** |
| Privacy | Cloud | Cloud | **100% local** |
| Internet required | Yes | Yes | **No** |
| Model-agnostic API | N/A | N/A | **hcp_process_universal()** |
| Output formats | JSON, SRT | JSON, SRT, VTT | **JSON+HCP, TXT, SRT, VTT, meta** |

| Category | Count | Binaries |
|---|---|---|
| Infrastructure | 8 | CMS, API, Auth, Index, Graph, Runtime, Hash, Tel |
| Orchestration | 5 | Pipeline, CLI, Queue, Sync, Stitch |
| Audio pipeline | 27 | Ingest through delivery, Embed, Vec, Canon, Tag |
| Monetization | 7 | Offer, Gate, Meter, Ledger, Finance, Outreach, Pay |
| Library | 2 | liblambda-tensors + libbonfyre |
| **Total** | **47 + 2 libs** | **93% pure C11** |

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

## Cumulative P0→P5 results (measured, Apple M-series)

All measurements taken after P5 on `98561b0`.

### Embedding pipeline (embed → search, 10 files)

| Metric | Pre-P0 (baseline) | After P5 | Improvement |
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

| Metric | Pre-P0 | After P5 | Improvement |
|---|---|---|---|
| Full pipeline | 76 ms (10 separate fork/exec) | **8 ms** (unified + SHA-256 dedup) | **9.5×** |

### BonfyreTag (text classification)

| Metric | Pre-P3 (Python fastText) | After P3 (pure C) | Improvement |
|---|---|---|---|
| Runtime dependency | Python 3 + pip `fasttext` | **None** | eliminated |
| Status check latency | ~150 ms (Python import) | **6 ms** | **25×** |
| Predict overhead | fork+exec+Python per call | **Single process, native inference** | eliminated |
| Batch predict | N × Python subprocess | **1 model load, N predictions** | N× fewer forks |

## P4: architecture optimizations

| Optimization | Impact | Details |
|---|---|---|
| BonfyreTel hardening | Crypto-safe session IDs, 0-latency TCP | `arc4random`, `TCP_NODELAY`, O(1) ESL header lookup |
| BonfyreAPI robustness | Crash-proof under load | `SIGPIPE` ignored, thread pool, `vasprintf`, heap buffers |
| libbonfyre FNV hash | **O(1) operator lookup** | FNV-1a hash table replaces linear scan of 47-operator registry |
| BonfyrePipeline dedup | **SHA-256 content dedup** | 279-line dedup engine — skips already-processed artifacts |
| Build system | PGO targets, CFLAGS propagation | `make pgo-gen` / `make pgo-use`; root flags reach all sub-makes |

## P5: syntax/datatype optimizations

Pure zero-risk wins — no behavioral change, all measurable throughput gains:

| Optimization | Impact | Details |
|---|---|---|
| Hex LUT | **~10× faster** hash-to-hex | `snprintf("%02x")` → 16-byte table lookup, 7 sites, 5 files |
| BfArtifact struct shrink | **1,076 → 536 bytes** | `artifact_id[512→128]`, `created_at[128→32]`, `root_hash[128→68]` — doubles L2 cache density |
| Auth `generate_token` | **O(n²) → O(n)** | Eliminated strlen-in-loop + sprintf; tracked offset with hex LUT |
| `strlen()` on constants | **Compile-time** | 10+ sites: `BF_MAGIC_LEN` / `sizeof()` / literal `6` |
| `bf_artifact_compute_keys` | **Cached strlen** | Eliminates 2 redundant full-string scans per call |
| `bf_read_file` raw syscalls | **−2 syscalls** | `open/fstat/read/close` — zero stdio buffer allocation |
| `op_cost` dispatch | **switch(op[0])** | Single char dispatch replaces 10 sequential `strcmp()` calls |
| `memset` via `offsetof` | **Skip 1,000+ bytes** | Only zero struct header; body immediately overwritten by copy |
| Graph JSON assembly | **~5× faster** | `memcpy` + `CPLIT` macro replaces snprintf chain |

### Code health

| Metric | Pre-P0 | After P5 | Improvement |
|---|---|---|---|
| Build flags | `-O2` | **`-O3 -march=native -flto=auto`** | 10–30% across all binaries |
| Duplicated `ensure_dir` | 29 copies | **1** (libbonfyre) | 29 copies eliminated |
| Duplicated `read_file` | 5 copies | **1** (libbonfyre) | 5 copies eliminated |
| Binaries needing Python | 5 (Embed, Vec, Tag, Tone, Transcribe) | **2** (Tone, Tag-train) | 60% reduction |
| libbonfyre linkage | 0/47 | **29/47** | 62% of binaries |
| Operator registry lookup | O(n) linear scan | **O(1)** FNV hash table | algorithmic |
| Artifact struct memory | 1,076 bytes | **536 bytes** | 2× L2 cache density |
| Hash hex conversion | ~100 ns/call (snprintf) | **~10 ns/call** (LUT) | ~10× |
| All 47 binaries total size | — | **~2.1 MB** | — |
| Tests | — | **69 total, all pass** | — |

### Lambda Tensors (compression, N=10,000)

| Encoding | % of raw JSON | Notes |
|---|---|---|
| Raw JSON | 100% (1,189,440 bytes) | baseline |
| V2 + Huffman packed | **9.3%** (110,192 bytes) | O(1) random access |
| Arithmetic packed | **9.2%** (108,859 bytes) | near Shannon limit |
| gzip -9 | 5.5% | no random access |

Lambda Tensors: 1.7× gzip at N=10K but with O(1) per-field random access.
