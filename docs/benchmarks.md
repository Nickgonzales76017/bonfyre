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

## Binary sizes

| Category | Count | Total size |
|---|---|---|
| Infrastructure | 7 | ~529 KB |
| Orchestration | 5 | ~183 KB |
| Audio pipeline | 18 | ~607 KB |
| Category | Count | Notes |
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
| **Multi-threaded (`intra_op=ncpu`, `ORT_ENABLE_ALL`)** | **227 ms** | **187%** |

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
