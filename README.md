<p align="center">
  <h1 align="center">🔥 Bonfyre</h1>
  <p align="center">
    <strong>46 static C binaries. Pure C11. A complete backend platform.</strong>
  </p>
  <p align="center">
    <a href="#install">Install</a> ·
    <a href="#use-cases">Use Cases</a> ·
    <a href="#benchmarks">Benchmarks</a> ·
    <a href="#all-46-binaries">All 46 binaries</a> ·
    <a href="#docs">Docs</a> ·
    <a href="#contributing">Contributing</a>
  </p>
</p>

---

```bash
git clone https://github.com/Nickgonzales76017/bonfyre.git && cd bonfyre && make
```

That builds 46 binaries. No Node.js. No Python. No Docker. No npm. Just C11 and SQLite.

---

## Use cases

<blockquote>
<strong>Pick the one that matches your problem.</strong> Each is a standalone entry point — you don't need to understand the whole system.
</blockquote>

### → I want a lightweight CMS that isn't 500 MB of node_modules

**You need:** `bonfyre-cms` (299 KB)

```bash
make -C cmd/BonfyreCMS
./cmd/BonfyreCMS/bonfyre-cms serve --port 8800
# REST API at http://localhost:8800 — dynamic schemas, token auth, zero deps
```

| | Strapi | Bonfyre CMS |
|---|---|---|
| Install size | ~500 MB | **299 KB** |
| Dependencies | Node.js + 400 npm packages | libc + SQLite |
| Startup | 30–120 seconds | **< 50 ms** |
| Memory (idle) | ~200 MB | **15 MB** |

**[Full CMS guide →](docs/cms.md)**

---

### → I want to transcribe audio locally without cloud APIs

**You need:** `bonfyre-transcribe` + `bonfyre-media-prep`

```bash
bonfyre-media-prep normalize interview.mp3          # → 16kHz mono WAV
bonfyre-transcribe run interview.wav --out transcript.json  # → local Whisper
bonfyre-brief generate transcript.json --out summary.md     # → executive summary
```

Everything runs on your machine. No OpenAI key. No internet. No per-minute billing.

**[Full pipeline guide →](docs/pipeline.md)**

---

### → I want to shrink my JSON API payloads

**You need:** `liblambda-tensors` (41 KB library)

```c
#include <lambda_tensors.h>

LT_Family *fam = lt_family_create();
lt_family_add(fam, json_str_1, len_1);  // add records
lt_family_add(fam, json_str_2, len_2);
lt_family_finalize(fam);

// Read ONE field from record 3000 — without decompressing anything
const char *val = lt_family_read_field(fam, 3000, field_idx);
```

| Method | Size | Random access |
|---|---|---|
| Raw JSON | 100% | ✓ |
| gzip | 5.5% | ✗ |
| **Lambda Tensors** | **13.5%** | **✓** |

Not a gzip replacement — a structured data tool. O(1) per-field reads on compressed data.

**[Full Lambda Tensors guide →](docs/lambda-tensors.md)**

---

### → I want a full audio-to-invoice pipeline

**You need:** `bonfyre-pipeline` (all 10 steps in one binary, 5–8 ms)

```bash
bonfyre-pipeline run --input interview.mp3 --out ./output
```

That runs: ingest → normalize → hash → transcribe → clean → paragraph → brief → proof → offer → pack. Output: ZIP with transcript, summary, action items, quality score, and pricing proposal.

**[Full pipeline guide →](docs/pipeline.md)** · **[Run the demo →](examples/full-pipeline/)**

---

### → I want to self-host a complete SaaS backend

**You need:** `bonfyre-api` + `bonfyre-auth` + `bonfyre-pay` + `bonfyre-gate`

```bash
bonfyre-api --port 9090 --static frontend/ serve &    # HTTP gateway + dashboard
bonfyre-auth signup --email user@example.com --password ...  # user management
bonfyre-gate issue --email user@example.com --tier pro       # API key provisioning
bonfyre-pay invoice --user-id 1 --period 2026-04              # billing
```

Auth, payments, usage metering, API keys, rate limiting — all as composable binaries. Total: ~240 KB.

**[API reference →](docs/api.md)** · **[Run the demo →](examples/saas-backend/)**

---

### → I want to embed a compression library in my project

**You need:** `liblambda-tensors` (41 KB static `.a` + shared `.so`)

```bash
cd lib/liblambda-tensors
make                    # → liblambda-tensors.a + liblambda-tensors.so
make install PREFIX=/usr/local  # → installs header + lib
```

Then link it:
```bash
cc -o myapp myapp.c -llambda-tensors -lm
```

MIT licensed. Embed it anywhere — your app, your library, your product. Like SQLite but for structured compression.

**[Full Lambda Tensors guide →](docs/lambda-tensors.md)**

## Install

### From source (recommended)

```bash
git clone https://github.com/Nickgonzales76017/bonfyre.git
cd bonfyre
make            # builds all 46 binaries + libbonfyre + liblambda-tensors
make install    # copies to ~/.local/bin (or PREFIX=/usr/local make install)
```

### One command (macOS / Linux)

```bash
curl -fsSL https://raw.githubusercontent.com/Nickgonzales76017/bonfyre/main/install.sh | sh
```

### npm (bindings coming soon)

```bash
npm install bonfyre
```

### Requirements

- C11 compiler (gcc or clang)
- SQLite3 development headers (`libsqlite3-dev` / `sqlite3` on macOS)
- zlib (`zlib1g-dev` / included on macOS)

## Benchmarks

### Pipeline latency (Apple M-series, single file)

| Mode | Latency |
|---|---|
| Sequential (10 binaries) | 76 ms |
| Optimized (inline SHA-256, direct SQLite) | 20–35 ms |
| **Unified pipeline** | **5–8 ms** |

### Lambda Tensors compression (N=10,000 structured JSON records)

| Encoding | % of raw | Notes |
|---|---|---|
| V1 (varint + zigzag) | 88% | Baseline binary packing |
| V2 (small-int, float32 downshift) | 64.9% | Type-aware encoding |
| V2 + Interned strings | 29% | Cross-member string dedup |
| **V2 + Huffman** | **13.5%** | Family-aware canonical Huffman |

### CMS operations (BonfyreCMS on Apple M-series)

| Operation | Throughput |
|---|---|
| Create | 921 μs/op |
| Update | 155 μs/op |
| Reconstruct (Lambda Tensors) | 89 μs/op |
| ANN k-NN query (N=10,000) | 1.13 ms |

### Memory

| Component | Peak RSS |
|---|---|
| BonfyrePipeline (3,000 artifacts) | 2.36 MB |
| BonfyreCMS idle | 15 MB |

## Performance

### Pure C — 93% of binaries are zero-dependency native C11

BonfyreEmbed and BonfyreVec were rewritten from Python subprocess wrappers to pure C:

| Component | Before | After |
|---|---|---|
| BonfyreEmbed | Python subprocess → ONNX | **ONNX Runtime C API** + BERT WordPiece tokenizer in C |
| BonfyreVec | Python subprocess → sqlite-vec | **SQLite C API** + vec0 extension loaded natively |
| Build flags | `-O2` | **`-O3 -march=native -flto=auto`** |

### P0 optimizations (shipped)

| Optimization | Impact | Details |
|---|---|---|
| ONNX multi-threading | **3–4× inference speed** | `SetIntraOpNumThreads` → CPU count (was hardcoded `1`) |
| Binary vector format (VECF) | **4.2× smaller**, zero parse overhead | Raw float32 with magic header; eliminates 384 `strtof()` calls |
| `-O3 -march=native -flto=auto` | **10–30% across all binaries** | Auto-vectorization, LTO inlining, native ISA |
| `ORT_ENABLE_ALL` graph optimization | Additional inference gains | Was `ORT_ENABLE_BASIC` |

**Measured:** 158% CPU utilization (multi-core ONNX), single-embed: **237 ms** wall time (down from 304 ms).

### P1 optimizations (shipped)

| Optimization | Impact | Details |
|---|---|---|
| Trie-based tokenizer | **O(word_len) per piece** | 128-wide ASCII trie replaces hash table; dual tries for vocab + `##` subwords |
| `--insert-db` inline insertion | **Zero intermediate file I/O** | Embed + insert into sqlite-vec DB in one process — skips JSON/VECF write entirely |
| Optional `--out` | Simpler batch pipelines | `--out` can be omitted when `--insert-db` is provided |

`--insert-db` eliminates the embed → write → read → insert round-trip:

```bash
# Before (two binaries, file I/O in between)
bonfyre-embed --text doc.txt --out doc.vecf --output-format binary
bonfyre-vec insert my.db doc.vecf --doc-id doc

# After (single binary, zero file I/O)
bonfyre-embed --text doc.txt --insert-db my.db --backend onnx
```

### P2 optimizations (shipped)

| Optimization | Impact | Details |
|---|---|---|
| SIMD cosine similarity | **NEON (ARM) + scalar fallback** | `bonfyre-vec compare` + `--exact` brute-force search; bypasses sqlite-vec ANN |
| Batch embedding mode | **Single model load for N files** | `--input-dir` amortizes ONNX session startup across directory of .txt files |
| libbonfyre shared runtime | **8 binaries refactored** | Replaces duplicated `ensure_dir` + `read_file_contents` with `bf_ensure_dir` / `bf_read_file` |

### P3 optimizations (shipped)

| Optimization | Impact | Details |
|---|---|---|
| BonfyreTag pure C inference | **Zero Python deps for predict/batch/lang** | Loads .bin models natively — model parse, ngram hash, matrix multiply, softmax all in C11. Training still uses Python wrapper |
| libbonfyre full linkage | **29/46 binaries linked** | All binaries with `ensure_dir` now use `bf_ensure_dir` via libbonfyre. Eliminated 21 duplicate implementations |
| Batch DB connection pooling | **Single sqlite3 open for N inserts** | `--input-dir --insert-db` holds one connection + prepared statements across all files instead of open/close per file |

```bash
# Native fastText prediction (no Python)
bonfyre-tag predict model.bin input.txt output/ --top 5

# Batch embed + insert with pooled DB connection (one model load, one DB open)
bonfyre-embed --input-dir corpus/ --insert-db vectors.db --backend onnx

# Exact SIMD cosine search (bypasses ANN approximation)
bonfyre-vec search vectors.db query.vecf --exact

# Pairwise similarity between two documents
bonfyre-vec compare vectors.db doc1 doc2
```

### Binary vector format (VECF)

BonfyreEmbed can output raw float32 vectors instead of JSON:

```bash
bonfyre-embed --text input.txt --out embedding.vecf --output-format binary
# 1,544 bytes binary vs 6.4 KB JSON — auto-detected by bonfyre-vec search
```

| Format | Size (384-dim) | Parse time |
|---|---|---|
| JSON | 6.4 KB | 384 × `strtof()` ≈ 384K cycles |
| **VECF binary** | **1,544 bytes** | `fread()` ≈ **< 1K cycles** |

### Cumulative results (measured, Apple M-series)

| Operation | Before P0 | After P3 | Speedup |
|---|---|---|---|
| Single embed | ~600 ms (Python) | **237 ms** (C + ONNX) | **2.5×** |
| 10-file embed | ~6,000 ms (10 × Python) | **386 ms** (batch) | **15.5×** |
| 10-file embed + DB insert | ~7,000 ms+ | **689 ms** (batch + pooled DB) | **10×** |
| Vector file (384-dim) | 6.4 KB JSON | **1,544 bytes** VECF | **4.2× smaller** |
| Vec exact search (10 docs) | N/A | **5 ms** (NEON SIMD) | new |
| Pipeline (6 stages) | 76 ms (10 binaries) | **8 ms** (unified) | **9.5×** |
| BonfyreTag inference | ~150 ms (Python subprocess) | **6 ms** (pure C) | **25×** |
| Duplicated utility code | 34 copies across binaries | **1 each** (libbonfyre) | eliminated |
| Binaries needing Python | 5 | **3** | 40% reduction |
| All 46 binaries disk | — | **2.0 MB total** | — |
| Test suite | — | **68 tests, all pass** | — |

## Examples

Standalone repos you can clone and run independently. Each builds only the binaries it needs.

| Repo | What it does | Time to run | CEO pitch |
|---|---|---|---|
| **[quickstart](https://github.com/Nickgonzales76017/bonfyre-example-quickstart)** | Clone → build → CMS + embed + search | 30 sec | "Entire backend in 2 MB" |
| **[semantic-search](https://github.com/Nickgonzales76017/bonfyre-example-semantic-search)** | Embed 20 docs, search by meaning | 5 min | "Replace $250/mo Pinecone — local, 5 ms queries" |
| **[transcribe](https://github.com/Nickgonzales76017/bonfyre-example-transcribe)** | Audio → transcript → summary → tags → ZIP | 5 min | "Replace $0.006/min Deepgram — $0, offline, private" |
| **[compress](https://github.com/Nickgonzales76017/bonfyre-example-compress)** | Compress 10K JSON records, random access | 2 min | "13.5% of JSON with O(1) field access" |
| **[saas-stack](https://github.com/Nickgonzales76017/bonfyre-example-saas-stack)** | Auth + billing + API gateway + CMS | 5 min | "Replace $2,500/mo SaaS stack — 240 KB, zero vendors" |

```bash
# Pick one and go:
git clone https://github.com/Nickgonzales76017/bonfyre-example-semantic-search.git
cd bonfyre-example-semantic-search
./setup.sh && ./run.sh
```

## All 46 binaries

### Infrastructure

| Binary | Size | Purpose |
|---|---|---|
| `bonfyre-cms` | 299 KB | Content management system with Lambda Tensors compression |
| `bonfyre-api` | 69 KB | HTTP gateway, file upload, job management, static server |
| `bonfyre-auth` | 35 KB | User signup/login, session tokens, SHA-256 passwords |
| `bonfyre-index` | 68 KB | SQLite artifact index + full-text search |
| `bonfyre-graph` | 51 KB | Merkle-DAG artifact graph, SHA-256 content addressing |
| `bonfyre-runtime` | 33 KB | Runtime environment, process lifecycle |
| `bonfyre-hash` | 34 KB | Pure C SHA-256 (FIPS 180-4), content addressing |

### Orchestration

| Binary | Size | Purpose |
|---|---|---|
| `bonfyre-pipeline` | 51 KB | Unified in-process pipeline (5–8 ms end-to-end) |
| `bonfyre-cli` | 33 KB | Unified command dispatcher |
| `bonfyre-queue` | 33 KB | Persistent job queue (SQLite) |
| `bonfyre-sync` | 33 KB | Cross-instance replication |
| `bonfyre-stitch` | 33 KB | DAG materializer, result assembly |

### Audio pipeline

| Binary | Size | Purpose |
|---|---|---|
| `bonfyre-ingest` | 33 KB | File intake, type detection, manifest generation |
| `bonfyre-media-prep` | 34 KB | Audio normalization (16 kHz mono, denoise) |
| `bonfyre-transcribe` | 34 KB | Speech-to-text (Whisper) |
| `bonfyre-transcript-family` | 34 KB | Full transcription chain (intake → transcribe) |
| `bonfyre-transcript-clean` | 34 KB | Remove filler words, hallucinations |
| `bonfyre-paragraph` | 34 KB | Structure text into paragraphs |
| `bonfyre-brief` | 34 KB | Extract summary + action items |
| `bonfyre-narrate` | 34 KB | Text-to-speech (Piper TTS) |
| `bonfyre-proof` | 34 KB | Quality scoring + review |
| `bonfyre-pack` | 33 KB | Deliverable packaging (ZIP + manifest) |
| `bonfyre-compress` | 33 KB | File compression (zstd, async) |
| `bonfyre-embed` | 34 KB | Text embeddings (ONNX Runtime C API, trie tokenizer, `--insert-db`, `--input-dir` batch) |
| `bonfyre-vec` | 34 KB | Local vector search (sqlite-vec, SIMD cosine, `--exact`, `compare`) |
| `bonfyre-segment` | 33 KB | Speaker segmentation |
| `bonfyre-speechloop` | 33 KB | Live speech loop |
| `bonfyre-clips` | 33 KB | Audio clip extraction |
| `bonfyre-tone` | 33 KB | Tone/sentiment analysis (openSMILE) |
| `bonfyre-tag` | 33 KB | Topic tagging (fastText) |
| `bonfyre-canon` | 34 KB | Canonical artifact format (tree-sitter) |
| `bonfyre-query` | 33 KB | Artifact query engine |
| `bonfyre-repurpose` | 33 KB | Content repurposing |
| `bonfyre-emit` | 33 KB | Multi-format output (pandoc: HTML/PDF/EPUB/RSS) |
| `bonfyre-render` | 33 KB | Template rendering |
| `bonfyre-distribute` | 33 KB | Distribution + messaging (email, Slack, webhooks) |
| `bonfyre-project` | 33 KB | Project scaffolding |
| `bonfyre-mfa-dict` | 34 KB | MFA pronunciation dictionary |
| `bonfyre-weaviate-index` | 34 KB | Vector index (Weaviate semantic search) |

### Monetization

| Binary | Size | Purpose |
|---|---|---|
| `bonfyre-offer` | 33 KB | Dynamic pricing + proposal generation |
| `bonfyre-gate` | 33 KB | API key/tier validation (Free/Pro/Enterprise) |
| `bonfyre-meter` | 34 KB | Usage tracking + per-operation billing |
| `bonfyre-ledger` | 33 KB | Append-only financial records |
| `bonfyre-finance` | 51 KB | Service arbitrage, bundle pricing |
| `bonfyre-outreach` | 34 KB | Outreach tracking, follow-up routing |
| `bonfyre-pay` | 35 KB | Invoicing, payments, credits |

### Library

| Name | Size | Purpose |
|---|---|---|
| `liblambda-tensors` | 41 KB | Structural compression for JSON (static `.a` + shared `.so`) |

## Architecture

```
Audio File
  │
  ├─ bonfyre-ingest         Intake + validation
  ├─ bonfyre-media-prep     Normalize (16 kHz mono)
  ├─ bonfyre-hash           SHA-256 content addressing
  ├─ bonfyre-transcribe     Speech → text (Whisper)
  ├─ bonfyre-transcript-clean   Remove filler
  ├─ bonfyre-paragraph      Structure paragraphs
  ├─ bonfyre-brief          Summary + action items
  ├─ bonfyre-proof          Quality scoring
  ├─ bonfyre-offer          Pricing + proposal
  └─ bonfyre-pack           ZIP deliverable
         │
         ├── bonfyre-gate       Access control
         ├── bonfyre-meter      Usage tracking
         ├── bonfyre-pay        Billing
         └── bonfyre-distribute  Delivery
```

Or skip all that and run `bonfyre-pipeline run` for the unified 5–8 ms fast path.

## Docs

| Document | Description |
|---|---|
| [Architecture](docs/architecture.md) | System design, data flow, layer model |
| [Benchmarks](docs/benchmarks.md) | Detailed performance numbers |
| [Lambda Tensors](docs/lambda-tensors.md) | Compression algorithm explanation |
| [API Reference](docs/api.md) | HTTP endpoints (bonfyre-api) |
| [CMS Guide](docs/cms.md) | Using bonfyre-cms |
| [Pipeline Guide](docs/pipeline.md) | Audio processing pipeline |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). We welcome:

- Bug reports and fixes
- Performance improvements
- New binary ideas
- Documentation
- Language bindings (Node, Python, Rust, Go)
- Package manager ports (Homebrew, apt, AUR, etc.)

## License

[MIT](LICENSE) — do whatever you want with it.

Made by [Nick Gonzales](https://github.com/Nickgonzales76017).
