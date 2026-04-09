<p align="center">
  <h1 align="center">🔥 Bonfyre</h1>
  <p align="center">
    <strong>48 static C binaries. Pure C11. A complete backend platform.</strong>
  </p>
  <p align="center">
    <a href="#install">Install</a> ·
    <a href="#use-cases">Use Cases</a> ·
    <a href="#benchmarks">Benchmarks</a> ·
    <a href="#all-48-binaries">All 48 binaries</a> ·
    <a href="#docs">Docs</a> ·
    <a href="#contributing">Contributing</a>
  </p>
</p>

---

```bash
git clone https://github.com/Nickgonzales76017/bonfyre.git && cd bonfyre && make
```

That builds 48 binaries. No Node.js. No Python. No Docker. No npm. Just C11 and SQLite.

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
bonfyre-transcribe run interview.wav --out transcript.json  # → local Whisper + HCP
bonfyre-brief generate transcript.json --out summary.md     # → executive summary
```

Everything runs on your machine. No OpenAI key. No internet. No per-minute billing.

**HCP v3.2** — quad-channel spectral refinement (acoustic + morphological + bigram + trigram semantic), KIEL-CC Kalman innovation, E-T Gate audio-text verification, formant anchoring, morphological logit bias (active decoder constraint), and context-seeded constrained re-decode. Unified FFT pass cuts audio analysis overhead 86%. Pushes Whisper quality to **0.999** on base and **0.997** on tiny. Nine-layer hallucination detection. Multi-model support (tiny/base/small/medium/large). 6 quantization options (q4_0/q4_1/q5_0/q5_k/q8_0/fp16). [Open-source (MIT)](https://github.com/Nickgonzales76017/hcp-whisper).

Drop-in OpenAI replacement: `bonfyre-proxy serve --port 8787` — set `OPENAI_API_BASE=http://localhost:8787` and existing code works unchanged.

| | Deepgram | OpenAI Whisper API | **Bonfyre + HCP v3.2** |
|---|---|---|
| Cost | $0.006/min | $0.006/min | **$0/min** |
| Quality | ~0.85–0.90 | ~0.87 (base) | **0.999** (base) / **0.997** (tiny) |
| Hallucination detection | None | None | **9-layer + morphological logit bias** |
| Overhead | N/A | N/A | **<1% of decode (unified FFT)** |
| Privacy | Cloud | Cloud | **100% local** |

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
make            # builds all 48 binaries + libbonfyre + liblambda-tensors
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

### Transcription quality (BonfyreTranscribe v3.1 + HCP)

| Model | Avg Quality | Avg Uplift | Hallucination Rate |
|---|---|---|---|
| base.en + HCP | **0.999** | **+8.2%** | 0.09% |
| tiny.en + HCP | **0.997** | **+9.3%** | 0.00% |

Tiny + HCP v3.1 achieves 0.997 — within 0.1% of base, at ~5× faster decode.

| Metric | Before | After (HCP v3.1) | Change |
|---|---|---|---|
| Quality score (base avg) | 0.923 (Whisper base) | **0.999** (HCP refined) | **+8.2%** |
| Quality score (tiny avg) | 0.913 (Whisper tiny) | **0.997** (HCP refined) | **+9.3%** |
| Hallucinated segments | undetected | **1 / 1,096 (0.09%)** | 9-layer detection |
| HCP post-processing | N/A | **<18 ms** (720 s audio) | near-zero overhead |

> Complex-Domain Hierarchical Constraint Propagation: quad-channel complex lifting (acoustic + morphological + bigram + trigram semantic), radix-2 FFT, three-band adaptive spectral filter with Dirichlet anomaly detection, formant anchoring, context-seeded constrained re-decode. Pure C11, no external math library.

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

### P4 optimizations (shipped)

Architecture-level hardening across all binaries:

| Optimization | Impact | Details |
|---|---|---|
| BonfyreTel hardening | **Crypto-safe session IDs, 0-latency TCP** | `arc4random` replaces `rand()`, `TCP_NODELAY` on all sockets, scan_offset O(1) ESL header lookup |
| BonfyreAPI robustness | **Crash-proof under load** | `SIGPIPE` ignored, pre-spawned thread pool, `vasprintf` safe formatting, heap buffers for large requests |
| libbonfyre FNV hash | **O(1) operator lookup** | FNV-1a hash table replaces linear scan of 48-operator registry |
| BonfyrePipeline dedup | **SHA-256 content dedup** | 279-line dedup engine — skips already-processed artifacts in pipelines |
| Build system | **PGO targets, CFLAGS propagation** | `make pgo-gen` / `make pgo-use` for profile-guided optimization; root flags reach all sub-makes |

### P5 optimizations (shipped)

Pure syntax/datatype wins — zero behavioral change, massive throughput gains:

| Optimization | Impact | Details |
|---|---|---|
| Hex LUT (`snprintf("%02x")` → table lookup) | **~10× faster** hash-to-hex | Replaced across 7 call sites in 5 files |
| BfArtifact struct shrink | **1,076 → 536 bytes per struct** | `artifact_id[512→128]`, `created_at[128→32]`, `root_hash[128→68]` — doubles L2 cache density |
| Auth `generate_token` | **O(n²) → O(n)** | Eliminated strlen-in-loop + sprintf; tracked offset with hex LUT |
| `strlen()` on constants → `sizeof()-1` | **Compile-time everywhere** | 10+ call sites replaced with `BF_MAGIC_LEN` / `sizeof()` / literal `6` |
| `bf_artifact_compute_keys` | **Cached strlen** | 3 variables eliminate 2 redundant full-string scans per call |
| `bf_read_file` raw syscalls | **−2 syscalls, zero stdio overhead** | `open/fstat/read/close` replaces `fopen/fseek/ftell/fread/fclose` |
| `op_cost` dispatch | **strcmp chain → switch(op[0])** | Single char dispatch replaces 10 sequential `strcmp()` calls |
| `memset` via `offsetof` | **Only zero header, not full struct** | Avoids wiping 1,000+ bytes that get immediately overwritten by struct copy |
| Graph JSON assembly | **snprintf chain → memcpy** | `CPLIT` macro + direct `memcpy` — ~5× faster node hash computation |

### Cumulative results (measured, Apple M-series)

| Operation | Before P0 | After P5 | Speedup |
|---|---|---|---|
| Single embed | ~600 ms (Python) | **237 ms** (C + ONNX) | **2.5×** |
| 10-file embed | ~6,000 ms (10 × Python) | **386 ms** (batch) | **15.5×** |
| 10-file embed + DB insert | ~7,000 ms+ | **689 ms** (batch + pooled DB) | **10×** |
| Vector file (384-dim) | 6.4 KB JSON | **1,544 bytes** VECF | **4.2× smaller** |
| Vec exact search (10 docs) | N/A | **5 ms** (NEON SIMD) | new |
| Pipeline (6 stages) | 76 ms (10 binaries) | **8 ms** (unified + dedup) | **9.5×** |
| BonfyreTag inference | ~150 ms (Python subprocess) | **6 ms** (pure C) | **25×** |
| Hash hex conversion | ~100 ns/call (snprintf) | **~10 ns/call** (LUT) | **~10×** |
| Artifact struct memory | 1,076 bytes | **536 bytes** | **2× cache density** |
| Auth token generation | O(n²) (strlen loop) | **O(n)** (tracked offset) | algorithmic |
| Operator registry lookup | O(n) linear scan | **O(1)** (FNV hash table) | algorithmic |
| Duplicated utility code | 34 copies across binaries | **1 each** (libbonfyre) | eliminated |
| Binaries needing Python | 5 | **3** | 40% reduction |
| All 48 binaries disk | — | **~2.1 MB total** | — |
| Test suite | — | **69 tests, all pass** | — |

## Examples

Standalone repos you can clone and run independently. Each builds only the binaries it needs.

| Repo | What it does | Time to run | CEO pitch |
|---|---|---|---|
| **[quickstart](https://github.com/Nickgonzales76017/bonfyre-example-quickstart)** | Clone → build → CMS + embed + search | 30 sec | "Entire backend in 2 MB" |
| **[semantic-search](https://github.com/Nickgonzales76017/bonfyre-example-semantic-search)** | Embed 20 docs, search by meaning | 5 min | "Replace $250/mo Pinecone — local, 5 ms queries" |
| **[transcribe](https://github.com/Nickgonzales76017/bonfyre-example-transcribe)** | Audio → transcript → summary → tags → ZIP | 5 min | "Replace $0.006/min Deepgram — $0, offline, private" |
| **[compress](https://github.com/Nickgonzales76017/bonfyre-example-compress)** | Compress 10K JSON records, random access | 2 min | "13.5% of JSON with O(1) field access" |
| **[saas-stack](https://github.com/Nickgonzales76017/bonfyre-example-saas-stack)** | Auth + billing + API gateway + CMS | 5 min | "Replace $2,500/mo SaaS stack — 240 KB, zero vendors" |
| **[creator-starter](https://github.com/Nickgonzales76017/bonfyre-creator-starter)** | Fork → drop audio → publish via Actions | 2 min | "Ship a transcript site without writing code" |
| **[recipes](https://github.com/Nickgonzales76017/bonfyre-recipes)** | Ready-made pipeline recipes for common workflows | — | "Copy-paste pipelines for transcription, search, summarization" |

```bash
# Pick one and go:
git clone https://github.com/Nickgonzales76017/bonfyre-example-semantic-search.git
cd bonfyre-example-semantic-search
./setup.sh && ./run.sh
```

## All 49 binaries

### Infrastructure

| Binary | Size | Purpose |
|---|---|---|
| `bonfyre-cms` | 299 KB | Content management system with Lambda Tensors compression |
| `bonfyre-api` | 69 KB | HTTP gateway, file upload, job management, static server |
| `bonfyre-auth` | 35 KB | User signup/login, session tokens, SHA-256 passwords |
| `bonfyre-proxy` | 53 KB | OpenAI-compatible API shim (drop-in replacement for /v1/audio/transcriptions, /v1/chat/completions) |
| `bonfyre-index` | 68 KB | SQLite artifact index + full-text search |
| `bonfyre-graph` | 51 KB | Merkle-DAG artifact graph, SHA-256 content addressing |
| `bonfyre-runtime` | 33 KB | Runtime environment, process lifecycle |
| `bonfyre-hash` | 34 KB | Pure C SHA-256 (FIPS 180-4), content addressing |
| `bonfyre-tel` | 68 KB | FreeSWITCH ESL telephony adapter (SIP/RTP, call routing) |

### Orchestration

| Binary | Size | Purpose |
|---|---|---|
| `bonfyre-pipeline` | 51 KB | Unified in-process pipeline (5–8 ms end-to-end) |
| `bonfyre-orchestrate` | 45 KB | Machine-only orchestration planner with optional Gemma 4 assist |
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
| `bonfyre-narrate` | 68 KB | Verified TTS: inline FFT, 27-feature fingerprint, 6-layer fidelity scoring, closed-loop verification, zero external deps |
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

## Gemma 4 orchestration

`bonfyre-orchestrate` is a strict machine-only planner layer. End users never see prompts. Bonfyre keeps its deterministic baseline without a model, and when you configure a Gemma 4 endpoint the orchestrator can select extra Bonfyre blocks that improve quality and leverage without replacing the existing fast path.

```bash
bonfyre-orchestrate template audio > orchestrate-request.json
bonfyre-orchestrate plan orchestrate-request.json
BONFYRE_ORCHESTRATE_ENDPOINT=http://127.0.0.1:8000/v1/chat/completions \
BONFYRE_ORCHESTRATE_MODEL=google/gemma-4-E4B \
bonfyre-orchestrate plan orchestrate-request.json --out orchestrate-plan.json
```

## Docs

| Document | Description |
|---|---|
| [Architecture](docs/architecture.md) | System design, data flow, layer model |
| [Benchmarks](docs/benchmarks.md) | Detailed performance numbers |
| [Lambda Tensors](docs/lambda-tensors.md) | Compression algorithm explanation |
| [API Reference](docs/api.md) | HTTP endpoints (bonfyre-api) |
| [CMS Guide](docs/cms.md) | Using bonfyre-cms |
| [Pipeline Guide](docs/pipeline.md) | Audio processing pipeline |
| [Orchestrate Guide](docs/orchestrate.md) | Gemma 4 machine-only planning layer |

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
