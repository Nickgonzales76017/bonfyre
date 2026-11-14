# BonfyreEmbed

Text to vectors: ONNX-backed learned embeddings with hashed-token fallback
available only behind an explicit degraded flag. Refuses before it
misrepresents — a backend that cannot be honoured is a refusal, never a
silent substitution.

[Native C implementation](src/main.c) · [Search/index builder](../../scripts/build_semantic_index.sh) · [Vec storage](../BonfyreVec/README.md) · [vGPU + native editorial architecture](../../docs/native-data-compute-and-presentation.md)

## Build, test, install

```sh
make -C cmd/BonfyreEmbed
make -C cmd/BonfyreEmbed test
make -C cmd/BonfyreEmbed install MANAGED_PREFIX=~/.bonfyre
```

Tests exercise ONNX cache reuse, changed vocabulary, replaced model bytes,
missing-model refusal, store contention, and agreement against the real
teacher. Suites needing the declared local model say so and skip without it.

Known failure: `cache_identity.test.mjs` currently fails — a repeated
identical run prints no `cache hit`, and no new entry appears under
`~/.local/share/bonfyre/embeds/`, so the store path never fires for it.
Pre-existing in the checked-in source, unrelated to the install target.

## Use

```sh
bonfyre-embed --text <path> --out <path> [--backend onnx|hash] [--model <name>] [--dims <n>]
bonfyre-embed --input-dir <dir> --out <path>      # batch; loads the model once
bonfyre-embed --input-jsonl <file> --insert-db <db> [--doc-id <id>]
```

## Contract and limits

- A zero exit is not proof the requested representation was produced;
  check the receipt.
- A model name and vector width do not promise the same embedding
  geometry across runtime or asset changes.
- Neighbour overlap with the teacher is not retrieval quality.
- Corpus rows and computed embeddings carry no source identity,
  semantic truth, or authority on their own.

No version is pinned and none is published as a package yet. The live
contract is `cmd-embed` in the Architecture Atlas.

BonfyreEmbed runs a local text embedding model through the **ONNX Runtime C API** with BERT WordPiece tokenization and mean pooling. The deterministic hash backend is a different representation and must be named as such. An unavailable ONNX model, missing vocabulary or mismatched dimensions must not quietly produce a hashed vector and claim learned semantic search.

## More commands

~~~bash
# Use an installed ONNX model and its vocabulary; use the model's actual vector width.
bonfyre-embed --text ./passage.txt --out ./passage.json \
  --backend onnx --model ./models/actual-onnx-model

# Embed a source corpus in one session, preserving row IDs.
bonfyre-embed --input-jsonl ./corpus.jsonl --out ./vectors.json \
  --backend onnx --model ./models/actual-onnx-model \
  --text-field text --id-field id

# Explicitly opt in to a degraded/hash backend only when the application permits it.
bonfyre-embed --text ./passage.txt --out ./hashed.json \
  --backend hash --allow-degraded
~~~

Additional owner operations include `bonfyre embed pack`, `stat`, `gc`, `branch list/add/delta/apply`, `index`, `index-q8`, `nearest`, `merge`, `agree`, `tag`, `refs` and `log`. They are **separate** object store, search and representation operations, not one general-purpose “embedding capability.”

The [source's `--input-jsonl` implementation](src/main.c) uses native JSON handling with `--text-field` and `--id-field` defaults; it supports inserting into an identified vector store and optionally pruning invisible rows. The local `build_semantic_index.sh` script produces a typed corpus from retained items, runs the ONNX backend and may emit a Parquet representation for browser/columnar consumers.

## Identity, dimensions and downstream use

~~~mermaid
flowchart LR
  SOURCE["Source Library / publications / transcripts / documents"] --> TEXT["Stable source key + exact retained text"]
  TEXT --> MODEL["ONNX + BERT tokenizer + pooling"]
  MODEL --> RECEIPT["Model/vocab/runtime digest, actual dimensions and backend"]
  MODEL --> VECTORS["Float vectors with source keys"]
  VECTORS --> SQLITE["BonfyreVec with matching width"]
  SQLITE --> RESULT["Bounded nearest passages and citations"]
  RESULT --> EDITOR["Research table / Editor.js / Puck / document editor"]
  EDITOR --> PUBLISH["Reviewed source-linked article, account work or export"]
~~~

The vector's actual *geometry* depends on its full model/vocab/backend/normalization identity. The default 384-dimensional MiniLM examples and 768-dimensional Nomic paths do not share a ranking or distance threshold. An older cache, same model name, hashed-token fallback or a mixed-width store cannot constitute retrieval parity.

The [model-cache identity test](tests/cache_identity.test.mjs) checks reuse, changed vocabulary, replaced model bytes and missing model refusal. Tokenizer-visibility and real ONNX tests are distinct. The archive Atlas also notes remaining external-data-file asset hashing work; full model sidecar identity must be verified before broad caching claims.

## What vGPU, BitNet and FPQ do next

- [BonfyreVec](../BonfyreVec/README.md) stores/searches same-representation vectors. It preserves source IDs and can use an exact SIMD path.
- [vGPU kin](../BonfyreSurfaceScene/runtime/README.md) compares rows in one chosen chart and materializes related neighbors for downstream pages. It is not automatically reading the current BonfyreEmbed corpus; dimensions and source keys must match.
- [BitNet](../BonfyreBitNet/src/main.c) can reorder an exact eligible AddressPlane frontier under a separate passport/quality gate; it does not add or authorize sources.
- [FPQ](../BonfyreFPQ/README.md) measures compressed model *weights*. A high tensor reconstruction cosine does not ensure the ONNX embed model or Qwen generator preserves retrieval/task quality.

**Useful end result:** local semantic evidence that readers can inspect in a proper table and carry into a real publication, source review, research note or installed account app with the current owner's permissions. The retrieval backend does not turn a source similarity into a confirmed fact.
