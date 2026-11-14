# BonfyreVec

Local vector search: sqlite-vec under pure C with SIMD cosine. A store is
created at one width — match it to the embedding model or every query is
refused by name.

[Native C implementation](src/main.c) · [Exact deletion/rollback test](tests/delete_test.sh) · [ONNX embeddings](../BonfyreEmbed/README.md) · [vGPU numeric compute](../BonfyreSurfaceScene/runtime/README.md) · [Complete source-to-frontend guide](../../docs/native-data-compute-and-presentation.md)

## Build, test, install

```sh
make -C cmd/BonfyreVec
make -C cmd/BonfyreVec test
make -C cmd/BonfyreVec install MANAGED_PREFIX=~/.bonfyre
```

Tests run against fresh temp stores: width admission on both search paths,
exact multi-passage removal with rollback, persistence across processes.
No model or network needed.

## Use

```sh
bonfyre-vec init <db> [--dims N]     # default 384; changing models means a new store
bonfyre-vec insert <db> <embeddings.json>
bonfyre-vec delete <db> <exact-id> [exact-id ...]
bonfyre-vec search <db> <query.json> [--top N] [--exact]
bonfyre-vec compare <db> <id1> <id2>
bonfyre-vec count <db>
```

Delete binds exact passage IDs and removes vector and metadata rows in one
transaction; a missing database is never created, an empty ID is refused,
and absent IDs are harmless retries.

## Contract and limits

- Deleting passages never securely erases source files, embedding caches,
  backups, or database free pages.
- Document removal needs the caller to enumerate every indexed passage ID;
  no automatic source-to-passage removal is claimed.
- The command existing never authorizes a read or a mutation; scope still
  applies.

No version is pinned and none is published as a package yet. The live
contract is `cmd-vec` in the Architecture Atlas.

BonfyreVec uses the **SQLite C API and sqlite-vec's `vec0` loadable extension** for local vector retrieval. It also includes a separate **exact SIMD cosine** path, a persisted nearest-neighbor operation and source-specific passage deletion. The data lives in a local SQLite file, not a hosted vector service.

## More commands

~~~bash
bonfyre-vec init ./source-vectors.db --dims 384
bonfyre-vec insert ./source-vectors.db ./embeddings.json
bonfyre-vec search ./source-vectors.db ./query.json --top 10
bonfyre-vec search ./source-vectors.db ./query.json --exact --top 10
bonfyre-vec neighbors ./source-vectors.db --top 8
bonfyre-vec compare ./source-vectors.db passage-a passage-b
bonfyre-vec count ./source-vectors.db
bonfyre-vec delete ./source-vectors.db passage-a passage-b
bonfyre-vec status
~~~

**Width is part of the store contract.** `--dims` defaults to 384, but 768-dimensional learned embeddings require a 768-dimensional store and complete-width queries. The native source records a fixed-384 query buffer bug that truncated 768-dimensional searches; query paths now account for the stored width rather than assuming the initialization default is universal. When changing models or dimensions, create or migrate an independently verified store.

**Extension requirement:** `BONFYRE_VEC_EXT` overrides the `sqlite-vec` loadable-library path. The native code checks common macOS and Linux locations and refuses when the required extension cannot be loaded. The exact SIMD path and extension-based nearest search have different physical execution and performance envelopes.

## Stable passages, deletion and verification

The [delete test](tests/delete_test.sh) proves exact multi-passage removal through both search paths and checks persistence across fresh processes. A failed metadata deletion rolls back the **vector and artifact** updates together. This does not imply automatic source-to-passage discovery: the caller must enumerate the source's exact indexed passage IDs. It is not a claim of secure physical erasure of all storage blocks.

~~~mermaid
flowchart LR
  TEXT["Original retained source and passage keys"] --> EMBED["BonfyreEmbed: one known ONNX representation"]
  EMBED --> VEC["BonfyreVec local SQLite store, exact width"]
  VEC --> SEARCH["ANN/exact cosine or all-item neighbors"]
  SEARCH --> REVIEW["Source-linked result rows with score/backend"]
  REVIEW --> PUBLICATION["Reviewed research / article / workbook / installed app"]
  PUBLICATION --> OUTPUT["Native frontend, export or narration as permitted"]
~~~

For a publisher's archive, the useful output is a bounded set of source-linked reading candidates. A high cosine score does not show that two publications report the same event; a current URL or license needs to be checked by its owner. A downstream editor must not copy a whole private vector database into a public page. Results should carry exact row keys and the model/representation identity so a refreshed source or changed model cannot masquerade as the same search.

## How this differs from vGPU, BitNet and FPQ

The [vGPU kin kernel](../BonfyreSurfaceScene/runtime/kin-compute-vgpu.mjs) performs tiled pairwise similarity over one prepared numeric chart and persists neighbor rows via its compiler. It is **numeric acceleration**, with CPU fallback and a backend receipt; it is not the same as the sqlite-vec engine. Large tables can also use DuckDB-Wasm/Parquet SQL, unrelated to whether their source text has semantic vectors.

[BitNet](../BonfyreBitNet/src/main.c) ranks an already eligible candidate frontier under a separate quality/passport contract. [FPQ](../BonfyreFPQ/README.md) compresses model weights, not a SQL result set. Neither automatically changes the semantics or costs of the BonfyreVec store.

**Data-to-frontend acceptance:** prove that a real query over a large source cohort reaches the right bounded table, opens the correct retained passage, can enter a versioned authoring record, and appears in a released page or document only with a current authorization. GPU speed alone, an ANN top-k list alone, and the existence of an MCP tool are different kinds of evidence.
