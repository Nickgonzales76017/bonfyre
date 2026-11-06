# Local-first applications

Build software that works with local data, does its computing on the device and can later connect to external systems. Bonfÿre's storage, indexing, model, context and protocol components can be used independently or composed into a customer-facing application.

## Search three local notes

This example creates a SQLite index, finds a bicycle repair note from a question, updates the note and removes it. Each command opens the same file in a new process.

You need `bonfyre-embed`, `bonfyre-vec`, ONNX Runtime, the sqlite-vec extension and the installed `nomic-embed-text-onnx` model. Embed reads its `vocab.txt` and an ONNX model under `~/.cache/bonfyre/models/nomic-embed-text-onnx/onnx/`. It recognizes `model_qint8_arm64.onnx`, `model_O4.onnx` or `model.onnx`. Model setup is a prerequisite; the commands below make no model downloads.

From a configured source checkout, build the commands and select those binaries:

```sh Terminal
make -C cmd/BonfyreEmbed
make -C cmd/BonfyreVec
export PATH="$PWD/cmd/BonfyreEmbed:$PWD/cmd/BonfyreVec:$PATH"
```

Keep the following commands in the same shell so they share the workspace path. A fresh directory keeps repeat runs independent.

```sh Terminal
bonfyre_example=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-local-notes.XXXXXX")
printf '%s\n' "$bonfyre_example"

cat > "$bonfyre_example/notes.jsonl" <<'NOTES'
{"id":"flat-tire","text":"For a flat bicycle tire, remove the wheel, lift the tire bead with a tire lever, replace the inner tube and inflate the tire."}
{"id":"pottery","text":"The pottery workshop teaches clay shaping, glazing and kiln firing. Aprons and tools are supplied."}
{"id":"bakery","text":"The bakery accepts orders for sourdough bread and birthday cakes. Collect bread from the counter in the morning."}
NOTES

bonfyre-embed --input-jsonl "$bonfyre_example/notes.jsonl" \
  --model nomic-embed-text-onnx \
  --insert-db "$bonfyre_example/vector.sqlite"
bonfyre-vec count "$bonfyre_example/vector.sqlite"
```

The batch result should report `stored: 3`, `dims: 768`, `failed: 0`, `not_stored: 0` and `degraded: 0`. The count should read `artifacts: 3, vectors: 3`. Embed creates the database at the model's actual width. `bonfyre-vec init` creates a 384-dimensional store, so leave initialization to Embed for this 768-dimensional model.

## Ask a question

Use the same model for documents and questions. Generate the query file before searching:

```sh Terminal
printf '%s\n' 'How do I repair a punctured bicycle tire?' \
  > "$bonfyre_example/question.txt"
bonfyre-embed --text "$bonfyre_example/question.txt" \
  --model nomic-embed-text-onnx \
  --out "$bonfyre_example/question.json"
bonfyre-vec search "$bonfyre_example/vector.sqlite" \
  "$bonfyre_example/question.json" --top 3
bonfyre-vec search "$bonfyre_example/vector.sqlite" \
  "$bonfyre_example/question.json" --exact --top 3
```

Both searches should put `flat-tire` first. The default search includes the saved text, so an application can show the passage beside its ID. `--exact` scans stored vectors with cosine similarity. The default sqlite-vec store uses Euclidean distance; compare the returned IDs rather than treating the two distance values as the same metric. These search results identify passages; they do not generate a written answer.

## Change a note without adding a duplicate

Keep the same document ID when replacing its content:

```sh Terminal
printf '%s\n' \
  'For a punctured bicycle tire, inspect the tire for glass before fitting the replacement inner tube. Inflate it and check for leaks.' \
  > "$bonfyre_example/revised-note.txt"
bonfyre-embed --text "$bonfyre_example/revised-note.txt" \
  --model nomic-embed-text-onnx --doc-id flat-tire \
  --insert-db "$bonfyre_example/vector.sqlite"
bonfyre-vec count "$bonfyre_example/vector.sqlite"
bonfyre-vec search "$bonfyre_example/vector.sqlite" \
  "$bonfyre_example/question.json" --top 3
```

The count stays at three. The first result should contain the revised instruction about glass. Since the writer has exited before the next search starts, this also checks that the replacement survived reopening the database.

## Remove a note

Delete its exact ID, then search again through both paths:

```sh Terminal
bonfyre-vec delete "$bonfyre_example/vector.sqlite" flat-tire
bonfyre-vec count "$bonfyre_example/vector.sqlite"
bonfyre-vec search "$bonfyre_example/vector.sqlite" \
  "$bonfyre_example/question.json" --top 3
bonfyre-vec search "$bonfyre_example/vector.sqlite" \
  "$bonfyre_example/question.json" --exact --top 3
bonfyre-vec delete "$bonfyre_example/vector.sqlite" flat-tire
```

The first delete reports one vector and one artifact removed. The count becomes two, and neither search returns `flat-tire`. Repeating the delete reports zero removed. The source files remain in the example directory. A document indexed as several passages requires deleting each passage ID.

The remaining notes still appear in nearest-neighbor results, even though neither answers the bicycle question. Choose a relevance policy for your application before showing a result as an answer.

## Resolve setup errors

| Result | Next step |
|---|---|
| Model or vocabulary cannot be loaded | Check the installed model directory and ONNX file. Keep the document and query models identical. |
| sqlite-vec cannot be loaded | Point `BONFYRE_VEC_EXT` at the installed extension compatible with your SQLite build. |
| Store and query widths differ | Create a fresh index from the source notes with the chosen model, then regenerate the query. |
| `stored` is less than the source count | Read the batch's failure and vocabulary counters before querying. |
| A search returns unrelated notes | Inspect the source text and model coverage; nearest-neighbor search always ranks the available material. |

For your own corpus, retain stable IDs and the source text needed to rebuild the index. Changing embedding models requires re-embedding both documents and questions, even when both models happen to produce the same number of dimensions.

## Compose the application

An application may have several interfaces over its local state:

| Interface | Implementation role |
|---|---|
| C library | Storage, model and representation operations at the source |
| Bun | Development, integration and user-facing application tooling |
| Wasm | The same compiled code, callable from a browser where supported |
| SQLite and vector indexes | Local records and retrieval |
| Arrow / Parquet | Portable analytical representations and bulk material |
| WebAddress | Customer-specific pages, content and MCP endpoints |
| External API or Frappe | Optional interoperability, not a requirement for keeping your own data |

An application that runs in the browser should continue its permitted local work offline. Synchronization and remote execution can arrive later through typed changes and permission checks rather than silently replacing the local store.

## One set of permissions across interfaces

The same person may use the application in a browser, invoke a CLI command or call an MCP tool. A customer may act on their own records, not on every record reachable by the runtime.

Preserve the source identity, customer scope, operation type, time and evidence as work crosses interfaces. A partner can deploy the same application for multiple organizations without granting cross-customer data access.

## A component is also a product

Bonfÿre libraries should expose small stable interfaces when they solve independently useful problems: typed contracts, sqlite-vec and local storage, compression and model representations, audio pipelines, rendering, protocols, scheduling, finance and source-linked reconciliation.

The developer should not need the entire Aurekai deployment to use one library. A complete Aurekai operation can then combine those libraries with its customers, approvals, provider routes and commercial rules.

## Ship a working result

Use this checklist to complete a local-first example:

- Create a real local database and a real record.
- Run an operation through the actual component.
- Restart and prove the record and its identity survive.
- Render the result in a web, terminal or application view.
- Refuse a request made with a different customer's credentials.
- Record which external services, if any, were called.
- Verify the artifact's source, build and API version.

The example should include the minimal runnable source, dependency contracts and a clean start-to-finish session. Once that exists, the documentation can show its code, command output, WebAddress view and MCP interface from the same operation.

Continue with [models and compression](/models-and-compression) for running tensors directly, [integration and migration](/integration-and-migration) for external databases, or [white-label delivery](/white-label-delivery) for partner-operated applications.

## An additional retrieval path without a query-time transformer

The SQLite/ONNX walkthrough above remains its own supported implementation. StaticSemanticSearch adds another realization: `bonfyre-embed distill` creates a `.bfse` token model, the native `bf_static_embed` interface encodes text, and `site/matrix/static-embed.mjs` performs the corresponding browser operation. Its `.bfsi` indexes name the exact model. It does not reuse the walkthrough's 768-dimensional vectors as 128-dimensional static vectors.

WebAddress already uses the static implementation for published CMS sections and exports the model, index, passages and reader. Questions are encoded in the browser. This supplies a concrete path for the existing private document application without making a developer Mac its required public server, but that application's source import, passage notes, deletion and private persistence still need to be connected. No Documents migration or browser-only Training release is implied by the site-search deployment.

A receiving application must keep its original source bytes and passage coordinates, invalidate the right index when a source changes, and preserve annotations when search returns no useful result. Match the index to its model and its ordered passage companion. Retain a lexical path for exact names and make irrelevant or vocabulary-invisible queries explicit. Search similarity cannot verify a claim, grant access, interpret a shipment as delivered or publish somebody's private records.

The new native static operation also needs to exist in the selected installed binary. The October 18 UTC connector check found the managed Embed behind the checkout and returning old usage for `static`; until reviewed promotion, that interface cannot be described as available through ordinary native-family invocation.
