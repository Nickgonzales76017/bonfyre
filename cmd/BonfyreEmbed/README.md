# BonfyreEmbed

Text to vectors: ONNX-backed learned embeddings with hashed-token fallback
available only behind an explicit degraded flag. Refuses before it
misrepresents — a backend that cannot be honoured is a refusal, never a
silent substitution.

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
