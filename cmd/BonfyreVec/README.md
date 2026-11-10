# BonfyreVec

Local vector search: sqlite-vec under pure C with SIMD cosine. A store is
created at one width — match it to the embedding model or every query is
refused by name.

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
