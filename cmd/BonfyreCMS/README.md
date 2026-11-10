# BonfyreCMS

Stripped-binary content store: namespaced entries, atomic page writes with
operation history, guarded conditional updates, and pack images. SQLite
does the quoting; every value stays in its column.

## Build, test, install

```sh
make -C cmd/BonfyreCMS
make -C cmd/BonfyreCMS test
make -C cmd/BonfyreCMS install PREFIX=~/.bonfyre
```

The build runs its witness suites: hostile input, concurrent writers,
namespace isolation, rollback paths, and pack lifecycle.

## Use

```sh
bonfyre-cms entry create <type> [--ns NS] '<json>'
bonfyre-cms entry update <type> <id> '<json>' [--ns NS]
bonfyre-cms entry page-write @file.json --ns NS
bonfyre-cms entry import-jsonl <type> <file.jsonl> [--mode create|update]
bonfyre-cms schema list|apply|migrate [--db FILE]
bonfyre-cms serve [--port PORT] [--db FILE]
```

## Contract and limits

- An entry update is as large as its fields allow; a guarded update
  compares its condition in the same SQL statement, and a failed
  condition leaves content and history unchanged.
- A page write commits typed rows, fragments, and history together or
  discards all of it; failed writes emit no publication notifications.
- An atomic page transaction is not durable external effects, delivered
  notifications, or a verified deployment.
- The command existing never authorizes a commit; Authority gates it.

No version is pinned and none is published as a package yet. The live
contract is `cmd-c-m-s` in the Architecture Atlas.
