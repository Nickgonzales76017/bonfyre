# BonfyreQueue

WorkGraph-backed durable queue: enqueue under families, claim, attest runs
with provider sessions, acknowledge, and project per-account work, inbox,
and usage. The queue remembers; nothing runs twice by accident.

## Build, test, install

```sh
make -C cmd/BonfyreQueue
make -C cmd/BonfyreQueue test
make -C cmd/BonfyreQueue install MANAGED_PREFIX=~/.bonfyre
```

The build runs scope, inbox, wire, work-index, and dispatch suites,
including live shadow checks against copies of real owner stores.

## Use

```sh
bonfyre-queue enqueue TYPE INPUT [--mission ID] [--family F] [--db FILE]
bonfyre-queue claim --subject S --scope SCOPE [--mission ID] --db FILE
bonfyre-queue account-work index|read|usage|audit --db INDEX --subject S --scope SCOPE
bonfyre-queue account-inbox list|set|receipt --subject S --scope SCOPE --actor user:ID --db FILE
```

Enqueue files under a settable family (default `queue`); claim binds a
provider session to the attempt at run-attest time.

## Contract and limits

- A projected run is not provider usage or cost; usage comes only from
  Meter's provider rows joined by the exact session.
- An empty filtered claim is not an empty queue — it names the families
  that do hold ready work.
- A reverted operator action projects as cancelled, never completed.
- The command existing never authorizes a commit; Authority gates it.

No version is pinned and none is published as a package yet. The live
contract is `cmd-queue` in the Architecture Atlas.
