# BonfyreBridge

Move records between systems and prove what arrived: whole-table copies
with read-back comparison, schema mapping from cited type rules, and
verification that re-checks rows, checksums, types, and constraints.

## Build, test, install

```sh
make -C cmd/BonfyreBridge
make -C cmd/BonfyreBridge test
make -C cmd/BonfyreBridge install MANAGED_PREFIX=~/.bonfyre
```

## Use

```sh
bonfyre-bridge db --from URL --to URL [--tables a,b] [--replace]
bonfyre-bridge db-verify --from URL --to URL [--tables a,b]
bonfyre-bridge schema-map --from sqlite:PATH --plan plan.json --rules rules.json --out mapping.json
bonfyre-bridge map-verify --from sqlite:PATH --to URL --mapping mapping.json --out diff.json [--expect diff.json]
```

URL: `postgres://`, `mysql://`, `sqlite:`, or `env:NAME` to keep a password
off the command line. A table has arrived when its row count and a digest
over every value agree on both sides.

## Contract and limits

- A matching digest is agreement of the compared values, not proof the
  source system is correct.
- `db-verify` writes nothing; it only compares.
- The command existing never authorizes a read or a mutation; scope
  still applies.

No version is pinned and none is published as a package yet. The live
contract is `cmd-bridge` in the Architecture Atlas.
