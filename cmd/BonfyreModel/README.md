# BonfyreModel

Local model registry and verification: manifests, sha256-verified weight
files, representation passports, and a memory ledger that says what fits
before anything loads.

## Build, test, install

```sh
make -C cmd/BonfyreModel
make -C cmd/BonfyreModel test
make -C cmd/BonfyreModel install MANAGED_PREFIX=~/.bonfyre
```

Tests validate manifests, verify exact bytes with mutation and refusal
cases, check representation passports, and judge a fixture ledger.

## Use

```sh
bonfyre-model setup <manifest.json>     # register, fetch, verify, readiness
bonfyre-model fetch <id>                # materialize a registered model
bonfyre-model local [--json]            # what fits on this machine
bonfyre-model list                      # models already known
```

Manifest replacement validates before mutation and keeps model identity
references; a new hash is reported stored only after its database update
succeeds.

## Contract and limits

- Computing a digest grants no provenance, no inference quality, and no
  permission to execute anything.
- A corrected size is the size of the bytes hashed, nothing more.
- `local` saying a model fits is not a promise it will load; memory moves
  between the reading and the request.

No version is pinned and none is published as a package yet. The live
contract is `cmd-model` in the Architecture Atlas.
