# BonfyreMeter

Usage metering and billing: priced events, work-token reports, provider
isolation, and effective-dated valuation. No price without an admitted
rate — unknown stays unknown, never zero.

## Build, test, install

```sh
make -C cmd/BonfyreMeter
make -C cmd/BonfyreMeter test
make -C cmd/BonfyreMeter install MANAGED_PREFIX=~/.bonfyre
```

Suites cover priced and unpriced token reports, cross-account refusal,
idempotent replay, governed section comparison through the real Queue
CLI, and a private reconciled export replay where present.

## Use

```sh
bonfyre-meter record --key K --op OP --receipt REF --attempt-ref REF --realization-ref REF --at RFC3339
bonfyre-meter usage --key K [--since DATE]
bonfyre-meter work-costs --db METER --work-db WORK_INDEX --subject ACCOUNT --scope SCOPE --mission TASK
```

## Contract and limits

- A rate is never implied by an operation name; a priced event names
  the rate it was priced under.
- A projected run is not provider usage or cost.
- Reads write nothing; refusals leave no output.

No version is pinned and none is published as a package yet. The live
contract is `cmd-meter` in the Architecture Atlas.
