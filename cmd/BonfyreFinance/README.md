# BonfyreFinance

Service arbitrage, labor pipeline, and bundle pricing: the service book
behind every commerce grammar (`service add` with buy/sell prices), job
costing and review, opportunity ranking, and margin math.

## Build, test, install

```sh
make -C cmd/BonfyreFinance
make -C cmd/BonfyreFinance test
make -C cmd/BonfyreFinance install MANAGED_PREFIX=~/.bonfyre
```

Tests replay behavior vectors on small real fixtures in fresh temp dirs
plus the CRM and browser suites (the browser suites need their services).

## Use

```sh
bonfyre-finance [--db FILE] service add --name N --buy B --sell S --source SRC
bonfyre-finance [--db FILE] service list
bonfyre-finance [--db FILE] job log --service S --cost C --sold S [--quality Q]
bonfyre-finance [--db FILE] job list [--status S]
bonfyre-finance [--db FILE] margin
bonfyre-finance payment-options --root DIR --input JSON
```

Prices are buys and sells in the service book; Authority gates any commit
the command proposes.

## Contract and limits

- The command existing never authorizes an effect.
- Certification is blocked for want of a real input; `--help` answering
  is not a witness. Deeper work toward the full BonfyreFS-level contract
  is ongoing.

No version is pinned and none is published as a package yet. The live
contract is `cmd-finance` in the Architecture Atlas.
