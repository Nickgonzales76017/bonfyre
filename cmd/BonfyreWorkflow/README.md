# BonfyreWorkflow

Workflow profile browser: operator-facing suites describing the shape of
work Bonfyre should perform. Profiles, not executable recipes.

## Build, test, install

```sh
make -C cmd/BonfyreWorkflow
make -C cmd/BonfyreWorkflow test
make -C cmd/BonfyreWorkflow install MANAGED_PREFIX=~/.bonfyre
```

Tests replay the command behavior vectors on small real fixtures.

## Use

```sh
bonfyre-workflow list [term] [--json] [--compact]
bonfyre-workflow show <WORKFLOW_ID | path/to/workflow.json>
bonfyre-workflow steps <WORKFLOW_ID | path/to/workflow.json>
```

## Contract and limits

- A profile describes work shape; it is not a recipe or a capability.
- The command existing never authorizes an effect.

No version is pinned and none is published as a package yet. The live
contract is `cmd-workflow` in the Architecture Atlas.
