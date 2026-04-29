# Bonfyre Status And Drift

Bonfyre now distinguishes between command dispatch availability, command health, and registry density.

## Meanings

`ready`
- The dispatcher found a runnable binary path for the command.

`healthy`
- The selected command responded to a lightweight `status` or `--help` probe.

`repo`
- The active command path is a repo-built binary under `cmd/...`.

`installed`
- The active command path is an installed binary, usually under `~/.local/bin`.

`stale`
- An installed binary differs from the repo-built binary for the same command name.

## Registry Layers

Bonfyre currently has two different state surfaces that matter day to day:

`layeros/state`
- Large LayerArtifact and DisCIPL operating state.
- Includes `layers.db`, `index.db`, `discipl.db`, `graph.db`, and `queue.db`.

`~/.local/share/bonfyre/catalog.db`
- Smaller home catalog used by older family/model/recipe browsing surfaces.

The two are related, but they are not the same inventory.

## Useful Commands

Populate the command and registry surfaces after pulling new runtime code:

```bash
make
bonfyre doctor sync-subcommands
bonfyre-index layers --root layeros/state
bonfyre list --health
bonfyre workflow list
bonfyre recipe list
bonfyre layer registry --root layeros/state
```

Use the direct binary paths instead of `bonfyre ...` if you are validating a fresh build before install.

Inspect registry density:

```bash
bonfyre status registries --root layeros/state
bonfyre status registries --root layeros/state --json
```

Inspect command drift:

```bash
bonfyre status commands
bonfyre status commands --json
```

Write a deep operational snapshot:

```bash
bonfyre status snapshot --root layeros/state --out /tmp/bonfyre_ops_deep
```

Preview repo-to-install sync without changing anything:

```bash
bonfyre doctor sync-subcommands --dry-run
```

Synchronize matching repo-built subcommands into `~/.local/bin`:

```bash
bonfyre doctor sync-subcommands
```

Show health labels in the command surface:

```bash
bonfyre list --health
bonfyre list --compact --health
bonfyre list --json --health
```

## Notes

When you run `bonfyre` from inside the Bonfyre repo, the dispatcher prefers repo-built binaries where possible. This reduces command drift during development and makes the command surface reflect the current working tree more accurately than the installed fleet alone.
