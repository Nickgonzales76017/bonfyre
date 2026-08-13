# Bonfyre Quickstart

This is the fastest real path to see Bonfyre working today.

The current front door is:

```text
capture -> probe -> artifacts -> recipe -> stitch
```

## 1. Build Bonfyre

```bash
git clone https://github.com/Nickgonzales76017/bonfyre-oss.git
cd bonfyre-oss
make
```

If you want the unified CLI on your path:

```bash
make install
```

## 2. Populate The Command And Registry Surfaces

Run this once after a fresh clone, and again whenever `cmd/`, `lib/`, or registry-related code changes land.

```bash
./cmd/BonfyreCLI/bonfyre doctor sync-subcommands
./cmd/BonfyreIndex/bonfyre-index layers --root layeros/state
./cmd/BonfyreCLI/bonfyre list --health
./cmd/BonfyreWorkflow/bonfyre-workflow list
./cmd/BonfyreRecipe/bonfyre-recipe list
./cmd/BonfyreLayer/bonfyre-layer registry --root layeros/state
```

What that does:

- syncs repo-built subcommands into the CLI surface
- refreshes the layer index projection
- confirms command health labels
- confirms workflow, recipe, and layer registries are populated

## 3. Inspect BonfyreWire

Start by looking at the current operator surface:

```bash
./cmd/BonfyreWire/bonfyre-wire --help
./cmd/BonfyreCLI/bonfyre wire doctor
```

`BonfyreWire` can ingest PCAPs or synthetic captures, fingerprint devices, materialize canonical artifacts, and generate stitch-ready recipes.

## 4. Run The Discovery Pipeline

Using a capture file:

```bash
./cmd/BonfyreWire/bonfyre-wire ingest-pcap capture.pcap --dumb-device --root layeros/state
./cmd/BonfyreWire/bonfyre-wire probe <capture_id> --root layeros/state
./cmd/BonfyreWire/bonfyre-wire artifacts <capture_id> --root layeros/state
./cmd/BonfyreWire/bonfyre-wire recipe <capture_id> --root layeros/state > recipe.json
./cmd/BonfyreStitch/bonfyre-stitch plan recipe.json
```

Using the unified CLI:

```bash
bonfyre wire ingest-pcap capture.pcap --dumb-device --root layeros/state
bonfyre wire probe <capture_id> --root layeros/state
bonfyre wire artifacts <capture_id> --root layeros/state
bonfyre wire recipe <capture_id> --root layeros/state > recipe.json
bonfyre stitch plan recipe.json
```

What you get:

- `probe`: device fingerprinting and chain suggestions
- `artifacts`: canonical `BfArtifact` JSON files under `layeros/state/wire/artifacts/<capture_id>/`
- `recipe`: a stitch-compatible recipe you can inspect, save, and replay

## 5. Useful Truth Checks

Check the command surface:

```bash
bonfyre list --health
```

Check registry density:

```bash
bonfyre status registries --root layeros/state
```

Check workflows:

```bash
bonfyre workflow list
```

Check recipes:

```bash
bonfyre recipe list
```

## 6. State You Should Expect

Bonfyre uses two important state surfaces:

- `layeros/state`
  - LayerArtifact, graph, queue, and wire operating state
- `~/.local/share/bonfyre/catalog.db`
  - smaller catalog used by workflow, recipe, family, and model browsing surfaces

If the CLI looks stale after updating code, rerun the registry population commands from step 2.

## 7. Next Docs

- [README.md](README.md)
- [docs/bonfyre_wire.md](docs/bonfyre_wire.md)
- [docs/bonfyre_status_and_drift.md](docs/bonfyre_status_and_drift.md)
- [docs/architecture.md](docs/architecture.md)
