# Bonfÿre quickstart

Choose the shortest path that answers your question. You do not need to build
the whole estate to understand the machine.

## Path A — inspect architecture and maturity

This is the safest first path. The Architecture Atlas is the canonical live
registry; its views are reversible and every nontrivial maturity claim points
to source or a witness.

```bash
git clone https://github.com/Nickgonzales76017/bonfyre-oss.git
cd bonfyre-oss

python3 architecture/atlas.py validate
python3 architecture/atlas.py expand ExecutionView
python3 architecture/atlas.py expand CapabilityView
python3 architecture/atlas.py expand EvidenceView
python3 architecture/atlas.py get work-graph
python3 architecture/atlas.py loss
```

Read maturity literally:

- `architectural` or `declared` names a contract or intended organ.
- `implemented` has a real source path.
- `measured` or `proven` requires a witness.
- A public capability identity is not necessarily installed, callable, or
  workload-proven.

## Path B — run the semantic evidence

The Python control plane is frozen as a reference implementation and
conformance oracle. Running it verifies WorkGraph, capability, authority,
provider/resource, evidence, Frappe, and replay semantics without pretending it
is the production runtime.

```bash
cd components/BonfyreControlPlane
python3 -m pytest tests/ -q
```

Useful focused checks:

```bash
python3 -m pytest tests/test_work_and_events.py -q
python3 -m pytest tests/test_capability_closure.py -q
python3 -m pytest tests/test_frappe_native_records.py -q
python3 -m pytest tests/test_evidence_graphs.py -q
```

## Path C — build and inspect native powers

```bash
make
./cmd/BonfyreCLI/bonfyre doctor sync-subcommands
./cmd/BonfyreIndex/bonfyre-index layers --root layeros/state
./cmd/BonfyreCLI/bonfyre list --health
./cmd/BonfyreWorkflow/build/bonfyre-workflow list
./cmd/BonfyreRecipe/build/bonfyre-recipe list
./cmd/BonfyreLayer/bonfyre-layer registry --root layeros/state
```

The health output matters more than a fixed binary count. Bonfÿre separates a
defined Power from a built, resolvable, health-probed, or workload-proven
realization.

Two state surfaces are relevant:

- `layeros/state` holds LayerArtifact, graph, queue, and wire operating state.
- `~/.local/share/bonfyre/catalog.db` supports smaller workflow, recipe, family,
  and model browsing projections.

After `cmd/`, `lib/`, or registry changes, repeat the sync and index commands.

## Path D — turn an authorized observation into a replayable plan

Use a PCAP you own or are authorized to inspect:

```bash
./cmd/BonfyreWire/bonfyre-wire ingest-pcap capture.pcap --dumb-device --root layeros/state
./cmd/BonfyreWire/bonfyre-wire probe <capture_id> --root layeros/state
./cmd/BonfyreWire/bonfyre-wire artifacts <capture_id> --root layeros/state
./cmd/BonfyreWire/bonfyre-wire recipe <capture_id> --root layeros/state > recipe.json
./cmd/BonfyreStitch/bonfyre-stitch compile recipe.json --output recipe-program
```

This demonstrates a small but real observation-to-program path:

```text
observation → typed artifacts → recipe → compiled composition program
```

Inspect the recipe before compilation or execution. A generated program is not authority to
perform an effect.

## Path E — inspect institutional projections

The nine Frappe products are installed under `integrations/frappe-bench/apps`.
Their DocType grammars are extracted by `BonfyreFrappeCompiler` and witnessed
against shared Bonfÿre semantics.

```bash
python3 components/BonfyreControlPlane/witness_frappe_apps.py
cd components/BonfyreControlPlane
python3 -m pytest tests/test_frappe_native_records.py tests/test_app_surface_witnesses.py -q
```

## Path F — inspect a model/runtime surface

Model Commons is larger than one model or quantizer. Begin by inspecting the
live Model view and a native command's contract:

```bash
python3 architecture/atlas.py expand ModelView
python3 architecture/atlas.py get fpq-transform
make -C cmd/BonfyreFPQ
./cmd/BonfyreFPQ/bonfyre-fpq --help
```

## Next docs

- [README](README.md)
- [Architecture](docs/architecture.md)
- [Architecture Atlas](architecture/README.md)
- [Status and drift](docs/bonfyre_status_and_drift.md)
- [Wire](docs/bonfyre_wire.md)
- [Pages runtime](docs/pages-runtime.md)
