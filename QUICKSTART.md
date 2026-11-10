# Bonfÿre quickstart

Choose the shortest path that answers your question. You do not need to build
the whole estate to understand the machine.

To see finished work before building anything, open the
[work samples](https://aurekai-work-samples.pages.dev/) or the
[guides](https://docs.aurekai.work/).

## Path A — inspect architecture and maturity

This is the safest first path. The Architecture Atlas is the canonical live
registry; its views are reversible and every nontrivial maturity claim points
to source or a witness.

```bash
git clone https://github.com/Nickgonzales76017/bonfyre-oss.git
cd bonfyre-oss

bun architecture/atlas.mjs validate
bun architecture/atlas.mjs expand ExecutionView
bun architecture/atlas.mjs expand CapabilityView
bun architecture/atlas.mjs expand EvidenceView
bun architecture/atlas.mjs get work-graph
bun architecture/atlas.mjs loss
```

Read maturity literally:

- `architectural` or `declared` names a contract or intended organ.
- `implemented` has a real source path.
- `measured` or `proven` requires a witness.
- A public capability identity is not necessarily installed, callable, or
  workload-proven.

## Path B — run the semantic evidence

The control-plane suite is the conformance oracle. Running it verifies work,
capability, authority, provider/resource, evidence, and replay semantics
without pretending it is the production runtime. (The old Python suite it
replaced is archived; the `.mjs` suite below is the live one.)

```bash
bun test components/BonfyreControlPlane/tests
```

Useful focused checks:

```bash
bun test components/BonfyreControlPlane/tests/obligation-work.test.mjs
bun test components/BonfyreControlPlane/tests/occurrence-origin.test.mjs
bun test components/BonfyreControlPlane/tests/protocol-authority.test.mjs
bun test components/BonfyreControlPlane/tests/protocol-contract.test.mjs
```

Where the GPU backend is absent or denied (sandboxes, CI, VMs), the three
transcription tests need `BONFYRE_TRANSCRIBE_NO_GPU=1` on the same command.

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
bun architecture/atlas.mjs get frappe-core
ls cmd/BonfyreFrappeCompiler/tests/
```

The contract witnesses there (`native_schema_ingest_smoke.sh`,
`cms_status_roundtrip_test.sh`) run against a built compiler binary passed as
their argument.

## Path F — inspect a model/runtime surface

Model Commons is larger than one model or quantizer. Begin by inspecting the
live Model view and a native command's contract:

```bash
bun architecture/atlas.mjs expand ModelView
bun architecture/atlas.mjs get fpq-transform
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
