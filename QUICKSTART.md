# Bonfÿre quickstart

**Aurekai** invests engineering, AI, reusable software and distribution in
public products and digital opportunities. **Bonfÿre** is its native engine:
source context, identity, capabilities, work, authority, execution and evidence.

AI can generate software quickly; what matters here is whether a result is
composable, authorized, verified and reusable. Choose the shortest path that
answers your question. You do not need the entire development estate to begin.

[Business and technical model](docs/aurekai-bonfyre-model.md) ·
[Aurekai Work](https://aurekai.work) ·
[Life](https://aurekai.life) ·
[Live](https://aurekai.live) ·
[Shop](https://aurekai.shop)

## 1. See the public work

The [work samples](https://aurekai-work-samples.pages.dev/) have browser tools
you can try, including fixture planning, a product editor and an intake check.
The [guides](https://docs.aurekai.work/) cover white-label sites, migrations,
document workspaces and a studio app. These examples use public or fictional
material; they are not receipts for paid customer deployments.

## 2. Inspect the currently published source

```sh
git clone https://github.com/Nickgonzales76017/bonfyre.git
cd bonfyre
```

Public `main` contains a published source cut, Architecture Atlas, demos,
components and reviewed documents. The **local operating runtime and public
source can differ in revision and capability**. A named architecture, buildable
command, external provider profile and deployed service each require separate
verification. Read the command's own dependencies and tests before using it.

The present source is organized under `cmd/`, `lib/`, `engine/` and
`integrations/`. Historical notebook material is under
[`evidence/origins/notebook-era/`](evidence/origins/notebook-era/), rather than
being an alternative source root.

### Optional: work across the other repositories

The default clone does **not** initialize the federated source donors.
The current public source atlas enumerates **70 pinned source references**
(69 companions plus an upstream reference); these are not automatically
installed integrations. Select only the repositories relevant to the job:

```bash
bash scripts/federated_modules.sh list
bash scripts/federated_modules.sh init products
bash scripts/federated_modules.sh init extractions
bash scripts/federated_modules.sh status products
```

Profiles include `products`, `extractions`, `examples`, `aurekai`,
`performance`, `lineage`, `upstream`, `frappe`, `interfaces`, `one PATH`,
and `all`. Run `init lineage` only for older source histories. The Frappe and interface donor families are large
and source-only; fetch just one project when possible:

```bash
bash scripts/federated_modules.sh init one integrations/federated/interfaces/opentui
```

To deliberately fetch an entire large family, set the opt-in explicitly:

```bash
BONFYRE_ALLOW_LARGE_MODULES=1 bash scripts/federated_modules.sh init frappe
```

Avoid `init all` on a storage-constrained machine. No
submodule contents are needed for the top-level native `make` target.

The `extractions` group follows the reviewed nested `bonfyre-core` and
Lambda Tensors gitlinks, so the extracted command sources have their shared
library dependencies. All other profiles remain non-recursive by default.

Submodule SHAs are pinned in Git, not pulled from `main` on every run. To
change a pin, review the referenced repository and commit its new gitlink in
a focused PR. Each companion retains its own license and build contract.

## 3. Build and inspect a native command

With a C toolchain and the dependencies specified by the [Makefile](Makefile):

```sh
make
./cmd/BonfyreWire/bonfyre-wire --help
./cmd/BonfyreCLI/bonfyre list --health
```

The result of a build is not proof of authority to perform an effect or of
support on every operating system. If a command has its own README or Makefile,
use that command's stated requirements.

## 4. Turn an authorized capture into a reviewable recipe

Only use a capture you own or are authorized to inspect:

```sh
./cmd/BonfyreWire/bonfyre-wire ingest-pcap capture.pcap --dumb-device --root layeros/state
./cmd/BonfyreWire/bonfyre-wire probe <capture_id> --root layeros/state
./cmd/BonfyreWire/bonfyre-wire artifacts <capture_id> --root layeros/state
./cmd/BonfyreWire/bonfyre-wire recipe <capture_id> --root layeros/state > recipe.json
```

This example demonstrates an observation → artifacts → proposed-recipe
boundary. The redirect to `recipe.json` is a compatibility output for this
specific command, not the platform's canonical internal interchange format.
Modern native owners use typed artifacts and declared contracts. Inspect the
output and exact source revision before any downstream compiler or executor.
Generating a recipe is **not** authority to execute it.

## 5. Continue with the right guide

- [Bonfÿre overview](README.md)
- [Architecture](docs/architecture.md)
- [Public client delivery portfolio](docs/client-operations-portfolio.md)
- [Wire](docs/bonfyre_wire.md)
- [Published guides](https://docs.aurekai.work/)
- [Work samples](https://aurekai-work-samples.pages.dev/)
