# Bonfÿre quickstart

**Aurekai** operates and develops businesses, publications, community experiences, software, and customer projects. **Bonfÿre** provides independent native tools for actual jobs: WordPress and site exchange, cPanel/InterWorx/Plesk inventory, XLSX review, scanned-PDF OCR, document delivery, typed frontend authoring, scoped MCP project/workspace operations, Git source work, model execution, and explicit account/provider onboarding. These are not all implemented by a generic identity/capability matrix.

AI can generate software quickly; what matters here is whether a result is
composable, authorized, verified and reusable. Choose the shortest path that
answers your question. You do not need the entire development estate to begin.

[Concrete workflow and export guide](docs/real-workflows-and-interoperability.md) ·
[MCP project/workspace tool guide](integrations/mcp/README.md) ·
[Provider onboarding](integrations/browser/onboarding/README.md) ·
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

## 5. Try a real file, site or integration workflow

**A buildable binary is not necessarily installed or wired to a live customer.** Start with the command's own tests and required files; source, fixture and public/deployed readback are different levels of acceptance.

~~~bash
# Site interchange — one published native source, three different output shapes.
bonfyre site export notes --out notes.html
bonfyre site export notes --format wordpress --out notes.xml
bonfyre site export notes --format frappe --out notes.json

# Existing XLSX: inspect sheet/cell provenance; do not post transactions.
bonfyre books inspect --workbook bank.xlsx

# Scan/PDF: machine-local page locations, confidence and recognized text.
bonfyre-ocr lines scanned-letter.pdf

# Report conversion (Pandoc and appropriate PDF engine must be installed).
bash components/PandocFormatter/bin/pandoc_format.sh convert report.md --to docx
bash components/PandocFormatter/bin/pandoc_format.sh brief-to-pdf brief.md --out brief.pdf
~~~

The corresponding [site export/import test](cmd/BonfyreWebAddress/tests/site_product_test.sh), [Books XLSX source](cmd/BonfyreDecide/src/books_xlsx.c), [OCR C/Swift owner](cmd/BonfyreOCR/src/main.swift) and [Pandoc formatter](components/PandocFormatter/README.md) explain what each operation actually preserves and refuses. If the command or its dependencies are unavailable in your checkout, treat the examples as **source-interface documentation**, not a verified invocation on your machine.

For WordPress **events**, use the [signed bridge guide](integrations/wordpress/README.md); exporting a WXR file is a different operation. For a **signed-in assistant**, the [Project MCP](integrations/mcp/README.md) and [Workspace/Workbook MCP](integrations/mcp/workspace_mcp.mjs) preserve the current tool owner, record revision, review and effects, rather than creating a second database. For **YouTube, Spotify, Patreon and other services**, consult the [200-target onboarding ledger](integrations/browser/onboarding/targets.tsv) and [provider scope guide](integrations/browser/onboarding/README.md) before assuming any token or API action exists.

### If you are exploring a React/Svelte/PHP source project

A real conversion requires inspecting routes, state, forms, handlers, permissions, assets, CSS and runtime requirements. [The unified frontend guide](docs/authoring-frontend-architecture.md) explains the tested typed Puck/GrapesJS/Editor.js and C rendering path. **It is not a verified automatic SvelteKit/React/PHP-to-native converter**, and cloning a repository or previewing a page cannot satisfy that acceptance.

## 6. Continue with the right guide

- [Bonfÿre overview](README.md)
- [Architecture](docs/architecture.md)
- [Real workflows and exports](docs/real-workflows-and-interoperability.md)
- [MCP Project/Workspace](integrations/mcp/README.md)
- [WordPress signed events and WXR exchange](integrations/wordpress/README.md)
- [Public client delivery portfolio](docs/client-operations-portfolio.md)
- [Wire](docs/bonfyre_wire.md)
- [Published guides](https://docs.aurekai.work/)
- [Work samples](https://aurekai-work-samples.pages.dev/)
