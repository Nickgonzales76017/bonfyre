# Bonfÿre quickstart

Choose the shortest path that answers your question. You don't need to build the
entire development estate to see how Bonfÿre is used.

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

Public `main` contains an earlier native source snapshot. The newer Architecture
Atlas, ServiceSite and integrated institutional work are being reviewed separately
before publication; do not assume their local development instructions are
already executable from this checkout.

The present source is organized under `cmd/`, `lib/`, `engine/` and
`integrations/`. Historical notebook material is under
[`evidence/origins/notebook-era/`](evidence/origins/notebook-era/), rather than
being an alternative source root.

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

This gives a reviewable chain: observation → typed artifacts → proposed recipe.
Inspect the output before using a downstream compiler or executor. Generating a
recipe is not authorization to execute it.

## 5. Continue with the right guide

- [Bonfÿre overview](README.md)
- [Architecture](docs/architecture.md)
- [Public client delivery portfolio](docs/client-operations-portfolio.md)
- [Wire](docs/bonfyre_wire.md)
- [Published guides](https://docs.aurekai.work/)
- [Work samples](https://aurekai-work-samples.pages.dev/)
