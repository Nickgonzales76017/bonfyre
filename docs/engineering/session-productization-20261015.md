# Installed source reuse and public workbench

## Installed native entry

The normal entry is `bonfyre-runtime representation`. It delegates to the installed release, not the development checkout. The Capability registry discovers the launcher under `representation`; its cost remains unmeasured. The missing atlas description remains a separate source-write gate.

```sh
$HOME/.bonfyre/bin/bonfyre-runtime representation doctor
$HOME/.bonfyre/bin/bonfyre-runtime representation verify \
  --estate "$HOME/.bonfyre/edge-workgraph" \
  --kind workgraph.workgraph_transitions
```

Verification is read-only and returns `REUSABLE`, `SNAPSHOT_REQUIRED`, or `HELD`. Exit zero requires the whole selected demand to be reusable. A changed source can require a new snapshot without requiring every selected table to be rebuilt. Apply is explicit:

```sh
$HOME/.bonfyre/bin/bonfyre-runtime representation apply \
  --estate "$HOME/.bonfyre/edge-workgraph" \
  --kind workgraph.workgraph_transitions
```

The October 15 installation lowered the live 32-row WorkGraph transition table. A separate process verified reuse with no source snapshot. Later WorkGraph activity changes that source; a subsequent snapshot requirement is not an installation failure. No global source scan, application permission, or automatic recurring job is implied.

The current release pointer is `~/.bonfyre/share/bonfyre/representation/current`. It selects a content-addressed directory containing the existing Manifold/Fabric modules, CUE policy, and Hash binary. Each invocation checks the release manifest and its member digests before dispatch. Missing or changed members fail rather than falling back to checkout files. This is integrity checking, not a signed software supply-chain certificate.

The installed runtime still needs Bun, CUE and DuckDB on the host. The current verification is Apple Silicon macOS with the already installed dependencies, not clean-machine portability or Linux/Windows certification.

## Rebuild and activate through the owners

From the repository root:

```sh
make -C scripts test-representation-release
make -C scripts install-representation-release
sh scripts/organ_drift.sh --verify --only bonfyre-hash
sh scripts/organ_drift.sh --verify --only bonfyre-runtime
```

The release installer stages through `install-representation-fabric`, verifies every member, runs the installed entry point, and swaps the owned `current` link. Concurrent activation, an escaped current link, unowned launcher, stale expected release, or failed build is refused. The previous release directory remains available; there is no automatic rollback CLI. Keep old managed binaries outside the managed binary directory so discovery does not mistake a backup for a capability.

`organ_drift.sh --sync --only NAME` is the separate managed-binary promotion operation. It requires the command's Makefile target and verifies the promoted digest. Never run an unscoped sync to deliver this change. The Runtime Makefile now declares its existing target so the owner can discover it.

## Public browser workbench

Open the [Source and program workbench](https://browser-workbench.aurekai-work-samples.pages.dev/), also linked under **Try the work** on the [Work Samples site](https://aurekai-work-samples.pages.dev/).

Import saved captures, select their exact source references, edit one of the existing 36 program templates, review cycle costs, and save a revision-checked draft. **Open full studio** uses that same browser store. It does not require copying the draft into another form. A stale tab cannot overwrite a newer revision.

The web edition keeps captures and drafts in this browser and origin. Capturing another tab, opening native collected material, and submitting native work are disabled here. Those operations require the extension and its approved native host. Compilation does not create a subscription, charge, merchant approval, published offer, or cross-device synchronization.

The deployment copies an explicit source allowlist from `integrations/browser/extension`. It excludes the extension manifest, background service, native host, credentials and saved user records. Static assets do not add a Worker or a native intake route. Build and repeat publication through the reusable source owner:

```sh
make -C integrations/browser test-workbench
make -C integrations/browser verify-workbench
make -C integrations/browser publish-workbench
```

The publisher uses `components/BonfyreServiceSite/cloud.mjs` and the existing account. It verifies all served source digests and that the production deployment did not change while publishing the `browser-workbench` preview branch. An identical release is read back without another deployment. The public gallery is a separate canonical `service_site` CMS record, published by `site.mjs publish`; do not patch its generated output as a substitute for a CMS update.

## Session continuity

The existing Cloudflare Worker, Workflow, Queue and authenticated native pull consumer remain the source-sync owner. Read their current state before changing them. Do not create another scheduler, expose loopback credentials, resume the globally paused operator, or turn source-sync permission into acquisition, purchase, payment or publication authority.

Canonical sources are under `integrations/cloudflare`; the initial live completion and retry witness is `packs/no-simulation/cloudflare-workgraph-20261015/readback.json`. The released source reuse owner can be called explicitly through `BonfyreRuntime` with `representation` arguments from MCP. A hash or successful materialization does not grant permission to act on the source.

The Brief Studio and ITAD previews remain separate applications already linked from Work Samples. Their native acquisition Auth/WorkGraph foundation is present in `components/BonfyreServiceSite/{account-auth,acquisition-access,acquisition-store}.mjs`, but mounting its account handler in those public applications and transferring browser drafts into an authorized organization remain unfinished. Public preview availability is not shared organization persistence.

## Verification and release boundaries

`output/verification/session-productization-20261015` retains installed-release identity, managed binary comparisons and prior images, source hashes, publisher readback, public asset digests, and browser verification. The scope includes 68 native regression tests, the Runtime dispatch witness, local and public 18-check workbench journeys, and the eight-route public gallery audit. Browser fixtures are labeled synthetic and submit no remote writes.

The WorkGraph operation is `productization:session-20261015`. Its completion is scoped to installed/public release operations actually verified, not completion of all 30 experiences or every architecture-wave task.

The source-commit owner currently reports `automatic operator commits are disabled`. Architecture declarations also remain outside the low-risk source-write roots. The pending atlas backfill needs its authorized source-edit path before it can be exported. Do not retrieve an approval token, edit policy, or use a different tool merely to bypass these boundaries. Preserve existing dirty and staged files and keep source commit, runtime activation, and public publication as separate facts.
