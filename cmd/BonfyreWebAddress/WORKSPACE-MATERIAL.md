# Workspace material storage

The existing workspace materializer stores identical file bodies once within its
configured package root. Releases retain their exact manifest identity. Pack
installations, Auth grants and CMS workspace records keep their current owners.

## Layout and readers

New release directories contain `MANIFEST.tsv` and `STORAGE.tsv`. The latter
contains `layout\tshared-bodies-v1\n`; the manifest still maps paths to SHA-256
and byte length. Bodies live at `_bodies/sha256/<first-two-hex>/<digest>` under
the same package root. There is no public bare-digest read endpoint.

`scripts/materialize_workspace_package.mjs` calls the shared implementation in
`scripts/workspace_material.mjs`. Creator and recurring-program packages already
use its `retainWorkspaceFiles` entrypoint. Consumers needing bytes should use
`readWorkspaceFile(root, release, path)` or `loadWorkspaceFiles(root, release)`;
joining a release directory and member path works only for legacy layouts.

`src/application_workspace.h` includes `src/workspace_package.h`. The native
reader selects the layout only after checking release-manifest membership. It
uses libbonfyre hashing and descriptor-relative no-follow traversal. Legacy
tree releases remain readable and are not migrated on access. Missing or
corrupt members are refused, including when a plausible replacement exists in
the other layout.

## Publication and reuse

The writer validates and snapshots caller-owned bytes before asynchronous work.
It publishes each body without overwriting an existing digest coordinate, then
publishes the release after its bodies and directory metadata have been synced.
A concurrent winner must verify. Failed downloads are canceled and joined;
failed publication removes only that invocation's staging directory. Published
bodies are not removed on failure because another release may reference them.

An unchanged release writes no new body or manifest. Verification still reads
and hashes retained bytes. The returned counters separate body writes, body
reuse, verification reads and manifest bytes; these are not filesystem free-space
measurements. Legacy migration and orphan-body collection are not implemented.

## Checks

Run `make -C cmd/BonfyreWebAddress -j2` from the repository root. The normal
target builds the native reader and runs 40 workspace storage tests plus the
two existing native-workbench package tests and the other WebAddress witnesses.
These passed on the Mac checkout during this contribution. The storage fixture
keeps seven releases with one shared 8 MiB asset: 58,720,319 logical payload
bytes use 8,388,671 body bytes, avoiding 50,331,648 duplicate body bytes.
Manifest metadata is additional. No live-estate reclaim was measured.

## Rollout

The source changes are applied and the repository binary was rebuilt. Since
2026-10-17 the public serving process runs this reader from
`~/.bonfyre/workspace-material-release-20261017` (its `service.before.plist`
points back at the previous release). A canary matched live on 39 routes first,
and all seven legacy releases with assets read byte-exact afterwards.
Deploy the compatible reader before producing shared-layout releases for that
process. Its OS account must have access to the configured package root.
Application rollback must retain the compatible reader while any referenced
release uses shared bodies. Copying a release directory alone is no longer a
complete deployment or backup: its manifest's bodies must accompany it.
Existing production data has not been migrated or deleted. The corresponding
source paths, storage witnesses and rollout record are now present in
`architecture/storage/representation-manifold.yaff`, checked 2026-10-18 UTC.
