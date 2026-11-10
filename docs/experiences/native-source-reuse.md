# Reuse unchanged source material

The installed representation command checks selected SQLite tables before rebuilding their derived material. An unchanged, verified demand can retain its Parquet files and downstream bindings without making another source-database snapshot.

This is a native developer workflow. The [source and program workspace](/source-program-workspace) has a browser-local edition and a native destination edition. Opening either interface does not grant access to these databases; its draft storage and this source-reuse operation have separate owners.

## Check the installed release

```sh Terminal
$HOME/.bonfyre/bin/bonfyre-runtime representation doctor
```

A valid installation reports `INSTALLED_VERIFIED`, its release identifier and checked file count. The launcher reads the selected release under `~/.bonfyre/share/bonfyre/representation/current`. Missing or changed release members are refused, rather than replaced silently with development-checkout files.

The release uses the existing native Hash and RepresentationManifold owners. Bun, CUE and DuckDB remain host prerequisites. The current installer has been exercised on the development Mac; a clean-machine installation and other operating systems need separate verification.

## Verify one selected demand

Use the estate and semantic kind you are authorized to inspect. For an installation with the existing edge WorkGraph store:

```sh Terminal
$HOME/.bonfyre/bin/bonfyre-runtime representation verify \
  --estate "$HOME/.bonfyre/edge-workgraph" \
  --kind workgraph.workgraph_transitions
```

| Result | Meaning | Exit status |
|---|---|---|
| `REUSABLE` | The selected source and retained representation passed the current checks. | 0 |
| `SNAPSHOT_REQUIRED` | Matching materialization receipts are missing or the selected source changed. | 3 |
| `HELD` | A source or retained representation could not be safely verified. | 3 |

Verification opens the existing Manifold read-only. It creates no source snapshot, materialization, semantic seed, work completion or authorization grant. A successful result describes that observation window; subsequent source changes can invalidate it.

## Materialize through the same owner

After reviewing the source scope, replace `verify` with `apply` in the command. This is an explicit write to derived representation state. It is not a database-wide rollout or permission to process a different estate.

For unchanged demands, the compiler checks the exact source coordinates, typed table content, compiler identity, binding, active frontier, output digest and row count. A retained source connection detects changes during verification. Changed or cold owners retain the ordinary consistent-snapshot path, while unrelated selected tables can reuse their existing outputs.

The source table and output bytes are still read and hashed. Snapshot-free reuse is not an end-to-end zero-copy claim. Full-estate compilation retains its historical snapshot behavior, and a cross-store transaction is not established by matching digests.

The October 15 installed-path check materialized the existing WorkGraph transition table and then verified it in a fresh process with one reused representation, zero lowerings and zero source-snapshot bytes. That is a bounded execution witness, not a claim that every live CMS table has been migrated.

## Install an updated representation release

From the authorized source checkout:

```sh Terminal
make -C scripts test-representation-release
make -C scripts install-representation-release
```

The installer stages the existing Manifold/Fabric implementation, checks its inventory and installed entry point, and switches the owned release pointer. Failed builds, stale activation expectations and an existing activation lock refuse the switch. Native binary promotion remains with the existing `scripts/organ_drift.sh` owner and its release gates.

## Existing Cloudflare source synchronization

The owned Work Samples watcher feeds `bonfyre-workgraph-activation` through the existing Cloudflare Worker and Queue. A native pull consumer admits the supported source-sync family, reuses retained ForeignTwin material and WorkGraph checkpoints, and acknowledges delivery only after matching native completion and signed cloud completion.

The deployed consumer is `com.bonfyre.edge-workgraph-consumer`, scheduled every 60 seconds; the source watcher checks every 15 minutes. These are existing integrations, not new schedules created by the representation installer. A current lease, acknowledgment and completed work item must be checked independently of their configured cadence.

Two live source-sync workflows completed on October 15, including recovery from an injected transient failure on native attempt two. This does not establish Oracle or Supabase executor takeover. The globally paused operator remains separate, and the public browser workspace exposes no local credential or native-ingress endpoint.

The [interface guide](/interfaces-and-mcp) explains the CLI and assistant boundaries. [Building blocks](/building-blocks) separates installed release paths from the wider library packaging work.
