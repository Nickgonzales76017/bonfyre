# Remaining connected-product work

These requirements remain part of the original installation-to-product target.
Household access and the audio arrangement adapter do not complete this list.

## Abandoned upload reclamation

Use the existing RetentionGC owner in `engine/core/src/retention.c` and
`engine/core/include/bf_retention.h`. It already owns retention tiers, holds,
removability and collection. Its current kind table has no workspace-upload
class. The connector refused a patch adding that class because engine/core is
outside its writable roots. No alternate collector or scheduled deletion was
created, and no uploaded body was removed.

The required extension is a workspace-upload retention class with a bounded
inactivity lease. Existing uploads need a migration-time grace period; they must
not be treated as expired merely because older rows lack a timestamp. Active
upload chunks renew the lease. Completed uploads remain protected until the
draft transaction either commits references or leaves an explicitly abandoned
candidate. Replays and current drafts are not the only roots: every retained
record body and operation receipt capable of restoring a previous revision must
retain its referenced files.

Use one native transaction for the final liveness recheck, RetentionGC decision,
tombstone, chunk removal and quota release. The current retention collector opens
its own transaction, so it needs a composable transaction boundary before CMS can
invoke this safely. Collection must remain bounded and idempotent. Corrupt or
unsupported reference profiles, incomplete migration, held work and ambiguous
ownership must refuse collection, not count as unreferenced.

Required tests cover an abandoned partial upload, a completed but unadopted
upload, active chunk renewal, source references retained only by an old receipt,
concurrent adoption versus collection, interrupted collection, corrupt metadata,
old-row migration, repeated sweep, released quota and retained current drafts.
Logical reclamation must not be described as physical erasure from WAL or backups.

## Training

Keep the existing `training-workspace.mjs`, creator access, publishing, plan,
presentation and request owners. Creator requests already use native WorkGraph;
programs and presentation already use their CMS owners. The member browser's
`training:member:primary` holds personal workouts and sensor data. A household
or organization installation must never remap that key into a shared record
family. Actor-private synchronization and the existing offline session contract
need an explicit adapter before training can be published as connected.

## Brief Studio and ITAD

`components/BonfyreServiceSite/acquisition-store.mjs` already implements an
organization-scoped WorkGraph checkpoint with expected revisions and source
verification. `acquisition-access.mjs` applies native account authority. Preserve
that owner rather than replacing its head with a second generic CMS draft store.
The specialized Brief Studio and ITAD interfaces live in the retained public
procurement site and use their own draft contracts. Their reviewed installation,
native same-origin serving, original-draft adoption and typed handoff still need
to be connected and exercised through the actual editors.

## Outputs and commercial effects

A saved audio session and its playable WAV export do not yet enter MediaProgram,
MediaLibrary or a distribution channel. Those existing owners require separate
source identity, audience and rights binding. A reviewed procurement brief is
not a supplier order; a catalogue edit is not a merchant publication. Preserve
the native WorkGraph, Finance, Commitment, provider-account and publication
boundaries. The next commercial adapter must retain the exact approved action,
