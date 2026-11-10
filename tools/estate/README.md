# Estate tools, partially mirrored

`/opt/bonfyre/tools` on the live host holds 271 TypeScript tools and 5 shell
runners, 2.7 MB in total. It is not a git repository and no directory above it
is one. Exactly one of those files had a counterpart in this checkout.

That is the failure the CMS contract checker was written about — schema whose
only copy is inside a database is schema one deletion from gone — one level up,
and applied to the code that operates the institution rather than to its data.

The files here are the ones changed while repairing three failing
`bonfyre-hypercycle@` units, mirrored so the repairs have a home and a history.
They are a partial mirror and nothing keeps them in step with the host: treat
the host as authoritative, re-pull before editing, and install back explicitly.

Deciding whether the whole tree should be imported is a call for the estate's
owner, not something to be settled by a repair.

## What was repaired here

- `live-resource-lib.ts` — `execCommand` threw on a missing executable rather
  than returning the `{ ok: false }` its own type promises, so `aws` not being
  installed killed four cloud-readiness tools and took three units to `failed`.
- `cloud-fabric-lib.ts` — `cli_present` was the literal `true`. It is now
  probed with `aws --version`, which needs no credentials, so the field is an
  observation rather than an assertion, and an absent CLI reports `cli_absent`
  instead of being indistinguishable from an unready provider.
- `checkpoint93-profile-runner.sh` — a failing profile exited 1 with an empty
  journal. It now names each failing tool and its own last error on stderr.
  Receipts are appended with one Bun `writeSync` under `O_APPEND`, because
  concurrent profiles had already interleaved two lines into invalid JSON, and
  the summary that read them is now able to skip a bad line instead of dying on
  it.
- `live-institutional-closure-flow.ts` — the assertion message named the half
  that was fine. It now carries both counts.

## What was not repaired, because it is not broken

`bonfyre-hypercycle@graph-closure` still fails, correctly: the operator overview
reports 157 notification attempts and 0 actioned. Notifications are being sent
and none has ever been actioned. That is institutional state, and relaxing the
assertion to make the unit green is the maturity laundering this estate forbids.
