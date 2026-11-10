# State coordinates — who owns which store, per environment

Measured 2026-09-01 against both live environments. This is a map of what is,
not a proposal.

## The rule

A store is named by a declaration, never by where the caller was standing.

```
1. an explicit flag            --db / --schemas
2. the environment's own declaration   BONFYRE_*_DB / BONFYRE_*_SCHEMAS
3. one machine store under the state root
4. refuse — and name the variable to set
```

`$PWD` is never step 5. A store chosen by the caller's working directory is
indistinguishable from the real one once it exists, which is the whole failure
mode below.

## What went wrong

A grep for **bare filename** defaults named fifteen organs (`bonfyre_cms.db`,
`layers.db`, `finance.db`, `bonfyre-index.db`, `outreach.db`, `watch.db`,
`wire.db`, `discipl.db`, `capability.sqlite3`, `bonfyre-meter.db`,
`index.sqlite`, `ledger.db`, `meter.db`). Reading each call site cut that to
three real ones — see *Corrected scope* below. Where the default was genuinely
bare, every invocation opened or created a store beside whatever directory the
caller happened to be in.

Seventeen stray stores accumulated on the control plane, including **seven
different `bonfyre_cms.db` files** spanning 2026-04-27 to 2026-08-29, 0 to
483KB, each as authoritative-looking as the others:

```
~/.bonfyre/bin/bonfyre_cms.db                            344064 b   2026-08-14
~/.bonfyre/moe/finance.db                                 32768 b   2026-08-30
~/Developer/Bonfyre/bonfyre_cms.db                       483328 b   2026-07-30
~/Developer/Bonfyre/Aurekai/bonfyre_cms.db               475136 b   2026-06-07
~/Developer/Bonfyre/bonfyre-zig/bonfyre_cms.db           462848 b   2026-08-13
~/Developer/Bonfyre/cmd/BonfyreCMS/bonfyre_cms.db        409600 b   2026-08-29
~/Developer/Bonfyre/cmd/BonfyreCLI/bonfyre_cms.db        344064 b   2026-04-27
~/Developer/Bonfyre/integrations/frappe-bench/…          352256 b   2026-07-30
~/Developer/Bonfyre/integrations/frappe-bench/sites/…         0 b   2026-08-12
~/Developer/Bonfyre/layers.db                            462848 b   2026-05-29
~/Developer/Bonfyre/layeros/state/layers.db              462848 b   2026-08-23
~/Developer/Bonfyre/discipl.db                            98304 b   2026-08-30
~/Developer/Bonfyre/bonfyre-index.db                      45056 b   2026-04-29
~/Developer/Bonfyre/bonfyre-meter.db                      73728 b   2026-08-29
```

The sharper half: `bonfyre-cms` read **no environment variables at all**. The
deployment declares `BONFYRE_CMS_DB` and `BONFYRE_CMS_SCHEMAS`; the binary
ignored both. Reaching the CMS *through* the web-address organ used the right
store because that organ passes `--db`; reaching the same binary directly —
from the MCP, from `bonfyre-project cms`, from a shell — did not.

## Deployment (Oracle box, 207.211.172.160) — declared and correct

21 coordinates, all absolute, declared in systemd units and `/etc/bonfyre/*.env`.

| coordinate | path |
|---|---|
| `BONFYRE_MATRIX_DB` | `/var/lib/bonfyre/matrix.db` |
| `BONFYRE_AUTH_DB` | `/var/lib/bonfyre/auth.db` |
| `BONFYRE_CMS_DB` | `/var/lib/bonfyre/cms/bonfyre_cms.db` |
| `BONFYRE_CMS_SCHEMAS` | `/var/lib/bonfyre/cms/content-types` |
| `BONFYRE_FOREIGN_ROOT` | `/var/lib/bonfyre/foreign` |
| `BONFYRE_SITE_ROOT` | `/opt/bonfyre/site/matrix` |
| `BONFYRE_BIN_DIR` | `/usr/local/bin` |
| `BONFYRE_MATRIX_RECEIPT_DIR` | `/var/lib/bonfyre/matrix/receipts` |
| state family | `/var/lib/bonfyre/state/{capability,compete,control,economy,entity,finance,flow,layers,learn,models,sae,space,tier,time}.db` |

Also on disk, undeclared: `/var/lib/bonfyre/provider.db`,
`/var/lib/bonfyre/state/cms.db`, `/var/lib/bonfyre/state/fleming`,
`/var/lib/bonfyre/state/duckdb`, `/var/lib/bonfyre/mcp/tasks`.

## Control plane (this Mac) — declares nothing

No `BONFYRE_*` state coordinate is exported. Every organ run here falls to its
own default, which is how the seventeen strays happened.

**Open decision, not mine to make:** which of the seven `bonfyre_cms.db` files
is canonical for this machine, or whether the control plane should have a CMS
store at all rather than always addressing the deployment's. Until that is
answered, `bonfyre-cms` on the Mac refuses rather than picking one.

## Fixed

`bonfyre-cms` now resolves both coordinates in the declared order and refuses
rather than inventing an eighth store. Verified on the box from `/tmp` with no
environment at all: resolves `/var/lib/bonfyre/cms/bonfyre_cms.db`, loads
`content-types` (`cms_generation`, `frappe_app`, `wiki_page`), creates no stray.

`bonfyre-project` delegation resolves managed peers before checkout siblings,
so `cms`, `index` and `stitch` reach real organs in both layouts.

## The shared resolver

`bf_state_path()` in `lib/libbonfyre/src/bf_common.c` — the file whose own
banner says these utilities were "previously duplicated across every binary."
Order: explicit flag, environment declaration, `$BONFYRE_STATE_ROOT`,
`/var/lib/bonfyre`, `$HOME/.local/share/bonfyre`, each accepted only when its
parent directory already exists. Returns NULL rather than inventing a store, so
callers refuse and name the variable to set.

## Corrected scope

The first pass named thirteen organs from a grep for bare filenames. Reading
each call site cut that to **two**. The rest were already anchored and the grep
had matched a leaf name being passed to an anchoring function:

| organ | actual behaviour |
|---|---|
| `BonfyreDiscipl`, `BonfyreLayer` | `bf_layer_state_db_path()` falls back to `$HOME/.local/share/bonfyre/` |
| `BonfyreOutreach` | home-anchored; bare only if `$HOME` is unset |
| `BonfyreWire` | anchored to a caller-supplied root |
| `BonfyreCapability` | anchored to a resolved directory |
| `BonfyreFinance` | reads `BONFYRE_FINANCE_DB` already |

Genuinely bare, now fixed: **`BonfyreIndex`** and **`BonfyreMeter`**. Both had
strays on disk, which is what confirmed them.

## Six organs did not compile on the control plane

Found while verifying the above. `BonfyreIndex`, `BonfyreBrief`,
`BonfyrePipeline`, `BonfyreSceneDetect`, `BonfyreSwarm` and `BonfyreTel` pin
`_POSIX_C_SOURCE`, which hides the BSD interfaces they use — `strcasecmp`,
`ru_maxrss`, `MSG_DONTWAIT`, `INADDR_LOOPBACK` — on Darwin. The inverse of the
trap their own banner warns about. A `_DARWIN_C_SOURCE` guard under `__APPLE__`
fixes all six and is a no-op on Linux (verified by building each on the deploy
host).

All **89** libbonfyre-linked organs now build on macOS; 0 failures.

## Consolidation, 2026-09-01

The seven CMS stores were not seven copies. They were two schema generations
plus the deployment, and each held a different piece:

| generation | types | rows |
|---|---|---|
| A — commerce/proof (Jun–Jul) | `service_offer` `service_provider` `pricing_tier` `offer_artifact` `stake` `proof_meta` `proof_summary` `intake_package` `building` | 0 |
| B — content (Aug) | `cms_article` `cms_generation` `course_lesson` `frappe_app` `wiki_page` | 13 |
| deployment | B + `physical_carrier` | 43 |

Two things were stranded and are now merged into `/var/lib/bonfyre/cms/`:

- **9 commerce type definitions** existed only as `schema_json` rows inside two
  otherwise-empty SQLite files, with no `.json` anywhere on either host. 165
  fields total. Applied to the deployment and written as files, so the next
  reader does not depend on a database surviving.
- **4 records** — three research case studies and a teaching primitive, 10,915
  bytes, namespace `aurekai`, created 2026-08-13 — existed nowhere else. The
  deployment's own 33 `wiki_page` rows are namespace `root` and 28 of them are
  `estate-note-<timestamp>` machine output, so this was the only human-written
  content in the estate and it was on one laptop.

Verified 4/4 byte-identical after the merge, status preserved as `draft`, and
all three research routes still answer 404 publicly. CMS save is not
publication; the merge moved content, not visibility.

Deployment now: **15 types, 36 wiki_page, 1 course_lesson.**

### Backing up a WAL store

The first backup taken during this merge was wrong: `cp` of the `.db` alone,
while the store runs in WAL mode with a 2.4MB WAL — larger than the 425KB main
file. It captured Aug 30 state and silently missed everything since. Use
`sqlite3 <db> ".backup '<target>'"`, which snapshots WAL content consistently,
and verify with `PRAGMA integrity_check`.
