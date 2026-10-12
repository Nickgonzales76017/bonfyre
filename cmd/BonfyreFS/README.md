# BonfyreFS

Mounts a Bonfyre fabric as a read-only FUSE filesystem.

```
<mount>/
├── Missions/<mission-id>.json
└── Artifacts/<sha256>.json
```

Artifacts are content-addressed: the filename is the SHA-256 of the ingested
bytes, so `sha256sum <file>` predicts the path.

## Bootstrap

A fabric must exist before it can be mounted. BonfyreFS does not create one, and
the two steps live in different binaries, which is easy to miss:

```bash
export BONFYRE_STATE_DIR="$HOME/.bonfyre/fabric"
mkdir -p "$BONFYRE_STATE_DIR"

bonfyre fabric init                    # creates $BONFYRE_STATE_DIR/fabric.db
bonfyre fabric status                  # sanity check

mkdir -p /tmp/fabric-mnt
bonfyre-fs /tmp/fabric-mnt             # daemonises
```

Without `BONFYRE_STATE_DIR`:

```
bonfyrefs: BONFYRE_STATE_DIR is required
```

With it set but no `fabric init` yet:

```
bonfyrefs: cannot open /path/fabric.db: unable to open database file
```

Unmount with `umount <mount>` on macOS, `fusermount -u <mount>` on Linux.

## Populating it

An empty mount usually means an empty fabric rather than a broken one:

```bash
bonfyre mission create my-mission
bonfyre artifact ingest path/to/file text/plain
```

`bonfyre fabric status` reports `missions=` and `artifacts=`; if those are zero,
the mount is correctly showing nothing.

## Options

| flag | effect |
|---|---|
| `--mission <id>` | Scope the mount to one mission. `Artifacts/` then lists only artifacts appearing as an input or output in that mission's events, rather than every artifact in the fabric. |
| `-f` | Foreground. Useful for debugging; the filesystem stops when you interrupt it. |

## Requirements

macFUSE on macOS, `libfuse-dev` on Linux. The Makefile resolves FUSE through
`pkg-config`, falling back to macFUSE's `/usr/local` layout.

## Note on the fork

The database is opened in the FUSE `init` callback, after `fuse_main`
daemonises, and not in `main`. A SQLite handle does not survive the fork: the
child inherits locking state belonging to the parent and every query returns
zero rows rather than an error, so the mount succeeds and shows an empty tree.
That failure is indistinguishable from an empty fabric, and `-f` hides it
because foreground mode never forks. `.github/workflows/native-kernel.yml`
mounts as a daemon specifically to keep it from coming back.
