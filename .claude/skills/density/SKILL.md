---
name: density
description: Compression and storage density — archiving cold trees, set-level compression, reclaiming disk. Use when asked to free space, shrink something, archive releases or artifacts, or when disk pressure appears.
allowed-tools: Bash, Read
---

# Density

The governing result, measured here: **sets compress far better than the objects
in them, but only if the set is ordered so like sits beside like.**

Two release trees, 780 MB each, differing in 21 files:

```
compressed individually   188.6 MB
naive set tar             177.8 MB   barely better
ordered by relative path   95.5 MB   one tree's worth
```

`--long=27` is a 128 MB window. The duplicate sits 780 MB away, so a tree-by-tree
tar can never see it. Sorting entries by path-relative-to-tree puts the copies
adjacent, where a small window catches them. **The ordering is the lever, not the
window size.**

```bash
find $TREES -mindepth 0 > /tmp/all.txt
awk -F/ '{rel=$0; sub(/^[^\/]*\//,"",rel); print rel "\t" $0}' /tmp/all.txt \
  | sort | cut -f2 > /tmp/interleaved.txt
tar -cf - --numeric-owner --no-recursion -T /tmp/interleaved.txt \
  | zstd -19 --long=27 -T0 -o out.tar.zst
```

## Non-negotiable

- **`find -type f` is not the tree.** It drops symlinks and empty directories.
  A first attempt here silently lost `libngtcp2.so`. Use no `-type` filter and
  `--no-recursion`, so every entry is archived exactly once.
- **`--long` is part of the frame.** Decompress with the same flag. A plain
  `zstd -dc` refuses a window it never opted into. That is a reader contract, not
  corruption — an earlier session mistook it for one and retreated to a smaller
  window, losing the whole cross-tree gain.
- **Verify before removing.** Extract the archive and `diff -r --no-dereference`
  against the original, entry for entry, before anything is deleted. This has
  caught a real defect.
- **Check what points at it.** `*-current` symlinks resolve into `releases/`, and
  a running service may hold a binary open there. `lsof +D` and `find -type l
  -lname` before assuming a tree is cold.

## Where the gains are

Already taken: releases 2,060→386 MB, an OpenSSL build tree 1,900→172 MB,
explainer-agent 1,600→141 MB, and ~10 GB of caches and unreachable git objects.

Still on the table, and why they were left:

- `output/recursive-institutional-growth` — 969 MB allocated, 397 MB apparent.
  **572 MB is block slack** across 133,995 tiny files. Compresses to ~44 MB as a
  set, but the producer reads `artifacts/<canonical_id>/artifact.json` back, so
  the real fix is packing many records per file at write time, not an archive.
- explainer WAVs — lossless FLAC halves 995 MB, but 1,373 manifests name
  `artifact.wav` by absolute path and those are attestations. Re-pointing them at
  a re-encoded file makes them describe something that no longer exists.

SQLite here has no free-page waste; VACUUM is not the lever. Look for block
slack, near-duplicate trees, and build output that is reproducible from a
tarball already on disk.
