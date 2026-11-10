# Native file hash verification, 2026-10-13

The shared `bf_sha256_file` reader finalized a digest whenever `fread` returned zero, even when the stream reported an error. Hash's file and duplicate commands duplicated that behavior. On this machine the earlier Hash binary accepts a directory and prints the empty-file digest.

The shared reader now clears its output before opening and refuses open, read and close errors. It closes the stream after a read error and preserves the error for the caller. Hash's file and dedup commands use that reader; a hash failure aborts duplicate detection before any result is printed. Hash's build now refreshes the shared archive and depends on it, so a library correction reaches the binary on rebuild.

`before.log` records nine failures in 18 checks against the retained earlier primitive. `hash-before-tests.log` reproduces the directory bug against the earlier checkout binary; its fault-injected case uses current source, as the test explicitly builds that variant. Neither baseline is represented as an installed-binary fault injection.

Verification:

- `library.log`: the library suite passes, including 18 file-hash checks. Platform stdio fault streams inject immediate and partial read errors; a close wrapper injects a close error after releasing the real stream. Tests also cover cleanup, absent paths, directories and known digest vectors.
- `hash-tests.log` and `installed-hash.log`: four tests with 26 assertions pass. Twenty assertions run the selected checkout or installed binary; six run a separately compiled variant with injected partial-read failure. Six file sizes, including the 8 KiB boundary and a 1 MiB tail, match Node crypto's SHA-256. Equal complete files still produce a duplicate pair.
- `core.log`: all native core conformance tests pass.
- `fabric-reference.log` and `installed-fabric.log`: 149 reference assertions pass with actual native hash execution, generation changes and disclosure checks.
- Atlas validation and the full ExperienceMap build pass. `corpus-installation.json` records the refreshed installed corpus and its predecessor.

Hash and Fabric are installed with verified code signatures and retained rollback copies, recorded in `installation.json`. Native tool receipt `9806537f-505f-4609-b5d7-8bedb52f3912` confirms the installed Hash matches the checkout and computes the independently verified installed Fabric digest. Receipt `b8897467-d0c1-477a-81bb-7b30f5838b2c` records the expected directory refusal with empty stdout. Receipt `12fa0505-f95d-4d62-869d-ee47e2c39d47` reads a reference through the installed Fabric.

The reader retains its bounded 8 KiB buffer. It does not establish source immutability, a filesystem snapshot or complete directory enumeration, and duplicate reports grant no deletion authority. Other statically linked consumers need rebuilding to receive this library correction. No other installed binaries or public services were replaced in this change.
