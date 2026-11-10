# Native file digest acceleration, 2026-10-13

macOS file hashing now uses CommonCrypto from the existing libSystem dependency. The exported incremental `BfSha256` API retains its portable implementation and layout. Other platforms, and builds with `BONFYRE_SHA256_PORTABLE`, use that implementation for files too. Both file backends use the same 8 KiB read buffer and refuse open, read and close failures.

The interface is documented in [Apple's SHA manual](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man3/CC_SHA256.3cc.html) and verified against the local SDK header. `linkage.json` shows the three CommonCrypto imports in all four rebuilt consumers, with no additional dynamic library dependency.

Five alternating local measurements per fixture include process startup:

| Workload | Earlier median | New median |
| --- | ---: | ---: |
| Hash 8 KiB | 0.00465 s | 0.00431 s |
| Hash 8 MiB + 17 bytes | 0.04951 s | 0.01232 s |
| Hash 128 MiB + 17 bytes | 0.72043 s | 0.11917 s |
| Intake 64 MiB + 17 bytes, including copying and both hashes | 1.06720 s | 0.52522 s |

All file digests match an independent Python SHA-256. Intake source, normalized and content identities also match the independently hashed copied bytes. Hash benchmarks warm both commands before recording; repeated intake reads may use filesystem caches. These synthetic local fixtures do not establish cold-storage or production throughput. Scripts, individual measurements and binary identities are retained in `benchmark.py`, `benchmark.json`, `ingest-benchmark.py` and `ingest-benchmark.json`; temporary large files were removed.

Ingest previously ignored source and normalized hash errors. The earlier executable reports a ready intake for an absent source, reproduced in `ingest-before.log`. Both failures now stop the invocation before it writes new ready manifests. The negative test's injected variant is compiled from current source; the earlier-binary failure is the actual absent-input case.

Verification:

- The shared library suite passes, including 18 reader checks on each backend. Both cover partial stream errors, close failure, missing paths and cleanup.
- Hash passes 58 assertions across eleven sizes, both backends, an independent digest and failure cases. The installed-binary rerun passes; the portable and fault-injected variants are separately compiled test binaries.
- Ingest passes 47 assertions for text, table and raw input, independent source/output digests, absent and directory inputs, and injected failures at both hash stages. Its installed-binary rerun passes.
- Native core conformance, 149 Fabric reference assertions and the full Compress suite pass, including 23 stream assertions.
- Atlas validation and the full ExperienceMap build pass. `corpus-installation.json` records the refreshed installed corpus and its predecessor.

Hash, Fabric, Ingest and Compress are installed with verified signatures and rollback copies (`installation.json`). Ingest and Compress build rules now refresh and explicitly link the shared archive, so these consumers receive later library changes. The native tool receipts are:

- Hash `ab264946-c9ed-4828-bb84-b3b4e36d938d`, computing the installed Fabric digest.
- Ingest `9060e510-6483-4b89-afdf-fae49d15c422`, creating the retained synthetic intake.
- Compress `05cded3d-9670-428c-9977-d3f3c335ede0`, compiling four fixture passports into a local registry.
- Fabric `e2cdabf1-f59a-461f-8bca-2a8d8edf8c5f`, reading the managed Hash reference.

`owner-readback.json` independently checks the retained intake identities and the registry's source-generation digest. Other statically linked consumers still need rebuilding. The portable branch was tested on macOS, not on a Linux host. This change does not establish source immutability, fix every normalizer's write-error handling, or make intake publication atomic. No public service was restarted or deployed.
