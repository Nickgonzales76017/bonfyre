# One-pass compression measurement

The native `savings` command now counts a tar stream while compressing those same bytes through the already-linked libzstd. It counts the completed frame's output in memory and discards each output chunk. This removes the second archive read, the zstd subprocess and the temporary compressed file. Input and output buffers use libzstd's recommended stream sizes; the codec retains its own level-19 working memory.

The compressor uses level 19, a checksum and one worker, matching the current zstd CLI defaults used by family archives. Builds without libzstd headers retain the existing CLI fallback. Family archive creation itself is unchanged. The separate raw-byte metadata walk still assumes a stable directory; this is not a filesystem snapshot.

## Measurement

Five runs per binary and fixture alternated execution order. Times include command startup. `benchmark.json` retains every run, output measurement, binary digest and `/usr/bin/time -l` maximum-resident-set report. Raw, archive-stream and compressed-byte counts agree across every final comparison.

| Fixture | Previous median | New median | Previous reported max RSS | New reported max RSS |
| --- | ---: | ---: | ---: | ---: |
| 2,000 files, 4,644,500 content bytes | 2.481 s | 1.179 s | 93.3 MiB | 91.9 MiB |
| 64 MiB file, repeated 1 MiB block | 0.559 s | 0.555 s | 230.7 MiB | 147.3 MiB |

The small-file fixture was about 2.1 times as fast. The large fixture's elapsed time was effectively unchanged, with lower reported memory. These local synthetic workloads do not establish production throughput or whole-system peak memory.

An earlier single-thread library configuration changed compressed sizes. Its measurements remain in `benchmark-single-thread-exploration.json` and do not describe the installed result. The final larger-stream test verifies the CLI's worker-mode frame size as well as the small and empty cases.

## Verification and installation

`make -C cmd/BonfyreCompress test` passes the physical compiler, family pipeline, record factoring, savings accounting and streaming checks. Four streaming tests pass 23 assertions against the installed binary. They use real tar and zstd as an independent oracle, count tar invocations, detect any zstd subprocess, and require failed or missing producers to return no successful measurement. The previous installed build fails both single-pass assertions. A separate build without libzstd headers passes the existing savings-accounting checks.

Atlas validation passes. ExperienceMap compilation and corpus checks passed, but its exhaustive definition test exceeded its five-second timeout while benchmarks were running. The isolated `native-definition-witness` rerun passes both definition and owner-resolution suites; both logs are retained. The installed corpus was refreshed afterward.

`installation.json` records the installed digest and original rollback binary. MCP receipt `ba96d472-d355-45f6-8251-6d6a947a277b` reports an exact checkout/installed digest match and a successful measurement of the retained `smoke-family` fixture. This installation does not restart public services.

## REA evidence

The preceding investigation's `../reference-disclosure-20261013/rea-compress-savings.json` contains Evidence `ev_4ea360465340edbff1695306027a395f2e435ffe43f62c3e6491aada6f1cfa3f`. Ghidra recovered the installed `_cmd_savings` read loop and its later `_run_tar_zstd` call. Source inspection established the duplicate archive pass; the tests and measurements here verify the replacement. Static decompilation alone did not establish the speedup.
