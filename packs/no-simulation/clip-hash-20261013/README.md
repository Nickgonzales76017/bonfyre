# Bounded clip receipt hashing

The installed clip exporter now feeds SHA-256 through one reusable 1 MiB buffer. Previously it loaded each source video, rendered video and optional logo completely into memory. The native Clips command still invokes the existing packaged exporter and media tools.

The hash opens a regular file, retries interrupted reads, handles partial buffers and closes its descriptor on success or failure. It checks the byte count and descriptor/path metadata before returning a digest. A read error, truncation or detected replacement refuses the receipt. These checks cover hashing; they do not establish that the source stayed unchanged throughout the earlier render.

## Measurements

`benchmark.json` retains five alternating runs against a 256 MiB fixture with a partial final buffer. Every digest matches independent `shasum` output. Times include process startup; memory is the maximum resident set reported by `/usr/bin/time -l`.

| Hash implementation | Median elapsed time | Median reported maximum RSS |
| --- | ---: | ---: |
| Previous installed whole-file read | 0.390 s | 285.4 MiB |
| Bounded read | 0.247 s | 32.8 MiB |

This is a hash-stage benchmark, not an end-to-end video-export speedup. `export-before.mjs` preserves the previous module. `hash-before.mjs` exposes its unchanged hash function only for measurement; `hash-runner.mjs` invokes either implementation. The benchmark records source digests and fixture construction.

## Verification

- Hash tests pass 28 assertions against the source and installed module. They cover empty files and buffer boundaries, short reads, interrupted reads, I/O failure, truncation, replacement and descriptor cleanup. Fault cases run in isolated child processes with controlled filesystem calls.
- Export tests pass 22 assertions, including a real one-second portrait render, full decode, caption boundaries, output refusal and receipt digests checked with `shasum`. The retained `render/preview.png` was visually checked; it shows synthetic footage and the expected second caption.
- MCP receipt `1556ad1f-0fa8-40a7-8197-12f5db1123b8` records an export through installed `BonfyreClips`. Its source and rendered-file hashes were independently checked. The resulting video and receipt remain in `native-fixture/render/`.
- Atlas validation and ExperienceMap checks pass. The installed corpus was refreshed.

`installation.json` records the replaced module's digest and rollback copy. The native launcher binary was unchanged. No public service was restarted and no client footage was used. Approximately 5.8 GiB of disk remained available after verification.
