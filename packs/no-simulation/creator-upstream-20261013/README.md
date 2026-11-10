# Creator integration, 2026-10-13

The GitHub review found that Creator commit [1187f373](https://github.com/Nickgonzales76017/bonfyre/commit/1187f3736ebbdeba3b70edb39f04de15c752cd5c) had landed upstream while this checkout lacked its timeline compiler, PCM lowering and native mixer. Eight previously absent source and test files were imported without replacing existing local files. `sources.json` records their Git blob identities, SHA-256 digests and commit. Copies of the exact upstream bytes were retained under `Aurekai/` here; on 2026-10-27 they moved to `~/.bonfyre/archive/retired/creator-upstream-copies-20261027.tar.zst` (restore from the repository root with `zstd -dc --long=27 ~/.bonfyre/archive/retired/creator-upstream-copies-20261027.tar.zst | tar -xf -`).

The repository CLI now exposes `creator:compile`, `creator:edit` and `creator:pcm-plan`. These commands validate bounded metadata, preserve revision checks and return unexecuted candidates. Planning can succeed without source media present; it neither reads the media nor creates the requested output. The global `akai` command points to a separate checkout and was not changed.

`cmd/BonfyreCreatorMix` builds the upstream mixer with fast-math and contraction disabled. The signed binary is installed as `~/.bonfyre/bin/bonfyre-creator-mix` and discoverable through native MCP. `installation.json` records its digest. No previous mixer was replaced.

The retained synthetic demo contains three generated stereo stems and a six-second timeline. Its driver checks source digests before and after rendering, computes an independent float32 reference and compares every output sample. Dense and sparse outputs are byte-identical: `068bb4b051fbe6de2fc1936cdcf6d31e5ea518d0e84a0d972075cf4f8448f26f`. Both have 2,304,000 logical bytes; this filesystem reports 2,306,048 allocated bytes for dense output and 1,191,936 for sparse output. This fixture's allocation saving is not a general throughput measurement.

- [Listen to the generated mix](demo/mix.wav).
- [Phone preview](demo/preview-390.png) and [desktop preview](demo/preview-1440.png).
- [Sample verification](demo/verification.json) and [browser verification](demo/browser-verification.json).

The WAV is a 16-bit listening copy of the verified float32 mix. Chrome decodes it and advances playback without an audio error. The waveform reads the actual WAV; desktop and phone layouts fit their viewports with no script exception. Chrome's AudioContext resampled its decoded buffer to the device's 44.1 kHz rate; this is playback verification, not a browser float32 parity claim. The HTML preview needs HTTP serving for its waveform fetch.

The focused suite passes 20 tests with one WASM check skipped because the local wasm32/lld toolchain is absent. The full Aurekai suite passes 73 tests with that same skip. Atlas validation and the complete ExperienceMap build pass; `corpus-installation.json` records the refreshed installed corpus.

Native MCP receipt `5801d660-f6f9-4cc2-a4f6-62823c8063b5` renders the same synthetic stems through the installed mixer. `mcp-readback.json` confirms byte equality with the independent reference. The render records 190 active-stem probes rather than 846 potential probes, three explicit source seeks and 187 sequential seek elisions.

The upstream mixer remains an explicitly invoked, offline raw-PCM operation. It does not enforce source digests or media rights, and its receipt correctly remains `materialized_unverified`; verification is separate evidence from the demo driver. Codec decoding, recording, effects, an interactive editor and public delivery are not provided here. The demo is local and no public sample site was deployed.

The final disk audit found free space falling during the session while this evidence directory occupied about 8 MiB. Rebuildable Bun and REA package caches and an unused Cargo bare dependency cache were cleared; no Cargo checkouts or active Rust build were present. The active VS Code updater and installed REA toolchains were retained. `cache-reclaim.json` records the exact paths and free-space readings. The final check reports approximately 4.23 GiB available; cache allocation totals are not reclaimed-byte totals because package hard links can share storage.
