# Creator browser mixer evidence

The unchanged native kernel compiles to 259 bytes with installed Zig 0.13.0. `Aurekai/native/creator/wasm.lock.json` pins compiler flags, sources and artifact. `node cmd/BonfyreCreatorMix/tools/wasm.mjs --build` reproduced the artifact exactly.

`browser-verification.json` records Chrome byte parity with all 576,000 float32 values in the existing six-second synthetic native fixture, gain mute/reset, playback and stop, desktop/phone layout and script exception checks. Screenshots show both widths. The single first render measured 11.2 ms; no general performance or real-time latency claim follows.

`aurekai-tests.log` records 77 passing tests, none skipped. `atlas.log` and `experience-map.log` record registry checks. `corpus-installation.json` retains the previous and current installed corpus and all eight verified file digests. `verification.json` records changed source digests and final available space.

Run `bun cmd/BonfyreCreatorMix/tests/wasm-browser.mjs --serve` from the repository root to open the local sample at the printed URL. The native fixture remains in `packs/no-simulation/creator-upstream-20261013/demo/`. No public deployment was made.
