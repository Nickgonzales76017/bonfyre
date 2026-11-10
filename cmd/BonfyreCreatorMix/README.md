# Native Creator audio

`bonfyre-creator-mix` renders a bounded window of interleaved float32 little-endian audio. It accepts up to 64 stems and eight channels, with sample-frame offsets and gains. It preserves caller stem order, refuses nonfinite output and publishes without replacing an existing file. `--sparse-silence` stores unoccupied blocks as holes while preserving their exact logical bytes.

Build and verify from the repository root:

```sh
make -C cmd/BonfyreCreatorMix test
```

The implementation and its upstream tests come from [Creator commit 1187f373](https://github.com/Nickgonzales76017/bonfyre/commit/1187f3736ebbdeba3b70edb39f04de15c752cd5c). The local build forces ordinary float32 arithmetic with contraction disabled. The source digests are retained in `packs/no-simulation/creator-upstream-20261013/sources.json`.

Compile metadata and prepare an invocation through this checkout's CLI:

```sh
node Aurekai/bin/akai.mjs creator:compile --input timeline.json --shard-frames 180 --source-projection used > plan.json
node Aurekai/bin/akai.mjs creator:pcm-plan --plan plan.json --bindings bindings.json --window-index 0 --channels 2 --output /absolute/path/mix.f32 --sparse-silence > candidate.json
```

These commands read metadata only. The candidate records unverified source binding claims and does not run the mixer. An execution owner must verify the source bytes and have permission to read the media and create the output. The standalone native executable reports `materialized_unverified`; it does not enforce source digests or media rights.

`creator:edit --input timeline.json --edit edit.json` returns a candidate timeline for a revision-bound move, trim, gain, opacity, remove or duplicate operation. It does not change the input file. The global `akai` executable currently points to a separate checkout and was not changed by this integration.

Generate the synthetic six-second example in a fresh directory:

```sh
node cmd/BonfyreCreatorMix/tools/demo.mjs /tmp/creator-demo-new
bun cmd/BonfyreCreatorMix/tests/browser.mjs /tmp/creator-demo-new
```

The generator needs Node, the built mixer and FFmpeg. It verifies source digests before and after rendering, compares every mixed float32 sample to an independent reference, compares dense and sparse bytes, and writes a WAV with a browser preview. The browser check uses installed Chrome, tests playback and captures desktop and phone screenshots. The retained run is in `packs/no-simulation/creator-upstream-20261013/demo/`.

This is offline PCM mixing and timeline compilation. Codecs, effects, recording, an interactive editor, playback rights and public delivery remain separate work. The shared kernel also runs in the browser, with exact native/WASM parity checked by the required tests.


## Browser mixer

`Aurekai/src/creator-wasm.mjs` exposes `createCreatorMixer(bytes)`. Its `mixBlock` method accepts one to 1024 frames, one to eight channels and up to 64 ordered stems. Each stem supplies an interleaved `Float32Array`, gain from -4 to 4 and an optional frame offset `at` within the block. The result is an independent `Float32Array`. Invalid bounds, shared input buffers and nonfinite mixed samples are refused. Media loading, timeline scheduling and playback belong to the caller.

The 259-byte WASM artifact uses the unchanged native C kernel, has no host imports and cannot grow its 128 KiB linear memory. Returned blocks and caller-owned media consume additional JavaScript memory. The source and artifact hashes are pinned in `Aurekai/native/creator/wasm.lock.json`. Normal tests verify those hashes without requiring a compiler; rebuilding requires the already installed Zig 0.13.0:

```sh
make -C cmd/BonfyreCreatorMix wasm
bun cmd/BonfyreCreatorMix/tests/wasm-browser.mjs
bun cmd/BonfyreCreatorMix/tests/wasm-browser.mjs --serve
```

The last command prints a local URL for the interactive sample. It uses the retained six-second synthetic fixture by default; pass a freshly generated demo directory to use another copy. Gain controls remix the source PCM through WASM, and Web Audio plays the result. Initial output must match every byte of the native fixture before playback is enabled. The automated browser check verifies mute, reset, playback, stop and desktop/phone layouts. Receipts and screenshots are in `packs/no-simulation/creator-wasm-20261013/`.

This is an offline browser mixer. It does not claim AudioWorklet latency, zero-copy media, SIMD acceleration or a real-time audio engine.
