import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync, readFileSync, writeFileSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';
import {spawnSync} from 'node:child_process';
import {createCreatorMixer} from '../src/creator-wasm.mjs';
import {verifyCreatorWasm} from '../../cmd/BonfyreCreatorMix/tools/wasm.mjs';

const {bytes} = verifyCreatorWasm();
test('WASM has no host imports and cannot grow its 128 KiB memory', async () => {
  const module = await WebAssembly.compile(bytes);
  assert.deepEqual(WebAssembly.Module.imports(module), []);
  const instance = await WebAssembly.instantiate(module);
  assert.throws(() => instance.exports.memory.grow(1), RangeError);
});

test('WASM refuses malformed blocks, nonfinite audio and overflow, then recovers', async () => {
  const mixer = await createCreatorMixer(bytes);
  const stem = {samples: new Float32Array([1]), gain: 1};
  const valid = {frames: 1, channels: 1, stems: [stem]};
  for (const override of [
    {frames: 0}, {frames: 1025}, {frames: 1.5}, {channels: 0}, {channels: 9},
    {stems: Array(65).fill(stem)}, {stems: [null]},
    {stems: [{...stem, at: -1}]}, {stems: [{...stem, at: 1}]},
    {stems: [{...stem, gain: NaN}]}, {stems: [{...stem, gain: 4.01}]},
    {stems: [{...stem, samples: [1]}]},
    {stems: [{...stem, samples: new Float32Array(0)}]},
    {stems: [{...stem, samples: new Float32Array(2)}]},
    {channels: 2},
    ...[NaN, Infinity, -Infinity].flatMap(n => [0, 1].map(gain => ({stems: [{samples: new Float32Array([n]), gain}]}))),
    {stems: [{samples: new Float32Array([3.4e38]), gain: 4}]},
    {stems: [{samples: new Float32Array(new SharedArrayBuffer(4)), gain: 1}]}
  ]) assert.throws(() => mixer.mixBlock({...valid, ...override}), RangeError);
  assert.deepEqual([...mixer.mixBlock(valid)], [1]);
  const retained = mixer.mixBlock(valid);
  const silent = mixer.mixBlock({frames: 1024, channels: 8, stems: []});
  assert(silent.every(n => Object.is(n, 0)));
  assert.deepEqual([...retained], [1]);
});

test('native and WASM preserve every byte across channels, offsets, overlaps and 64 ordered stems', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'creator-wasm-parity-'));
  try {
    const source = resolve(import.meta.dirname, '../native/creator');
    const exe = join(dir, 'mix');
    const build = spawnSync(process.env.CC || 'cc', ['-O3', '-fno-fast-math', '-ffp-contract=off',
      join(source, 'mix.c'), join(source, 'mix_kernel.c'), '-lm', '-o', exe], {encoding: 'utf8'});
    assert.equal(build.status, 0, build.stderr);
    const mixer = await createCreatorMixer(bytes);
    for (const channels of [1, 2, 8]) {
      const frames = 3179;
      const samples = new Float32Array(4000 * channels);
      for (let i = 0; i < samples.length; i++) samples[i] = Math.sin(i * 1.718) * 0.67;
      const input = join(dir, `input-${channels}.f32`), output = join(dir, `output-${channels}.f32`);
      const original = Buffer.from(samples.buffer).slice();
      writeFileSync(input, original);
      const stems = Array.from({length: 64}, (_, i) => ({at: (i * 127) % 2100, from: i * 3,
        frames: 10 + i * 11, gain: Math.fround((i % 9 - 4) / 3.19)}));
      const argv = ['--samples', String(frames), '--channels', String(channels), '--output', output];
      for (const s of stems) argv.push('--stem', input, String(s.at), String(s.from), String(s.frames), String(s.gain));
      const run = spawnSync(exe, argv, {encoding: 'utf8'});
      assert.equal(run.status, 0, run.stderr);
      const result = new Float32Array(frames * channels);
      for (let at = 0; at < frames; at += 1024) {
        const count = Math.min(1024, frames - at), active = [];
        for (const s of stems) {
          const lo = Math.max(at, s.at), hi = Math.min(at + count, s.at + s.frames);
          if (lo < hi) active.push({at: lo - at, gain: s.gain,
            samples: samples.subarray((s.from + lo - s.at) * channels, (s.from + hi - s.at) * channels)});
        }
        result.set(mixer.mixBlock({frames: count, channels, stems: active}), at * channels);
      }
      assert.deepEqual(Buffer.from(result.buffer), readFileSync(output), `channels=${channels}`);
      assert.deepEqual(Buffer.from(samples.buffer), original);
    }
  } finally { rmSync(dir, {recursive: true, force: true}); }
});
