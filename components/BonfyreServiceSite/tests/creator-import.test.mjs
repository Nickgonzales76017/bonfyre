import {test, expect} from 'bun:test';
import {readFileSync} from 'node:fs';
import {decodeWav, prepareImports, renderClips, validateClips, audioDigest} from '../creator-import.mjs';
import {wavBytes} from '../creator-sample.mjs';
import {createCreatorMixer} from '../../../Aurekai/src/creator-wasm.mjs';
const wav = (rate = 8000, channels = 1, frames = rate) => wavBytes(new Float32Array(frames * channels).fill(.25), {sample_rate: rate, channels, frames});
const file = (bytes = wav(), name = 'Voice.wav') => ({bytes, name});
const mixer = await createCreatorMixer(readFileSync('Aurekai/native/creator/mix_kernel.wasm'));

test('WAV decode retains exact PCM and duplicates mono into stereo', () => {
  const decoded = decodeWav(wav());
  expect(decoded.frames).toBe(8000); expect(decoded.originalChannels).toBe(1); expect(decoded.sample_rate).toBe(8000);
  expect(decoded.samples.length).toBe(16000); expect(decoded.samples.every(n => n === .25)).toBe(true);
});
test('truncated, forged, unsupported and oversized WAV headers are refused before decoding', () => {
  const source = wav();
  expect(() => decodeWav(source.slice(0, -1))).toThrow();
  for (const [offset, value] of [[20, 6], [22, 8], [32, 77], [34, 24]]) {
    const bytes = source.slice(0); new DataView(bytes).setUint16(offset, value, true); expect(() => decodeWav(bytes)).toThrow();
  }
  expect(() => decodeWav(source, 100)).toThrow();
  const tooLong = wav(8000, 1, 8000 * 31); expect(() => decodeWav(tooLong)).toThrow();
});
test('float32 WAV validates nonfinite and out-of-range samples', () => {
  const bytes = new ArrayBuffer(48), v = new DataView(bytes), original = new Uint8Array(wav(8000, 1, 2));
  new Uint8Array(bytes).set(original.subarray(0, 44));
  v.setUint16(20, 3, true); v.setUint16(34, 32, true); v.setUint16(32, 4, true); v.setUint32(28, 32000, true); v.setFloat32(44, .125, true);
  expect(decodeWav(bytes).samples[0]).toBe(.125);
  v.setFloat32(44, NaN, true); expect(() => decodeWav(bytes)).toThrow('invalid');
  v.setFloat32(44, 17, true); expect(() => decodeWav(bytes)).toThrow('out-of-range');
});
test('imports enforce same rate, population and source hash before session replacement', async () => {
  await expect(prepareImports([file(), file(wav(16000), 'Bed.wav')])).rejects.toThrow('same sample rate');
  await expect(prepareImports(Array.from({length: 9}, () => file()))).rejects.toThrow('eight');
  await expect(prepareImports([{...file(), sha256: '0'.repeat(64)}])).rejects.toThrow('changed');
  const imported = await prepareImports([file()]); expect(imported.sources[0].sha256).toBe(await audioDigest(wav()));
});
test('trim, shift, gain and mute render exact independently calculated samples', async () => {
  const {sources, clips} = await prepareImports([file(), file(wav(), 'Second.wav')]);
  clips[0] = {...clips[0], at: 4000, from: 2000, frames: 4000, gain: .8};
  const rendered = renderClips(mixer, sources, clips);
  expect(rendered.frames).toBe(8000); expect(rendered.clipped).toBe(0);
  for (let f = 0; f < 8000; f++) {
    const expected = Math.fround(Math.fround(f >= 4000 ? Math.fround(.25 * Math.fround(.8)) : 0) + Math.fround(.25 * .5));
    if (rendered.output[f * 2] !== expected || rendered.output[f * 2 + 1] !== expected) throw Error('Independent sample mismatch at '+f);
  }
  clips[1].muted = true; expect(renderClips(mixer, sources, clips).output[0]).toBe(0);
  clips[0].frames = 9000; expect(() => validateClips(clips, sources)).toThrow('inside its source');
});
test('clipping is reported before PCM export and invalid edits leave inputs unchanged', async () => {
  const files = Array.from({length: 5}, (_, i) => file(wav(), `track${i}.wav`)), {sources, clips} = await prepareImports(files);
  clips.forEach(c => c.gain = 1);
  const result = renderClips(mixer, sources, clips);
  expect(result.peak).toBe(1.25); expect(result.clipped).toBe(16000);
  const before = sources[0].samples.slice(); clips[0].at = 240000;
  expect(() => renderClips(mixer, sources, clips)).toThrow(); expect(sources[0].samples).toEqual(before);
});
