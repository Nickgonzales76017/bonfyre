import {test, expect} from 'bun:test';
import {mkdtempSync, readFileSync, writeFileSync, cpSync, rmSync} from 'node:fs';
import {join, resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';
import {creatorFiles} from '../creator-material.mjs';
import {validateSample, validateGains, wavBytes, renderSample} from '../creator-sample.mjs';
import {createCreatorMixer} from '../../../Aurekai/src/creator-wasm.mjs';
import {renderSite} from '../build.mjs';
const directory = resolve(import.meta.dir, '../../../packs/aurekai-work-samples/audio');
const sha = bytes => createHash('sha256').update(bytes).digest('hex');

test('published audio package is allowlisted, verified and byte-exact with the native fixture', async () => {
  const packed = await creatorFiles(directory), {sample, asset_prefix} = packed;
  expect(asset_prefix).toMatch(/^v-[a-f0-9]{64}\/$/);
  expect(Object.keys(packed.files).every(name => name.startsWith(asset_prefix))).toBe(true);
  const files = Object.fromEntries(Object.entries(packed.files).map(([name, body]) => [name.slice(asset_prefix.length), body]));
  expect(Object.keys(files).sort()).toEqual(['chime.f32', 'creator-client.mjs', 'creator-import-client.mjs', 'creator-import.mjs', 'creator-sample.mjs', 'creator-timeline-model.mjs', 'creator-timeline.mjs', 'creator-wasm.mjs', 'creator-worker.mjs', 'harmony.f32', 'mix_kernel.wasm', 'pulse.f32', 'sample.json', 'store.mjs', 'vendor/wavesurfer-8.0.2/LICENSE', 'vendor/wavesurfer-8.0.2/regions.esm.js', 'vendor/wavesurfer-8.0.2/wavesurfer.esm.js', 'workspace-navigation.mjs']);
  expect(JSON.stringify(sample)).not.toContain('/Users/');
  const mixer = await createCreatorMixer(files['mix_kernel.wasm']);
  const sources = sample.stems.map(s => new Float32Array(new Uint8Array(files[s.file]).buffer));
  const mixed = renderSample(mixer, sample, sources, sample.stems.map(s => s.gain));
  expect(sha(new Uint8Array(mixed.buffer))).toBe(sample.native_sha256);
  expect(renderSample(mixer, sample, sources, [0, 0, 0]).every(n => Object.is(n, 0))).toBe(true);
  const wav = new DataView(wavBytes(mixed, sample));
  expect(wav.byteLength).toBe(44 + 576000 * 2); expect(wav.getUint32(24, true)).toBe(48000);
  expect(wav.getUint16(22, true)).toBe(2); expect(wav.getUint32(40, true)).toBe(576000 * 2);
  for (let i = 0; i < mixed.length; i++) {
    const v = Math.max(-1, Math.min(1, mixed[i]));
    if (wav.getInt16(44 + i * 2, true) !== Math.round(v * (v < 0 ? 32768 : 32767))) throw Error(`WAV sample ${i}`);
  }
  expect(sample.stems.reduce((n, s) => n + files[s.file].length, 0)).toBe(1536000);
});

test('source drift and incorrect native reference refuse packaging', async () => {
  const temp = mkdtempSync(join(tmpdir(), 'creator-material-'));
  try {
    cpSync(directory, temp, {recursive: true});
    const file = join(temp, 'pulse.f32'), original = readFileSync(file), changed = Buffer.from(original); changed[0] ^= 1; writeFileSync(file, changed);
    await expect(creatorFiles(temp)).rejects.toThrow('source drift');
    writeFileSync(file, original);
    const spec = JSON.parse(readFileSync(join(temp, 'sample.json'))); spec.native_sha256 = '0'.repeat(64); writeFileSync(join(temp, 'sample.json'), JSON.stringify(spec));
    await expect(creatorFiles(temp)).rejects.toThrow('native reference');
  } finally {rmSync(temp, {recursive: true, force: true});}
});

test('invalid source paths, frames, gains and oversized samples are refused', () => {
  const sample = JSON.parse(readFileSync(join(directory, 'sample.json')));
  for (const change of [s => s.stems[0].file = '../secret', s => s.stems[0].at = -1,
    s => s.stems[0].frames = s.frames + 1, s => s.stems[0].gain = NaN, s => s.frames = 96_000 * 31,
    s => s.stems[1].id = s.stems[0].id, s => s.channels = 0]) {
    const copy = structuredClone(sample); change(copy); expect(() => validateSample(copy)).toThrow();
  }
  for (const gains of [[NaN, 0, 0], [0, -1, 0], [0, 0, 1.01], [0], null]) expect(() => validateGains(gains, sample)).toThrow();
});

test('audio uses the existing workspace and home-page discovery', async () => {
  const content = JSON.parse(readFileSync(resolve(directory, '../site/content.json')));
  const page = content.pages.find(p => p.path === 'audio');
  Object.assign(page.sections[0], await creatorFiles(directory), {data: '/data/generated-audio/'});
  // Render only this workspace alongside a minimal home page, avoiding unrelated materials.
  content.pages = [page]; const samples = content.sections.find(s => s.id === 'samples'); samples.items = samples.items.filter(s => s.href === '/audio/'); content.sections = [content.sections[0], samples, content.sections.at(-1)]; content.sections[0].reel = undefined; content.navigation = [{label: 'Samples', href: '/#samples'}, {label: 'Contact', href: '/#ask'}];
  const built = renderSite(content); const audio = built.pages.find(p => p.path === 'audio');
  expect(audio.html).toContain('data-creator'); expect(audio.html).toContain('Download WAV');
  expect(audio.html).toContain('Save levels'); expect(audio.html).toContain('href="/#samples"');
});
