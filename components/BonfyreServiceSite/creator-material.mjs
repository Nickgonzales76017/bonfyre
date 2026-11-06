import {readFileSync} from 'node:fs';
import {join, resolve} from 'node:path';
import {createHash} from 'node:crypto';
import {verifyCreatorWasm} from '../../cmd/BonfyreCreatorMix/tools/wasm.mjs';
import {createCreatorMixer} from '../../Aurekai/src/creator-wasm.mjs';
import {validateSample, renderSample} from './creator-sample.mjs';

export async function creatorFiles(directory) {
  const sample = validateSample(JSON.parse(readFileSync(join(directory, 'sample.json'))));
  const {lock, bytes} = verifyCreatorWasm(), files = {'mix_kernel.wasm': bytes}, sources = [];
  const hash = body => createHash('sha256').update(body).digest('hex');
  for (const stem of sample.stems) {
    const body = readFileSync(join(directory, stem.file));
    if (body.length !== stem.frames * sample.channels * 4 || hash(body) !== stem.sha256) throw Error(`Audio source drift: ${stem.id}`);
    files[stem.file] = body;
    sources.push(new Float32Array(body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength)));
  }
  const reference = renderSample(await createCreatorMixer(bytes), sample, sources, sample.stems.map(s => s.gain));
  if (hash(new Uint8Array(reference.buffer)) !== sample.native_sha256) throw Error('Audio does not match the native reference.');
  for (const name of ['creator-client.mjs', 'creator-worker.mjs', 'creator-sample.mjs', 'creator-import.mjs', 'creator-import-client.mjs', 'creator-timeline.mjs', 'creator-timeline-model.mjs', 'workspace-navigation.mjs', 'vendor/wavesurfer-8.0.2/wavesurfer.esm.js', 'vendor/wavesurfer-8.0.2/regions.esm.js', 'vendor/wavesurfer-8.0.2/LICENSE']) files[name] = readFileSync(join(import.meta.dir, name));
  files['creator-wasm.mjs'] = readFileSync(resolve(import.meta.dir, '../../Aurekai/src/creator-wasm.mjs'));
  files['store.mjs'] = readFileSync(resolve(import.meta.dir, '../../integrations/browser/extension/store.mjs'));
  const descriptor = {...sample, kernel_sha256: lock.artifact_sha256, kernel_bytes: bytes.length};
  files['sample.json'] = Buffer.from(JSON.stringify(descriptor) + '\n');
  const identity = Object.fromEntries(Object.keys(files).sort().map(name => [name, hash(files[name])]));
  const asset_prefix = `v-${hash(JSON.stringify(identity))}/`;
  return {sample: descriptor, asset_prefix, files: Object.fromEntries(Object.entries(files).map(([name, body]) => [asset_prefix + name, body]))};
}
