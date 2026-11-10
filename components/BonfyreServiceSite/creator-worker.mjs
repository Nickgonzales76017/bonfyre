import {createCreatorMixer} from './creator-wasm.mjs';
import {validateSample, renderSample} from './creator-sample.mjs';
import {prepareImports, renderClips, validateClips} from './creator-import.mjs';

const digest = async bytes => [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))].map(n => n.toString(16).padStart(2, '0')).join('');
async function load(name, bytes, sha) {
  const response = await fetch(new URL(name, import.meta.url));
  if (!response.ok) throw Error('The audio could not be loaded. Reload to try again.');
  const reader = response.body.getReader(), chunks = []; let length = 0;
  for (;;) { const part = await reader.read(); if (part.done) break; length += part.value.byteLength;
    if (length > bytes) { await reader.cancel(); throw Error('The audio file is larger than expected.'); } chunks.push(part.value); }
  const body = new Uint8Array(length); let at = 0; for (const chunk of chunks) {body.set(chunk, at); at += chunk.length;}
  if (length !== bytes || await digest(body) !== sha) throw Error('The audio could not be verified. Reload to try again.');
  return body.buffer;
}
let ready, imported;
async function prepare(sample) {
  validateSample(sample);
  const [bytes, ...bodies] = await Promise.all([load('mix_kernel.wasm', sample.kernel_bytes, sample.kernel_sha256),
    ...sample.stems.map(s => load(s.file, s.frames * sample.channels * 4, s.sha256))]);
  return {sample, mixer: await createCreatorMixer(bytes), sources: bodies.map(body => new Float32Array(body))};
}
self.onmessage = async ({data}) => {
  const id = data?.id;
  try {
    if (!Number.isSafeInteger(id) || id < 0) throw Error('Invalid render request.');
    if (data.type === 'init') { if (ready) throw Error('Audio is already initialized.'); ready = prepare(data.sample); }
    else if (!['render', 'import', 'import-render'].includes(data.type) || !ready) throw Error('Audio is not ready.');
    const {sample, mixer, sources} = await ready;
    const start = performance.now();
    if (data.type === 'import' || data.type === 'import-render') {
      if (data.type === 'import') {
        const candidate = await prepareImports(data.files);
        if (data.clips) validateClips(data.clips, candidate.sources);
        candidate.clips = data.clips ?? candidate.clips;
        candidate.session = id;
        // Refused imports leave the previous session available.
        imported = candidate;
      }
      if (!imported) throw Error('Import audio before editing.');
      if (data.type === 'import-render' && data.session !== imported.session) throw Error('The audio session has changed. Reload your saved session.');
      const clips = data.type === 'import' ? imported.clips : data.clips;
      const result = renderClips(mixer, imported.sources, clips);
      self.postMessage({id, session: imported.session, type: 'import-rendered', format: {frames: result.frames, channels: result.channels, sample_rate: result.sample_rate},
        peak: result.peak, clipped: result.clipped, clips, sources: imported.sources.map(({name, sha256, frames, sample_rate, originalChannels, peaks}) => ({name, sha256, frames, sample_rate, originalChannels, peaks})),
        sha256: await digest(result.output.buffer), render_ms: performance.now() - start, buffer: result.output.buffer}, [result.output.buffer]);
      return;
    }
    const output = renderSample(mixer, sample, sources, data.type === 'init' ? sample.stems.map(s => s.gain) : data.gains);
    const sha256 = await digest(output.buffer);
    if (data.type === 'init' && sha256 !== sample.native_sha256) throw Error('The mix does not match its verified source.');
    self.postMessage({id, type: 'rendered', sha256, render_ms: performance.now() - start, buffer: output.buffer}, [output.buffer]);
  } catch (error) { self.postMessage({id, type: 'error', message: error.message}); }
};
