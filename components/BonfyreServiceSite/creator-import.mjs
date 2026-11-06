// Bounded WAV input and clip scheduling for the existing native/WASM mixer.
export const importLimits = {tracks: 8, seconds: 30, bytes: 24 * 1024 * 1024, pcm: 32 * 1024 * 1024};
export const audioDigest = async bytes => [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))].map(n => n.toString(16).padStart(2, '0')).join('');

export function decodeWav(bytes, remainingPCM = importLimits.pcm) {
  const fail = () => {throw Error('Use a complete PCM 16-bit or float32 WAV, mono or stereo, up to 30 seconds.');};
  if (!(bytes instanceof ArrayBuffer) || bytes.byteLength < 44 || bytes.byteLength > importLimits.bytes) fail();
  const v = new DataView(bytes), text = (at, count) => String.fromCharCode(...new Uint8Array(bytes, at, count));
  if (text(0, 4) !== 'RIFF' || text(8, 4) !== 'WAVE' || v.getUint32(4, true) + 8 !== bytes.byteLength) fail();
  let format, payload;
  for (let at = 12; at < bytes.byteLength;) {
    if (at + 8 > bytes.byteLength) fail();
    const name = text(at, 4), size = v.getUint32(at + 4, true), start = at + 8, end = start + size;
    if (end > bytes.byteLength) fail();
    if (name === 'fmt ') {if (format || size < 16) fail(); format = {kind: v.getUint16(start, true), channels: v.getUint16(start + 2, true), rate: v.getUint32(start + 4, true), byteRate: v.getUint32(start + 8, true), block: v.getUint16(start + 12, true), bits: v.getUint16(start + 14, true)};}
    if (name === 'data') {if (payload || !size) fail(); payload = {start, size};}
    at = end + (size % 2); if (at > bytes.byteLength) fail();
  }
  if (!format || !payload) fail();
  const {kind, channels, rate, byteRate, block, bits} = format;
  if (!(kind === 1 && bits === 16 || kind === 3 && bits === 32) || ![1, 2].includes(channels) || rate < 8000 || rate > 96000 ||
      block !== channels * bits / 8 || byteRate !== rate * block || payload.size % block) fail();
  const frames = payload.size / block;
  if (frames < 1 || frames > rate * importLimits.seconds || frames * 2 * 4 > remainingPCM) fail();
  // Mono is duplicated into stereo; each original sample remains in the retained WAV.
  const samples = new Float32Array(frames * 2);
  for (let f = 0; f < frames; f++) for (let ch = 0; ch < 2; ch++) {
    const at = payload.start + f * block + Math.min(ch, channels - 1) * bits / 8;
    const value = kind === 1 ? v.getInt16(at, true) / 32768 : v.getFloat32(at, true);
    if (!Number.isFinite(value) || Math.abs(value) > 16) throw Error('The WAV contains invalid or out-of-range samples.');
    samples[f * 2 + ch] = value;
  }
  return {samples, frames, channels: 2, originalChannels: channels, sample_rate: rate};
}

export function validateClips(clips, sources) {
  if (!Array.isArray(clips) || clips.length !== sources.length || !clips.length || clips.length > importLimits.tracks) throw Error('Choose one to eight WAV tracks.');
  const rate = sources[0].sample_rate;
  if (sources.some(s => s.sample_rate !== rate)) throw Error('Choose WAV files with the same sample rate.');
  let frames = 1;
  clips.forEach((clip, i) => {
    if (![clip.at, clip.from, clip.frames].every(Number.isSafeInteger) || clip.at < 0 || clip.from < 0 || clip.frames < 1 || clip.from + clip.frames > sources[i].frames || clip.at + clip.frames > rate * importLimits.seconds ||
        !Number.isFinite(clip.gain) || clip.gain < 0 || clip.gain > 1 || typeof clip.muted !== 'boolean') throw Error('Keep each clip inside its source and the 30-second timeline; levels must be from 0 to 1.');
    frames = Math.max(frames, clip.at + clip.frames);
  });
  if (sources.reduce((n, s) => n + s.samples.byteLength, 0) > importLimits.pcm) throw Error('Decoded tracks exceed the 32 MB session limit.');
  return {sample_rate: rate, channels: 2, frames};
}

export function renderClips(mixer, sources, clips) {
  const format = validateClips(clips, sources), output = new Float32Array(format.frames * format.channels);
  for (let at = 0; at < format.frames; at += 1024) {
    const frames = Math.min(1024, format.frames - at), stems = [];
    clips.forEach((clip, i) => {
      const lo = Math.max(at, clip.at), hi = Math.min(at + frames, clip.at + clip.frames);
      if (lo < hi && !clip.muted) stems.push({at: lo - at, gain: clip.gain, samples: sources[i].samples.subarray((clip.from + lo - clip.at) * 2, (clip.from + hi - clip.at) * 2)});
    });
    output.set(mixer.mixBlock({frames, channels: 2, stems}), at * 2);
  }
  let peak = 0, clipped = 0;
  for (const n of output) {peak = Math.max(peak, Math.abs(n)); if (Math.abs(n) > 1) clipped++;}
  return {...format, output, peak, clipped};
}

export async function prepareImports(files) {
  if (!Array.isArray(files) || !files.length || files.length > importLimits.tracks || files.reduce((n, f) => n + (f.bytes?.byteLength ?? Infinity), 0) > importLimits.bytes) throw Error('Choose up to eight WAV files, at most 24 MB combined.');
  const sources = []; let remainingPCM = importLimits.pcm;
  for (const file of files) {
    if (typeof file.name !== 'string' || !file.name.trim() || file.name.length > 255) throw Error('Invalid audio filename.');
    const source = decodeWav(file.bytes, remainingPCM), sha256 = await audioDigest(file.bytes); remainingPCM -= source.samples.byteLength;
    if (file.sha256 && file.sha256 !== sha256) throw Error('A saved audio source has changed.');
    const peaks = Array.from({length: 128}, (_, bin) => {
      let peak = 0;
      for (let f = Math.floor(bin * source.frames / 128); f < Math.floor((bin + 1) * source.frames / 128); f++) peak = Math.max(peak, Math.abs(source.samples[f * 2]), Math.abs(source.samples[f * 2 + 1]));
      return peak;
    });
    sources.push({...source, name: file.name, sha256, peaks});
  }
  const clips = sources.map(s => ({at: 0, from: 0, frames: s.frames, gain: .5, muted: false}));
  validateClips(clips, sources);
  return {sources, clips};
}
