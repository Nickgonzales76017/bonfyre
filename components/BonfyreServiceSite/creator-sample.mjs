export function validateSample(s) {
  const int = (n, a, b) => Number.isSafeInteger(n) && n >= a && n <= b;
  const hash = h => typeof h === 'string' && /^[a-f0-9]{64}$/.test(h);
  if (s?.schema !== 'service-site.creator-sample.v1' || !/^[a-z][a-z0-9-]{1,60}$/.test(s.id) ||
      !int(s.sample_rate, 8000, 96000) || !int(s.channels, 1, 8) || !int(s.frames, 1, s.sample_rate * 30) ||
      !hash(s.native_sha256) || !Array.isArray(s.stems) || !s.stems.length || s.stems.length > 64) throw Error('Invalid audio sample.');
  const ids = new Set(); let bytes = 0;
  for (const stem of s.stems) {
    if (!/^[a-z][a-z0-9-]{1,60}$/.test(stem.id) || ids.has(stem.id) || stem.file !== `${stem.id}.f32` ||
        typeof stem.label !== 'string' || !stem.label.trim() || stem.label.length > 80 ||
        !int(stem.frames, 1, s.frames) || !int(stem.at, 0, s.frames - stem.frames) ||
        !Number.isFinite(stem.gain) || stem.gain < 0 || stem.gain > 1 || !hash(stem.sha256)) throw Error('Invalid audio stem.');
    ids.add(stem.id); bytes += stem.frames * s.channels * 4;
  }
  if (bytes > 32 * 1024 * 1024 || s.frames * s.channels * 4 > 32 * 1024 * 1024) throw Error('Audio sample is too large.');
  return s;
}

export function validateGains(gains, sample) {
  if (!Array.isArray(gains) || gains.length !== sample.stems.length ||
      gains.some(g => !Number.isFinite(g) || g < 0 || g > 1)) throw Error('Levels must be between 0 and 1.');
  return gains;
}

export function renderSample(mixer, sample, sources, gains) {
  validateGains(gains, sample);
  const output = new Float32Array(sample.frames * sample.channels);
  for (let at = 0; at < sample.frames; at += 1024) {
    const frames = Math.min(1024, sample.frames - at), stems = [];
    sample.stems.forEach((stem, i) => {
      const lo = Math.max(at, stem.at), hi = Math.min(at + frames, stem.at + stem.frames);
      if (lo < hi) stems.push({at: lo - at, gain: gains[i],
        samples: sources[i].subarray((lo - stem.at) * sample.channels, (hi - stem.at) * sample.channels)});
    });
    output.set(mixer.mixBlock({frames, channels: sample.channels, stems}), at * sample.channels);
  }
  return output;
}

export function wavBytes(samples, sample) {
  if (!(samples instanceof Float32Array) || samples.length !== sample.frames * sample.channels) throw Error('Invalid audio output.');
  const bytes = new ArrayBuffer(44 + samples.length * 2), view = new DataView(bytes);
  const word = (at, text) => [...text].forEach((c, i) => view.setUint8(at + i, c.charCodeAt(0)));
  word(0, 'RIFF'); view.setUint32(4, bytes.byteLength - 8, true); word(8, 'WAVE'); word(12, 'fmt ');
  view.setUint32(16, 16, true); view.setUint16(20, 1, true); view.setUint16(22, sample.channels, true);
  view.setUint32(24, sample.sample_rate, true); view.setUint32(28, sample.sample_rate * sample.channels * 2, true);
  view.setUint16(32, sample.channels * 2, true); view.setUint16(34, 16, true); word(36, 'data'); view.setUint32(40, samples.length * 2, true);
  samples.forEach((n, i) => { if (!Number.isFinite(n)) throw Error('Invalid audio output.'); const v = Math.max(-1, Math.min(1, n)); view.setInt16(44 + i * 2, Math.round(v * (v < 0 ? 32768 : 32767)), true); });
  return bytes;
}
