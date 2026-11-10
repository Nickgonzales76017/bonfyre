// Browser-safe adapter for the shared native kernel. Callers own media loading,
// timeline scheduling and playback. Each call returns at most 1024 frames.
export async function createCreatorMixer(bytes) {
  const module = await WebAssembly.compile(bytes);
  if (WebAssembly.Module.imports(module).length) throw new Error('creator kernel must have no imports');
  const {exports: e} = await WebAssembly.instantiate(module);
  const capacity = 1024 * 8;
  const base = e.__heap_base?.value;
  if (!(e.memory instanceof WebAssembly.Memory) || e.memory.buffer.byteLength !== 131072 ||
      typeof e.aurekai_mix_f32 !== 'function' || !Number.isInteger(base) || base < 16384 ||
      base % 4 || base + capacity * 8 > e.memory.buffer.byteLength) {
    throw new Error('unexpected creator kernel ABI');
  }
  const dst = new Float32Array(e.memory.buffer, base, capacity);
  const src = new Float32Array(e.memory.buffer, base + capacity * 4, capacity);
  const integer = (n, lo, hi) => Number.isSafeInteger(n) && n >= lo && n <= hi;
  return Object.freeze({
    memoryBytes: e.memory.buffer.byteLength,
    mixBlock({frames, channels, stems}) {
      if (!integer(frames, 1, 1024) || !integer(channels, 1, 8) ||
          !Array.isArray(stems) || stems.length > 64) throw new RangeError('invalid mix block bounds');
      const count = frames * channels;
      dst.fill(0, 0, count);
      for (const stem of stems) {
        const {samples, gain, at = 0} = stem ?? {};
        if (!(samples instanceof Float32Array) || (typeof SharedArrayBuffer !== 'undefined' && samples.buffer instanceof SharedArrayBuffer) ||
            samples.length === 0 || samples.length % channels ||
            !integer(at, 0, frames - 1) || samples.length > (frames - at) * channels ||
            !Number.isFinite(gain) || gain < -4 || gain > 4) throw new RangeError('invalid mix stem');
        src.set(samples);
        e.aurekai_mix_f32(base + at * channels * 4, base + capacity * 4, Math.fround(gain), samples.length);
      }
      for (let i = 0; i < count; i++) {
        if (!Number.isFinite(dst[i])) throw new RangeError('nonfinite mixed sample');
      }
      return dst.slice(0, count);
    }
  });
}
