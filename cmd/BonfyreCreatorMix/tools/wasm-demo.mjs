import {createCreatorMixer} from './creator-wasm.mjs';

const status = document.querySelector('#status');
try {
  const load = async name => {
    const response = await fetch(name);
    if (!response.ok) throw new Error(`could not load ${name}`);
    return response.arrayBuffer();
  };
  const names = ['pulse', 'chime', 'harmony'];
  const [kernel, ...sources] = await Promise.all(['mix_kernel.wasm', ...names.map(n => `${n}.f32`)].map(load));
  const mixer = await createCreatorMixer(kernel);
  const stems = sources.map((body, i) => ({samples: new Float32Array(body), at: [0, 72000, 96000][i], frames: [48000, 72000, 72000][i]}));
  const sliders = names.map(n => document.getElementById(n));
  const canvas = document.querySelector('canvas'), ctx = canvas.getContext('2d');
  let mix, ac, playing;
  const stop = () => { playing?.stop(); playing = null; };
  function draw() {
    const width = canvas.clientWidth, scale = devicePixelRatio;
    canvas.width = width * scale; canvas.height = 160 * scale; ctx.scale(scale, scale);
    for (let ch = 0; ch < 2; ch++) {
      ctx.fillStyle = ch ? '#8abed0' : '#a8d4ad';
      for (let x = 0; x < width; x++) {
        let peak = 0;
        for (let f = Math.floor(x * 288000 / width); f < Math.floor((x + 1) * 288000 / width); f++) peak = Math.max(peak, Math.abs(mix[f * 2 + ch]));
        ctx.fillRect(x, (ch ? 120 : 40) - peak * 60, 1, Math.max(1, peak * 120));
      }
    }
  }
  function render() {
    stop(); const start = performance.now(); mix = new Float32Array(576000);
    for (let at = 0; at < 288000; at += 1024) {
      const frames = Math.min(1024, 288000 - at), active = [];
      stems.forEach((s, i) => {
        const lo = Math.max(at, s.at), hi = Math.min(at + frames, s.at + s.frames);
        if (lo < hi) active.push({at: lo - at, gain: Number(sliders[i].value), samples: s.samples.subarray((lo - s.at) * 2, (hi - s.at) * 2)});
      });
      mix.set(mixer.mixBlock({frames, channels: 2, stems: active}), at * 2);
    }
    const elapsed = performance.now() - start;
    sliders.forEach(s => document.querySelector(`output[for="${s.id}"]`).value = Number(s.value).toFixed(2));
    draw(); status.textContent = `Ready · 6 seconds · Rendered in ${elapsed.toFixed(1)} ms`;
    return elapsed;
  }
  const elapsed = render();
  const expected = new Uint8Array(await load('mix.f32')), actual = new Uint8Array(mix.buffer);
  if (expected.length !== actual.length || !expected.every((n, i) => n === actual[i])) throw new Error('browser and native audio differ');
  const digest = [...new Uint8Array(await crypto.subtle.digest('SHA-256', mix.buffer))].map(n => n.toString(16).padStart(2, '0')).join('');
  window.creatorProof = {samples: mix.length, bytes: actual.length, native_byte_parity: true, sha256: digest, kernel_bytes: kernel.byteLength, memory_bytes: mixer.memoryBytes, render_ms: elapsed};
  window.creatorState = () => ({gains: sliders.map(s => Number(s.value)), sum: mix.reduce((n, v) => n + Math.abs(v), 0), playing: Boolean(playing), clock: ac?.currentTime ?? 0});
  sliders.forEach(s => s.addEventListener('input', render));
  new ResizeObserver(draw).observe(canvas);
  document.querySelector('#play').onclick = async () => {
    stop(); ac ??= new AudioContext({sampleRate: 48000}); await ac.resume();
    const buffer = ac.createBuffer(2, 288000, 48000);
    for (let ch = 0; ch < 2; ch++) { const target = buffer.getChannelData(ch); for (let f = 0; f < target.length; f++) target[f] = mix[f * 2 + ch]; }
    const node = ac.createBufferSource(); node.buffer = buffer; node.connect(ac.destination);
    node.onended = () => { if (playing === node) playing = null; node.disconnect(); };
    playing = node; node.start();
  };
  document.querySelector('#stop').onclick = stop;
  document.querySelector('#reset').onclick = () => { sliders.forEach((s, i) => s.value = [0.4, 0.5, 0.5][i]); render(); };
  document.querySelectorAll('button').forEach(button => button.disabled = false);
} catch (error) { window.creatorError = error.message; status.textContent = `Audio unavailable: ${error.message}`; }
