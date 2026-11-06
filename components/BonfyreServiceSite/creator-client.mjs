import {readLocal, saveLocal} from './store.mjs';
import {validateGains, wavBytes} from './creator-sample.mjs';
import {bindImportedAudio} from './creator-import-client.mjs';

const root = document.querySelector('[data-creator]');
const sample = JSON.parse(root.querySelector('[data-creator-config]').textContent);
const $ = name => root.querySelector(`[data-ui="${name}"]`);
const inputs = [...root.querySelectorAll('[data-gain]')];
const key = `draft:creator:${sample.id}:${sample.native_sha256}`;
let revision = 0, saved = '', storageOK = true, saving = false;
let waveform, output, context, playing, playVersion = 0, started = 0, frame = 0, ready = false, busy = false, version = 0, nextId = 0;
const worker = new Worker(new URL('./creator-worker.mjs', import.meta.url), {type: 'module'});
const requests = new Map();
const gains = () => inputs.map(input => Number(input.value));
function controls() {
  for (const name of ['play', 'download']) $(name).disabled = !ready || busy;
  $('save').disabled = !ready || busy || saving || !storageOK || JSON.stringify(gains()) === saved;
  $('reset').disabled = !ready;
  $('stop').disabled = !playing;
  root.dataset.busy = String(busy); root.dataset.ready = String(ready);
}
function failAll(message) { for (const q of requests.values()) {clearTimeout(q.timer); q.reject(Error(message));} requests.clear(); }
worker.onerror = () => failAll('The audio worker stopped. Reload to try again.');
worker.onmessageerror = () => failAll('The audio worker returned an unreadable result.');
worker.onmessage = ({data}) => {
  const q = requests.get(data.id); if (!q) return; requests.delete(data.id); clearTimeout(q.timer);
  data.type === 'error' ? q.reject(Error(data.message)) : q.resolve(data);
};
function call(type, payload) {
  const id = nextId++;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {requests.delete(id); reject(Error('Audio took too long to load. Reload to try again.'));}, 20000);
    requests.set(id, {resolve, reject, timer}); worker.postMessage({id, type, ...payload});
  });
}
const canvas = $('wave'), pen = canvas.getContext('2d');
function draw() {
  if (!output || !canvas.clientWidth) return;
  const width = canvas.clientWidth, height = 180, scale = devicePixelRatio;
  const pixelWidth = Math.round(width * scale), pixelHeight = Math.round(height * scale);
  if (!waveform || waveform.width !== pixelWidth || waveform.height !== pixelHeight) {
    waveform = document.createElement('canvas'); waveform.width = pixelWidth; waveform.height = pixelHeight;
    const trace = waveform.getContext('2d'); trace.scale(scale, scale);
    for (let channel = 0; channel < sample.channels; channel++) {
      const mid = (channel + .5) * height / sample.channels, amplitude = height / sample.channels * .4;
      trace.fillStyle = ['#afd7b1', '#90c5d5'][channel % 2];
      for (let x = 0; x < width; x++) {
        let peak = 0;
        for (let f = Math.floor(x * sample.frames / width); f < Math.floor((x + 1) * sample.frames / width); f++) peak = Math.max(peak, Math.abs(output[f * sample.channels + channel]));
        trace.fillRect(x, mid - peak * amplitude, 1, Math.max(1, peak * amplitude * 2));
      }
    }
  }
  if (canvas.width !== pixelWidth || canvas.height !== pixelHeight) {canvas.width = pixelWidth; canvas.height = pixelHeight;}
  pen.setTransform(1, 0, 0, 1, 0, 0); pen.clearRect(0, 0, pixelWidth, pixelHeight); pen.drawImage(waveform, 0, 0); pen.scale(scale, scale);
  if (playing) { const seconds = Math.min(sample.frames / sample.sample_rate, context.currentTime - started); pen.fillStyle = '#ffffff'; pen.fillRect(seconds * sample.sample_rate / sample.frames * width, 0, 1, height); }
}
function stop() { playVersion++; if (playing) {playing.onended = null; playing.stop(); playing.disconnect(); playing = null;} cancelAnimationFrame(frame); root.dataset.playing = 'false'; controls(); draw(); }
function accept(result) {
  if (!(result.buffer instanceof ArrayBuffer) || result.buffer.byteLength !== sample.frames * sample.channels * 4) throw Error('Audio output is incomplete.');
  waveform = null; output = new Float32Array(result.buffer); root.dataset.mixSha256 = result.sha256;
  inputs.forEach(input => input.nextElementSibling.value = Number(input.value).toFixed(2)); draw();
}
async function remix() {
  version++; stop(); if (busy) return;
  busy = true; controls(); $('status').textContent = 'Updating your mix…';
  try {
    for (;;) { const requested = version, levels = validateGains(gains(), sample);
      const result = await call('render', {gains: levels});
      if (requested !== version) continue;
      accept(result); $('status').textContent = 'Ready to play.'; break;
    }
  } catch (error) { ready = false; $('status').textContent = error.message; root.dataset.error = error.message; }
  finally {busy = false; controls();}
}
inputs.forEach(input => input.addEventListener('input', () => {input.nextElementSibling.value = Number(input.value).toFixed(2); void remix();}));
$('reset').onclick = () => {inputs.forEach((input, i) => input.value = sample.stems[i].gain); void remix();};
$('stop').onclick = stop;
$('play').onclick = async () => {
  try {
    stop(); const ticket = playVersion; context ??= new AudioContext({sampleRate: sample.sample_rate}); await context.resume();
    if (ticket !== playVersion || busy || !ready) return;
    const buffer = context.createBuffer(sample.channels, sample.frames, sample.sample_rate);
    for (let channel = 0; channel < sample.channels; channel++) {const target = buffer.getChannelData(channel); for (let i = 0; i < sample.frames; i++) target[i] = output[i * sample.channels + channel];}
    playing = context.createBufferSource(); playing.buffer = buffer; playing.connect(context.destination);
    playing.onended = stop; started = context.currentTime; playing.start(); root.dataset.playing = 'true'; controls();
    const tick = () => {draw(); if (playing) frame = requestAnimationFrame(tick);}; tick();
  } catch { $('status').textContent = 'Playback could not start. Try Play mix again.'; }
};
$('download').onclick = () => {
  const url = URL.createObjectURL(new Blob([wavBytes(output, sample)], {type: 'audio/wav'}));
  const link = document.createElement('a'); link.href = url; link.download = 'aurekai-generated-mix.wav'; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
};
$('save').onclick = async () => {
  const levels = gains(); saving = true; controls();
  try {const row = await saveLocal(key, {schema: 'service-site.creator-levels.v1', gains: levels}, revision); revision = row.revision; saved = JSON.stringify(levels); $('saved').textContent = 'Levels saved on this browser.';}
  catch (error) { $('saved').textContent = /another tab/.test(error.message) ? 'Levels changed in another tab. Reload before saving.' : 'Levels could not be saved. You can still play and download this mix.'; storageOK = false; }
  finally {saving = false; controls();}
};
new ResizeObserver(draw).observe(canvas);
// A restored history page needs a fresh worker and AudioContext after cleanup.
addEventListener('pageshow', event => {if (event.persisted) location.reload();});
addEventListener('pagehide', () => {ready = false; stop(); worker.terminate(); failAll('Page closed.'); void context?.close();});
try {
  inputs.forEach(input => input.disabled = true); controls();
  const initial = await call('init', {sample}); accept(initial); root.dataset.referenceVerified = 'true';
  try {
    const row = (await readLocal(key)).find(r => r.key === key);
    if (row) {if (row.value?.schema !== 'service-site.creator-levels.v1') throw Error('Invalid saved levels.'); validateGains(row.value.gains, sample); revision = row.revision; inputs.forEach((input, i) => input.value = row.value.gains[i]); saved = JSON.stringify(row.value.gains); $('saved').textContent = 'Your saved levels are restored.';}
  } catch { storageOK = false; $('saved').textContent = 'Saved levels could not be opened. You can still play and download this mix.'; }
  ready = true; inputs.forEach(input => input.disabled = false);
  await remix();
  await bindImportedAudio({root,sample,call,stopSample:stop});root.dataset.importsReady='true';
  root.dataset.ready = String(ready);
} catch (error) { $('status').textContent = error.message; root.dataset.error = error.message; }
