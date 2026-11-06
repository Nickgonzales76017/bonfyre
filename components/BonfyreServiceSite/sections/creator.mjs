import {readFileSync} from 'node:fs';
import {join} from 'node:path';
import {importView} from '../creator-import-view.mjs';
export const needs = ['title', 'sample'];
export function render(s, site, h) {
  if (!/^\/data\/[a-z][a-z0-9-]*\/$/.test(s.data)) throw Error('Creator requires packaged audio.');
  if (!/^v-[a-f0-9]{64}\/$/.test(s.asset_prefix)) throw Error('Creator requires a versioned audio package.');
  const json = JSON.stringify(s.sample).replace(/</g, '\\u003c').replace(/>/g, '\\u003e').replace(/&/g, '\\u0026');
  return `<section id="${h.esc(s.id)}" class="creator-workspace" data-creator><style>${readFileSync(join(import.meta.dir, '../creator.css'), 'utf8')}</style>
<script type="application/json" data-creator-config>${json}</script>
<header class="creator-top"><a href="/#samples">← All work samples</a><a href="/#ask">Discuss an audio tool</a></header>
<div class="creator-body"><p class="creator-eyebrow">Audio workspace</p><h1>${h.esc(s.title)}</h1>
<nav class="creator-actions" aria-label="Audio workspace"><button type="button" data-creator-view="sample" aria-pressed="true">Tone mixer</button><button type="button" data-creator-view="imports">Arrangement</button></nav><div class="creator-panel" data-ui="sample-panel" aria-label="Mix controls">${s.sample.stems.map((stem, i) => `<div class="creator-track"><label for="creator-gain-${i}">${h.esc(stem.label)}</label><input id="creator-gain-${i}" data-gain type="range" min="0" max="1" step="0.01" value="${stem.gain}" disabled><output for="creator-gain-${i}">${stem.gain.toFixed(2)}</output></div>`).join('')}
<canvas data-ui="wave" aria-label="Waveform of the mixed audio"></canvas><div class="creator-clock"><span>0:00</span><span>${(s.sample.frames / s.sample.sample_rate).toFixed(0)} seconds</span></div>
<div class="creator-actions"><button data-ui="play" disabled>Play mix</button><button data-ui="stop" class="secondary" disabled>Stop</button><button data-ui="download" class="secondary" disabled>Download WAV</button></div>
<p class="creator-message creator-muted" data-ui="status" role="status">Loading audio…</p><div class="creator-actions"><button data-ui="save" class="secondary" disabled>Save levels</button><button data-ui="reset" class="secondary" disabled>Reset levels</button></div><p class="creator-message creator-muted" data-ui="saved" role="status">Saved levels stay in this browser.</p></div>
${importView(h.esc)}<footer class="creator-footer">Saved sessions stay on this device.</footer><noscript>JavaScript is needed to mix and play the audio.</noscript></div><script type="module" src="${h.esc(s.data + s.asset_prefix)}creator-client.mjs"></script></section>`;
}
