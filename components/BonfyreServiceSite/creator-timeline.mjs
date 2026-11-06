import {escapeHTML as esc} from './workspace-navigation.mjs';
import {gestureClip, sourceSelection} from './creator-timeline-model.mjs';

export function createTimeline({root, commit, canEdit, seek, report}) {
  const $ = name => root.querySelector(`[data-audio="${name}"]`);
  let sources = [], clips = [], format, selected = 0, extent = 6, zoom = 1, gesture, wave, region, waveKey = '', waveTicket = 0, waveAttempt = 0, syncing = false;
  const colors = ['#a8d4ad', '#90c5d5', '#d9b779', '#bcabdb', '#de9f99', '#87c5af', '#d4c993', '#a5b2df'];
  const number = frames => (frames / format.sample_rate).toFixed(6).replace(/0+$/, '').replace(/\.$/, '');
  const snap = () => $('snap').checked ? Math.max(1, Math.round(format.sample_rate / 10)) : 1;
  function fields() {
    clips.forEach((clip, i) => {
      const row = root.querySelector(`[data-clip="${i}"]`); if (!row) return;
      for (const [field, value] of Object.entries({at:number(clip.at), from:number(clip.from), to:number(clip.from + clip.frames), gain:clip.gain})) row.querySelector(`[data-edit="${field}"]`).value = value;
      row.querySelector('[data-edit=muted]').checked = clip.muted;
      row.querySelector('output').value = clip.gain.toFixed(2);
    });
  }
  function draw() {
    if (!format) return;
    const width = Math.max(500, root.querySelector('.creator-ruler-scroll').clientWidth) * zoom;
    $('arrangement').style.setProperty('--timeline-width', `${width}px`);
    const step = [.1, .2, .5, 1, 2, 5, 10, 15, 30].find(n => n >= extent / width * 60) ?? 30;
    $('ruler').innerHTML = Array.from({length: Math.ceil(extent / step)}, (_, i) => `<span style="left:${i * step / extent * 100}%">${(i * step).toFixed(step < 1 ? 1 : 0)}s</span>`).join('');
    clips.forEach((clip, i) => {
      const row = root.querySelector(`[data-clip="${i}"]`), block = row?.querySelector('[data-clip-block]'); if (!block) return;
      row.dataset.selected = String(i === selected); row.querySelector('[data-clip-fields]').hidden = i !== selected;
      row.querySelector('[data-select-track]').setAttribute('aria-pressed', String(i === selected));
      block.style.left = `${clip.at / format.sample_rate / extent * 100}%`; block.style.width = `${clip.frames / format.sample_rate / extent * 100}%`;
      block.dataset.muted = String(clip.muted); block.title = `${sources[i].name}: ${number(clip.at)} to ${number(clip.at + clip.frames)} seconds`;
      block.querySelector('[data-clip-time]').textContent = `${number(clip.frames)}s`;
      const canvas = block.querySelector('canvas'), width = canvas.clientWidth, height = 44, ratio = devicePixelRatio || 1;
      canvas.width = Math.max(1, Math.round(width * ratio)); canvas.height = height * ratio;
      const pen = canvas.getContext('2d'); pen.scale(ratio, ratio); pen.fillStyle = colors[i];
      for (let x = 0; x < width; x++) {
        const bin = Math.min(127, Math.floor((clip.from + x / width * clip.frames) / sources[i].frames * 128));
        const amplitude = Math.min(1, sources[i].peaks[bin] * clip.gain) * 21;
        pen.fillRect(x, 22 - amplitude, 1, Math.max(1, amplitude * 2));
      }
    });
    $('track-count').textContent = `${clips.length} tracks`;
    $('zoom-value').textContent = `${zoom}×`;
    if (region && clips[selected] && !syncing) {
      syncing = true; region.setOptions({start:clips[selected].from / format.sample_rate, end:(clips[selected].from + clips[selected].frames) / format.sample_rate}); syncing = false;
    }
    if (sources[selected]) $('selection-name').textContent = sources[selected].name;
  }
  async function sourceWave() {
    const source = sources[selected]; if (!source) return;
    const key = `${selected}:${source.sha256}`;
    if (waveKey === key) return;
    waveKey = key; const ticket = ++waveTicket;
    wave?.destroy(); wave = null; region = null; $('source-wave').replaceChildren();
    $('source-wave').dataset.ready = 'false'; $('wave-state').textContent = 'Loading waveform…'; $('wave-retry').hidden = true;
    try {
      const [{default:WaveSurfer}, {default:Regions}] = await Promise.all([import(`./vendor/wavesurfer-8.0.2/wavesurfer.esm.js${waveAttempt ? '?retry=' + waveAttempt : ''}`), import(`./vendor/wavesurfer-8.0.2/regions.esm.js${waveAttempt ? '?retry=' + waveAttempt : ''}`)]);
      if (ticket !== waveTicket) return;
      const plugin = Regions.create();
      wave = WaveSurfer.create({container:$('source-wave'), height:72, waveColor:colors[selected], progressColor:colors[selected], cursorWidth:0, interact:false, plugins:[plugin]});
      // Peaks come from the validated original WAV, with no second decode or audio URL.
      await wave.load('', [source.peaks], source.frames / source.sample_rate);
      if (ticket !== waveTicket) return;
      const clip = clips[selected];
      region = plugin.addRegion({id:'source-trim', start:clip.from / source.sample_rate, end:(clip.from + clip.frames) / source.sample_rate, color:'#a8d4ad33', drag:true, resize:true, minLength:1 / source.sample_rate});
      plugin.on('region-updated', r => {
        if (syncing) return;
        if (!canEdit()) {draw(); return;}
        const next = structuredClone(clips); next[selected] = sourceSelection(clips[selected], source, r.start, r.end); commit(next); fields(); draw();
      });
      $('source-wave').dataset.ready = 'true'; $('wave-state').textContent = 'Drag the selection edges to trim the source.';
    } catch (error) {
      if (ticket !== waveTicket) return;
      waveKey = ''; $('source-wave').dataset.ready = 'false'; $('wave-state').textContent = 'Waveform unavailable. The clip handles and timing fields still work.'; $('wave-retry').hidden = false;
    }
  }
  function select(index) {selected = index; draw(); void sourceWave();}
  function mount() {
    $('tracks').innerHTML = sources.map((s, i) => `<article class="creator-lane" data-clip="${i}" style="--track-color:${colors[i]};--track-row:${i + 1}">
<div class="creator-lane-label"><button class="creator-select-track" data-select-track="${i}">${esc(s.name)}</button><label class="creator-mute"><input type="checkbox" data-edit="muted">Mute</label></div>
<div class="creator-lane-scroll"><div class="creator-lane-track"><div class="creator-clip-block" data-clip-block data-track="${i}"><button class="creator-trim start" data-gesture="start" aria-label="Trim start of ${esc(s.name)}"></button><button class="creator-clip-move" data-gesture="move" aria-label="Move ${esc(s.name)}"><span>${esc(s.name)}</span><span data-clip-time></span><canvas aria-hidden="true"></canvas></button><button class="creator-trim end" data-gesture="end" aria-label="Trim end of ${esc(s.name)}"></button></div><i class="creator-playhead" aria-hidden="true"></i></div></div>
<div class="creator-clip-fields" data-clip-fields><label>Start in mix (s)<input data-edit="at" type="number" min="0" max="30" step="0.001"></label><label>Trim from (s)<input data-edit="from" type="number" min="0" max="30" step="0.001"></label><label>Trim to (s)<input data-edit="to" type="number" min="0.001" max="30" step="0.001"></label><label>Level <output></output><input data-edit="gain" type="range" min="0" max="1" step="0.01"></label></div></article>`).join('');
    $('tracks').style.setProperty('--track-count', sources.length);
    fields(); draw(); void sourceWave();
    for (const scroller of root.querySelectorAll('.creator-lane-scroll, .creator-ruler-scroll')) scroller.addEventListener('scroll', () => {
      for (const other of root.querySelectorAll('.creator-lane-scroll, .creator-ruler-scroll')) if (other !== scroller && other.scrollLeft !== scroller.scrollLeft) other.scrollLeft = scroller.scrollLeft;
    });
  }
  $('tracks').addEventListener('click', event => {const button = event.target.closest('[data-select-track]'); if (button) select(Number(button.dataset.selectTrack));});
  $('tracks').addEventListener('pointerdown', event => {
    const handle = event.target.closest('[data-gesture]'); if (!handle || !canEdit() || event.button !== 0) return;
    const index = Number(handle.closest('[data-track]').dataset.track); select(index);
    gesture = {handle, index, action:handle.dataset.gesture, original:structuredClone(clips), x:event.clientX, width:handle.closest('.creator-lane-track').clientWidth, id:event.pointerId};
    handle.setPointerCapture(event.pointerId); event.preventDefault();
  });
  $('tracks').addEventListener('pointermove', event => {
    if (!gesture || gesture.id !== event.pointerId) return;
    clips = structuredClone(gesture.original);
    clips[gesture.index] = gestureClip(clips[gesture.index], sources[gesture.index], gesture.action, (event.clientX - gesture.x) / gesture.width * extent * format.sample_rate, snap()); draw(); fields();
  });
  function endGesture(event, cancelled = false) {
    if (!gesture || event.pointerId !== gesture.id) return;
    const next = clips, original = gesture.original; gesture = null;
    clips = original; if (!cancelled && canEdit()) commit(next); else {draw(); fields();}
  }
  $('tracks').addEventListener('pointerup', event => endGesture(event));
  $('tracks').addEventListener('pointercancel', event => endGesture(event, true));
  $('tracks').addEventListener('keydown', event => {
    const handle = event.target.closest('[data-gesture]'); if (!handle || !['ArrowLeft','ArrowRight'].includes(event.key) || !canEdit()) return;
    event.preventDefault(); const i = Number(handle.closest('[data-track]').dataset.track), next = structuredClone(clips);
    next[i] = gestureClip(clips[i], sources[i], handle.dataset.gesture, (event.key === 'ArrowLeft' ? -1 : 1) * format.sample_rate * (event.shiftKey ? 1 : .1)); commit(next); fields();
  });
  $('ruler').addEventListener('click', event => {const box = $('ruler').getBoundingClientRect(); seek(Math.max(0, Math.min(format?.frames / format?.sample_rate || 0, (event.clientX - box.left) / box.width * extent)));});
  $('zoom').oninput = () => {zoom = Number($('zoom').value); draw();};
  $('fit').onclick = () => {extent = Math.min(30, Math.max(6, Math.ceil(format.frames / format.sample_rate + 2))); zoom = 1; $('zoom').value = 1; draw();};
  $('wave-retry').onclick = () => {waveAttempt++; waveKey = ''; void sourceWave();};
  const observer = new ResizeObserver(draw); observer.observe($('tracks'));
  return {
    update(next, rebuild = false) {sources = next.sources; clips = structuredClone(next.clips); format = next.format; extent = Math.min(30, Math.max(extent, Math.ceil(format.frames / format.sample_rate))); if (rebuild) {selected = 0; waveKey = ''; extent = Math.min(30, Math.max(6, Math.ceil(format.frames / format.sample_rate + 2))); mount();} else draw();},
    fields,
    position(seconds) {const percent = seconds / extent * 100; for (const head of root.querySelectorAll('.creator-playhead')) head.style.left = `${percent}%`;},
    destroy() {waveTicket++; wave?.destroy(); observer.disconnect();}
  };
}
