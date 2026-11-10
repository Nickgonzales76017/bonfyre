import {readLocal, saveLocal} from './store.mjs';
import {wavBytes} from './creator-sample.mjs';
import {importLimits, audioDigest} from './creator-import.mjs';
import {createTimeline} from './creator-timeline.mjs';
import {escapeHTML as esc} from './workspace-navigation.mjs';

export async function bindImportedAudio({root, sample, call, stopSample}) {
  const $ = name => root.querySelector(`[data-audio="${name}"]`), key = `draft:creator:${sample.id}:imports`;
  let files = [], sources = [], clips = [], format, output, mixHash, session, revision = 0, saved = '', available = true;
  let busy = false, saving = false, invalid = false, version = 0, context, playing, playVersion = 0, mode = 'sample', view = 'sources';
  let cursor = 0, started = 0, startOffset = 0, animation = 0, seeded = false;
  const undo = [], redo = [];
  const timeline = createTimeline({root, commit: commitClips, canEdit: () => !busy && !saving && !invalid, seek, report: text => status(text)});
  const title = () => $('title').value.trim(), permission = () => $('permission').value;
  const identity = () => JSON.stringify({sources: sources.map(s => s.sha256), clips, title: title(), permission: permission()});
  const dirty = () => files.length && identity() !== saved;
  const status = text => {$('status').textContent = text;};
  function controls() {
    const working = busy || saving;
    $('files').disabled = working;
    for (const el of root.querySelectorAll('[data-creator-view]')) el.disabled = working;
    for (const name of ['play', 'download', 'manifest']) $(name).disabled = !output || working || invalid;
    $('save').disabled = !output || working || invalid || !available || (!dirty() && !seeded) || !title();
    $('stop').disabled = !playing;
    $('undo').disabled = working || !undo.length; $('redo').disabled = working || !redo.length;
    $('sample-tracks').disabled = working; $('seek').disabled = !output || working || invalid;
    $('saved').textContent = !available ? 'Saving unavailable. Download the mix and notes to keep your work.' : !files.length ? 'Recordings and edits stay in this browser.' : seeded ? 'Sample tracks · not saved.' : dirty() ? 'Session changes not saved.' : 'Session saved in this browser.';
    $('workspace').dataset.busy = String(working); $('workspace').dataset.ready = String(Boolean(output) && !invalid);
    $('workspace').dataset.mixSha256 = mixHash ?? '';
  }
  function position(value) {
    cursor = Math.max(0, Math.min(format ? format.frames / format.sample_rate : 0, value));
    $('time').value = `${Math.floor(cursor / 60)}:${(cursor % 60).toFixed(3).padStart(6, '0')}`;
    $('seek').value = cursor; timeline.position(cursor);
  }
  function stop() {
    playVersion++;
    if (playing) {position(startOffset + context.currentTime - started); playing.onended = null; playing.stop(); playing.disconnect(); playing = null;}
    cancelAnimationFrame(animation); $('workspace').dataset.playing = 'false'; controls();
  }
  function seek(seconds) {stop(); position(seconds);}
  function remember() {undo.push(structuredClone(clips)); if (undo.length > 60) undo.shift(); redo.length = 0;}
  function commitClips(next) {
    if (busy || saving || JSON.stringify(next) === JSON.stringify(clips)) return;
    remember(); clips = structuredClone(next); timeline.update({sources, clips, format}); timeline.fields(); void render();
  }
  function editHistory(direction) {
    const from = direction === 'undo' ? undo : redo, to = direction === 'undo' ? redo : undo;
    if (busy || saving || !from.length) return;
    to.push(structuredClone(clips)); clips = from.pop(); timeline.update({sources, clips, format}); timeline.fields(); void render();
  }
  $('undo').onclick = () => editHistory('undo'); $('redo').onclick = () => editHistory('redo');
  $('seek').oninput = () => seek(Number($('seek').value));
  addEventListener('keydown', event => {
    if (mode !== 'imports' || event.target.closest('input,textarea,select,[contenteditable=true]')) return;
    if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'z') {event.preventDefault(); editHistory(event.shiftKey ? 'redo' : 'undo');}
    if (event.code === 'Space' && event.target === document.body) {event.preventDefault(); if (playing) stop(); else $('play').click();}
  });
  function route(write = false) {
    root.querySelector('[data-ui="sample-panel"]').hidden = mode !== 'sample'; $('workspace').hidden = mode !== 'imports';
    for (const button of root.querySelectorAll('[data-creator-view]')) button.setAttribute('aria-pressed', String(button.dataset.creatorView === mode));
    for (const id of ['sources', 'timeline', 'export']) $(id).hidden = id !== view;
    for (const button of root.querySelectorAll('[data-audio-view]')) button.setAttribute('aria-pressed', String(button.dataset.audioView === view));
    if (write) {const url = new URL(location.href); url.searchParams.set('workspace', mode); url.searchParams.set('view', view); history.pushState(null, '', url);}
  }
  function restoreRoute() {
    const url = new URL(location.href); mode = url.searchParams.get('workspace') === 'sample' ? 'sample' : 'imports';
    view = ['sources', 'timeline', 'export'].includes(url.searchParams.get('view')) ? url.searchParams.get('view') : 'timeline';
    stop(); stopSample(); route();
  }
  root.addEventListener('click', event => {
    const workspace = event.target.closest('[data-creator-view]'), panel = event.target.closest('[data-audio-view]');
    if (workspace && !busy && !saving) {mode = workspace.dataset.creatorView; stop(); stopSample(); route(true);}
    if (panel) {view = panel.dataset.audioView; route(true);}
  });
  addEventListener('popstate', restoreRoute);
  const seconds = frames => (frames / format.sample_rate).toFixed(3);
  const clipSeconds = frames => (frames / format.sample_rate).toFixed(6).replace(/0+$/, '').replace(/\.$/, '');
  function renderSources() {
    $('source-list').innerHTML = sources.map(s => `<article class="creator-source"><strong>${esc(s.name)}</strong><span>${(s.frames / s.sample_rate).toFixed(2)} seconds · ${s.sample_rate} Hz · ${s.originalChannels === 1 ? 'Mono' : 'Stereo'}</span><details><summary>Source fingerprint</summary><code>${s.sha256}</code></details></article>`).join('');
  }
  function drawTimelines() {if (format) timeline.update({sources, clips, format});}
  function renderTracks() {timeline.update({sources, clips, format}, true);}
  function accept(result) {
    if (!(result.buffer instanceof ArrayBuffer) || result.buffer.byteLength !== result.format.frames * 2 * 4) throw Error('The audio output is incomplete.');
    sources = result.sources; clips = result.clips; format = result.format; output = new Float32Array(result.buffer); mixHash = result.sha256; session = result.session; invalid = false;
    $('duration').textContent = `${seconds(format.frames)} seconds`;
    $('levels').textContent = result.clipped ? `${result.clipped.toLocaleString()} samples exceed the WAV range. Lower track levels before exporting to avoid clipping.` : `Peak level ${(result.peak * 100).toFixed(1)}%. No samples exceed the WAV range.`;
    $('export-summary').textContent = `${sources.length} recordings · ${seconds(format.frames)} seconds · stereo ${format.sample_rate} Hz · 16-bit WAV`;
    $('seek').max = format.frames / format.sample_rate; position(cursor); drawTimelines(); controls();
  }
  async function render() {
    version++; stop(); if (busy) return;
    busy = true; controls();
    try {
      for (;;) {const requested = version, candidate = structuredClone(clips); let result;
        try {result = await call('import-render', {clips: candidate, session});} catch (error) {if (requested !== version) continue; throw error;}
        if (requested !== version) continue; accept(result); status('Session ready.'); break;}
    } catch (error) {invalid = true; status(error.message);}
    finally {busy = false; controls();}
  }
  $('tracks').addEventListener('input', event => {
    const input = event.target.closest('[data-edit]'), track = input?.closest('[data-clip]'); if (!track || saving) return;
    remember();
    const i = Number(track.dataset.clip), field = input.dataset.edit, clip = clips[i];
    if (field === 'muted') clip.muted = input.checked;
    else if (field === 'gain') {clip.gain = Number(input.value); track.querySelector('output').value = clip.gain.toFixed(2);}
    else {
      const from = Number(track.querySelector('[data-edit="from"]').value), to = Number(track.querySelector('[data-edit="to"]').value), at = Number(track.querySelector('[data-edit="at"]').value);
      if ([from, to, at].some(n => !Number.isFinite(n))) {invalid = true; status('Enter valid clip positions.'); controls(); return;}
      clip.from = Math.round(from * format.sample_rate); clip.frames = Math.round(to * format.sample_rate) - clip.from; clip.at = Math.round(at * format.sample_rate);
    }
    void render();
  });
  for (const name of ['title', 'permission']) $(name).addEventListener('input', controls);
  $('files').onchange = async () => {
    const selection = [...$('files').files]; $('files').value = ''; if (!selection.length || busy || saving) return;
    if (files.length && (!seeded || dirty()) && !confirm('Replace the open audio session? Save or download anything you want to keep first.')) return;
    if (selection.length > importLimits.tracks || selection.reduce((n, f) => n + f.size, 0) > importLimits.bytes) {status('Choose up to eight WAV files, at most 24 MB combined.'); return;}
    stop(); busy = true; controls(); status('Reading your recordings…');
    try {
      const next = await Promise.all(selection.map(async file => ({name: file.name, bytes: await file.arrayBuffer()})));
      const result = await call('import', {files: next});
      files = next.map((file, i) => ({...file, sha256: result.sources[i].sha256})); seeded = false; cursor = 0; undo.length = redo.length = 0; accept(result); renderSources(); renderTracks();
      $('permission').value = 'unreviewed'; $('title').value = 'My audio session'; status('Recordings ready. Trim and position them on the timeline.'); view = 'timeline'; route(true);
    } catch (error) {status(error.message);}
    finally {busy = false; controls();}
  };
  $('save').onclick = async () => {
    if (busy || saving || invalid || !available || !output || !title()) return;
    const value = {files: structuredClone(files), clips: structuredClone(clips), title: title(), permission: permission()}, savedIdentity = identity();
    saving = true; for (const input of $('workspace').querySelectorAll('input,select')) input.disabled = true; controls();
    try {const row = await saveLocal(key, value, revision); revision = row.revision; saved = savedIdentity; seeded = false; status('Session saved with its recordings and edits.');}
    catch (error) {available = false; status(error.message.includes('another tab') ? 'This session changed in another tab. Reload before saving over it.' : 'The browser could not save this session. Download the mix and notes to keep your work.');}
    finally {saving = false; for (const input of $('workspace').querySelectorAll('input,select')) input.disabled = false; controls();}
  };
  $('play').onclick = async () => {
    if (!output || busy || invalid) return;
    stopSample(); stop(); const ticket = playVersion;
    try {if (context?.sampleRate !== format.sample_rate) {await context?.close(); context = new AudioContext({sampleRate: format.sample_rate});} await context.resume();
      if (ticket !== playVersion || busy || invalid) return;
      const buffer = context.createBuffer(2, format.frames, format.sample_rate);
      for (let ch = 0; ch < 2; ch++) {const target = buffer.getChannelData(ch); for (let f = 0; f < format.frames; f++) target[f] = output[f * 2 + ch];}
      playing = context.createBufferSource(); playing.buffer = buffer; playing.connect(context.destination); if (cursor >= format.frames / format.sample_rate) position(0);
      startOffset = cursor; started = context.currentTime; playing.onended = () => {stop(); position(0);}; playing.start(0, cursor); $('workspace').dataset.playing = 'true'; controls();
      const tick = () => {if (playing) {position(startOffset + context.currentTime - started); animation = requestAnimationFrame(tick);}}; tick();
    } catch {status('Playback could not start. Try Play session again.');}
  };
  $('stop').onclick = stop;
  const download = (body, type, name) => {const url = URL.createObjectURL(new Blob([body], {type})), link = document.createElement('a'); link.href = url; link.download = name; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);};
  $('download').onclick = () => {if (!output || busy || invalid) return; download(wavBytes(output, format), 'audio/wav', 'aurekai-audio-session.wav');};
  $('manifest').onclick = () => {
    if (!output || busy || invalid) return;
    const note = [title(), `${seconds(format.frames)} seconds · stereo · ${format.sample_rate} Hz`, `Recording permissions: ${permission() === 'own' ? 'Creator states they made the recordings' : permission() === 'licensed' ? 'Creator states they have permission' : 'Still to review'}`, `Mix PCM SHA-256: ${mixHash}`, '',
      ...sources.flatMap((s, i) => [s.name, `Source WAV SHA-256: ${s.sha256}`, `Source ${seconds(clips[i].from)} to ${seconds(clips[i].from + clips[i].frames)} seconds; starts at ${seconds(clips[i].at)} seconds; level ${clips[i].gain}; ${clips[i].muted ? 'muted' : 'audible'}`, '']),
      'Saved and mixed locally. These notes do not establish publication permission.'].join('\n');
    download(note, 'text/plain;charset=utf-8', 'aurekai-audio-session.txt');
  };
  
  addEventListener('beforeunload', event => {if (dirty()) {event.preventDefault(); event.returnValue = '';}});
  addEventListener('pagehide', () => {stop(); timeline.destroy(); void context?.close();});
  async function openSample(automatic = false) {
    if (busy || saving || (!automatic && files.length && !confirm('Replace the open session with generated sample tracks?'))) return;
    busy = true; stop(); controls(); status('Opening sample tracks…');
    try {
      const next = await Promise.all(sample.stems.map(async stem => {
        const response = await fetch(new URL(stem.file, import.meta.url)); if (!response.ok) throw Error('Sample track unavailable.');
        const bytes = await response.arrayBuffer(); if (await audioDigest(bytes) !== stem.sha256) throw Error('Sample track has changed.');
        return {name:stem.label + ' (generated).wav', bytes:wavBytes(new Float32Array(bytes), {...sample, frames:stem.frames})};
      }));
      const sampleClips = sample.stems.map(stem => ({at:stem.at, from:0, frames:stem.frames, gain:stem.gain, muted:false}));
      const result = await call('import', {files:next, clips:sampleClips});
      files = next.map((file, i) => ({...file, sha256:result.sources[i].sha256})); cursor = 0; undo.length = redo.length = 0;
      $('title').value = 'Generated tone arrangement'; $('permission').value = 'unreviewed';
      accept(result); renderSources(); renderTracks(); seeded = true; saved = identity();
      status('Generated sample tracks.'); view = 'timeline'; route(!automatic);
    } catch (error) {status(error.message);}
    finally {busy = false; controls();}
  }
  $('sample-tracks').onclick = () => void openSample();
  restoreRoute();
  try {
    const row = (await readLocal(key)).find(row => row.key === key);
    if (row) {
      const value = row.value;
      if (typeof value?.title !== 'string' || !value.title.trim() || value.title.length > 100 || !['own', 'licensed', 'unreviewed'].includes(value.permission)) throw Error('Saved session metadata is invalid.');
      busy = true; controls(); const result = await call('import', {files: value.files, clips: value.clips});
      files = value.files; revision = row.revision; $('title').value = value.title; $('permission').value = value.permission;
      accept(result); renderSources(); renderTracks(); saved = identity(); status('Your saved recordings and edits are restored.');
    }
  } catch (error) {available = false; status('The saved session could not be opened. '+error.message);}
  finally {busy = false; controls();}
  if (!files.length && available && mode === 'imports') await openSample(true);
  return {stop};
}
