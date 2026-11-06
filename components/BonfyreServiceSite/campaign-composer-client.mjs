// Linked artboards for one quotation: square post, story and link card, drawn
// on canvases at full size. The words and citation are shared; select an
// artboard to change its own type scale, alignment and colours. Each format
// downloads as a PNG, and as a short WebM in which the quotation appears word
// by word. A quotation edited away from its source is allowed and labelled.
import * as C from './campaign-composer.mjs';

const box = document.querySelector('[data-ui="compose"]');
const $ = n => box.querySelector(`[data-c="${n}"]`);
let post = null, active = 'square', opening = 0, changed = () => {}, sourceStatus = () => '';
const canvases = Object.fromEntries(Object.keys(C.FORMATS).map(f => [f, box.querySelector(`canvas[data-format="${f}"]`)]));
const measurers = {};
function measureFor(ctx) { return (text, size) => { ctx.font = `italic ${size}px "Instrument Serif", Georgia, serif`; return ctx.measureText(text).width; }; }

function draw(format, shown, draft = post) {
  const c = canvases[format], ctx = c.getContext('2d'), L = C.layout(draft.content, format, draft.overrides[format], measurers[format] ??= measureFor(ctx));
  c.width = L.w; c.height = L.h; const th = L.theme;
  ctx.fillStyle = th.ground; ctx.fillRect(0, 0, L.w, L.h);
  ctx.fillStyle = th.accent; ctx.fillRect(L.pad, L.h - L.pad - (format === 'card' ? 70 : 100), 80, 6);
  ctx.textBaseline = 'alphabetic'; ctx.textAlign = L.align === 'center' ? 'center' : 'left';
  if (L.claim) { ctx.fillStyle = th.accent; ctx.font = `500 ${L.claim.size}px "DM Mono", monospace`; ctx.fillText(L.claim.text.length > 52 ? L.claim.text.slice(0, 51) + '…' : L.claim.text, L.claim.x, L.claim.y + L.claim.size); }
  ctx.fillStyle = th.ink; ctx.font = `italic ${L.quote.size}px "Instrument Serif", Georgia, serif`;
  const lines = shown ? C.revealAt(L.quote.lines, shown) : L.quote.lines;
  lines.forEach((line, i) => ctx.fillText(line, L.x, L.quote.y + i * L.quote.lineHeight));
  ctx.fillStyle = th.ink; ctx.font = `600 ${L.cite.size}px "DM Sans", sans-serif`; ctx.fillText(L.cite.text, L.x, L.cite.y);
  ctx.fillStyle = th.muted; ctx.font = `500 ${L.mark.size}px "DM Mono", monospace`; ctx.textAlign = L.mark.anchor === 'center' ? 'center' : 'right';
  ctx.fillText(L.mark.text, L.mark.x, format === 'story' ? L.h - L.pad + 10 : L.mark.y + L.mark.size);
  return L;
}
function render() {
  const notes = [], overflowing = new Set();
  for (const f of Object.keys(C.FORMATS)) { const L = draw(f); canvases[f].closest('figure').classList.toggle('is-active', f === active); if (L.quote.overflow) { overflowing.add(f); notes.push(`${C.FORMATS[f].label}: the words do not fit even at the smallest size; shorten them.`); } }
  $('png').disabled = $('webm').disabled = overflowing.has(active); $('png-all').disabled = overflowing.size > 0;
  const exact = C.quotesExactly(post.content, post.source);
  $('exact').textContent = exact ? 'The words are the source\'s own, exactly.' : 'Edited: this no longer quotes the source exactly, so do not present it as a quotation.';
  $('exact').dataset.tone = exact ? '' : 'bad';
  $('notes').textContent = [sourceStatus(), ...notes].filter(Boolean).join(' ');
  const o = post.overrides[active] ?? {};
  $('which').textContent = C.FORMATS[active].label;
  $('scale').value = o.scale ?? 1;
  for (const b of box.querySelectorAll('[data-align]')) b.setAttribute('aria-pressed', String((o.align ?? 'left') === b.dataset.align));
  for (const b of box.querySelectorAll('[data-theme]')) b.setAttribute('aria-pressed', String((o.theme ?? 'night') === b.dataset.theme));
}
export function resetComposer() {
  opening++; post = null; changed = () => {}; sourceStatus = () => ''; box.hidden = true;
}
export function refreshComposer({ syncInputs = false } = {}) {
  if (post && !box.hidden) {
    if (syncInputs) { $('quote').value = post.content.quote; $('claim').value = post.content.claim; }
    render();
  }
}
function keep() {
  post.active = active; changed();
  // Button and keyboard edits must reach the shared store's dirty-work guard.
  box.dispatchEvent(new Event('change', { bubbles: true }));
}
export async function openComposer({ quote, cite, claim, source, draft, onChange = () => {}, getSourceStatus = () => '' }) {
  const ticket = ++opening;
  await Promise.all(['italic 64px "Instrument Serif"', '600 32px "DM Sans"', '500 26px "DM Mono"'].map(f => document.fonts.load(f).catch(() => null)));
  if (ticket !== opening) return;
  post = draft ?? { content: { quote, cite, claim }, source: source ?? quote, overrides: {}, active: 'square' };
  active = post.active; changed = onChange; sourceStatus = getSourceStatus;
  $('quote').value = post.content.quote; $('claim').value = post.content.claim ?? ''; $('status').textContent = '';
  box.hidden = false; render(); box.dispatchEvent(new Event('change', { bubbles: true })); box.scrollIntoView({ behavior: 'smooth', block: 'start' });
}
box.addEventListener('input', e => {
  if (!post) return; const k = e.target.dataset.c;
  if (k === 'quote') post.content.quote = e.target.value.replace(/\s+/g, ' ').trim();
  if (k === 'claim') post.content.claim = e.target.value.trim();
  if (k === 'scale') (post.overrides[active] ??= {}).scale = +e.target.value;
  keep(); render();
});
box.addEventListener('keydown', e => {
  const figure = e.target.closest('figure[data-pick]');
  if (post && figure && ['Enter', ' '].includes(e.key)) { e.preventDefault(); active = figure.dataset.pick; keep(); render(); }
});
box.addEventListener('click', async e => {
  const t = e.target.closest('button, figure'); if (!t || !post) return;
  if (t.dataset.pick) { active = t.dataset.pick; keep(); render(); }
  if (t.dataset.align) { (post.overrides[active] ??= {}).align = t.dataset.align; keep(); render(); }
  if (t.dataset.theme) { (post.overrides[active] ??= {}).theme = t.dataset.theme; keep(); render(); }
  if (t.dataset.c === 'png') await png(active);
  if (t.dataset.c === 'png-all') for (const f of Object.keys(C.FORMATS)) await png(f);
  if (t.dataset.c === 'webm') await motion(active);
  if (t.dataset.c === 'close') resetComposer();
});
const save = (blob, name) => { const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = name; a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 2000); };
const name = f => `${post.content.cite.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')}-${f}`;
export async function pngBlob(format) { if (draw(format).quote.overflow) throw Error('These words do not fit this artboard. Shorten them before exporting.'); const blob = await new Promise(ok => canvases[format].toBlob(ok, 'image/png')); if (!blob) throw Error('The browser could not create the image.'); return blob; }
async function png(f) { const ticket = opening, filename = name(f) + '.png'; try { const b = await pngBlob(f); if (ticket !== opening) return; save(b, filename); $('status').textContent = `${C.FORMATS[f].label} saved as PNG, ${(b.size / 1024).toFixed(0)} KB.`; } catch (error) { if (ticket === opening) $('status').textContent = error.message; } }
// A recorded reveal: the canvas is captured while the words appear, then held for a second.
export async function webmBlob(format, seconds = 4) {
  if (draw(format).quote.overflow) throw Error('These words do not fit this artboard. Shorten them before exporting.');
  if (!window.MediaRecorder) throw Error('this browser cannot record video');
  const type = ['video/webm;codecs=vp9', 'video/webm;codecs=vp8', 'video/webm'].find(t => MediaRecorder.isTypeSupported(t));
  if (!type) throw Error('this browser cannot record WebM');
  const ticket = opening, snapshot = structuredClone(post), c = canvases[format], stream = c.captureStream(30);
  const rec = new MediaRecorder(stream, { mimeType: type, videoBitsPerSecond: 4_000_000 }), parts = [];
  rec.ondataavailable = e => e.data.size && parts.push(e.data);
  const done = new Promise(ok => rec.onstop = ok); rec.start(200);
  const start = performance.now();
  try {
    await new Promise((ok, fail) => { const step = () => {
      if (ticket !== opening) { fail(Error('Recording stopped because the artboard was closed or replaced.')); return; }
      const t = (performance.now() - start) / 1000; draw(format, Math.min(t, seconds), snapshot);
      if (t < seconds + 1) requestAnimationFrame(step); else ok();
    }; step(); });
    rec.stop(); await done;
    return new Blob(parts, { type: 'video/webm' });
  } finally {
    if (rec.state !== 'inactive') rec.stop();
    stream.getTracks().forEach(track => track.stop());
    if (post && !box.hidden) render();
  }
}
async function motion(f) { const ticket = opening, filename = name(f) + '.webm'; $('status').textContent = 'Recording…'; try { const b = await webmBlob(f); if (ticket !== opening) return; save(b, filename); $('status').textContent = `${C.FORMATS[f].label} saved as a ${(b.size / 1024).toFixed(0)} KB WebM.`; } catch (e) { if (ticket === opening) $('status').textContent = e.message; } }
window.aurekaiComposer = { openComposer, pngBlob, webmBlob };
