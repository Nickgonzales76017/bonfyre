// Integer-frame edits project into the existing imported-session clip contract.
export function gestureClip(original, source, action, deltaFrames, snapFrames = 1) {
  if (!['move', 'start', 'end'].includes(action) || !Number.isFinite(deltaFrames) || !Number.isSafeInteger(snapFrames) || snapFrames < 1) throw Error('Invalid timeline edit.');
  const clip = {...original}, limit = source.sample_rate * 30;
  const delta = Math.round(deltaFrames / snapFrames) * snapFrames;
  const bound = (n, lo, hi) => Math.max(lo, Math.min(hi, n));
  if (action === 'move') clip.at = bound(clip.at + delta, 0, limit - clip.frames);
  if (action === 'start') {
    const change = bound(delta, Math.max(-clip.at, -clip.from), clip.frames - 1);
    clip.at += change; clip.from += change; clip.frames -= change;
  }
  if (action === 'end') clip.frames = bound(clip.frames + delta, 1, Math.min(source.frames - clip.from, limit - clip.at));
  return clip;
}
export function sourceSelection(clip, source, start, end) {
  const from = Math.max(0, Math.min(source.frames - 1, Math.round(start * source.sample_rate)));
  const to = Math.max(from + 1, Math.min(source.frames, Math.round(end * source.sample_rate), from + source.sample_rate * 30 - clip.at));
  return {...clip, from, frames: to - from};
}
