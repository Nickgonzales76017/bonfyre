// Aurekai's page loader: the knight holds the middle of a small field while
// waves close in. It runs only until the page's own content arrives.
// Fixed-timestep update with requestAnimationFrame rendering, so the game
// moves at one speed on every display. No images, no dependencies.

const W = 96, H = 40, STEP = 1000 / 30, CX = W / 2, CY = H / 2 + 1, REACH = 11;

// One character per pixel: i ink, a accent, m muted, g ground, . clear.
const KNIGHT = [
  '....aa..', '...aa...', '..iiii..', '.iiggii.', '.iiiiii.', '..iiii..',
  '.miaaim.', '.miiiim.', '..i..i..', '..i..i..', '.ii..ii.'
];
const IMP = [
  ['.mmm.', 'mgmgm', 'mmmmm', 'm.m.m'],
  ['.mmm.', 'mgmgm', 'mmmmm', '.m.m.']
];

function palette(node) {
  const css = getComputedStyle(document.documentElement), own = getComputedStyle(node).color;
  // World tokens first, then the account pages' matrix tokens, then the text colour.
  const pick = (...names) => names.map(n => css.getPropertyValue(n).trim()).find(Boolean);
  const faint = own.replace(/^rgb\((.*)\)$/, 'rgba($1, .5)');
  return {
    i: pick('--w-ink', '--ink') || own, a: pick('--w-accent', '--accent') || own,
    m: pick('--w-muted', '--r-muted') || faint, g: pick('--w-ground', '--paper') || 'transparent'
  };
}

function sprite(ctx, colors, rows, x, y, flip, tint) {
  for (let r = 0; r < rows.length; r++) {
    const row = rows[r];
    for (let c = 0; c < row.length; c++) {
      const ch = row[flip ? row.length - 1 - c : c];
      if (ch === '.') continue;
      ctx.fillStyle = tint && ch !== 'g' ? colors.a : colors[ch];
      ctx.fillRect(x + c, y + r, 1, 1);
    }
  }
}

function fresh() {
  return { tick: 0, wave: 1, swing: -1, hurt: 0, face: 1, foes: [], bursts: [] };
}

function spawn(s) {
  const angle = Math.random() * Math.PI * 2;
  s.foes.push({
    x: CX + Math.cos(angle) * 50, y: CY + Math.sin(angle) * 17,
    speed: 0.16 + Math.random() * 0.1 + s.wave * 0.03, step: Math.random() * 20 | 0
  });
}

// One fixed step of play.
function update(s) {
  s.tick++;
  if (s.tick % 150 === 0) s.wave = s.wave % 4 + 1;
  if (s.tick % Math.max(4, 16 - s.wave * 3) === 0 && s.foes.length < 36) spawn(s);
  if (s.swing < 0 && s.tick % 36 === 0) s.swing = 0;
  if (s.swing >= 0 && ++s.swing > 12) s.swing = -1;
  if (s.hurt) s.hurt--;
  let nearest = Infinity;
  s.foes = s.foes.filter(f => {
    const dx = CX - f.x, dy = CY - f.y, d = Math.hypot(dx, dy);
    if (d < nearest) { nearest = d; s.face = dx > 0 ? -1 : 1; }
    if (s.swing >= 0 && d < REACH || d < 4) {
      if (d < 4) s.hurt = 6;
      s.bursts.push({ x: f.x, y: f.y, life: 8 });
      return false;
    }
    f.x += dx / d * f.speed; f.y += dy / d * f.speed * 0.8; f.step++;
    return true;
  });
  s.bursts = s.bursts.filter(b => --b.life > 0);
}

function render(ctx, colors, s) {
  ctx.clearRect(0, 0, W, H);
  ctx.fillStyle = colors.m;
  for (let x = 2; x < W; x += 6) ctx.fillRect(x, H - 2, 2, 1);
  for (const f of s.foes) sprite(ctx, colors, IMP[(f.step >> 3) & 1], Math.round(f.x) - 2, Math.round(f.y) - 2, f.x > CX);
  for (const b of s.bursts) {
    ctx.fillStyle = b.life > 4 ? colors.a : colors.m;
    const r = 9 - b.life, x = Math.round(b.x), y = Math.round(b.y);
    for (const [dx, dy] of [[r, 0], [-r, 0], [0, r], [0, -r]]) ctx.fillRect(x + dx, y + dy, 1, 1);
  }
  const bob = s.swing < 0 && (s.tick >> 4) & 1;
  sprite(ctx, colors, KNIGHT, CX - 4, CY - 6 + bob, s.face < 0, s.hurt > 0);
  if (s.swing >= 0) {
    // The blade sweeps a full turn; the arc behind it fades to muted.
    for (let k = 0; k < 4; k++) {
      const t = (s.swing - k) / 12 * Math.PI * 2 * s.face;
      ctx.fillStyle = k ? colors.m : colors.i;
      for (let r = 5; r < (k ? REACH - 2 : REACH); r++) {
        if (k && r % 2) continue;
        ctx.fillRect(Math.round(CX + Math.cos(t) * r), Math.round(CY + Math.sin(t) * r * 0.6), 1, 1);
      }
    }
  }
}

// Shows the loader after `anchor` until `signal` aborts. Returns a stop function.
export function horde(anchor, signal) {
  if (!anchor?.isConnected || signal?.aborted) return () => {};
  const canvas = document.createElement('canvas');
  canvas.width = W; canvas.height = H;
  canvas.setAttribute('aria-hidden', 'true');
  canvas.dataset.pageLoader = '';
  canvas.style.cssText = 'display:block;width:min(288px,100%);aspect-ratio:96/40;image-rendering:pixelated;pointer-events:none;margin:12px 0';
  anchor.after(canvas);
  const ctx = canvas.getContext('2d'), colors = palette(anchor), state = fresh();
  let frame = 0, last = 0, lag = 0, stopped = false;

  const loop = now => {
    lag += Math.min(250, now - (last || now)); last = now;
    while (lag >= STEP) { update(state); lag -= STEP; }
    render(ctx, colors, state);
    frame = requestAnimationFrame(loop);
  };
  const pause = () => { cancelAnimationFrame(frame); frame = 0; last = 0; };
  const visible = () => { if (document.hidden) pause(); else if (!frame && !stopped && !still) frame = requestAnimationFrame(loop); };
  const stop = () => {
    if (stopped) return;
    stopped = true; pause(); canvas.remove();
    document.removeEventListener('visibilitychange', visible);
  };

  const still = matchMedia('(prefers-reduced-motion: reduce)').matches;
  if (still) {
    // A single held frame: the knight standing, five imps at a distance.
    for (let n = 0; n < 5; n++) spawn(state);
    render(ctx, colors, state);
  } else {
    document.addEventListener('visibilitychange', visible);
    visible();
  }
  signal?.addEventListener('abort', stop, { once: true });
  return stop;
}
