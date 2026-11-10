// Setting a price with the arithmetic in view. A capability's cost to serve
// gives the floor; a demand curve through one stated reference point gives
// how many would buy at each price; the price that earns most, and the
// break-even volume, follow. Tiers (a monthly fee with included use and an
// overage rate) are tried against a spread of customer usage. A Van Westendorp
// survey, if one is loaded, gives the range buyers call acceptable. Nothing
// here sets a price: the result is a proposal for a person to accept.

export const unitCost = c => (+c.computeSeconds || 0) * (+c.computeRate || 0) + (+c.dataCost || 0) + (+c.supportMinutes || 0) / 60 * (+c.supportRate || 0);

// Constant elasticity through (refPrice, refVolume): volume = refVolume * (price / refPrice)^-e. Linear is offered
// too: volume falls to zero at `choke` times the reference price.
export function demand(price, { refPrice, refVolume, elasticity = 1.5, shape = 'elastic', choke = 3 }) {
  if (!(price > 0) || !(refPrice > 0)) return 0;
  if (shape === 'linear') return Math.max(0, refVolume * (1 - (price - refPrice) / (refPrice * (choke - 1))));
  return refVolume * Math.pow(price / refPrice, -elasticity);
}

export function curve(c, m, { from, to, steps = 200 } = {}) {
  const u = unitCost(c), lo = from ?? Math.max(u * 1.01, m.refPrice * 0.2), hi = to ?? m.refPrice * 3, fixed = +c.fixedMonthly || 0, out = [];
  for (let i = 0; i <= steps; i++) { const p = lo + (hi - lo) * i / steps, q = demand(p, m); out.push({ price: p, volume: q, revenue: p * q, profit: (p - u) * q - fixed }); }
  const best = out.reduce((a, b) => b.profit > a.profit ? b : a, out[0]);
  // With constant elasticity above 1 the profit-maximising price has a closed form: unit cost × e / (e − 1).
  const exact = m.shape !== 'linear' && m.elasticity > 1 && u > 0 ? u * m.elasticity / (m.elasticity - 1) : null;
  return { unit: u, points: out, best, exact };
}
// Margin is rounded to a millionth so float error (0.205 − 0.105) does not add a unit.
export const breakEven = (price, c) => { const margin = Math.round((price - unitCost(c)) * 1e6) / 1e6; return margin > 0 ? Math.ceil((+c.fixedMonthly || 0) / margin - 1e-9) : Infinity; };

// ------------------------------------------------------------ tiers

export function rng(seed) { let a = seed >>> 0 || 1; return () => { a = (a + 0x6d2b79f5) >>> 0; let t = a; t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; }; }
// Monthly use per customer, lognormal by its median and how far the busiest tenth runs above it.
export function usage({ customers = 200, median = 100, p90 = 600, seed = 1 }) {
  const r = rng(seed), mu = Math.log(median), sigma = Math.max(1e-6, Math.log(p90 / median) / 1.2816), out = [];
  for (let i = 0; i < customers; i++) { let u = 0, v = 0; while (!u) u = r(); v = r(); out.push(Math.exp(mu + sigma * Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v))); }
  return out;
}
export const bill = (use, t) => (+t.fee || 0) + Math.max(0, use - (+t.included || 0)) * (+t.overage || 0);
// Each customer picks the cheapest tier for their use (and pay-per-use when it is cheaper than every tier).
export function tierOutcome(uses, tiers, { perUse = null, unit = 0 } = {}) {
  const options = [...tiers.map((t, i) => ({ ...t, i })), ...(perUse ? [{ name: 'Pay per use', fee: 0, included: 0, overage: perUse, i: tiers.length }] : [])];
  const rows = options.map(o => ({ name: o.name, customers: 0, revenue: 0, use: 0 }));
  for (const u of uses) { let pick = options[0], cost = bill(u, pick); for (const o of options) { const b = bill(u, o); if (b < cost - 1e-9) { pick = o; cost = b; } }
    rows[pick.i].customers++; rows[pick.i].revenue += cost; rows[pick.i].use += u; }
  const revenue = rows.reduce((a, r) => a + r.revenue, 0), use = rows.reduce((a, r) => a + r.use, 0);
  return { rows, revenue, perUnit: use ? revenue / use : 0, margin: revenue - use * unit, customers: uses.length };
}

// ------------------------------------------------------------ what buyers say

// Van Westendorp: four prices per respondent (too cheap, cheap, expensive, too expensive). The acceptable range runs
// from where "too cheap" crosses "expensive" to where "cheap" crosses "too expensive".
export function vanWestendorp(rows) {
  const clean = rows.filter(r => [r.tooCheap, r.cheap, r.expensive, r.tooExpensive].every(Number.isFinite) && r.tooCheap <= r.cheap && r.cheap <= r.expensive && r.expensive <= r.tooExpensive);
  if (clean.length < 5) return { usable: clean.length, dropped: rows.length - clean.length };
  const prices = [...new Set(clean.flatMap(r => [r.tooCheap, r.cheap, r.expensive, r.tooExpensive]))].sort((a, b) => a - b), n = clean.length;
  const share = (p, k, below) => clean.filter(r => below ? r[k] >= p : r[k] <= p).length / n;
  const lines = prices.map(p => ({ p, tooCheap: share(p, 'tooCheap', true), cheap: share(p, 'cheap', true), expensive: share(p, 'expensive', false), tooExpensive: share(p, 'tooExpensive', false) }));
  const cross = (a, b) => { for (let i = 1; i < lines.length; i++) { const d0 = lines[i - 1][a] - lines[i - 1][b], d1 = lines[i][a] - lines[i][b]; if (d0 >= 0 && d1 <= 0) { const t = d0 === d1 ? 0 : d0 / (d0 - d1); return lines[i - 1].p + t * (lines[i].p - lines[i - 1].p); } } return null; };
  return { usable: n, dropped: rows.length - n, lines, low: cross('tooCheap', 'expensive'), optimal: cross('tooCheap', 'tooExpensive'), indifferent: cross('cheap', 'expensive'), high: cross('cheap', 'tooExpensive') };
}
export function readSurvey(text) {
  const lines = String(text).split(/\r?\n/).filter(l => l.trim()), head = lines[0].toLowerCase().split(',');
  const at = re => head.findIndex(h => re.test(h));
  const i = { tooCheap: at(/too.?cheap|too.?low/), cheap: at(/^(?!.*too).*(cheap|bargain|low)/), expensive: at(/^(?!.*too).*(expensive|high)/), tooExpensive: at(/too.?(expensive|high)/) };
  if (Object.values(i).some(x => x < 0)) throw Error('The survey needs four columns: too cheap, cheap, expensive and too expensive.');
  return lines.slice(1).map(l => { const c = l.split(',').map(x => +x.replace(/[^0-9.]/g, '')); return { tooCheap: c[i.tooCheap], cheap: c[i.cheap], expensive: c[i.expensive], tooExpensive: c[i.tooExpensive] }; });
}

// What a person would accept or change: the proposed price and the reasons, never a price set.
export function proposal(cap, c, m, chosen, tiers) {
  const cv = curve(c, m), u = unitCost(c);
  return { capability: cap.id, proposed_unit_price: +chosen.toFixed(4), unit_cost: +u.toFixed(4), margin_per_unit: +(chosen - u).toFixed(4), break_even_volume: breakEven(chosen, c),
    profit_maximising_price_by_model: +cv.best.price.toFixed(4), assumptions: { cost: c, demand: m }, tiers, set_by: null,
    cannot_infer: 'a proposal is not a price; the desk sets one only when a person accepts it' };
}
