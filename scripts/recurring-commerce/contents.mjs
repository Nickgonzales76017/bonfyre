// Contents reference retained observations. Prices and product labels are projections,
// never copied into the authored selection or accepted as merchant authority.
const check = (ok, message) => { if (!ok) throw Error(message); };
const whole = (n, min, max) => Number.isSafeInteger(n) && n >= min && n <= max;
const exactKeys = (value, keys) => value && Object.keys(value).every(k => keys.includes(k));
export function contentReferences(contents) {
  if (!contents) return [];
  return [...new Set([...contents.defaults, ...contents.cycles.flatMap(c => c.items)].map(item => item.source))];
}
function checkItems(items) {
  check(Array.isArray(items) && items.length <= 50, 'A cycle supports up to 50 different products.');
  const ids = new Set(), products = new Set();
  for (const item of items) {
    check(exactKeys(item, ['id','source','offer','quantity']) && /^[a-z0-9:_-]{1,120}$/i.test(item.id ?? ''), 'Invalid content line identity.');
    check(/^observation:[a-f0-9]{64}$/.test(item.source ?? '') && whole(item.offer, 0, 1199), 'Choose an exact retained product offer.');
    check(whole(item.quantity, 1, 1000), 'Product quantities must be whole numbers from one to 1000.');
    const key = item.source + ':' + item.offer;
    check(!ids.has(item.id) && !products.has(key), 'Combine quantities instead of selecting the same product twice.');
    ids.add(item.id); products.add(key);
  }
}
export function checkContents(contents, dates, pattern) {
  check(exactKeys(contents, ['pricing','minimum','maximum','per_product','defaults','cycles']), 'Unsupported contents configuration.');
  check(['fixed','itemized'].includes(contents.pricing), 'Choose fixed bundle or itemized pricing.');
  check(!['finite_gift','try_before_buy'].includes(pattern) || contents.pricing === 'fixed', 'Prepaid gifts and try-on programs keep their separate price agreement.');
  check(dates.length > 0, 'Cycle contents require a dated program. Stock-triggered ordering uses its own observation.');
  check(whole(contents.minimum, 0, 1000) && whole(contents.maximum, 1, 1000) && contents.minimum <= contents.maximum && whole(contents.per_product, 1, contents.maximum), 'Set consistent minimum, maximum and per-product quantities.');
  checkItems(contents.defaults);
  check(Array.isArray(contents.cycles) && contents.cycles.length <= 60, 'Use at most 60 customized cycles.');
  const seen = new Set();
  for (const cycle of contents.cycles) {
    check(exactKeys(cycle, ['on','items']) && dates.includes(cycle.on) && !seen.has(cycle.on), 'A contents override no longer belongs to this series. Restore its default contents before changing the series.');
    seen.add(cycle.on); checkItems(cycle.items);
  }
  return contents;
}
export function projectContents(contents, anchor, observations, terms) {
  const custom = contents.cycles.find(c => c.on === anchor), items = custom?.items ?? contents.defaults;
  const sources = new Map(observations.map(o => [o.id, o])), problems = [];
  let units = 0, known = 0, missingPrice = false;
  const lines = items.map(item => {
    units += item.quantity;
    if (item.quantity > contents.per_product) problems.push('A product exceeds the per-product limit.');
    const source = sources.get(item.source), offer = source?.offers?.[item.offer];
    if (!source || !offer) problems.push('A selected product source is unavailable.');
    const unavailable = /(?:OutOfStock|SoldOut|Discontinued)$/i.test(offer?.availability ?? '');
    if (unavailable) problems.push((offer.product_name || 'A product') + ' was listed as unavailable. Choose a replacement.');
    const cents = offer && !offer.range && whole(offer.price?.usd_cents, 0, 100000000) ? offer.price.usd_cents : null;
    if (cents === null) missingPrice = true;
    const subtotal = cents === null ? null : cents * item.quantity;
    if (subtotal !== null) { check(whole(subtotal, 0, 100000000), 'Product subtotal exceeds the supported range.'); known += subtotal; }
    return {...item, title:offer?.product_name ?? 'Unavailable source', sku:offer?.sku ?? null, url:offer?.product_url ?? source?.captured?.url ?? '',
      unit_cents:cents, subtotal_cents:subtotal, availability:offer?.availability ?? null, recorded_at:source?.captured?.captured_at ?? null};
  });
  if (units < contents.minimum) problems.push('Choose at least ' + contents.minimum + ' units.');
  if (units > contents.maximum) problems.push('Choose at most ' + contents.maximum + ' units.');
  if (contents.pricing === 'itemized' && missingPrice) problems.push('An exact USD price is missing. It is not counted as zero.');
  check(whole(known, 0, 100000000), 'Contents subtotal exceeds the supported range.');
  const subtotal = contents.pricing === 'fixed' ? terms.price_cents : missingPrice ? null : known;
  const total = subtotal === null ? null : subtotal + (terms.shipping_cents ?? 0);
  check(total === null || whole(total, 0, 100000000), 'Cycle total exceeds the supported range.');
  return {selection:custom ? 'custom' : 'default', pricing:contents.pricing, lines, units,
    minimum:contents.minimum, maximum:contents.maximum, problems:[...new Set(problems)],
    product_subtotal_cents:subtotal, modeled_cents:problems.length ? null : total, shipping_cents:terms.shipping_cents ?? 0,
    standing:'proposal; recorded prices, not current inventory or a checkout quote'};
}
export function replaceContents(input, scope, items, settings = {}) {
  const next = structuredClone(input);
  const contents = next.terms.contents ??= {pricing:'itemized', minimum:1, maximum:12, per_product:6, defaults:[], cycles:[]};
  for (const key of Object.keys(settings)) check(['pricing','minimum','maximum','per_product'].includes(key), 'Unsupported contents setting.');
  Object.assign(contents, settings);
  if (scope === 'defaults') contents.defaults = structuredClone(items);
  else {
    contents.cycles = contents.cycles.filter(c => c.on !== scope);
    if (items !== null) contents.cycles.push({on:scope, items:structuredClone(items)});
    contents.cycles.sort((a,b) => a.on.localeCompare(b.on));
  }
  return next;
}
// Stable source offer matching never guesses from a title or an array position.
export function sourceOfferIdentity(offer) {
  return [offer.sku ?? '', offer.product_url ?? '', offer.offer_url ?? ''].map(s => s.length + ':' + s).join('|');
}
