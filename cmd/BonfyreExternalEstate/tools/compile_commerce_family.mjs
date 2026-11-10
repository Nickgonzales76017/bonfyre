#!/usr/bin/env bun
// Receipt-bound commerce projections. Observation is never an effect or authority.
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';

const ROOT = resolve(fileURLToPath(import.meta.url), '../../../..');
const BOUNDARY = 'an observed product, offer, price, or availability is a source statement at one time; it is not inventory truth, a buyer quote, reservation, checkout authority, payment, settlement, delivery, or ownership';
const requireValue = (ok, message) => { if (!ok) throw new Error(message); };
const stable = (prefix, ...parts) => `${prefix}-${createHash('sha256').update(parts.map(String).join('\x1f')).digest('hex').slice(0, 24)}`;
const row = (type, body) => ({ type, namespace: 'root', body });
const published = body => ({ ...body, status: 'published' });
const base = e => [e.source_ref, e.receipt_ref, e.observed_at, e.merchant];
const read = path => JSON.parse(readFileSync(path, 'utf8'));
const jsonl = values => values.map(value => JSON.stringify(value)).join('\n') + (values.length ? '\n' : '');
const pathValue = (record, path) => (path || []).reduce((value, key) =>
  value && typeof value === 'object' && Object.hasOwn(value, key) ? value[key] : undefined, record);

export function minorPrice(value, divisor = 100) {
  const raw = typeof value === 'string' ? value.trim() : value;
  requireValue((typeof raw === 'string' && /^\d+$/.test(raw)) ||
    (typeof raw === 'number' && Number.isSafeInteger(raw) && raw >= 0),
  'price must be a non-negative integer minor unit');
  requireValue(Number.isSafeInteger(divisor) && divisor > 0, 'invalid currency minor unit');
  const amount = BigInt(raw), unit = BigInt(divisor);
  return `${amount / unit}.${String(amount % unit).padStart(String(divisor - 1).length, '0')}`;
}

export function decimalPrice(value) {
  const raw = String(value).trim();
  const match = /^([+-]?)(?:(\d+)(?:\.(\d*))?|\.(\d+))(?:[eE]([+-]?\d+))?$/.exec(raw);
  requireValue(match, 'price must be a non-negative decimal');
  const wholePart = match[2] || '0', fractionPart = match[3] || match[4] || '';
  requireValue(match[1] !== '-' || !/[1-9]/.test(wholePart + fractionPart),
    'price must be a non-negative decimal');
  const exponent = Number(match[5] || 0);
  requireValue(Number.isSafeInteger(exponent) && Math.abs(exponent) <= 10000,
    'price exponent outside supported range');
  const digits = wholePart + fractionPart, point = wholePart.length + exponent;
  const padded = point <= 0 ? `0.${'0'.repeat(-point)}${digits}`
    : point >= digits.length ? `${digits}${'0'.repeat(point - digits.length)}`
      : `${digits.slice(0, point)}.${digits.slice(point)}`;
  const [whole, fraction = ''] = padded.split('.');
  const canonical = `${whole.replace(/^0+(?=\d)/, '') || '0'}${fraction.replace(/0+$/, '') ? `.${fraction.replace(/0+$/, '')}` : ''}`;
  return canonical;
}

function identity(source, receipt, at, merchant, external, title, brand = '', sku = '', gtin = '', asin = '', mpn = '') {
  const pid = stable('product', source, merchant, external);
  return [pid, row('product_identity', published({ product_id: pid, title, brand, gtin, sku, asin, mpn,
    source_ref: source, receipt_ref: receipt, observed_at: at,
    cannot_infer: 'an identifier alias -> variant equivalence, merchant stock, or a right to buy' }))];
}

function offer(source, receipt, at, merchant, pid, external, href, seller = '', condition = 'new') {
  requireValue(href, 'observed merchant offer needs canonical_href');
  const oid = stable('offer', source, merchant, external, href);
  return [oid, row('merchant_offer', published({ offer_id: oid, product_id: pid, merchant,
    seller: seller || merchant, canonical_href: href, condition, fulfillment: '',
    source_ref: source, receipt_ref: receipt, observed_at: at, cannot_infer: BOUNDARY }))];
}

function price(source, receipt, at, pid, amount, currency, kind = 'current', oid = '') {
  return row('price_observation', published({ observation_id: stable('price', source, pid, oid, amount, at, kind),
    product_id: pid, offer_id: oid, amount, currency, kind, source_ref: source,
    receipt_ref: receipt, observed_at: at, cannot_infer: BOUNDARY }));
}

function availability(source, receipt, at, oid, available) {
  return row('availability_observation', published({ observation_id: stable('availability', source, oid, available ? 'True' : 'False', at),
    offer_id: oid, state: available ? 'in_stock' : 'out_of_stock', quantity_stated: '',
    source_ref: source, receipt_ref: receipt, observed_at: at, cannot_infer: BOUNDARY }));
}

const adapters = {
  'woocommerce-store-api': e => {
    const [source, receipt, at, merchant] = base(e);
    requireValue(Array.isArray(e.payload), 'woocommerce payload must be an array');
    return e.payload.flatMap(p => {
      const external = String(p.id ?? ''), title = p.name || '', href = p.permalink || '', prices = p.prices || {};
      requireValue(external && title && href, 'woocommerce product needs id, name, permalink');
      const [pid, product] = identity(source, receipt, at, merchant, external, title, (p.brands || [{}])[0]?.name || '', p.sku || '');
      const [oid, merchantOffer] = offer(source, receipt, at, merchant, pid, external, href);
      const minorUnit = Number(prices.currency_minor_unit ?? 2);
      requireValue(Number.isInteger(minorUnit) && minorUnit >= 0 && minorUnit <= 12, 'invalid currency minor unit');
      return [product, merchantOffer, price(source, receipt, at, pid, minorPrice(prices.price, 10 ** minorUnit), prices.currency_code || '', 'current', oid), availability(source, receipt, at, oid, !!p.is_in_stock)];
    });
  },
  'shopify-ajax': e => {
    const [source, receipt, at, merchant] = base(e), p = e.payload;
    requireValue(p && !Array.isArray(p) && p.id && p.title && p.url, 'shopify payload needs id, title, url');
    return (p.variants || []).flatMap(v => {
      requireValue(v.id, 'shopify variant needs id');
      const external = `${p.id}:${v.id}`;
      const [pid, product] = identity(source, receipt, at, merchant, external, p.title, p.vendor || '', v.sku || '', v.barcode || '');
      const [oid, merchantOffer] = offer(source, receipt, at, merchant, pid, external, p.url);
      return [product, merchantOffer, price(source, receipt, at, pid, minorPrice(v.price), p.currency || 'USD', 'current', oid), availability(source, receipt, at, oid, !!v.available)];
    });
  },
  'ebay-browse': e => {
    const [source, receipt, at, merchant] = base(e), items = e.payload?.itemSummaries || [];
    requireValue(Array.isArray(items), 'ebay browse payload needs itemSummaries');
    return items.flatMap(item => {
      const external = item.itemId || '', title = item.title || '', href = item.itemWebUrl || '', money = item.price || {};
      requireValue(external && title && href && Object.hasOwn(money, 'value') && money.currency,
        'ebay item needs identity, URL, and price');
      const [pid, product] = identity(source, receipt, at, merchant, external, title, '', '', typeof item.gtin === 'string' ? item.gtin : '');
      const [oid, merchantOffer] = offer(source, receipt, at, merchant, pid, external, href, item.seller?.username || '', String(item.condition || 'new').toLowerCase());
      const rows = [product, merchantOffer, price(source, receipt, at, pid, decimalPrice(money.value), money.currency, 'current', oid)];
      const state = String(item.availabilityStatus || '').toUpperCase();
      if (state === 'IN_STOCK' || state === 'OUT_OF_STOCK') rows.push(availability(source, receipt, at, oid, state === 'IN_STOCK'));
      return rows;
    });
  },
  keepa: e => {
    const [source, receipt, at, merchant] = base(e), p = e.payload;
    requireValue(p && p.asin && p.title, 'keepa normalized payload needs asin and title');
    const [pid, product] = identity(source, receipt, at, merchant, p.asin, p.title, p.brand || '', '', '', p.asin);
    const rows = [product];
    for (const point of p.history || []) {
      requireValue(point.observed_at && Object.hasOwn(point, 'amount') && point.currency,
        'keepa history point needs observed_at, amount, currency');
      rows.push(price(source, receipt, point.observed_at, pid, decimalPrice(point.amount), point.currency, 'historical'));
    }
    for (const item of p.offers || []) {
      requireValue(item.href && Object.hasOwn(item, 'amount') && item.currency, 'keepa offer needs href, amount, currency');
      const [oid, merchantOffer] = offer(source, receipt, at, merchant, pid, item.id || item.href, item.href, item.seller || '', item.condition || 'new');
      rows.push(merchantOffer, price(source, receipt, at, pid, decimalPrice(item.amount), item.currency, 'current', oid));
      if (typeof item.available === 'boolean') rows.push(availability(source, receipt, at, oid, item.available));
    }
    return rows;
  },
  'open-prices': e => {
    const [source, receipt, at, merchant] = base(e), items = e.payload?.items || [];
    requireValue(Array.isArray(items), 'open prices payload needs items');
    return items.flatMap(item => {
      const external = String(item.product_code || ''), title = item.product_name || external;
      requireValue(external && Object.hasOwn(item, 'price') && item.currency,
        'open prices item needs product_code, price, currency');
      const observedMerchant = item.location?.osm_name || merchant;
      const [pid, product] = identity(source, receipt, at, observedMerchant, external, title, '', external);
      return [product, price(source, receipt, item.observed_at || at, pid, decimalPrice(item.price), item.currency, 'historical_paid')];
    });
  },
};

const providerIndex = spec => Object.fromEntries(['phenotypes', 'providers'].flatMap(section =>
  (spec[section] || []).map(item => [item.id, item])));

export function compileEnvelope(spec, envelope) {
  const provider = envelope?.phenotype || envelope?.provider;
  requireValue(provider && Object.hasOwn(providerIndex(spec), provider) && Object.hasOwn(adapters, provider),
    'undeclared or unimplemented commerce provider');
  for (const field of ['source_ref', 'receipt_ref', 'observed_at', 'merchant', 'payload']) {
    const value = envelope[field];
    requireValue(value && (!Array.isArray(value) || value.length) &&
      (typeof value !== 'object' || Array.isArray(value) || Object.keys(value).length), `missing ${field}`);
  }
  return adapters[provider](envelope);
}

export function classifyProbe(_spec, probe) {
  const root = probe?.root, responses = probe?.responses;
  requireValue(typeof root === 'string' && root && responses && typeof responses === 'object' && !Array.isArray(responses),
    'probe needs root and responses');
  return (_spec?.phenotypes || []).flatMap(({ id, discovery }) => (discovery?.probes || []).flatMap(rule => {
    const answer = pathValue(responses, [rule.response_key]);
    const value = pathValue(answer, rule.items_path);
    const candidate = rule.shape === 'array_item' ? Array.isArray(value) && value.length ? value[0] : null
      : rule.shape === 'object' && value && typeof value === 'object' && !Array.isArray(value) ? value : null;
    if (!candidate || typeof candidate !== 'object' || Array.isArray(candidate) ||
      !(rule.required || []).every(key => Object.hasOwn(candidate, key)) ||
      !(rule.nonempty || []).every(key => Object.hasOwn(candidate, key) && candidate[key]) ||
      ((rule.required_any || []).length && !(rule.required_any || []).some(key => Object.hasOwn(candidate, key) && candidate[key]))) return [];
    return [{ root, phenotype: id, endpoint: rule.response_key,
      truth_level: 'observed_endpoint_shape', next: rule.next }];
  }));
}

export function planRoute(spec, policy, request) {
  const intent = request?.intent || '', rule = (policy.routes || []).find(item => item.intent === intent);
  requireValue(rule, 'unknown commerce observation intent');
  const available = new Set(request.available || []), entries = providerIndex(spec);
  const choices = rule.prefer.flatMap(provider => {
    const declared = entries[provider];
    if (!declared || !rule.requires.every(capability => (declared.capabilities || []).includes(capability))) return [];
    const credential = declared.credential || declared.discovery?.credential || '';
    return [{ provider, state: available.has(provider) ? 'ready' : credential ? 'waiting_credential' : 'waiting_source_frontier',
      requires: rule.requires, credential }];
  });
  requireValue(choices.length, 'no declared provider satisfies the required capability');
  return { intent, choices, selected: choices.find(choice => choice.state === 'ready') || null,
    cannot_infer: 'a route plan is not permission to contact a provider, a fresh observation, a product match, or a commercial effect' };
}

export function expand(rows) {
  const sources = [], aliases = [], seen = new Set();
  for (const item of rows) {
    const body = item.body || {};
    if (item.type === 'merchant_offer') {
      const href = body.canonical_href || '';
      let host;
      try { const url = new URL(href); if (['http:', 'https:'].includes(url.protocol)) host = url.hostname.toLowerCase().replace(/^www\./, ''); }
      catch { /* A relative or malformed link cannot become a candidate. */ }
      if (host && !seen.has(href)) {
        seen.add(href);
        sources.push({ address: href, protocol: 'commerce-offer', material: 'commerce', state: 'shadow',
          truth_level: 'observed_link_candidate', title: body.merchant || host,
          discovered_from: `receipt-bound merchant_offer ${body.offer_id || ''}`, identity: '',
          cannot_infer: 'an observed offer URL -> a callable shop API, a whole catalogue, merchant authority, or buyer-specific availability' });
      }
    }
    if (item.type === 'product_identity') for (const kind of ['gtin', 'asin', 'mpn', 'sku']) {
      const value = body[kind];
      if (value) aliases.push({ product_id: body.product_id, kind, value,
        source_ref: body.source_ref || '', receipt_ref: body.receipt_ref || '',
        cannot_infer: 'an alias value -> cross-merchant identity equivalence' });
    }
  }
  return { sources, aliases };
}

if (import.meta.main) {
  try {
    const [mode, ...argv] = process.argv.slice(2), args = {};
    for (let i = 0; i < argv.length; i += 2) {
      requireValue(argv[i]?.startsWith('--') && argv[i + 1], 'expected --key value arguments');
      args[argv[i].slice(2)] = argv[i + 1];
    }
    const providers = () => read(args.providers || args.spec || resolve(ROOT, 'schemas/control/commerce-observation-providers.json'));
    if (mode === 'materialize' || mode === 'classify') {
      requireValue(args.input && args.out, 'missing --input or --out');
      const result = mode === 'classify' ? classifyProbe(providers(), read(args.input)) : compileEnvelope(providers(), read(args.input));
      writeFileSync(args.out, jsonl(result));
      console.log(JSON.stringify({ rows: result.length, mode, cannot_infer: BOUNDARY }));
    } else if (mode === 'route') {
      requireValue(args.request && args.out, 'missing --request or --out');
      const result = planRoute(providers(), read(args.routing || resolve(ROOT, 'schemas/control/commerce-observation-routing.json')), read(args.request));
      writeFileSync(args.out, JSON.stringify(result, null, 2) + '\n');
      console.log(JSON.stringify({ intent: result.intent, ready: !!result.selected }));
    } else if (mode === 'expand') {
      requireValue(args.input && args['sources-out'] && args['aliases-out'], 'missing expansion input or output');
      const rows = readFileSync(args.input, 'utf8').split('\n').filter(Boolean).map(JSON.parse);
      const result = expand(rows);
      writeFileSync(args['sources-out'], jsonl(result.sources));
      writeFileSync(args['aliases-out'], jsonl(result.aliases));
      console.log(JSON.stringify({ merchant_candidates: result.sources.length, aliases: result.aliases.length,
        cannot_infer: 'a candidate is not a shop API, catalogue, or product match' }));
    } else throw new Error('unknown commerce compiler mode');
  } catch (error) { console.error(`commerce family compiler: ${error.message}`); process.exitCode = 2; }
}
