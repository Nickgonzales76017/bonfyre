import { baseline, restoreDraft, review, draftKey, priceCents } from './catalogue-model.mjs';
import { assessPlan, validatePlanRecordKey, planItemLimit } from './planner-model.mjs';

export const releasePrefix = 'release:catalogue:';
const fail = message => { throw Error(message); };
const canonical = value => Array.isArray(value) ? '[' + value.map(canonical).join(',') + ']' : value && typeof value === 'object' ? '{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}' : JSON.stringify(value);
export async function releaseDigest(value) {
  return [...new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(canonical(value))))].map(b => b.toString(16).padStart(2, '0')).join('');
}
const validRow = row => typeof row?.key === 'string' && Number.isSafeInteger(row.revision) && row.revision > 0 && row.value && typeof row.value === 'object';
const safeCents = value => { if (!Number.isSafeInteger(value) || value < 0) fail('A room subtotal exceeds supported precision.'); return value; };
const content = ({ title, store, entries, rooms, fixture_catalogues, impact }) => ({ title, store, entries, rooms, fixture_catalogues, impact });
const hash = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);
const text = (value, limit) => typeof value === 'string' && value.length <= limit;

export function catalogueRoomImpact(drafts, rooms, catalogues) {
  const known = new Map();
  for (const catalogue of catalogues) {
    if (!/^[a-f0-9]{64}$/.test(catalogue?.digest ?? '') || known.has(catalogue.digest)) fail('Ambiguous fixture collection.');
    known.set(catalogue.digest, catalogue);
  }
  const changes = new Map();
  for (const draft of drafts) {
    const id = JSON.stringify([draft.source_url, draft.product_id]);
    if (changes.has(id)) fail('Two drafts refer to the same merchant product.');
    changes.set(id, draft);
  }
  const impacted = [], unresolved = []; let checked = 0;
  for (const row of rooms) {
    try {
      const plan = row.value?.plan, catalogue = known.get(plan?.catalogue);
      if (!catalogue) fail('The saved room’s fixture collection is unavailable.');
      if (catalogue.currency !== 'USD') fail('This room uses an unsupported currency.');
      validatePlanRecordKey(row.key, plan);
      const assessment = assessPlan(plan, catalogue); safeCents(assessment.subtotal); checked++;
      let delta = 0n; const items = [], errors = [];
      for (const item of assessment.lines) {
        const product = item.product, draft = changes.get(JSON.stringify([product.source_url, product.product_id]));
        if (!draft) continue;
        const fields = draft.changes.filter(change => !change.field.includes(':') || change.field.startsWith('prep:') || change.field.endsWith(':' + product.variant_id));
        if (!fields.length) continue;
        const price = fields.find(change => change.field === 'price:' + product.variant_id);
        let difference = 0n;
        if (product.source_sha256 !== draft.source_sha256) errors.push('The product draft and room use different source observations.');
        if (price) {
          if (priceCents(price.before) !== BigInt(product.price_cents)) errors.push('The room price differs from the draft’s original price.');
          difference = priceCents(price.after) - priceCents(price.before);
        }
        delta += difference;
        items.push({ placement: item.key, product_id: product.product_id, variant_id: product.variant_id, label: product.label,
          source_url: product.source_url, fields: fields.map(change => change.field), before_cents: product.price_cents,
          proposed_cents: safeCents(Number(BigInt(product.price_cents) + difference)) });
      }
      if (items.length) impacted.push({ key: row.key, revision: row.revision, title: plan.title, project_id: plan.id,
        catalogue: catalogue.digest, currency: 'USD', before_cents: assessment.subtotal,
        proposed_cents: errors.length ? null : safeCents(Number(BigInt(assessment.subtotal) + delta)), delta_cents: errors.length ? null : Number(delta),
        budget_cents: plan.budget, exceeds_budget: errors.length ? null : plan.budget !== null && BigInt(assessment.subtotal) + delta > BigInt(plan.budget),
        items, layout_issues: assessment.issues, blockers: [...new Set(errors)] });
    } catch (error) { unresolved.push({ key: row.key, revision: row.revision, reason: error.message }); }
  }
  return { checked, impacted, unresolved, coverage: 'Saved room records in this snapshot; proposed prices exclude delivery and tax.' };
}

export async function prepareCatalogueRelease({ id = crypto.randomUUID(), title, records, keys, products, catalogues = [], createdAt = new Date().toISOString() }) {
  if (!/^[a-zA-Z0-9-]{8,80}$/.test(id) || typeof title !== 'string' || !title.trim() || title.length > 120 || !Number.isFinite(Date.parse(createdAt))) fail('Give the release a name of up to 120 characters.');
  if (!Array.isArray(keys) || !keys.length || keys.length > 100 || new Set(keys).size !== keys.length) fail('Choose between one and 100 distinct product drafts.');
  const byKey = new Map(records.map(row => [row.key, row]));
  if (byKey.size !== records.length) fail('The record collection contains duplicate identities.');
  if (new Set(products.map(draftKey)).size !== products.length) fail('The product collection contains duplicate identities.');
  const entries = [];
  for (const key of [...keys].sort()) {
    const row = byKey.get(key), product = products.find(product => draftKey(product) === key);
    if (!validRow(row) || !product || row.key !== draftKey(product)) fail('A selected product draft is missing. Reload the workspace.');
    const draft = row.value;
    if (draft.state !== 'draft' || draft.source_url !== product.source_url || draft.observed_at !== product.observed_at || typeof product.description_text !== 'string') fail('A product draft does not match its retained source.');
    const original = baseline(product), edited = restoreDraft(product, original, draft), checked = review(product, original, edited);
    if (checked.errors.length || !checked.changes.length || draft.changes.length !== checked.changes.length) fail('A selected draft has no valid changes.');
    const fields = changes => changes.map(({ field, before, after }) => [field, before, after]).sort((a, b) => a[0].localeCompare(b[0]));
    if (canonical(fields(draft.changes)) !== canonical(fields(checked.changes))) fail('A selected draft differs from its validated changes.');
    entries.push({ key, revision: row.revision, sha256: await releaseDigest(draft), title: edited.title, image: product.image ?? null,
      draft: structuredClone(draft) });
  }
  if (new Set(entries.map(entry => entry.draft.store)).size !== 1) fail('Stage one merchant catalogue per release.');
  const rooms = records.filter(row => row.key.startsWith('fixture-plan:') && !row.key.endsWith(':latest'));
  if (rooms.length > 200 || rooms.some(row => !validRow(row))) fail('The room collection cannot be reviewed in this release.');
  const roomSources = await Promise.all(rooms.sort((a, b) => a.key.localeCompare(b.key)).map(async row => ({ key: row.key, revision: row.revision, sha256: await releaseDigest(row.value) })));
  const impact = catalogueRoomImpact(entries.map(entry => entry.draft), rooms, catalogues);
  const content = { title: title.trim(), store: entries[0].draft.store, entries, rooms: roomSources, fixture_catalogues: catalogues.map(catalogue => catalogue.digest).sort(), impact };
  const value = { schema: 'service-site.catalogue-release.v1', id, created_at: createdAt, state: 'staged', ...content,
    sha256: await releaseDigest(content), review: null, publication: null };
  if (new TextEncoder().encode(JSON.stringify(value)).length > 220 * 1024) fail('This release exceeds 220 KiB. Select fewer drafts.');
  return value;
}

export function releaseChecks(value) {
  const invalid = () => fail('Invalid saved release.');
  if (value?.schema !== 'service-site.catalogue-release.v1' || !['staged', 'reviewed'].includes(value.state) || !hash(value.sha256) ||
      !text(value.id,80) || !/^[a-zA-Z0-9-]{8,80}$/.test(value.id) || !text(value.title,120) || !value.title.trim() ||
      !text(value.store,200) || !value.store || !text(value.created_at,64) || !Number.isFinite(Date.parse(value.created_at)) || value.publication !== null ||
      !Array.isArray(value.entries) || !value.entries.length || value.entries.length > 100 || !Array.isArray(value.rooms) || value.rooms.length > 200 ||
      !Array.isArray(value.fixture_catalogues) || value.fixture_catalogues.length > 20 || value.fixture_catalogues.some(digest => !hash(digest))) invalid();
  const seen = new Set();
  for (const row of [...value.entries,...value.rooms]) {
    if (!text(row?.key,500) || seen.has(row.key) || !Number.isSafeInteger(row.revision) || row.revision < 1 || !hash(row.sha256)) invalid();
    seen.add(row.key);
  }
  for (const entry of value.entries) {
    const d = entry.draft;
    if (!text(entry.title,255) || d?.store !== value.store || !text(d?.store,200) || !/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$/.test(d?.product_id ?? '') || entry.key !== `draft:catalogue:${d.store}:${d.product_id}` ||
        d.schema !== 'service-site.product-draft.v1' || d.state !== 'draft' || !hash(d.source_sha256) || !text(d.source_url,4000) ||
        !Array.isArray(d.changes) || !d.changes.length || d.changes.some(c => !text(c?.field,200) || !text(c.before,50000) || !text(c.after,50000) || (c.label !== undefined && !text(c.label,500)))) invalid();
  }
  if (value.rooms.some(row => !row.key.startsWith('fixture-plan:') || row.key.endsWith(':latest'))) invalid();
  const impact = value.impact;
  if (!impact || !Number.isSafeInteger(impact.checked) || impact.checked < 0 || impact.checked > value.rooms.length || !text(impact.coverage,1000) ||
      !Array.isArray(impact.impacted) || !Array.isArray(impact.unresolved) || impact.impacted.length > value.rooms.length || impact.unresolved.length > value.rooms.length) invalid();
  for (const room of impact.impacted) {
    if (!value.rooms.some(row => row.key === room.key && row.revision === room.revision) || !text(room.title,100) ||
        !Array.isArray(room.items) || room.items.length > planItemLimit || room.items.some(item => !text(item?.label,1000) || !text(item.placement,40) || !Array.isArray(item.fields) || item.fields.some(field => !text(field,200))) ||
        !Array.isArray(room.blockers) || room.blockers.some(message => !text(message,1000)) ||
        !Array.isArray(room.layout_issues) || room.layout_issues.some(message => !text(message,1000)) ||
        !Number.isSafeInteger(room.before_cents) || room.before_cents < 0 ||
        (room.proposed_cents !== null && (!Number.isSafeInteger(room.proposed_cents) || room.proposed_cents < 0)) ||
        (room.delta_cents !== null && !Number.isSafeInteger(room.delta_cents))) invalid();
  }
  if (impact.unresolved.some(room => !value.rooms.some(row => row.key === room.key && row.revision === room.revision) || !text(room.reason,1000))) invalid();
  if (value.state === 'staged' ? value.review !== null : !value.review || value.review.sha256 !== value.sha256 || !text(value.review.note,2000) || !Number.isFinite(Date.parse(value.review.at))) invalid();
  if (new TextEncoder().encode(JSON.stringify(value)).length > 220 * 1024) invalid();
  return [...value.entries, ...value.rooms].map(row => ({ key: row.key, expectedRevision: row.revision, checkOnly: true }));
}
export async function verifyCatalogueRelease(value) {
  releaseChecks(value);
  if (await releaseDigest(content(value)) !== value.sha256) fail('The saved release content does not match its digest.');
  for (const entry of value.entries) if (await releaseDigest(entry.draft) !== entry.sha256) fail('A release draft does not match its digest.');
  return value;
}
export async function reviewCatalogueRelease(saved, current, note, at = new Date().toISOString()) {
  await verifyCatalogueRelease(saved); await verifyCatalogueRelease(current);
  if (saved.id !== current.id || saved.sha256 !== current.sha256 || await releaseDigest(content(saved)) !== current.sha256) fail('Products or rooms changed after staging. Refresh the release and review it again.');
  if (current.impact.unresolved.length || current.impact.impacted.some(room => room.blockers.length)) fail('Resolve the room source checks before finishing review.');
  if (typeof note !== 'string' || note.length > 2000 || !Number.isFinite(Date.parse(at))) fail('Use a review note of up to 2000 characters.');
  return { ...current, state: 'reviewed', review: { sha256: current.sha256, note: note.trim(), at }, publication: null };
}

export async function saveCatalogueRelease(store, value, expectedRevision = 0) {
  await verifyCatalogueRelease(value);
  const checks = releaseChecks(value), key = releasePrefix + value.id;
  const rows = await store.saveLocalBatch([...checks, { key, value, expectedRevision }]);
  const saved = rows.find(row => row.key === key);
  if (!saved || saved.revision !== expectedRevision + 1 || await releaseDigest(saved.value) !== await releaseDigest(value)) fail('The release save could not be confirmed. Reload saved releases before trying again.');
  return saved;
}
