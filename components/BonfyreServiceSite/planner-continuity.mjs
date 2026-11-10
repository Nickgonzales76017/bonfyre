// Pure migration proposal for the existing fixture planner. The host supplies its
// validatePlan and assessPlan functions. No catalogue, storage, receipt or effect owner is replaced.
const fail = message => { throw Error(message); };
const plain = x => x && typeof x === 'object' && !Array.isArray(x);
const identity = p => JSON.stringify([p.source_url, p.product_id, p.variant_id]);
function canonical(value) {
  if (value === undefined) return 'null';
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (plain(value)) return '{' + Object.keys(value).sort().map(k => JSON.stringify(k) + ':' + canonical(value[k])).join(',') + '}';
  return JSON.stringify(value);
}
function index(catalogue) {
  if (!plain(catalogue) || catalogue.schema !== 'service-site.fixture-catalogue.v1'
      || !/^[a-f0-9]{64}$/.test(catalogue.digest ?? '') || !Array.isArray(catalogue.products)
      || !catalogue.products.length || catalogue.products.length > 32) fail('Missing retained fixture release.');
  const aliases = new Map(), exact = new Map();
  for (const product of catalogue.products) {
    if (!plain(product) || typeof product.id !== 'string' || !product.id
        || !/^\d+$/.test(product.product_id) || !/^\d+$/.test(product.variant_id)
        || !/^[a-f0-9]{64}$/.test(product.source_sha256 ?? '')
        || !Number.isFinite(Date.parse(product.observed_at))
        || !Number.isSafeInteger(product.price_cents) || product.price_cents < 0) fail('Incomplete fixture source identity.');
    const url = new URL(product.source_url);
    if (url.protocol !== 'https:' || url.origin !== catalogue.origin || url.username || url.password) fail('Fixture source is outside its merchant.');
    const key = identity(product);
    if (aliases.has(product.id) || exact.has(key)) fail('Ambiguous fixture or source-option identity.');
    aliases.set(product.id, product); exact.set(key, product);
  }
  return { aliases, exact };
}
const viewFields = ['title', 'label', 'option', 'use', 'image', 'price_cents', 'dimensions', 'allowance', 'questions'];
const evidenceFields = ['source_sha256', 'observed_at'];
const compactAssessment = assessment => {
  if (!Number.isSafeInteger(assessment.subtotal) || assessment.subtotal < 0) fail('Listed subtotal exceeds supported precision.');
  return { subtotal_cents: assessment.subtotal, issues: [...assessment.issues], questions: [...assessment.questions] };
};

/** Compare owner-supplied releases and prepare a new plan without overwriting the saved one.
 * Match source URL plus product and variant IDs; names and local display aliases are not identity.
 * A missing selection blocks the whole candidate instead of silently dropping customer choices.
 */
export function reviewPlanRefresh(plan, previous, current, owner) {
  if (typeof owner?.validatePlan !== 'function' || typeof owner?.assessPlan !== 'function') fail('Use the existing fixture planner owner.');
  const old = index(previous), next = index(current);
  if (previous.id !== current.id || previous.origin !== current.origin) fail('Cannot migrate a project to another merchant.');
  if (previous.currency !== 'USD' || current.currency !== 'USD') fail('The current fixture owner supports USD only.');
  // A digest is supplied by the material owner. Refuse visible disagreement even
  // before that owner re-verifies the retained material; no proof of a signature is implied.
  if (previous.digest === current.digest && canonical(previous) !== canonical(current)) fail('Different catalogue contents share a release digest.');
  owner.validatePlan(plan, previous);
  const preserved = structuredClone(plan), candidate = structuredClone(plan);
  candidate.catalogue = current.digest;
  const selected = new Map();
  for (const item of plan.items) {
    const product = old.aliases.get(item.product);
    if (!product) fail('Saved plan refers to an unknown fixture.');
    const key = identity(product);
    if (!selected.has(key)) selected.set(key, { product, items: [] });
    selected.get(key).items.push(item.key);
  }
  const changes = [], evidenceOnly = [], missing = [], aliases = new Map();
  for (const [key, { product, items }] of selected) {
    const replacement = next.exact.get(key);
    if (!replacement) {
      missing.push({ fixture: product.id, product_id: product.product_id, variant_id: product.variant_id,
        source_url: product.source_url, label: product.label, item_keys: items, reason: 'Exact source option is absent from the new release.' });
      continue;
    }
    aliases.set(product.id, replacement.id);
    const fields = viewFields.filter(field => canonical(product[field]) !== canonical(replacement[field]));
    if (product.id !== replacement.id) fields.push('reference');
    const observation = evidenceFields.filter(field => product[field] !== replacement[field]);
    const row = { product_id: product.product_id, variant_id: product.variant_id, source_url: product.source_url, item_keys: items };
    if (fields.length) changes.push({ ...row, fields, before: Object.fromEntries(fields.map(field => [field, structuredClone(field === 'reference' ? product.id : product[field] ?? null)])),
      after: Object.fromEntries(fields.map(field => [field, structuredClone(field === 'reference' ? replacement.id : replacement[field] ?? null)])) });
    else if (observation.length) evidenceOnly.push({ ...row, fields: observation });
  }
  const before = compactAssessment(owner.assessPlan(preserved, previous));
  if (!missing.length) {
    for (const item of candidate.items) item.product = aliases.get(item.product);
    owner.validatePlan(candidate, current);
  }
  const after = missing.length ? null : compactAssessment(owner.assessPlan(candidate, current));
  return {
    schema: 'service-site.fixture-refresh-review.v1', project_id: plan.id,
    previous_digest: previous.digest, current_digest: current.digest,
    status: missing.length ? 'needs_selection' : changes.length ? 'needs_review'
      : previous.digest !== current.digest ? 'evidence_refresh' : 'unchanged',
    original: preserved, candidate: missing.length ? null : candidate,
    changes, evidence_only: evidenceOnly, missing,
    before, after, subtotal_delta_cents: after ? after.subtotal_cents - before.subtotal_cents : null,
    new_issues: after ? after.issues.filter(issue => !before.issues.includes(issue)) : [],
    resolved_issues: after ? before.issues.filter(issue => !after.issues.includes(issue)) : [],
    requires_user_choice: missing.length > 0, receipt: null, approval_carried_forward: false, effect: 'none'
  };
}

/** Bind an explicit customer choice to the exact version they reviewed.
 * Recompute the proposal; never trust a serialized review object as current state.
 * Returns a candidate for the host's revision-checked save, not a saved or approved plan.
 */
export function useReviewedRefresh(plan, previous, current, expected, owner) {
  const review = reviewPlanRefresh(plan, previous, current, owner);
  if (!plain(expected) || expected.previous_digest !== review.previous_digest || expected.current_digest !== review.current_digest
      || canonical(expected.original) !== canonical(review.original)) fail('The plan or catalogue changed after review. Review it again.');
  if (!review.candidate) fail('Resolve missing fixture choices before continuing.');
  return { plan: structuredClone(review.candidate), receipt: null };
}

/** Plain text for an existing review region. Render with textContent. */
export function refreshBrief(review) {
  const cents = n => new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(n / 100);
  return [review.original.title, 'Catalogue update review', '',
    ...review.changes.map(change => `Product ${change.product_id}, option ${change.variant_id}: ${change.fields.join(', ')} changed; ${change.item_keys.length} placement(s) affected.`),
    ...review.missing.map(item => `${item.label}: the selected option is no longer in this collection. Choose a replacement or remove it explicitly.`),
    ...(review.evidence_only.length ? [`${review.evidence_only.length} selected option(s) have refreshed source observations with unchanged displayed facts.`] : []),
    ...(review.after ? [`Listed subtotal: ${cents(review.before.subtotal_cents)} before; ${cents(review.after.subtotal_cents)} after. Delivery and tax excluded.`] : []),
    ...review.new_issues.map(issue => 'New question: ' + issue), '',
    'Your layout, project details and notes are retained. Confirm the new information before saving. No previous receipt or approval is carried forward.'
  ].join('\n');
}
