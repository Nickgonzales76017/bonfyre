// Admission checks for the existing enrollment carrier. No queue or secret store.
export const HUMAN_STAGES = Object.freeze(['existing_access_checked', 'account_bound', 'scope_confirmed']);
export const REQUIRED_STAGES = Object.freeze([...HUMAN_STAGES.slice(0, 2), 'credential_sealed', 'credential_verified', 'scope_confirmed', 'consumer_bound']);
const record = value => value !== null && typeof value === 'object' && !Array.isArray(value);

export function assertReviewedItem(identity, mission, revision, expected) {
  if (!['gmail', 'proton'].includes(identity) || !Number.isSafeInteger(revision) || revision < 0)
    throw Error('Invalid enrollment identity or checkpoint revision.');
  if (!record(expected) || !/^\d{3}-(?:gmail|proton)$/.test(expected.work_id ?? '') ||
      !Number.isSafeInteger(expected.revision) || expected.revision < 0)
    throw Error('Reload the native item before changing it: work_id and reviewed revision are required.');
  if (mission !== `enroll-${expected.work_id}` || !expected.work_id.endsWith(`-${identity}`) || expected.revision !== revision)
    throw Error('This panel is stale or belongs to another item. Reload; nothing was changed.');
}

export function humanStage(stage, note, at = new Date().toISOString()) {
  if (!HUMAN_STAGES.includes(stage))
    throw Error('This stage needs its native owner result; it cannot be asserted by a button.');
  if (typeof note !== 'string' || note.length > 400) throw Error('A note must be at most 400 characters.');
  return { at, note, evidence_kind: 'human_assertion' };
}

export function completionBlockers(stages) {
  if (!record(stages)) return [...REQUIRED_STAGES];
  return REQUIRED_STAGES.filter(stage => {
    const value = stages[stage];
    if (!record(value) || !value.at) return true;
    if (HUMAN_STAGES.includes(stage)) return value.evidence_kind !== 'human_assertion';
    if (stage === 'credential_sealed') return value.evidence_kind !== 'native_seal' || value.owner !== 'BonfyreCredentials';
    if (stage === 'credential_verified') return value.evidence_kind !== 'native_verification' || value.owner !== 'BonfyreCredentials' || !value.receipt_ref;
    return value.evidence_kind !== 'native_consumer_binding' || !value.owner || !value.receipt_ref;
  });
}

export function requireBrowserContext(identity, inIncognitoContext, window) {
  if (!['gmail', 'proton'].includes(identity) || typeof inIncognitoContext !== 'boolean' ||
      !record(window) || !Number.isInteger(window.id) || window.id < 0 ||
      typeof window.incognito !== 'boolean' || window.type && window.type !== 'normal')
    throw Error('A normal browser window with a known identity context is required.');
  const privateLane = identity === 'proton';
  if (inIncognitoContext !== privateLane || window.incognito !== privateLane)
    throw Error('Gmail work stays in a regular window; Proton work stays in incognito. No tab was changed.');
}
