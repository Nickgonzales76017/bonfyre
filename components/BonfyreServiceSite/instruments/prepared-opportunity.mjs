// Prepare the existing research -> product handoff without manufacturing estimates.
import {readExactRecord} from './working-record.mjs';
import {validateExcerpt} from './research.mjs';
import {digest} from './data.mjs';
import {validatePlan} from './product.mjs';
export async function preparedOpportunity(store, key) {
  if (typeof key !== 'string' || !/^draft:instruments:incoming:[A-Za-z0-9._-]{1,180}$/.test(key)) throw Error('Choose a retained research opportunity.');
  const rows = (await store.readLocal(key)).filter(row => row.key === key);
  if (rows.length !== 1 || !Number.isSafeInteger(rows[0].revision) || rows[0].revision < 1) throw Error('This opportunity is not available in the current browser.');
  const row = structuredClone(rows[0]), value = row.value;
  if (!value || !Number.isSafeInteger(value.sourceRevision) || value.sourceRevision < 1 || value.candidate?.id !== key.slice('draft:instruments:incoming:'.length) || !Array.isArray(value.candidate.evidence) || !value.candidate.evidence.length || value.candidate.evidence.length > 100) throw Error('The opportunity has incomplete source coordinates.');
  const source = await readExactRecord(store,'research',value.sourceRecord);
  const used = new Set(value.candidate.evidence.map(ref => ref.sourceId));
  for (const id of used) {
    const material = source.value.sources?.find(s => s.id === id);
    if (!material || typeof material.body !== 'string' || await digest(material.body) !== material.sha256) throw Error('An opportunity source no longer passes its retained text digest.');
  }
  for (const ref of value.candidate.evidence) if (!validateExcerpt(source.value.sources,ref)) throw Error('A selected research passage changed or is missing. Review it in the source editor before adopting this candidate.');
  return {row,source,candidate:structuredClone(value.candidate),sourceAdvanced:source.revision !== value.sourceRevision};
}
export function adoptOpportunity(plan, prepared) {
  const candidate = structuredClone(prepared.candidate);
  if (plan.kind !== 'product' || plan.stories.some(story => story.id === candidate.id)) throw Error('This opportunity is already in this map, or the receiving record is not a product plan.');
  candidate.origin = {record:prepared.row.value.sourceRecord,revision:prepared.row.value.sourceRevision,incoming:prepared.row.key,incomingRevision:prepared.row.revision};
  const next = {...structuredClone(plan),stories:[...structuredClone(plan.stories),candidate],selected:candidate.id};
  validatePlan(next);
  return next;
}
