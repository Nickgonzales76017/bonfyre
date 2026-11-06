import { validateWorkbook, calculationErrors } from './workbook-model.mjs';
import {validateScenarioBook} from './workbook-scenarios.mjs';
import { validateReview, reviewBodyLimit } from './workbook-review.mjs';

export const workbookPrefix = 'finance-workbook:';
export const workbookKey = key => {
  if (typeof key !== 'string' || !/^finance-workbook:[a-zA-Z0-9_-]{1,100}$/.test(key)) throw Error('Invalid workbook record identity.');
  return key;
};
export async function readWorkbookReview(record) {
  if (!record || !Number.isSafeInteger(record.revision) || record.revision < 1) throw Error('Invalid saved workbook revision.');
  workbookKey(record.key);
  const value = record.value;
  let snapshot;
  if (value?.schema === 'bonfyre.finance-workbook.v1') snapshot = value.snapshot;
  else if (value?.schema === 'bonfyre.finance-workbook.v2') {
    if (!(value.snapshot instanceof Blob) || value.snapshot.size > 12 * 1024 * 1024) throw Error('Invalid saved workbook body.');
    snapshot = JSON.parse(await value.snapshot.text());
  } else throw Error('The saved record is not a financial workbook.');
  validateWorkbook(snapshot);
  let review = null;
  if (value.review !== undefined && value.review !== null) {
    if (!(value.review instanceof Blob) || value.review.size > reviewBodyLimit) throw Error('Invalid saved review decisions.');
    review = validateReview(JSON.parse(await value.review.text()), snapshot);
  }
  return { key: record.key, revision: record.revision, snapshot, source: value.source ?? null, review, scenarios: value.scenarios==null?null:validateScenarioBook(value.scenarios,snapshot) };
}
export function workbookReviewValue(snapshot, source = null, review = null, scenarios = null) {
  validateWorkbook(snapshot);
  if (calculationErrors(snapshot).length) throw Error('Resolve calculation errors before saving the review.');
  if (review) validateReview(review, snapshot);
  if(scenarios)validateScenarioBook(scenarios,snapshot);
  // The existing native owner limits record metadata to 256 KiB. Workbook
  // bodies use its immutable, checksummed file transport, just like media.
  return { schema: 'bonfyre.finance-workbook.v2', title: snapshot.name,
    snapshot: new Blob([JSON.stringify(snapshot)], { type: 'application/json' }), source, scenarios:scenarios?structuredClone(scenarios):null,
    review: review ? new Blob([JSON.stringify(review)], { type: 'application/json' }) : null,
    calculation: { engine: 'Univer 1.1.0', errors: 0 }, status: 'review-draft' };
}
