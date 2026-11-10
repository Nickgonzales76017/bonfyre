import { Worker } from 'node:worker_threads';
import { validateWorkbook } from './workbook-model.mjs';

export function calculateWorkbook(snapshot, { timeout = 10000, edits = [], review = null, includeLineage = false } = {}) {
  validateWorkbook(snapshot);
  if(typeof includeLineage!=='boolean')throw Error('Choose whether to include formula lineage.');
  if (!Array.isArray(edits) || edits.length > 32) throw Error('Choose at most 32 workbook edits.');
  if (!Number.isInteger(timeout) || timeout < 1 || timeout > 30000) throw Error('Invalid calculation deadline.');
  return new Promise((resolve, reject) => {
    const worker = new Worker(new URL('./workbook-calculation-worker.mjs', import.meta.url));
    let settled = false;
    const finish = (error, result) => {
      if (settled) return; settled = true; clearTimeout(timer); worker.terminate();
      if (error) reject(error); else resolve(result);
    };
    const timer = setTimeout(() => finish(Error('Workbook calculation exceeded its deadline. No calculated result was accepted.')), timeout);
    worker.once('message', result => finish(result.error ? Error(result.error) : null, result));
    worker.once('error', error => finish(error));
    worker.once('exit', code => { if (!settled) finish(Error(`Workbook calculation ended without a result (${code}).`)); });
    worker.postMessage({ snapshot: structuredClone(snapshot), edits, review, includeLineage });
  });
}
