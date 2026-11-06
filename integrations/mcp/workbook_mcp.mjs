// Financial semantics stay in the workbook owner. This adapter carries only
// bounded projections and exact candidate bodies through the native store.
import { createHash } from 'node:crypto';
import { encode, decode } from '../browser/workspace-msgpack.mjs';
import { WORKBOOK_APP_URI } from './workbook_app_resource.mjs';

const hash = { type: 'string', pattern: '^[a-f0-9]{64}$' };
const row = { type: 'integer', minimum: 0, maximum: 4999 };
const column = { type: 'integer', minimum: 0, maximum: 127 };
const object = (properties, required = Object.keys(properties)) => ({ type: 'object', additionalProperties: false, properties, required });
const array = (items, maxItems) => ({ type: 'array', minItems: 1, maxItems, items });
const identity = { binding: { type: 'string', pattern: '^ws-[a-f0-9]{40}$' }, key: { type: 'string', pattern: '^finance-workbook:[a-zA-Z0-9_-]{1,100}$' } };
const version = { expected_revision: { type: 'integer', minimum: 1, maximum: 2147483646 }, sha256: hash };
const roles = ['description', 'amount', 'account', 'date', 'suggested', 'review', 'sourceSheet', 'sourceRow', 'sourceCell', 'sourceHash'];
const proposal = { ...identity, ...version,
  table: object({ sheetId: { type: 'string', minLength: 1, maxLength: 128 }, headerRow: row, columns: object(Object.fromEntries(roles.map(role => [role, column])), ['description', 'amount', 'account']) }),
  edits: array(object({ row, role: { type: 'string', enum: ['amount', 'account'] }, value: { type: ['number', 'string'], maxLength: 1000 } }), 32),
  reviewed: array(object({ row, fingerprint: hash }), 100), reopen: array(row, 100),
  note: { type: 'string', maxLength: 1000 }, review_time: { type: 'string', minLength: 24, maxLength: 24 }
};
const tool = (name, description, properties, required, write = false) => ({ name, description, inputSchema: object(properties, required), annotations: { readOnlyHint: !write, destructiveHint: write, openWorldHint: false } });
export const WORKBOOK_MCP_TOOLS = [
  tool('workbook_review', 'Read a bounded page of the same saved financial review used by the workbook editor. Recalculates supported formulas without saving; returns current row fingerprints, exceptions and source metadata. Retain revision and sha256 for previews and for later pages. Optional cell inspection compares original addresses; moved rows do not imply source lineage. No original XLSX download, posting or approval.', { ...identity, ...version,
    release: hash,
    offset: { type: 'integer', minimum: 0, maximum: 4999 }, limit: { type: 'integer', minimum: 1, maximum: 100 },
    status: { type: 'string', enum: ['all', 'pending', 'reviewed', 'duplicates'] },
    inspect: object({ sheetId: { type: 'string', minLength: 1, maxLength: 128 }, row, column, rows: { type: 'integer', minimum: 1, maximum: 20 }, columns: { type: 'integer', minimum: 1, maximum: 12 } })
  }, Object.keys(identity)),
  tool('workbook_preview', 'Preview financial review changes against an exact saved revision and digest. Supports explicit column mapping, amount/account edits through Univer, content-bound review decisions and reopening rows. Recalculates before validating; returns changed cells and decisions plus a preview digest and review_time. Does not save files or records. Review annotations are not financial approvals.', proposal, [...Object.keys(identity), ...Object.keys(version)]),
  tool('workbook_update', 'Save the exact workbook_preview proposal through the native account owner. Repeat its inputs, review_time and preview_sha256 with a caller-retained request_id. Rechecks current access and revision; preserves original source file references. On an uncertain response, reread before retrying. Does not post, pay or publish.', { ...proposal, preview_sha256: hash, request_id: { type: 'string', pattern: '^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,78}$' } }, [...Object.keys(identity), ...Object.keys(version), 'review_time', 'preview_sha256', 'request_id'], true)
];
WORKBOOK_MCP_TOOLS[0]._meta = { ui: { resourceUri: WORKBOOK_APP_URI } };
WORKBOOK_MCP_TOOLS[0].description += ' In UI hosts this opens an interactive review. Pass the release from workspace_list to enable exact editor continuation.';
const sha = value => createHash('sha256').update(value).digest('hex');
const fail = (message, status = 400) => { throw Object.assign(Error(message), { status }); };
const clip = (value, limit = 500) => typeof value === 'string' && value.length > limit ? value.slice(0, limit) : value;
const bounded = value => { if (Buffer.byteLength(JSON.stringify(value)) > 256 * 1024) fail('The review response is too large. Request fewer rows.'); return value; };
const bytes = value => Buffer.from(JSON.stringify(value));
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
let domain;
async function owner() {
  return domain ??= Promise.all(['workbook-model.mjs', 'workbook-review.mjs', 'workbook-calculation.mjs', 'workbook-scenarios.mjs'].map(name => import('../../components/BonfyreServiceSite/' + name))).then(parts => Object.assign({}, ...parts));
}

export async function callWorkbookMcp(name, args, { read, files, commit, savedWorkUrl }) {
  const row = await read();
  if (!row || row[2] === null) fail('This financial workbook was not found.', 404);
  const identity = { binding: args.binding, key: args.key, revision: row[1], sha256: sha(row[2]) };
  if ((args.expected_revision === undefined) !== (args.sha256 === undefined)) fail('Provide both the observed revision and digest.');
  if (args.offset && args.expected_revision === undefined) fail('Continue review pages with the observed revision and digest.');
  if (args.expected_revision !== undefined && (args.expected_revision !== row[1] || args.sha256 !== identity.sha256)) fail('The workbook changed. Read its current revision before continuing.', 409);
  const packet = decode(row[2], { maxBytes: 256 * 1024 });
  if (!Array.isArray(packet) || packet.length !== 3 || ![2, 3].includes(packet[0]) || !Array.isArray(packet[2]) || packet[2].length > 64) fail('Invalid saved workbook representation.');
  const value = packet[1], refs = packet[2], known = new Map(), limits = { snapshot: 12, review: 8, 'source.original': 8, 'source.baseline': 12 };
  if (!['bonfyre.finance-workbook.v1', 'bonfyre.finance-workbook.v2'].includes(value?.schema)) fail('This record is not a financial workbook.');
  for (const ref of refs) {
    const path = Array.isArray(ref?.[0]) ? ref[0].join('.') : '';
    if (!Array.isArray(ref) || ref.length !== (packet[0] === 3 ? 7 : 6) || !Object.hasOwn(limits, path) || !same(ref[0], path.split('.')) || known.has(path) || !/^[a-f0-9]{64}$/.test(ref[1]) || !Number.isSafeInteger(ref[2]) || ref[2] < 0 || ref[2] > limits[path] * 1024 * 1024 || (ref[6] ?? 0) !== 0) fail('Invalid financial workbook file reference.');
    if (typeof ref[3] !== 'string' || Buffer.byteLength(ref[3]) > 255 || ref[4] !== null && (typeof ref[4] !== 'string' || Buffer.byteLength(ref[4]) > 1024 || !Number.isSafeInteger(ref[5]) || ref[5] < 0) || ref[4] === null && ref[5] !== null) fail('Invalid financial workbook file metadata.');
    let target = value; for (const part of ref[0]) { if (!target || !Object.hasOwn(target, part)) fail('Missing financial workbook file location.'); target = target[part]; }
    if (target !== null) fail('Invalid financial workbook file location.');
    known.set(path, ref);
  }
  async function jsonFile(path) { const ref = known.get(path); if (!ref) fail('Missing saved ' + path + ' body.'); return JSON.parse(await (await files.get(ref[1], ref[2])).text()); }
  const d = await owner();
  const snapshot = d.validateWorkbook(value.schema.endsWith('.v1') ? value.snapshot : await jsonFile('snapshot'));
  if(value.scenarios!=null)d.validateScenarioBook(value.scenarios,snapshot);
  const retainedReview = known.has('review') ? d.validateReview(await jsonFile('review'), snapshot) : null;
  if (value.review !== undefined && value.review !== null && !known.has('review')) fail('Invalid saved review decisions.');
  let review = retainedReview ?? d.detectReview(snapshot);
  if (name === 'workbook_review') {
    let calculated, calculation;
    try { const result = await d.calculateWorkbook(snapshot); calculated = result.snapshot; calculation = { engine: result.engine, errors: result.errors }; }
    catch (error) { calculated = snapshot; calculation = { error: error.message, calculated: false }; }
    const projection = review ? await d.projectReview(calculated, review) : null;
    const filter = args.status ?? 'all', all = projection?.rows.filter(r => filter === 'pending' ? !r.reviewed : filter === 'reviewed' ? r.reviewed : filter === 'duplicates' ? r.issues.includes('Possible duplicate') : true) ?? [];
    const offset = args.offset ?? 0, limit = args.limit ?? 50;
    const rows = all.slice(offset, offset + limit).map(r => ({ ...Object.fromEntries(Object.entries(r).map(([k, v]) => [k, clip(v)])), source: Object.fromEntries(Object.entries(r.source).map(([k, v]) => [k, clip(v)])), truncated_fields: [...Object.entries(r), ...Object.entries(r.source).map(([k, v]) => ['source.' + k, v])].filter(([, v]) => typeof v === 'string' && v.length > 500).map(([k]) => k) }));
    const result = { ...identity, title: snapshot.name, table: review?.table ?? null, mapping_required: !review, calculation,
      sheets: snapshot.sheetOrder.map(id => ({ id, name: snapshot.sheets[id].name, rows: snapshot.sheets[id].rowCount, columns: snapshot.sheets[id].columnCount })),
      summary: projection ? { total: projection.total, reviewed: projection.reviewed, pending: projection.pending, missing_amounts: projection.missingAmounts, possible_duplicates: projection.possibleDuplicates } : null,
      offset, status: filter, release: args.release ?? null, matched: all.length, more: offset + rows.length < all.length, next_offset: offset + rows.length < all.length ? offset + rows.length : null, rows,
      original: known.has('source.original') ? { sha256: known.get('source.original')[1], bytes: known.get('source.original')[2] } : null, saved_work_url: savedWorkUrl };
    if (args.inspect) {
      const range = args.inspect, sheet = calculated.sheets[range.sheetId];
      if (!snapshot.sheetOrder.includes(range.sheetId) || range.row + range.rows > sheet.rowCount || range.column + range.columns > sheet.columnCount) fail('Choose cells within the worksheet.');
      const baseline = known.has('source.baseline') ? d.validateWorkbook(await jsonFile('source.baseline')) : null;
      const cell = v => v ? { value: clip(v.v ?? null, 4096), formula: clip(v.f ?? null, 4096), truncated: [v.v, v.f].some(x => typeof x === 'string' && x.length > 4096) } : null;
      result.cells = [];
      for (let r = range.row; r < range.row + range.rows; r++) for (let c = range.column; c < range.column + range.columns; c++) result.cells.push({ row: r, column: c, current: cell(sheet.cellData?.[r]?.[c]), imported: cell(baseline?.sheets[range.sheetId]?.cellData?.[r]?.[c]) });
      result.source_comparison = 'Original cell addresses only; inserted or moved rows require a source check.';
    }
    return bounded(result);
  }
  if (!args.table && !args.edits?.length && !args.reviewed?.length && !args.reopen?.length) fail('Choose column mapping, edits or row decisions to preview.');
  if (args.table) {
    const next = d.createReview(args.table, snapshot);
    const equivalent = review && review.table.sheetId === next.table.sheetId && review.table.headerRow === next.table.headerRow && roles.every(role => review.table.columns[role] === next.table.columns[role]);
    if (!equivalent) review = next;
  }
  if (!review) fail('Choose the transaction worksheet and columns first.');
  const editKeys = (args.edits ?? []).map(edit => `${edit.row}:${edit.role}`);
  if (new Set(editKeys).size !== editKeys.length) fail('Choose each transaction field once.');
  if (new Set(args.reopen ?? []).size !== (args.reopen?.length ?? 0) || args.reviewed?.some(r => args.reopen?.includes(r.row))) fail('Choose distinct rows for review and reopening.');
  const reviewTime = args.review_time ?? new Date().toISOString(), time = Date.parse(reviewTime);
  if (!Number.isFinite(time) || new Date(time).toISOString() !== reviewTime || time > Date.now() + 30000 || time < Date.now() - 15 * 60000) fail('This preview time expired. Prepare a current preview.', 409);
  const result = await d.calculateWorkbook(snapshot, { edits: args.edits ?? [], review });
  if (result.errors.length) fail('Resolve calculation errors before saving: ' + result.errors.slice(0, 10).map(e => `${e.sheet}!${e.cell}: ${e.error}`).join(', '));
  if (args.reviewed?.length) review = await d.markRowsReviewed(result.snapshot, review, args.reviewed, args.note ?? '', { at: reviewTime });
  if (args.reopen?.length) review = d.reopenReviewRows(review, args.reopen);
  const before = retainedReview ? await d.projectReview(snapshot, retainedReview) : null, after = await d.projectReview(result.snapshot, review);
  const candidate = structuredClone(packet); candidate[1].schema = 'bonfyre.finance-workbook.v2'; candidate[1].snapshot = null; candidate[1].review = null;
  candidate[1].calculation = { engine: result.engine, errors: 0 }; candidate[1].status = 'review-draft';
  candidate[2] = candidate[2].filter(ref => !['snapshot', 'review'].includes(ref[0].join('.')));
  const bodies = [['snapshot', bytes(result.snapshot)], ['review', bytes(review)]];
  for (const [path, body] of bodies) candidate[2].push([[path], sha(body), body.length, 'application/json', null, null, ...(candidate[0] === 3 ? [0] : [])]);
  const body = encode(candidate, { maxBytes: 256 * 1024 });
  const previewHash = sha(encode([1, args.binding, args.key, identity.revision, identity.sha256, reviewTime, body]));
  const changed = []; let changedCount = 0;
  const describeCell = cell => ({ value: clip(cell?.v ?? null), formula: clip(cell?.f ?? null), truncated: [cell?.v, cell?.f].some(v => typeof v === 'string' && v.length > 500) });
  for (const id of snapshot.sheetOrder) {
    const beforeCells = snapshot.sheets[id].cellData ?? {}, afterCells = result.snapshot.sheets[id].cellData ?? {};
    for (const r of new Set([...Object.keys(beforeCells), ...Object.keys(afterCells)])) for (const c of new Set([...Object.keys(beforeCells[r] ?? {}), ...Object.keys(afterCells[r] ?? {})])) {
      const old = beforeCells[r]?.[c], next = afterCells[r]?.[c];
      if (!same([old?.v ?? null, old?.f ?? null], [next?.v ?? null, next?.f ?? null])) { changedCount++; if (changed.length < 100) changed.push({ sheetId: id, row: Number(r), column: Number(c), before: describeCell(old), after: describeCell(next) }); }
    }
  }
  const summary = { ...identity, state: 'workbook_preview', review_time: reviewTime, preview_sha256: previewHash,
    table: review.table, changed_cells: changed, changed_cell_count: changedCount, changed_cells_truncated: changedCount > 100,
    reviewed_before: before?.reviewed ?? 0, reviewed_after: after.reviewed, pending_after: after.pending,
    decisions_cleared: Math.max(0, (retainedReview?.decisions.length ?? 0) - review.decisions.length),
    decisions: after.rows.filter(r => args.reviewed?.some(s => s.row === r.row) || args.reopen?.includes(r.row)).map(r => ({ row: r.row, reviewed: r.reviewed, note: r.note })),
    source_references_preserved: candidate[2].filter(ref => ref[0][0] === 'source').length, saved_work_url: savedWorkUrl };
  bounded(summary);
  if (name === 'workbook_preview') return summary;
  if (args.preview_sha256 !== previewHash) fail('The confirmed preview does not match these workbook changes.', 409);
  try {
    for (const [, data] of bodies) if (await files.put(new Blob([data], { type: 'application/json' })) !== sha(data)) fail('The file owner returned a different workbook digest.', 502);
    const saved = await commit(body);
    return { ...saved, state: 'workbook_saved', review_time: reviewTime, preview_sha256: previewHash, reviewed: after.reviewed, pending: after.pending };
  } catch (error) { error.request_id = args.request_id; error.reconcile = 'Read the saved workbook before retrying; a failed response alone does not prove no write.'; throw error; }
}
