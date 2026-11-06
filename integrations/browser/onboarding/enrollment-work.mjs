// Read projections of the existing enrollment Queue and immutable checkpoints.
// These local identity lanes are not an Auth account or organization audience.
import {existsSync} from 'node:fs';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {Database} from 'bun:sqlite';
import {readCheckpointMaterial} from './checkpoint-material.mjs';
import {REQUIRED_STAGES,completionBlockers} from '../extension/enrollment-integrity.mjs';

const filters = new Set(['all','ready','open','waiting','decisions','finished']);
const states = new Set(['ready','blocked','running','retry_wait','complete','dead_letter','cancel_requested','cancelled']);
const pageSize = 20;
const columns = 'mission_id,node_id,operator_id,family,input_uri,status,next_attempt_at_ms,lease_expires_at_ms,updated_at_ms,failure_class,failure_message';
function lane(identity, account) {
  if (!['gmail','proton'].includes(identity) || typeof account !== 'string' || !account || account.length > 320)
    throw Error('A configured enrollment identity is required.');
}
function work(identity, value) {
  if (typeof value !== 'string' || !/^\d{3}-(?:gmail|proton)$/.test(value) || !value.endsWith('-'+identity))
    throw Error('The selected work belongs to another enrollment identity.');
  return value;
}
function bounded(value, limit, empty = false, field = 'description') {
  if (typeof value !== 'string' || !empty && !value || value.length > limit || /[\u0000-\u001f\u007f]/.test(value))
    throw Error('The native enrollment '+field+' is invalid.');
  return value;
}
function time(value) {
  if (value == null || value === 0) return null;
  if (!Number.isSafeInteger(value) || value < 0 || value > 8640000000000000)
    throw Error('The native enrollment time is invalid.');
  return value;
}
function project(row, identity, account, revision) {
  if (!row || row.node_id !== 'job' || row.family !== 'enrollment-'+identity || row.operator_id !== 'queue.account.enrollment' ||
      !states.has(row.status) || !Number.isSafeInteger(revision) || revision < 0 ||
      typeof row.input_uri !== 'string' || Buffer.byteLength(row.input_uri) > 16384)
    throw Error('The native enrollment record is unavailable.');
  const input = JSON.parse(row.input_uri); // Existing Queue input protocol; this projection adds no state format.
  const workId = work(identity, input?.work_id);
  if (row.mission_id !== 'enroll-'+workId || input.identity !== identity || input.account !== account)
    throw Error('The native enrollment item disagrees with its identity.');
  const address = new URL(bounded(input.signup_url, 2048, false, 'provider address'));
  if (address.protocol !== 'https:' || address.username || address.password)
    throw Error('The native provider address is invalid.');
  const provider = input.credential_provider || '';
  if (provider && !/^[a-z0-9_-]{1,64}$/.test(provider)) throw Error('The native provider binding is invalid.');
  const item = {work_id:workId,identity,account,platform:bounded(input.platform,160,false,'platform'),signup_url:address.href,credential_provider:provider};
  for (const key of ['source_id','api_gate','supplied_state','account_group'])
    if (input[key] !== undefined) item[key] = bounded(input[key],400,true,key);
  const nextAttempt = time(row.next_attempt_at_ms), leaseUntil = time(row.lease_expires_at_ms);
  return {item,revision,status:row.status,next_attempt_at_ms:nextAttempt,lease_expires_at_ms:leaseUntil,
    updated_at_ms:time(row.updated_at_ms),reason:bounded(row.failure_message || '',400,true,'failure message'),
    needs_decision:row.status === 'dead_letter' && row.failure_class === 'dependency',
    resumable:['ready','running'].includes(row.status) || row.status === 'retry_wait' && (!nextAttempt || nextAttempt <= Date.now())};
}
function open(c) {
  const db = new Database(c.db,{readonly:true});
  db.exec('PRAGMA query_only=ON; PRAGMA busy_timeout=1000; BEGIN');
  return db;
}
function latest(db, mission) {
  return db.query('SELECT revision,artifact_uri FROM workgraph_checkpoints WHERE mission_id=? AND node_id=\'job\' ORDER BY revision DESC LIMIT 1').get(mission) || {revision:0,artifact_uri:null};
}
function checkpointView(saved) {
  const text = (value, limit) => typeof value === 'string' ? value.replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g,'').slice(0,limit) : '';
  const stages = {};
  for (const stage of REQUIRED_STAGES) {
    const source = saved.stages?.[stage];
    if (!source || typeof source !== 'object' || Array.isArray(source)) continue;
    const value = {};
    for (const [key,limit] of Object.entries({at:64,note:400,evidence_kind:64,owner:128,receipt_ref:2048}))
      if (source[key] !== undefined) value[key] = text(source[key],limit);
    stages[stage] = value;
  }
  const notes = (Array.isArray(saved.notes)?saved.notes:[]).slice(-10).map(note => ({at:text(note?.at,64),why:text(note?.why,400),note:text(note?.note,400)}));
  return {stages,notes,interruption:text(saved.interruption,400),missing:completionBlockers(stages)};
}

export function listEnrollmentWork(c, identity, account, options = {}) {
  lane(identity,account);
  const filter = options.filter ?? 'all', cursor = options.cursor ?? '', search = options.search ?? '';
  if (!filters.has(filter) || typeof cursor !== 'string' || typeof search !== 'string' || search.length > 100 || /[\u0000-\u001f\u007f]/.test(search))
    throw Error('Choose a supported enrollment filter and a search of at most 100 characters.');
  if (cursor) work(identity,cursor);
  if (!existsSync(c.db)) return {seeded:false,identity,filter,search,items:[],cursor:null,more:false};
  const db = open(c);
  try {
    const clauses = ["family=?", "node_id='job'", 'mission_id>?'], args = ['enrollment-'+identity,cursor?'enroll-'+cursor:''];
    if (filter === 'ready') {clauses.push("(status='ready' OR status='retry_wait' AND (next_attempt_at_ms IS NULL OR next_attempt_at_ms<=?))");args.push(Date.now());}
    if (filter === 'open') clauses.push("status='running'");
    if (filter === 'waiting') clauses.push("status='retry_wait'");
    if (filter === 'decisions') clauses.push("status='dead_letter' AND failure_class='dependency'");
    if (filter === 'finished') clauses.push("status IN ('complete','cancelled')");
    if (search) {clauses.push("input_uri LIKE ? ESCAPE '\\'");args.push('%'+search.replace(/[\\%_]/g,'\\$&')+'%');}
    const rows = db.query('SELECT '+columns+' FROM workgraph_nodes WHERE '+clauses.join(' AND ')+' ORDER BY mission_id LIMIT ?').all(...args,pageSize+1);
    const more = rows.length > pageSize, items = rows.slice(0,pageSize).map(row => project(row,identity,account,latest(db,row.mission_id).revision));
    return {seeded:true,identity,filter,search,items,more,cursor:more?items.at(-1).item.work_id:null};
  } finally {db.close();}
}

export function inspectEnrollmentWork(c, identity, account, workId) {
  lane(identity,account);work(identity,workId);
  if (!existsSync(c.db)) throw Error('The native enrollment queue is unavailable.');
  const db = open(c);
  try {
    const mission = 'enroll-'+workId;
    const row = db.query('SELECT '+columns+' FROM workgraph_nodes WHERE mission_id=? AND node_id=\'job\'').get(mission);
    if (!row) throw Error('The selected enrollment item no longer exists.');
    const cp = latest(db,mission), view = project(row,identity,account,cp.revision);
    if (cp.revision > 0 && !cp.artifact_uri) throw Error('The selected checkpoint has no readable material. Its progress cannot be confirmed.');
    const legacy = join(c.root,'checkpoints',workId+'.json'), uri = cp.artifact_uri || (existsSync(legacy)?pathToFileURL(legacy).href:null);
    const version = uri && /\/r([1-9][0-9]*)-[0-9a-f]{64}\.mpk$/.exec(new URL(uri).pathname);
    if (version && Number(version[1]) !== cp.revision) throw Error('The selected checkpoint pointer disagrees with its material revision.');
    const saved = uri?readCheckpointMaterial(c.root,workId,uri):{stages:{}};
    return {...view,how:'inspected',read_only:true,...checkpointView(saved),
      cannot_infer:'Viewing setup does not claim work, clear a decision, confirm provider access or complete enrollment.'};
  } finally {db.close();}
}
