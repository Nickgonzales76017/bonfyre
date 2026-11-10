import { Database } from 'bun:sqlite';
import { createHash, randomUUID } from 'node:crypto';
import { mkdirSync, openSync, closeSync, fsyncSync, writeFileSync, readFileSync, linkSync, unlinkSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { pathToFileURL } from 'node:url';

export const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
const b64 = v => Buffer.from(String(v ?? ''), 'utf8').toString('base64url');
const fail = message => Object.assign(new Error(message), { status: 503 });
export const missionId = (id, revision, action) =>
  'review-' + sha256([id, revision, action].map(v => String(v).length + ':' + v).join('')).slice(0,44);
export const familyFor = (policy, action) => action === 'approve' ? policy.approvedFamily :
  ({ submit:'review-pending', reject:'review-rejected', revise:'review-revision-requested',
     delegate:'review-delegated', escalate:'review-escalated', resubmit:'review-pending' })[action];

function artifactBytes(e) {
  return Buffer.from([
    'review_transition v1', '  review ' + e.review_id,
    '  revision ' + e.revision, '  action ' + e.action,
    '  state ' + e.state, '  experience ' + e.experience,
    '  institution ' + e.institution, '  actor_user ' + e.actor_user,
    '  actor_subject_b64 ' + b64(e.actor_subject),
    '  assigned_user ' + (e.assigned_user ?? 'none'),
    '  source_uri_b64 ' + b64(e.source_uri),
    '  source_sha256 ' + e.source_sha256,
    '  reason_b64 ' + b64(e.reason),
    '  at ' + e.at, '  prev_sha256 ' + e.previous_digest, ''
  ].join('\n'), 'utf8');
}
function immutable(file, bytes) {
  mkdirSync(dirname(file), { recursive: true, mode: 0o700 });
  const temp = file + '.' + randomUUID() + '.tmp', fd = openSync(temp, 'wx', 0o600);
  try { writeFileSync(fd, bytes); fsyncSync(fd); } finally { closeSync(fd); }
  try {
    try { linkSync(temp, file); }
    catch (error) {
      if (error.code !== 'EEXIST' || !existsSync(file) || !readFileSync(file).equals(bytes))
        throw fail('Content-addressed review artifact changed');
    }
    const dir = openSync(dirname(file), 'r');
    try { fsyncSync(dir); } finally { closeSync(dir); }
  } finally { unlinkSync(temp); }
}
const ddl = [
  'CREATE TABLE IF NOT EXISTS reviews(id TEXT PRIMARY KEY,experience TEXT NOT NULL,institution TEXT NOT NULL,author_user INTEGER NOT NULL,author_actor TEXT NOT NULL,source_uri TEXT NOT NULL,source_sha256 TEXT NOT NULL,state TEXT NOT NULL,assigned_user INTEGER,revision INTEGER NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)',
  'CREATE TABLE IF NOT EXISTS events(review_id TEXT NOT NULL REFERENCES reviews(id),revision INTEGER NOT NULL,action TEXT NOT NULL,state TEXT NOT NULL,experience TEXT NOT NULL,institution TEXT NOT NULL,actor_user INTEGER NOT NULL,actor_subject TEXT NOT NULL,assigned_user INTEGER,source_uri TEXT NOT NULL,source_sha256 TEXT NOT NULL,reason TEXT NOT NULL,at TEXT NOT NULL,previous_digest TEXT NOT NULL,digest TEXT NOT NULL,artifact_uri TEXT NOT NULL,request_key TEXT NOT NULL,intent_digest TEXT NOT NULL,PRIMARY KEY(review_id,revision),UNIQUE(review_id,request_key))',
  'CREATE TABLE IF NOT EXISTS outbox(review_id TEXT NOT NULL,revision INTEGER NOT NULL,mission TEXT NOT NULL UNIQUE,family TEXT NOT NULL,input_uri TEXT NOT NULL,admitted_at TEXT,PRIMARY KEY(review_id,revision),FOREIGN KEY(review_id,revision) REFERENCES events(review_id,revision))'
].join(';') + ';';

export function reviewState(home, policies) {
  mkdirSync(home, { recursive: true, mode: 0o700 });
  const db = new Database(join(home, 'reviews.db'), { create: true, strict: true });
  db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;');
  const v = db.query('PRAGMA user_version').get().user_version;
  if (v !== 0 && v !== 1) { db.close(); throw fail('Unknown review schema version'); }
  db.exec(ddl);
  if (!v) db.exec('PRAGMA user_version=1');
  const record = id => db.query('SELECT * FROM reviews WHERE id=?').get(id);
  const history = id => db.query('SELECT * FROM events WHERE review_id=? ORDER BY revision').all(id);
  const outbox = id => db.query('SELECT * FROM outbox WHERE review_id=? ORDER BY revision').all(id);
  const artifactPath = (id, digest) => join(home, 'artifacts', sha256(id).slice(0,24), 'review-'+digest+'.yaff');
  function verify(row) {
    if (!row) throw Object.assign(new Error('Review not found'), { status:404 });
    const policy = policies.get(row.experience);
    if (!policy || policy.institution !== row.institution) throw fail('Review authority registry changed');
    const records = history(row.id), effects = outbox(row.id);
    if (records.length !== row.revision || effects.length !== row.revision) throw fail('Incomplete review history');
    let previous = '0'.repeat(64);
    for (let i=0; i<records.length; i++) {
      const e = records[i], job = effects[i], bytes=artifactBytes(e);
      const digest=sha256(bytes), file=artifactPath(row.id,digest), uri=pathToFileURL(file).href;
      if (e.revision !== i+1 || e.previous_digest !== previous || e.digest !== digest ||
          e.artifact_uri !== uri || !existsSync(file) || !readFileSync(file).equals(bytes) ||
          job.revision !== i+1 || job.mission !== missionId(row.id,e.revision,e.action) ||
          job.family !== familyFor(policy,e.action) || job.input_uri !== uri)
        throw fail('Review history or native effect binding changed');
      previous = digest;
    }
    const first = records[0], last=records.at(-1);
    if (last && (last.state !== row.state || last.assigned_user !== row.assigned_user ||
        last.source_uri !== row.source_uri || last.source_sha256 !== row.source_sha256 ||
        first.actor_user !== row.author_user || first.actor_subject !== row.author_actor))
      throw fail('Review projection disagrees with immutable history');
    return { row, policy, records, effects };
  }
  function append(row,policy,actor,action,next,assigned,reason,key,intent,uri=row.source_uri,digest=row.source_sha256,at=new Date().toISOString()) {
    const revision=row.revision+1, previous=row.revision ? history(row.id).at(-1).digest : '0'.repeat(64);
    const event={review_id:row.id,revision,action,state:next,experience:row.experience,institution:row.institution,
      actor_user:actor.userId,actor_subject:actor.actor,assigned_user:assigned,source_uri:uri,source_sha256:digest,
      reason,at,previous_digest:previous};
    const bytes=artifactBytes(event),hash=sha256(bytes),file=artifactPath(row.id,hash),address=pathToFileURL(file).href;
    immutable(file,bytes);
    db.query('INSERT INTO events(review_id,revision,action,state,experience,institution,actor_user,actor_subject,assigned_user,source_uri,source_sha256,reason,at,previous_digest,digest,artifact_uri,request_key,intent_digest) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)')
      .run(row.id,revision,action,next,row.experience,row.institution,actor.userId,actor.actor,assigned,uri,digest,reason,at,previous,hash,address,key,intent);
    db.query('UPDATE reviews SET state=?,assigned_user=?,revision=?,source_uri=?,source_sha256=?,updated_at=? WHERE id=?')
      .run(next,assigned,revision,uri,digest,at,row.id);
    db.query('INSERT INTO outbox(review_id,revision,mission,family,input_uri) VALUES(?,?,?,?,?)')
      .run(row.id,revision,missionId(row.id,revision,action),familyFor(policy,action),address);
  }
  function create(id,policy,actor,uri,digest,at) {
    db.query("INSERT INTO reviews(id,experience,institution,author_user,author_actor,source_uri,source_sha256,state,assigned_user,revision,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'pending',NULL,0,?,?)")
      .run(id,policy.experience,policy.institution,actor.userId,actor.actor,uri,digest,at,at);
  }
  return Object.freeze({record,history,outbox,verify,append,create,
    transaction:fn => db.transaction(fn)(),
    markAdmitted:(id,rev,at) => db.query('UPDATE outbox SET admitted_at=? WHERE review_id=? AND revision=? AND admitted_at IS NULL').run(at,id,rev),
    close:()=>db.close()});
}
