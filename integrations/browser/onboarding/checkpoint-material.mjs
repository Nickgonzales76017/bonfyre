// Immutable material for the existing native WorkGraph checkpoint pointer.
// The native queue alone admits a revision. An uncertain commit leaves an orphan,
// never an overwritten winner or an independently authoritative latest-state file.
import {constants,openSync,closeSync,readFileSync,writeFileSync,fsyncSync,fstatSync,lstatSync,realpathSync,mkdirSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {join,resolve,sep,basename} from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {encode,decode,maxPacket} from '../workspace-msgpack.mjs';
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
const object = x => x !== null && typeof x === 'object' && !Array.isArray(x);
function work(workId) {
  if (!/^\d{3}-(?:gmail|proton)$/.test(workId)) throw Error('Invalid checkpoint work identity.');
}
function within(root,path) {
  const parent=realpathSync(root), actual=realpathSync(path);
  if (!actual.startsWith(parent+sep)) throw Error('Checkpoint material is outside this enrollment.');
  return actual;
}
export function writeCheckpointMaterial(root,workId,revision,state) {
  work(workId);
  if (!Number.isSafeInteger(revision)||revision<1) throw Error('Invalid checkpoint revision.');
  if (!object(state)||!object(state.stages)) throw Error('Invalid checkpoint material.');
  const base=join(root,'checkpoints'), dir=join(base,workId);
  for (const candidate of [base,dir]) {
    try { mkdirSync(candidate,{mode:0o700}); } catch(error) {if(error.code!=='EEXIST')throw error;}
    if(lstatSync(candidate).isSymbolicLink()||!lstatSync(candidate).isDirectory())
      throw Error('Symlinked or non-directory checkpoint parent refused.');
    within(root,candidate);
  }
  const bytes=encode(state), sha=digest(bytes), path=join(dir,`r${revision}-${sha}.mpk`);
  let fd;
  try {
    fd=openSync(path,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);
    writeFileSync(fd,bytes); fsyncSync(fd);
  } catch(error) {
    if(error.code!=='EEXIST') throw error;
    const old=readCheckpointMaterial(root,workId,pathToFileURL(path).href);
    if(digest(encode(old))!==sha) throw Error('Existing checkpoint material disagrees with its digest.');
  } finally { if(fd!==undefined)closeSync(fd); }
  const dfd=openSync(dir,constants.O_RDONLY); try{fsyncSync(dfd);}finally{closeSync(dfd);}
  return pathToFileURL(path).href;
}
export function readCheckpointMaterial(root,workId,uri) {
  work(workId);
  const path=fileURLToPath(uri), base=resolve(root,'checkpoints'), exact=resolve(path);
  if(exact!==join(base,`${workId}.json`)&&!exact.startsWith(join(base,workId)+sep))
    throw Error('Checkpoint material belongs to another item.');
  const actual=within(base,path), canonicalBase=join(realpathSync(root),'checkpoints');
  if(lstatSync(base).isSymbolicLink() ||
    actual!==join(canonicalBase,`${workId}.json`)&&!actual.startsWith(join(canonicalBase,workId)+sep))
    throw Error('Checkpoint material belongs to another item or a symlinked directory.');
  const fd=openSync(path,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);
  let bytes;
  try {
    const before=fstatSync(fd);
    if(!before.isFile()||before.size>maxPacket)throw Error('Checkpoint material is not a bounded regular file.');
    bytes=readFileSync(fd);
    const after=fstatSync(fd);
    if(before.size!==after.size||before.mtimeMs!==after.mtimeMs||bytes.length>maxPacket)
      throw Error('Checkpoint material changed while reading.');
  } finally{closeSync(fd);}
  const match=/^r[1-9][0-9]*-([0-9a-f]{64})\.mpk$/.exec(basename(path));
  let state;
  if(match){if(digest(bytes)!==match[1])throw Error('Checkpoint digest mismatch.');state=decode(bytes);}
  else if(path.endsWith('.json'))state=JSON.parse(bytes.toString('utf8')); // read-only legacy compatibility
  else throw Error('Unsupported checkpoint material.');
  if(!object(state)||!object(state.stages))throw Error('Invalid checkpoint material.');
  return state;
}
