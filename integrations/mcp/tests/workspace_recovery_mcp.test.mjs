import {test,expect} from 'bun:test';
import {createWorkspaceMcp} from '../workspace_mcp.mjs';
import {deployedRecoveryProfile} from '../workspace_recovery_mcp.mjs';
import {encode,decode} from '../../browser/workspace-msgpack.mjs';
import {createHash} from 'node:crypto';

// This fixture verifies the account transport and exact compiled batch. Native
// CMS/browser acceptance must be run separately against the actual native owner.
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const id='ws-'+'a'.repeat(40),release='b'.repeat(64),session='owner-current-session';
const target={binding:id,release,context:'self'};
const pack='experience.connected-instruments.workspace';
const continuations='view\tlabel\tentry\tprefix\ttitle\tquery\nproduction\tProduction\tinstruments/production/\tdraft:instruments:composition:\ttitle\trecord=$key\n';
function material(){
 const body='A source-backed workflow draft with a required follow-up review.',source={id:'source-1',title:'Original session',body,url:'',sha256:sha(body),coordinate:'UTF-16 offsets into exact original text'};
 return [1,'bonfyre.authored-workspace-recovery.v1',{archive_sha256:'c'.repeat(64),workflows:[{
  recovery_key:'meeting-review',source:{conversation_id:'session-1',message_ids:['session:session-1:line:4:block:1'],source_sha256:sha(body)},
  production:{kind:'composition',schema:'bonfyre.authored-workspace.v1',id:'reviewed-meeting',title:'Meeting follow-up',description:'Review the original source and its exact requirements.',layout:'columns',appearance:'paper',panels:[{id:'research',kind:'research',label:'Original requirements',role:'source',recordKey:'draft:instruments:research:meeting-source',revision:0}],connections:[],focus:'research'},
  records:[{key:'draft:instruments:research:meeting-source',value:{kind:'research',title:'Meeting requirements',sources:[source],findings:[],selectedSource:source.id,selectedFinding:null,tab:'sources'}}]
 }]}];
}
const encoded=value=>Buffer.from(encode(value)).toString('base64');
async function fixture(){
 const expected=await deployedRecoveryProfile(),rows=new Map(),requests=[],ledger=new Map();let writeCount=0;
 const state={canWrite:true,release,profile:{...expected},dropAfterCommit:false,deny:false,editAfterCommit:false,readbackUnavailable:false,wrongReadKey:false,wrongReceiptKey:false,failReadOnce:false};
 const response=value=>new Response(encode(value),{headers:{'content-type':'application/msgpack'}});
 const profileText=()=>Object.entries(state.profile).map(([key,value])=>key+'\t'+value).join('\n')+'\n';
 const request=async(input,options={})=>{
  const url=new URL(input),headers=new Headers(options.headers);requests.push({path:url.pathname,method:options.method??'GET',auth:headers.get('authorization'),origin:headers.get('origin'),redirect:options.redirect});
  if(headers.get('authorization')!=='Bearer '+session||state.deny)return new Response('',{status:403});
  if(url.pathname==='/apps/_destinations')return response([1,[['self','Personal',state.canWrite]]]);
  if(url.pathname==='/apps/_bindings')return response([1,'user:19','self',[[id,pack,'instruments/',state.release,'Connected instruments',true]]]);
  if(url.pathname===`/apps/${id}/CONTINUATIONS.tsv`)return new Response(continuations, {headers:{'content-type':'text/plain'}});
  if(url.pathname===`/apps/${id}/instruments/RECOVERY.tsv`)return new Response(profileText(),{headers:{'content-type':'text/plain'}});
  if(url.pathname!==`/apps/${id}/_records`)return new Response('',{status:404});
  const operation=url.searchParams.get('operation');
  if(operation==='get'){
   if(state.failReadOnce){state.failReadOnce=false;throw Error('temporary owner disconnect');}
   if(writeCount&&state.readbackUnavailable)return new Response('',{status:503});
   const key=url.searchParams.get('prefix'),row=structuredClone(rows.get(key)??null);if(row&&writeCount&&state.wrongReadKey)row[0]+='-different-key';
   return response([1,writeCount,false,key,row?[row]:[]]);
  }
  if(operation!=='write'||options.method!=='POST'||!state.canWrite)return new Response('',{status:403});
  expect(headers.get('origin')).toBe('http://127.0.0.1:28901');
  const packet=decode(new Uint8Array(options.body)),[version,requestId,changes]=packet;
  expect(version).toBe(1);expect(changes.length).toBeGreaterThan(1);
  const fingerprint=sha(options.body),prior=ledger.get(requestId);
  if(prior){if(prior.fingerprint!==fingerprint)return new Response('',{status:409});return response(prior.reply);}
  if(changes.some(([key,expectedRevision,mode])=>expectedRevision!==0||mode!==0||rows.has(key)))return new Response('',{status:409});
  const committed=changes.map(([key,, ,bytes])=>[key,1,new Uint8Array(bytes)]);for(const row of committed)rows.set(row[0],structuredClone(row));writeCount++;
  const reply=[1,writeCount,false,'',committed];ledger.set(requestId,{fingerprint,reply});
  if(state.editAfterCommit){const row=rows.get(committed[0][0]),value=decode(row[2]);value[1].title='An edit in the native editor';rows.set(row[0],[row[0],2,encode(value)]);}
  if(state.dropAfterCommit){state.dropAfterCommit=false;throw Error('Lost native acknowledgement');}
  if(state.wrongReceiptKey){const changed=structuredClone(reply);changed[4][0][0]+='-different-key';return response(changed);}
  return response(reply);
 };
 const call=createWorkspaceMcp({request,ownerUrl:'http://127.0.0.1:28901',publicOrigin:'https://aurekai.work'});
 const preview=(packet=material(),who=session)=>call('workspace_recovery_preview',{...target,material_base64:encoded(packet)},who);
 const commit=(review,extra={})=>call('workspace_recovery_import',{...target,review_base64:review.review_base64,preview_sha256:review.preview_sha256,request_id:'reviewed-import-1',...extra},session);
 return{state,rows,requests,preview,commit,call,get writes(){return writeCount;}};
}

test('preview preserves exact private source; import writes and reopens one atomic native composition',async()=>{
 const f=await fixture(),review=await f.preview();expect(f.writes).toBe(0);expect(review.disposition).toBe('ready');expect(review.source.message_ids).toEqual(['session:session-1:line:4:block:1']);
 const result=await f.commit(review);expect(result.state).toBe('native_work_attached');expect(result.records).toHaveLength(2);expect(f.writes).toBe(1);expect(result.external_actions_executed).toBe(false);
 expect(result.editor_url).toContain('/instruments/production/?record=draft%3Ainstruments%3Acomposition%3Arecovered-');
 const source=f.rows.get(result.records.find(row=>row.key.includes(':research:')).key);expect(decode(source[2])[1].sources[0].body).toBe(material()[2].workflows[0].records[0].value.sources[0].body);
 expect(f.requests.every(r=>r.auth==='Bearer '+session&&r.redirect==='error')).toBe(true);
 const second=await f.commit(review);expect(second.disposition).toBe('retained');expect(f.writes).toBe(1);
});

test('read-only MCP recovers one lost workspace response without retrying forbidden access',async()=>{
 const f=await fixture(),key='draft:instruments:research:read-retry';
 f.rows.set(key,[key,1,encode([2,{kind:'research',title:'Retained',sources:[],findings:[]},[]])]);
 f.state.failReadOnce=true;
 const saved=await f.call('workspace_records',{binding:id,key},session);
 expect(saved.record.key).toBe(key);
 expect(f.requests.filter(r=>r.path.endsWith('/_records')&&r.method==='GET')).toHaveLength(2);
 f.state.deny=true;
 await expect(f.call('workspace_records',{binding:id,key},session)).rejects.toMatchObject({status:403});
 expect(f.requests.filter(r=>r.path.endsWith('/_records')&&r.method==='GET')).toHaveLength(3);
});

test('a lost acknowledgement is reconciled against every current saved record',async()=>{
 const f=await fixture(),review=await f.preview();f.state.dropAfterCommit=true;
 const result=await f.commit(review);expect(result.disposition).toBe('recovered');expect(result.records.every(row=>row.revision===1)).toBe(true);expect(f.writes).toBe(1);
});

test('post-commit read failure retains request identity and reconciliation guidance without another write',async()=>{
 const f=await fixture(),review=await f.preview();f.state.readbackUnavailable=true;
 let failure;try{await f.commit(review);}catch(error){failure=error;}
 expect(failure?.code).toBe('recovery_uncertain');expect(failure?.status).toBe(502);expect(failure?.request_id).toBe('reviewed-import-1');expect(failure?.reconcile).toContain('every child');expect(f.writes).toBe(1);expect(f.rows.size).toBe(2);
 f.state.readbackUnavailable=false;const recovered=await f.commit(review);expect(recovered.disposition).toBe('retained');expect(f.writes).toBe(1);
});

test('a wrong receipt key requires exact current readback, and a wrong-key read cannot confirm attachment',async()=>{
 const f=await fixture(),review=await f.preview();f.state.wrongReceiptKey=true;f.state.wrongReadKey=true;
 let failure;try{await f.commit(review);}catch(error){failure=error;}
 expect(failure?.code).toBe('recovery_uncertain');expect(failure?.status).toBe(502);expect(failure?.request_id).toBe('reviewed-import-1');expect(failure?.reconcile).toContain('every child');expect(f.writes).toBe(1);
 f.state.wrongReadKey=false;const retained=await f.commit(review);expect(retained.disposition).toBe('retained');expect(f.writes).toBe(1);expect(retained.records.every(row=>!row.key.endsWith('-different-key'))).toBe(true);
 const exact=await fixture(),exactReview=await exact.preview();exact.state.wrongReceiptKey=true;const reconciled=await exact.commit(exactReview);expect(reconciled.disposition).toBe('recovered');expect(exact.writes).toBe(1);expect(reconciled.records.every(row=>exact.rows.has(row.key))).toBe(true);
});

test('current account access is rechecked before import and cannot be supplied as an actor argument',async()=>{
 const f=await fixture(),review=await f.preview();f.state.canWrite=false;await expect(f.commit(review)).rejects.toThrow();expect(f.writes).toBe(0);
 f.state.canWrite=true;f.state.deny=true;await expect(f.commit(review)).rejects.toThrow();expect(f.writes).toBe(0);
 f.state.deny=false;await expect(f.call('workspace_recovery_preview',{...target,material_base64:encoded(material()),actor:'user:1'},session)).rejects.toThrow();
 await expect(f.preview(material(),'another-account')).rejects.toThrow();expect(f.writes).toBe(0);
});

test('a different installed release or validator closure stops before creating any records',async()=>{
 const f=await fixture(),review=await f.preview();f.state.release='d'.repeat(64);await expect(f.commit(review)).rejects.toThrow();expect(f.writes).toBe(0);
 f.state.release=release;f.state.profile.compiler_sha256='e'.repeat(64);await expect(f.commit(review)).rejects.toThrow();await expect(f.preview()).rejects.toThrow();expect(f.writes).toBe(0);
});

test('altered source bodies, a forged review digest and a different destination cannot save',async()=>{
 const f=await fixture(),review=await f.preview(),bad=decode(Buffer.from(review.review_base64,'base64'));bad.workflow.changes[0].value.sources[0].body='Altered without a matching source digest';
 await expect(f.commit({...review,review_base64:encoded(bad)})).rejects.toThrow();
 await expect(f.commit(review,{preview_sha256:'f'.repeat(64)})).rejects.toThrow();
 await expect(f.commit(review,{binding:'ws-'+'9'.repeat(40)})).rejects.toThrow();expect(f.writes).toBe(0);
});

test('edited or partial recovered work is preserved rather than replaced by import',async()=>{
 const f=await fixture(),review=await f.preview();await f.commit(review);const [key,row]=f.rows.entries().next().value;
 const value=decode(row[2]);value[1].title='User edited this';f.rows.set(key,[key,2,encode(value)]);
 await expect(f.commit(review)).rejects.toThrow();expect(f.writes).toBe(1);expect(decode(f.rows.get(key)[2])[1].title).toBe('User edited this');
 f.rows.delete(key);await expect(f.commit(review)).rejects.toThrow();expect(f.writes).toBe(1);
});

test('a post-commit concurrent edit prevents a false complete attachment receipt',async()=>{
 const f=await fixture(),review=await f.preview();f.state.editAfterCommit=true;
 await expect(f.commit(review)).rejects.toThrow();expect(f.writes).toBe(1);expect([...f.rows.values()].some(row=>row[1]===2)).toBe(true);
});

test('the account transport bounds native input and refuses unreviewed multi-workflow batches',async()=>{
 const f=await fixture(),packet=material();packet[2].workflows.push({...structuredClone(packet[2].workflows[0]),recovery_key:'another-meeting'});
 await expect(f.preview(packet)).rejects.toThrow();
 await expect(f.call('workspace_recovery_preview',{...target,material_base64:'not canonical'},session)).rejects.toThrow();
 await expect(f.call('workspace_recovery_preview',{...target,material_base64:'A'.repeat(700000)},session)).rejects.toThrow();expect(f.writes).toBe(0);
});

test('unsupported native request identities are refused before any write',async()=>{
 const f=await fixture(),review=await f.preview();
 for(const request_id of ['Uppercase','has.dot','has:colon','x'.repeat(80)])await expect(f.commit(review,{request_id})).rejects.toThrow();
 expect(f.writes).toBe(0);
});
