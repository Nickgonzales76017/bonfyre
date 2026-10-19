import {readFile,mkdir,writeFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import path from 'node:path';
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
export function parseProjection(text){
 const lines=text.trimEnd().split('\n');if(lines.shift()!=='source\tprojection\tsha256\tbytes')throw Error('Unknown instrument projection manifest.');
 if(!lines.length||lines.length>100)throw Error('Projection member count is outside the release bound.');
 const seen=new Set();return lines.map(line=>{const [source,projection,hash,bytes,...extra]=line.split('\t');
  if(extra.length||!source||!projection||!/^[-A-Za-z0-9_./]+$/.test(projection)||projection.startsWith('/')||projection.split('/').some(x=>!x||x==='.'||x==='..')||seen.has(projection)||!/^([a-f0-9]{64})$/.test(hash)||!/^\d+$/.test(bytes)||Number(bytes)>16*1024*1024)throw Error('Invalid or repeated projection member.');
  seen.add(projection);return {projection,sha256:hash,bytes:Number(bytes)};
 });
}
export async function probeMember(base,member,{request=fetch,timeout=10000}={}){
 const url=new URL(member.projection,base);if(url.origin!==new URL(base).origin)throw Error('Member escaped its publication origin.');
 let response,reader;
 try{
  response=await request(url,{method:'GET',redirect:'error',credentials:'omit',cache:'no-store',signal:AbortSignal.timeout(timeout)});
  if(response.status!==200)return {...member,status:response.status,ok:false,reason:'HTTP status'};
  if(!response.body)return {...member,status:response.status,ok:false,reason:'No response body'};
  reader=response.body.getReader();let bytes=0;const hash=createHash('sha256');
  for(;;){const part=await reader.read();if(part.done)break;bytes+=part.value.byteLength;if(bytes>member.bytes){await reader.cancel();return {...member,status:200,ok:false,reason:'Body exceeds expected length',observedBytes:bytes};}hash.update(part.value);}
  const observedHash=hash.digest('hex');return {...member,status:200,ok:bytes===member.bytes&&observedHash===member.sha256,observedBytes:bytes,observedHash,reason:bytes!==member.bytes?'Length mismatch':observedHash!==member.sha256?'Hash mismatch':''};
 }catch(error){try{await reader?.cancel();}catch{}return {...member,status:response?.status??null,ok:false,reason:String(error.message).slice(0,500)};}
}
export async function waitForProjection(base,members,{request=fetch,attempts=3,delay=5000,timeout=10000,budgetMs=90000,pause=ms=>new Promise(r=>setTimeout(r,ms))}={}){
 const url=new URL(base);if(!['http:','https:'].includes(url.protocol)||url.username||url.password||url.search||url.hash||!url.pathname.endsWith('/'))throw Error('Use a plain publication directory URL.');
 if(!Number.isInteger(attempts)||attempts<1||attempts>3||!members.length||members.length>110||!Number.isSafeInteger(budgetMs)||budgetMs<1000||budgetMs>120000)throw Error('Invalid read-only readiness bounds.');
 const rounds=[],deadline=Date.now()+budgetMs;
 for(let attempt=1;attempt<=attempts;attempt++){
  const results=new Array(members.length);let cursor=0;
  await Promise.all(Array.from({length:Math.min(3,members.length)},async()=>{for(;;){const i=cursor++;if(i>=members.length)return;const remaining=deadline-Date.now();results[i]=remaining<=0?{...members[i],ok:false,reason:'Readiness time budget exhausted'}:await probeMember(base,members[i],{request,timeout:Math.max(1,Math.min(timeout,remaining))});}}));
  rounds.push({attempt,results});if(results.every(r=>r.ok))return {ok:true,origin:base,rounds};if(Date.now()>=deadline)break;if(attempt<attempts)await pause(Math.min(delay,deadline-Date.now()));
 }
 return {ok:false,origin:base,rounds};
}
export async function verifyPublishedInstruments({base,root=process.cwd(),evidence='instrument-live-evidence'}={}){
 const dir=path.join(root,'site/instruments'),manifest=await readFile(path.join(dir,'SOURCE.tsv'),'utf8'),members=parseProjection(manifest);
 for(const name of ['index.html','app.mjs','instruments.css','UPSTREAM-LICENSE.txt','SOURCE.tsv']){const bytes=await readFile(path.join(dir,name));if(members.some(m=>m.projection===name))throw Error('Repeated entry member.');members.push({projection:name,bytes:bytes.length,sha256:sha(bytes)});}
 for(const member of members){const bytes=await readFile(path.join(dir,member.projection));if(bytes.length!==member.bytes||sha(bytes)!==member.sha256)throw Error('Local tested projection changed: '+member.projection);}
 const report=await waitForProjection(base,members);await mkdir(evidence,{recursive:true});await writeFile(path.join(evidence,'publication-readiness.json'),JSON.stringify(report,null,2));
 if(!report.ok)throw Error('Public files do not yet match the tested release: '+report.rounds.at(-1).results.filter(r=>!r.ok).map(r=>r.projection+' ('+r.reason+')').join(', '));
 console.log('Verified '+members.length+' published files against tested byte lengths and SHA-256. No application effects were retried.');return report;
}
