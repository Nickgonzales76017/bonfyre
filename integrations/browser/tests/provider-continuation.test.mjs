// Actual WebWork SQLite owner + actual native packet validator. Chrome APIs are
// controlled doubles here; installed-extension coverage is a separate witness.
import {test,expect} from 'bun:test';
import {readFileSync,writeFileSync,mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {WebWork} from '../../../cmd/BonfyreSourceFrontier/tools/web_work.mjs';
import {encode,decode} from '../workspace-msgpack.mjs';
import {admitProviderPacket} from '../host/provider-admission.mjs';
import {createProviderBridge} from '../extension/provider-continuation.mjs';
const origin='https://aurekai.work',challenge='a'.repeat(64),clock=Date.UTC(2026,9,21,20);
function owner(subject='user:1',scope='self',canAdminister=false,path=':memory:'){
 return new WebWork(path,{subject,scope,actor:'user:1',clock:()=>clock,config:{canAdminister}});
}
function plan(native,mode='account') {return native.plan({requestId:'provider-review-001-'+mode,title:'Native provider setup',mode,spec:{provider:'exa',operation:'search',query:'Source guide',objective:'Local provider continuation witness',limit:3}});}
const review=(native,job)=>({context:native.context,jobId:job.id,revision:job.revision,binding:{...native.binding}});
function packet(native,job){const r=review(native,job);return {origin,review:r,challenge,packet:Array.from(encode([1,true,native.provider({id:job.id,revision:job.revision,challenge,origin})]))};}

test('native provider projection is read-only, owner-scoped and never credential proof',()=>{
 const native=owner(),job=plan(native);try{
  const p=admitProviderPacket(packet(native,job),{now:clock});expect(p.work.id).toBe(job.id);expect(p.work.revision).toBe(1);expect(p.provider.url).toBe('https://dashboard.exa.ai/api-keys');expect(p.binding).toEqual({actor:'user:1',subject:'user:1',scope:'self'});
  expect(native.get(job.id).providerContinuation).toMatchObject({canContinue:true,canAdminister:false,credentialInput:null});expect(native.get(job.id).state).toBe('planned');expect(native.get(job.id).revision).toBe(1);expect(native.list().binding).toEqual(job.binding);expect(native.connectionStatus().binding).toEqual(job.binding);expect(p).not.toHaveProperty('connected');expect(p).not.toHaveProperty('credential');
 }finally{native.close();}
});
test('public, archived and stale work cannot admit provider continuation',()=>{
 const native=owner();try{const publicJob=plan(native,'public');expect(publicJob.providerContinuation.canContinue).toBe(false);expect(()=>native.provider({id:publicJob.id,revision:1,challenge,origin})).toThrow('public');const job=plan(native);expect(()=>native.provider({id:job.id,revision:2,challenge,origin})).toThrow('changed');const archived=native.archive({id:job.id,revision:1});expect(()=>native.provider({id:job.id,revision:archived.revision,challenge,origin})).toThrow('Restore');}finally{native.close();}
});
test('exact native actor/destination closure cannot cross owner or organization',()=>{
 const dir=mkdtempSync(join(tmpdir(),'provider-owner-')),path=join(dir,'work.db'),a=owner('user:1','self',false,path),b=new WebWork(path,{subject:'user:2',scope:'self',actor:'user:2'}),org=owner('institution:studio','institution',false,path);
 try{const job=plan(a);expect(()=>b.provider({id:job.id,revision:1,challenge,origin})).toThrow('destination');expect(()=>org.provider({id:job.id,revision:1,challenge,origin})).toThrow('destination');const own=plan(org);expect(org.get(own.id).binding).toEqual({actor:'user:1',subject:'institution:studio',scope:'institution'});const message=packet(a,job);message.review.binding={actor:'user:2',subject:'user:2',scope:'self'};expect(()=>admitProviderPacket(message,{now:clock})).toThrow('account');}finally{a.close();b.close();org.close();rmSync(dir,{recursive:true,force:true});}
});
test('native packet refuses changed purpose, challenge, provider, owner, origin and expiry',()=>{
 const native=owner(),job=plan(native);try{
  const original=native.provider({id:job.id,revision:1,challenge,origin});
  for(const change of [p=>p.purpose='login-complete',p=>p.challenge='b'.repeat(64),p=>p.provider.url='https://attacker.example/',p=>p.origin='https://aurekai.life',p=>p.work.scope='b'.repeat(64),p=>p.work.revision=2,p=>p.returnPath='/account',p=>p.expiresAt=clock-1,p=>p.secret='not-allowed']){const changed=structuredClone(original);change(changed);expect(()=>admitProviderPacket({...packet(native,job),packet:Array.from(encode([1,true,changed]))},{now:clock})).toThrow();}
  expect(()=>admitProviderPacket({...packet(native,job),packet:Array(65537).fill(0)},{now:clock})).toThrow('bounded');expect(()=>admitProviderPacket(packet(native,job),{now:clock+60001})).toThrow('expired');
 }finally{native.close();}
});
test('read access never advertises key intake; stale/admin-refused guarded key changes stop before credential lookup',()=>{
 const read=owner(),admin=owner('user:1','self',true);try{
  const r=plan(read),a=plan(admin);expect(r.providerContinuation.canAdminister).toBe(false);expect(a.providerContinuation).toMatchObject({canAdminister:true,credentialInput:'native_api_key'});
  const input={id:r.id,revision:1,provider:'exa',secret:'fixture-key-only'};expect(()=>read.connection(input)).toThrow('administrator');expect(input.secret).toBe('');
  const legacy={provider:'exa',secret:'fixture-key-only'};expect(()=>read.connection(legacy)).toThrow('administrator');expect(legacy.secret).toBe('');expect(()=>read.connection({provider:'exa'},true)).toThrow('administrator');
  const stale={id:a.id,revision:2,binding:{...admin.binding},provider:'exa',secret:'fixture-key-only'};expect(()=>admin.connection(stale)).toThrow('changed');expect(stale.secret).toBe('');expect(admin.get(a.id).revision).toBe(1);
  const switched={provider:'exa',secret:'fixture-key-only',binding:{actor:'user:2',subject:'user:2',scope:'self'}};expect(()=>admin.connection(switched)).toThrow('account or destination');expect(switched.secret).toBe('');
 }finally{read.close();admin.close();}
});
test('native key owner refuses a password or MFA declaration before attempting credential mutation',()=>{
 const dir=mkdtempSync(join(tmpdir(),'provider-declaration-')),binary=join(dir,'credential-status.mjs'),log=join(dir,'calls');
 // Only the credential subprocess is a double. Actual WebWork guard, transaction
 // and nativeCredential invocation run; no real credential database is opened.
 writeFileSync(binary,'#!'+process.execPath+'\nimport {appendFileSync} from "node:fs";appendFileSync('+JSON.stringify(log)+',process.argv[2]+"\\n");process.stdout.write(JSON.stringify({providers:[{provider:"exa",fields:[{field:"password",required:true,set:false}]}]}));\n',{mode:0o700});
 const native=new WebWork(':memory:',{subject:'user:1',scope:'self',actor:'user:1',config:{canAdminister:true,credentialBinary:binary,credentialDB:join(dir,'fixture.db'),credentialProviders:join(dir,'fixture.providers')}});try{const input={provider:'exa',secret:'fixture-key-only',binding:{...native.binding}};expect(()=>native.connection(input)).toThrow('API-key declaration');expect(readFileSync(log,'utf8')).toBe('status\n');expect(input.secret).toBe('');}finally{native.close();rmSync(dir,{recursive:true,force:true});}
});
test('a same-organization account switch refuses key changes before any credential lookup',()=>{
 const native=new WebWork(':memory:',{subject:'institution:studio',scope:'institution',actor:'user:2',config:{canAdminister:true}}),job=plan(native),binding={actor:'user:1',subject:'institution:studio',scope:'institution'};
 try{for(const extra of [{},{id:job.id,revision:job.revision}]){const value={provider:'exa',secret:'fixture-key-only',binding,...extra};expect(()=>native.connection(value)).toThrow('account or destination');expect(value.secret).toBe('');expect(()=>native.connection({provider:'exa',binding,...extra},true)).toThrow('account or destination');}expect(native.get(job.id).revision).toBe(1);}finally{native.close();}
});

function browser(native,job){
 const values={},tabs=new Map([[1,{id:1,windowId:10,incognito:false,url:origin+'/sources/web',active:true}]]),documents=new Map([[1,'document-a']]),calls=[];let id=1,failStorage=false,nativeGate=null,pinGate=null;
 const activate=key=>{for(const tab of tabs.values())tab.active=tab.id===key;};
 const chrome={runtime:{id:'fixture-extension'},sidePanel:{setOptions:async options=>{calls.push(['panel-options',options]);},open:async options=>{calls.push(['panel',options]);}},windows:{get:async id=>({id,type:'normal',incognito:false})},tabs:{
  get:async key=>{if(!tabs.has(key))throw Error('closed');return {...tabs.get(key)};},
  query:async options=>[...tabs.values()].filter(tab=>tab.windowId===options.windowId&&(!options.active||tab.active)).map(tab=>({...tab})),
  create:async options=>{const tab={id:++id,incognito:false,windowId:options.windowId,url:options.url,active:options.active===true};tabs.set(tab.id,tab);if(tab.active)activate(tab.id);calls.push(['create',options]);return {...tab};},
  update:async(key,options)=>{if(!tabs.has(key))throw Error('closed');Object.assign(tabs.get(key),options);if(options.active)activate(key);calls.push(['update',key,options]);return {...tabs.get(key)};},
  remove:async key=>{tabs.delete(key);calls.push(['remove',key]);}
 },storage:{session:{get:async key=>key===null?structuredClone(values):{[key]:structuredClone(values[key])},set:async data=>{if(failStorage)throw Error('storage unavailable');Object.assign(values,structuredClone(data));},remove:async keys=>{for(const key of Array.isArray(keys)?keys:[keys])delete values[key];}}},scripting:{executeScript:async options=>{
  const documentId=documents.get(options.target.tabId);if(options.target.documentIds[0]!==documentId)throw Error('document changed');
  if(options.func.name==='readProviderInDocument'){const {review,challenge}=options.args[0];return [{frameId:0,documentId,result:{ok:true,packet:Array.from(encode([1,true,native.provider({id:review.jobId,revision:review.revision,challenge,origin})]))}}];}
  if(pinGate)await pinGate;
  return [{frameId:0,documentId,result:true}];
 }}};
 const bridge=createProviderBridge({chrome,nonce:()=>challenge,now:()=>clock,ask:async(operation,message)=>{calls.push(['native',operation]);if(nativeGate)await nativeGate;return {ok:true,result:admitProviderPacket(message,{now:clock})};}});
 const sender=(tabId=1)=>({id:chrome.runtime.id,url:tabs.get(tabId).url,origin,frameId:0,documentId:documents.get(tabId),documentLifecycle:'active',tab:{...tabs.get(tabId)}}),request=(target=job)=>({channel:'aurekai-provider-v1',operation:'continue',gesture:true,review:review(native,target)});
 return {bridge,tabs,calls,values,sender,request,activate,setNative(value){native=value;},setDocument(value,tabId=1){documents.set(tabId,value);},setStorageFailure(value){failStorage=value;},setNativeGate(value){nativeGate=value;},setPinGate(value){pinGate=value;}};
}
test('first provider opens only after retaining blank-tab identity; later clicks preserve OAuth/MFA URL',async()=>{
 const native=owner(),job=plan(native),b=browser(native,job);try{
  await b.bridge.handle({channel:'aurekai-provider-v1',operation:'capabilities'},b.sender());
  const first=await b.bridge.handle(b.request(),b.sender());expect(first.state).toBe('provider_open');expect(first.companion).toBe('open');expect(b.calls.filter(c=>c[0]==='create')).toHaveLength(1);expect(b.calls.find(c=>c[0]==='create')[1].url).toBe('about:blank');
  b.tabs.get(2).url='https://provider.example/consent?keep=this';const again=await b.bridge.handle(b.request(),b.sender());expect(again.state).toBe('provider_focused');expect(b.tabs.get(2).url).toBe('https://provider.example/consent?keep=this');expect(b.calls.filter(c=>c[0]==='create')).toHaveLength(1);expect(b.calls.filter(c=>c[0]==='update'&&Object.hasOwn(c[2],'url'))).toHaveLength(1);expect(native.get(job.id).state).toBe('planned');
 }finally{native.close();}
});
test('concurrent provider clicks serialize onto one tab and do not claim work',async()=>{
 const native=owner(),job=plan(native),b=browser(native,job);try{await Promise.all([b.bridge.handle(b.request(),b.sender()),b.bridge.handle(b.request(),b.sender())]);expect(b.calls.filter(c=>c[0]==='create')).toHaveLength(1);expect(native.get(job.id).revision).toBe(1);}finally{native.close();}
});
test('cross-origin, child frame, unknown extension, absent gesture and URL injection refuse before native read',async()=>{
 const native=owner(),job=plan(native),b=browser(native,job);try{
  for(const sender of [{...b.sender(),origin:'https://attacker.example'},{...b.sender(),frameId:1},{...b.sender(),id:'another-extension'},{...b.sender(),documentId:''},{...b.sender(),tab:{...b.sender().tab,incognito:true}}])expect(()=>b.bridge.handle(b.request(),sender)).toThrow();
  expect(()=>b.bridge.handle({...b.request(),gesture:false},b.sender())).toThrow('gesture');expect(()=>b.bridge.handle({...b.request(),url:'https://attacker.example'},b.sender())).toThrow('supported');expect(b.calls).toEqual([]);
 }finally{native.close();}
});
test('scope clear while admission is pending prevents tab effects without canceling native work',async()=>{
 const native=owner(),job=plan(native),b=browser(native,job);let release;try{b.setNativeGate(new Promise(r=>release=r));const operation=b.bridge.handle(b.request(),b.sender());await new Promise(r=>setTimeout(r,0));await b.bridge.handle({channel:'aurekai-provider-v1',operation:'clear'},b.sender());release();await expect(operation).rejects.toThrow('cleared');expect(b.calls.filter(c=>['create','update','remove'].includes(c[0]))).toEqual([]);expect(native.get(job.id).state).toBe('planned');}finally{release?.();native.close();}
});
test('document replacement after native read refuses, and storage failure closes only its new blank tab',async()=>{
 const native=owner(),job=plan(native),b=browser(native,job);let release;try{b.setNativeGate(new Promise(r=>release=r));const operation=b.bridge.handle(b.request(),b.sender());await new Promise(r=>setTimeout(r,0));b.setDocument('document-b');release();await expect(operation).rejects.toThrow('document');expect(b.calls.filter(c=>c[0]==='create')).toEqual([]);
  const fresh=browser(native,job);fresh.setStorageFailure(true);await expect(fresh.bridge.handle(fresh.request(),fresh.sender())).rejects.toThrow('storage');expect([...fresh.tabs.keys()]).toEqual([1]);expect(fresh.calls.filter(c=>c[0]==='update')).toEqual([]);
 }finally{release?.();native.close();}
});
test('cleared companion presentation preserves exact tab mapping; closed provider can explicitly reopen',async()=>{
 const native=owner(),job=plan(native),b=browser(native,job);try{await b.bridge.handle(b.request(),b.sender());b.tabs.get(2).url='https://provider.example/mfa';await b.bridge.handle({channel:'aurekai-provider-v1',operation:'clear'},b.sender());expect(b.tabs.has(2)).toBe(true);await expect(b.bridge.returnToAurekai(10)).rejects.toThrow('Reopen this provider setup');await b.bridge.handle(b.request(),b.sender());expect(b.tabs.get(2).url).toBe('https://provider.example/mfa');b.tabs.delete(2);await b.bridge.handle(b.request(),b.sender());expect(b.calls.filter(c=>c[0]==='create')).toHaveLength(2);expect(native.get(job.id).revision).toBe(1);}finally{native.close();}
});
test('two actors reading the same organization work never reuse one another’s retained provider tab',async()=>{
 const dir=mkdtempSync(join(tmpdir(),'provider-actor-')),path=join(dir,'work.db'),a=owner('institution:studio','institution',false,path),b=new WebWork(path,{subject:'institution:studio',scope:'institution',actor:'user:2',clock:()=>clock}),job=plan(a),browserState=browser(a,job);
 try{await browserState.bridge.handle(browserState.request(),browserState.sender());browserState.tabs.get(2).url='https://provider.example/account-a-mfa';await browserState.bridge.handle({channel:'aurekai-provider-v1',operation:'clear'},browserState.sender());browserState.setNative(b);await browserState.bridge.handle(browserState.request(),browserState.sender());expect(browserState.calls.filter(c=>c[0]==='create')).toHaveLength(2);expect(browserState.tabs.get(2).url).toBe('https://provider.example/account-a-mfa');expect(browserState.tabs.get(3).url).toBe('https://dashboard.exa.ai/api-keys');expect(Object.keys(browserState.values).filter(k=>k.startsWith('provider-tab-'))).toHaveLength(2);expect(b.get(job.id).revision).toBe(1);}finally{a.close();b.close();rmSync(dir,{recursive:true,force:true});}
});
test('a corrupt retained return path is refused without changing or redirecting any tab',async()=>{
 const native=owner(),job=plan(native),b=browser(native,job);try{await b.bridge.handle(b.request(),b.sender());const key=Object.keys(b.values).find(k=>k.startsWith('provider-tab-')),saved=decode(Uint8Array.from(b.values[key]));saved[7]='//attacker.example/';b.values[key]=Array.from(encode(saved));const count=b.calls.length;await expect(b.bridge.handle(b.request(),b.sender())).rejects.toThrow('reference needs review');expect(b.calls.slice(count).filter(c=>['create','update','remove'].includes(c[0]))).toEqual([]);}finally{native.close();}
});
test('one Aurekai document cannot clear another tab’s companion, and return pins the retained source document',async()=>{
 const native=owner(),job=plan(native),b=browser(native,job);try{await b.bridge.handle(b.request(),b.sender());const old=b.values['provider-current-10-2'];b.tabs.set(9,{id:9,windowId:10,incognito:false,url:origin+'/another'});await b.bridge.handle({channel:'aurekai-provider-v1',operation:'clear'},{...b.sender(),tab:b.tabs.get(9),documentId:'other-source'});expect(b.values['provider-current-10-2']).toEqual(old);
  await b.bridge.returnToAurekai(10);expect(b.calls.at(-1)).toEqual(['update',1,{active:true}]);b.setDocument('replaced-document');b.activate(2);await b.bridge.returnToAurekai(10);expect(b.calls.at(-1)[0]).toBe('create');expect(b.calls.at(-1)[1].url).toBe(origin+'/sources/web?'+new URLSearchParams({context:native.context,job:job.id}));expect(b.tabs.has(2)).toBe(true);expect(native.get(job.id).revision).toBe(1);
 }finally{native.close();}
});
test('return follows the active provider attachment when another work was opened more recently',async()=>{
 const native=owner(),a=plan(native),b=browser(native,a),other=native.plan({requestId:'second-return-work',title:'Another retained setup',mode:'account',spec:{provider:'exa',operation:'search',query:'Other',objective:'Return attachment witness',limit:3}});
 try{
  await b.bridge.handle(b.request(),b.sender());b.tabs.get(2).url='https://provider.example/first-mfa';
  b.tabs.set(9,{id:9,windowId:10,incognito:false,url:origin+'/sources/web?job='+other.id,active:false});b.setDocument('document-b',9);
  await b.bridge.handle(b.request(other),b.sender(9));b.tabs.get(3).url='https://provider.example/second-mfa';
  b.activate(2);await b.bridge.returnToAurekai(10);expect(b.calls.at(-1)).toEqual(['update',1,{active:true}]);
  b.activate(3);await b.bridge.returnToAurekai(10);expect(b.calls.at(-1)).toEqual(['update',9,{active:true}]);
  await b.bridge.handle({channel:'aurekai-provider-v1',operation:'clear'},b.sender());b.activate(2);await expect(b.bridge.returnToAurekai(10)).rejects.toThrow('Reopen this provider setup');b.activate(3);await b.bridge.returnToAurekai(10);expect(b.calls.at(-1)).toEqual(['update',9,{active:true}]);
  expect(b.tabs.get(2).url).toBe('https://provider.example/first-mfa');expect(b.tabs.get(3).url).toBe('https://provider.example/second-mfa');expect(native.get(a.id).revision).toBe(1);expect(native.get(other.id).revision).toBe(1);
 }finally{native.close();}
});
test('a replaced provider tab keeps its exact return presentation and old window records remain readable',async()=>{
 const native=owner(),job=plan(native),b=browser(native,job);try{
  await b.bridge.handle(b.request(),b.sender());b.tabs.set(12,{...b.tabs.get(2),id:12,url:'https://provider.example/mfa'});b.tabs.delete(2);b.activate(12);await b.bridge.replaced(12,2);expect(b.values['provider-current-10-2']).toBeUndefined();expect(b.values['provider-current-10-12']).toBeDefined();await b.bridge.returnToAurekai(10);expect(b.calls.at(-1)).toEqual(['update',1,{active:true}]);
  b.values['provider-current-10']=b.values['provider-current-10-12'];delete b.values['provider-current-10-12'];b.activate(12);await b.bridge.returnToAurekai(10);expect(b.calls.at(-1)).toEqual(['update',1,{active:true}]);expect(b.tabs.get(12).url).toBe('https://provider.example/mfa');expect(native.get(job.id).revision).toBe(1);
 }finally{native.close();}
});
test('return refuses an unrelated active tab and ambiguous attachments instead of using last-opened work',async()=>{
 const native=owner(),job=plan(native),b=browser(native,job);try{
  await b.bridge.handle(b.request(),b.sender());b.tabs.set(9,{id:9,windowId:10,incognito:false,url:'https://unrelated.example/',active:false});b.activate(9);let count=b.calls.length;
  await expect(b.bridge.returnToAurekai(10)).rejects.toThrow('Select the provider tab');expect(b.calls.slice(count)).toEqual([]);
  b.activate(2);const key=Object.keys(b.values).find(k=>k.startsWith('provider-tab-')),saved=decode(Uint8Array.from(b.values[key]));saved[2]='b'.repeat(64);saved[7]='/sources/web?'+new URLSearchParams({context:'self',job:saved[2]});b.values['provider-tab-'+saved[1]+'-'+saved[2]+'-'+saved[10]]=Array.from(encode(saved));count=b.calls.length;
  await expect(b.bridge.returnToAurekai(10)).rejects.toThrow('more than one');expect(b.calls.slice(count)).toEqual([]);expect(native.get(job.id).revision).toBe(1);
 }finally{native.close();}
});
test('a presentation clear during return refuses before any source tab is activated',async()=>{
 const native=owner(),job=plan(native),b=browser(native,job);let release;try{
  await b.bridge.handle(b.request(),b.sender());b.setPinGate(new Promise(resolve=>release=resolve));const count=b.calls.length,pending=b.bridge.returnToAurekai(10);await new Promise(resolve=>setTimeout(resolve,0));await b.bridge.handle({channel:'aurekai-provider-v1',operation:'clear'},b.sender());release();await expect(pending).rejects.toThrow('presentation changed');expect(b.calls.slice(count)).toEqual([]);expect(b.tabs.get(2).active).toBe(true);expect(native.get(job.id).revision).toBe(1);
 }finally{release?.();native.close();}
});
test('extension projections are byte-exact copies of the canonical contract and MessagePack codec',()=>{
 for(const name of ['workspace-msgpack.mjs','provider-contract.mjs'])expect(readFileSync(new URL('../extension/'+name,import.meta.url),'utf8')).toBe('// Generated by scripts/project-provider-work-browser.mjs; edit the canonical browser module.\n'+readFileSync(new URL('../'+name,import.meta.url),'utf8'));
});
