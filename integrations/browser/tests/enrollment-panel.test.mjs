// Actual panel module and handlers, with browser/native transport doubles.
// This is not a Chrome installation or an external provider enrollment witness.
import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
class Element {
  constructor(tag){this.tagName=tag;this.children=[];this.dataset={};this.attributes={};this.disabled=false;this._text='';this.value='';this.hidden=false;this.parentElement=null;}
  set textContent(v){this.replaceChildren();this._text=String(v);}
  get textContent(){return this._text+this.children.map(x=>typeof x==='string'?x:x.textContent).join('');}
  get isConnected(){return this.root===true||this.parentElement?.isConnected===true;}
  append(...nodes){for(const node of nodes){if(typeof node!=='string'){node.remove();node.parentElement=this;}this.children.push(node);}}
  insertBefore(node,before){if(before===null){this.append(node);return;}assert.ok(this.children.includes(before));node.remove();node.parentElement=this;this.children.splice(this.children.indexOf(before),0,node);}
  prepend(...nodes){for(const node of [...nodes].reverse())this.insertBefore(node,this.children[0]??null);}
  addEventListener(name,handler){(this.listeners??={})[name]=handler;}
  replaceChildren(...nodes){for(const child of this.children)if(typeof child!=='string')child.parentElement=null;this._text='';this.children=[];this.append(...nodes);}
  remove(){if(this.parentElement){this.parentElement.children=this.parentElement.children.filter(node=>node!==this);this.parentElement=null;}}
  setAttribute(name,value){this.attributes[name]=String(value);}
  reportValidity(){return all(this).filter(node=>node.tagName==='input').every(node=>!node.required||!!node.value);}
}
const all=root=>[root,...root.children.flatMap(x=>typeof x==='string'?[]:all(x))];
const event=()=>{const listeners=[];return{addListener:f=>listeners.push(f),emit:(...a)=>listeners.forEach(f=>f(...a))};};
async function fixture(incognito=false,restored={}){
  const box=new Element('div'),say=new Element('p'),head=new Element('head'),calls=[],tabs=restored.tabs||new Map(),session=restored.session||{};let nextId=tabs.size+1,lock=Promise.resolve();
  box.root=say.root=head.root=true;const pageEvents={};let intercept=null;
  const identity=incognito?'proton':'gmail';
  let snapshot=restored.snapshot||{how:'resumed',read_only:false,status:'running',resumable:true,item:{work_id:'001-'+identity,identity,account:identity+'@example.invalid',platform:'Fixture provider',signup_url:'https://provider.example.invalid/login'},revision:0,stages:{}};
  const window={id:1,incognito,type:'normal'};
  const browser={extension:{inIncognitoContext:incognito},windows:{getCurrent:async()=>({...window})},storage:{session:{get:async key=>({[key]:session[key]}),set:async v=>Object.assign(session,v),remove:async key=>{delete session[key];}}},tabs:{
    onReplaced:event(),get:async id=>{if(!tabs.has(id))throw Error('closed');return{...tabs.get(id)};},
    create:async p=>{calls.push(['create',p]);const tab={id:nextId++,incognito,windowId:p.windowId,url:p.url};tabs.set(tab.id,tab);return{...tab};},
    update:async(id,p)=>{calls.push(['update',id,p]);Object.assign(tabs.get(id),p);return{...tabs.get(id)};},
    remove:async id=>{calls.push(['remove',id]);tabs.delete(id);}
  }};
  const document={head,getElementById:id=>id==='enroll'?box:say,createElement:tag=>new Element(tag),querySelectorAll:selector=>{assert.equal(selector,'link[rel="stylesheet"]');return all(head).filter(node=>node.tagName==='link'&&node.rel==='stylesheet');}};
  const context=vm.createContext({URL,TextEncoder,TextDecoder,AbortController,console,document,chrome:browser,navigator:{locks:{request:(_key,fn)=>{const p=lock.then(fn);lock=p.catch(()=>{});return p;}}},window:{addEventListener:(name,fn)=>{pageEvents[name]=fn;},aurekaiAsk:async(op,args)=>{
    calls.push([op,args]);
    const intercepted=await intercept?.(op,args);if(intercepted!==undefined)return intercepted;
    if(op==='enroll_resume')return{ok:true,result:structuredClone(snapshot)};
    if(op==='enroll_list')return{ok:true,result:{seeded:true,identity,items:[{...structuredClone(snapshot),read_only:true}],more:false,cursor:null}};
    if(op==='enroll_inspect')return args.work_id===snapshot.item.work_id?{ok:true,result:{...structuredClone(snapshot),read_only:true,how:'inspected'}}:{ok:false,reason:'The selected setup is unavailable.'};
    if(args.expected?.work_id!==snapshot.item.work_id||args.expected?.revision!==snapshot.revision)return{ok:false,reason:'stale'};
    if(op==='enroll_continue'){snapshot={...snapshot,read_only:false,status:'running',how:'continued'};return{ok:true,result:structuredClone(snapshot)};}
    if(op==='enroll_record'){snapshot.stages[args.stage]={at:'now',evidence_kind:'human_assertion'};snapshot.revision++;return{ok:true,result:{revision:snapshot.revision,state:{stages:structuredClone(snapshot.stages)}}};}
    if(op==='enroll_seal'){snapshot.stages.credential_sealed={at:'now',evidence_kind:'native_seal',owner:'BonfyreCredentials',provider:args.provider,field:args.field};snapshot.revision++;return{ok:true,result:{revision:snapshot.revision,state:{stages:structuredClone(snapshot.stages)}}};}
    if(op==='enroll_wait'){snapshot={...snapshot,read_only:true,status:'retry_wait',resumable:false,next_attempt_at_ms:Date.now()+600000,revision:snapshot.revision+1,notes:[{at:'now',why:'Waiting for email',note:''}]};return{ok:true,result:{released:'001-'+identity}};}
    throw Error('unexpected operation '+op);
  }}});
  const source=relative=>{const url=new URL(relative,import.meta.url);return new vm.SourceTextModule(readFileSync(url,'utf8'),{context,identifier:url.href,initializeImportMeta:meta=>{meta.url=url.href;}});};
  const closedLink=modules=>name=>{if(!Object.hasOwn(modules,name))throw Error('Unexpected import: '+name);return modules[name];};
  const guard=source('../extension/enrollment-integrity.mjs'),codec=source('../workspace-msgpack.mjs'),contract=source('../provider-contract.mjs');
  for(const dependency of [guard,codec,contract])await dependency.link(closedLink({}));
  const card=source('../extension/foreign-sign-in.mjs');await card.link(closedLink({'../workspace-msgpack.mjs':codec,'../provider-contract.mjs':contract}));
  const panel=source('../extension/panel-enrollment.mjs');await panel.link(closedLink({'./enrollment-integrity.mjs':guard,'./foreign-sign-in.mjs':card}));await panel.evaluate();
  const button=text=>all(box).find(n=>n.tagName==='button'&&n.textContent===text);
  const click=async text=>{const b=button(text);assert.ok(b,text);assert.equal(b.disabled,false,text);await b.onclick();};
  return{box,say,calls,tabs,session,window,browser,button,click,events:pageEvents,intercept:fn=>{intercept=fn;},nodes:()=>all(box),snapshot:()=>structuredClone(snapshot),changeSnapshot:fn=>{snapshot=fn(snapshot);},ready:()=>click('Continue native enrollment'),reopen:()=>fixture(incognito,{tabs,session,snapshot:structuredClone(snapshot)})};
}
test('panel opens without native claim or provider tab; no identity toggle',async()=>{
 const f=await fixture();assert.equal(f.calls.length,0);assert.ok(f.button('Continue native enrollment'));assert.equal(f.button('Proton'),undefined);
});
test('mutations carry reviewed work/revision and update the following review',async()=>{
 const f=await fixture();await f.ready();await f.click('Checked for existing access');await f.click('Confirmed the intended account');
 const writes=f.calls.filter(x=>x[0]==='enroll_record');assert.equal(writes.length,2);assert.equal(writes[0][1].expected.revision,0);assert.equal(writes[1][1].expected.revision,1);assert.equal(writes[1][1].expected.work_id,'001-gmail');assert.equal(f.button('Complete verified capability').disabled,true);
});
test('reopen keeps one work tab and preserves an in-progress OAuth redirect',async()=>{
 const f=await fixture();await f.ready();await f.click('Take over to sign in');const tab=[...f.tabs.values()][0];tab.url='https://identity.example.invalid/consent';await f.click('Take over to sign in');assert.equal(f.tabs.size,1);assert.equal(tab.url,'https://identity.example.invalid/consent');assert.equal(f.calls.filter(x=>x[0]==='create').length,1);
});
test('closed provider tab is recreated without losing native item',async()=>{
 const f=await fixture();await f.ready();await f.click('Take over to sign in');f.tabs.clear();await f.click('Take over to sign in');assert.equal(f.tabs.size,1);assert.equal(f.session['enroll-tab-gmail'].work_id,'001-gmail');
});
test('wrong browser context refuses before native or browser changes',async()=>{
 const f=await fixture();f.window.incognito=true;await f.ready();assert.equal(f.calls.length,0);assert.match(f.say.textContent,/regular window/);
});
test('a stale panel stops mutations until native reload',async()=>{
 const f=await fixture();await f.ready();f.changeSnapshot(s=>({...s,revision:10}));await f.click('Checked for existing access');assert.equal(f.button('Confirmed the intended account').disabled,true);assert.equal(f.calls.filter(x=>x[0]==='enroll_record').length,1);await f.click('Reload native item');assert.equal(f.button('Confirmed the intended account'),undefined);await f.click('Use this account');assert.equal(f.button('Confirmed the intended account').disabled,false);
});
test('another item tab is neither overwritten nor closed',async()=>{
 const f=await fixture();await f.ready();f.session['enroll-tab-gmail']={id:99,work_id:'002-gmail'};f.tabs.set(99,{id:99,windowId:1,incognito:false,url:'https://different.example.invalid/'});await f.click('Take over to sign in');await f.click('Unload provider page');assert.equal(f.tabs.size,1);assert.equal(f.calls.filter(x=>['create','update','remove'].includes(x[0])).length,0);
});
test('Proton waiting preserves its exact setup and incognito provider tab without advancing work',async()=>{
 const f=await fixture(true);await f.ready();await f.click('Take over to sign in');assert.equal(f.calls.find(x=>x[0]==='create')[1].windowId,1);assert.equal([...f.tabs.values()][0].incognito,true);await f.click('Waiting for email');assert.equal(f.tabs.size,1);assert.equal(f.calls.filter(x=>x[0]==='enroll_done').length,0);assert.equal(f.calls.filter(x=>x[0]==='enroll_resume').length,1);assert.match(f.box.textContent,/001-proton/);assert.equal(f.button('Use this account').disabled,true);assert.equal(f.button('Waiting for email'),undefined);
});
const publicField={field:'api_key',label:'API key',sensitive:true,required:true,help:'Copy from this provider account.'};
const declared=(snapshot,fields=[publicField])=>({...snapshot,item:{...snapshot.item,credential_provider:'fixture'},credential_fields_available:true,credential_fields:fields});
test('credential entry needs exact native field metadata and seals once with the reviewed identity',async()=>{
 const f=await fixture();f.changeSnapshot(s=>({...s,item:{...s.item,credential_provider:'fixture'}}));await f.ready();assert.equal(f.button('Seal this credential'),undefined);
 f.changeSnapshot(s=>declared(s));await f.click('Reload native item');await f.click('Use this account');const input=f.nodes().find(n=>n.tagName==='input'),form=f.nodes().find(n=>n.tagName==='form');input.value='fixture-key-canary';const submitted=form.onsubmit({preventDefault(){}});assert.equal(input.value,'');await submitted;
 const seals=f.calls.filter(row=>row[0]==='enroll_seal');assert.equal(seals.length,1);assert.equal(seals[0][1].field,'api_key');assert.equal(seals[0][1].provider,'fixture');assert.equal(seals[0][1].identity,'gmail');assert.equal(seals[0][1].expected.work_id,'001-gmail');assert.equal(seals[0][1].expected.revision,0);assert.equal(seals[0][1].value,'fixture-key-canary');assert.equal(f.box.textContent.includes('fixture-key-canary'),false);assert.equal(f.snapshot().stages.credential_verified,undefined);assert.equal(f.button('Complete verified capability').disabled,true);assert.match(f.box.textContent,/Provider verification remains a separate step/);
});
test('changing a declared field or dismissing clears secret entry without touching native credentials',async()=>{
 const f=await fixture();f.changeSnapshot(s=>declared(s,[publicField,{...publicField,field:'api_secret',label:'API secret'}]));await f.ready();const first=f.nodes().find(n=>n.tagName==='input');first.value='fixture-only-secret';const choice=f.nodes().find(n=>n.tagName==='select');choice.value='api_secret';choice.onchange();assert.equal(first.value,'');const input=f.nodes().find(n=>n.tagName==='input');assert.equal(input.attributes['aria-label'],'API secret');input.value='second-fixture-secret';await f.click('Not now');assert.equal(input.value,'');assert.equal(f.calls.filter(row=>row[0]==='enroll_seal').length,0);assert.equal(f.button('Take over to sign in').disabled,true);
});
test('an uncertain seal cannot echo a secret or remain available for replay',async()=>{
 const f=await fixture();f.changeSnapshot(s=>declared(s));await f.ready();f.intercept(op=>op==='enroll_seal'?{ok:false,reason:'fixture-secret-canary'}:undefined);const input=f.nodes().find(n=>n.tagName==='input');input.value='fixture-secret-canary';await f.nodes().find(n=>n.tagName==='form').onsubmit({preventDefault(){}});assert.equal(input.value,'');assert.equal(f.say.textContent.includes('fixture-secret-canary'),false);assert.equal(f.button('Seal this credential').disabled,true);assert.match(f.say.textContent,/Reload the native item/);
});
test('closing a panel during the native recheck prevents the pending provider open',async()=>{
 const f=await fixture();await f.ready();let release;f.intercept(op=>op==='enroll_inspect'?new Promise(resolve=>{release=resolve;}):undefined);const opening=f.click('Take over to sign in');await new Promise(resolve=>setImmediate(resolve));assert.equal(typeof release,'function');f.events.pagehide();release({ok:true,result:f.snapshot()});await opening;assert.equal(f.calls.filter(row=>['create','update'].includes(row[0])).length,0);assert.equal(f.box.textContent.includes('Connect Fixture provider'),false);
});
test('reopening a closed panel reads the selected checkpoint and preserves its OAuth tab',async()=>{
 const f=await fixture();await f.ready();await f.click('Take over to sign in');const tab=[...f.tabs.values()][0];tab.url='https://identity.example.invalid/consent?step=2';f.events.pagehide();
 const reopened=await f.reopen();assert.equal(reopened.calls.length,0);await reopened.ready();assert.equal(reopened.calls.filter(row=>row[0]==='enroll_resume').length,0);assert.equal(reopened.calls.find(row=>row[0]==='enroll_inspect')[1].work_id,'001-gmail');assert.equal(reopened.button('Checked for existing access'),undefined);
 await reopened.click('Take over to sign in');assert.equal(reopened.tabs.size,1);assert.equal(tab.url,'https://identity.example.invalid/consent?step=2');assert.equal(reopened.calls.filter(row=>row[0]==='create').length,0);await reopened.click('Use this account');const call=reopened.calls.find(row=>row[0]==='enroll_continue');assert.deepEqual(JSON.parse(JSON.stringify(call[1].expected)),{work_id:'001-gmail',revision:0});assert.equal(call[1].confirmed,true);
});
test('finding and reviewing saved work makes no native claim, provider request or credential form',async()=>{
 const f=await fixture();f.changeSnapshot(s=>declared(s));await f.click('Find a setup');await f.click('Review Fixture provider · 001-gmail');assert.equal(f.calls.filter(row=>['enroll_resume','enroll_continue','create','enroll_seal'].includes(row[0])).length,0);assert.equal(f.button('Seal this credential'),undefined);assert.equal(f.button('Use this account').disabled,false);assert.equal(f.session['enroll-view-gmail'].work_id,'001-gmail');
});
test('a decision remains inspectable without a resume control that can clear it',async()=>{
 const f=await fixture();f.changeSnapshot(s=>({...s,status:'dead_letter',resumable:false,needs_decision:true,reason:'Terms review'}));await f.click('Find a setup');await f.click('Review Fixture provider · 001-gmail');assert.equal(f.button('Use this account').disabled,true);await f.click('Take over to sign in');assert.equal(f.tabs.size,1);assert.equal(f.calls.filter(row=>['enroll_resume','enroll_continue','enroll_done'].includes(row[0])).length,0);assert.match(f.box.textContent,/waiting for a decision/);
});
test('queue pagination and filtering preserve the search and exact continuation cursor',async()=>{
 const f=await fixture();f.intercept((op,args)=>op==='enroll_list'?{ok:true,result:{seeded:true,items:[],more:!args.cursor,cursor:args.cursor?null:'020-gmail'}}:undefined);await f.click('Find a setup');const query=f.nodes().find(n=>n.attributes['aria-label']==='Find a provider'),choice=f.nodes().find(n=>n.attributes['aria-label']==='Setup status');query.value='100%_ fixture';choice.value='waiting';await f.click('Find setups');await f.click('More setups');await f.click('Previous setups');const calls=f.calls.filter(row=>row[0]==='enroll_list').map(row=>row[1]);assert.equal(calls[1].search,'100%_ fixture');assert.equal(calls[1].filter,'waiting');assert.equal(calls[2].cursor,'020-gmail');assert.equal(calls[3].cursor,'');assert.equal(f.button('Previous setups').disabled,true);
});
test('missing selected work remains unavailable instead of silently claiming another item',async()=>{
 const f=await fixture();f.session['enroll-view-gmail']={work_id:'200-gmail'};await f.ready();assert.equal(f.calls.filter(row=>row[0]==='enroll_resume').length,0);assert.equal(f.tabs.size,0);assert.match(f.say.textContent,/unavailable/);assert.equal(f.button('Use this account'),undefined);
});
test('provider coordinates are rechecked and another browser window retains its tab',async()=>{
 const f=await fixture();await f.ready();f.changeSnapshot(s=>({...s,item:{...s.item,signup_url:'https://other.example.invalid/'}}));await f.click('Take over to sign in');assert.equal(f.tabs.size,0);assert.match(f.say.textContent,/provider or account changed/);
 const g=await fixture();await g.ready();g.session['enroll-tab-gmail']={id:99,work_id:'001-gmail',window_id:2};g.tabs.set(99,{id:99,windowId:2,incognito:false,url:'https://provider.example.invalid/consent'});await g.click('Unload provider page');assert.equal(g.tabs.size,1);assert.equal(g.calls.filter(row=>row[0]==='remove').length,0);
});
test('expanded setup details stay open when a reviewed checkpoint advances',async()=>{
 const f=await fixture();await f.ready();await f.click('Expand setup details');await f.click('Checked for existing access');assert.ok(f.button('Collapse setup details'));assert.equal(f.nodes().find(n=>n.className==='fsi-details').hidden,false);
});
test('uncertain continuation reloads only its exact item and cannot repeat the claim automatically',async()=>{
 const f=await fixture();await f.click('Find a setup');await f.click('Review Fixture provider · 001-gmail');f.intercept(op=>op==='enroll_continue'?Promise.reject(Error('transport lost')):undefined);await f.click('Use this account');assert.equal(f.button('Use this account').disabled,true);await f.click('Reload native item');assert.equal(f.calls.filter(row=>row[0]==='enroll_continue').length,1);assert.equal(f.calls.filter(row=>row[0]==='enroll_resume').length,0);assert.equal(f.calls.filter(row=>row[0]==='enroll_inspect').at(-1)[1].work_id,'001-gmail');
});
