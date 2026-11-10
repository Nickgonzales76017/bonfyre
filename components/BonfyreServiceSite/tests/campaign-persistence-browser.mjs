// Local integration witness: actual research package, shared IndexedDB owner,
// and isolated Chrome. No source searches or native account effects.
import assert from 'node:assert/strict';
import { synthesisFiles } from '../synthesis-material.mjs';
import { render } from '../sections/synthesis.mjs';
import { withChrome } from '../../../integrations/browser/capture/chrome-session.mjs';
import { fileURLToPath } from 'node:url';

const material = synthesisFiles(fileURLToPath(new URL('../../../packs/aurekai-work-samples/site/synthesis-four-day-week-20261019.json', import.meta.url)));
const esc = text => String(text).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const html = '<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Research persistence check</title>'
  + render({ id:'research', title:'Research desk', data:'/data/research/', ...material }, {}, { esc });
const server = Bun.serve({ hostname:'127.0.0.1', port:0, fetch(request) {
  const path = new URL(request.url).pathname;
  if (path === '/') return new Response(html, {headers:{'content-type':'text/html'}});
  const name = path.replace(/^\/data\/research\//, ''), body = material.files[name];
  return body ? new Response(body, {headers:{'content-type':name.endsWith('.json')?'application/json':'text/javascript'}}) : new Response('Missing', {status:404});
}});
const seen = [];
try {
  await withChrome(async ({open}) => {
    const tab = await open();
    const run = async (tab, expression) => {
      const result = await tab.page('Runtime.evaluate', {expression,awaitPromise:true,returnByValue:true});
      if (result.exceptionDetails) throw Error(result.exceptionDetails.exception?.description ?? result.exceptionDetails.text);
      return result.result.value;
    };
    const setup = `const q=s=>document.querySelector('[data-ui="'+s+'"]'), c=s=>document.querySelector('[data-c="'+s+'"]'), i=s=>document.querySelector('[data-is="'+s+'"]');
      const wait=async f=>{const end=Date.now()+10000;while(!f()){if(Date.now()>end)throw Error('wait timed out');await new Promise(r=>setTimeout(r,30));}};
      const change=(el,value)=>{el.value=value;el.dispatchEvent(new Event('input',{bubbles:true}));};`;
    const ready = tab => run(tab, `(async()=>{${setup}await wait(()=>i('save')&&!q('ready').hidden);})()`);
    await tab.go(server.url.href); await ready(tab);
    const initial = await run(tab, `(async()=>{${setup}
      document.querySelector('[data-post]').click();await wait(()=>!q('compose').hidden);
      const pick=document.querySelector('[data-pick="story"]');pick.focus();pick.dispatchEvent(new KeyboardEvent('keydown',{key:'Enter',bubbles:true}));
      change(c('quote'),'Edited campaign wording');change(c('claim'),'A reviewed claim');change(c('scale'),'0.8');
      document.querySelector('[data-theme="paper"]').click();document.querySelector('[data-align="center"]').click();
      change(i('name'),'Campaign persistence');i('save').click();await wait(()=>/Saved/.test(i('said').textContent));
      return {url:location.href,active:c('which').textContent};})()`);
    assert.equal(initial.active,'Story'); seen.push('keyboard selection and named save');
    // Reload the exact saved record, then reopen its artboard through the real button.
    await tab.go(initial.url); await ready(tab);
    const restored = await run(tab, `(async()=>{${setup}await wait(()=>/Opened/.test(i('said').textContent));
      document.querySelector('[data-post]').click();await wait(()=>!q('compose').hidden);
      return {quote:c('quote').value,claim:c('claim').value,scale:c('scale').value,active:c('which').textContent,
        theme:document.querySelector('[data-theme="paper"]').getAttribute('aria-pressed'),align:document.querySelector('[data-align="center"]').getAttribute('aria-pressed')};})()`);
    assert.deepEqual(restored,{quote:'Edited campaign wording',claim:'A reviewed claim',scale:'0.8',active:'Story',theme:'true',align:'true'});
    seen.push('reload preserves words, claim, scale, theme, alignment and active format');
    // A second saved record has no campaigns. Button edits alone must open the discard prompt.
    await run(tab, `(async()=>{${setup}q('empty').click();change(i('name'),'Empty research');i('copy').click();await wait(()=>/Saved "Empty/.test(i('said').textContent));})()`);
    await tab.go(initial.url); await ready(tab);
    const guard = await run(tab, `(async()=>{${setup}await wait(()=>/Opened/.test(i('said').textContent));
      document.querySelector('[data-post]').click();await wait(()=>!q('compose').hidden);document.querySelector('[data-theme="lime"]').click();
      await wait(()=>[...i('list').options].some(o=>o.textContent==='Empty research'));
      i('list').value=[...i('list').options].find(o=>o.textContent==='Empty research').value;i('list').dispatchEvent(new Event('change',{bubbles:true}));
      await wait(()=>document.querySelector('dialog[open]'));i('keep').click();
      return c('quote').value;})()`);
    assert.equal(guard,'Edited campaign wording'); seen.push('button-only edit triggers discard guard and Keep editing retains work');
    // Real concurrent tab: first save advances the record; second save must conflict.
    const other = await open(); await other.go(initial.url); await ready(other);
    await run(other, `(async()=>{${setup}await wait(()=>/Opened/.test(i('said').textContent));document.querySelector('[data-post]').click();await wait(()=>!q('compose').hidden);change(c('quote'),'Second tab wording');})()`);
    await run(tab, `(async()=>{${setup}i('save').click();await wait(()=>/Saved/.test(i('said').textContent));})()`);
    const conflict = await run(other, `(async()=>{${setup}i('save').click();await wait(()=>document.querySelector('[data-save-state="conflict"]'));return c('quote').value;})()`);
    assert.equal(conflict,'Second tab wording'); seen.push('stale save conflicts and preserves second-tab artboard edits');
    // Project download and file import retain the same state without another store.
    const exported = await run(tab, `(async()=>{${setup}
      const original=URL.createObjectURL;let body;URL.createObjectURL=b=>{body=b;return original(b);};
      const click=HTMLAnchorElement.prototype.click;HTMLAnchorElement.prototype.click=function(){};
      try{q('dl-project').click();return await body.text();}finally{URL.createObjectURL=original;HTMLAnchorElement.prototype.click=click;}})()`);
    assert.equal(JSON.parse(exported).campaigns[0].content.quote,'Edited campaign wording');
    await run(tab, `(async()=>{${setup}q('empty').click();const dt=new DataTransfer();dt.items.add(new File([${JSON.stringify(exported)}],'roundtrip.json',{type:'application/json'}));q('open-project').files=dt.files;q('open-project').dispatchEvent(new Event('change',{bubbles:true}));await wait(()=>/Opened roundtrip/.test(q('message').textContent));document.querySelector('[data-post]').click();await wait(()=>!q('compose').hidden);})()`);
    assert.equal(await run(tab, `document.querySelector('[data-c="quote"]').value`),'Edited campaign wording');
    seen.push('project download and file import preserve saved artboards');
    const invalid = JSON.parse(exported); invalid.campaigns[0].overrides.story.scale=99;
    await run(tab, `(async()=>{${setup}const dt=new DataTransfer();dt.items.add(new File([${JSON.stringify(JSON.stringify(invalid))}],'invalid.json'));q('open-project').files=dt.files;q('open-project').dispatchEvent(new Event('change',{bubbles:true}));await wait(()=>/could not be opened/.test(q('message').textContent));})()`);
    assert.equal(await run(tab, `document.querySelector('[data-c="quote"]').value`),'Edited campaign wording');
    seen.push('invalid import leaves current editor unchanged');
    for(const [index,mutate]of [p=>p.claims=null,p=>p.decisions=[],p=>p.records[0].authors=null,p=>p.records[0].url='javascript:alert(1)',p=>p.records.push(p.records[0])].entries()){
      const broken=JSON.parse(exported);mutate(broken);
      const remaining=await run(tab,`(async()=>{${setup}const dt=new DataTransfer();dt.items.add(new File([${JSON.stringify(JSON.stringify(broken))}],'broken-${index}.json'));q('open-project').files=dt.files;q('open-project').dispatchEvent(new Event('change',{bubbles:true}));await wait(()=>q('message').textContent.startsWith('broken-${index}.json could not be opened'));return {quote:c('quote').value,open:!q('compose').hidden};})()`);
      assert.deepEqual(remaining,{quote:'Edited campaign wording',open:true});
    }
    seen.push('malformed records, claims, decisions, unsafe source addresses and duplicate identities leave the current review and artboard intact');
    // Changing the project while fonts are pending must not reopen the previous project.
    const race = await run(tab, `(async()=>{${setup}c('close').click();const load=document.fonts.load.bind(document.fonts);let release;const gate=new Promise(ok=>release=ok);document.fonts.load=()=>gate;
      document.querySelector('[data-post]').click();q('empty').click();release([]);await new Promise(ok=>setTimeout(ok,50));document.fonts.load=load;return q('compose').hidden;})()`);
    assert.equal(race,true); seen.push('project switch cancels pending composer open');
    // A fresh 390px page uses the same package and saved record.
    await other.page('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:true});
    await other.go(initial.url); await ready(other);
    const mobile = await run(other, `(async()=>{${setup}await wait(()=>/Opened/.test(i('said').textContent));document.querySelector('[data-post]').click();await wait(()=>!q('compose').hidden);return {overflow:document.documentElement.scrollWidth-innerWidth,quote:c('quote').value};})()`);
    assert.equal(mobile.overflow,0);assert.equal(mobile.quote,'Edited campaign wording');seen.push('390px saved-artboard reopening without horizontal overflow');
    const exports = await run(other, `(async()=>{const png=await createImageBitmap(await window.aurekaiComposer.pngBlob('story'));
      const webm=await window.aurekaiComposer.webmBlob('card',0.1), video=document.createElement('video'), url=URL.createObjectURL(webm);video.src=url;
      try{await new Promise((ok,fail)=>{video.onloadedmetadata=ok;video.onerror=()=>fail(Error('Video could not be decoded'));});
      return {png:[png.width,png.height],video:[video.videoWidth,video.videoHeight],bytes:webm.size};}finally{URL.revokeObjectURL(url);png.close();}})()`);
    assert.deepEqual(exports.png,[1080,1920]);assert.deepEqual(exports.video,[1200,630]);assert.ok(exports.bytes>1000);seen.push('restored artboards export real PNG and decodable WebM');
    const cancelled = await run(other, `(async()=>{${setup}const result=window.aurekaiComposer.webmBlob('card',1).then(()=>false,error=>error.message);q('empty').click();return await result;})()`);
    assert.match(cancelled,/closed or replaced/);seen.push('project replacement cancels an active video recording');
    // Delete the originating quotation, save and reload. The artboard must
    // remain reachable without recreating evidence or an empty replacement.
    await other.go(initial.url);await ready(other);
    await run(other,`(async()=>{${setup}await wait(()=>/Opened/.test(i('said').textContent));document.querySelector('[data-campaign-open="0"]').click();await wait(()=>!q('compose').hidden);
      document.querySelector('[data-unfile]').click();await wait(()=>/removed/.test(c('notes').textContent));i('save').click();await wait(()=>document.querySelector('[data-save-state="saved"]'));})()`);
    await other.go(initial.url);await ready(other);
    const orphan=await run(other,`(async()=>{${setup}await wait(()=>/Opened/.test(i('said').textContent));document.querySelector('[data-campaign-open="0"]').click();await wait(()=>!q('compose').hidden);return {quote:c('quote').value,note:c('notes').textContent,count:document.querySelectorAll('[data-campaign-open]').length};})()`);
    assert.equal(orphan.quote,'Edited campaign wording');assert.match(orphan.note,/removed/);assert.equal(orphan.count,1);seen.push('orphaned artboard remains reachable after quotation removal, save and reload');
    const correction=await run(other,`(async()=>{${setup}document.querySelector('[data-campaign-review="0"]').click();const d=document.querySelector('dialog[open]');
      if(!d)throw Error('Source review did not open');const source=d.querySelector('[data-source-preview]').textContent,original=d.querySelector('blockquote').textContent;
      d.querySelector('[data-source-apply]').click();await wait(()=>!document.querySelector('dialog[open]'));
      const result={source,original,quote:c('quote').value,theme:document.querySelector('[data-theme="lime"]').getAttribute('aria-pressed'),note:c('notes').textContent,overflow:document.documentElement.scrollWidth-innerWidth};
      i('save').click();await wait(()=>document.querySelector('[data-save-state="saved"]'));return result;})()`);
    assert.ok(correction.source);assert.notEqual(correction.source,correction.original);assert.equal(correction.quote,'Edited campaign wording');assert.equal(correction.theme,'true');assert.equal(correction.note,'');assert.equal(correction.overflow,0);
    await other.go(initial.url);await ready(other);
    const corrected=await run(other,`(async()=>{${setup}await wait(()=>/Opened/.test(i('said').textContent));document.querySelector('[data-campaign-open="0"]').click();await wait(()=>!q('compose').hidden);
      document.querySelector('[data-campaign-source="0"]').click();return {quote:c('quote').value,note:c('notes').textContent,abstract:q('abstract').textContent};})()`);
    assert.equal(corrected.quote,'Edited campaign wording');assert.equal(corrected.note,'');assert.ok(corrected.abstract.includes(correction.source));seen.push('reviewed source replacement survives reload, preserves edits and opens the receiving source');
    const sourceWords=await run(other,`(async()=>{${setup}document.querySelector('[data-campaign-review="0"]').click();const d=document.querySelector('dialog[open]'),wording=d.querySelector('[data-source-wording]');
      wording.value='source';wording.dispatchEvent(new Event('change',{bubbles:true}));const preview=d.querySelector('[data-wording-preview]').textContent;
      d.querySelector('[data-source-apply]').click();await wait(()=>!document.querySelector('dialog[open]'));return {preview,words:c('quote').value,exact:c('exact').dataset.tone};})()`);
    assert.equal(sourceWords.preview,correction.source);assert.equal(sourceWords.words,correction.source);assert.equal(sourceWords.exact,'');seen.push('source words replace editorial wording only after explicit reviewed selection');
    // Import an upstream abstract correction without deleting its filed evidence.
    const changedProject=JSON.parse(exported),changedRecord=changedProject.excerpts[0].record;
    const staleQuotes=changedProject.excerpts.filter(e=>e.record===changedRecord).map(e=>e.text);
    changedProject.records.find(r=>r.id===changedRecord).abstract='The upstream abstract was corrected and its earlier quotations require review.';
    const correctionReport=await run(tab,`(async()=>{${setup}const dt=new DataTransfer();dt.items.add(new File([${JSON.stringify(JSON.stringify(changedProject))}],'corrected-source.json'));q('open-project').files=dt.files;q('open-project').dispatchEvent(new Event('change',{bubbles:true}));await wait(()=>/Opened corrected-source/.test(q('message').textContent));return {writeup:q('writeup').textContent,claims:q('claims').textContent,excerpts:document.querySelectorAll('[data-unfile]').length};})()`);
    for(const quote of staleQuotes)assert.equal(correctionReport.writeup.includes(quote),false);
    assert.match(correctionReport.writeup,/omitted from this write-up/);assert.match(correctionReport.claims,/not counted/);assert.equal(correctionReport.excerpts,changedProject.excerpts.length);
    seen.push('upstream abstract correction removes stale evidence from claim standing and write-up while retaining filed quotations');
    // Transport ignores abort deliberately: stale readback must still be refused.
    const searchRace=await run(tab,`(async()=>{${setup}q('empty').click();q('src-openalex').checked=true;q('src-epmc').checked=false;
      const original=fetch,requests=[];window.fetch=(url,options)=>String(url).startsWith('https://api.openalex.org/')?new Promise(resolve=>requests.push({url,signal:options.signal,resolve})):original(url,options);
      const answer=(request,id)=>request.resolve(new Response(JSON.stringify({results:[{id:'https://openalex.org/'+id,title:id,abstract_inverted_index:{['A retained abstract with enough words to be a usable research record for this controlled provider response.']:[0]},authorships:[]}]}),{headers:{'content-type':'application/json'}}));
      try{q('query').value='earlier';q('search').dispatchEvent(new Event('submit',{cancelable:true}));await wait(()=>requests.length===1);
        q('query').value='later';q('search').dispatchEvent(new Event('submit',{cancelable:true}));await wait(()=>requests.length===2);answer(requests[1],'CurrentResult');await wait(()=>q('list').textContent.includes('CurrentResult'));answer(requests[0],'ObsoleteResult');await new Promise(resolve=>setTimeout(resolve,60));
        return {list:q('list').textContent,aborted:requests[0].signal.aborted};
      }finally{window.fetch=original;}})()`);
    assert.equal(searchRace.aborted,true);assert.match(searchRace.list,/CurrentResult/);assert.doesNotMatch(searchRace.list,/ObsoleteResult/);seen.push('a later source search cancels its predecessor and ignores obsolete readback even when transport ignores abort');
    const replacementRace=await run(tab,`(async()=>{${setup}const original=fetch;let pending,signal;window.fetch=(url,options)=>String(url).startsWith('https://api.openalex.org/')?new Promise(resolve=>{pending=resolve;signal=options.signal;}):original(url,options);
      try{q('query').value='replaced';q('search').dispatchEvent(new Event('submit',{cancelable:true}));await wait(()=>pending);q('empty').click();pending(new Response(JSON.stringify({results:[{id:'https://openalex.org/Wold',title:'Wrong project result',abstract_inverted_index:{['A retained abstract with enough words to be a usable research record for this controlled provider response.']:[0]},authorships:[]}]}),{headers:{'content-type':'application/json'}}));await new Promise(resolve=>setTimeout(resolve,60));return {list:q('list').textContent,status:q('search-state').textContent,aborted:signal.aborted};}
      finally{window.fetch=original;}})()`);
    assert.equal(replacementRace.aborted,true);assert.doesNotMatch(replacementRace.list,/Wrong project result/);assert.equal(replacementRace.status,'');seen.push('replacing the research project cancels pending source reads and prevents cross-project adoption');
    const partial=await run(tab,`(async()=>{${setup}const original=fetch;window.fetch=(url,options)=>String(url).startsWith('https://api.openalex.org/')?Promise.resolve(new Response('',{status:503})):String(url).startsWith('https://www.ebi.ac.uk/')?Promise.resolve(new Response(JSON.stringify({resultList:{result:[{source:'MED',id:'partial',title:'Partial provider success',abstractText:'A retained abstract with enough words to be a usable research record for this controlled provider response.'}]}}),{headers:{'content-type':'application/json'}})):original(url,options);
      try{q('src-epmc').checked=true;q('query').value='partial provider';q('search').dispatchEvent(new Event('submit',{cancelable:true}));await wait(()=>q('list').textContent.includes('Partial provider success'));return q('search-state').textContent;}finally{window.fetch=original;}})()`);
    assert.match(partial,/1 new/);assert.match(partial,/OpenAlex answered 503/);seen.push('one unavailable provider leaves the successful source results usable and reports the failure');
    const lateImport=await run(tab,`(async()=>{${setup}const data=${JSON.stringify(exported)};const dt=new DataTransfer();dt.items.add(new File([data],'restore-for-race.json'));q('open-project').files=dt.files;q('open-project').dispatchEvent(new Event('change',{bubbles:true}));await wait(()=>q('message').textContent.startsWith('Opened restore-for-race'));document.querySelector('[data-campaign-open="0"]').click();await wait(()=>!q('compose').hidden);
      const original=File.prototype.text;let release;File.prototype.text=function(){return this.name==='delayed-review.json'?new Promise(resolve=>release=resolve):original.call(this);};
      try{const late=new DataTransfer();late.items.add(new File([data],'delayed-review.json'));q('open-project').files=late.files;q('open-project').dispatchEvent(new Event('change',{bubbles:true}));await wait(()=>release);change(c('quote'),'Edited while the import was reading');release(data);await wait(()=>q('message').textContent.includes('review changed while'));return c('quote').value;}finally{File.prototype.text=original;}})()`);
    assert.equal(lateImport,'Edited while the import was reading');seen.push('a delayed file import cannot overwrite edits made while the file was reading');
    const replacementImport=await run(tab,`(async()=>{${setup}const original=File.prototype.text;let release;File.prototype.text=function(){return this.name==='replaced-review.json'?new Promise(resolve=>release=resolve):original.call(this);};
      try{const dt=new DataTransfer();dt.items.add(new File([${JSON.stringify(exported)}],'replaced-review.json'));q('open-project').files=dt.files;q('open-project').dispatchEvent(new Event('change',{bubbles:true}));await wait(()=>release);q('empty').click();release(${JSON.stringify(exported)});await wait(()=>q('message').textContent.includes('review changed while'));return {question:q('question').value,excerpts:document.querySelectorAll('[data-unfile]').length};}finally{File.prototype.text=original;}})()`);
    assert.deepEqual(replacementImport,{question:'Your question',excerpts:0});seen.push('a delayed file import cannot replace a different project');
    const overflowExport=await run(tab,`(async()=>{${setup}const dt=new DataTransfer();dt.items.add(new File([${JSON.stringify(exported)}],'export-fit.json'));q('open-project').files=dt.files;q('open-project').dispatchEvent(new Event('change',{bubbles:true}));await wait(()=>q('message').textContent.startsWith('Opened export-fit'));document.querySelector('[data-campaign-open="0"]').click();await wait(()=>!q('compose').hidden);change(c('quote'),'W'.repeat(400));
      const refusal=await Promise.all([window.aurekaiComposer.pngBlob('story'),window.aurekaiComposer.webmBlob('story',0.1)].map(p=>p.then(()=>false,error=>error.message)));
      const blocked={refusal,disabled:[c('png').disabled,c('png-all').disabled,c('webm').disabled],note:c('notes').textContent};change(c('quote'),'A short message that fits.');const bitmap=await createImageBitmap(await window.aurekaiComposer.pngBlob('story'));blocked.recovered=[bitmap.width,bitmap.height];bitmap.close();return blocked;})()`);
    assert.deepEqual(overflowExport.disabled,[true,true,true]);assert.ok(overflowExport.refusal.every(message=>/do not fit/.test(message)));assert.match(overflowExport.note,/do not fit/);assert.deepEqual(overflowExport.recovered,[1080,1920]);seen.push('overwide words block PNG and motion export without cropping; shortening the message restores actual image export');
  }, {timeoutMs:60000});
  console.log(JSON.stringify({passed:seen.length,checks:seen},null,2));
} finally { server.stop(true); }
