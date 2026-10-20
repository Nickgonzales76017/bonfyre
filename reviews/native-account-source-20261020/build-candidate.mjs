import {readFileSync,writeFileSync,mkdirSync,mkdtempSync,realpathSync} from 'node:fs';
import {dirname,join,resolve,relative,isAbsolute} from 'node:path';
import {fileURLToPath} from 'node:url';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';

// This is review tooling, never a production dependency or policy bypass.
// It reads the current owners and writes only a disposable candidate and patch.
export const directory=dirname(fileURLToPath(import.meta.url));
export const expected=new Map([
 ['site/matrix/surfaces/account-native.html','d4bd2c25cfe9760d963fef7406bbc961ac66dc60127af31c9d1ce4e9cc6465e8'],
 ['site/matrix/surfaces/_kit.js','308fb72f1f57dca488e925e325ddcdc4c7e99e6e0797800307b9cc2338254092'],
 ['site/matrix/bonfyre-kit.js','308fb72f1f57dca488e925e325ddcdc4c7e99e6e0797800307b9cc2338254092'],
 ['site/matrix/surfaces/link.html','36bce3753d9e88f913e22915be9f086f07ccf2690f8a317766f1b54cf4d508a8'],
 ['site/matrix/aurekai-bar.css','5fc20966c0aee079b2852dcd1a6fe37b94e7f6dbed10e96b0b7116ba8c9cc477'],
 ['cmd/BonfyreWebAddress/src/account_navigation_markup.h','0a458c3e307524ba59d19b5c760d0e19abb11753b574e2ff5297ee9632fe2d54'],
]);
export const digest=value=>createHash('sha256').update(value).digest('hex');
const source=name=>readFileSync(join(directory,name),'utf8');
export function once(text,pattern,replacement,label=String(pattern)){
 const count=typeof pattern==='string'?text.split(pattern).length-1:[...text.matchAll(new RegExp(pattern.source,pattern.flags.replace('g','')+'g'))].length;
 if(count!==1)throw Error('Expected one '+label+', found '+count+'. Reconcile the current owner first.');
 return text.replace(pattern,replacement);
}
export function accountPage(text){
 text=once(text,'<style>','<style>\n#signed[hidden],#out[hidden],.account-tools-host[hidden]{display:none!important}\n');
 text=once(text,'<script defer src="/assets/matrix-htmx.js"></script>','<script src="/assets/bonfyre-kit.js"></script><script defer src="/assets/matrix-htmx.js"></script>');
 text=once(text,/const out=document\.querySelector\('#out'\)[\s\S]*?(?=<\/script><script>document\.querySelector\('#institution-open'\))/,source('account-client.js'));
 text=once(text,'hx-trigger="load" hx-swap="innerHTML" hx-headers=', 'hx-trigger="account-ready" hx-history="false" hx-swap="innerHTML" hx-headers=');
 return text;
}
export function kit(text){
 text=once(text,/  K\._session = null;[\s\S]*?(?=  \/\* Signed in is a question)/,source('kit-session.js')+'\n');
 text=once(text,"          slot.innerHTML = 'You are signed in as ' + s.who", "          slot.textContent = 'You are signed in as ' + s.who");
 // A refused section does not invite a signed-in person to register again.
 text=once(text,"      }).catch(() => {});", "        else if(slot && s.state !== 'anonymous') slot.textContent = 'Account access could not be checked. Open Account to retry.';\n      }).catch(() => {});");
 return text;
}
export function barCSS(text){
 const marker='/* ------------------------------------------------- surfaces in borrowed sheets';
 const at=text.indexOf(marker);if(at<0||text.indexOf(marker,at+1)!==-1)throw Error('Shared-style boundary changed.');
 // The owning donor sheets keep their grid, images, color system and typography.
 // No legacy restyling is copied into another global stylesheet.
 return text.slice(0,at)+`/* Specialized page anatomy stays in its own stylesheet. */\n@media(prefers-reduced-motion:reduce){.ak-bar *{transition:none!important;animation:none!important}}\n`;
}
export function navigation(text){
 const anchor='"  window.addEventListener(\'pagehide\', () => { suspended = true; invalidate(); });\\n" \\\n';
 const line=JSON.stringify("  window.addEventListener('bonfyre:account-changed', () => { invalidate(); void refresh(true); });\n")+' \\\n';
 return once(text,anchor,line+anchor);
}
export function sourcePage(text){
 text=once(text,'</style></head>',source('source-view.css')+'\n</style></head>');
 text=once(text,'.plan{display:none}', '.plan{display:block;grid-column:1/-1}');
 const navigation='<nav class="source-navigation" aria-label="Source work"><a href="/s/sets/all">Discovery sets</a><a href="/sources">Publisher library</a><a href="#detail">Inspect source</a><a href="#source-destinations">Destinations and publication</a><a href="/apps?context=self">Installed work</a></nav>';
 const comparison='<section class="source-comparison" aria-labelledby="source-comparison-title"><h2 id="source-comparison-title">Compare selected sources</h2><p id="source-selection-note" role="status"></p><div class="source-comparison-scroll" id="source-comparison" tabindex="0" aria-label="Selected source comparison"></div><div class="actions"><button type="button" class="ghost" id="clear-source-selection" disabled>Clear comparison</button><button type="button" class="ghost" id="export-source-selection" disabled>Export exact references</button></div></section>';
 text=once(text,'<section class="work">',navigation+comparison+'<section class="work">');
 text=once(text,'<aside class="plan">','<aside class="plan" id="source-destinations" tabindex="-1">');
 text=once(text,'</script><script>\n(async()=>', '</script><script type="module">\n(async()=>');
 text=once(text,/function list\(\)\{[\s\S]*?(?=function cataloguePath\(offset\))/, '');
 text=once(text,/function cataloguePath\(offset\)\{[\s\S]*?(?=function status\(name,x,detail\))/, '');
 const declarations=source('source-cut.mjs').replace('export class SourceCut','class SourceCut')+'\n'+source('source-view.js');
 text=once(text,'const S=t=>t.release.stages,B=t=>t.release.source_contract;','const S=t=>t.release.stages,B=t=>t.release.source_contract;\n'+declarations+'\nlet publishing=false;');
 text=once(text,'function formationReady(){let b=', 'function formationReady(){let b=');
 text=once(text,"if(b)b.disabled=!(picked&&$('world-title').value.trim()&&$('world-lede').value.trim())", "if(b)b.disabled=publishing||!(picked&&$('world-title').value.trim()&&$('world-lede').value.trim())");
 text=once(text,"if(!picked||!title||!lede)return;let token=K.token()", "if(publishing||!picked||!title||!lede)return;const selectedSource=picked;publishing=true;let token=K.token()");
 text=once(text,'source=B(picked).source,selector=', 'source=B(selectedSource).source,selector=');
 text=once(text,'subject_address:picked.subject_id,source_selector:selector','subject_address:selectedSource.subject_id,source_selector:selector');
 text=once(text,'character,lede,subject:picked.subject_id,sources:', 'character,lede,subject:selectedSource.subject_id,sources:');
 text=once(text,'finally{formationReady()}}async function observe()', 'finally{publishing=false;formationReady()}}async function observe()');
 text=once(text,"searchTerm=e.target.value.trim();clearTimeout(searchTimer);searchTimer=setTimeout(reloadSearch,220)","searchTerm=e.target.value.trim();clearTimeout(searchTimer);cut.prepare(searchTerm);searchTimer=setTimeout(reloadSearch,220)");
 // The old local JSON sidecar is retired. Destination choices are transient UI;
 // the existing CMS remains the owner when a publication is explicitly submitted.
 text=once(text,/const key='bonfyre\.foreign\.distribution-plan\.v2',read=\(\)=>\{[\s\S]*?(?=;function plan\(\))/,"const destinationDraft=new Map(),read=()=>Object.fromEntries(destinationDraft)");
 text=once(text,'localStorage.setItem(key,JSON.stringify(x))','destinationDraft.set(picked.url,[...a])');
 text=once(text,'localStorage.removeItem(key)','destinationDraft.clear()');
 text=once(text,'>Download plan</button>','>Review destinations</button>');
 text=once(text,/\$\('download'\)\.onclick=\(\)=>\{[\s\S]*?(?=;let \[t,e\]=await Promise\.all)/,"$('download').onclick=()=>{if(!picked)return;const selected=read()[picked.url]||[],box=E('div','claim');box.setAttribute('role','status');box.textContent='Selected source: '+B(picked).source+'\\n'+selected.map(id=>id+': '+destinationState(id,picked).state).join('\\n')+'\\nThese are choices for review. No destination has been activated.';$('destination-review-result')?.remove();box.id='destination-review-result';$('choices').after(box)}");
 text=once(text,/let \[t,e\]=await Promise\.all\([\s\S]*?list\(\);plan\(\)\}\)\(\);/,"$('clear-source-selection').onclick=()=>cut.clearSelection();$('export-source-selection').onclick=exportSourceSelection;window.addEventListener('pagehide',()=>{clearTimeout(searchTimer);cut.prepare(searchTerm)});window.addEventListener('pageshow',event=>{if(event.persisted)void cut.search(searchTerm)});void K.foreignEdges().then(result=>{edges=result.ok?(result.edges||[]):[];if(picked)plan()}).catch(()=>{});await cut.search(searchTerm);})();");
 // Preserve endpoint inspection as an explicit request. The source workbench no
 // longer starts a whole-receipt scan and 32 metadata reads merely by opening.
 text=once(text,"const accountDoorList = document.getElementById('account-door-list');\ntry {",`const accountDoorList = document.getElementById('account-door-list');
accountDoorList.textContent='Inspect retained authorization metadata when needed. This does not connect an account.';
const inspect=document.createElement('button');inspect.type='button';inspect.className='ghost';inspect.textContent='Inspect known authorization endpoints';accountDoorList.before(inspect);
inspect.addEventListener('click',async()=>{inspect.disabled=true;
const controller=new AbortController(),deadline=setTimeout(()=>controller.abort(),15000);
async function metadataRead(url){const response=await fetch(url,{credentials:'same-origin',cache:'no-store',signal:controller.signal,headers:{Accept:'application/json'}});if(!response.ok)throw Error('metadata unavailable');const reader=response.body.getReader(),chunks=[];let size=0;try{for(;;){const part=await reader.read();if(part.done)break;size+=part.value.byteLength;if(size>2*1024*1024){await reader.cancel();throw Error('history too large for this view');}chunks.push(part.value)}}finally{reader.releaseLock()}const bytes=new Uint8Array(size);let offset=0;for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.length}return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes))}
try {`);
 text=once(text,/  const response = await fetch\('\/foreign\/receipts'[\s\S]*?const listing = await response\.json\(\);/,"  const listing = await metadataRead('/foreign/receipts');");
 text=once(text,/    const bodyResponse = await fetch\(bodyURL[\s\S]*?const metadata = await bodyResponse\.json\(\)\.catch\(\(\) => null\);/,"    const metadata = await metadataRead(bodyURL.href).catch(()=>null);");
 text=once(text,"if (door.protocol !== 'https:') continue;","if (door.protocol !== 'https:' || door.username || door.password) continue;");
 text=once(text,"  accountDoorList.textContent = 'Observed account metadata is unavailable; no connection is inferred.';\n}","  accountDoorList.textContent = 'Metadata could not be read within this view’s size or time limit. No account was connected.';\n}finally{clearTimeout(deadline);inspect.disabled=false}\n});");
 return text;
}
export function buildCandidate(root,output){
 root=realpathSync(root);output=output?realpathSync(output):mkdtempSync(join(tmpdir(),'bonfyre-candidate-'));
 const rel=relative(root,output);if(!rel||(!rel.startsWith('..')&&!isAbsolute(rel)))throw Error('Candidate output must be outside the project.');
 const originals=new Map();
 for(const [path,hash]of expected){const bytes=readFileSync(join(root,path));if(digest(bytes)!==hash)throw Error('Source changed: '+path);originals.set(path,bytes.toString('utf8'));}
 const changes=new Map();
 for(const [path,text]of originals){const result=path.endsWith('account-native.html')?accountPage(text):path.endsWith('_kit.js')||path.endsWith('bonfyre-kit.js')?kit(text):path.endsWith('link.html')?sourcePage(text):path.endsWith('aurekai-bar.css')?barCSS(text):navigation(text);changes.set(path,result);}
 let patch='';const manifest=['path\tbefore_sha256\tafter_sha256\tbytes'];
 for(const [path,text]of changes){
  const before=join(output,'before',path),after=join(output,'candidate',path);mkdirSync(dirname(before),{recursive:true});mkdirSync(dirname(after),{recursive:true});writeFileSync(before,originals.get(path));writeFileSync(after,text);
  const diff=spawnSync('git',['diff','--no-index','--no-ext-diff','--no-color','--unified=3',before,after],{encoding:'utf8',maxBuffer:2*1024*1024});if(diff.status!==0&&diff.status!==1)throw Error(diff.stderr||'Diff failed.');
  patch+=diff.stdout.replace(/^diff --git .+$/m,'diff --git a/'+path+' b/'+path).replace(/^--- .+$/m,'--- a/'+path).replace(/^\+\+\+ .+$/m,'+++ b/'+path);
  manifest.push([path,expected.get(path),digest(text),Buffer.byteLength(text)].join('\t'));
 }
 writeFileSync(join(output,'candidate.patch'),patch);writeFileSync(join(output,'manifest.tsv'),manifest.join('\n')+'\n');
 return {root,output,originals,changes};
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){const result=buildCandidate(process.argv[2],process.argv[3]);console.log('Candidate only: '+result.output);console.log('Native project and runtime unchanged.');}
