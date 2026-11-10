import {mountWorkbookEditor} from './workbook-editor.mjs';
import {readWorkbookReview,workbookKey} from './workbook-record.mjs';
import {createWorkbookLifetime,openWorkbookStore,clearWorkbookHost} from './workbook-lifetime.mjs';
const host=document.querySelector('[data-workbook-editor]'),form=document.querySelector('[data-workbook-open]'),status=document.querySelector('[data-workbook-status]'),lifetime=createWorkbookLifetime();
const query=new URL(location.href).searchParams;let editor=null,storePromise;
const currentStore=()=>storePromise??=openWorkbookStore(lifetime);
lifetime.onClose(()=>{editor?.dispose();editor=null;for(const n of form.elements){n.disabled=true;if(n.tagName==='INPUT')n.value='';}form.hidden=true;status.textContent='';status.hidden=true;clearWorkbookHost(host);});
async function open(key,snapshot,source){
 const store=await currentStore();lifetime.assert();
 const mounted=await mountWorkbookEditor(host,{key,snapshot,source,store,lifetime});lifetime.assert();editor=mounted;
 form.hidden=true;status.hidden=true;host.dataset.ready='true';
}
async function selectedFile(file){
 if(!file||!/\.xlsx$/i.test(file.name)||file.size>8*1024*1024)throw Error('Choose an XLSX workbook of at most 8 MiB.');
 lifetime.assert();const worker=new Worker(new URL('./workbook-import-worker.js',import.meta.url),{type:'module'});
 try{
  const bytes=await file.arrayBuffer();lifetime.assert();
  return await new Promise((resolve,reject)=>{
   let stop=()=>{};const timer=setTimeout(()=>finish(Error('Workbook import exceeded 30 seconds.')),30000);
   const finish=(error,result)=>{clearTimeout(timer);stop();worker.terminate();error?reject(error):resolve(result);};
   stop=lifetime.onClose(()=>finish(Error('The workbook session closed.')));
   worker.onmessage=({data})=>{try{lifetime.assert();finish(data.error?Error(data.error):null,data.result);}catch(error){finish(error);}};
   worker.onerror=()=>finish(Error('Workbook import could not finish.'));
   worker.postMessage({bytes,name:file.name},[bytes]);
  });
 }finally{worker.terminate();}
}
form.addEventListener('submit',async event=>{
 event.preventDefault();if(lifetime.closed)return;const controls=[...form.elements];controls.forEach(n=>{n.disabled=true;});status.textContent='Opening workbook…';
 try{
  const file=document.querySelector('[data-workbook-file]').files[0],review=await selectedFile(file);lifetime.assert();const key='finance-workbook:'+crypto.randomUUID();
  const url=new URL(location.href);url.searchParams.set('workbook',key);history.replaceState(null,'',url);await open(key,review.snapshot,review.source);
 }catch(error){if(!lifetime.closed){status.hidden=false;status.textContent=error.message;}}
 finally{if(!lifetime.closed)controls.forEach(n=>{n.disabled=false;});}
});
if(query.has('workbook')){
 form.hidden=true;
 try{
  if(query.getAll('workbook').length!==1)throw Error('Choose one saved workbook.');
  const key=workbookKey(query.get('workbook')),store=await currentStore();lifetime.assert();
  const record=(await store.readLocal(key)).find(row=>row.key===key);lifetime.assert();
  if(!record)throw Error('This saved workbook was not found in the current workspace.');
  const review=await readWorkbookReview(record);lifetime.assert();await open(key,review.snapshot,review.source);
 }catch(error){if(!lifetime.closed){status.textContent=error.message;status.setAttribute('role','alert');}}
}
