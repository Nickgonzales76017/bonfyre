// Consent-only projection of integrations/browser/extension/store.mjs.
// Reuse the existing same-origin browser owner; never create a parallel store.
import {readLocal} from '../spaces/data/product-workspace/store.mjs';
// A user-selected transfer keeps the original browser records unchanged.
// The recipient receives nothing until the source window confirms the selection.
function offerBrowserDraftTransfer(){
  if(!globalThis.document||document.querySelector('meta[name="bonfyre-workspace-store"]')||!window.opener)return;
  const query=new URL(location.href).searchParams,nonce=query.get('aurekai-transfer'),recipient=query.get('aurekai-recipient');
  if(!/^[a-f0-9-]{36}$/.test(nonce??'')||!/^https:\/\/aurekai\.(?:work|life|live|shop)$/.test(recipient??'')&&!/^http:\/\/127\.0\.0\.1:[0-9]+$/.test(recipient??''))return;
  const allowed=key=>typeof key==='string'&&['draft:catalogue:','fixture-plan:','product-model:'].some(prefix=>key.startsWith(prefix));
  const make=(tag,text)=>{const node=document.createElement(tag);if(text)node.textContent=text;return node;};
  const dialog=make('dialog');dialog.setAttribute('aria-label','Choose browser drafts to bring into Aurekai');dialog.style.cssText='max-width:660px;width:calc(100% - 48px);max-height:85vh;overflow:auto;padding:24px;border:1px solid;border-radius:12px;color:#17201c;background:#fafbf8;font:16px system-ui';
  const heading=make('h2','Bring existing browser work'),explanation=make('p','Choose the drafts and model files to review at '+recipient+'. Originals stay in this browser. Nothing is sent until you confirm below.'),status=make('p'),list=make('div'),open=make('button','Show my saved drafts'),send=make('button','Send selected work for review'),cancel=make('button','Cancel');
  for(const button of [open,send,cancel]){button.type='button';button.style.cssText='min-height:44px;padding:10px 16px;margin:8px 8px 0 0;font:inherit';}send.hidden=true;
  status.setAttribute('role','status');dialog.append(heading,explanation,open,list,status,send,cancel);document.body.append(dialog);dialog.showModal();
  let rows=[],busy=false;
  cancel.onclick=()=>{if(!busy){dialog.close();dialog.remove();}};
  open.onclick=async()=>{
    if(busy)return;busy=true;open.disabled=true;status.textContent='Reading the saved work on this browser…';
    try{
      rows=(await readLocal()).filter(row=>allowed(row.key));
      if(rows.length>200)throw Error('This browser has more than 200 eligible records. Open a narrower working browser profile before transferring.');
      list.replaceChildren();
      for(const row of rows){const label=make('label'),checkbox=make('input');checkbox.type='checkbox';checkbox.value=row.key;label.style.cssText='display:block;padding:10px 0;border-bottom:1px solid #d6ded7';
        const title=row.value?.plan?.title??row.value?.title??row.value?.files?.map(file=>file.name).join(', ')??row.value?.binding?.productId??row.key;
        label.append(checkbox,document.createTextNode(' '+String(title).slice(0,200)));label.append(make('small',' · '+(row.key.startsWith('product-model:')?'3D model record':row.key.startsWith('fixture-plan:')?'Room plan':'Product draft')));list.append(label);
      }
      status.textContent=rows.length?'Select any required 3D model records with your room plan. Historical review references are not approvals.':'No catalogue drafts, room plans or model records were found in this browser.';send.hidden=!rows.length;open.hidden=true;
    }catch(error){status.textContent=error.message;open.disabled=false;}finally{busy=false;}
  };
  send.onclick=()=>{
    if(busy)return;
    try{
      const selected=new Set([...list.querySelectorAll('input:checked')].map(input=>input.value)),chosen=rows.filter(row=>selected.has(row.key));
      if(!chosen.length||chosen.length>100)throw Error('Choose between one and 100 records to review.');
      let bytes=0,nodes=0;const visit=(value,depth=0)=>{if(depth>32||++nodes>100000)throw Error('This selection is too large. Choose fewer drafts.');if(value instanceof Blob)bytes+=value.size;else if(value instanceof Uint8Array)bytes+=value.byteLength;else if(typeof value==='string')bytes+=new TextEncoder().encode(value).length;else if(value&&typeof value==='object')for(const key of Object.keys(value)){bytes+=key.length;visit(value[key],depth+1);}if(bytes>32*1024*1024)throw Error('Choose less than 32 MiB of draft and model data at once.');};
      for(const row of chosen)visit(row.value);
      window.opener.postMessage({type:'bonfyre.workspace.transfer.v1',nonce,rows:chosen.map(({key,value,revision})=>({key,value,revision}))},recipient);
      send.disabled=true;status.textContent='Sent for review. Return to Aurekai to choose what to adopt. These originals have not changed.';
    }catch(error){status.textContent=error.message;}
  };
}
if(globalThis.document)queueMicrotask(offerBrowserDraftTransfer);
