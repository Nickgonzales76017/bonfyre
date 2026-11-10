import {validatePlan,money} from './planner-model.mjs';
import {readLocal,saveLocal} from './store.mjs';
import {movedFixture,rotateFixture} from './product-scene-model.mjs';
import {productAssetStore} from './product-scene-store.mjs';
import {loadProductSceneEngine,SCENE_EXTENSIONS} from './product-scene-engine.mjs';
import {mountProductScene} from './product-scene.mjs';
import {publishedModelFiles} from './product-scene-source.mjs';
import {changeRoomQuantity,replaceRoomOption,roomQuantityRows} from './catalogue-room-model.mjs';
export function samePlacement(a,b) {
 if(!a||!b||a.id!==b.id||a.catalogue!==b.catalogue||a.width!==b.width||a.depth!==b.depth||a.items.length!==b.items.length)return false;
 return a.items.every((item,i)=>['key','product','x','y','rotation'].every(k=>item[k]===b.items[i][k]));
}
const el=(tag,text,attrs={})=>{const n=document.createElement(tag);if(text)n.textContent=text;for(const[k,v]of Object.entries(attrs))n.setAttribute(k,v);return n;};
/** Alternate views over the same live plan. All mutations return to the original owner. */
export function createPlannerProductScene({root,catalogue,models={},onSelect,onChange,canEdit,fail}) {
 let plan,selection,studio=null,destroyed=false,mode='2d',booted=false,loading=Promise.resolve();const attempted=new Set();
 const center=root.querySelector('.fp-center'),heading=root.querySelector('.fp-canvas-heading');
 const holder=el('div','',{'data-ui':'product-scene',hidden:''});center.insertBefore(holder,root.querySelector('.fp-legend'));
 const two=el('button','2D plan',{type:'button','aria-pressed':'true'}),three=el('button','3D room',{type:'button','aria-pressed':'false','data-ui':'product-scene-toggle'});heading.append(two,three);
 const existing=[...center.querySelectorAll('.fp-spatial-toolbar,.fp-board,.fp-spatial-actions,[data-ui="spatial-status"],[data-ui="spatial-retry"]')];
 const originalHidden=new Map(existing.map(n=>[n,n.hidden]));
 const store=productAssetStore({scope:catalogue.digest,readLocal,saveLocal,decoders:SCENE_EXTENSIONS});
 const quantities=el('div','',{'data-ui':'material-quantities',class:'fp-material-quantities'}),optionBox=el('div','',{'data-ui':'selected-option'});
 root.querySelector('.fp-summary').insertBefore(quantities,root.querySelector('.fp-total'));root.querySelector('[data-ui="position"]').append(optionBox);
 const css=el('style',`.fp-material-quantities{border-top:1px solid #bbb9a2;padding-top:12px}.fp-material-row{display:grid;grid-template-columns:1fr 64px;gap:8px;align-items:center;margin:8px 0;font-size:13px}.fp-material-row input{width:64px;min-height:44px;font-size:16px}.fp-material-row small{display:block}.fp-canvas-heading{flex-wrap:wrap;gap:6px}.fp-canvas-heading button{min-height:44px}.fp-canvas-heading button[aria-pressed=true]{background:#d2eaa7;color:#162318}.fp-center .ps-workbench{margin:0}.fp-center .ps-grid{grid-template-columns:minmax(0,1fr)}.fp-center .ps-inspector{max-height:270px;border-left:0}.fp-center .ps-viewport{height:500px}.fp-center .ps-product-list{display:none}@media(max-width:760px){.fp-center .ps-viewport{height:360px}}`);root.append(css);
 function edit(next,key=selection){try{if(!canEdit())throw Error('Finish the current operation first.');validatePlan(next,catalogue);onChange(next,key);}catch(e){fail(e.message);}}
 function renderQuantities(){
  if(!plan)return;quantities.replaceChildren(el('strong','Quantities & listed costs'));
  for(const row of roomQuantityRows(plan,catalogue)){
   const line=el('label','',{class:'fp-material-row'}),label=el('span',row.product.label),input=el('input','',{type:'number',min:0,max:12,step:1,value:row.quantity,'aria-label':row.product.label+' quantity'});
   label.append(el('small',`${row.product.option} · ${money(row.total)}`));input.disabled=!canEdit();input.onchange=()=>{try{edit(changeRoomQuantity(plan,catalogue,row.product.id,Number(input.value)));}catch(e){fail(e.message);input.value=row.quantity;}};line.append(label,input);quantities.append(line);
  }
  optionBox.replaceChildren();const current=plan.items.find(i=>i.key===selection),source=catalogue.products.find(p=>p.id===current?.product);if(!source)return;
  const options=catalogue.products.filter(p=>p.product_id===source.product_id&&p.source_url===source.source_url),label=el('label','Observed option'),select=el('select','',{'aria-label':'Selected fixture option'});
  for(const p of options){const o=el('option',`${p.option} · ${money(p.price_cents)}`,{value:p.id});o.selected=p.id===source.id;select.append(o);}select.disabled=options.length===1||!canEdit();select.onchange=()=>{try{edit(replaceRoomOption(plan,catalogue,selection,select.value));}catch(e){fail(e.message);}};label.append(select);optionBox.append(label);
 }
 async function ensureModels(){
  if(!studio||destroyed)return;await studio.ready;
  for(const id of new Set(plan.items.map(i=>i.product))){
   if(attempted.has(id))continue;attempted.add(id);
   try{
    if(await studio.restore(id))continue;
    const descriptor=models[id],product=catalogue.products.find(p=>p.id===id);
    if(!descriptor)continue;if(descriptor.sourceDigest!==product.source_sha256)throw Error('Model binding belongs to another source generation.');
    studio.selectProduct(id);await studio.importFiles(await publishedModelFiles(descriptor));
   }catch(error){attempted.delete(id);fail(error.message);}
  }
  if(destroyed)return;studio.update(plan,selection);studio.view('room');
 }
 async function show(next){
  mode=next;holder.hidden=mode!=='3d';two.setAttribute('aria-pressed',String(mode==='2d'));three.setAttribute('aria-pressed',String(mode==='3d'));
  for(const node of existing)node.hidden=mode==='3d'||originalHidden.get(node);
  if(mode==='2d')return;
  if(!studio){
   const projected={...catalogue,products:catalogue.products.map(p=>({...p,modelCredit:models[p.id]?.credit,geometryNote:models[p.id]?.geometryNote}))};
   studio=mountProductScene({host:holder,catalogue:projected,loadEngine:loadProductSceneEngine,store,canEdit,onSelect,
    onMove(key,position,size,base){try{if(!samePlacement(plan,base))throw Error('The plan changed during the move. Try again.');edit(movedFixture(plan,key,position,size),key);}catch(error){fail(error.message);studio.update(plan,selection);}},
    onRotate(key){edit(rotateFixture(plan,key),key);}
   });studio.update(plan,selection);
  }
  loading=loading.then(ensureModels).catch(error=>{fail(error.message);show('2d');});await loading;
 }
 two.onclick=()=>show('2d');three.onclick=()=>show('3d');
 return {
  update(next,selected){plan=structuredClone(next);selection=selected??next.items[0]?.key;studio?.update(plan,selection);renderQuantities();if(!booted){booted=true;if(new URL(location.href).searchParams.get('scene')==='room')queueMicrotask(()=>show('3d'));}},
  inspect:()=>studio?.inspect()??null,
  destroy(){destroyed=true;studio?.destroy();two.remove();three.remove();holder.remove();quantities.remove();optionBox.remove();css.remove();}
 };
}
