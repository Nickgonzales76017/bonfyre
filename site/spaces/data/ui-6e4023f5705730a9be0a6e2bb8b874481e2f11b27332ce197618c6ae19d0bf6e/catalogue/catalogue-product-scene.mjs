import {mountProductScene} from './product-scene.mjs';
import {loadProductSceneEngine,SCENE_EXTENSIONS} from './product-scene-engine.mjs';
import {productAssetStore} from './product-scene-store.mjs';
import {readLocal,saveLocal} from './store.mjs';
import {resolveRoomOption,newRoomPlan,addRoomUnits} from './catalogue-room-model.mjs';
import {validatePlan,assessPlan,money} from './planner-model.mjs';
import {publishedModelFiles} from './product-scene-source.mjs';
const el=(tag,text,attrs={})=>{const n=document.createElement(tag);if(text)n.textContent=text;for(const [k,v]of Object.entries(attrs))n.setAttribute(k,v);return n;};
/** The selected catalogue option opens a shared viewer and the SAME planner record. */
export function createCatalogueRoomDialog({routes=[],canLeave=()=>true,onError=()=>{}}={}) {
 let dialog=null,scene=null,product=null,selected=null,generation=0,busy=false,projectRows=[];
 async function open(source,variantId){
  close();product=source;
  dialog=el('dialog','',{class:'cr-dialog','aria-label':'Configure product and place in a room'});
  const style=el('style',`.cr-dialog{width:min(1300px,96vw);max-width:96vw;max-height:95vh;border:1px solid #546449;border-radius:14px;background:#111713;color:#edf3e6;padding:16px;font:14px system-ui}.cr-dialog::backdrop{background:#000a}.cr-controls{display:flex;flex-wrap:wrap;gap:14px;align-items:end}.cr-controls label{display:grid;gap:6px;flex:1;min-width:150px}.cr-controls input,.cr-controls select,.cr-controls button{font:inherit;min-height:44px;padding:10px;border:1px solid #6b785f;border-radius:5px;background:#f4f7ed;color:#142016}.cr-heading button{min-height:44px;padding:8px 14px;color:#edf3e6;background:#283223;border:1px solid #66755d;border-radius:6px}.cr-controls button{background:#d2eaa7;color:#152012;cursor:pointer}.cr-heading{display:flex;justify-content:space-between;align-items:center}.cr-heading h2{margin:4px 0 14px}.cr-status{min-height:24px}.cr-dialog .ps-workbench{margin:12px 0}`);
  const head=el('div','',{class:'cr-heading'}),title=el('h2',source.title),dismiss=el('button','Close',{type:'button','aria-label':'Close product configuration'});dismiss.onclick=close;head.append(title,dismiss);
  const controls=el('div','',{class:'cr-controls'}),option=el('select','',{'aria-label':'Existing product option'}),qty=el('input','',{type:'number',min:1,max:12,step:1,value:1,'aria-label':'Quantity to place'}),destination=el('select','',{'aria-label':'Destination room'}),place=el('button','Place in a room',{type:'button'}),status=el('p','',{class:'cr-status',role:'status'}),holder=el('div');
  const label=(text,node)=>{const l=el('label',text);l.append(node);return l;};controls.append(label('Observed option',option),label('Quantity',qty),label('Continue in',destination),place);
  for(const v of source.variants){const opt=el('option',`${v.label} · $${v.price}`,{value:v.id});opt.selected=v.id===variantId;option.append(opt);}
  dialog.append(style,head,controls,status,holder);document.body.append(dialog);dialog.addEventListener('cancel',e=>{e.preventDefault();close();});dialog.showModal();
  const say=(text)=>{status.textContent=text;};
  const updateTotal=()=>{if(!selected){place.disabled=true;return;}const n=Number(qty.value);place.disabled=busy||!Number.isInteger(n)||n<1||n>12;say(`${money(selected.fixture.price_cents*(Number.isInteger(n)?n:0))} listed fixture cost. Freight and tax excluded. Product-editor draft prices do not change this source price.`);};
  async function select(){
   const currentDialog=dialog;currentDialog.dataset.ready='false';let failed=false;
   const serial=++generation;busy=true;place.disabled=true;scene?.destroy();scene=null;holder.replaceChildren();
   try{
    selected=resolveRoomOption(source,option.value,routes);
    if(!selected){say('This exact option is not in a published room collection. No room or price mapping was inferred.');return;}
    const {route,fixture,binding}=selected,store=productAssetStore({scope:route.catalogue.digest,readLocal,saveLocal,decoders:SCENE_EXTENSIONS});
    destination.replaceChildren(el('option','New space',{value:''}));
    projectRows=(await readLocal('fixture-plan:'+route.form+':')).filter(r=>{try{validatePlan(r.value?.plan,route.catalogue);return r.key==='fixture-plan:'+route.form+':'+r.value.plan.id;}catch{return false;}});
    for(const r of projectRows)destination.append(el('option',r.value.plan.title,{value:r.value.plan.id}));
    if(serial!==generation)return;
    scene=mountProductScene({host:holder,catalogue:route.catalogue,store,loadEngine:loadProductSceneEngine});const activeScene=scene;activeScene.selectProduct(fixture.id);await activeScene.ready;
    if(serial!==generation)return;
    const restored=await activeScene.restore(fixture.id);
    if(serial!==generation)return;
    if(!restored&&binding.model){const files=await publishedModelFiles(binding.model);if(serial!==generation)return;await activeScene.importFiles(files);}
    if(serial!==generation)return;
    currentDialog.dataset.fixture=fixture.id;currentDialog.dataset.ready='true';
   }catch(error){failed=true;if(serial===generation){say(error.message);onError(error.message);}}finally{if(serial===generation){busy=false;if(selected&&!failed)updateTotal();else place.disabled=true;}}
  }
  option.onchange=select;qty.oninput=updateTotal;
  place.onclick=async()=>{
   if(busy||!selected||!canLeave())return;busy=true;place.disabled=true;
   try{
    const {route,fixture}=selected,n=Number(qty.value),existing=projectRows.find(r=>r.value.plan.id===destination.value);
    const plan=existing?addRoomUnits(existing.value.plan,route.catalogue,fixture.id,n):newRoomPlan(route.catalogue,fixture.id,n);
    // Model bodies stay outside the plan and are written only once per source scope.
    if(scene.inspect().models.length)await scene.saveBinding();
    const row=await saveLocal('fixture-plan:'+route.form+':'+plan.id,{plan,receipt:null},existing?.revision??0);
    // Navigation follows the retained row; a failed latest shortcut cannot lose it.
    const target=new URL(route.path,location.origin);target.searchParams.set('project',row.value.plan.id);target.searchParams.set('scene','room');
    location.assign(target.href);
   }catch(error){say(error.message);onError(error.message);}finally{busy=false;place.disabled=false;}
  };
  await select();
 }
 function close(){generation++;scene?.destroy();scene=null;dialog?.close();dialog?.remove();dialog=null;selected=null;}
 return {open,close};
}
