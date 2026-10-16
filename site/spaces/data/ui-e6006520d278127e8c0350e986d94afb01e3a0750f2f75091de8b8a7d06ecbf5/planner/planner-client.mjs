import {createEditHistory} from '../../../shared/interaction-222ab2b3f0cb0334bca5/edit-history.mjs';
import {createPlannerProductScene} from './planner-product-scene.mjs';
import {startingPlan,validatePlan,assessPlan,money,planBrief,planRequest} from './planner-model.mjs';
import {readLocal,saveLocal} from './store.mjs';
import {copyPlan} from './planner-projects.mjs';
import {bindProjects} from './planner-project-client.mjs';
import {createSpatialEditor} from './planner-spatial.mjs';

const root=document.querySelector('[data-planner]');
const cfg=JSON.parse(root.querySelector('[data-planner-config]').textContent),catalogue=cfg.catalogue;
const ui=name=>root.querySelector(`[data-ui="${name}"]`);
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const product=id=>catalogue.products.find(p=>p.id===id);
let projectWorkspace, spatial, productScene, lastValid;
const planHistory=createEditHistory({capture:()=>lastValid??plan,restore:value=>{plan=value;lastValid=structuredClone(value);},limit:60});
let plan,revision=0,saved=null,selected=null,receipt=null,busy=false,conflict=false,storageAvailable=true;
const prefix='fixture-plan:'+cfg.form+':';
const key=()=>prefix+plan.id;
const error=message=>{ui('error').hidden=!message;ui('error').textContent=message||'';};
const message=text=>{ui('message').textContent=text;};
const unchanged=()=>saved===JSON.stringify(plan);
const valid=()=>{try{validatePlan(plan,catalogue);return true;}catch(e){error(e.message);return false;}};
function controls() {
  for(const node of root.querySelectorAll('input,textarea,[data-add],[data-preset],[data-ui="rotate"],[data-ui="remove"],[data-ui="duplicate-project"],[data-ui="new-project"]'))node.disabled=busy;
  ui('save').disabled=busy||conflict||!storageAvailable;
  ui('undo').disabled=busy||!planHistory.canUndo;ui('redo').disabled=busy||!planHistory.canRedo;
  spatial?.refresh();
  ui('submit').disabled=busy||conflict||!unchanged()||Boolean(receipt);
  ui('save-state').textContent=conflict?'Changed in another tab. Reload to continue.':!storageAvailable?'Local saving unavailable':unchanged()?'Saved in this browser':'Changes not saved';
  ui('receipt').textContent=receipt?`Received by Aurekai. Reference: ${receipt}`:unchanged()?'Your saved plan is ready to send.':'Save the plan before sending.';
}
function changed() {receipt=null;error('');message('');if(valid()){if(lastValid)planHistory.transact(()=>{lastValid=structuredClone(plan);});render();}controls();}
function editHistory(direction){if(busy)return;if(!planHistory[direction]())return;receipt=null;error('');fields();render();positionEditor();}
function spatialChange(next,primary=selected){if(busy)return;try{validatePlan(next,catalogue);plan=next;selected=primary;changed();positionEditor();}catch(e){error(e.message);}}
ui('undo').onclick=()=>editHistory('undo');ui('redo').onclick=()=>editHistory('redo');
root.addEventListener('keydown',event=>{if(event.target.closest('input,textarea,select'))return;if((event.metaKey||event.ctrlKey)&&event.key.toLowerCase()==='z'){event.preventDefault();editHistory(event.shiftKey?'redo':'undo');}});
function fields() {
  for(const name of ['title','width','depth','deadline','notes'])ui(name).value=plan[name];
  ui('budget').value=plan.budget===null?'':(plan.budget/100).toFixed(2);
}
function readFields() {
  plan.title=ui('title').value;plan.width=Number(ui('width').value);plan.depth=Number(ui('depth').value);plan.deadline=ui('deadline').value;plan.notes=ui('notes').value;
  const b=ui('budget').value;
  plan.budget=b===''?null:/^\d+(?:\.\d{1,2})?$/.test(b)?Math.round(Number(b)*100):NaN;
  changed();
}
function positionEditor() {
  const item=plan.items.find(i=>i.key===selected);ui('position').hidden=!item;
  if(!item)return;
  ui('selected-name').textContent=product(item.product).label;
  ui('x').value=item.x;ui('y').value=item.y;
}
function select(key) {selected=key;render();positionEditor();}
function canvas() {
  const x0=60,y0=40,scale=Math.min(610/plan.width,440/plan.depth),w=plan.width*scale,h=plan.depth*scale;
  const grid=[];for(let i=1;i<plan.width;i++)grid.push(`<path d="M${x0+i*scale} ${y0}v${h}"/>`);for(let i=1;i<plan.depth;i++)grid.push(`<path d="M${x0} ${y0+i*scale}h${w}"/>`);
  let content=`<rect x="${x0}" y="${y0}" width="${w}" height="${h}" fill="#fffef9" stroke="#8c9685" stroke-width="2"/><g stroke="#e6e7dd" stroke-width="1">${grid.join('')}</g><text x="${x0+w/2}" y="${y0-13}" text-anchor="middle" fill="#676a64" font-size="13">${plan.width} ft</text><text x="20" y="${y0+h/2}" fill="#676a64" font-size="13" transform="rotate(-90 20 ${y0+h/2})" text-anchor="middle">${plan.depth} ft</text><path d="M${x0+w*.4} ${y0+h}h${w*.2}" stroke="#fffef9" stroke-width="5"/><text x="${x0+w/2}" y="${y0+h+25}" text-anchor="middle" fill="#676a64" font-size="11">FRONT / ACCESS TO CONFIRM</text>`;
  for(const [index,item] of plan.items.entries()){
    const p=product(item.product),known=p.dimensions?.depth&&p.dimensions?.width,dim=p.allowance;
    const px=x0+item.x*scale,py=y0+item.y*scale;
    const dw=dim?(item.rotation?dim.depth:dim.width)/12*scale:18,dh=dim?(item.rotation?dim.width:dim.depth)/12*scale:18;
    const stroke=item.key===selected?'#b5282c':known?'#62765f':'#a27b34';
    content+=`<g data-item="${esc(item.key)}" role="button" tabindex="0" aria-label="Move ${esc(p.label)}" transform="translate(${px},${py})"><rect width="${dw}" height="${dh}" rx="${dim?3:9}" fill="${known?'#d5dfce':dim?'#f2e7ce':'#c75a3d'}" stroke="${stroke}" stroke-width="${item.key===selected?3:1.5}" ${dim&&!known?'stroke-dasharray="5 4"':''}/><text x="${dw/2}" y="${dh/2+4}" text-anchor="middle" font-size="12" font-weight="600" fill="${dim?'#344334':'#fff'}">${index+1}</text>${dim?`<text x="${dw/2}" y="${dh+15}" font-size="10" fill="#454d40" text-anchor="middle">${esc(p.use)}</text>`:''}</g>`;
  }
  ui('canvas').innerHTML=content;spatial?.update(plan,selected);productScene?.update(plan,selected);
}
function render() {
  const a=assessPlan(plan,catalogue);
  ui('room-label').textContent=`${plan.width} × ${plan.depth} ft`;
  ui('selection').innerHTML=a.lines.map((i,n)=>`<button type="button" data-select="${esc(i.key)}" aria-pressed="${i.key===selected}"><img src="${esc(i.product.image)}" alt="" referrerpolicy="no-referrer"><span><b>${n+1}. ${esc(i.product.label)}</b><small>${money(i.product.price_cents)} · ${esc(i.product.option)}</small></span></button>`).join('');
  ui('subtotal').textContent=money(a.subtotal);
  ui('budget-note').textContent=plan.budget===null?'Add your budget to compare.':a.subtotal>plan.budget?`${money(a.subtotal-plan.budget)} above your fixture budget.`:`${money(plan.budget-a.subtotal)} remains before delivery and tax.`;
  ui('issues').innerHTML=a.issues.map(s=>`<li>${esc(s)}</li>`).join('');
  ui('brief').textContent=planBrief(plan,catalogue);canvas();lastValid=structuredClone(plan);controls();
}
async function save() {
  if(busy||!valid()||conflict||!storageAvailable)return false;
  busy=true;controls();
  try {const row=await saveLocal(key(),{plan,receipt:unchanged()?receipt:null},revision);revision=row.revision;saved=JSON.stringify(plan);try{await saveLocal(prefix+'latest',{id:plan.id});}catch{}
    try{await projectWorkspace?.refresh();}catch{}history.replaceState(null,'',projectURL());message('Plan saved on this device.');controls();return true;}
  catch(e){if(e.message.includes('another tab'))conflict=true;error(e.message);controls();return false;}
  finally{busy=false;controls();}
}
function encoded(plan) {const bytes=new TextEncoder().encode(JSON.stringify(plan));return btoa(String.fromCharCode(...bytes)).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');}
function decoded(hash) {if(hash.length>22000)throw Error('This plan link is too long.');const raw=atob(hash.replace(/-/g,'+').replace(/_/g,'/'));return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(Uint8Array.from(raw,c=>c.charCodeAt(0))));}
function download() {const link=document.createElement('a'),url=URL.createObjectURL(new Blob([planBrief(plan,catalogue)],{type:'text/plain;charset=utf-8'}));link.href=url;link.download='fixture-project-brief.txt';link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}

ui('details').addEventListener('submit',e=>e.preventDefault());ui('details').addEventListener('input',readFields);
ui('save').addEventListener('click',save);
root.addEventListener('click',e=>{
  if(busy)return;
  const b=e.target.closest('[data-select],[data-item],[data-add],[data-preset]');if(!b)return;
  if(b.dataset.select||b.dataset.item){const key=b.dataset.select||b.dataset.item;if(spatial)spatial.select(key,e.shiftKey||e.metaKey);else select(key);return;}
  if(b.dataset.add){if(plan.items.length>=12){error('This plan holds up to twelve fixtures. Remove one before adding another.');return;}const item={key:crypto.randomUUID(),product:b.dataset.add,x:1,y:1,rotation:0};plan.items.push(item);selected=item.key;changed();positionEditor();message('Fixture added. Drag it into position.');}
  if(b.dataset.preset){if(!confirm('Replace this fixture arrangement? Your project details will stay.'))return;plan.items=startingPlan(catalogue,b.dataset.preset).items;selected=null;changed();positionEditor();}
});
ui('canvas').addEventListener('keydown',e=>{if(busy)return;const el=e.target.closest('[data-item]');if(!el)return;if(['Enter',' '].includes(e.key)){e.preventDefault();select(el.dataset.item);return;}const move={ArrowLeft:[-.25,0],ArrowRight:[.25,0],ArrowUp:[0,-.25],ArrowDown:[0,.25]}[e.key];if(!move)return;e.preventDefault();const item=plan.items.find(i=>i.key===el.dataset.item);selected=item.key;item.x=Math.max(0,Math.min(60,item.x+move[0]));item.y=Math.max(0,Math.min(60,item.y+move[1]));changed();positionEditor();ui('canvas').querySelector(`[data-item="${CSS.escape(selected)}"]`)?.focus();});
for(const axis of ['x','y'])ui(axis).addEventListener('input',()=>{const item=plan.items.find(i=>i.key===selected);if(item){item[axis]=Number(ui(axis).value);changed();}});
ui('rotate').addEventListener('click',()=>{const item=plan.items.find(i=>i.key===selected);if(item){item.rotation=item.rotation===90?0:90;changed();}});
ui('remove').addEventListener('click',()=>{if(plan.items.length<=1){error('Keep at least one fixture in the plan.');return;}plan.items=plan.items.filter(i=>i.key!==selected);selected=null;changed();positionEditor();});
ui('share').addEventListener('click',async()=>{
  if(!valid())return;const url=new URL(location.href);url.hash='plan='+encoded(plan);ui('share-box').hidden=false;ui('share-url').value=url.href;
  try {await navigator.clipboard.writeText(url.href);message('Link copied. Anyone with it can open a copy of this layout and its notes.');}catch{ui('share-url').select();message('Copy the selected link to share this plan.');}
});
ui('review').addEventListener('click',()=>{if(!valid())return;ui('brief').textContent=planBrief(plan,catalogue);controls();ui('review-dialog').showModal();});
ui('close-review').addEventListener('click',()=>ui('review-dialog').close());
ui('download').addEventListener('click',download);ui('print').addEventListener('click',()=>window.print());
ui('submit-form').addEventListener('submit',async e=>{
  e.preventDefault();if(busy||!unchanged()||receipt||!valid())return;
  busy=true;controls();ui('receipt').textContent='Sending your saved plan…';
  try{
    const body=planRequest(plan,catalogue,{form:cfg.form,page:cfg.page,name:ui('name').value,email:ui('email').value});
    throw Error('Native review is not connected. Download the brief for review.');const response=await fetch('/api/lead/'+cfg.form,{method:'POST',headers:{'content-type':'application/json'},body});
    const result=await response.json();if(!response.ok||!result.ok||!/^lead:planner:[a-f0-9]{64}$/.test(result.receipt))throw Error(result.why||'No receipt was returned. Your saved plan can be retried.');
    receipt=result.receipt;
    try{const row=await saveLocal(key(),{plan,receipt},revision);revision=row.revision;controls();}catch{controls();ui('receipt').textContent=`Received by Aurekai. Reference: ${receipt}. This browser could not save the receipt; keep this reference.`;}
  }catch(e){controls();ui('receipt').textContent=`Could not send: ${e.message}`;}finally{busy=false;ui('submit').disabled=!unchanged()||Boolean(receipt)||conflict;}
});

function projectURL(){const url=new URL(location.href);url.searchParams.set('project',plan.id);return url;}
function replacePlan(next){planHistory.clear();lastValid=null;plan=next;revision=0;saved=null;receipt=null;conflict=false;selected=null;error('');fields();render();positionEditor();}
async function boot() {
  ui('products').innerHTML=catalogue.products.map(p=>`<article class="fp-product"><div class="fp-photo"><img src="${esc(p.image)}" alt="${esc(p.label)}" referrerpolicy="no-referrer"></div><div class="fp-product-body"><span class="fp-use">${esc(p.use)}</span><h3>${esc(p.label)}</h3><p>${esc(p.option)}</p><p class="fp-photo-note">Listing photo may show other options.</p><p>${p.dimensions?.depth?`${p.dimensions.width}″ × ${p.dimensions.depth}″ footprint`:p.dimensions?`${p.dimensions.width}″ wide · depth to confirm`:'Countertop size to confirm'}</p><p class="fp-price">${money(p.price_cents)}</p><a href="${esc(p.source_url)}" target="_blank" rel="noopener noreferrer">Listing observed ${esc(p.observed_at.slice(0,10))} ↗</a><button type="button" data-add="${esc(p.id)}">+ Add to plan</button></div></article>`).join('');
  plan=startingPlan(catalogue);
  if(location.hash.startsWith('#plan=')){
    try{const incoming=decoded(location.hash.slice(6));validatePlan(incoming,catalogue);plan={...incoming,id:crypto.randomUUID()};message('You opened a shared copy. Changes you make stay in your own plan.');history.replaceState(null,'',location.pathname+location.search);}catch(e){error('Could not open the shared plan. '+e.message);}
  }else{
    try{const rows=await readLocal(prefix),last=rows.find(r=>r.key===prefix+'latest');const requested=new URL(location.href).searchParams.get('project');const kept=rows.find(r=>r.key===prefix+(requested||last?.value.id));if(requested&&!kept)message('This project is not saved on this browser. A new plan is open.');if(kept){validatePlan(kept.value.plan,catalogue);plan=kept.value.plan;revision=kept.revision;saved=JSON.stringify(plan);receipt=/^lead:planner:[a-f0-9]{64}$/.test(kept.value.receipt)?kept.value.receipt:null;}}
    catch(e){storageAvailable=false;error('Could not reopen the saved plan. You can still explore and download a brief. '+e.message);}
  }
  fields();render();positionEditor();
  projectWorkspace=bindProjects({root,catalogue,prefix,current:()=>({id:plan.id,unsaved:!unchanged()}),open:row=>{
    planHistory.clear();lastValid=null;plan=structuredClone(row.value.plan);revision=row.revision;saved=JSON.stringify(plan);receipt=/^lead:planner:[a-f0-9]{64}$/.test(row.value.receipt??'')?row.value.receipt:null;conflict=false;selected=null;error('');message('');fields();render();positionEditor();
  },create:()=>replacePlan(startingPlan(catalogue)),copy:()=>{if(!valid())throw Error('Correct the plan before making a copy.');replacePlan(copyPlan(plan,catalogue));message('This is a new option. Save it to keep both plans.');},
  canLeave:()=>unchanged()||confirm('Leave this plan without saving its changes?'),isBusy:()=>busy,fail:error});
  try{await projectWorkspace.init();}catch{error('Saved projects could not be opened. You can still explore and download a brief.');}
  root.dataset.ready='true';
}
spatial=createSpatialEditor({root,catalogue,onSelect:select,onChange:spatialChange,canEdit:()=>{try{return !busy&&Boolean(validatePlan(plan,catalogue));}catch{return false;}},fail:error});
productScene=createPlannerProductScene({root,catalogue,models:cfg.sceneModels??{},onSelect:select,onChange:spatialChange,canEdit:()=>{try{return !busy&&!conflict&&Boolean(validatePlan(plan,catalogue));}catch{return false;}},fail:error});
boot().catch(e=>error('The planner could not open. '+e.message));
