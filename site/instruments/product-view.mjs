import {el,button,field,modal,datatable,download,filePicker} from './ui.mjs';
import {number,safeURL} from './data.mjs';
import {initialProduct} from './examples.mjs';
import {analyzePlan,suggestPlan,importStories,exportStories,rice,validatePlan} from './product.mjs';
export async function mount(ctx,initial){
 let state=initial??{kind:'product',...structuredClone(initialProduct),selected:'source'},filter='';validatePlan(state);
 const chosen=()=>state.stories.find(s=>s.id===state.selected);
 function select(id){state.selected=id;ctx.select(id);renderInspector();for(const n of ctx.main.querySelectorAll('[data-story]'))n.classList.toggle('selected',n.dataset.story===id);}
 function update(s,key,value){try{const copy=structuredClone(state),target=copy.stories.find(x=>x.id===s.id);target[key]=value;validatePlan(copy);ctx.commit(()=>Object.assign(s,target));renderBoard();}catch(error){ctx.status(error.message,true);renderInspector();}}
 function renderInspector(){
  const s=chosen();ctx.inspector.replaceChildren(el('h3',{text:s?'Story details':'Select a story'}));if(!s)return;
  const changes=(key,control,read)=>{control.onchange=ev=>update(s,key,read?read(ev.target.value):ev.target.value);return control;};
  const release=el('select',{},...['Unscheduled',...state.releases.map(r=>r.name)].map(name=>el('option',{value:name,text:name})));release.value=s.release;
  ctx.inspector.append(el('span',{class:'badge',text:s.id}),field('Outcome / story',changes('title',el('textarea',{value:s.title}))),field('Activity',changes('epic',el('input',{value:s.epic}))),field('Release',changes('release',release)));
  const nums=el('div',{class:'inline-fields'});
  for(const [key,label]of [['reach','Estimated reach'],['impact','Impact estimate'],['confidence','Confidence %'],['effort','Effort units']]){
   const control=el('input',{type:'number',value:s[key],min:key==='effort'?'.001':'0',step:'any'});
   control.onchange=ev=>{try{update(s,key,number(ev.target.value,label,{min:key==='effort'?.001:0,max:key==='confidence'?100:1e9}));ctx.inspector.querySelector('#rice-output').textContent='RICE '+rice(chosen()).toFixed(2);}catch(error){ctx.status(error.message,true);renderInspector();}};
   nums.append(field(label,control));
  }
  ctx.inspector.append(nums,el('p',{class:'readout',id:'rice-output',text:'RICE '+rice(s).toFixed(2)}),el('p',{class:'fine muted',text:'These are editable estimates. A ranking is not verified demand or a delivery prediction.'}));
  const deps=el('select',{multiple:true,size:Math.min(6,Math.max(2,state.stories.length-1))},...state.stories.filter(x=>x.id!==s.id).map(x=>el('option',{value:x.id,selected:s.requires.includes(x.id),text:x.title})));
  deps.onchange=()=>update(s,'requires',[...deps.selectedOptions].map(o=>o.value));
  ctx.inspector.append(field('Prerequisites (multiple selection)',deps),field('Acceptance criteria — one per line',changes('criteria',el('textarea',{value:s.criteria.join('\n')}),v=>v.split('\n').filter(Boolean))));
  if(s.evidence?.length){ctx.inspector.append(el('h3',{text:'Retained research passages'}));for(const ref of s.evidence){const block=el('blockquote',{class:'quote',text:ref.quote});if(safeURL(ref.sourceURL))block.append(el('small',{},el('a',{href:safeURL(ref.sourceURL),target:'_blank',rel:'noopener noreferrer',text:ref.sourceTitle||'Original source'})));ctx.inspector.append(block);}ctx.inspector.append(el('p',{class:'fine muted',text:'Pinned passages accompany this opportunity. They do not set reach, effort or confidence.'}));}
  ctx.inspector.append(button('Remove story',()=>{if(state.stories.some(x=>x.requires.includes(s.id))){ctx.status('Another story depends on this one. Resolve that dependency first.',true);return;}ctx.commit(()=>{state.stories=state.stories.filter(x=>x.id!==s.id);state.selected=state.stories[0]?.id;});render();}));
 }
 function render(){
  ctx.main.replaceChildren(el('div',{class:'toolbar'},button('Add story',addStory),button('Import CSV',()=>filePicker('.csv',importCSV)),button('Release capacities',capacityDialog),button('Preview capacity proposal',proposal),button('Research inbox',inbox),el('span',{class:'spacer'}),el('input',{placeholder:'Filter stories',value:filter,'aria-label':'Filter stories',oninput:ev=>{filter=ev.target.value;renderBoard();}})),el('div',{id:'plan-conflicts'}),el('section',{class:'panel story-board',id:'story-board'}));renderBoard();renderInspector();
 }
 async function importCSV(file){try{if(file.size>2*1024*1024)throw Error('Choose a smaller CSV.');const stories=importStories(await file.text()),copy={...state,stories};validatePlan(copy);const d=modal('Replace working story collection?',el('p',{text:`${stories.length} imported stories will replace this working collection. The saved revision remains unchanged until Save.`}),[button('Use these stories',()=>{validatePlan({...state,stories});ctx.commit(()=>{state.stories=stories;state.selected=stories[0]?.id;});d.close();render();},'primary')]);}catch(error){ctx.status(error.message,true);}}
 function renderBoard(){
  const target=ctx.main.querySelector('#story-board');if(!target)return;
  const result=analyzePlan(state),conflicts=ctx.main.querySelector('#plan-conflicts');conflicts.replaceChildren(...result.problems.map(p=>el('div',{class:'conflict',text:p.message})));
  const releases=['Unscheduled',...state.releases.map(r=>r.name)],epics=[...new Set(state.stories.map(s=>s.epic))],grid=el('div',{class:'story-grid'});grid.style.gridTemplateColumns=`130px repeat(${releases.length},minmax(180px,1fr))`;grid.append(el('div',{class:'story-head',text:'USER ACTIVITY'}));
  for(const name of releases){const load=result.loads.find(r=>r.name===name);grid.append(el('div',{class:'story-head'},name,el('small',{class:'count',text:load?`${load.used} / ${load.capacity} effort`:'No commitment'})));}
  for(const epic of epics){grid.append(el('div',{class:'story-lane',text:epic}));for(const release of releases){
   const cell=el('div',{class:'story-cell','data-release':release,'data-epic':epic});cell.ondragover=ev=>{ev.preventDefault();cell.classList.add('over');};cell.ondragleave=()=>cell.classList.remove('over');
   cell.ondrop=ev=>{ev.preventDefault();cell.classList.remove('over');const id=ev.dataTransfer.getData('application/x-aurekai-story'),s=state.stories.find(x=>x.id===id);if(!s)return;ctx.commit(()=>{s.release=release;s.epic=epic;state.selected=id;});renderBoard();renderInspector();};
   for(const s of state.stories.filter(s=>s.epic===epic&&s.release===release&&(s.title+' '+s.id).toLowerCase().includes(filter.toLowerCase()))){
    const card=el('article',{class:'story-card'+(s.id===state.selected?' selected':''),draggable:true,tabIndex:0,'data-story':s.id,onclick:()=>select(s.id),onkeydown:ev=>{if(ev.key==='Enter'){select(s.id);ctx.inspector.querySelector('textarea')?.focus();}}},el('div',{class:'story-meta'},s.id,el('span',{text:s.effort+' effort'})),el('p',{text:s.title}),el('div',{class:'story-meta'},'RICE '+rice(s).toFixed(1),s.requires.length?'↳ '+s.requires.length+' prerequisite':''));
    card.ondragstart=ev=>ev.dataTransfer.setData('application/x-aurekai-story',s.id);cell.append(card);
   }grid.append(cell);
  }}target.replaceChildren(grid);if(!epics.length)target.append(el('div',{class:'empty',text:'Add or import the first story.'}));
 }
 function addStory(){const title=el('input'),epic=el('input',{value:'New activity'});const d=modal('Add a story',el('div',{},field('Outcome',title),field('Activity',epic)),[button('Add',()=>{try{if(!title.value.trim())throw Error('Write an outcome.');const s={id:crypto.randomUUID(),title:title.value,epic:epic.value||'New activity',reach:0,impact:0,confidence:0,effort:1,release:'Unscheduled',requires:[],criteria:[],evidence:[]};validatePlan({...state,stories:[...state.stories,s]});ctx.commit(()=>{state.stories.push(s);state.selected=s.id;});d.close();render();}catch(error){ctx.status(error.message,true);}},'primary')]);}
 function capacityDialog(){
  const copy=structuredClone(state.releases),body=el('div'),rows=el('div');body.append(rows);
  const redraw=()=>{rows.replaceChildren();for(const r of copy){const input=el('input',{type:'number',min:0,step:'any',value:r.capacity,onchange:ev=>{try{r.capacity=number(ev.target.value,'Capacity',{min:0,max:100000});}catch(error){ev.target.value=r.capacity;ctx.status(error.message,true);}}});rows.append(field(r.name+' capacity',input));}};
  redraw();const name=el('input',{placeholder:'Additional release name'}),error=el('p',{class:'conflict',hidden:true});body.append(field('New release',name),button('Add release',()=>{if(copy.length<24&&name.value.trim()&&!copy.some(r=>r.name===name.value.trim())){copy.push({name:name.value.trim(),capacity:0});name.value='';redraw();}}));
  const d=modal('Release capacities',el('div',{},body,error),[button('Apply capacities',()=>{try{validatePlan({...state,releases:copy});ctx.commit(()=>state.releases=copy);d.close();render();}catch(e){error.hidden=false;error.textContent=e.message;}},'primary')]);
 }
 function proposal(){try{const before=state.stories,proposal=suggestPlan(state),rows=proposal.plan.stories.map(s=>[s.title,state.stories.find(x=>x.id===s.id).release,s.release,s.effort]);const d=modal('Review the proposed allocation',el('div',{},el('p',{text:proposal.method}),el('p',{text:proposal.unassigned.length+' stories remain unassigned.'}),datatable(['Story','Current','Proposed','Effort'],rows)),[button('Apply reviewed proposal',()=>{if(before!==state.stories){ctx.status('Working stories changed. Preview the allocation again.',true);return;}ctx.commit(()=>state.stories=proposal.plan.stories);d.close();render();},'primary')]);}catch(error){ctx.status(error.message,true);}}
 async function inbox(){try{const rows=await ctx.inbox(),body=el('div');for(const row of rows){const exists=state.stories.some(s=>s.id===row.value.candidate.id),b=button(exists?'Already in this map':'Add to this map',()=>{try{const s=structuredClone(row.value.candidate);validatePlan({...state,stories:[...state.stories,s]});ctx.commit(()=>{state.stories.push(s);state.selected=s.id;});d.close();render();}catch(e){ctx.status(e.message,true);}});b.disabled=exists;body.append(el('article',{class:'finding'},el('p',{text:row.value.candidate.title}),el('p',{class:'fine muted',text:row.value.candidate.researchTitle}),b));}if(!rows.length)body.append(el('p',{text:'In Research editor, select a sourced finding and choose “Offer as product opportunity”. Its saved source context will appear here.'}));const d=modal('Research opportunities',body);}catch(error){ctx.status(error.message,true);}}
 render();return {title:'Product story map',subtitle:'Arrange outcomes, inspect dependencies and review capacity before committing a plan.',capture:()=>structuredClone(state),restore:s=>{validatePlan(s);state=s;render();},render,export:()=>{const r=analyzePlan(state);if(r.problems.length)throw Error('Resolve the displayed capacity and dependency conflicts before exporting this plan.');download('product-plan.csv',exportStories(state),'text/csv');},acceptSelection:id=>state.stories.some(s=>s.id===id)};
}
