import {el,button,field,datatable,download,filePicker,workerRPC} from './ui.mjs';
import {migrationSQL} from './schema.mjs';
export async function mount(ctx,initial){
 let state=initial??{kind:'schema',title:'Working database',bytes:null,selected:'tasks',positions:{},acceptedSQL:[]},snapshot=null,pending=null,busy=false,generation=0;
 const rpc=workerRPC('./sqlite-worker.js',{timeout:20000});
 async function inspect(){const token=++generation,source=state;busy=true;ctx.refresh();try{const r=await rpc.call(source.bytes?'inspect':'example',{input:source.bytes});if(token!==generation||source!==state)return;snapshot=r.state;state.bytes=r.bytes;if(!snapshot.tables.some(t=>t.name===state.selected))state.selected=snapshot.tables[0]?.name;render();ctx.status('SQLite '+snapshot.version+' inspected the actual database. Changes operate on a working copy.');}finally{if(token===generation){busy=false;ctx.refresh();}}}
 const table=()=>snapshot.tables.find(t=>t.name===state.selected);
 function select(name){state.selected=name;ctx.select(name);renderInspector();renderRows();for(const n of ctx.main.querySelectorAll('[data-table]'))n.classList.toggle('active',n.dataset.table===name);}
 function positions(){snapshot.tables.forEach((t,i)=>state.positions[t.name]??={x:40+i%4*265,y:35+Math.floor(i/4)*230});}
 function lines(svg){svg.replaceChildren();for(const t of snapshot.tables)for(const fk of t.foreignKeys){const a=state.positions[t.name],b=state.positions[fk.table];if(!b)continue;const p=document.createElementNS('http://www.w3.org/2000/svg','path'),x1=a.x+215,y1=a.y+55,x2=b.x,y2=b.y+55;p.setAttribute('d',`M${x1},${y1} C${x1+65},${y1} ${x2-65},${y2} ${x2},${y2}`);const title=document.createElementNS('http://www.w3.org/2000/svg','title');title.textContent=`${t.name}.${fk.from} → ${fk.table}.${fk.to}`;p.append(title);svg.append(p);}}
 async function openFile(file){if(busy){ctx.status('Finish the current database operation first.',true);return;}try{if(file.size>16*1024*1024)throw Error('Choose a database up to 16 MiB.');busy=true;ctx.refresh();const input=new Uint8Array(await file.arrayBuffer()),r=await rpc.call('inspect',{input});ctx.commit(()=>{state.bytes=r.bytes;state.title=file.name;state.selected=r.state.tables[0]?.name;state.acceptedSQL=[];state.positions={};snapshot=r.state;pending=null;});render();}catch(e){ctx.status(e.message,true);}finally{busy=false;ctx.refresh();}}
 function render(){
  if(!snapshot)return;positions();
  ctx.main.replaceChildren(el('div',{class:'toolbar'},button('Open SQLite database',()=>filePicker('.sqlite,.sqlite3,.db',openFile)),button('Export accepted SQL',()=>download('accepted-changes.sql',state.acceptedSQL.join('\n'))),el('span',{class:'spacer'}),el('span',{class:'badge',text:'SQLite '+snapshot.version+' · '+state.bytes.byteLength.toLocaleString()+' bytes'})));
  const inner=el('div',{class:'schema-inner'}),svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.setAttribute('class','schema-links');svg.setAttribute('width','1100');svg.setAttribute('height','760');inner.append(svg);
  for(const t of snapshot.tables){
   const p=state.positions[t.name],card=el('article',{class:'schema-table'+(t.name===state.selected?' active':''),'data-table':t.name});
   const head=el('div',{class:'schema-title',tabIndex:0,text:t.name+' · '+t.rows+' rows',onkeydown:ev=>{if(ev.key==='Enter'){select(t.name);ctx.inspector.querySelector('select')?.focus();}if(['ArrowUp','ArrowDown','ArrowLeft','ArrowRight'].includes(ev.key)){ev.preventDefault();p.x=Math.max(0,Math.min(850,p.x+({'ArrowLeft':-10,'ArrowRight':10}[ev.key]||0)));p.y=Math.max(0,Math.min(650,p.y+({'ArrowUp':-10,'ArrowDown':10}[ev.key]||0)));card.style.left=p.x+'px';card.style.top=p.y+'px';lines(svg);}}});
   card.style.left=p.x+'px';card.style.top=p.y+'px';card.append(head);
   for(const c of t.columns)card.append(el('div',{class:'schema-column'},button((c.pk?'◆ ':'')+c.name,()=>{select(t.name);const control=ctx.inspector.querySelector('#source-column');control.value=c.name;control.dispatchEvent(new Event('input',{bubbles:true}));}),el('span',{class:'muted',text:c.type+(c.notnull?' !':'')})));
   card.onclick=event=>{if(!event.target.closest('button'))select(t.name);};
   head.onpointerdown=ev=>{if(ev.button!==0)return;ev.preventDefault();select(t.name);head.setPointerCapture(ev.pointerId);const ox=ev.clientX-p.x,oy=ev.clientY-p.y;const move=e=>{p.x=Math.max(0,Math.min(850,e.clientX-ox));p.y=Math.max(0,Math.min(650,e.clientY-oy));card.style.left=p.x+'px';card.style.top=p.y+'px';lines(svg);};const end=()=>{head.removeEventListener('pointermove',move);head.removeEventListener('pointerup',end);head.removeEventListener('pointercancel',end);};head.addEventListener('pointermove',move);head.addEventListener('pointerup',end);head.addEventListener('pointercancel',end);};inner.append(card);
  }
  lines(svg);ctx.main.append(el('section',{class:'panel'},el('div',{class:'panel-head'},el('h2',{text:state.title}),el('span',{class:'fine muted',text:'Drag table headers · select columns · original file remains untouched'})),el('div',{class:'schema-canvas'},inner)),el('section',{class:'panel',id:'row-preview'}));renderRows();renderInspector();
 }
 function renderRows(){
  const t=table(),target=ctx.main.querySelector('#row-preview');if(!target)return;target.replaceChildren();if(!t)return;
  const cells=t.sample.map(row=>t.columns.map(c=>{const value=row[c.name];return value instanceof Uint8Array?'BLOB '+value.byteLength+' bytes':value===null?'NULL':String(value);}));
  target.append(el('div',{class:'panel-head'},el('h3',{text:t.name+' · actual row preview'}),el('span',{class:'count',text:'Up to 30 rows'})),datatable(t.columns.map(c=>c.name),cells));
 }
 function renderInspector(){
  const t=table();ctx.inspector.replaceChildren(el('h3',{text:'Rehearse a schema change'}));if(!t){ctx.inspector.append(el('p',{text:'This database has no ordinary tables.'}));return;}
  const kind=el('select',{},...['add-column','rename-column','rename-table','create-index'].map(v=>el('option',{value:v,text:v.replaceAll('-',' ')}))),column=el('select',{id:'source-column'},...t.columns.map(c=>el('option',{value:c.name,text:c.name}))),name=el('input',{placeholder:'New name'}),type=el('select',{},...['TEXT','INTEGER','REAL','BLOB','NUMERIC'].map(v=>el('option',{value:v,text:v}))),notNull=el('input',{type:'checkbox'}),defaultValue=el('input',{placeholder:'Leave blank for no default'}),unique=el('input',{type:'checkbox'}),sql=el('pre',{class:'code',text:'Choose an operation and a new name.'}),result=el('div');
  ctx.inspector.append(el('p',{class:'fine muted',text:'SQLite executes the generated change against a separate copy, checks integrity and foreign keys, and returns the resulting database.'}),field('Operation',kind),field('Source column',column),field('New column, table or index name',name),field('Added column type',type),field('Required added column',notNull),field('Added column default',defaultValue),field('Unique index',unique),sql);
  const operation=()=>({kind:kind.value,table:t.name,column:column.value,name:name.value,type:type.value,notNull:notNull.checked,unique:unique.checked,defaultValue:defaultValue.value===''?null:defaultValue.value});
  let proposalVersion=0;const preview=()=>{proposalVersion++;pending=null;result.replaceChildren();try{sql.textContent=migrationSQL(snapshot,operation());}catch(e){sql.textContent=e.message;}};
  for(const n of [kind,column,name,type,notNull,defaultValue,unique])n.addEventListener('input',preview);
  const run=button('Run on isolated working copy',async()=>{
   if(busy){ctx.status('Another database operation is running.',true);return;}
   try{
    const op=operation(),original=state.bytes,version=proposalVersion;sql.textContent=migrationSQL(snapshot,op);busy=true;ctx.refresh();run.disabled=true;ctx.status('Executing real SQLite migration…');
    const r=await rpc.call('migrate',{input:original,operation:op});
    if(original!==state.bytes||version!==proposalVersion||!run.isConnected)throw Error('The input database or operation changed. Result not adopted.');
    pending={...r,source:original};
    const counts=r.state.tables.map(nt=>[nt.name,r.before.tables.find(bt=>bt.name===nt.name)?.rows??(op.kind==='rename-table'&&nt.name===op.name?r.before.tables.find(bt=>bt.name===op.table)?.rows:'—'),nt.rows]);
    result.replaceChildren(el('h3',{text:'Execution succeeded'}),el('p',{class:'fine',text:'Integrity: ok · Foreign keys: no violations'}),datatable(['Table','Rows before','Rows after'],counts),button('Use this tested database',()=>{if(!pending||pending.source!==state.bytes){ctx.status('Rehearsal is stale. Run it again.',true);return;}const accepted=pending;ctx.commit(()=>{state.bytes=accepted.bytes;state.acceptedSQL.push(accepted.sql);snapshot=accepted.state;if(op.kind==='rename-table')state.selected=op.name;pending=null;});render();ctx.status('Accepted into this working copy. Save or export to retain it. No external database changed.');},'primary'));
    ctx.status('Migration executed; original input preserved until you accept.');
   }catch(e){result.replaceChildren(el('p',{class:'conflict',text:e.message}));ctx.status('Migration refused. The original database was kept.',true);}finally{busy=false;run.disabled=false;ctx.refresh();}
  },'primary');
  ctx.inspector.append(run,result,el('details',{},el('summary',{text:'Actual table definition'}),el('pre',{class:'code',text:t.sql})));if(snapshot.foreignKeyErrors.length)ctx.inspector.append(el('p',{class:'conflict',text:snapshot.foreignKeyErrors.length+' existing foreign-key violations.'}));
 }
 await inspect();return {title:'Schema migration studio',subtitle:'Inspect a real SQLite file and execute reviewed changes on a disposable copy.',capture:()=>structuredClone(state),restore:s=>{state=s;pending=null;inspect().catch(e=>ctx.status(e.message,true));},load:async s=>{const previous=state;state=s;pending=null;try{await inspect();}catch(e){state=previous;await inspect();throw e;}},render,export:()=>download('working-copy.sqlite',state.bytes,'application/vnd.sqlite3'),acceptSelection:id=>snapshot.tables.some(t=>t.name===id),cleanup:()=>rpc.close(),ready:()=>!busy,historyLimit:3};
}
