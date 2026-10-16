import {createWorkspaceHost} from './workspace-host.mjs';
import {changeMarkup} from './review-sheet.mjs';
/** Presentation assembly over an already mounted collection editor. This is
 * reusable by any collection binding; changes and saved effects are still its own. */
export async function composeCollectionWorkspace({host,scope,sourceDigests,readRows,changes,dirty,inspect,command,loadDocking,signal}) {
  const docking=await loadDocking(),doc=host.ownerDocument;
  if(signal?.aborted)return null;
  const source=Array.from(sourceDigests()).sort().join('\n'),bytes=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(scope+'\n'+source));
  const fingerprint=Array.from(new Uint8Array(bytes),b=>b.toString(16).padStart(2,'0')).join('');
  if(signal?.aborted)return null;
  const stage=host.querySelector('.cw-grid-stage'),table=host.querySelector('[data-grid="table"]'),detail=host.querySelector('[data-grid="inspector"]');
  const delta=doc.createElement('section');delta.className='cw-change-workspace';stage.append(delta);
  const heading=doc.createElement('h3');heading.textContent='Working changes';delta.append(heading);
  const deltaBody=doc.createElement('div');delta.append(deltaBody);
  const confirm=doc.createElement('button');confirm.type='button';confirm.className='cw-button cw-button-primary';confirm.textContent='Review & save changes';confirm.onclick=()=>command('review');delta.append(confirm);
  const registry=new Map([
    ['records',{kind:'EditableCollection',label:'Records',element:()=>table}],
    ['inspector',{kind:'RecordInspector',label:'Selected record',element:()=>detail}],
    ['changes',{kind:'ChangeReview',label:'Working changes',element:()=>delta}]
  ]);
  const ids=['undo','redo','review','reset'];registry.get('records').commands=ids.map(id=>({id:'collection.'+id,label:({undo:'Undo record edit',redo:'Redo record edit',review:'Review & save record changes',reset:'Discard unsaved edits'})[id],group:'Records',enabled:()=>!host.querySelector(`[data-grid-action="${id}"]`).disabled,run:()=>command(id)}));
  for(const [id,b]of registry){b.acceptSelection=key=>readRows().some(r=>r.key===key);b.dirty=dirty;if(id==='inspector')b.selection=key=>{const row=readRows().find(r=>r.key===key);if(row)inspect(row);};}
  const scene={schema:'bonfyre-surface-scene.v1',source_digest:fingerprint,focus:{subject_address:'collection:'+scope,actor_scope:'browser-local'},
    slots:[...registry].map(([id,b],i)=>({slot_id:id,ordinal:i,extent:'workspace',accepts:[b.kind]})),
    bindings:[...registry].map(([id,b])=>({binding_id:id,slot_id:id,application_kind:b.kind,source_collection:scope,continuity_key:'collection:'+scope,action_aperture:(b.commands??[]).map(c=>c.id).join(',')})),
    interaction:{desktop_posture:{title:'Collection workspace',theme:'dark',initial_layout:'edit',layouts:[
      {id:'edit',label:'Edit & inspect',panels:[{id:'records'},{id:'inspector',relative:'records',direction:'right',width:320},{id:'changes',relative:'inspector',direction:'within'}]},
      {id:'review',label:'Review changes',panels:[{id:'records'},{id:'changes',relative:'records',direction:'below',height:260},{id:'inspector',relative:'records',direction:'right',width:300}]},
      {id:'records',label:'Full-width records',panels:[{id:'records'}]}
    ]}}};
  const work=createWorkspaceHost(host,{scene,contributions:registry,docking,before:stage});stage.hidden=true;
  function refresh(){const rows=changes();deltaBody.innerHTML=rows.length?changeMarkup(rows):'<p>No pending changes. Edits appear here before saving.</p>';confirm.disabled=!rows.length;work.refresh();}
  refresh();
  return {...work,refresh,
    selected(key){work.session.select('records',key??null);if(key)work.open('inspector',{side:true});},
    dispose(){work.dispose();delta.remove();stage.hidden=false;}
  };
}
