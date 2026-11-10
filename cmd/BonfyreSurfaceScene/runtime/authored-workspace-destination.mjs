import {prepareProductionTransfer,commitProductionTransfer} from './production-runtime/authored-workspace-transfer.mjs';
import {validateRecoveryInstrument} from './production-runtime/authored-instrument-validation.mjs';
import {readWorkspaceDestinations,readProductionBindings,verifyProductionDestination,productionPack} from '../bridge/workspace-destinations.mjs';
import {openDestinationStore} from '../bridge/workspace-store.mjs';
import {prepareRecord,digest} from '../bridge/workspace-value.mjs';
import {validateExcerpt} from './research.mjs';
import {el,button,field,modal} from './ui.mjs';
const digestValue=async value=>digest(await prepareRecord(value).encodeWith(async blob=>digest(await blob.arrayBuffer())));
const profileNames=['schema','compiler_sha256','validator_sha256','checks_sha256','composition_sha256','renderer_sha256','value_codec_sha256','msgpack_codec_sha256'];
async function recoveryProfile(binding){
  const response=await fetch('/apps/'+binding+'/instruments/RECOVERY.tsv',{credentials:'same-origin',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(15000)});
  if(!response.ok)throw Error('This production needs the current recovery and Data Checks editor in the receiving workspace.');
  const reader=response.body.getReader(),parts=[];let size=0;for(;;){const {done,value}=await reader.read();if(done)break;if((size+=value.length)>4096){await reader.cancel();throw Error('The receiving recovery profile exceeds its bound.');}parts.push(value);}
  const bytes=new Uint8Array(size);let at=0;for(const part of parts){bytes.set(part,at);at+=part.length;}const text=new TextDecoder('utf-8',{fatal:true}).decode(bytes),rows=text.split('\n');
  if(rows.pop()!==''||rows.length!==profileNames.length||rows.some((row,i)=>{const fields=row.split('\t');return fields.length!==2||fields[0]!==profileNames[i]||(i===0?fields[1]!=='bonfyre.authored-workspace-recovery.v1':!/^[a-f0-9]{64}$/.test(fields[1]));}))throw Error('The receiving editor has an unsupported recovery profile.');
  return text;
}
async function validateResearchEvidence(source,candidate){
  if(!Array.isArray(source?.sources)||!Array.isArray(candidate?.evidence)||!candidate.evidence.length)throw Error('Include the original research and its reviewed passages before preparing another workspace.');
  for(const id of new Set(candidate.evidence.map(ref=>ref.sourceId))){const material=source.sources.find(s=>s.id===id);if(!material||await digest(new TextEncoder().encode(material.body))!==material.sha256)throw Error('A retained research source no longer matches its text digest.');}
  if(!candidate.evidence.every(ref=>validateExcerpt(source.sources,ref)))throw Error('A finding no longer matches its original passage. Review the research before preparing another workspace.');
  return true;
}
export async function openProductionDestination({sourceEndpoint,sourceRelease,productionKey,productionRevision,sourceStore}){
  const destination=el('select',{'aria-label':'Receiving destination'}),editor=el('select',{'aria-label':'Receiving production editor'}),status=el('p',{role:'status','aria-live':'polite'}),preview=el('section'),accepted=el('input',{type:'checkbox'});
  const consent=field('I have reviewed the receiving destination and the included working material.',accepted);
  const content=el('div',{},el('p',{text:'Prepare a separate editable production in one of your workspaces. Review who can access that destination and the source material included below. Later changes stay with each copy.'}),field('Receiving destination',destination),field('Receiving production editor',editor),status,preview,consent);
  const dialog=modal('Prepare this production in another workspace',content),close=dialog.querySelector('header button');let busy=false,complete=false,plan=null,receiver=null;
  async function verifyPreparedTarget(target,production){
    const admitted=await verifyProductionDestination(target);if(!production?.recoveryOrigin&&!production?.panels.some(p=>p.kind==='checks'))return admitted;
    const context=document.querySelector('meta[name="bonfyre-workspace-context"]')?.content,sourceContext=/^user:[1-9][0-9]*$/.test(context??'')?'self':context,sourceId=sourceEndpoint.split('/')[2];
    const currentSource=async()=>{const binding=(await readProductionBindings(sourceContext)).find(b=>b.id===sourceId);if(!binding?.ready||binding.release!==sourceRelease)throw Error('The source editor changed. Reopen this production in its current release before copying.');};
    await currentSource();const [source,targetProfile]=await Promise.all([recoveryProfile(sourceId),recoveryProfile(target.id)]);
    if(source!==targetProfile)throw Error('The receiving editor has different recovery or Data Checks semantics. Update that workspace and review the production again.');
    await currentSource();return verifyProductionDestination(target);
  }
  const retry=button('Refresh destinations',()=>void run(loadDestinations)),review=button('Review prepared production',()=>void run(prepare)),create=button('Create prepared production',()=>void run(commit),'primary');
  dialog.querySelector('footer').append(retry,review,create);consent.hidden=true;create.hidden=true;
  function controls(){for(const n of dialog.querySelectorAll('button,select,input'))n.disabled=busy||complete;close.disabled=busy;create.disabled=busy||complete||!plan||!accepted.checked;review.disabled=busy||complete||!editor.value;}
  async function run(fn){if(busy||complete||!dialog.open)return;busy=true;controls();try{await fn();}catch(error){if(dialog.open)status.textContent=error.message;}finally{busy=false;if(dialog.open)controls();}}
  function clearPreview(){plan=null;receiver=null;accepted.checked=false;preview.replaceChildren();consent.hidden=create.hidden=true;}
  function setupLink(){const context=destination.value;const link=el('a',{href:'/packs/setup?'+new URLSearchParams({context,pack:productionPack}),target:'_blank',rel:'noopener noreferrer',text:'Enable Production Studio in this workspace ↗'});status.replaceChildren(el('span',{text:'This workspace has no ready production editor. '}),link);}
  async function loadEditors(){clearPreview();editor.replaceChildren();if(!destination.value)return;const rows=await readProductionBindings(destination.value);if(!dialog.open)return;for(const row of rows.filter(r=>r.ready)){const option=el('option',{value:row.id,text:row.label});option.dataset.release=row.release;editor.append(option);}if(!editor.children.length)setupLink();else status.textContent='Review the prepared production before creating any receiving records.';}
  async function loadDestinations(){clearPreview();destination.replaceChildren();editor.replaceChildren();const rows=await readWorkspaceDestinations();if(!dialog.open)return;for(const row of rows){const option=el('option',{value:row.context,text:row.label+(row.canWrite?'':' — read only')});option.disabled=!row.canWrite;destination.append(option);}const first=[...destination.options].find(o=>!o.disabled);if(first){destination.value=first.value;await loadEditors();}else status.textContent='No destination currently permits this account to create working records.';}
  async function prepare(){
    clearPreview();const option=editor.selectedOptions[0];if(!option)throw Error('Choose a ready receiving editor.');
    const target=await verifyProductionDestination({context:destination.value,id:option.value,release:option.dataset.release});
    const prepared=await prepareProductionTransfer({sourceEndpoint,sourceRelease,productionKey,productionRevision,sourceStore,target,digestValue,validateResearchEvidence,validateInstrument:validateRecoveryInstrument});if(!dialog.open)return;
    await verifyPreparedTarget(target,prepared.production);if(!dialog.open)return;
    plan=prepared;receiver=openDestinationStore('/apps/'+target.id+'/_records');preview.append(el('h3',{text:prepared.production.title}),el('p',{text:'Receiving workspace: '+target.destinationLabel}),el('p',{text:prepared.production.description||'No production purpose supplied.'}));
    const list=el('ul');for(const panel of prepared.production.panels){const value=prepared.changes.find(c=>c.key===panel.recordKey)?.value;let summary=panel.label+' · '+panel.role;if(panel.kind==='research')summary+=' · '+value.sources.length+' retained sources and '+value.findings.length+' authored findings';else if(panel.kind==='checks')summary+=' · '+value.contract.entities.length+' declared inputs, '+value.contract.predicates.length+' predicates and '+value.tables.length+' retained tables';else if(panel.kind==='product')summary+=' · '+value.stories.length+' editable stories';else if(panel.kind==='schema')summary+=' · complete SQLite working file, '+(value.bytes?.byteLength??0).toLocaleString()+' bytes';else summary+=' · complete retained process log and field mapping';list.append(el('li',{text:summary}));if(panel.kind==='research'){const details=el('details',{},el('summary',{text:'Included research source titles'}));for(const source of value.sources)details.append(el('p',{text:source.title}));list.append(el('li',{},details));}}
    preview.append(list,el('p',{text:'The source production stays in its current workspace. The receiving copy includes its bound working records and the research evidence needed by adopted stories.'}));consent.hidden=create.hidden=false;status.textContent='Review the destination and included material, then create the prepared production.';
  }
  async function commit(){
    if(!plan||!accepted.checked)throw Error('Review the prepared production and its destination first.');
    const row=await commitProductionTransfer(plan,{sourceStore,targetStore:receiver,verifyTarget:target=>verifyPreparedTarget(target,plan.production),digestValue,validateResearchEvidence,validateInstrument:validateRecoveryInstrument});complete=true;
    const href='/apps/'+plan.target.id+'/'+plan.target.entry+'?'+new URLSearchParams({record:row.key,revision:String(row.revision)});
    status.replaceChildren(el('p',{text:'The receiving production and its working material are saved.'}),el('a',{href,target:'_blank',rel:'noopener noreferrer',class:'primary',text:'Open prepared production ↗'}));create.hidden=true;consent.hidden=true;
  }
  destination.onchange=()=>void run(loadEditors);editor.onchange=()=>{clearPreview();controls();};accepted.onchange=controls;dialog.addEventListener('cancel',event=>{if(busy)event.preventDefault();});
  await run(loadDestinations);return dialog;
}
