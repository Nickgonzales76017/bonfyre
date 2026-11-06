import {mkdirSync,mkdtempSync,readFileSync,writeFileSync,rmSync,existsSync,renameSync,statSync} from 'node:fs';
import {resolve,join} from 'node:path';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {checkSettings,defaultSettings} from './training-settings.mjs';
const digest=value=>createHash('sha256').update(typeof value==='string'||Buffer.isBuffer(value)?value:JSON.stringify(value)).digest('hex');
const conflict=()=>Object.assign(Error('The studio appearance changed. Reload it before applying your changes.'),{status:409});
const faces={sans:'"DM Sans", ui-sans-serif, system-ui, sans-serif',serif:'"Instrument Serif", ui-serif, serif',mono:'"DM Mono", ui-monospace, monospace'};
// Templates saved before the bundled faces still name the system stacks.
const earlierFaces={'-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif':'sans','Georgia, serif':'serif','ui-monospace, monospace':'mono'};
export const presentationInstitution=id=>{const name=`training-${id}-appearance`;if(!/^[a-z0-9-]{1,63}$/.test(name))throw Error('Invalid studio appearance grant.');return name;};
export function checkedStudioBrand(value){
  if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(k=>!['name','accent','ground','ink','font','density','logo','icon'].includes(k)))throw Error('Unexpected studio appearance fields.');
  checkSettings({...defaultSettings(),brand:value});
  return {name:value.name.trim(),accent:value.accent.toLowerCase(),ground:value.ground.toLowerCase(),ink:value.ink.toLowerCase(),font:value.font,density:value.density,logo:value.logo??null,icon:value.icon??null};
}
export function presentationStore(home,material,{binary=resolve(import.meta.dir,'../../cmd/BonfyreCMS/bonfyre-cms')}={}){
  home=resolve(home);mkdirSync(home,{recursive:true,mode:0o700});const db=join(home,'catalogue.db'),assets=join(home,'brand-assets'),schemas=resolve(import.meta.dir,'../../cmd/BonfyreCMS/content-types');mkdirSync(assets,{recursive:true,mode:0o700});
  function native(...args){try{return execFileSync(binary,[...args,'--db',db,'--schemas',schemas],{encoding:'utf8',env:{...process.env,BONFYRE_FACT_DELTAS:''},stdio:'pipe',timeout:10000,maxBuffer:1024*1024});}catch{throw Error('The native presentation owner refused this operation.');}}
  for(const type of ['training_studio_presentation','presentation_template'])native('schema','apply',join(schemas,type+'.json'));
  const studio=id=>{const found=material.creators.find(c=>c.id===id);if(!found)throw Error('Unknown studio.');return found;};
  const namespace=id=>{studio(id);return 'training-'+id;};
  function rows(type,id){const result=JSON.parse(native('entry','list',type,'--ns',namespace(id),'--limit','2')).data;if(!Array.isArray(result)||result.length>1)throw Error('Ambiguous studio presentation.');return result.map(r=>({id:r.id,...r.attributes}));}
  function write(type,id,body,row,expected){const scratch=mkdtempSync(join(home,'presentation-')),file=join(scratch,'body.json');writeFileSync(file,JSON.stringify(body),{mode:0o600});try{return JSON.parse(row?native('entry','update',type,String(row), '@'+file,'--ns',namespace(id),'--if-field','content_sha','--if-value',expected):native('entry','create',type,'@'+file,'--ns',namespace(id)));}finally{rmSync(scratch,{recursive:true,force:true});}}
  function saveAsset(asset){if(!asset)return null;const bytes=Buffer.from(asset.dataUrl.slice(22),'base64'),sha=digest(bytes),file=join(assets,sha+'.png');if(!existsSync(file)){const scratch=mkdtempSync(join(assets,'pending-'));try{const pending=join(scratch,'asset.png');writeFileSync(pending,bytes,{mode:0o600});renameSync(pending,file);}finally{rmSync(scratch,{recursive:true,force:true});}}return {sha,width:asset.width,height:asset.height};}
  function loadAsset(asset){if(!asset)return null;if(!/^[a-f0-9]{64}$/.test(asset.sha))throw Error('Invalid studio image.');const file=join(assets,asset.sha+'.png');if(statSync(file).size>512*1024)throw Error('Studio image exceeds its size limit.');const bytes=readFileSync(file);if(bytes.length>512*1024||digest(bytes)!==asset.sha)throw Error('Studio image changed.');return {dataUrl:'data:image/png;base64,'+bytes.toString('base64'),width:asset.width,height:asset.height};}
  function profileContent(row){return {revision:row.revision,name:row.brand_name,template:row.template_key,templateSha:row.template_sha,logo:row.logo??null,icon:row.icon??null};}
  function templateContent(brand){return {density:brand.density==='compact'?'tight':'standard',typography_mode:brand.font==='serif'?'optical-serif':'uniform-sans',layout_tokens:{ground:brand.ground,ink:brand.ink,accent:brand.accent,face_display:faces[brand.font],face_body:faces[brand.font==='serif'?'sans':brand.font],face_mono:faces.mono}};}
  function read(id){const row=rows('training_studio_presentation',id)[0];if(!row)return {revision:0,brand:checkedStudioBrand(studio(id).settings.brand)};
    if(row.studio_key!==id||!Number.isSafeInteger(row.revision)||row.revision<1||row.content_sha!==digest(profileContent(row))||!/^training-[a-z0-9-]+-[a-f0-9]{64}$/.test(row.template_key))throw Error('Studio presentation changed outside its owner.');
    // Each template lives in its own namespace, so its reader never scans old revisions.
    const found=JSON.parse(native('entry','list','presentation_template','--ns',row.template_key,'--limit','2')).data;if(found.length!==1)throw Error('Studio template not found.');const t=found[0].attributes;
    const content={density:t.density,typography_mode:t.typography_mode,layout_tokens:t.layout_tokens};if(t.template!==row.template_key||t.status!=='published'||digest(content)!==row.template_sha)throw Error('Studio template changed.');
    const tokens=t.layout_tokens,font=Object.keys(faces).find(k=>faces[k]===tokens.face_display)??earlierFaces[tokens.face_display];const brand=checkedStudioBrand({name:row.brand_name,accent:tokens.accent,ground:tokens.ground,ink:tokens.ink,font,density:t.density==='tight'?'compact':t.density==='standard'?'comfortable':'invalid',logo:loadAsset(row.logo),icon:loadAsset(row.icon)});
    return {revision:row.revision,brand,template:row.template_key};
  }
  function save(id,actor,input){
    if(!input||Array.isArray(input)||Object.keys(input).some(k=>!['brand','expectedRevision'].includes(k))||!Number.isSafeInteger(input.expectedRevision)||input.expectedRevision<0||input.expectedRevision>=2147483646)throw Error('Invalid appearance revision.');
    const brand=checkedStudioBrand(input.brand),previous=read(id),row=rows('training_studio_presentation',id)[0];
    if(previous.revision===input.expectedRevision+1&&row?.editor_actor===actor&&JSON.stringify(previous.brand)===JSON.stringify(brand))return previous;
    if(previous.revision!==input.expectedRevision||(row?.revision??0)!==previous.revision)throw conflict();
    const content=templateContent(brand),templateSha=digest(content),template=`training-${id}-${templateSha}`,existing=JSON.parse(native('entry','list','presentation_template','--ns',template,'--limit','2')).data;
    if(!existing.length){const body={template,reference_source:'authored',reference_name:brand.name,reference_url:'urn:bonfyre:training:'+id,reference_categories:'fitness',family:'training',renderer_mix:'html',hero_mode:'quiet-title',navigation_mode:'sidebar',media_mode:'none',card_mode:'soft-panel',chrome_mode:'minimal',semantic_roles:{accepts:['training']},acceptance_contract:'Studio appearance only.',cannot_infer:'Appearance grants no access to member records.',status:'published',...content};
      const scratch=mkdtempSync(join(home,'template-')),file=join(scratch,'body.json');writeFileSync(file,JSON.stringify(body),{mode:0o600});try{native('entry','create','presentation_template','--ns',template,'@'+file);}catch(error){const raced=JSON.parse(native('entry','list','presentation_template','--ns',template,'--limit','2')).data;if(raced.length!==1)throw error;}finally{rmSync(scratch,{recursive:true,force:true});}
    }
    const ready=JSON.parse(native('entry','list','presentation_template','--ns',template,'--limit','2')).data;
    const t=ready[0]?.attributes;if(ready.length!==1||t.template!==template||t.status!=='published'||digest({density:t.density,typography_mode:t.typography_mode,layout_tokens:t.layout_tokens})!==templateSha)throw Error('Studio template changed.');
    const next={studio_key:id,revision:previous.revision+1,brand_name:brand.name,template_key:template,template_sha:templateSha,logo:saveAsset(brand.logo),icon:saveAsset(brand.icon),editor_actor:actor,status:'published'};next.content_sha=digest(profileContent(next));
    try{write('training_studio_presentation',id,next,row?.id,row?.content_sha);}catch(error){const current=read(id);if(current.revision!==previous.revision)throw conflict();throw error;}
    const saved=read(id);if(saved.revision!==next.revision||JSON.stringify(saved.brand)!==JSON.stringify(brand))throw conflict();return saved;
  }
  return {read,save};
}
