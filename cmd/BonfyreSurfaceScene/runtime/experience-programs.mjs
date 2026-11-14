// One reusable, owner-neutral composition grammar for native PageComposition
// and SurfaceScene. Pattern names alone never constitute an experience.
import {STITCH_PATTERNS,STITCH_REFERENCES} from "./stitch-grammar.mjs";

// Role names are from the compiled native PageComposition vocabulary.
// Each program yields actual regions in four visual/interaction zones.
// Other declared regions remain in the primary zone in owner sequence.
export const EXPERIENCE_PROGRAMS=Object.freeze({
  profile:      {hero:["lead"],rail:["named","channels","places"],wide:["feed","talk","wall"],views:["overview","focus","compare"]},
  organization: {hero:["lead"],rail:["named","channels","communities"],wide:["feed","wall"],views:["overview","focus","compare"]},
  directory:    {hero:["lead"],rail:["channels","places"],wide:["results","table","communities"],views:["overview","focus","compare"]},
  detail:       {hero:["lead","watch"],rail:["named","channels","places"],wide:["parts","talk"],views:["overview","focus","compare"]},
  discover:     {hero:["lead","stage"],rail:["places","channels"],wide:["feed","grid","wall","happening"],views:["overview","focus","compare"]},
  reader:       {hero:["lead"],rail:["parts","named","channels"],wide:["topics"],views:["overview","focus"]},
  connections:  {hero:["lead"],rail:["named","channels"],wide:["table","results"],views:["overview","focus","compare"]},
  workbench:    {hero:["lead"],rail:["named","channels"],wide:["table","results","feed"],views:["overview","focus","compare"]},
  review:       {hero:["lead"],rail:["named","channels"],wide:["table","results","talk"],views:["overview","focus","compare"]},
  feed:         {hero:["lead"],rail:["channels","named"],wide:["feed","topics"],views:["overview","focus","compare"]},
  market:       {hero:["lead"],rail:["channels","places"],wide:["grid","table","results"],views:["overview","focus","compare"]},
  media:        {hero:["watch","listen"],rail:["named","channels","places"],wide:["grid","wall","playlist"],views:["overview","focus","compare"]},
  spatial:      {hero:["places","stage"],rail:["named","channels"],wide:["wall","grid"],views:["overview","focus","compare"]},
  device:       {hero:["lead"],rail:["channels"],wide:["feed","listen"],views:["overview","focus"]},
});
const own=(o,k)=>Object.prototype.hasOwnProperty.call(o,k);
const array=v=>Array.isArray(v)?v:[];
const accepted=new Set(["overview","focus","compare"]);
for(const [pattern,program] of Object.entries(EXPERIENCE_PROGRAMS)){
  if(!own(STITCH_PATTERNS,pattern))throw Error("Experience without native pattern: "+pattern);
  const used=new Set();
  for(const zone of ["hero","rail","wide"]){
    for(const role of program[zone]){if(used.has(role))throw Error("Repeated experience role: "+role);used.add(role);}
    Object.freeze(program[zone]);
  }
  if(program.views[0]!=="overview"||new Set(program.views).size!==program.views.length||
     !program.views.every(v=>accepted.has(v)))throw Error("Invalid reader posture: "+pattern);
  Object.freeze(program.views);Object.freeze(program);
}
if(Object.keys(EXPERIENCE_PROGRAMS).length!==Object.keys(STITCH_PATTERNS).length)
  throw Error("Every Stitch pattern needs an executable experience recipe");

// Route presentation is authored data: route_presentation rows in the CMS
// (cmd/BonfyreCMS/content-types/route_presentation.json), read by
// cmd/BonfyreWebAddress/src/route_presentation.h. No route table lives here.

export function experiencePattern({reference,pattern}={}){
  if(reference!==undefined&&!own(STITCH_REFERENCES,reference))throw RangeError("Unknown Stitch reference");
  if(reference!==undefined&&pattern!==undefined&&STITCH_REFERENCES[reference]!==pattern)
    throw RangeError("Conflicting experience reference");
  const key=pattern??STITCH_REFERENCES[reference];
  if(!own(EXPERIENCE_PROGRAMS,key))throw RangeError("Unknown experience program");
  return key;
}
export function experienceZone(pattern,role){
  const program=EXPERIENCE_PROGRAMS[pattern];
  if(!program)throw RangeError("Unknown experience program");
  return program.hero.includes(role)?"hero":program.rail.includes(role)?"rail":
         program.wide.includes(role)?"wide":"primary";
}

// Native Scene supplies exact identity, extent, owner projection, bindings and
// action apertures. This compiler never constructs material or an effect.
export function compileSceneExperience(scene,options={}){
  if(scene?.schema!=="bonfyre-surface-scene.v1"||!scene.source_digest||
     typeof scene.focus?.subject_address!=="string"||!scene.focus.subject_address)
    throw TypeError("A resolved, digested native SurfaceScene is required");
  const pattern=experiencePattern(options),spec=STITCH_PATTERNS[pattern];
  const areas=[],ids=new Set();
  const bindings=array(scene.bindings),boundSlots=new Set(bindings.map(binding=>binding.slot_id));
  for(const slot of array(scene.slots)){
    if(!slot.slot_id||ids.has(slot.slot_id))throw Error("Invalid or repeated semantic slot");
    ids.add(slot.slot_id);
    const role=slot.role||slot.slot_name||slot.slot_id;
    const materialized=boundSlots.has(slot.slot_id);
    areas.push({id:slot.slot_id,kind:"semantic_slot",role,zone:experienceZone(pattern,role),
      materialization:materialized?"installed":"declared",
      extent:slot.extent,owner:"native-slot",continuity:slot.continuity_key||"",
      sourceProjection:"",apertures:[]});
  }
  for(const fragment of array(scene.fragments)){
    if(!fragment.fragment_id||ids.has(fragment.fragment_id))throw Error("Invalid or repeated fragment");
    ids.add(fragment.fragment_id);
    const role=fragment.role||fragment.renderer_variant||fragment.surface_role||fragment.surface_grammar||fragment.renderer_family;
    areas.push({id:fragment.fragment_id,kind:"world_fragment",role,zone:experienceZone(pattern,role),
      materialization:fragment.source_projection?"source-bound":"server-rendered",
      extent:fragment.extent,owner:"native-fragment",continuity:fragment.continuity_group||"",
      sourceProjection:fragment.source_projection||"",apertures:[]});
  }
  const visible=areas.filter(area=>area.materialization!=="declared");
  const modes=EXPERIENCE_PROGRAMS[pattern].views.filter(mode=>
    mode==="overview"||(mode==="focus"&&visible.length>0)||(mode==="compare"&&visible.length>1));
  const grouped=Object.fromEntries(["hero","primary","rail","wide"].map(zone=>
    [zone,visible.filter(area=>area.zone===zone).map(area=>area.id)]));
  return Object.freeze({schema:"aurekai-experience-program.v1",pattern,shell:spec.shell,
    sourceDigest:scene.source_digest,subject:scene.focus.subject_address,actorScope:scene.focus.actor_scope||"",
    modes:Object.freeze(modes),zones:Object.freeze(grouped),
    regions:Object.freeze(areas.map(area=>Object.freeze(area))),
    bindingIds:Object.freeze(bindings.map(binding=>binding.binding_id)),
    // Actual owner operations are not copied into the experience interpreter.
    effectAuthority:"native-owner-only"});
}

// Authoring is another view of the SAME compiled experience, not a separate
// import format. Native CMS owns revisions, and the donor compiler and Scene
// still own their source contracts and effect apertures.
import {newAuthoringTree,validateAuthoringTree,assertAuthoringContinuity,
 authoringCoordinate} from "./authoring-tree.mjs";
import {portableSha256} from "../../../components/BonfyreServiceSite/authoring/sha256-portable.mjs";

const hashSource=value=>{
 const text=JSON.stringify(value);
 if(typeof text!=="string")throw TypeError("An exact compiled source is required");
 return portableSha256(new TextEncoder().encode(text));
};
const digestOnly=value=>{
 const digest=typeof value==="string"?value.replace(/^sha256:/,""):"";
 if(!/^[a-f0-9]{64}$/.test(digest))throw TypeError("Native experience requires a full source digest");
 return digest;
};
const makeNode=(key,kind,heading,ref,digest,payload={},metadata={},bindings={},children=[])=>({
 id:authoringCoordinate("compiled:"+key),kind,
 props:{heading,body:"",eyebrow:"",label:"",href:"",variant:"editorial",source_ref:ref,source_digest:digest,
  readOnly:true,payload,metadata,bindings},responsive:{},children
});
const fieldKind=p=>p==="heading"?"ui:badge":"ui:field";
const fieldInput=p=>["money","quantity","count","rating"].includes(p)?"number":
 p==="date"?"date":"text";
const fieldMultiline=p=>["prose","longform","rich-text","code","authored-markup",
 "composition"].includes(p);
const sourceText=(value,label,max=220)=>{
 if(typeof value!=="string"||value.length>max||
    /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(value))
  throw TypeError("Invalid compiled "+label);
 return value;
};
const nativeTree=(site_id,page,root)=>{
 const tree=newAuthoringTree(site_id,page);
 tree.blocks.push(root);
 return validateAuthoringTree(tree);
};

/** One Frappe DocType from its authoritative phase-12 surface program.
 * All declared fields retain their original structure inside typed nodes;
 * a source declaration is NEVER an executable Frappe controller or a record.
 */
export function authorFrappeExperience({surface,program_id,site_id,page="/"}){
 if(surface?.schema!=="bonfyre.frappe-surface-programs.v1"||
    !Array.isArray(surface.programs)||typeof surface.app!=="string")
  throw TypeError("Requires the native Frappe surface-program compilation");
 const candidates=surface.programs.filter(x=>x?.program_id===program_id);
 if(candidates.length!==1)throw Error("Exactly one selected native program required");
 const program=candidates[0],ref=sourceText(program_id,"program address",1000);
 if(!ref.startsWith("frappe:"+surface.app+":")||!Array.isArray(program.layout)||
    !program.record_posture||!program.counts)throw Error("Incomplete native Frappe program");
 const digest=hashSource({app:surface.app,program}),signature=ref+":"+program.doctype;
 const identity={owner:"BonfyreFrappeCompiler",program_id:ref};
 const used=new Set(),flat=[];
 let layoutCount=0;
 const root=makeNode(signature,"ui:section",sourceText(program.doctype,"DocType"),
  ref,digest,{heading:program.doctype,lede:program.description||"",tone:"surface"},
  {sourceKind:"frappe-program",doctype:program.doctype,module:program.module||"",
   record_posture:program.record_posture,title_field:program.title_field||""},identity);
 for(const [ti,tab]of program.layout.entries()){
  if(!Array.isArray(tab.sections))throw Error("Frappe tab missing sections");
  if(tab.empty_in_metadata&&tab.sections.some(s=>s.columns?.some(c=>c.fields?.length)))
   throw Error("Source claims an empty tab containing fields");
  // An empty donor tab is retained as source metadata, not a fabricated pane.
  if(tab.empty_in_metadata)continue;
  const tabNode=makeNode(signature+":tab:"+ti,"ui:section",sourceText(tab.tab||program.doctype,"tab"),
   ref,digest,{heading:tab.tab||program.doctype,lede:"",tone:"surface"},
   {sourceKind:"frappe-tab",tabIndex:ti,empty_in_metadata:false},identity);
  for(const [si,section]of tab.sections.entries()){
   if(!Array.isArray(section.columns))throw Error("Frappe section missing columns");
   const sectionNode=makeNode(signature+":tab:"+ti+":section:"+si,"ui:section",
    sourceText(section.section||tab.tab||program.doctype,"section"),ref,digest,
    {heading:section.section||tab.tab||program.doctype,lede:"",tone:"surface"},
    {sourceKind:"frappe-section",tabIndex:ti,sectionIndex:si,
     collapsible:section.collapsible===true,organ:section.organ||""},identity);
   for(const [ci,column]of section.columns.entries()){
    if(!Array.isArray(column.fields))throw Error("Frappe column missing fields");
    const columnNode=makeNode(signature+":tab:"+ti+":section:"+si+":column:"+ci,
     "ui:stack","",ref,digest,{gap:12},
     {sourceKind:"frappe-column",tabIndex:ti,sectionIndex:si,columnIndex:ci},identity);
    for(const [fi,field]of column.fields.entries()){
     if(!field||typeof field!=="object"||typeof field.presentation!=="string"||
        typeof field.withheld!=="string"||typeof field.field!=="string")
      throw Error("Frappe field contract is incomplete");
     const address=[ti,si,ci,fi].join(".");
     if(field.field&&!/^[a-zA-Z0-9_]{1,160}$/.test(field.field))
      throw Error("Unaddressable compiled field");
     if(!field.field&&!field.withheld)throw Error("Unnamed field without refusal");
     if(field.field){if(used.has(field.field))throw Error("Duplicate Frappe field address");used.add(field.field);}
     flat.push(field);
     const name=sourceText(field.label||field.field||"Withheld field","field label");
     const blocked=!!field.withheld;
     const kind=blocked?"ui:alert":fieldKind(field.presentation);
     const payload=blocked?{tone:"warning",title:name,body:field.withheld,dismissible:false}:
      kind==="ui:badge"?{label:name}:
      {label:name,name:field.field,type:fieldInput(field.presentation),
        value:"",placeholder:"",multiline:fieldMultiline(field.presentation),
        rows:"3",maxlength:"500",required:false,disabled:true};
     columnNode.children.push(makeNode(signature+":field:"+address,kind,name,ref,digest,
      payload,{sourceKind:"frappe-field",address,field:structuredClone(field)},
      {...identity,field:field.field||"",address}));
    }
    if(columnNode.children.length)sectionNode.children.push(columnNode);
   }
   if(sectionNode.children.length)tabNode.children.push(sectionNode);
  }
  if(tabNode.children.length)root.children.push(tabNode);
  layoutCount++;
 }
 const withheld=flat.filter(f=>f.withheld).length,rendered=flat.length-withheld;
 if(flat.length!==program.counts.rendered+program.counts.withheld||
    withheld!==program.counts.withheld||rendered!==program.counts.rendered||
    program.counts.fields!==flat.length+program.counts.layout)
  throw Error("Compiled Frappe field/accounting contract mismatch");
 // Preserve otherwise invisible empty-tab declarations with exact tab indices.
 root.props.payload.empty_tabs=program.layout.flatMap((tab,index)=>
  tab.empty_in_metadata?[{index,label:tab.tab||""}]:[]);
 root.props.payload.source_field_count=flat.length;
 return nativeTree(site_id,page,root);
}

/** Source-bound Scene regions use the same authoring tree as a DocType.
 * Only materialized native regions become blocks. Installed commands stay in
 * native owner apertures, not in a generated HTML button or copied handler.
 */
export function authorSceneExperience({scene,site_id,page="/",reference,pattern,look="editorial"}){
 const sourceDigest=digestOnly(scene?.source_digest);
 const exp=compileSceneExperience(scene,{reference,pattern});
 const ref=sourceText(exp.subject,"native subject address",1000);
 const key=ref+":"+exp.pattern;
 const root=makeNode(key,"ui:section","Experience",ref,sourceDigest,
  {heading:"Experience",lede:"",tone:"surface"},
  {sourceKind:"native-scene",pattern:exp.pattern,shell:exp.shell,
   actorScope:exp.actorScope,look}, {owner:"BonfyreSurfaceScene",subject:ref});
 const zoneSequence=["hero","primary","rail","wide"];
 for(const zone of zoneSequence){
  const regions=exp.regions.filter(r=>r.zone===zone&&r.materialization!=="declared");
  if(!regions.length)continue;
  const group=makeNode(key+":"+zone,"ui:section",zone,ref,sourceDigest,
   {heading:zone,lede:"",tone:"surface"},
   {sourceKind:"native-zone",zone}, {owner:"BonfyreSurfaceScene",subject:ref});
  for(const region of regions){
   const bound=(scene.bindings||[]).filter(b=>b.slot_id===region.id).map(b=>b.binding_id);
   const {apertures:_ownerOnly,...inertRegion}=region;
   group.children.push(makeNode(key+":"+region.id,"ui:card",region.role||"Region",
    ref,sourceDigest,{title:region.role||"Region",body:"",actions:[]},
    {sourceKind:"native-region",region:structuredClone(inertRegion)},
    {owner:"BonfyreSurfaceScene",subject:ref,region_id:region.id,binding_ids:bound}));
  }
  root.children.push(group);
 }
 root.props.payload.modes=exp.modes;
 root.props.payload.declared_regions=exp.regions.filter(r=>r.materialization==="declared")
  .map(r=>({id:r.id,role:r.role,zone:r.zone}));
 return nativeTree(site_id,page,root);
}

/** Merge a compiler projection into an ordinary account-owned authoring draft.
 * Identities already present cannot be replaced. A later source version must
 * be reviewed as a different candidate, never silently absorb an editor's work.
 */
export function composeAuthoredExperience(base,projection){
 validateAuthoringTree(base);validateAuthoringTree(projection);
 if(base.site_id!==projection.site_id||base.page!==projection.page)
  throw Error("Experience destination differs from retained page");
 const assembled=structuredClone(base);
 assembled.blocks.push(...structuredClone(projection.blocks));
 return assertAuthoringContinuity(base,validateAuthoringTree(assembled));
}
