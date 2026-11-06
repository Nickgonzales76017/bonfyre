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
