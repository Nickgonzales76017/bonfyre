import {programServicePolicy} from './program-service-policy.mjs';
const contextPattern=/^(self|(institution|household):[a-z0-9-]{1,63})$/;
const object=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
if(!object(programServicePolicy)||!object(programServicePolicy.packs)||!object(programServicePolicy.aliases)||!object(programServicePolicy.continuations)||!object(programServicePolicy.grammar_basis))throw Error('Program service policy is incomplete.');
export const programServicePacks=Object.freeze({...programServicePolicy.packs});
const aliases=Object.freeze({...programServicePolicy.aliases});
const roleSpecs=Object.freeze(Object.fromEntries(Object.entries(programServicePolicy.continuations).map(([role,specs])=>[role,Object.freeze(specs.map(spec=>Object.freeze({...spec})))]))),grammarBasis=Object.freeze(Object.fromEntries(Object.entries(programServicePolicy.grammar_basis).map(([role,spec])=>[role,Object.freeze({...spec,owner_families:Object.freeze([...spec.owner_families])})])));
const key=value=>aliases[String(value??'').trim().toLowerCase().replace(/[_\s]+/g,'-')]||String(value??'').trim().toLowerCase().replace(/[_\s]+/g,'-');
const human=value=>String(value).replaceAll('_',' ').replaceAll('-',' ');
export function programServiceContinuations(roles,tools,{context='self',toolsAvailable=true}={}){
 if(!Array.isArray(roles)||!Array.isArray(tools)||!contextPattern.test(context))throw Error('Program continuation coordinates are incomplete.');
 const installed=new Map(tools.filter(tool=>tool&&typeof tool.pack==='string').map(tool=>[tool.pack,tool])),out=new Map();
 const add=(id,row,role)=>{const prior=out.get(id);if(prior){if(!prior.roles.includes(role))prior.roles.push(role);return;}out.set(id,{...row,roles:[role]});};
 for(const raw of [...new Set(roles.map(value=>String(value??'')).filter(Boolean))]){
  const role=key(raw),basis=grammarBasis[role],mapped=basis?.state==='typed'?basis.continuation:role,specs=roleSpecs[mapped]||[];
  if(!specs.length){
   if(basis?.state==='pending_exact_owner'){add('pending:'+role,{kind:'pending',state:'pending_exact_owner',label:human(raw),description:'Aurekai knows this interaction family, but its exact Program owner continuation is not wired yet.',href:null,owner_families:[...basis.owner_families],boundary:'InteractionGrammar names the owner family and target lowerer only; no owner result, rendered surface, authority or effect is inferred.'},raw);}
   else add('unresolved:'+role,{kind:'unresolved',state:'unresolved',label:human(raw),description:'No typed Aurekai continuation is declared for this program facet yet.',href:null,boundary:'No owner is inferred from an unknown semantic slot.'},raw);
   continue;
  }
  for(const spec of specs){
   if(spec.kind==='workspace'){
    const tool=installed.get(spec.pack),state=!toolsAvailable?'unknown':tool?.ready?'ready':tool?'unready':'not_installed';
    const href=state==='ready'?tool.href:state==='unknown'?'/apps?'+new URLSearchParams({context}):'/packs/setup?'+new URLSearchParams({pack:spec.pack,context});
    add('workspace:'+spec.pack,{kind:'workspace',state,pack:spec.pack,label:state==='ready'?tool.label:spec.label,href,description:spec.description,boundary:'This prepares one program facet in another owner. It does not copy the Program record or claim the facet executed.',workspace:tool||null},raw);
   }else if(spec.kind==='surface'){
    const href=spec.id==='programs'?spec.href+'?'+new URLSearchParams({context}):spec.href;
    add('surface:'+spec.id,{...spec,state:'available',href,boundary:'A service surface is a continuation of attention, not same-record identity or effect authority.'},raw);
   }else{
    add('account:'+spec.id,{...spec,state:'available',href:spec.path+'?'+new URLSearchParams({context}),boundary:'Account/provider continuation reviews prerequisites only; it does not prove a connection or grant provider authority.'},raw);
   }
  }
 }
 return [...out.values()];
}

export function programToolChoices(roles,tools){
 return programServiceContinuations(roles,tools).filter(row=>row.kind==='workspace'&&row.state==='ready').map(row=>row.workspace);
}
