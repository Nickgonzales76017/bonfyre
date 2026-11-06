// Programme requirements consume the native AddressPlane. This module carries
// references and coverage, never copies owner rows or declares requirements met.
import {spawnSync} from 'node:child_process';
const coordinate=/^[a-z][a-z0-9_]*:[a-z0-9_-]+$/;
const kinds=new Set(['external_occurrence','external_source','pack','projection','provider','compute','transformation','protocol_chain','media_library','creator_channel','capability_program']);
const fail=message=>{throw new Error(message)};
export function demandContracts(value){
  if(!Array.isArray(value)||value.length>32)fail('at most 32 declared source demands');
  const ids=new Set();
  return value.map(d=>{
    if(!d||typeof d!=='object'||typeof d.id!=='string'||!d.id||d.id.length>160||ids.has(d.id))fail('unique source-demand identity required');
    ids.add(d.id);
    if(!Array.isArray(d.kinds)||!d.kinds.length||d.kinds.length>kinds.size||d.kinds.some(k=>!kinds.has(k)))fail('explicit supported owner kinds required');
    for(const [field,empty]of [['need',false],['forbid',true]]){
      const values=d[field]??[];
      if(!Array.isArray(values)||(!empty&&!values.length)||values.length>32||values.some(v=>typeof v!=='string'||v.length>200||!coordinate.test(v)))fail('invalid '+field+' coordinates');
    }
    const searches=d.discovery_queries??[];
    if(!Array.isArray(searches)||searches.length>16||searches.some(v=>typeof v!=='string'||!v.trim()||v.length>500))fail('invalid residual discovery query');
    return {id:d.id,kinds:[...new Set(d.kinds)].sort(),need:[...new Set(d.need)].sort(),forbid:[...new Set(d.forbid??[])].sort(),discovery_queries:searches};
  });
}
export function nativeAddressRun(argv){
  const r=spawnSync(argv[0],argv.slice(1),{encoding:'utf8',timeout:30000,maxBuffer:4*1024*1024});
  return {exitCode:r.status,stdout:r.stdout??'',stderr:r.error?.message||r.stderr||''};
}
export function resolveDemands(input,{binary,index,runFn=nativeAddressRun}={}){
  const demands=demandContracts(input),memo=new Map(),resolutions=[];
  if(!binary||!index)fail('AddressPlane owner and index are required');
  for(const demand of demands){
    const candidates=[],coverage=[];
    for(const kind of demand.kinds){
      const argv=[binary,'forward','--db',index,'--kind',kind,'--limit','256',...demand.need.flatMap(t=>['--need',t]),...demand.forbid.flatMap(t=>['--forbid',t])];
      const key=JSON.stringify(argv);let result=memo.get(key);
      if(!result){
        const called=runFn(argv);if(called.exitCode!==0)fail('AddressPlane resolution failed; no fallback discovery: '+called.stderr.slice(0,400));
        result=JSON.parse(called.stdout);
        if(result.has_more===true)fail('AddressPlane result exceeds the explicit resolution bound; refine the demand before publishing references');
        if(result.candidate_kind!==kind||result.truth!=='eligible_for_further_consideration'||!Array.isArray(result.survivors)||!Number.isSafeInteger(result.eligible)||result.eligible<0||result.returned!==result.survivors.length||result.returned>256||result.eligible<result.returned||typeof result.has_more!=='boolean')fail('incomplete AddressPlane response');
        memo.set(key,result);
      }
      const seen=new Set();
      for(const c of result.survivors){
        if(!c||['identity','owner_ref','exact_verifier','lowerer_ref'].some(k=>typeof c[k]!=='string'||!c[k]||c[k].length>8192)||seen.has(c.identity))fail('invalid owner reference');
        if(!Array.isArray(c.dependency_refs)||c.dependency_refs.length>16384||c.dependency_refs.some(v=>typeof v!=='string'||!v||v.length>8192))fail('invalid dependency references');
        seen.add(c.identity);candidates.push({...c,candidate_kind:kind});
      }
      coverage.push({kind,considered:result.considered,eligible:result.eligible,returned:result.returned,has_more:result.has_more,reason:result.reason});
    }
    resolutions.push({demand_id:demand.id,state:candidates.length?'owner_verification_required':'no_indexed_candidate',candidates,coverage,
      discovery_queries:candidates.length?[]:demand.discovery_queries});
  }
  return {owner:'AddressPlane',resolutions,owner_queries:memo.size,requirements_satisfied:0};
}
export function declaredDemands(selector,templateDemands){
  if(selector.address_demands!==undefined)return demandContracts(selector.address_demands);
  if(templateDemands!==undefined&&templateDemands!==null)return demandContracts(templateDemands);
  const families=selector.capability_families??[];
  if(!Array.isArray(families)||families.length>16||families.some(f=>typeof f!=='string'||!/^[a-z0-9_-]+$/.test(f)))fail('invalid declared capability families');
  // No guessed provider, archetype dictionary or textual similarity. A family
  // can be advertised as an operation or produced material; both remain candidates.
  return demandContracts(families.flatMap(f=>['operation','produces'].map(axis=>({id:axis+':'+f,
    kinds:['external_source','pack','projection','provider','compute','transformation','protocol_chain','media_library','creator_channel','capability_program'],need:[axis+':'+f]}))));
}
export function referencedSources(resolution,cms,namespace){
  const sources=[];
  for(const r of resolution.resolutions)for(const c of r.candidates){
    const prefix='CMS:internet_source:';
    if(c.candidate_kind!=='external_source'||!c.owner_ref.startsWith(prefix))continue;
    const id=c.owner_ref.slice(prefix.length);
    if(c.identity!=='cms:internet_source:'+id||c.exact_verifier!=='CMS:publication')continue;
    const row=cms.query("SELECT source FROM internet_source WHERE source=? AND status='published' AND namespace IN (?, 'root')").get(id,namespace);
    if(row)sources.push(row.source);
  }
  return [...new Set(sources)].sort();
}

// One non-owning collection projection shared by initial compilation and later
// refresh. Author-selected sources survive; only this resolver's own derived
// membership is replaced when its exact owner references disappear.
export function resolveCollection(selector,templateDemands,{cms,namespace='root',binary,index,runFn=nativeAddressRun}){
  if(!selector||Array.isArray(selector)||typeof selector!=='object')fail('invalid source selector');
  for(const key of ['source_refs','resolved_source_refs'])if(selector[key]!==undefined&&(!Array.isArray(selector[key])||selector[key].some(v=>typeof v!=='string'||!v||v.length>500)))fail('invalid '+key);
  const contracts=declaredDemands(selector,templateDemands);
  const local=contracts.length?resolveDemands(contracts,{binary,index,runFn}):{owner:'AddressPlane',resolutions:[],owner_queries:0,requirements_satisfied:0};
  const sourceRefs=contracts.length?referencedSources(local,cms,namespace):[];
  const candidates=local.resolutions.flatMap(r=>r.candidates);
  const candidateRefs=[...new Set(candidates.map(c=>c.identity))].sort();
  const dependencies=[...new Set(candidates.flatMap(c=>[c.identity,c.owner_ref,...c.dependency_refs]))].sort();
  const priorOwned=new Set(selector.resolved_source_refs??[]),preserved=(selector.source_refs??[]).filter(id=>!priorOwned.has(id));
  const projected={...selector,source_refs:[...new Set([...preserved,...sourceRefs])].sort(),candidate_refs:candidateRefs,
    resolved_source_refs:sourceRefs.filter(id=>!preserved.includes(id)),resolved_dependency_refs:dependencies,
    resolution_state:!contracts.length?'requirements_undeclared':candidates.length?'owner_verification_required':'no_indexed_candidate'};
  if(projected.source_refs.length>300||candidateRefs.length>4096||dependencies.length>16384)fail('resolution exceeds collection capacity; nothing partially published');
  return {selector:projected,local,sourceRefs,candidateRefs,contracts,
    discoveryQueries:[...new Set(local.resolutions.flatMap(r=>r.discovery_queries))]};
}
