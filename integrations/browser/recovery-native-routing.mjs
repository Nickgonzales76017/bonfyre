import {routeRecoveredResearch,recoveryIdentity,recoveryOwnerPacks} from './recovery-owner-routing.mjs';

const structural=Object.freeze({
  controller:{owner_type:'native_family',owner:'BonfyreFrappeCompiler',state:'native_owner',reason:'Recovered Frappe controller lineage belongs to the native Frappe compiler before any product projection.'},
  'protocol-family':{owner_type:'native_family',owner:'BonfyreExperienceMap',state:'native_owner',reason:'Protocol families belong to the existing ExperienceMap definitions and resolution path.'},
  platform:{owner_type:'native_family',owner:'BonfyreCapability',state:'native_owner',reason:'Platform rows are capability/architecture material, not an end-user Research workflow.'},
  'reuse-candidate':{owner_type:'project',owner:'BonfyreProject',state:'project_review',reason:'Reusable implementation candidates require source-owner review before they become a runtime capability.'},
  'browser-compute':{owner_type:'native_family',owner:'BonfyreCapability',state:'needs_bridge',reason:'Browser-compute requirements need capability matching and a bounded execution bridge.'},
  composition:{owner_type:'native_family',owner:'BonfyreCapability',state:'needs_bridge',reason:'Composition requirements need capability matching rather than a generic research completion claim.'},
  programme:{owner_type:'workspace',owner:recoveryOwnerPacks.programs,state:'workspace_candidate',reason:'Programme material continues in the existing Programs workspace.'},
  'source-family':{owner_type:'workspace',owner:recoveryOwnerPacks.research,state:'workspace_candidate',reason:'Source-family requirements continue in the Source library owner.'},
  'partner-mechanism':{owner_type:'workspace',owner:recoveryOwnerPacks.connections,state:'workspace_candidate',reason:'Partner mechanisms continue through Connection Studio and its scoped account connections.'},
  'institution-setup-flow':{owner_type:'surface',owner:'BonfyreWebAddress',route:'/packs/setup?context=self',state:'surface_owner',reason:'Institution setup belongs to the signed-in PackGraph setup flow.'},
  'institution-continuity':{owner_type:'surface',owner:'BonfyreWebAddress',route:'/apps?context=self',state:'surface_owner',reason:'Institution continuity belongs to account-scoped application continuation.'}
});

const installedMap=installed=>new Map((installed??[]).map(row=>[row.pack,row]));

export function routeRecoveredNative(record,{installed=[]}={}){
  const identity=recoveryIdentity(record);
  const kind=identity.kinds.length===1?identity.kinds[0]:'';
  let selected=structural[kind]??null;
  if(selected?.owner_type==='workspace'){
    const installation=installedMap(installed).get(selected.owner);
    selected={...selected,installed:Boolean(installation),ready:Boolean(installation?.ready),binding:installation?.binding??null,editor_url:installation?.editor_url??null};
    return {schema:'bonfyre.recovered-native-route.v1',identity,state:selected.ready?'owner_available':selected.installed?'owner_unready':'owner_not_installed',selected,candidates:[selected],boundary:'A recovered route is a continuation target only; historical instructions remain inert until the current native owner admits an explicit action.'};
  }
  if(selected)return {schema:'bonfyre.recovered-native-route.v1',identity,state:selected.state,selected:{...selected},candidates:[{...selected}],boundary:'A recovered route is a continuation target only; historical instructions remain inert until the current native owner admits an explicit action.'};
  const lexical=routeRecoveredResearch(record,{installed});
  if(kind==='skill'&&!lexical.selected){
    const installation=installedMap(installed).get(recoveryOwnerPacks.analysis);
    selected={owner_type:'workspace',owner:recoveryOwnerPacks.analysis,state:'workspace_candidate',reason:'Unclassified skill material requires review in the existing skill-instruments workspace.',installed:Boolean(installation),ready:Boolean(installation?.ready),binding:installation?.binding??null,editor_url:installation?.editor_url??null};
    return {schema:'bonfyre.recovered-native-route.v1',identity,state:selected.ready?'owner_available':selected.installed?'owner_unready':'owner_not_installed',selected,candidates:[selected],boundary:'A recovered route is a continuation target only; historical instructions remain inert until the current native owner admits an explicit action.'};
  }
  return {...lexical,schema:'bonfyre.recovered-native-route.v1'};
}

export function nativeRecoveryCoverage(records,{installed=[]}={}){
  const routes=records.map(record=>routeRecoveredNative(record,{installed}));
  const states={},owners={};
  for(const route of routes){states[route.state]=(states[route.state]??0)+1;if(route.selected){const key=route.selected.owner??route.selected.pack;owners[key]=(owners[key]??0)+1;}}
  return {schema:'bonfyre.recovered-native-coverage.v1',total:routes.length,states,owners,routes};
}

export const recoveredStructuralOwners=structural;
