import {readLocal,saveLocal} from './store.mjs';
import {checkRoutine} from './training-model.mjs';
import {checkPlan} from './training-plan.mjs';
function checked(value,studio){
  if(!value||typeof value.available!=='boolean'||!Array.isArray(value.programs)||value.programs.length>100)throw Error('Invalid studio catalogue.');
  const ids=new Set();for(const p of value.programs){checkRoutine(p.routine);if(p.routine.source?.creator!==studio||p.routine.source.program!==p.key||p.routine.source.version!==String(p.version)||!Number.isSafeInteger(p.version)||p.version<1||typeof p.description!=='string'||p.description.length>1000||ids.has(p.routine.id))throw Error('Program does not match its studio release.');ids.add(p.routine.id);}
  if(value.plans!==undefined&&(!Array.isArray(value.plans)||value.plans.length>100))throw Error('Invalid plan catalogue.');for(const p of value.plans??[]){checkPlan(p.plan);if(p.plan.source?.creator!==studio||p.plan.source.program!==p.key||p.plan.source.version!==String(p.version)||!Number.isSafeInteger(p.version)||p.version<1||ids.has(p.plan.id))throw Error('Plan does not match its studio release.');ids.add(p.plan.id);}
  return value;
}
export async function loadCatalogue(studio,{strict=false}={}){
  const key=`training:catalogue:${studio}`,stored=(await readLocal(key)).find(r=>r.key===key);let cached={programs:[],checkedAt:null,available:false};
  if(stored){try{cached=checked(stored.value,studio);}catch{/* A damaged public cache must not prevent opening personal training. */}}
  try{
    const response=await fetch('/api/training/programs/'+encodeURIComponent(studio),{cache:'no-store',signal:AbortSignal.timeout(3000)});if(!response.ok)throw Error('Could not refresh studio programs.');
    const result=checked(await response.json(),studio);if(!result.available)return {...cached,fresh:false};
    const value={programs:result.programs,plans:result.plans??[],checkedAt:Date.now(),available:true};await saveLocal(key,value,stored?.revision??0);return {...value,fresh:true};
  }catch(error){if(strict)throw error;return {...cached,fresh:false};}
}
