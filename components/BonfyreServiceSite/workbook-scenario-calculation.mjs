import {validateWorkbook} from './workbook-model.mjs';
function runWorker(snapshot,analysis,{timeout=12000,signal,workerFactory=()=>new Worker(new URL('./workbook-scenario-worker.js',import.meta.url),{type:'module'})}={}){
 validateWorkbook(snapshot);
 if(!Number.isInteger(timeout)||timeout<1||timeout>30000)throw Error('Invalid scenario calculation deadline.');
 if(signal?.aborted)return Promise.reject(Error('Scenario calculation cancelled.'));
 return new Promise((resolve,reject)=>{
  let worker,timer,settled=false;
  const finish=(error,result)=>{if(settled)return;settled=true;clearTimeout(timer);signal?.removeEventListener('abort',cancel);worker?.terminate();if(error)reject(error);else resolve(result);};
  const cancel=()=>finish(Error('Scenario calculation cancelled.'));
  try{
   worker=workerFactory();timer=setTimeout(()=>finish(Error('Scenario calculation exceeded its deadline. No changes were applied.')),timeout);
   signal?.addEventListener('abort',cancel,{once:true});
   worker.onmessage=({data})=>finish(data?.error?Error(data.error):null,data);
   worker.onerror=()=>finish(Error('Scenario calculation failed. No changes were applied.'));
   worker.onmessageerror=()=>finish(Error('Scenario calculation returned an unreadable result.'));
   worker.postMessage({snapshot:structuredClone(snapshot),...(analysis?{analysis:structuredClone(analysis)}:{})});
  }catch(error){finish(error);}
 });
}

export const calculateScenario=(snapshot,options)=>runWorker(snapshot,null,options);
export const calculateWorkbookAnalysis=(snapshot,analysis,options={})=>runWorker(snapshot,analysis,{timeout:30000,...options});
