import {importTraces,analyzeTraces} from './traces.mjs';
self.onmessage=({data})=>{try{if(data.action!=='traces')throw Error('Unknown observed-data operation.');const parsed=importTraces(data.text,data.mapping);self.postMessage({id:data.id,ok:true,parsed,result:analyzeTraces(parsed.events)});}catch(e){self.postMessage({id:data.id,ok:false,error:e.message});}};
