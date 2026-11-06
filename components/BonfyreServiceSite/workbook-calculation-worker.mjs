import {parentPort} from 'node:worker_threads';
import {evaluateWorkbook} from './workbook-evaluate.mjs';
parentPort.once('message',async data=>{
 try{parentPort.postMessage(await evaluateWorkbook(data));}
 catch(error){parentPort.postMessage({error:error.message});}
 finally{parentPort.close();}
});
