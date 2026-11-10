import {evaluateWorkbook} from './workbook-evaluate.mjs';
import {analyzeWorkbook} from './workbook-analysis.mjs';
self.onmessage=async({data})=>{
 try{self.postMessage(await (data.analysis?analyzeWorkbook(data.snapshot,data.analysis,snapshot=>evaluateWorkbook({snapshot})):evaluateWorkbook(data)));}
 catch(error){self.postMessage({error:error.message});}
 finally{self.close();}
};
