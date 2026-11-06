import {test,expect} from 'bun:test';
import * as XLSX from '../../BonfyreDecision/vendor/sheetjs-0.20.3/xlsx.mjs';
import {workbookSnapshot} from '../workbook-model.mjs';
import {createScenarioBook,addScenarioDriver} from '../workbook-scenarios.mjs';
import {analyzeWorkbook} from '../workbook-analysis.mjs';
import {calculateWorkbook} from '../workbook-calculation.mjs';
function fixture(){
 const x=XLSX.utils.book_new();XLSX.utils.book_append_sheet(x,XLSX.utils.aoa_to_sheet([[100],[60],[{t:'n',f:'A1-A2',v:-999}],[2],[{t:'n',f:'A3/A4',v:-999}]]),'Model');
 const snapshot=workbookSnapshot(x,{id:'analysis-test'}),book=addScenarioDriver(snapshot,createScenarioBook(snapshot),{name:'Sales',sheetId:'sheet-0',row:0,column:0});
 return{snapshot,request:{book,driverId:book.drivers[0].id,target:{sheetId:'sheet-0',row:4,column:0}}};
}
test('sensitivity evaluates every requested input and its baseline through real formulas without changing the workbook',async()=>{
 const{snapshot,request}=fixture(),before=structuredClone(snapshot);
 const result=await analyzeWorkbook(snapshot,{...request,kind:'sensitivity',values:[80,100,140]},calculateWorkbook);
 expect(result.baseline).toEqual({input:100,result:20});expect(result.rows).toEqual([{input:80,result:10},{input:100,result:20},{input:140,result:40}]);expect(result.evaluations).toBe(3);expect(snapshot).toEqual(before);
},15000);
test('goal seeking returns a checked numeric result within tolerance, including endpoints and a current match',async()=>{
 const{snapshot,request}=fixture(),args={...request,kind:'goal',minimum:100,maximum:180,goal:40,tolerance:0.00001};
 const found=await analyzeWorkbook(snapshot,args,calculateWorkbook);expect(found.solution).toEqual({input:140,result:40,residual:0});expect(found.evaluations).toBe(3);
 const end=await analyzeWorkbook(snapshot,{...args,goal:60},calculateWorkbook);expect(end.solution.input).toBe(180);
 const current=await analyzeWorkbook(snapshot,{...args,goal:20},calculateWorkbook);expect(current.solution.input).toBe(100);expect(current.evaluations).toBe(1);
 expect(snapshot.sheets['sheet-0'].cellData[0][0].v).toBe(100);
},15000);
test('invalid input, nonformula result, unbracketed target and calculation failures return no solution',async()=>{
 const{snapshot,request}=fixture(),args={...request,kind:'sensitivity',values:[80,140]};
 for(const values of [[1],[1,1],[1,Infinity],Array.from({length:16},(_,i)=>i)])await expect(analyzeWorkbook(snapshot,{...args,values},calculateWorkbook)).rejects.toThrow('two to 15');
 await expect(analyzeWorkbook(snapshot,{...args,target:{sheetId:'sheet-0',row:0,column:0}},calculateWorkbook)).rejects.toThrow('formula cell');
 await expect(analyzeWorkbook(snapshot,{...request,kind:'goal',minimum:80,maximum:140,goal:100,tolerance:0.01},calculateWorkbook)).rejects.toThrow('bracket');
 snapshot.sheets['sheet-0'].cellData[4][0].f='=1/(A1-140)';
 await expect(analyzeWorkbook(snapshot,args,calculateWorkbook)).rejects.toThrow('calculation error');
},15000);
test('a discontinuous model cannot claim convergence merely because the interval became narrow',async()=>{
 const{snapshot,request}=fixture();snapshot.sheets['sheet-0'].cellData[0][0].v=-1;let calls=0;
 const calculate=async input=>{calls++;const result=structuredClone(input);result.sheets['sheet-0'].cellData[4][0].v=input.sheets['sheet-0'].cellData[0][0].v<0?0:1;return{snapshot:result,errors:[]};};
 await expect(analyzeWorkbook(snapshot,{...request,kind:'goal',minimum:-1,maximum:1,goal:0.5,tolerance:0.01},calculate)).rejects.toThrow('24 search steps');
 expect(calls).toBe(26);expect(snapshot.sheets['sheet-0'].cellData[0][0].v).toBe(-1);
});
test('the bounded maximum sensitivity sweep completes with the actual evaluator',async()=>{
 const{snapshot,request}=fixture(),values=Array.from({length:15},(_,i)=>80+i*10),result=await analyzeWorkbook(snapshot,{...request,kind:'sensitivity',values},calculateWorkbook);
 expect(result.rows).toHaveLength(15);expect(result.evaluations).toBe(15);expect(result.rows.at(-1)).toEqual({input:220,result:80});
},20000);
