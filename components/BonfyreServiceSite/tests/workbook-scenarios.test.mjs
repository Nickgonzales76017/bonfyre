import {test,expect} from 'bun:test';
import * as XLSX from '../../BonfyreDecision/vendor/sheetjs-0.20.3/xlsx.mjs';
import {workbookSnapshot,exportWorkbookSnapshot} from '../workbook-model.mjs';
import {createScenarioBook,addScenarioDriver,saveAssumptionSet,removeScenarioDriver,planAssumptionSet,previewAssumptionSet,acceptAssumptionPreview,validateScenarioBook} from '../workbook-scenarios.mjs';
import {workbookReviewValue,readWorkbookReview} from '../workbook-record.mjs';
import {calculateWorkbook} from '../workbook-calculation.mjs';
import {calculateScenario} from '../workbook-scenario-calculation.mjs';
function fixture(){
 const book=XLSX.utils.book_new();XLSX.utils.book_append_sheet(book,XLSX.utils.aoa_to_sheet([[100],[60],[{t:'n',f:'A1-A2',v:999}],[2],[{t:'n',f:'A3/A4',v:999}]]),'Model');
 const snapshot=workbookSnapshot(book,{id:'scenario-model'});let scenarios=createScenarioBook(snapshot);
 for(const[name,row]of [['Sales',0],['Costs',1],['Months',3]])scenarios=addScenarioDriver(snapshot,scenarios,{name,sheetId:'sheet-0',row,column:0});
 scenarios=saveAssumptionSet(snapshot,scenarios,{name:'Growth',values:scenarios.drivers.map((d,i)=>({driverId:d.id,value:[140,65,2][i]}))});
 return{snapshot,scenarios,id:scenarios.sets[0].id};
}
test('scenario preview recalculates both baselines, preserves formulas and only applies an exact current candidate',async()=>{
 const{snapshot,scenarios,id}=fixture(),original=structuredClone(snapshot),preview=await previewAssumptionSet(snapshot,scenarios,id,calculateWorkbook);
 expect(snapshot).toEqual(original);expect(preview.changes.map(c=>c.after)).toEqual([140,65,2]);
 expect(preview.outputs).toContainEqual({address:'Model!A3',before:40,after:75});
 expect(preview.outputs).toContainEqual({address:'Model!A5',before:20,after:37.5});
 const accepted=acceptAssumptionPreview(snapshot,scenarios,preview);expect(accepted.sheets['sheet-0'].cellData[2][0].f).toBe('=A1-A2');
 const bytes=exportWorkbookSnapshot(accepted),reopened=await calculateWorkbook(workbookSnapshot(XLSX.read(bytes,{type:'array'})));
 expect(reopened.snapshot.sheets['sheet-0'].cellData[4][0].v).toBe(37.5);
 snapshot.sheets['sheet-0'].cellData[0][0].v=101;expect(()=>acceptAssumptionPreview(snapshot,scenarios,preview)).toThrow('changed');
 expect(()=>acceptAssumptionPreview(original,{...scenarios,sets:[]},preview)).toThrow('changed');
},30000);
test('named inputs and sets roundtrip through the existing record without replacing original bytes',async()=>{
 const{snapshot,scenarios,id}=fixture();const calculated=(await calculateWorkbook(planAssumptionSet(snapshot,scenarios,id).snapshot)).snapshot;
 const source={original:new Blob(['exact imported file'])},value=workbookReviewValue(calculated,source,null,scenarios);
 const read=await readWorkbookReview({key:'finance-workbook:scenario',revision:4,value});
 expect(read.scenarios).toEqual(scenarios);expect(await read.source.original.text()).toBe('exact imported file');
 scenarios.drivers[0].name='Changed outside record';expect(read.scenarios.drivers[0].name).toBe('Sales');
 const old=await readWorkbookReview({key:'finance-workbook:old',revision:1,value:{schema:'bonfyre.finance-workbook.v1',snapshot:calculated}});
 expect(old.scenarios).toBeNull();value.scenarios.workbookId='other';await expect(readWorkbookReview({key:'finance-workbook:scenario',revision:4,value})).rejects.toThrow('assumption');
},15000);
test('structural changes, formula drivers, duplicated cells, incomplete sets and nonfinite assumptions are refused',()=>{
 const{snapshot,scenarios,id}=fixture();
 expect(()=>addScenarioDriver(snapshot,scenarios,{name:'Duplicate',sheetId:'sheet-0',row:0,column:0})).toThrow('distinct');
 expect(()=>addScenarioDriver(snapshot,scenarios,{name:'Formula',sheetId:'sheet-0',row:2,column:0})).toThrow('numeric input');
 expect(()=>saveAssumptionSet(snapshot,scenarios,{name:'Incomplete',values:[]})).toThrow('every driver');
 const broken=structuredClone(scenarios);broken.sets[0].values[0].value=Infinity;expect(()=>validateScenarioBook(broken,snapshot)).toThrow('finite');
 snapshot.sheets['sheet-0'].rowCount++;expect(()=>planAssumptionSet(snapshot,scenarios,id)).toThrow('structure changed');
 snapshot.sheets['sheet-0'].rowCount--;snapshot.sheets['sheet-0'].cellData[0][0].f='=1';expect(()=>planAssumptionSet(snapshot,scenarios,id)).toThrow('numeric input');
});
test('driver removal keeps other set values, and adding a driver captures current values in every set',()=>{
 const{snapshot,scenarios}=fixture();const next=removeScenarioDriver(snapshot,scenarios,scenarios.drivers[1].id);
 expect(next.sets[0].values.map(v=>v.value)).toEqual([140,2]);
 const added=addScenarioDriver(snapshot,next,{name:'New costs',sheetId:'sheet-0',row:1,column:0});expect(added.sets[0].values.at(-1).value).toBe(60);
 let empty=added;for(const d of added.drivers)empty=removeScenarioDriver(snapshot,empty,d.id);expect(empty.sets).toEqual([]);
 const capped=structuredClone(scenarios);capped.sets=Array.from({length:25},(_,i)=>({...capped.sets[0],id:'set-'+i,name:'Set '+i}));expect(()=>validateScenarioBook(capped,snapshot)).toThrow('Invalid');
});
test('an error-producing assumption cannot produce an applicable preview',async()=>{
 const{snapshot,scenarios,id}=fixture();scenarios.sets[0].values[2].value=0;
 await expect(previewAssumptionSet(snapshot,scenarios,id,calculateWorkbook)).rejects.toThrow('scenario calculation');
 expect(snapshot.sheets['sheet-0'].cellData[3][0].v).toBe(2);
},25000);
test('browser calculation deadline, cancellation, delivery errors and start failures terminate without accepting late results',async()=>{
 const{snapshot}=fixture();let ended=0,worker;
 const workerFactory=()=>worker={terminate(){ended++;},postMessage(){}};
 await expect(calculateScenario(snapshot,{timeout:1,workerFactory})).rejects.toThrow('deadline');expect(ended).toBe(1);
 const c=new AbortController(),pending=calculateScenario(snapshot,{signal:c.signal,workerFactory});c.abort();await expect(pending).rejects.toThrow('cancelled');expect(ended).toBe(2);
 const result=calculateScenario(snapshot,{workerFactory});worker.onmessageerror();await expect(result).rejects.toThrow('unreadable');expect(ended).toBe(3);
 await expect(calculateScenario(snapshot,{workerFactory:()=>{throw Error('start failed');}})).rejects.toThrow('start failed');
 await expect(calculateScenario(snapshot,{signal:c.signal,workerFactory})).rejects.toThrow('cancelled');expect(ended).toBe(3);
});
