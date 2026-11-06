import {validateWorkbook, calculationErrors} from './workbook-model.mjs';
import {cellAddress} from './workbook-lineage.mjs';

export const scenarioLimit=64*1024;
const fail=message=>{throw Error(message);};
const label=value=>typeof value==='string'&&value.trim().length>0&&value.length<=100;
const id=value=>typeof value==='string'&&/^[a-zA-Z0-9_-]{1,100}$/.test(value);
const number=value=>typeof value==='number'&&Number.isFinite(value);
const distinct=(rows,key)=>new Set(rows.map(key)).size===rows.length;
export const createScenarioBook=snapshot=>({schema:'bonfyre.workbook-scenarios.v1',workbookId:snapshot.id,drivers:[],sets:[]});
export function validateScenarioBook(book,snapshot){
 if(!book||book.schema!=='bonfyre.workbook-scenarios.v1'||book.workbookId!==snapshot.id||!Array.isArray(book.drivers)||book.drivers.length>24||!Array.isArray(book.sets)||book.sets.length>24)fail('Invalid workbook assumption sets.');
 if(new TextEncoder().encode(JSON.stringify(book)).length>scenarioLimit)fail('Assumption sets exceed 64 KiB.');
 for(const d of book.drivers)if(!id(d.id)||!label(d.name)||!id(d.sheetId)||!label(d.sheetName)||!Number.isInteger(d.row)||d.row<0||d.row>=5000||!Number.isInteger(d.column)||d.column<0||d.column>=128||!Number.isInteger(d.rowCount)||d.rowCount<1||d.rowCount>5000||!Number.isInteger(d.columnCount)||d.columnCount<1||d.columnCount>128)fail('Invalid named driver.');
 if(!distinct(book.drivers,d=>d.id)||!distinct(book.drivers,d=>d.name.trim().toLowerCase())||!distinct(book.drivers,d=>JSON.stringify([d.sheetId,d.row,d.column])))fail('Choose distinct driver names and cells.');
 for(const s of book.sets){
  if(!id(s.id)||!label(s.name)||!Array.isArray(s.values)||s.values.length!==book.drivers.length||!s.values.length||!distinct(s.values,v=>v.driverId)||s.values.some(v=>!book.drivers.some(d=>d.id===v.driverId)||!number(v.value)))fail('Each assumption set needs one finite number for every driver.');
 }
 if(!distinct(book.sets,s=>s.id)||!distinct(book.sets,s=>s.name.trim().toLowerCase()))fail('Choose distinct assumption set names.');
 return book;
}
export function currentDriver(snapshot,driver){
 const sheet=snapshot.sheets[driver.sheetId];
 if(!snapshot.sheetOrder.includes(driver.sheetId)||sheet.name!==driver.sheetName||sheet.rowCount!==driver.rowCount||sheet.columnCount!==driver.columnCount)fail('The worksheet structure changed. Remove and bind this driver again: '+driver.name);
 const cell=sheet.cellData?.[driver.row]?.[driver.column];
 if(!cell||cell.f||!number(cell.v))fail('Choose a numeric input cell without a formula: '+driver.name);
 return {value:cell.v,address:sheet.name+'!'+cellAddress(driver.row,driver.column)};
}
export function addScenarioDriver(snapshot,book,{name,sheetId,row,column}){
 validateWorkbook(snapshot);validateScenarioBook(book,snapshot);
 const sheet=snapshot.sheets[sheetId];
 if(!sheet||!snapshot.sheetOrder.includes(sheetId)||!Number.isInteger(row)||row<0||row>=sheet.rowCount||!Number.isInteger(column)||column<0||column>=sheet.columnCount)fail('Choose a cell within the worksheet.');
 const driver={id:crypto.randomUUID(),name:name.trim(),sheetId,sheetName:sheet.name,row,column,rowCount:sheet.rowCount,columnCount:sheet.columnCount};
 const {value}=currentDriver(snapshot,driver),next=structuredClone(book);next.drivers.push(driver);
 for(const set of next.sets)set.values.push({driverId:driver.id,value});
 return validateScenarioBook(next,snapshot);
}
export function removeScenarioDriver(snapshot,book,driverId){
 validateScenarioBook(book,snapshot);const next=structuredClone(book);
 next.drivers=next.drivers.filter(d=>d.id!==driverId);
 for(const s of next.sets)s.values=s.values.filter(v=>v.driverId!==driverId);
 if(!next.drivers.length)next.sets=[];
 return validateScenarioBook(next,snapshot);
}
export function saveAssumptionSet(snapshot,book,{name,values,setId=null}){
 validateScenarioBook(book,snapshot);
 for(const d of book.drivers)currentDriver(snapshot,d);
 const next=structuredClone(book),set={id:setId??crypto.randomUUID(),name:name.trim(),values:structuredClone(values)};
 if(setId){const i=next.sets.findIndex(s=>s.id===setId);if(i<0)fail('This assumption set was removed.');next.sets[i]=set;}else next.sets.push(set);
 return validateScenarioBook(next,snapshot);
}
export function planAssumptionSet(snapshot,book,setId){
 validateWorkbook(snapshot);validateScenarioBook(book,snapshot);
 const set=book.sets.find(s=>s.id===setId);if(!set)fail('Choose a saved assumption set.');
 const next=structuredClone(snapshot),changes=[];
 for(const d of book.drivers){
  const before=currentDriver(snapshot,d),value=set.values.find(v=>v.driverId===d.id).value;
  next.sheets[d.sheetId].cellData[d.row][d.column]={...next.sheets[d.sheetId].cellData[d.row][d.column],v:value,t:2};
  changes.push({name:d.name,address:before.address,before:before.value,after:value});
 }
 return {snapshot:validateWorkbook(next),changes,name:set.name};
}
export async function previewAssumptionSet(snapshot,book,setId,calculate){
 const before=structuredClone(snapshot),plan=planAssumptionSet(before,book,setId);
 const baseline=await calculate(structuredClone(before));validateWorkbook(baseline.snapshot);
 if(baseline.errors?.length||calculationErrors(baseline.snapshot).length)fail('Resolve current workbook calculation errors before comparing assumptions.');
 const result=await calculate(plan.snapshot);validateWorkbook(result.snapshot);
 if(result.errors?.length||calculationErrors(result.snapshot).length)fail('Resolve scenario calculation errors before applying this set.');
 const outputs=[];let outputCount=0;
 for(const sheetId of before.sheetOrder)for(const[r,row]of Object.entries(baseline.snapshot.sheets[sheetId].cellData??{}))for(const[c,cell]of Object.entries(row)){
  if(cell.f){const next=result.snapshot.sheets[sheetId]?.cellData?.[r]?.[c];if(!next||next.f!==cell.f)fail('Calculation changed a formula.');
   if(cell.v!==next.v){outputCount++;if(outputs.length<100)outputs.push({address:before.sheets[sheetId].name+'!'+cellAddress(+r,+c),before:cell.v,after:next.v});}
  }
 }
 return {...plan,snapshot:result.snapshot,outputs,outputCount,base:JSON.stringify(before),book:JSON.stringify(book)};
}
export function acceptAssumptionPreview(snapshot,book,preview){
 if(!preview||JSON.stringify(snapshot)!==preview.base||JSON.stringify(book)!==preview.book)fail('The workbook or assumptions changed. Preview this set again.');
 validateWorkbook(preview.snapshot);if(calculationErrors(preview.snapshot).length)fail('Resolve scenario calculation errors.');
 return structuredClone(preview.snapshot);
}
