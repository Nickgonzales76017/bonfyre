import {validateWorkbook,calculationErrors} from './workbook-model.mjs';
import {validateScenarioBook,currentDriver} from './workbook-scenarios.mjs';
import {cellAddress} from './workbook-lineage.mjs';
const finite=value=>typeof value==='number'&&Number.isFinite(value);
const refuse=message=>{throw Error(message);};
export async function analyzeWorkbook(snapshot,request,calculate){
 validateWorkbook(snapshot);validateScenarioBook(request?.book,snapshot);
 const {book,driverId,target,kind}=request,driver=book.drivers.find(d=>d.id===driverId);
 if(!driver)refuse('Choose a named numeric driver.');
 const input=currentDriver(snapshot,driver),sheet=snapshot.sheets[target?.sheetId];
 if(!sheet||!snapshot.sheetOrder.includes(target.sheetId)||!Number.isInteger(target.row)||target.row<0||target.row>=sheet.rowCount||!Number.isInteger(target.column)||target.column<0||target.column>=sheet.columnCount||!sheet.cellData?.[target.row]?.[target.column]?.f)refuse('Choose one formula cell as the result to analyze.');
 if(!['sensitivity','goal'].includes(kind))refuse('Choose sensitivity or goal analysis.');
 if(kind==='sensitivity'&&(!Array.isArray(request.values)||request.values.length<2||request.values.length>15||request.values.some(v=>!finite(v))||new Set(request.values).size!==request.values.length))refuse('Choose two to 15 distinct finite input values.');
 if(kind==='goal'&&(!finite(request.minimum)||!finite(request.maximum)||request.minimum>=request.maximum||!finite(request.goal)||!finite(request.tolerance)||request.tolerance<=0))refuse('Choose a finite increasing input interval, target value and positive result tolerance.');
 const original=structuredClone(snapshot),formula=sheet.cellData[target.row][target.column].f,cache=new Map();let evaluations=0;
 async function point(value){
  if(cache.has(value))return cache.get(value);
  const next=structuredClone(original);next.sheets[driver.sheetId].cellData[driver.row][driver.column]={...next.sheets[driver.sheetId].cellData[driver.row][driver.column],v:value,t:2};
  const calculated=await calculate(next);validateWorkbook(calculated.snapshot);
  if(calculated.errors?.length||calculationErrors(calculated.snapshot).length)refuse('This input produces a workbook calculation error: '+value);
  const result=calculated.snapshot.sheets[target.sheetId]?.cellData?.[target.row]?.[target.column];
  if(result?.f!==formula||!finite(result.v))refuse('The result must remain a formula with a finite numeric value.');
  evaluations++;const row={input:value,result:result.v};cache.set(value,row);return row;
 }
 const baseline=await point(input.value),common={kind,driver:{id:driver.id,name:driver.name,address:input.address},target:{...target,address:sheet.name+'!'+cellAddress(target.row,target.column),formula},baseline};
 if(kind==='sensitivity'){
  const rows=[];for(const value of request.values)rows.push(await point(value));
  return {...common,rows,evaluations};
 }
 const residual=row=>{const difference=row.result-request.goal;if(!finite(difference))refuse('The target difference exceeds the supported numeric range.');return difference;};
 const accepted=row=>Math.abs(residual(row))<=request.tolerance;
 const result=row=>({...common,solution:{...row,residual:residual(row)},goal:request.goal,tolerance:request.tolerance,evaluations,method:'bounded-bisection'});
 if(baseline.input>=request.minimum&&baseline.input<=request.maximum&&accepted(baseline))return result(baseline);
 let lower=await point(request.minimum),upper=await point(request.maximum);
 if(accepted(lower))return result(lower);if(accepted(upper))return result(upper);
 if(Math.sign(residual(lower))===Math.sign(residual(upper)))refuse('The endpoint results do not bracket this target. Choose another input interval.');
 for(let iteration=0;iteration<24;iteration++){
  const mid=lower.input/2+upper.input/2;
  if(mid===lower.input||mid===upper.input)break;
  const candidate=await point(mid);if(accepted(candidate))return result(candidate);
  if(Math.sign(residual(candidate))===Math.sign(residual(lower)))lower=candidate;else upper=candidate;
 }
 refuse('No input met the result tolerance within 24 search steps. No assumptions were changed.');
}
