import {validateWorkbook} from './workbook-model.mjs';
const check=(condition,message)=>{if(!condition)throw Error(message);};
const integer=(n,max)=>Number.isSafeInteger(n)&&n>=0&&n<max;
const identity=(sheet,row,column)=>JSON.stringify([sheet,row,column]);
export function cellAddress(row,column){let letters='';for(let n=column+1;n;n=Math.floor((n-1)/26))letters=String.fromCharCode(65+(n-1)%26)+letters;return letters+(row+1);}
export function parseCellAddress(value,sheet){
 const m=/^\$?([A-Z]{1,3})\$?([1-9][0-9]*)$/i.exec(String(value).trim());check(m,'Choose one cell, such as B4.');
 const column=[...m[1].toUpperCase()].reduce((n,c)=>n*26+c.charCodeAt(0)-64,0)-1,row=Number(m[2])-1;
 check(integer(row,sheet.rowCount)&&integer(column,sheet.columnCount),'Choose a cell within this worksheet.');return{row,column};
}
// The formula engine supplies parsed references and graph edges. No formula text
// is parsed here, and ranges stay ranges instead of becoming millions of cells.
export function projectWorkbookDependencies(snapshot,trees){
 validateWorkbook(snapshot);check(Array.isArray(trees)&&trees.length<=20000,'Dependency inspection supports at most 20,000 formulas.');
 const expected=new Map();for(const sheetId of snapshot.sheetOrder)for(const[r,row]of Object.entries(snapshot.sheets[sheetId].cellData??{}))for(const[c,cell]of Object.entries(row))if(cell.f)expected.set(identity(sheetId,+r,+c),cell.f);
 check(expected.size<=20000,'Dependency inspection supports at most 20,000 formulas.');
 const ids=new Map(),seen=new Set();
 const nodes=trees.map(tree=>{
  check(tree&&tree.unitId===snapshot.id&&snapshot.sheets[tree.subUnitId]&&integer(tree.treeId,2147483647)&&!ids.has(tree.treeId)&&integer(tree.row,5000)&&integer(tree.column,128),'The engine returned an unknown formula identity.');
  const id=identity(tree.subUnitId,tree.row,tree.column);check(!seen.has(id)&&expected.get(id)===tree.formula,'The formula graph differs from the current workbook.');seen.add(id);ids.set(tree.treeId,id);
  check(Array.isArray(tree.rangeList)&&tree.rangeList.length<=20000&&Array.isArray(tree.parents),'The engine returned incomplete formula references.');
  const inputs=tree.rangeList.map(item=>{
   const r=item.range;check(item.unitId===snapshot.id&&snapshot.sheets[item.sheetId]&&r&&integer(r.startRow,5000)&&integer(r.endRow,5000)&&integer(r.startColumn,128)&&integer(r.endColumn,128)&&r.endRow>=r.startRow&&r.endColumn>=r.startColumn,'A formula refers outside the supported workbook.');
   return{sheetId:item.sheetId,startRow:r.startRow,endRow:r.endRow,startColumn:r.startColumn,endColumn:r.endColumn};
  });
  return{id,sheetId:tree.subUnitId,row:tree.row,column:tree.column,formula:tree.formula,inputs,dependentTreeIds:tree.parents};
 });
 check(seen.size===expected.size,'The engine did not return every current formula.');
 let edges=0;for(const node of nodes){node.dependents=[...new Set(node.dependentTreeIds.map(id=>{check(ids.has(id),'The engine returned an incomplete dependency edge.');return ids.get(id);} ))];edges+=node.dependents.length+node.inputs.length;delete node.dependentTreeIds;}
 check(edges<=200000,'The formula graph exceeds the inspection limit.');
 return{schema:'bonfyre.workbook-dependencies.v1',workbookId:snapshot.id,workbookName:snapshot.name,engine:'Univer 1.1.0',sheets:snapshot.sheetOrder.map(id=>({id,name:snapshot.sheets[id].name,rowCount:snapshot.sheets[id].rowCount,columnCount:snapshot.sheets[id].columnCount})),nodes};
}
export function inspectWorkbookDependencies(graph,{sheetId,row,column}){
 const sheet=graph.sheets.find(s=>s.id===sheetId);check(sheet&&integer(row,sheet.rowCount)&&integer(column,sheet.columnCount),'Choose a current workbook cell.');
 const byId=new Map(graph.nodes.map(n=>[n.id,n])),id=identity(sheetId,row,column),formula=byId.get(id);
 const direct=graph.nodes.filter(n=>n.inputs.some(r=>r.sheetId===sheetId&&row>=r.startRow&&row<=r.endRow&&column>=r.startColumn&&column<=r.endColumn));
 const visited=new Set(direct.map(n=>n.id)),queue=[...visited];for(let at=0;at<queue.length;at++)for(const next of byId.get(queue[at]).dependents)if(!visited.has(next)){visited.add(next);queue.push(next);}
 return{cell:{sheetId,row,column,address:cellAddress(row,column),label:sheet.name+'!'+cellAddress(row,column)},formula:formula?.formula??null,inputs:formula?.inputs??[],directDependents:direct,indirectDependents:queue.filter(id=>!direct.some(n=>n.id===id)).map(id=>byId.get(id))};
}
export async function readWorkbookDependencies({snapshot,formula,generation,currentGeneration}){
 const graph=projectWorkbookDependencies(snapshot,await formula.getAllDependencyTrees(8000));
 check(currentGeneration()===generation,'The workbook changed during dependency inspection. Inspect it again.');return graph;
}
