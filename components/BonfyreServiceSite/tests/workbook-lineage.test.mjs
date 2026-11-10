import {test,expect} from 'bun:test';
import * as XLSX from '../../BonfyreDecision/vendor/sheetjs-0.20.3/xlsx.mjs';
import {workbookSnapshot} from '../workbook-model.mjs';
import {calculateWorkbook} from '../workbook-calculation.mjs';
import {inspectWorkbookDependencies,projectWorkbookDependencies,readWorkbookDependencies,parseCellAddress} from '../workbook-lineage.mjs';
function sample(){
 const book=XLSX.utils.book_new();
 XLSX.utils.book_append_sheet(book,XLSX.utils.aoa_to_sheet([[120,{t:'n',f:'SUM(A1:A2)',v:0},{t:'n',f:'B1*2',v:0},{t:'s',f:'IF(TRUE,"A1","B2")',v:''}],[80]]),"Drivers' Opex");
 XLSX.utils.book_append_sheet(book,XLSX.utils.aoa_to_sheet([[{t:'n',f:"'Drivers'' Opex'!C1+10",v:0},{t:'n',f:"SUM('Drivers'' Opex'!A:A)",v:0}]]),'Summary');
 return workbookSnapshot(book,{id:'lineage-test',name:'Formula lineage fixture'});
}
test('the actual formula engine supplies ranges, quoted sheet references and direct/transitive dependencies',async()=>{
 const result=await calculateWorkbook(sample(),{includeLineage:true}),graph=result.lineage;expect(result.errors).toEqual([]);expect(graph.nodes).toHaveLength(5);
 const selected=inspectWorkbookDependencies(graph,{sheetId:'sheet-0',row:0,column:0});
 expect(selected.directDependents.map(n=>[n.sheetId,n.row,n.column])).toEqual([['sheet-0',0,1],['sheet-1',0,1]]);
 expect(selected.indirectDependents.map(n=>[n.sheetId,n.row,n.column])).toEqual([['sheet-0',0,2],['sheet-1',0,0]]);
 const quoted=inspectWorkbookDependencies(graph,{sheetId:'sheet-1',row:0,column:0});expect(quoted.inputs).toEqual([{sheetId:'sheet-0',startRow:0,endRow:0,startColumn:2,endColumn:2}]);
 expect(inspectWorkbookDependencies(graph,{sheetId:'sheet-0',row:0,column:3}).inputs).toEqual([]);
 expect(graph.nodes.find(n=>n.sheetId==='sheet-1'&&n.column===1).inputs).toEqual([{sheetId:'sheet-0',startRow:0,endRow:99,startColumn:0,endColumn:0}]);
 expect(result.snapshot.sheets['sheet-1'].cellData[0][0].v).toBe(410);
},20000);
test('changing a formula rebuilds its dependencies and preserves source input data',async()=>{
 const snapshot=sample(),before=structuredClone(snapshot),first=await calculateWorkbook(snapshot,{includeLineage:true});
 expect(snapshot).toEqual(before);const next=structuredClone(first.snapshot);next.sheets['sheet-0'].cellData[0][2].f='=A2*3';
 const after=await calculateWorkbook(next,{includeLineage:true}),view=inspectWorkbookDependencies(after.lineage,{sheetId:'sheet-0',row:0,column:0});
 expect(view.indirectDependents).toEqual([]);expect(after.snapshot.sheets['sheet-1'].cellData[0][0].v).toBe(250);expect(first.lineage.nodes.find(n=>n.column===2).formula).toBe('=B1*2');
},20000);
test('missing or mismatched engine nodes cannot become a complete workbook graph',()=>{
 const snapshot=sample();expect(()=>projectWorkbookDependencies(snapshot,[])).toThrow('every current formula');
 const tree={unitId:snapshot.id,subUnitId:'sheet-0',treeId:1,row:0,column:1,formula:'=A1',rangeList:[],parents:[]};expect(()=>projectWorkbookDependencies(snapshot,[tree])).toThrow('differs');
 expect(()=>parseCellAddress('A1:B2',snapshot.sheets['sheet-0'])).toThrow('one cell');expect(()=>parseCellAddress('A0',snapshot.sheets['sheet-0'])).toThrow();expect(()=>parseCellAddress('A101',snapshot.sheets['sheet-0'])).toThrow('within');
 expect(parseCellAddress('$B$2',snapshot.sheets['sheet-0'])).toEqual({row:1,column:1});
});
test('obsolete dependency results are refused, including empty workbooks',async()=>{
 const book=XLSX.utils.book_new();XLSX.utils.book_append_sheet(book,XLSX.utils.aoa_to_sheet([[1]]),'Values');const snapshot=workbookSnapshot(book);
 await expect(readWorkbookDependencies({snapshot,formula:{getAllDependencyTrees:async()=>[]},generation:1,currentGeneration:()=>2})).rejects.toThrow('changed');
 expect((await calculateWorkbook(snapshot,{includeLineage:true})).lineage.nodes).toEqual([]);
 expect(()=>calculateWorkbook(snapshot,{includeLineage:'yes'})).toThrow('whether');
});
