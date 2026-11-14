import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {createEditHistory,sameEdit} from '../runtime/renderers/edit-history.mjs';
import {createReviewSheet,changeMarkup} from '../runtime/renderers/review-sheet.mjs';
import {createRecordGrid} from '../runtime/renderers/record-grid.mjs';

function historyFixture(limit=3) {
  let value={qty:1}; const history=createEditHistory({capture:()=>value,restore:v=>value=v,limit});
  return {history,get:()=>value,edit:qty=>history.transact(()=>{value.qty=qty;})};
}
test('history retains edits independently across identical record identities',()=>{
  const a=historyFixture(),b=historyFixture();a.edit(2);assert.equal(b.get().qty,1);a.history.undo();assert.equal(a.get().qty,1);a.history.redo();assert.equal(a.get().qty,2);
});
test('no-op edit preserves redo',()=>{
  const x=historyFixture();x.edit(2);x.history.undo();assert.equal(x.edit(1),false);assert.equal(x.history.canRedo,true);
});
test('invalid edit restores values and preserves redo',()=>{
  const x=historyFixture();x.edit(2);x.history.undo();assert.throws(()=>x.history.transact(()=>{x.get().qty=-1;throw Error('Invalid quantity');}));assert.equal(x.get().qty,1);assert.equal(x.history.canRedo,true);
});
test('history bounds apply to owner-selected slices without copying source state',()=>{
  let edits=0;const source={revision:1},h=createEditHistory({capture:()=>edits,restore:v=>edits=v,limit:2});for(let n=1;n<=4;n++)h.transact(()=>edits=n);source.revision=2;h.undo();h.undo();assert.equal(h.undo(),false);assert.equal(edits,2);assert.equal(source.revision,2);
});
test('failed restore preserves an undo entry',()=>{
  let value=1,fail=false;const h=createEditHistory({capture:()=>value,restore:v=>{if(fail)throw Error('Source changed');value=v;}});h.transact(()=>value=2);fail=true;assert.throws(()=>h.undo());assert.equal(h.canUndo,true);fail=false;h.undo();assert.equal(value,1);
});
test('asynchronous edits are rejected before invocation',()=>{
  const x=historyFixture();let called=false;assert.throws(()=>x.history.transact(async()=>{called=true;}));assert.equal(called,false);
});
test('editable equality preserves dates and typed values',()=>{
  assert.equal(sameEdit(new Date(1),new Date(2)),false);assert.equal(sameEdit(new Uint16Array([3]),new Uint16Array([3])),true);assert.equal(sameEdit(new Uint16Array([3]),new Uint16Array([4])),false);assert.throws(()=>sameEdit(new Map(),new Map()));
});
class Element extends EventTarget {
  constructor(){super();this.dataset={};this.open=false;this.isConnected=true;this.ownerDocument={activeElement:{isConnected:true,focus:()=>this.focused=true},createElement:()=>new Element()};}
  showModal(){this.open=true;}
  close(){this.open=false;}
}
function reviewFixture(){
  const nodes=Object.fromEntries(['dialog','title','body','confirm','status'].map(key=>[key,new Element()]));let version=1;
  return {...nodes,review:createReviewSheet({...nodes,currentVersion:()=>version}),change:()=>version++};
}
test('review escapes source text and distinguishes empty, zero and false',()=>{
  const text=changeMarkup([{title:'<img onerror=x>',field:'price',before:null,after:0},{title:'Flag',field:'active',before:true,after:false}]);assert.ok(text.includes('&lt;img'));assert.ok(text.includes('<del>Empty</del>'));assert.ok(text.includes('<ins>0</ins>'));assert.ok(text.includes('<ins>false</ins>'));
});
test('review binds the exact candidate, action and generation',()=>{
  const x=reviewFixture(),payload={id:'reviewed'};x.review.show({heading:'Review',changes:[],action:'save',label:'Save',payload});assert.equal(x.review.take('save'),payload);assert.throws(()=>x.review.take('apply'));x.change();assert.throws(()=>x.review.take('save'),/changed/);
});
test('pending review prevents cancellation and close; completed review restores focus',()=>{
  const x=reviewFixture();x.review.show({heading:'Review',changes:[],action:'save',label:'Save',payload:1});x.review.pending(true);const e=new Event('cancel',{cancelable:true});x.dialog.dispatchEvent(e);assert.equal(e.defaultPrevented,true);assert.equal(x.review.close(),false);x.review.pending(false);assert.equal(x.review.close(),true);assert.equal(x.dialog.focused,true);
});
test('destroy removes review cancellation handlers',()=>{
  const x=reviewFixture();x.review.show({heading:'Review',changes:[],action:'save',label:'Save',payload:1});x.review.pending(true);x.review.destroy();const e=new Event('cancel',{cancelable:true});x.dialog.dispatchEvent(e);assert.equal(e.defaultPrevented,false);assert.equal(x.dialog.open,false);
});
function gridFixture({rows=[{id:'a',qty:1},{id:'b',qty:2}],canEdit=()=>true,onEdit=()=>{},copyRows}={}) {
  let opts,nodes=[],dead=false;const selections=[],errors=[];const host=new Element();
  const api={setGridAriaProperty(){},getSelectedRows:()=>nodes.filter(n=>n.selected).map(n=>n.data),getEditingCells:()=>[],onFilterChanged(){},stopEditing(){},destroy(){dead=true;},
    forEachNode:fn=>nodes.forEach(fn),deselectAll(){nodes.forEach(n=>n.selected=false);opts.onSelectionChanged();},
    setGridOption(name,value){if(name==='rowData')nodes=value.map(data=>({data,selected:false,isSelected(){return this.selected;},setSelected(value){this.selected=value;opts.onSelectionChanged();}}));}};
  const library={createGrid(_host,options){opts=options;api.setGridOption('rowData',opts.rowData);return api;}};
  const grid=createRecordGrid(host,{library,rows,canEdit,onEdit,copyRows,onSelection:ids=>selections.push(ids),onError:e=>errors.push(e),columns:({setter})=>[{field:'qty',valueSetter:setter('qty',value=>{const n=Number(value);if(!Number.isSafeInteger(n)||n<1)throw Error('Invalid quantity');return n;})}]});
  return {grid,host,selections,errors,get nodes(){return nodes;},get dead(){return dead;},edit:value=>opts.columnDefs[0].valueSetter({data:nodes[0].data,newValue:value})};
}
test('grid reconciles stable selection without spurious callbacks',()=>{
  const x=gridFixture();x.nodes[1].setSelected(true);x.grid.setRows([{id:'b',qty:4},{id:'a',qty:3}]);assert.deepEqual(x.grid.selectedIds(),['b']);assert.deepEqual(x.selections,[['b']]);x.grid.clearSelection();assert.deepEqual(x.selections,[['b'],[]]);
});
test('invalid or unchanged grid edits never reach the owner',()=>{
  let count=0;const x=gridFixture({onEdit:()=>count++});x.edit('0');x.edit('1');assert.equal(count,0);assert.equal(x.errors.length,1);x.edit('3');assert.equal(count,1);
});
test('identity and current permission are checked before writes',()=>{
  let allowed=false,count=0;const rows=[{id:'a',qty:1}],x=gridFixture({rows,canEdit:()=>allowed,onEdit:(row,key,value)=>{row[key]=value;count++;}});x.edit('2');assert.equal(count,0);allowed=true;x.edit('2');assert.equal(count,1);assert.equal(rows[0].qty,1);assert.throws(()=>x.grid.setRows([{id:'a'},{id:'a'}]));assert.throws(()=>gridFixture({copyRows:rows=>rows.map(row=>({...row,id:'wrong'}))}));
});
test('destroyed grid stops edits without affecting another mount',()=>{
  let count=0;const x=gridFixture({onEdit:()=>count++}),y=gridFixture();x.grid.destroy();x.edit('5');assert.equal(count,0);assert.equal(x.dead,true);assert.equal(x.host.dataset.ready,undefined);y.grid.setRows([{id:'a',qty:7}]);assert.equal(y.nodes[0].data.qty,7);
});
test('native copies match all five components exercised by the released donor tests',()=>{
  const expected={'edit-history.mjs':'e08c5f06082478041ebfcb6a06278b50f76ca84e51f1d6463c9ae4a0becb4e2c','review-sheet.mjs':'88a354ee64f2271b6f5f866f60f8cd5fbe4f214788e2c97229a28d2511bc7013','record-grid.mjs':'0635adf187860c0bea56a381f1dd00f5d809cd25a63161334da6e506285a5444','collection-workbench.mjs':'431d9d13c36fb23f9e8ed69ec2989080de843b6f914d7abde7bb734848f946da','scene-workbench.mjs':'ebe3db9e25c119170e6d49419445e57d33091af1c60c3c89ca0ae60c32874976'};
  for(const[name,sha]of Object.entries(expected))assert.equal(createHash('sha256').update(readFileSync(new URL('../runtime/renderers/'+name,import.meta.url))).digest('hex'),sha,name+' must be reverified before changing its release identity');
});
