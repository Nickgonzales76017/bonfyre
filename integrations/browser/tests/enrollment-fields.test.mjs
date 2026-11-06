import {test} from 'node:test';
import assert from 'node:assert/strict';
import {enrollmentFields} from '../onboarding/enrollment-fields.mjs';
const field={field:'api_key',label:'API key',sensitive:true,required:true,help:'Copy from the provider account.'};
const declaration=fields=>JSON.stringify([{provider:'fixture',fields}]);
test('enrollment projects exact public fields without stored values or unrelated providers',()=>{
 const result=enrollmentFields(JSON.stringify([{provider:'other',fields:[{...field,field:'other_key'}]},{provider:'fixture',value:'not a credential projection',fields:[{...field,value:'never-return',set:true},{...field,field:'project_id',label:'Project ID',sensitive:false,required:false}]}]),'fixture');
 assert.deepEqual(result,[field,{...field,field:'project_id',label:'Project ID',sensitive:false,required:false}]);
 assert.equal(JSON.stringify(result).includes('never-return'),false);assert.equal('value' in result[0],false);assert.equal('set' in result[0],false);
});
test('missing, duplicate, oversized, and malformed provider declarations expose no entry fields',()=>{
 for(const source of ['{',JSON.stringify({provider:'fixture',fields:[field]}),JSON.stringify([]),JSON.stringify([{provider:'other',fields:[field]}]),JSON.stringify([{provider:'fixture',fields:[field]},{provider:'fixture',fields:[field]}]),' '.repeat(1024*1024+1)])assert.equal(enrollmentFields(source,'fixture'),null);
 assert.equal(enrollmentFields(declaration([field]),'../fixture'),null);
});
test('field declarations preserve types and refuse duplicates or unsafe labels',()=>{
 for(const fields of [[],Array.from({length:33},(_,i)=>({...field,field:'key_'+i})),[field,field],[{...field,sensitive:'true'}],[{...field,required:null}],[{...field,label:''}],[{...field,label:'API\0key'}],[{...field,field:'custom.key'}],[{...field,help:'x'.repeat(401)}]])assert.equal(enrollmentFields(declaration(fields),'fixture'),null);
 assert.deepEqual(enrollmentFields(declaration([{...field,help:undefined}]),'fixture'),[{...field,help:''}]);
});
