import {test} from 'node:test';
import assert from 'node:assert/strict';
import {foreignSignInFields,signInAccountChoices} from '../extension/foreign-sign-in.mjs';
test('sign-in presentation accepts only bounded adapter fields without initial values',()=>{
 const input={name:'api_key',label:'API key',kind:'secret',required:true,maxLength:4095};const fields=foreignSignInFields([input]);assert.deepEqual(fields,[{...input,help:''}]);assert.equal(Object.isFrozen(fields[0]),true);assert.equal('help' in input,false);
 for(const patch of [{value:'secret'},{defaultValue:'secret'},{autocomplete:'current-password'},{kind:'password'},{name:'user.password'},{maxLength:4096},{required:'yes'},{label:''},{help:'x'.repeat(401)}])assert.throws(()=>foreignSignInFields([{...input,...patch}]));
 assert.throws(()=>foreignSignInFields([input,input]));assert.throws(()=>foreignSignInFields(Array.from({length:9},(_,i)=>({...input,name:'field_'+i}))));
});
test('account choices contain admitted display identities only and never credential payloads',()=>{
 const input={id:'gmail',identifier:'fixture@example.invalid',label:'Regular browser account'};
 const result=signInAccountChoices([input],'gmail');assert.equal(Object.isFrozen(result),true);assert.equal(Object.isFrozen(result[0]),true);assert.equal(result[0].identifier,input.identifier);assert.equal('note' in input,false);
 for(const extra of [{password:'hidden'},{secret:'hidden'},{token:'hidden'},{value:'hidden'},{identifier:'x'.repeat(321)},{id:'untrusted/identity'}])assert.throws(()=>signInAccountChoices([{...input,...extra}],'gmail'));
 for(const rows of [[],[input,input],null,Array(9).fill(input)])assert.throws(()=>signInAccountChoices(rows,'gmail'));assert.throws(()=>signInAccountChoices([input],'proton'));
});
