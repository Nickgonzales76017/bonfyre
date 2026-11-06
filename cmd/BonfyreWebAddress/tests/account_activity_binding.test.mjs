// Real native codec tests for identity-stable Inbox actions and bound Queue reads.
import {afterAll,beforeAll,expect,test} from 'bun:test';
import {mkdtempSync,readFileSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {resolve,join} from 'node:path';
import {execFileSync} from 'node:child_process';
import {encode,decode} from '../../../integrations/browser/workspace-msgpack.mjs';
const root=resolve(import.meta.dir,'../../..');let home,binary;
const binding={actor:'user:7',subject:'institution:acme',scope:'institution'};
const change={id:'account-shared',version:'a'.repeat(64),revision:0,action:'dismiss',requestId:'native-binding-001'};
beforeAll(()=>{
 home=mkdtempSync(join(tmpdir(),'account-binding-'));binary=join(home,'codec');
 const header=readFileSync(join(root,'cmd/BonfyreWebAddress/src/account_activity_web.h'),'utf8').split('static int serve_account_activity(')[0]+'\n#endif\n';
 const source=`#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "bf_msgpack.h"
#define AW_PACKET (512u*1024u)
static int psetup_decode(const char *value,size_t len,char *out,size_t cap){if(len>=cap)return 0;memcpy(out,value,len);out[len]=0;return 1;}
${header}
int main(int argc,char **argv){
 if(argc!=5)return 2;unsigned char input[AW_PACKET+1];size_t bytes=fread(input,1,sizeof(input),stdin);if(ferror(stdin))return 2;
 bf_mp_writer_t out;bf_mp_writer_init(&out,AW_PACKET+768);
 int result=!strcmp(argv[1],"change")?aaw_inbox_change((const char*)input,bytes,argv[2],argv[3],argv[4],&out):aaw_bound_envelope(input,bytes,argv[2],argv[3],argv[4],&out);
 printf("%d\\n",result);if(result==1&&fwrite(out.buf,1,out.len,stdout)!=out.len)return 3;
 bf_mp_writer_free(&out);return 0;
}`;
 writeFileSync(join(home,'codec.c'),source);
 execFileSync(process.env.CC||'cc',['-std=c11','-O1','-Wall','-Wextra','-Werror','-Wno-unused-function','-Wno-misleading-indentation',
  '-I'+join(root,'lib/libbonfyre/include'),join(home,'codec.c'),join(root,'lib/libbonfyre/bf_msgpack.c'),'-o',binary],{stdio:'pipe'});
});
afterAll(()=>{if(home)rmSync(home,{recursive:true,force:true});});
function native(mode,input,current=binding){
 const bytes=execFileSync(binary,[mode,current.actor,current.subject,current.scope],{input}),end=bytes.indexOf(10);
 const result=Number(bytes.subarray(0,end).toString());return {result,bytes:bytes.subarray(end+1),value:result===1?decode(bytes.subarray(end+1)):null};
}
const joinBytes=(...parts)=>Uint8Array.from(parts.flatMap(part=>[...part]));
const rawMap=entries=>joinBytes([0x80+entries.length],...entries.map(([key,value])=>joinBytes(encode(key),value)));
const requestWithBinding=value=>joinBytes([0x92,1],rawMap([...Object.entries(change).map(([key,value])=>[key,encode(value)]),['expectedBinding',value]]));
test('accepted mutations reach Queue with only its original five fields and unchanged request identity',()=>{
 for(const current of [binding,{actor:'user:7',subject:'user:7',scope:'self'},{actor:'user:7',subject:'household:home',scope:'household'}]){
  const result=native('change',encode([1,{...change,expectedBinding:current}]),current);
  expect(result.result).toBe(1);expect(result.value).toEqual([1,change]);expect(Object.keys(result.value[1])).toHaveLength(5);
 }
});
test('actor, destination and scope switches refuse an admitted old view before forwarding any payload',()=>{
 for(const current of [{...binding,actor:'user:8'},{...binding,subject:'institution:other'},{...binding,scope:'household'}]){
  const result=native('change',encode([1,{...change,expectedBinding:binding}]),current);
  expect(result.result).toBe(-1);expect(result.bytes.byteLength).toBe(0);
 }
});
test('missing, duplicated, nested and extra binding keys cannot become an identity guard',()=>{
 const invalid=[encode(null),encode({actor:binding.actor,subject:binding.subject}),encode({...binding,role:'admin'}),
  encode({...binding,actor:{value:'user:7'}}),encode({...binding,actor:'user:7\0'}),
  rawMap([['actor',encode('user:7')],['actor',encode('user:7')],['scope',encode('institution')]])];
 for(const value of invalid)expect(native('change',requestWithBinding(value)).result).toBe(0);
 expect(native('change',encode([1,change])).result).toBe(0);
});
test('ambiguous or interrupted mutation packets never forward a Queue change',()=>{
 const input=encode([1,{...change,expectedBinding:binding}]);
 const duplicated=joinBytes([0x92,1],rawMap([
  ['id',encode(change.id)],['id',encode(change.id)],['revision',encode(0)],['action',encode('read')],['requestId',encode(change.requestId)],['expectedBinding',encode(binding)]]));
 for(const bytes of [input.slice(0,-1),joinBytes(input,[0]),duplicated,encode([2,{...change,expectedBinding:binding}]),encode([1,{...change,expectedBinding:binding,actor:'user:7'}])])expect(native('change',bytes).result).toBe(0);
 for(const patch of [{revision:-1},{revision:0.5},{revision:Number.MAX_SAFE_INTEGER},{action:'cancel'},{requestId:'short'},{version:'A'.repeat(64)}])expect(native('change',encode([1,{...change,...patch,expectedBinding:binding}])).result).toBe(0);
});
test('successful lists, details, Inbox acknowledgements and retained receipts carry fresh native bindings',()=>{
 for(const payload of [{tasks:[],more:false,cursor:''},{task:{id:change.id}},{notices:[],more:false,cursor:''},
  {requestId:change.requestId,replayed:false,notice:{id:change.id}},{requestId:change.requestId,replayed:true,notice:{id:change.id}}]){
  const result=native('response',encode([1,true,payload]));
  expect(result.result).toBe(1);expect(result.value).toEqual([1,true,{...payload,binding,source:'account-activity'}]);
 }
 const switched={...binding,actor:'user:8'};
 expect(native('response',encode([1,true,{tasks:[]}]),switched).value[2].binding).toEqual(switched);
});
test('native failure receipts retain their exact bytes without acquiring a success binding',()=>{
 const bytes=encode([1,false,{code:'version_conflict',message:'The current notice changed.'}]);
 const result=native('response',bytes);expect(result.result).toBe(1);expect(new Uint8Array(result.bytes)).toEqual(bytes);
});
test('malformed, colliding, duplicate and excessive-depth native replies fail closed',()=>{
 const good=encode([1,true,{tasks:[]}]);
 const duplicate=joinBytes([0x93,1,0xc3],rawMap([['tasks',encode([])],['tasks',encode([])]]));
 const deep=joinBytes([0x93,1,0xc3],rawMap([['tasks',joinBytes(new Uint8Array(27).fill(0x91),[0xc0])]]));
 for(const bytes of [good.slice(0,-1),joinBytes(good,[0]),duplicate,deep,encode([1,true,[]]),encode([1,true,{binding}]),encode([1,true,{source:'spoof'}])]){
  expect(native('response',bytes).result).toBe(0);
 }
});
