// Compile the gateway itself with the real MessagePack decoder. The fixture models
// only the already-tested native permission/process owners; no provider is contacted.
import {test,expect,beforeAll,afterAll} from 'bun:test';
import {mkdtempSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {resolve,join} from 'node:path';
import {execFileSync} from 'node:child_process';
import {retainedSourceChange,readWebPacket} from '../../../integrations/browser/web-workbench.mjs';
import {prepareFile} from '../../BonfyreSourceFrontier/tools/source_material.mjs';
import {createHash} from 'node:crypto';
import {encode,decode} from '../../../integrations/browser/workspace-msgpack.mjs';
const root=resolve(import.meta.dir,'../../..');let home,binary;
beforeAll(()=>{home=mkdtempSync(join(tmpdir(),'source-web-gateway-'));binary=join(home,'gateway');
 const source=String.raw`
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include "bf_msgpack.h"
#define BF_ESTATE_BAR ""
#define AW_PACKET (512u*1024u)
static int status_code=0,native_calls=0,permission=-9,fault=0;static size_t native_size=0;
static char native_subject[160],native_actor[160],native_operation[24],native_revision[16],native_challenge[65],native_origin[64];
static int aw_hex(const char *s,size_t n){return strlen(s)==n&&strspn(s,"0123456789abcdef")==n;}
static int psetup_hex(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}
static int psetup_decode(const char *s, size_t len, char *out, size_t cap) {
    size_t n = 0;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c == '%') {
            if (i + 2 >= len || psetup_hex(s[i+1]) < 0 || psetup_hex(s[i+2]) < 0) return 0;
            c = (unsigned char)(psetup_hex(s[i+1])*16+psetup_hex(s[i+2])); i += 2;
        } else if (c == '+') c = ' ';
        if (c < 33 || c > 126 || n + 1 >= cap) return 0;
        out[n++] = (char)c;
    }
    out[n] = 0; return 1;
}
static int aw_header(const char *raw,const char *name,char *out,size_t cap){
    out[0]=0;size_t length=strlen(name);int found=0;
    for(const char *p=raw?strchr(raw,'\n'):NULL;p && *p;p=strchr(p,'\n')){
        p++;if(!strncasecmp(p,name,length)&&p[length]==':'){
            if(found++)return 0;const char *value=p+length+1;while(*value==' '||*value=='\t')value++;const char *end=value+strcspn(value,"\r\n");while(end>value&&(end[-1]==' '||end[-1]=='\t'))end--;
            if((size_t)(end-value)>=cap)return 0;memcpy(out,value,(size_t)(end-value));out[end-value]=0;
        }
    }
    return found==1;
}
static int psetup_context(const char *bearer,const char *db,const char *context,int write,char *actor,size_t acap,char *subject,size_t scap,const char **scope){
 (void)db;if(permission==-9)permission=write;if(!bearer||!strcmp(bearer,"anonymous"))return 0;
 if(strcmp(context,"self")&&strcmp(context,"institution:fixture"))return -1;
 if(!strcmp(bearer,"reader")&&write)return -1;if(!strcmp(bearer,"writer")&&write==1)return -1;
 snprintf(actor,acap,"user:7");snprintf(subject,scap,"%s",!strcmp(context,"self")?"user:7":"institution:fixture");*scope=!strcmp(context,"self")?"self":"institution";return 1;
}
static void aw_response(int code,const char *mime,const void *body,size_t size,int public_material){(void)mime;(void)body;(void)size;(void)public_material;status_code=code;}
static void aw_error(int code,const char *message){(void)message;status_code=code;}
static int aw_native_result(char *const args[],const void *body,size_t bytes,unsigned char **out,size_t *size,int failure_body){
 (void)body;(void)failure_body;native_calls++;native_size=bytes;if(strcmp(args[0],"/native/bun")||strcmp(args[3],"/native/web_work.mjs"))abort();
  for(size_t i=4;args[i];i+=2){if(!strcmp(args[i],"--subject"))snprintf(native_subject,sizeof(native_subject),"%s",args[i+1]);if(!strcmp(args[i],"--actor"))snprintf(native_actor,sizeof(native_actor),"%s",args[i+1]);if(!strcmp(args[i],"--operation"))snprintf(native_operation,sizeof(native_operation),"%s",args[i+1]);if(!strcmp(args[i],"--revision"))snprintf(native_revision,sizeof(native_revision),"%s",args[i+1]);if(!strcmp(args[i],"--challenge"))snprintf(native_challenge,sizeof(native_challenge),"%s",args[i+1]);if(!strcmp(args[i],"--origin"))snprintf(native_origin,sizeof(native_origin),"%s",args[i+1]);}
 bf_mp_writer_t w;bf_mp_writer_init(&w,32);bf_mp_pack_array(&w,3);bf_mp_pack_uint(&w,1);bf_mp_pack_bool(&w,1);bf_mp_pack_map(&w,0);if(fault==1)bf_mp_pack_nil(&w);*size=w.len;*out=malloc(w.len);memcpy(*out,w.buf,w.len);bf_mp_writer_free(&w);return fault==2?6:0;
}
#include "${resolve(root,'cmd/BonfyreWebAddress/src/source_web_work.h')}"
int main(int argc,char **argv){if(argc!=8)return 2;setenv("BONFYRE_BUN_BINARY","/native/bun",1);setenv("BONFYRE_WEB_WORK_OWNER","/native/web_work.mjs",1);setenv("BONFYRE_SOURCE_FRONTIER_DB","/native/frontier.db",1);
 const unsigned char body[]={0x92,0x01,0x81,0xa1,'a',0};char raw[2048];fault=atoi(argv[7]);snprintf(raw,sizeof(raw),"POST /sources/_web HTTP/1.1\r\nHost: aurekai.life\r\nOrigin: %s\r\nContent-Type: %s\r\nContent-Length: %s\r\n\r\n",argv[4],argv[5],argv[6]);
 if(!serve_source_web_work(argv[2],"/sources/_web",argv[1],(const char*)body,raw,"aurekai.life",argv[3],"/native/auth.db"))return 3;
  printf("%d %d %d %s %s %s %zu\n",status_code,native_calls,permission,native_subject,native_actor,native_operation,native_size);if(!strcmp(native_operation,"provider"))printf("%s %s %s\n",native_revision,native_challenge,native_origin);return 0;}
`;
 writeFileSync(join(home,'gateway.c'),source);execFileSync(process.env.CC||'cc',['-std=c11','-Wall','-Wextra','-Werror','-Wno-misleading-indentation','-I'+resolve(root,'lib/libbonfyre/include'),join(home,'gateway.c'),resolve(root,'lib/libbonfyre/bf_msgpack.c'),'-o',binary],{stdio:'pipe'});
});
afterAll(()=>{if(home)rmSync(home,{recursive:true,force:true});});
const call=(query,{method='GET',user='owner',origin='https://aurekai.life',type='application/msgpack',length='6',fault=0}={})=>execFileSync(binary,[query,method,user,origin,type,length,String(fault)],{encoding:'utf8'}).trim().split(' ');
test('gateway binds native identity and permits only declared same-origin MessagePack operations',()=>{
 expect(call('context=self&operation=plan',{method:'POST'})).toEqual(['200','1','2','user:7','user:7','plan','6']);
 expect(call('context=institution%3Afixture&operation=connect',{method:'POST'})).toEqual(['200','1','1','institution:fixture','user:7','connect','6']);
 expect(call('context=self&operation=status',{user:'reader'})).toEqual(['200','1','0','user:7','user:7','status','0']);
});
test('anonymous, reader writes, writer credential administration and forged identity are refused before native invocation',()=>{
 for(const args of [{user:'anonymous'},{user:'reader'}])expect(call('context=self&operation=plan',{method:'POST',...args}).slice(0,2)).toEqual(['403','0']);
 expect(call('context=self&operation=connect',{method:'POST',user:'writer'}).slice(0,2)).toEqual(['403','0']);
 expect(call('context=user%3A99&operation=list').slice(0,2)).toEqual(['403','0']);
 expect(call('context=self&operation=list&subject=user%3A99').slice(0,2)).toEqual(['400','0']);
});
test('the native child never receives unsupported origin, content, length, route coordinates or trailing reply data',()=>{
 for(const options of [{origin:'https://foreign.test'},{origin:'https://aurekai.life\r\nOrigin: https://aurekai.life'},{type:'application/json'},{length:'65537'},{length:'-1'},{length:'0'}])expect(Number(call('context=self&operation=start',{method:'POST',...options})[1])).toBe(0);
 for(const query of ['context=self&operation=start','context=self&operation=list&operation=status','context=self&operation=fetch','context=self&operation=get&job=wrong','context=self&operation=list&cursor=-1','context=self%00&operation=list','context=self&operation=list&','context=self&operation=recover&request=x'])expect(call(query).slice(0,2)).toEqual(['400','0']);
 expect(call('context=self&operation=status',{fault:1})[0]).toBe('503');expect(call('context=self&operation=status',{fault:2})[0]).toBe('503');
});
test('exact recover, result inspection and comparison retain their bounded coordinates',()=>{
 expect(call('context=self&operation=recover&request=lost-request-0001')[0]).toBe('200');const id='a'.repeat(64);
 for(const op of ['item','compare'])expect(call('context=self&operation='+op+'&job='+id+'&cursor='+id)[0]).toBe('200');
 expect(call('context=self&operation=plan&job='+id,{method:'POST'})[0]).toBe('400');
});
test('provider continuation carries exact native work, review and challenge coordinates',()=>{
 const id='a'.repeat(64),challenge='b'.repeat(64),query='context=self&operation=provider&job='+id+'&revision=7&challenge='+challenge;
 expect(call(query,{user:'reader'})).toEqual(['200','1','0','user:7','user:7','provider','0\n7',challenge,'https://aurekai.life']);
 expect(call(query,{user:'anonymous'}).slice(0,2)).toEqual(['403','0']);
 for(const changed of [query.replace('revision=7','revision=0'),query.replace('revision=7','revision=-1'),query.replace('revision=7','revision=01'),query.replace(challenge,'invalid'),query+'&challenge='+challenge,query+'&origin=https%3A%2F%2Fevil.test',query.replace('&revision=7','')])expect(call(changed).slice(0,2)).toEqual(['400','0']);
 expect(call(query,{method:'POST'}).slice(0,2)).toEqual(['400','0']);
});
test('receiving material preserves exact source bytes and provenance, and refuses a changed digest',async()=>{
 const body='A complete source passage.\n<script>untrusted text</script>',hash=createHash('sha256').update(body).digest('hex'),job={id:'a'.repeat(64),digest:'b'.repeat(64),revision:3,title:'Review',state:'completed',archived:false,spec:{provider:'exa'},summary:{observedAt:'2026-10-21T12:00:00Z'}},item={id:'c'.repeat(64),title:'Source',url:'https://example.org/source',body,bodySHA256:hash,kind:'search-excerpt',truncated:true,notes:'Reviewed.'};
 const change=await retainedSourceChange(job,item,prepareFile,{collection:'Research'});expect(await change.value.original.text()).toBe(body);expect(change.value.contentSHA256).toBe(hash);expect(change.value.notes).toContain('truncated');expect(change.value.notes).toContain(job.id);expect(change.expectedRevision).toBe(0);
 await expect(retainedSourceChange(job,{...item,body:'changed'},prepareFile)).rejects.toThrow('digest');await expect(retainedSourceChange(job,{...item,preview:true},prepareFile)).rejects.toThrow('complete');
});
test('shared bounded replies preserve native failures, reviewed write packets and cancellation',async()=>{
 let cancelled=false;await expect(readWebPacket('/sources/_web',{request:async()=>new Response(new ReadableStream({cancel(){cancelled=true;}}),{status:503})})).rejects.toThrow('same saved run');expect(cancelled).toBe(true);let requested;expect(await readWebPacket('/sources/_web',{payload:{id:'reviewed'},request:async(_url,options)=>{requested=options;return new Response(encode([1,true,{saved:true}]),{headers:{'Content-Type':'application/msgpack'}});}})).toEqual([1,true,{saved:true}]);expect(requested.method).toBe('POST');expect(decode(requested.body)).toEqual([1,{id:'reviewed'}]);expect(requested.headers['Content-Type']).toBe('application/msgpack');const oversized=new Response(new ReadableStream({start(c){c.enqueue(new Uint8Array(512*1024+1));},cancel(){throw Error('Cancel failed');}}),{headers:{'Content-Type':'application/msgpack'}});await expect(readWebPacket('/sources/_web',{request:async()=>oversized})).rejects.toThrow('bound');expect(oversized.body.locked).toBe(false);const lifetime=new AbortController();let lateCancelled=false;await expect(readWebPacket('/sources/_web',{signal:lifetime.signal,request:async()=>{lifetime.abort(Error('Account changed'));return new Response(new ReadableStream({cancel(){lateCancelled=true;}}),{headers:{'Content-Type':'application/msgpack'}});}})).rejects.toThrow('Account changed');expect(lateCancelled).toBe(true);
});
test('MessagePack preserves a leading U+FEFF in retained text and map keys',()=>{const value={'\ufeffkey':'\ufeffExact source text'};expect(decode(encode(value))).toEqual(value);});
test('MessagePack still refuses malformed UTF-8, truncated strings and unpaired surrogates',()=>{for(const bytes of [[0xa1,0xff],[0xa2,0xc0,0xaf],[0xa3,0xed,0xa0,0x80],[0xa3,0xef,0xbb]])expect(()=>decode(new Uint8Array(bytes))).toThrow();expect(()=>encode('\ud800')).toThrow();});
