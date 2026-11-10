// Exercise the real native HTTP admission and MessagePack decoder with isolated owners.
import {test,expect,beforeAll,afterAll} from 'bun:test';
import {mkdtempSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {resolve,join} from 'node:path';
import {execFileSync} from 'node:child_process';
const root=resolve(import.meta.dir,'../../..');let home,binary;
beforeAll(()=>{home=mkdtempSync(join(tmpdir(),'activity-gateway-'));binary=join(home,'gateway');
 const source=String.raw`
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include "bf_msgpack.h"
#define BF_ESTATE_BAR ""
#define AW_PACKET (512u*1024u)
static int status_code=0,native_calls=0,permission=-9,fault=0;static size_t native_size=0;
static char native_subject[160],native_actor[160],native_operation[24],native_task[129];
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
 (void)db;permission=write;if(!bearer||!strcmp(bearer,"anonymous"))return 0;
 if(strcmp(context,"self")&&strcmp(context,"institution:fixture"))return -1;
 if(!strcmp(bearer,"reader")&&write)return -1;if(!strcmp(bearer,"writer")&&write==1)return -1;
 snprintf(actor,acap,"user:7");snprintf(subject,scap,"%s",!strcmp(context,"self")?"user:7":"institution:fixture");*scope=!strcmp(context,"self")?"self":"institution";return 1;
}
static void aw_response(int code,const char *mime,const void *body,size_t size,int public_material){(void)mime;(void)body;(void)size;(void)public_material;status_code=code;}
static void aw_error(int code,const char *message){(void)message;status_code=code;}
static int aw_native_result(char *const args[],const void *body,size_t bytes,unsigned char **out,size_t *size,int failure_body){
 (void)body;(void)failure_body;native_calls++;native_size=bytes;if(strcmp(args[0],"/native/bun")||strcmp(args[3],"/native/fabric_activity_cli.mjs"))abort();
 for(size_t i=4;args[i];i+=2){if(!strcmp(args[i],"--task"))snprintf(native_task,sizeof(native_task),"%s",args[i+1]);if(!strcmp(args[i],"--subject"))snprintf(native_subject,sizeof(native_subject),"%s",args[i+1]);if(!strcmp(args[i],"--actor"))snprintf(native_actor,sizeof(native_actor),"%s",args[i+1]);if(!strcmp(args[i],"--operation"))snprintf(native_operation,sizeof(native_operation),"%s",args[i+1]);}
 bf_mp_writer_t w;bf_mp_writer_init(&w,32);bf_mp_pack_array(&w,3);bf_mp_pack_uint(&w,1);bf_mp_pack_bool(&w,1);bf_mp_pack_map(&w,0);if(fault==1)bf_mp_pack_nil(&w);*size=w.len;*out=malloc(w.len);memcpy(*out,w.buf,w.len);bf_mp_writer_free(&w);return fault==2?6:0;
}
#include "${resolve(root,'cmd/BonfyreWebAddress/src/activity_workspace.h')}"
int main(int argc,char **argv){if(argc!=8)return 2;setenv("BONFYRE_BUN_BINARY","/native/bun",1);setenv("BONFYRE_ACTIVITY_OWNER","/native/fabric_activity_cli.mjs",1);setenv("BONFYRE_SOURCE_FRONTIER_DB","/native/frontier.db",1);
 const unsigned char body[]={0x92,0x01,0x81,0xa1,'a',0};char raw[2048];fault=atoi(argv[7]);snprintf(raw,sizeof(raw),"POST /apps/_fabric HTTP/1.1\r\nHost: aurekai.life\r\nOrigin: %s\r\nContent-Type: %s\r\nContent-Length: %s\r\n\r\n",argv[4],argv[5],argv[6]);
 if(!serve_activity_workspace(argv[2],"/apps/_fabric",argv[1],(const char*)body,raw,"aurekai.life",argv[3],"/native/auth.db"))return 3;
 printf("%d %d %d %s %s %s %zu\n",status_code,native_calls,permission,native_subject,native_actor,native_operation,native_size);if(native_task[0])printf("%s\n",native_task);return 0;}
`;
 writeFileSync(join(home,'gateway.c'),source);execFileSync(process.env.CC||'cc',['-std=c11','-Wall','-Wextra','-Werror','-Wno-misleading-indentation','-I'+resolve(root,'lib/libbonfyre/include'),join(home,'gateway.c'),resolve(root,'lib/libbonfyre/bf_msgpack.c'),'-o',binary],{stdio:'pipe'});
});
afterAll(()=>{if(home)rmSync(home,{recursive:true,force:true});});
const call=(query,{method='GET',user='owner',origin='https://aurekai.life',type='application/msgpack',length='6',fault=0}={})=>execFileSync(binary,[query,method,user,origin,type,length,String(fault)],{encoding:'utf8'}).trim().split(' ');
test('reads are scoped by the current native actor and membership',()=>{
 expect(call('context=self&operation=read')).toEqual(['200','1','0','user:7','user:7','read','0']);
 expect(call('context=institution%3Afixture&operation=read',{user:'reader'})).toEqual(['200','1','0','institution:fixture','user:7','read','0']);
 expect(call('context=self&operation=read',{user:'anonymous'}).slice(0,2)).toEqual(['403','0']);
 expect(call('context=institution%3Aother&operation=read').slice(0,2)).toEqual(['403','0']);
});
test('notification presentation is recipient-scoped and accepts no task command',()=>{
 expect(call('context=institution%3Afixture&operation=notification',{method:'POST',user:'reader'})).toEqual(['200','1','0','institution:fixture','user:7','notification','6']);
 for(const operation of ['cancel','submit','start','dismiss','read'])expect(call('context=self&operation='+operation,{method:'POST'}).slice(0,2)).toEqual(['400','0']);
 expect(call('context=self&operation=notification').slice(0,2)).toEqual(['400','0']);
 expect(call('context=self&operation=notification&task=a123',{method:'POST'}).slice(0,2)).toEqual(['400','0']);
});
test('cross-origin, ambiguous and unsupported requests cannot reach Fabric',()=>{
 for(const options of [{origin:'https://foreign.test'},{origin:'https://aurekai.life\r\nOrigin: https://aurekai.life'},{type:'application/json'},{length:'65537'},{length:'-1'},{length:'0'}])expect(call('context=self&operation=notification',{method:'POST',...options})[1]).toBe('0');
 for(const query of ['context=self&operation=read&actor=user%3A99','context=self&operation=read&subject=user%3A99','context=self&operation=read&context=self','context=self&operation=read&','context=self%00&operation=read','context=self&operation=read&task=../file','context=self&operation=read&task=.hidden'])expect(call(query).slice(0,2)).toEqual(['400','0']);
});
test('exact task reads preserve their native coordinate and refuse interrupted replies',()=>{
 expect(call('context=self&operation=read&task=abc123_task-4').at(-1)).toBe('0\nabc123_task-4');
 for(const fault of [1,2])expect(call('context=self&operation=read',{fault})[0]).toBe('503');
});
test('model effects require destination write access and keep browser identity outside argv',()=>{
 for(const operation of ['model-review','model-submit','model-cancel','model-reconcile','model-verify-transfer']){
  expect(call('context=institution%3Afixture&operation='+operation,{method:'POST',user:'reader'}).slice(0,3)).toEqual(['403','0','2']);
  expect(call('context=institution%3Afixture&operation='+operation,{method:'POST',user:'writer'}).slice(0,6)).toEqual(['200','1','2','institution:fixture','user:7',operation]);
  expect(call('context=self&operation='+operation).slice(0,2)).toEqual(['400','0']);
  expect(call('context=self&operation='+operation+'&task=abc123',{method:'POST'}).slice(0,2)).toEqual(['400','0']);
 }
 expect(call('context=self&operation=model-read').slice(0,2)).toEqual(['400','0']);
 expect(call('context=self&operation=model-read&task=abc123').slice(0,3)).toEqual(['200','1','0']);
 expect(call('context=self&operation=model-capabilities').slice(0,3)).toEqual(['200','1','0']);
 expect(call('context=self&operation=model-capabilities&task=abc123').slice(0,2)).toEqual(['400','0']);
});
test('push uses current recipient access, explicit methods and the same origin boundary',()=>{
 expect(call('context=self&operation=push-status').slice(0,3)).toEqual(['200','1','0']);
 for(const operation of ['push-subscribe','push-unsubscribe','push-ack','push-resolve']){
  expect(call('context=institution%3Afixture&operation='+operation,{method:'POST',user:'reader'}).slice(0,6)).toEqual(['200','1','0','institution:fixture','user:7',operation]);
  expect(call('context=self&operation='+operation).slice(0,2)).toEqual(['400','0']);
  expect(call('context=self&operation='+operation,{method:'POST',origin:'https://foreign.test'}).slice(0,2)).toEqual(['403','0']);
  expect(call('context=self&operation='+operation+'&task=abc123',{method:'POST'}).slice(0,2)).toEqual(['400','0']);
 }
});
