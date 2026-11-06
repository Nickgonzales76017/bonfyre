// Exercise the real native HTTP admission and MessagePack decoder with isolated owners.
import {test,expect,beforeAll,afterAll} from 'bun:test';
import {mkdtempSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {resolve,join} from 'node:path';
import {execFileSync} from 'node:child_process';
const root=resolve(import.meta.dir,'../../..');let home,binary;
beforeAll(()=>{home=mkdtempSync(join(tmpdir(),'activity-gateway-'));binary=join(home,'gateway');
 const source=String.raw`
#define _XOPEN_SOURCE 700
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <limits.h>
#include <unistd.h>
#include "bf_msgpack.h"
#define BF_ESTATE_BAR ""
#define AW_PACKET (512u*1024u)

#define PSETUP_DESTINATIONS_MAX 201
typedef struct {char context[96],label[320];int can_write;} PackSetupDestination;
static void html(const char *s){(void)s;}
static void url_query_value(const char *s){(void)s;}
static void psetup_start(int code,const char *s){(void)code;(void)s;}
static void psetup_end(void){}
static int psetup_destinations(const char *b,const char *a,PackSetupDestination *r,size_t *c){(void)b;(void)a;(void)r;*c=0;return 1;}
static const char *page_cms_env(const char *key,const char *fallback){const char *s=getenv(key);return s?s:fallback;}
static int bfpg_token(const char *s,size_t cap){return s&&*s&&strlen(s)<cap&&strspn(s,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:")==strlen(s);}
static int status_code=0,native_calls=0,permission=-9,fault=0;static size_t native_size=0;
static char native_subject[160],native_actor[160],native_operation[24],native_task[129],native_source_db[160];
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
 (void)body;(void)failure_body;native_calls++;native_size=bytes;if(strcmp(args[0],"/native/bun"))abort();
 for(size_t i=4;args[i];i+=2){if(!strcmp(args[i],"--source-db"))snprintf(native_source_db,sizeof(native_source_db),"%s",args[i+1]);if(!strcmp(args[i],"--instance"))snprintf(native_task,sizeof(native_task),"%s",args[i+1]);if(!strcmp(args[i],"--subject"))snprintf(native_subject,sizeof(native_subject),"%s",args[i+1]);if(!strcmp(args[i],"--actor"))snprintf(native_actor,sizeof(native_actor),"%s",args[i+1]);if(!strcmp(args[i],"--operation"))snprintf(native_operation,sizeof(native_operation),"%s",args[i+1]);}
 bf_mp_writer_t w;bf_mp_writer_init(&w,32);bf_mp_pack_array(&w,3);bf_mp_pack_uint(&w,1);bf_mp_pack_bool(&w,1);bf_mp_pack_map(&w,0);if(fault==1)bf_mp_pack_nil(&w);*size=w.len;*out=malloc(w.len);memcpy(*out,w.buf,w.len);bf_mp_writer_free(&w);return fault==2?127:0;
}
#include "${resolve(root,'cmd/BonfyreWebAddress/src/program_workspace.h')}"
int main(int argc,char **argv){if(argc!=8)return 2;setenv("BONFYRE_BUN_BINARY","/native/bun",1);setenv("BONFYRE_PROGRAM_OWNER",__FILE__,1);setenv("BONFYRE_PROGRAM_SOURCE_OWNER",__FILE__,1);setenv("BONFYRE_SOURCE_FRONTIER_DB","/native/frontier.db",1);
 const unsigned char body[]={0x92,0x01,0x81,0xa1,'a',0};char raw[2048];fault=atoi(argv[7]);snprintf(raw,sizeof(raw),"POST /apps/_programs HTTP/1.1\r\nHost: aurekai.life\r\nOrigin: %s\r\nContent-Type: %s\r\nContent-Length: %s\r\n\r\n",argv[4],argv[5],argv[6]);
 if(fault==3)unsetenv("BONFYRE_SOURCE_FRONTIER_DB");if(fault==4)setenv("BONFYRE_SOURCE_FRONTIER_DB","relative.db",1);if(fault==5)setenv("BONFYRE_PROGRAM_SOURCE_OWNER","/missing-owner.mjs",1);
 if(!serve_program_workspace(argv[2],"/apps/_programs",argv[1],(const char*)body,raw,"aurekai.life",argv[3],"/native/auth.db"))return 3;
 printf("%d %d %d %s %s %s %zu\n",status_code,native_calls,permission,native_subject,native_actor,native_operation,native_size);if(native_task[0])printf("%s\n",native_task);if(native_source_db[0])printf("%s\n",native_source_db);return 0;}
`;
 writeFileSync(join(home,'gateway.c'),source);execFileSync(process.env.CC||'cc',['-std=c11','-Wall','-Wextra','-Werror','-Wno-misleading-indentation','-I'+resolve(root,'lib/libbonfyre/include'),join(home,'gateway.c'),resolve(root,'lib/libbonfyre/bf_msgpack.c'),'-o',binary],{stdio:'pipe'});
});
afterAll(()=>{if(home)rmSync(home,{recursive:true,force:true});});
const call=(query,{method='GET',user='owner',origin='https://aurekai.life',type='application/msgpack',length='6',fault=0}={})=>execFileSync(binary,[query,method,user,origin,type,length,String(fault)],{encoding:'utf8'}).trim().split(' ');

test('reads preserve exact account and destination admission',()=>{
 expect(call('context=self&operation=catalog').slice(0,6)).toEqual(['200','1','0','user:7','user:7','catalog']);
 expect(call('context=institution%3Afixture&operation=read',{user:'reader'}).slice(0,6)).toEqual(['200','1','0','institution:fixture','user:7','read']);
 expect(call('context=self&operation=catalog',{user:'anonymous'}).slice(0,2)).toEqual(['403','0']);
 expect(call('context=institution%3Aother&operation=read').slice(0,2)).toEqual(['403','0']);
});
test('planning, saved work and source effects require current write membership',()=>{
 for(const operation of ['review','prepare','change','source-plan','source-start','source-decision']){
  expect(call('context=institution%3Afixture&operation='+operation,{method:'POST',user:'reader'}).slice(0,3)).toEqual(['403','0','2']);
  expect(call('context=institution%3Afixture&operation='+operation,{method:'POST',user:'writer'}).slice(0,6)).toEqual(['200','1','2','institution:fixture','user:7',operation]);
  expect(call('context=self&operation='+operation).slice(0,2)).toEqual(['400','0']);
 }
});
test('ambiguous identities, unsupported commands and cross-origin writes never reach the owner',()=>{
 for(const query of ['context=self&operation=read&actor=user:99','context=self&operation=read&subject=user:99','context=self&context=self&operation=read','context=self&operation=read&','context=self%00&operation=read','context=self&operation=execute','context=self&operation=catalog&instance=other','operation=catalog','context=self&operation=read&db=/tmp/fake'])expect(call(query).slice(0,2)).toEqual(['400','0']);
 for(const options of [{origin:'https://foreign.test'},{origin:'http://aurekai.life'},{origin:'https://aurekai.life\r\nOrigin: https://aurekai.life'},{type:'application/json'},{length:'32769'},{length:'999999'},{length:'-1'},{length:'0'}])expect(call('context=self&operation=prepare',{method:'POST',...options})[1]).toBe('0');
 for(const operation of ['catalog','read'])expect(call('context=self&operation='+operation,{method:'POST'}).slice(0,2)).toEqual(['400','0']);
});
test('receipts and retained source reads require current read admission and bounded same-origin bodies',()=>{
 for(const operation of ['change-receipt','source-status','source-read','source-item','source-recover']){
  const query='context=institution%3Afixture&operation='+operation;
  expect(call(query,{method:'POST',user:'reader'}).slice(0,6)).toEqual(['200','1','0','institution:fixture','user:7',operation]);
  expect(call(query,{method:'POST',user:'anonymous'}).slice(0,2)).toEqual(['403','0']);
  expect(call(query).slice(0,2)).toEqual(['400','0']);
  expect(call(query+'&instance=other',{method:'POST'}).slice(0,2)).toEqual(['400','0']);
  expect(call(query,{method:'POST',origin:'https://foreign.test'}).slice(0,2)).toEqual(['403','0']);
 }
 expect(call('context=self&operation=change',{method:'POST',length:'32768'}).at(-1)).toBe('32768');
});
test('source execution uses only the explicitly configured owner and database',()=>{
 expect(call('context=self&operation=source-start',{method:'POST'}).at(-1)).toBe('6\n/native/frontier.db');
 for(const fault of [3,4,5])expect(call('context=self&operation=source-start',{method:'POST',fault}).slice(0,2)).toEqual(['503','0']);
 for(const query of ['context=self&operation=source-start&source-db=/tmp/other','context=self&operation=source-plan&job=forged','context=self&operation=source-connect'])expect(call(query,{method:'POST'}).slice(0,2)).toEqual(['400','0']);
});
test('exact instance reads and typed owner errors keep their boundaries',()=>{
 expect(call('context=self&operation=read&instance=pws-example').at(-1)).toBe('0\npws-example');
 for(const fault of [1,2])expect(call('context=self&operation=read',{fault})[0]).toBe('503');
 expect(call('context=self&operation=prepare&instance=pws-example',{method:'POST'}).slice(0,2)).toEqual(['400','0']);
});
