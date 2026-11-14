/* Standalone WebAddress boundary witness: no QR or public service build.
 * Auth and subprocess results are stubs; binary packet framing is real. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include "bf_msgpack.h"

static int status, forwarded, mismatch;
static const char *accepted="server-session";
static int psetup_context(const char *session,const char *db,const char *context,int role,
                          char *actor,size_t actor_cap,char *subject,size_t subject_cap,
                          const char **scope){
    (void)db;
    if(!session||strcmp(session,accepted)||strcmp(context,"self")||role)return 0;
    snprintf(actor,actor_cap,"actor:test");snprintf(subject,subject_cap,"user:1");
    *scope="self";return 1;
}
static int aw_header(const char *raw,const char *name,char *out,size_t cap){
    out[0]=0;size_t length=strlen(name);int found=0;
    for(const char *p=raw?strchr(raw,'\n'):NULL;p&&*p;p=strchr(p,'\n')){
        p++;if(!strncasecmp(p,name,length)&&p[length]==':'){
            if(found++)return 0;
            const char *value=p+length+1;while(*value==' '||*value=='\t')value++;
            const char *end=value+strcspn(value,"\r\n");
            while(end>value&&(end[-1]==' '||end[-1]=='\t'))end--;
            if((size_t)(end-value)>=cap)return 0;
            memcpy(out,value,(size_t)(end-value));out[end-value]=0;
        }
    }
    return found==1;
}
static void aw_response(int code,const char *mime,const void *body,size_t size,int public_material){
    (void)mime;(void)body;(void)size;(void)public_material;status=code;
}
static void aw_error(int code,const char *message){aw_response(code,"text/plain",message,strlen(message),0);}
static int aw_native_result(char *const argv[],const void *body,size_t bytes,
                            unsigned char **out,size_t *size,int failure_body){
    (void)failure_body;
    if(strcmp(argv[0],"/bin/sh")||strcmp(argv[3],"/bin/sh")||
       strcmp(argv[4],"--account-bound")){mismatch++;return 6;}
    bf_mp_reader_t r;bf_mp_value_t v;
    bf_mp_reader_init(&r,body,bytes);
    if(bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=3||
       bf_mp_read_next(&r,&v)!=BF_MP_UINT||v.v.u!=1||
       bf_mp_read_next(&r,&v)!=BF_MP_STR||v.v.raw.len!=strlen(accepted)||
       memcmp(v.v.raw.ptr,accepted,v.v.raw.len)||bf_mp_read_next(&r,&v)!=BF_MP_BIN||
       v.v.raw.len!=2||memcmp(v.v.raw.ptr,"{}",2)||r.pos!=r.len){
        mismatch++;return 6;
    }
    const char reply[]="{\"schema\":\"bonfyre.command.dispatch.v1\",\"status\":202,\"result\":{\"admitted\":true}}";
    *size=sizeof(reply)-1;*out=(unsigned char*)malloc(*size);
    if(!*out)return 6;memcpy(*out,reply,*size);forwarded++;return 0;
}
#include "../../BonfyreWebAddress/src/native_command_work.h"
static void require(int accepted_result,int expected,const char *label){
    if(!accepted_result||status!=expected){
        fprintf(stderr,"%s: got %d expected %d\n",label,status,expected);exit(1);
    }
}
int main(void){
    const char *good="POST /apps/_native-command HTTP/1.1\r\n"
       "Origin: https://aurekai.work\r\nContent-Type: application/json\r\n"
       "Content-Length: 2\r\n\r\n";
    setenv("BONFYRE_BUN_BINARY","/bin/sh",1);
    setenv("BONFYRE_NATIVE_COMMAND_DISPATCH","/bin/sh",1);
    status=0;require(serve_native_command_work("POST","/apps/_native-command",NULL,
       "{}",good,"aurekai.work",accepted,"/fixture/auth"),202,"valid account packet");
    if(forwarded!=1||mismatch){fputs("native command packet mismatch\n",stderr);return 1;}
    status=0;require(serve_native_command_work("POST","/apps/_native-command",NULL,
       "{}",good,"aurekai.work",NULL,"/fixture/auth"),403,"anonymous");
    status=0;require(serve_native_command_work("GET","/apps/_native-command",NULL,
       NULL,good,"aurekai.work",accepted,"/fixture/auth"),405,"wrong method");
    status=0;require(serve_native_command_work("POST","/apps/_native-command","redirect=1",
       "{}",good,"aurekai.work",accepted,"/fixture/auth"),405,"query injection");
    const char *foreign="POST /apps/_native-command HTTP/1.1\r\n"
      "Origin: https://another.example\r\nContent-Type: application/json\r\n"
      "Content-Length: 2\r\n\r\n";
    status=0;require(serve_native_command_work("POST","/apps/_native-command",NULL,
       "{}",foreign,"aurekai.work",accepted,"/fixture/auth"),403,"foreign origin");
    const char *wrong="POST /apps/_native-command HTTP/1.1\r\n"
      "Origin: https://aurekai.work\r\nContent-Type: text/plain\r\n"
      "Content-Length: 2\r\n\r\n";
    status=0;require(serve_native_command_work("POST","/apps/_native-command",NULL,
       "{}",wrong,"aurekai.work",accepted,"/fixture/auth"),400,"content type");
    unsetenv("BONFYRE_NATIVE_COMMAND_DISPATCH");
    status=0;require(serve_native_command_work("POST","/apps/_native-command",NULL,
       "{}",good,"aurekai.work",accepted,"/fixture/auth"),503,"uninstalled native owner");
    if(forwarded!=1||mismatch){fputs("refused call reached native owner\n",stderr);return 1;}
    puts("native command WebAddress: PASS");return 0;
}
