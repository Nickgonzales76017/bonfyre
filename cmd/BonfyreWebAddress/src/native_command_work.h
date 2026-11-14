#ifndef BONFYRE_NATIVE_COMMAND_WORK_H
#define BONFYRE_NATIVE_COMMAND_WORK_H
/* Review-gated native command dispatch for an installed account. WebAddress
 * supplies the authenticated session; no browser field can confer authority,
 * select an executor, or turn the command into arbitrary shell execution. */
static int serve_native_command_work(const char *method,const char *target,const char *query,
                                     const char *body,const char *raw,const char *host,
                                     const char *bearer,const char *auth_db){
    if(strcmp(target,"/apps/_native-command"))return 0;
    if(strcmp(method,"POST")||(query&&*query)){
        aw_error(405,"Use the reviewed command submission method.\n");return 1;
    }
    char actor[160],subject[160];const char *scope=NULL;
    if(!bearer||psetup_context(bearer,auth_db,"self",0,actor,sizeof(actor),subject,sizeof(subject),&scope)!=1){
        aw_error(403,"A current account session is required.\n");return 1;
    }
    char origin[300],expected[300],content[80],length[24];
    if(!host||!*host||strlen(host)>250||strcspn(host,"\r\n/\\ ")!=strlen(host)){
        aw_error(403,"Submit from this account's workspace.\n");return 1;
    }
    int local=!strcmp(host,"localhost")||!strcmp(host,"127.0.0.1")||
              !strncmp(host,"localhost:",10)||!strncmp(host,"127.0.0.1:",10);
    snprintf(expected,sizeof(expected),"%s://%s",local?"http":"https",host);
    if(!aw_header(raw,"Origin",origin,sizeof(origin))||strcmp(origin,expected)){
        aw_error(403,"Submit from this account's workspace.\n");return 1;
    }
    size_t bytes=0;
    if(!body||!aw_header(raw,"Content-Type",content,sizeof(content))||
       strcmp(content,"application/json")||
       !aw_header(raw,"Content-Length",length,sizeof(length))||!*length||strlen(length)>4||
       strspn(length,"0123456789")!=strlen(length)||
       (bytes=(size_t)strtoul(length,NULL,10))<2||bytes>4096){
        aw_error(400,"Submit one bounded reviewed command intent.\n");return 1;
    }
    const char *bun=getenv("BONFYRE_BUN_BINARY"),*owner=getenv("BONFYRE_NATIVE_COMMAND_DISPATCH");
    if(!bun||*bun!='/'||!owner||*owner!='/'||access(owner,R_OK)){
        aw_error(503,"The native command owner is not installed. No work was queued.\n");return 1;
    }
    bf_mp_writer_t packet;bf_mp_writer_init(&packet,8192);
    int invalid=bf_mp_pack_array(&packet,3)||bf_mp_pack_uint(&packet,1)||
        bf_mp_pack_str_cstr(&packet,bearer)||bf_mp_pack_bin(&packet,body,(uint32_t)bytes);
    if(invalid||packet.len>8192){bf_mp_writer_free(&packet);aw_error(400,"Invalid account command packet.\n");return 1;}
    char *argv[]={(char*)bun,"--no-env-file","--no-install",(char*)owner,"--account-bound",NULL};
    unsigned char *output=NULL;size_t size=0;
    int rc=aw_native_result(argv,packet.buf,packet.len,&output,&size,1);
    bf_mp_writer_free(&packet);
    if((rc==0||rc==3||rc==4)&&output&&size&&size<=8192)
        aw_response(rc==0?202:rc==4?400:403,"application/json; charset=utf-8",output,size,0);
    else aw_error(503,"The reviewed command outcome is unconfirmed; inspect the original review before retrying.\n");
    free(output);return 1;
}
#endif
