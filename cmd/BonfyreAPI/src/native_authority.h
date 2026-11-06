#ifndef BONFYRE_API_NATIVE_AUTHORITY_H
#define BONFYRE_API_NATIVE_AUTHORITY_H
/* Native authorization is a conjunction, never a CLI discovery shortcut.
 * This reader invokes Auth and CMS, which own live actor and pack closures.
 * Review and effect owners remain independently mandatory before mutation. */
static int api_owner_stdin(const char *bin, char *const argv[], const char *secret,
                           char *output, size_t cap) {
    int input[2] = {-1,-1}, result[2] = {-1,-1};
    if (!bin || !*bin || !secret || !*secret || cap < 2 ||
        pipe(input) || pipe(result)) {
        if (input[0]>=0) close(input[0]); if (input[1]>=0) close(input[1]);
        if (result[0]>=0) close(result[0]); if (result[1]>=0) close(result[1]);
        return -1;
    }
    pid_t pid=fork();
    if(pid<0){close(input[0]);close(input[1]);close(result[0]);close(result[1]);return -1;}
    if(pid==0){
        close(input[1]);close(result[0]);
        if(dup2(input[0],STDIN_FILENO)<0 || dup2(result[1],STDOUT_FILENO)<0)_exit(127);
        close(input[0]);close(result[1]);
        int quiet=open("/dev/null",O_WRONLY);
        if(quiet>=0){dup2(quiet,STDERR_FILENO);close(quiet);}
        execvp(bin,argv);_exit(127);
    }
    close(input[0]);close(result[1]);
    size_t length=strlen(secret),off=0;
    while(off<length){
        ssize_t n=write(input[1],secret+off,length-off);
        if(n<0&&errno==EINTR)continue;
        if(n<=0)break;
        off+=(size_t)n;
    }
    close(input[1]);
    size_t used=0;int eof=0;
    while(used+1<cap){
        ssize_t n=read(result[0],output+used,cap-1-used);
        if(n<0&&errno==EINTR)continue;
        if(n<0)break;
        if(!n){eof=1;break;}
        used+=(size_t)n;
    }
    output[used]='\0';close(result[0]);
    int status=0;
    if(waitpid(pid,&status,0)!=pid||!WIFEXITED(status)||WEXITSTATUS(status)||off!=length||!eof)return -1;
    return 0;
}
static int api_native_actor(const char *session,int *user,char *actor,size_t acap){
    const char *bin=getenv("BONFYRE_AUTH_BINARY"),*db=getenv("BONFYRE_AUTH_DB");
    if(!bin||!*bin||!db||!*db||!session||!*session||strlen(session)>120)return 0;
    char *argv[]={(char*)bin,"--db",(char*)db,"actor-closure-stdin",NULL};
    char output[4096];
    if(api_owner_stdin(bin,argv,session,output,sizeof(output)))return 0;
    char err[96]="";
    bf_json_doc_t *doc=bf_json_parse(output,strlen(output),err,sizeof(err));
    if(!doc)return 0;
    const bf_json_node_t *root=bf_json_root(doc);
    const bf_json_node_t *yes=bf_json_obj_get(doc,root,"authenticated");
    const bf_json_node_t *principal=bf_json_obj_get(doc,root,"auth_principal");
    const bf_json_node_t *id=principal?bf_json_obj_get(doc,principal,"user_id"):NULL;
    const bf_json_node_t *a=bf_json_obj_get(doc,root,"actor_subject");
    int ok=yes&&yes->type==BF_JSON_BOOL&&bf_json_get_bool(yes)&&
        principal&&principal->type==BF_JSON_OBJECT&&
        id&&id->type==BF_JSON_INT&&bf_json_get_int(id)>0&&bf_json_get_int(id)<2147483647&&
        a&&bf_json_get_str_copy(a,actor,acap);
    if(ok)*user=(int)bf_json_get_int(id);
    bf_json_free(doc);
    return ok;
}
static int api_native_institution(const char *session,const char *institution,const char *role){
    const char *bin=getenv("BONFYRE_AUTH_BINARY"),*db=getenv("BONFYRE_AUTH_DB");
    if(!bin||!*bin||!db||!*db||!institution||!*institution)return 0;
    char *argv[]={(char*)bin,"--db",(char*)db,"institution-authorize-stdin",
      "--institution",(char*)institution,"--role",(char*)role,NULL};
    char output[1024],err[96]="";
    if(api_owner_stdin(bin,argv,session,output,sizeof(output)))return 0;
    bf_json_doc_t *doc=bf_json_parse(output,strlen(output),err,sizeof(err));
    if(!doc)return 0;
    const bf_json_node_t *yes=bf_json_obj_get(doc,bf_json_root(doc),"authorized");
    int ok=yes&&yes->type==BF_JSON_BOOL&&bf_json_get_bool(yes);
    bf_json_free(doc);return ok;
}
static int api_authority_coordinate(const char *text,size_t cap){
    if(!text||!*text||strlen(text)>=cap)return 0;
    for(const unsigned char *p=(const unsigned char*)text;*p;p++)
        if(!((*p>='a'&&*p<='z')||(*p>='0'&&*p<='9')||*p=='-'||*p=='.'||*p=='_'))return 0;
    return 1;
}
static int api_native_pack(const char *pack,const char *subject,const char *scope,
                            const char *actor,char *pinned,size_t cap){
    const char *bin=getenv("BONFYRE_CMS_BINARY"),*db=getenv("BONFYRE_CMS_DB");
    if(!bin||!*bin||!db||!*db||!api_authority_coordinate(pack,96))return 0;
    char *argv[]={(char*)bin,"pack-lifecycle","--operation","current","--pack",(char*)pack,
      "--version","","--subject",(char*)subject,"--scope",(char*)scope,"--actor",(char*)actor,
      "--expected-digest","","--expected-revision","0","--db",(char*)db,NULL};
    char output[32768]="";
    if(run_binary(bin,argv,output,sizeof(output)))return 0;
    int schema=0,active=0;char digest[65]="",pin[65]="";
    char *save=NULL;
    for(char *line=strtok_r(output,"\n",&save);line;line=strtok_r(NULL,"\n",&save)){
        char *value=strchr(line,'\t');if(!value)continue;*value++='\0';
        if(!strcmp(line,"schema"))schema=!strcmp(value,"bonfyre.pack.lifecycle.v1");
        else if(!strcmp(line,"state"))active=!strcmp(value,"active");
        else if(!strcmp(line,"digest")&&strlen(value)==64)memcpy(digest,value,65);
        else if(!strcmp(line,"pinned_digest")&&strlen(value)==64)memcpy(pin,value,65);
    }
    if(!schema||!active||!*pin||strcmp(digest,pin)||strspn(pin,"abcdef0123456789")!=64||cap<65)return 0;
    memcpy(pinned,pin,65);return 1;
}
/* The selected destination comes from native Auth, not from a caller-supplied
 * actor. Household is deliberately held until its CMS access proof is wired.
 * Passing this function DOES NOT authorize a WorkGraph or external effect. */
static int api_native_preflight(const HttpRequest *req,const char *context,const char *pack,
                                 char *actor,size_t acap,char *subject,size_t scap,
                                 char *digest,size_t dcap){
    int user=0;char principal[192]="";
    if(!req->session_token[0]||!api_native_actor(req->session_token,&user,principal,sizeof(principal)))return 0;
    if(!api_authority_coordinate(pack,96))return 0;
    char native_actor[64];
    snprintf(native_actor,sizeof(native_actor),"user:%d",user);
    if(!strcmp(context,"self")){
        snprintf(subject,scap,"%s",native_actor);
        if(!api_native_pack(pack,subject,"self",native_actor,digest,dcap))return 0;
    }else if(!strncmp(context,"institution:",12)){
        const char *name=context+12;
        if(!api_authority_coordinate(name,64)||!api_native_institution(req->session_token,name,"write"))return 0;
        snprintf(subject,scap,"%s",context);
        if(!api_native_pack(pack,subject,"institution",native_actor,digest,dcap))return 0;
    }else return 0;
    snprintf(actor,acap,"%s",principal);
    return 1;
}
static void api_authority_preflight(HttpRequest *req,HttpResponse *resp){
    char context[128]="",pack[128]="",actor[192]="",subject[192]="",digest[65]="";
    if(!query_param(req->query,"context",context,sizeof(context))||
       !query_param(req->query,"pack",pack,sizeof(pack))){
        http_resp_json(resp,400,"{\"error\":\"Select an exact account context and installed pack\"}");return;
    }
    if(!api_native_preflight(req,context,pack,actor,sizeof(actor),subject,sizeof(subject),digest,sizeof(digest))){
        http_resp_json(resp,403,"{\"schema\":\"bonfyre.command-authority.v1\",\"admitted\":false,\"reason\":\"current_actor_destination_or_pack_refused\"}");return;
    }
    bf_jsonw out;bf_jsonw_init(&out,0);bf_jsonw_obj(&out);
    bf_jsonw_key(&out,"schema");bf_jsonw_str(&out,"bonfyre.command-authority.v1");
    bf_jsonw_key(&out,"actor");bf_jsonw_str(&out,actor);
    bf_jsonw_key(&out,"destination");bf_jsonw_str(&out,subject);
    bf_jsonw_key(&out,"pack_digest");bf_jsonw_str(&out,digest);
    bf_jsonw_key(&out,"actor_and_pack_current");bf_jsonw_bool(&out,1);
    bf_jsonw_key(&out,"admitted");bf_jsonw_bool(&out,0);
    bf_jsonw_key(&out,"next_required");bf_jsonw_str(&out,"effect-specific reviewed WorkGraph admission at the bound destination");
    bf_jsonw_obj_end(&out);api_finish_json(resp,&out);
}
#endif
