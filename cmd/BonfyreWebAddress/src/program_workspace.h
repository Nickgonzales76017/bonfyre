#ifndef BONFYRE_PROGRAM_WORKSPACE_H
#define BONFYRE_PROGRAM_WORKSPACE_H
/* ProgramFabric retains program state. This aperture resolves current account
 * authority and carries bounded native packets; it cannot adopt another owner. */
typedef struct { char context[96],operation[24],instance[160],template_id[161];unsigned seen; } ProgramWorkspaceQuery;
/* A deep link names one template the same way the catalogue does; anything else
 * is refused rather than narrowed. */
static int program_template_token(const char *s){size_t n=s?strlen(s):0;return n>0&&n<=160&&isalnum((unsigned char)*s)&&strspn(s,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")==n;}
static int program_workspace_query(const char *query,ProgramWorkspaceQuery *out,int machine){
    memset(out,0,sizeof(*out));snprintf(out->context,sizeof(out->context),"self");
    if(query&&strlen(query)>2048)return 0;
    for(const char *p=query?query:"";*p;){
        const char *end=strchr(p,'&');if(!end)end=p+strlen(p);
        const char *eq=memchr(p,'=',(size_t)(end-p));char name[24],*dest=NULL;size_t cap=0;unsigned bit=0;
        if(!eq||!psetup_decode(p,(size_t)(eq-p),name,sizeof(name)))return 0;
        if(!strcmp(name,"context")){dest=out->context;cap=sizeof(out->context);bit=1;}
        else if(!strcmp(name,"operation")&&machine){dest=out->operation;cap=sizeof(out->operation);bit=2;}
        else if(!strcmp(name,"instance")){dest=out->instance;cap=sizeof(out->instance);bit=4;}
        else if(!strcmp(name,"template")&&!machine){dest=out->template_id;cap=sizeof(out->template_id);bit=8;}else return 0;
        if((out->seen&bit)||!psetup_decode(eq+1,(size_t)(end-eq-1),dest,cap)||!*dest)return 0;
        out->seen|=bit;p=*end?end+1:end;if(*end&&!*p)return 0;
    }
    if(!bfpg_token(out->context,sizeof(out->context)))return 0;
    if(*out->instance&&(!bfpg_token(out->instance,sizeof(out->instance))))return 0;
    if(*out->template_id&&(*out->instance||!program_template_token(out->template_id)))return 0;
    return !machine||(out->seen&3)==3;
}
static int program_workspace_packet(const unsigned char *bytes,size_t size){
    if(!bytes||!size||size>AW_PACKET)return 0;
    bf_mp_reader_t reader;bf_mp_value_t value;bf_mp_reader_init(&reader,bytes,size);
    if(bf_mp_read_next(&reader,&value)!=BF_MP_ARRAY||value.v.count!=3)return 0;
    bf_mp_type_t type=bf_mp_read_next(&reader,&value);
    if(!((type==BF_MP_UINT&&value.v.u==1)||(type==BF_MP_INT&&value.v.i==1)))return 0;
    return bf_mp_read_next(&reader,&value)==BF_MP_BOOL&&bf_mp_skip(&reader)==0&&reader.pos==reader.len;
}
static int serve_program_workspace(const char *method,const char *target,const char *query,const char *body,
                                   const char *raw,const char *host,const char *bearer,const char *auth_db){
    int machine=!strcmp(target,"/apps/_programs");
    if(!machine&&strcmp(target,"/programs"))return 0;
    ProgramWorkspaceQuery in;int writing=!strcmp(method,"POST");
    if(!program_workspace_query(query,&in,machine)||(!writing&&strcmp(method,"GET"))||(!machine&&writing)){
        aw_error(400,"Choose a current destination and one program operation.\n");return 1;
    }
    int reading_post=!strcmp(in.operation,"change-receipt")||!strcmp(in.operation,"source-status")||
        !strcmp(in.operation,"source-read")||!strcmp(in.operation,"source-item")||!strcmp(in.operation,"source-recover");
    if(machine){
        int action=!strcmp(in.operation,"review")||!strcmp(in.operation,"prepare")||!strcmp(in.operation,"change")||
            !strcmp(in.operation,"source-plan")||!strcmp(in.operation,"source-start")||!strcmp(in.operation,"source-decision")||reading_post;
        int read=!strcmp(in.operation,"read"),catalog=!strcmp(in.operation,"catalog");
        if((writing?(!action||*in.instance):(!read&&!catalog))||(*in.instance&&!read)){
            aw_error(400,"Use this program operation's declared method and coordinates.\n");return 1;
        }
    }
    char actor[160],subject[160];const char *scope=NULL;
    int permission=psetup_context(bearer,auth_db,in.context,writing&&!reading_post?2:0,actor,sizeof(actor),subject,sizeof(subject),&scope);
    if(permission!=1){
        if(!permission&&!machine){
            char back[640];snprintf(back,sizeof(back),"/programs?context=%s%s%s%s%s",in.context,*in.instance?"&instance=":"",in.instance,*in.template_id?"&template=":"",in.template_id);
            fputs("HTTP/1.1 303 See Other\r\nLocation: /login?next=",stdout);url_query_value(back);
            fputs("\r\nCache-Control: private, no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);
        }else aw_error(403,"Sign in with current access to this program's destination.\n");return 1;
    }
    if(!machine){
        int can_write=psetup_context(bearer,auth_db,in.context,2,actor,sizeof(actor),subject,sizeof(subject),&scope)==1;
        psetup_start(200,"Programs");
        fputs("<link rel=\"stylesheet\" href=\"/apps/program-workspace.css\"><section data-program-workspace data-context=\"",stdout);html(in.context);
        fputs("\" data-instance=\"",stdout);html(in.instance);fputs("\" data-template=\"",stdout);html(in.template_id);printf("\" data-can-write=\"%s\">",can_write?"true":"false");
        fputs("<header class=\"program-intro\"><p class=\"program-eyebrow\">Aurekai · continuing work</p><h1>Programs</h1><p>Turn an institutional plan, service or recurring need into work you can continue.</p></header>",stdout);
        PackSetupDestination rows[PSETUP_DESTINATIONS_MAX];size_t count=0;
        int choices=psetup_destinations(bearer,auth_db,rows,&count);
        if(choices==1){
            fputs("<form class=\"program-context\" action=\"/programs\" method=\"get\"><label for=\"program-context\">Work belonging to</label><select id=\"program-context\" name=\"context\">",stdout);
            for(size_t i=0;i<count;i++){fputs("<option value=\"",stdout);html(rows[i].context);fputs(!strcmp(rows[i].context,in.context)?"\" selected>":"\">",stdout);html(rows[i].label);fputs("</option>",stdout);}
            fputs("</select>",stdout);
            if(*in.template_id){fputs("<input type=\"hidden\" name=\"template\" value=\"",stdout);html(in.template_id);fputs("\">",stdout);}
            fputs("<button>Change workspace</button></form>",stdout);
        }else fputs("<p role=\"alert\">The complete destination list is unavailable. Refresh to change workspace.</p>",stdout);
        fputs("<div data-program-app><p role=\"status\">Opening your programs…</p></div><noscript>Enable JavaScript to open and work on a program.</noscript></section><script type=\"module\" src=\"/apps/program-workspace.mjs\"></script>",stdout);
        psetup_end();return 1;
    }
    size_t bytes=0;
    if(writing){
        char origin[300],expected[300],content[80],length[24];
        if(!host||!*host||strlen(host)>250||strcspn(host,"\r\n/\\ ")!=strlen(host)){
            aw_error(403,"Submit from this program's current workspace.\n");return 1;
        }
        int local=!strcmp(host,"localhost")||!strcmp(host,"127.0.0.1")||!strncmp(host,"localhost:",10)||!strncmp(host,"127.0.0.1:",10);
        snprintf(expected,sizeof(expected),"%s://%s",local?"http":"https",host);
        if(!aw_header(raw,"Origin",origin,sizeof(origin))||strcmp(origin,expected)){
            aw_error(403,"Submit from this program's current workspace.\n");return 1;
        }
        if(!body||!aw_header(raw,"Content-Type",content,sizeof(content))||strcmp(content,"application/msgpack")||
           !aw_header(raw,"Content-Length",length,sizeof(length))||!*length||strlen(length)>5||
           strspn(length,"0123456789")!=strlen(length)||(bytes=(size_t)strtoul(length,NULL,10))==0||bytes>32768){
            aw_error(400,"This program request is missing or too large.\n");return 1;
        }
    }
    int source_work=!strncmp(in.operation,"source-",7);
    const char *bun=getenv("BONFYRE_BUN_BINARY"),*owner=getenv(source_work?"BONFYRE_PROGRAM_SOURCE_OWNER":"BONFYRE_PROGRAM_OWNER"),*client=getenv("BONFYRE_WORKSPACE_CLIENT");
    char inferred[PATH_MAX],resolved[PATH_MAX];
    if((!owner||!*owner)&&client&&*client=='/'&&snprintf(inferred,sizeof(inferred),"%s/../../scripts/%s",client,source_work?"program-source-work.mjs":"program-workspace-owner.mjs")<(int)sizeof(inferred)&&realpath(inferred,resolved))owner=resolved;
    const char *db=page_cms_env("BONFYRE_CMS_DB","/var/lib/bonfyre/cms/bonfyre_cms.db");
    const char *source_db=getenv("BONFYRE_SOURCE_FRONTIER_DB");
    if(!bun||*bun!='/'||!owner||*owner!='/'||*db!='/'||access(owner,R_OK)||(source_work&&(!source_db||*source_db!='/'))){
        aw_error(503,"The program owner is unavailable. Retained work is unchanged.\n");return 1;
    }
    char *argv[]={(char*)bun,"--no-env-file","--no-install",(char*)owner,"--operation",in.operation,
        "--actor",actor,"--subject",subject,"--scope",(char*)scope,"--db",(char*)db,
        source_work?"--source-db":"--instance",source_work?(char*)source_db:in.instance,NULL};
    unsigned char *output=NULL;size_t size=0;int rc=aw_native_result(argv,writing?body:NULL,bytes,&output,&size,1);
    if((rc==0||rc==3||rc==4||rc==6)&&program_workspace_packet(output,size))aw_response(200,"application/msgpack",output,size,0);
    else aw_error(503,writing&&!reading_post?"The outcome is unconfirmed. Reopen the same program to check its saved work.\n":"Programs could not be read completely.\n");
    free(output);return 1;
}
#endif
