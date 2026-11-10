#ifndef BONFYRE_ACCOUNT_ACTIVITY_WEB_H
#define BONFYRE_ACCOUNT_ACTIVITY_WEB_H

/* Auth supplies the audience on every call; Queue supplies the work and the
 * recipient receipt. No browser-provided actor or global operator count enters
 * this projection. A notification action requires read access to its context. */
typedef struct { char context[96],operation[24],mission[128],cursor[24],request[97];unsigned seen; } AawQuery;
static int aaw_atom(const char *s,size_t max) {
    return s&&*s&&strlen(s)<=max&&strspn(s,"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._:-")==strlen(s);
}
static int aaw_query(const char *query,AawQuery *out) {
    memset(out,0,sizeof(*out));if(!query||strlen(query)>2048)return 0;
    for(const char *p=query;*p;){
        const char *end=strchr(p,'&');if(!end)end=p+strlen(p);
        const char *eq=memchr(p,'=',(size_t)(end-p));char key[24],*dest=NULL;size_t cap=0;unsigned bit=0;
        if(!eq||!psetup_decode(p,(size_t)(eq-p),key,sizeof(key)))return 0;
        if(!strcmp(key,"context")){dest=out->context;cap=sizeof(out->context);bit=1;}
        else if(!strcmp(key,"operation")){dest=out->operation;cap=sizeof(out->operation);bit=2;}
        else if(!strcmp(key,"mission")){dest=out->mission;cap=sizeof(out->mission);bit=4;}
        else if(!strcmp(key,"cursor")){dest=out->cursor;cap=sizeof(out->cursor);bit=8;}
        else if(!strcmp(key,"request")){dest=out->request;cap=sizeof(out->request);bit=16;}else return 0;
        if((out->seen&bit)||!psetup_decode(eq+1,(size_t)(end-eq-1),dest,cap)||!*dest)return 0;
        out->seen|=bit;p=*end?end+1:end;if(*end&&!*p)return 0;
    }
    return (out->seen&3)==3;
}
static int aaw_operation(const AawQuery *q,int writing) {
    if(writing)return q->seen==3&&!strcmp(q->operation,"inbox-set");
    if(!strcmp(q->operation,"get"))return q->seen==7&&aaw_atom(q->mission,127);
    if(!strcmp(q->operation,"inbox-receipt"))return q->seen==19&&strlen(q->request)>=8&&aaw_atom(q->request,96);
    if(!strcmp(q->operation,"agents")||!strcmp(q->operation,"compute"))return q->seen==3;
    if(strcmp(q->operation,"list")&&strcmp(q->operation,"inbox")&&strcmp(q->operation,"outputs"))return 0;
    if(q->seen==3)return 1;
    if(!strcmp(q->operation,"inbox")&&q->seen==7)return aaw_atom(q->mission,127);
    return q->seen==11&&q->cursor[0]>='1'&&q->cursor[0]<='9'&&strlen(q->cursor)<=16&&
        strspn(q->cursor,"0123456789")==strlen(q->cursor)&&strtoull(q->cursor,NULL,10)<=UINT64_C(8640000000000000);
}
static int aaw_string(bf_mp_reader_t *r,char *out,size_t cap) {
    bf_mp_value_t v;
    if(bf_mp_read_next(r,&v)!=BF_MP_STR||!v.v.raw.len||v.v.raw.len>=cap||memchr(v.v.raw.ptr,0,v.v.raw.len))return 0;
    memcpy(out,v.v.raw.ptr,v.v.raw.len);out[v.v.raw.len]=0;return 1;
}
static int aaw_version(bf_mp_reader_t *r) {
    bf_mp_value_t v;bf_mp_type_t type=bf_mp_read_next(r,&v);
    return (type==BF_MP_UINT&&v.v.u==1)||(type==BF_MP_INT&&v.v.i==1);
}
/* This is a concurrency guard, never account authority. The current Auth/CMS
 * context above is the only source of the actor and destination forwarded. */
static int aaw_expected_binding(bf_mp_reader_t *r,const char *actor,const char *subject,const char *scope) {
    bf_mp_value_t v;unsigned seen=0;int matches=1;
    if(bf_mp_read_next(r,&v)!=BF_MP_MAP||v.v.count!=3)return 0;
    for(unsigned i=0;i<3;i++){
        char key[16],value[160];unsigned bit=0;const char *expected=NULL;
        if(!aaw_string(r,key,sizeof(key)))return 0;
        if(!strcmp(key,"actor")){bit=1;expected=actor;}
        else if(!strcmp(key,"subject")){bit=2;expected=subject;}
        else if(!strcmp(key,"scope")){bit=4;expected=scope;}else return 0;
        if((seen&bit)||!aaw_string(r,value,sizeof(value)))return 0;
        seen|=bit;if(strcmp(value,expected))matches=0;
    }
    return seen==7?(matches?1:-1):0;
}
/* Queue retains its original five-field mutation contract. Decode the exact
 * browser packet first, compare the current binding, then remove that guard. */
static int aaw_inbox_change(const char *body,size_t size,const char *actor,const char *subject,const char *scope,bf_mp_writer_t *out) {
    if(!body||!size||size>4096)return 0;
    bf_mp_reader_t r;bf_mp_value_t v;bf_mp_reader_init(&r,body,size);
    if(bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=2||!aaw_version(&r)||
       bf_mp_read_next(&r,&v)!=BF_MP_MAP||v.v.count!=6)return 0;
    char id[128]="",version[65]="",action[16]="",request[97]="";uint64_t revision=0;unsigned seen=0;int binding=0;
    for(unsigned i=0;i<6;i++){
        char key[24];unsigned bit=0;
        if(!aaw_string(&r,key,sizeof(key)))return 0;
        if(!strcmp(key,"id"))bit=1;else if(!strcmp(key,"version"))bit=2;
        else if(!strcmp(key,"revision"))bit=4;else if(!strcmp(key,"action"))bit=8;
        else if(!strcmp(key,"requestId"))bit=16;else if(!strcmp(key,"expectedBinding"))bit=32;else return 0;
        if(seen&bit)return 0;
        seen|=bit;
        if(bit==1){if(!aaw_string(&r,id,sizeof(id))||!aaw_atom(id,127))return 0;}
        else if(bit==2){if(!aaw_string(&r,version,sizeof(version))||strlen(version)!=64||strspn(version,"0123456789abcdef")!=64)return 0;}
        else if(bit==4){
            bf_mp_type_t type=bf_mp_read_next(&r,&v);
            if(type==BF_MP_UINT)revision=v.v.u;else if(type==BF_MP_INT&&v.v.i>=0)revision=(uint64_t)v.v.i;else return 0;
            if(revision>=UINT64_C(9007199254740991))return 0;
        }else if(bit==8){
            if(!aaw_string(&r,action,sizeof(action))||(strcmp(action,"read")&&strcmp(action,"unread")&&strcmp(action,"dismiss")&&strcmp(action,"restore")))return 0;
        }else if(bit==16){if(!aaw_string(&r,request,sizeof(request))||strlen(request)<8||!aaw_atom(request,96))return 0;}
        else {binding=aaw_expected_binding(&r,actor,subject,scope);if(!binding)return 0;}
    }
    if(seen!=63||r.pos!=r.len)return 0;
    if(binding!=1)return -1;
    return !bf_mp_pack_array(out,2)&&!bf_mp_pack_uint(out,1)&&!bf_mp_pack_map(out,5)&&
        !bf_mp_pack_kv_str(out,"id",id)&&!bf_mp_pack_kv_str(out,"version",version)&&
        !bf_mp_pack_str_cstr(out,"revision")&&!bf_mp_pack_uint(out,revision)&&
        !bf_mp_pack_kv_str(out,"action",action)&&!bf_mp_pack_kv_str(out,"requestId",request);
}
static int aaw_skip_value(bf_mp_reader_t *r,unsigned depth,size_t *budget) {
    if(depth>24||!*budget)return 0;
    --*budget;bf_mp_value_t v;bf_mp_type_t type=bf_mp_read_next(r,&v);
    if(type==BF_MP_ARRAY||type==BF_MP_MAP){
        size_t count=(size_t)v.v.count;
        if(type==BF_MP_MAP){if(count>*budget/2)return 0;count*=2;}
        if(count>*budget)return 0;
        for(size_t i=0;i<count;i++)if(!aaw_skip_value(r,depth+1,budget))return 0;
        return 1;
    }
    return type==BF_MP_NIL||type==BF_MP_BOOL||type==BF_MP_INT||type==BF_MP_UINT||
        type==BF_MP_FLOAT||type==BF_MP_DOUBLE||type==BF_MP_STR||type==BF_MP_BIN;
}
/* Attach the binding to every successful Queue read/action/receipt. Preserve
 * native fields exactly, reject colliding keys, and retain native error packets. */
static int aaw_bound_envelope(const unsigned char *bytes,size_t size,const char *actor,const char *subject,const char *scope,bf_mp_writer_t *out) {
    if(!bytes||!size||size>AW_PACKET)return 0;
    bf_mp_reader_t r;bf_mp_value_t v;bf_mp_reader_init(&r,bytes,size);
    if(bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=3||!aaw_version(&r)||bf_mp_read_next(&r,&v)!=BF_MP_BOOL)return 0;
    int okay=v.v.b;
    if(bf_mp_read_next(&r,&v)!=BF_MP_MAP||v.v.count>64)return 0;
    uint32_t count=v.v.count;size_t start=r.pos,budget=32768;char keys[64][65];
    for(uint32_t i=0;i<count;i++){
        if(!aaw_string(&r,keys[i],sizeof(keys[i])))return 0;
        if(okay&&(!strcmp(keys[i],"binding")||!strcmp(keys[i],"source")))return 0;
        for(uint32_t j=0;j<i;j++)if(!strcmp(keys[i],keys[j]))return 0;
        if(!aaw_skip_value(&r,0,&budget))return 0;
    }
    if(r.pos!=r.len)return 0;
    if(!okay){
        if(!out->buf||out->cap<size)return 0;
        memcpy(out->buf,bytes,size);out->len=size;return 1;
    }
    if(bf_mp_pack_array(out,3)||bf_mp_pack_uint(out,1)||bf_mp_pack_bool(out,1)||bf_mp_pack_map(out,count+2))return 0;
    size_t payload=size-start;
    if(!out->buf||out->len>out->cap||payload>out->cap-out->len)return 0;
    memcpy(out->buf+out->len,bytes+start,payload);out->len+=payload;
    return !bf_mp_pack_str_cstr(out,"binding")&&!bf_mp_pack_map(out,3)&&
        !bf_mp_pack_kv_str(out,"actor",actor)&&!bf_mp_pack_kv_str(out,"subject",subject)&&!bf_mp_pack_kv_str(out,"scope",scope)&&
        !bf_mp_pack_kv_str(out,"source","account-activity")&&out->len<=AW_PACKET;
}
static int serve_account_activity(const char *method,const char *target,const char *query,const char *body,
                                   const char *raw,const char *host,const char *bearer,const char *auth_db) {
    if(strcmp(target,"/apps/_activity"))return 0;
    int writing=!strcmp(method,"POST");AawQuery q;
    if((!writing&&strcmp(method,"GET"))||!aaw_query(query,&q)||!aaw_operation(&q,writing)){
        aw_error(400,"Use one declared account activity operation and its exact coordinates.\n");return 1;
    }
    char actor[160],subject[160];const char *scope=NULL;
    if(psetup_context(bearer,auth_db,q.context,0,actor,sizeof(actor),subject,sizeof(subject),&scope)!=1){
        aw_error(403,"Sign in with current access to this account or organization.\n");return 1;
    }
    if(!strcmp(q.operation,"compute")&&strcmp(q.context,"self")){
        aw_error(403,"Compute evidence belongs to the current personal account.\n");return 1;
    }
    size_t bytes=0;
    if(writing){
        char origin[300],expected[300],content[80],length[24];
        if(!host||!*host||strlen(host)>250||strcspn(host,"\r\n/\\ ")!=strlen(host)){
            aw_error(403,"Submit this change from its current account workspace.\n");return 1;
        }
        int local=!strcmp(host,"localhost")||!strcmp(host,"127.0.0.1")||!strncmp(host,"localhost:",10)||!strncmp(host,"127.0.0.1:",10);
        snprintf(expected,sizeof(expected),"%s://%s",local?"http":"https",host);
        if(!aw_header(raw,"Origin",origin,sizeof(origin))||strcmp(origin,expected)){
            aw_error(403,"Submit this change from its current account workspace.\n");return 1;
        }
        if(!body||!aw_header(raw,"Content-Type",content,sizeof(content))||strcmp(content,"application/msgpack")||
           !aw_header(raw,"Content-Length",length,sizeof(length))||!*length||strlen(length)>4||
           strspn(length,"0123456789")!=strlen(length)||(bytes=(size_t)strtoul(length,NULL,10))==0||bytes>4096){
            aw_error(400,"Submit a bounded Inbox change packet.\n");return 1;
        }
    }
    const char *bindir=getenv("BONFYRE_BIN_DIR");if(!bindir||!*bindir)bindir="/usr/local/bin";
    char binary[PATH_MAX],database[PATH_MAX];
    int inbox=!strncmp(q.operation,"inbox",5),n=0;char *args[48];
    /* Work and its Inbox are read from the account work index over the owners
     * that record it (operator, fabric, workgraph). Fleming's queue is one
     * client's store and answers only where no index is configured. */
    const char *work=getenv("BONFYRE_ACCOUNT_WORK_DB"),*owner=getenv("BONFYRE_ACCOUNT_WORK_OWNER");
    int indexed=work&&*work=='/';
    if(*bindir!='/'||snprintf(binary,sizeof(binary),"%s/bonfyre-queue",bindir)>=(int)sizeof(binary)||access(binary,X_OK)||
       (indexed?snprintf(database,sizeof(database),"%s",work)>=(int)sizeof(database):
        (*fleming_root()!='/'||snprintf(database,sizeof(database),"%s/fleming-work.db",fleming_root())>=(int)sizeof(database)||access(database,R_OK)))){
        aw_error(503,"The native account activity owner is unavailable.\n");return 1;
    }
    if(!indexed&&!strcmp(q.operation,"outputs")){aw_error(503,"The account output index is unavailable.\\n");return 1;}
    /* Agent runs are read only from the account work index; Fleming has none. */
    if(!indexed&&(!strcmp(q.operation,"agents")||!strcmp(q.operation,"compute"))){aw_error(503,"The account index is unavailable.\n");return 1;}
    args[n++]=binary;args[n++]=inbox?"account-inbox":indexed?"account-work":"activity";
    if(inbox)args[n++]=writing?"set":!strcmp(q.operation,"inbox-receipt")?"receipt":"list";
    if(indexed&&!inbox)args[n++]=!strcmp(q.operation,"outputs")?"outputs":!strcmp(q.operation,"agents")?"agents":!strcmp(q.operation,"compute")?"compute":"read";
    args[n++]="--db";args[n++]=database;args[n++]="--subject";args[n++]=subject;args[n++]="--scope";args[n++]=(char*)scope;
    char sources[2048]="";
    if(indexed&&!inbox&&owner&&*owner&&strlen(owner)<64){
        const char *list=getenv("BONFYRE_ACCOUNT_WORK_SOURCES");
        if(list&&strlen(list)<sizeof(sources)){
            snprintf(sources,sizeof(sources),"%s",list);args[n++]="--owner";args[n++]=(char*)owner;
            for(char *save=NULL,*one=strtok_r(sources,";",&save);one&&n<40;one=strtok_r(NULL,";",&save)){args[n++]="--source";args[n++]=one;}
        }
    }
    if(inbox){args[n++]="--actor";args[n++]=actor;}
    if(q.mission[0]){args[n++]="--mission";args[n++]=q.mission;}
    if(q.cursor[0]){args[n++]="--cursor";args[n++]=q.cursor;}
    if(q.request[0]){args[n++]="--request";args[n++]=q.request;}args[n]=NULL;
    bf_mp_writer_t forwarded;bf_mp_writer_init(&forwarded,4096);
    if(writing){
        int accepted=aaw_inbox_change(body,bytes,actor,subject,scope,&forwarded);
        if(accepted!=1){
            aw_error(accepted<0?409:400,accepted<0?"The current account changed. Reopen this notification before acting.\n":"Submit one exact Inbox change with its current account binding.\n");
            bf_mp_writer_free(&forwarded);return 1;
        }
    }
    unsigned char *output=NULL;size_t size=0;
    int rc=aw_native_result(args,writing?forwarded.buf:NULL,writing?forwarded.len:0,&output,&size,1);
    bf_mp_writer_free(&forwarded);
    bf_mp_writer_t bound;bf_mp_writer_init(&bound,size<=AW_PACKET?size+768:0);
    if(!rc&&aaw_bound_envelope(output,size,actor,subject,scope,&bound))aw_response(200,"application/msgpack",bound.buf,bound.len,0);
    else aw_error(503,writing?"The Inbox change is unconfirmed. Check this request or retry the same change.\n":"Account activity could not be read.\n");
    bf_mp_writer_free(&bound);free(output);return 1;
}
/* Existing Fleming JSON is wire compatibility only. Parse one exact object so
 * a nested/duplicate context cannot select a different admission audience. */
static int aaw_fleming_input(const char *body,char account[33],char context[96]) {
    if(!body||strlen(body)>4096)return 0;
    bf_json_doc_t *doc=bf_json_parse_str(body,NULL,0);const bf_json_node_t *root=doc?bf_json_root(doc):NULL;
    int okay=0;unsigned seen=0;snprintf(context,96,"self");account[0]=0;
    if(!root||root->type!=BF_JSON_OBJECT||root->child_count<1||root->child_count>3)goto done;
    for(int i=root->first_child;i>=0;){
        const bf_json_node_t *v=bf_json_at(doc,i);if(!v||v->type!=BF_JSON_STRING||v->key_len<1||v->key_len>7)goto done;
        char key[8];memcpy(key,v->key,(size_t)v->key_len);key[v->key_len]=0;
        if(strlen(key)!=(size_t)v->key_len)goto done;
        unsigned bit=0;char *dest=NULL;size_t cap=0;
        if(!strcmp(key,"account")){bit=1;dest=account;cap=33;}else if(!strcmp(key,"context")){bit=2;dest=context;cap=96;}
        else if(!strcmp(key,"note")){bit=4;if(v->val.str.len<0||v->val.str.len>160)goto done;}else goto done;
        if(seen&bit)goto done;
        seen|=bit;
        if(dest){if(v->val.str.len<1||(size_t)v->val.str.len>=cap)goto done;memcpy(dest,v->val.str.ptr,(size_t)v->val.str.len);dest[v->val.str.len]=0;if(strlen(dest)!=(size_t)v->val.str.len)goto done;}
        i=v->next;
    }
    okay=(seen&1)&&strspn(account,"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-")==strlen(account);
done:if(doc)bf_json_free(doc);return okay;
}
#endif
