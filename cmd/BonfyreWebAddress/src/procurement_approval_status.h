#ifndef BONFYRE_WEB_PROCUREMENT_APPROVAL_STATUS_H
#define BONFYRE_WEB_PROCUREMENT_APPROVAL_STATUS_H

typedef struct {
    char key[512], id[160], authority_subject[256], body_sha[65], fees_attribution[32];
    long long revision,known_total_cents;
    int total_known;
} PasApproval;

static int pas_text(bf_mp_reader_t *r,char *out,size_t cap,int empty_ok){
    bf_mp_value_t v;
    if(bf_mp_read_next(r,&v)!=BF_MP_STR||v.v.raw.len>=cap||memchr(v.v.raw.ptr,0,v.v.raw.len))return 0;
    memcpy(out,v.v.raw.ptr,v.v.raw.len);out[v.v.raw.len]=0;
    return empty_ok||out[0];
}
static int pas_uint(bf_mp_reader_t *r,long long *out){
    bf_mp_value_t v;bf_mp_type_t t=bf_mp_read_next(r,&v);
    if(t==BF_MP_UINT&&v.v.u<INT_MAX){*out=(long long)v.v.u;return 1;}
    if(t==BF_MP_INT&&v.v.i>=0&&v.v.i<INT_MAX){*out=v.v.i;return 1;}
    return 0;
}
static int pas_bool(bf_mp_reader_t *r,int expected){
    bf_mp_value_t v;return bf_mp_read_next(r,&v)==BF_MP_BOOL&&v.v.b==expected;
}
static int pas_nullable_amount(bf_mp_reader_t *r,long long *out,int *known){
    bf_mp_value_t v;bf_mp_type_t t=bf_mp_read_next(r,&v);*known=0;*out=0;
    if(t==BF_MP_NIL)return 1;
    if(t==BF_MP_UINT&&v.v.u<=9007199254740991ULL){*known=1;*out=(long long)v.v.u;return 1;}
    if(t==BF_MP_INT&&v.v.i>=0&&v.v.i<=9007199254740991LL){*known=1;*out=v.v.i;return 1;}
    return 0;
}
static int pas_query_key(const char *query,char *out,size_t cap){
    if(!query||!*query)return 0;const char *eq=strchr(query,'=');
    if(!eq||strchr(eq+1,'&')||strncmp(query,"key=",4))return 0;
    return psetup_decode(eq+1,strlen(eq+1),out,cap)&&!strncmp(out,"procurement-approval:",21)&&strlen(out)>21;
}
static int pas_authority(bf_mp_reader_t *r,char *subject,size_t cap){
    bf_mp_value_t v;if(bf_mp_read_next(r,&v)!=BF_MP_MAP||v.v.count>32)return 0;
    const uint32_t fields=v.v.count;
    int owner=0,sub=0,decision=0,permissions=0;
    for(uint32_t i=0;i<fields;i++){
        char key[80];if(!pas_text(r,key,sizeof(key),0))return 0;
        if(!strcmp(key,"owner")){char value[64];if(!pas_text(r,value,sizeof(value),0)||strcmp(value,"AuthorityGraph"))return 0;owner=1;}
        else if(!strcmp(key,"subject")){if(!pas_text(r,subject,cap,0))return 0;sub=1;}
        else if(!strcmp(key,"decision")){char value[32];if(!pas_text(r,value,sizeof(value),0)||strcmp(value,"pending"))return 0;decision=1;}
        else if(!strcmp(key,"required_permissions")){
            if(bf_mp_read_next(r,&v)!=BF_MP_ARRAY||v.v.count!=2)return 0;
            char a[32],b[32];if(!pas_text(r,a,sizeof(a),0)||!pas_text(r,b,sizeof(b),0)||strcmp(a,"approve")||strcmp(b,"spend"))return 0;permissions=1;
        } else if(bf_mp_skip(r))return 0;
    }
    return owner&&sub&&decision&&permissions;
}
static int pas_approval_body(const uint8_t *body,size_t bytes,const char *record_key,PasApproval *out){
    bf_mp_reader_t r;bf_mp_value_t v;long long version=0;memset(out,0,sizeof(*out));
    bf_mp_reader_init(&r,body,bytes);
    if(bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=3||!pas_uint(&r,&version)||(version<2||version>4)||
       bf_mp_read_next(&r,&v)!=BF_MP_MAP||v.v.count>64)return 0;
    int schema=0,id=0,status=0,authority=0,total=0,fees=0,flags=0;
    for(uint32_t i=0;i<v.v.count;i++){
        char key[96];if(!pas_text(&r,key,sizeof(key),0))return 0;
        if(!strcmp(key,"schema")){char value[96];if(!pas_text(&r,value,sizeof(value),0)||strcmp(value,"bonfyre.procurement.approval-request.v1"))return 0;schema=1;}
        else if(!strcmp(key,"id")){if(!pas_text(&r,out->id,sizeof(out->id),0))return 0;id=1;}
        else if(!strcmp(key,"status")){char value[32];if(!pas_text(&r,value,sizeof(value),0)||strcmp(value,"pending"))return 0;status=1;}
        else if(!strcmp(key,"authority")){if(!pas_authority(&r,out->authority_subject,sizeof(out->authority_subject)))return 0;authority=1;}
        else if(!strcmp(key,"known_total_cents")){if(!pas_nullable_amount(&r,&out->known_total_cents,&out->total_known))return 0;total=1;}
        else if(!strcmp(key,"fees_attribution")){if(!pas_text(&r,out->fees_attribution,sizeof(out->fees_attribution),0)||(strcmp(out->fees_attribution,"full_quote")&&strcmp(out->fees_attribution,"unallocated")))return 0;fees=1;}
        else if(!strcmp(key,"approved")||!strcmp(key,"spend_authorized")||!strcmp(key,"quote_accepted")||
                !strcmp(key,"commitment_opened")||!strcmp(key,"order_created")||!strcmp(key,"payment_presented")){
            if(!pas_bool(&r,0))return 0;flags++;
        } else if(bf_mp_skip(&r))return 0;
    }
    if(bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=0||r.pos!=r.len||flags!=6)return 0;
    if(!schema||!id||!status||!authority||!total||!fees||strcmp(record_key+21,out->id))return 0;
    snprintf(out->key,sizeof(out->key),"%s",record_key);bf_sha256_hex(body,bytes,out->body_sha);return 1;
}
static int pas_native_get(const char *binding,const char *subject,const char *scope,const char *actor,const char *key,PasApproval *out){
    const char *bin=page_cms_env("BONFYRE_CMS_BINARY","/usr/local/bin/bonfyre-cms"),*cms=page_cms_env("BONFYRE_CMS_DB","/var/lib/bonfyre/cms/bonfyre_cms.db");
    char *const argv[]={(char*)bin,"pack-workspace","--operation","get","--binding",(char*)binding,"--subject",(char*)subject,"--scope",(char*)scope,"--actor",(char*)actor,"--prefix",(char*)key,"--cursor","","--epoch","-1","--db",(char*)cms,NULL};
    unsigned char *packet=NULL;size_t bytes=0;int rc=aw_native_result(argv,NULL,0,&packet,&bytes,1),ok=0;
    if(rc||!packet||bytes>AW_PACKET)goto done;
    bf_mp_reader_t r;bf_mp_value_t v;long long version=0,epoch=0,revision=0;char cursor[512],returned[512];
    bf_mp_reader_init(&r,packet,bytes);
    if(bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=5||!pas_uint(&r,&version)||version!=1||!pas_uint(&r,&epoch)||
       bf_mp_read_next(&r,&v)!=BF_MP_BOOL||v.v.b||!pas_text(&r,cursor,sizeof(cursor),1)||
       bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=1||bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=3||
       !pas_text(&r,returned,sizeof(returned),0)||strcmp(returned,key)||!pas_uint(&r,&revision)||revision<1||
       bf_mp_read_next(&r,&v)!=BF_MP_BIN||r.pos!=r.len)goto done;
    if(!pas_approval_body(v.v.raw.ptr,v.v.raw.len,key,out))goto done;out->revision=revision;ok=1;
done: free(packet);return ok;
}
typedef struct PasReconciliation {
    char state[32],decision[160],outcome[64],decider[256],decided_at[64],receipt[512];
    int assent,approve,spend,amount_ready,eligible;
} PasReconciliation;

static int pas_reconcile(const char *key,const PasApproval *approval,const char *auth_db,PasReconciliation *out){
    memset(out,0,sizeof(*out));sqlite3 *db=cms_open_ro();if(!db)return 503;
    sqlite3_stmt *s=NULL;char backing[64]="";int count=0;
    const char *sql="SELECT decision_id,outcome,decided_by,backing,COALESCE(receipt_ref,''),decided_at FROM governance_decision WHERE namespace='root' AND status='published' AND proposal_id=?1 AND COALESCE(evidence_ref,'')=?2 ORDER BY id";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK){sqlite3_close(db);return 503;}
    sqlite3_bind_text(s,1,key,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,approval->body_sha,-1,SQLITE_STATIC);
    while(sqlite3_step(s)==SQLITE_ROW){
        if(++count>1)break;const char *vals[6];for(int i=0;i<6;i++)vals[i]=(const char*)sqlite3_column_text(s,i);
        if(!vals[0]||!vals[1]||!vals[2]||!vals[3]||!vals[4]||!vals[5]||
           snprintf(out->decision,sizeof(out->decision),"%s",vals[0])>=(int)sizeof(out->decision)||
           snprintf(out->outcome,sizeof(out->outcome),"%s",vals[1])>=(int)sizeof(out->outcome)||
           snprintf(out->decider,sizeof(out->decider),"%s",vals[2])>=(int)sizeof(out->decider)||
           snprintf(backing,sizeof(backing),"%s",vals[3])>=(int)sizeof(backing)||
           snprintf(out->receipt,sizeof(out->receipt),"%s",vals[4])>=(int)sizeof(out->receipt)||
           snprintf(out->decided_at,sizeof(out->decided_at),"%s",vals[5])>=(int)sizeof(out->decided_at)){count=2;break;}
    }
    sqlite3_finalize(s);s=NULL;if(count>1){sqlite3_close(db);return 409;}
    if(count==1&&sqlite3_prepare_v2(db,"SELECT 1 FROM assent_signature WHERE namespace='root' AND status='published' AND about=?1 AND digest=?2 AND assented_by=?3 LIMIT 1",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,key,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,approval->body_sha,-1,SQLITE_STATIC);sqlite3_bind_text(s,3,out->decider,-1,SQLITE_STATIC);out->assent=sqlite3_step(s)==SQLITE_ROW;
    }
    sqlite3_finalize(s);sqlite3_close(db);
    if(count==1&&out->assent&&!strcmp(out->outcome,"approved")&&!strcmp(backing,"human_approval")){
        BfAuthority *auth=NULL;if(!bf_authority_open(auth_db,&auth,stderr)&&auth){
            out->approve=bf_authority_has(auth,out->decider,"approve",approval->authority_subject,out->decided_at,"procurement")==1;
            out->spend=bf_authority_has(auth,out->decider,"spend",approval->authority_subject,out->decided_at,"procurement")==1;
            bf_authority_close(auth);
        }
    }
    int authority_ready=count==1&&out->assent&&out->approve&&out->spend&&!strcmp(out->outcome,"approved")&&!strcmp(backing,"human_approval");
    out->amount_ready=approval->total_known&&!strcmp(approval->fees_attribution,"full_quote");
    out->eligible=authority_ready&&out->amount_ready;
    snprintf(out->state,sizeof(out->state),"%s",!count?"pending":out->eligible?"eligible":!strcmp(out->outcome,"rejected")?"rejected":authority_ready?"amount_unresolved":"decision_unverified");
    return 1;
}

static int pas_status(const char *method,const char *query,const char *binding,const char *subject,const char *scope,const char *actor,const char *auth_db){
    if(strcmp(method,"GET")){aw_error(405,"Use GET to reconcile an approval request.\n");return 1;}
    char key[512];if(!pas_query_key(query,key,sizeof(key))){aw_error(400,"Choose one exact procurement approval request.\n");return 1;}
    PasApproval approval;if(!pas_native_get(binding,subject,scope,actor,key,&approval)){aw_error(404,"The retained approval request is unavailable or invalid.\n");return 1;}
    PasReconciliation reconciliation;int rr=pas_reconcile(key,&approval,auth_db,&reconciliation);
    if(rr==409){aw_error(409,"More than one decision claims this exact approval request. Reconcile governance first.\n");return 1;}
    if(rr!=1){aw_error(503,"The decision owner could not be read.\n");return 1;}
    bf_mp_writer_t w;bf_mp_writer_init(&w,1024);
    int ok=!bf_mp_pack_array(&w,2)&&!bf_mp_pack_uint(&w,1)&&!bf_mp_pack_map(&w,17)&&
        !bf_mp_pack_kv_str(&w,"state",reconciliation.state)&&!bf_mp_pack_kv_str(&w,"record_key",key)&&!bf_mp_pack_str_cstr(&w,"revision")&&!bf_mp_pack_uint(&w,(uint64_t)approval.revision)&&
        !bf_mp_pack_kv_str(&w,"body_sha256",approval.body_sha)&&!bf_mp_pack_kv_str(&w,"authority_subject",approval.authority_subject)&&!bf_mp_pack_kv_str(&w,"decision_id",reconciliation.decision)&&
        !bf_mp_pack_kv_str(&w,"outcome",reconciliation.outcome)&&!bf_mp_pack_kv_str(&w,"decided_by",reconciliation.decider)&&!bf_mp_pack_kv_str(&w,"decided_at",reconciliation.decided_at)&&
        !bf_mp_pack_str_cstr(&w,"assent_present")&&!bf_mp_pack_bool(&w,reconciliation.assent)&&!bf_mp_pack_str_cstr(&w,"approve_authority")&&!bf_mp_pack_bool(&w,reconciliation.approve)&&
        !bf_mp_pack_str_cstr(&w,"spend_authority")&&!bf_mp_pack_bool(&w,reconciliation.spend)&&!bf_mp_pack_str_cstr(&w,"amount_known")&&!bf_mp_pack_bool(&w,reconciliation.amount_ready)&&
        !bf_mp_pack_str_cstr(&w,"known_total_cents")&&(approval.total_known?!bf_mp_pack_int(&w,approval.known_total_cents):!bf_mp_pack_nil(&w))&&
        !bf_mp_pack_str_cstr(&w,"eligible_for_order_intent")&&!bf_mp_pack_bool(&w,reconciliation.eligible)&&
        !bf_mp_pack_kv_str(&w,"cannot_infer","Eligibility requires an exact fully attributed spend total and is still not quote acceptance, reservation, commitment, purchase order, payment, settlement, fulfillment or delivery.")&&
        !bf_mp_pack_kv_str(&w,"decision_receipt",reconciliation.receipt);
    if(ok&&w.len<=8192)aw_response(200,"application/msgpack",w.buf,w.len,0);else aw_error(503,"The approval reconciliation could not be encoded.\n");
    bf_mp_writer_free(&w);return 1;
}
#endif
