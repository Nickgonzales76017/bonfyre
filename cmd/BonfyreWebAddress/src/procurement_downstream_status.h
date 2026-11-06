#ifndef BONFYRE_PROCUREMENT_DOWNSTREAM_STATUS_H
#define BONFYRE_PROCUREMENT_DOWNSTREAM_STATUS_H

#include "bf_commerce.h"

typedef struct PdsOrder {
    char key[512], id[160], digest[65], commitment[256], exchange[256], body_sha[65];
    long long revision;
} PdsOrder;

static int pds_text(bf_mp_reader_t *r,char *out,size_t cap,int empty_ok){
    bf_mp_value_t v;if(bf_mp_read_next(r,&v)!=BF_MP_STR||v.v.raw.len>=cap)return 0;
    memcpy(out,v.v.raw.ptr,v.v.raw.len);out[v.v.raw.len]=0;return empty_ok||out[0];
}
static int pds_uint(bf_mp_reader_t *r,long long *out){
    bf_mp_value_t v;bf_mp_type_t t=bf_mp_read_next(r,&v);
    if(t==BF_MP_UINT&&v.v.u<INT_MAX){*out=(long long)v.v.u;return 1;}
    if(t==BF_MP_INT&&v.v.i>=0&&v.v.i<INT_MAX){*out=v.v.i;return 1;}
    return 0;
}
static int pds_bool(bf_mp_reader_t *r,int expected){
    bf_mp_value_t v;return bf_mp_read_next(r,&v)==BF_MP_BOOL&&v.v.b==expected;
}
static int pds_query_key(const char *query,char *out,size_t cap){
    if(!query||!*query)return 0;const char *eq=strchr(query,'=');
    if(!eq||strchr(eq+1,'&')||strncmp(query,"key=",4))return 0;
    return psetup_decode(eq+1,strlen(eq+1),out,cap)&&!strncmp(out,"procurement-order-intent:",25)&&strlen(out)>25;
}
static int pds_commitment(bf_mp_reader_t *r,char *resource,size_t cap){
    bf_mp_value_t v;if(bf_mp_read_next(r,&v)!=BF_MP_MAP||v.v.count>24)return 0;
    int owner=0,status=0,proposed=0,quote=0,terms=0;
    for(uint32_t i=0;i<v.v.count;i++){
        char key[80];if(!pds_text(r,key,sizeof(key),0))return 0;
        if(!strcmp(key,"owner")){char value[64];if(!pds_text(r,value,sizeof(value),0)||strcmp(value,"CommitmentGraph"))return 0;owner=1;}
        else if(!strcmp(key,"status")){char value[32];if(!pds_text(r,value,sizeof(value),0)||strcmp(value,"unmaterialized"))return 0;status=1;}
        else if(!strcmp(key,"proposed_resource")){if(!pds_text(r,resource,cap,0))return 0;proposed=1;}
        else if(!strcmp(key,"requires_quote_acceptance")){if(!pds_bool(r,1))return 0;quote=1;}
        else if(!strcmp(key,"requires_supplier_counterparty_terms")){if(!pds_bool(r,1))return 0;terms=1;}
        else if(bf_mp_skip(r))return 0;
    }
    return owner&&status&&proposed&&quote&&terms;
}
static int pds_commerce(bf_mp_reader_t *r,char *exchange,size_t cap){
    bf_mp_value_t v;if(bf_mp_read_next(r,&v)!=BF_MP_MAP||v.v.count>24)return 0;
    int owner=0,status=0,id=0,next=0;
    for(uint32_t i=0;i<v.v.count;i++){
        char key[80];if(!pds_text(r,key,sizeof(key),0))return 0;
        if(!strcmp(key,"owner")){char value[64];if(!pds_text(r,value,sizeof(value),0)||strcmp(value,"CommerceChain"))return 0;owner=1;}
        else if(!strcmp(key,"status")){char value[32];if(!pds_text(r,value,sizeof(value),0)||strcmp(value,"unmaterialized"))return 0;status=1;}
        else if(!strcmp(key,"exchange_candidate")){if(!pds_text(r,exchange,cap,0))return 0;id=1;}
        else if(!strcmp(key,"next_required_stage")){char value[64];if(!pds_text(r,value,sizeof(value),0)||strcmp(value,"product"))return 0;next=1;}
        else if(bf_mp_skip(r))return 0;
    }
    return owner&&status&&id&&next;
}
static int pds_order_body(const uint8_t *body,size_t bytes,const char *record_key,PdsOrder *out){
    bf_mp_reader_t r;bf_mp_value_t v;long long version=0;memset(out,0,sizeof(*out));
    bf_mp_reader_init(&r,body,bytes);
    if(bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=3||!pds_uint(&r,&version)||(version<2||version>4)||
       bf_mp_read_next(&r,&v)!=BF_MP_MAP||v.v.count>96)return 0;
    int schema=0,id=0,status=0,commitment=0,commerce=0,eligible=0,effects=0;
    for(uint32_t i=0;i<v.v.count;i++){
        char key[96];if(!pds_text(&r,key,sizeof(key),0))return 0;
        if(!strcmp(key,"schema")){char value[96];if(!pds_text(&r,value,sizeof(value),0)||strcmp(value,"bonfyre.procurement.order-intent.v1"))return 0;schema=1;}
        else if(!strcmp(key,"id")){if(!pds_text(&r,out->id,sizeof(out->id),0))return 0;id=1;}
        else if(!strcmp(key,"status")){char value[32];if(!pds_text(&r,value,sizeof(value),0)||strcmp(value,"prepared"))return 0;status=1;}
        else if(!strcmp(key,"digest")){if(!pds_text(&r,out->digest,sizeof(out->digest),0)||strlen(out->digest)!=64||strspn(out->digest,"0123456789abcdef")!=64)return 0;}
        else if(!strcmp(key,"commitment_handoff")){if(!pds_commitment(&r,out->commitment,sizeof(out->commitment)))return 0;commitment=1;}
        else if(!strcmp(key,"commerce_chain")){if(!pds_commerce(&r,out->exchange,sizeof(out->exchange)))return 0;commerce=1;}
        else if(!strcmp(key,"authority_eligible")){if(!pds_bool(&r,1))return 0;eligible=1;}
        else if(!strcmp(key,"quote_accepted")||!strcmp(key,"reservation_created")||!strcmp(key,"cart_created")||
                !strcmp(key,"commitment_opened")||!strcmp(key,"order_created")||!strcmp(key,"purchase_order_issued")||
                !strcmp(key,"supplier_accepted")||!strcmp(key,"payable_created")||!strcmp(key,"payment_presented")||
                !strcmp(key,"settled")||!strcmp(key,"fulfilled")||!strcmp(key,"shipped")||!strcmp(key,"received")){
            if(!pds_bool(&r,0))return 0;effects++;
        } else if(bf_mp_skip(&r))return 0;
    }
    if(bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=0||r.pos!=r.len||effects!=13)return 0;
    if(!schema||!id||!out->digest[0]||!status||!commitment||!commerce||!eligible||strcmp(record_key+25,out->id))return 0;
    snprintf(out->key,sizeof(out->key),"%s",record_key);bf_sha256_hex(body,bytes,out->body_sha);return 1;
}
static int pds_native_get(const char *binding,const char *subject,const char *scope,const char *actor,const char *key,PdsOrder *out){
    const char *bin=page_cms_env("BONFYRE_CMS_BINARY","/usr/local/bin/bonfyre-cms"),*cms=page_cms_env("BONFYRE_CMS_DB","/var/lib/bonfyre/cms/bonfyre_cms.db");
    char *const argv[]={(char*)bin,"pack-workspace","--operation","get","--binding",(char*)binding,"--subject",(char*)subject,"--scope",(char*)scope,"--actor",(char*)actor,"--prefix",(char*)key,"--cursor","","--epoch","-1","--db",(char*)cms,NULL};
    unsigned char *packet=NULL;size_t bytes=0;int rc=aw_native_result(argv,NULL,0,&packet,&bytes,1),ok=0;
    if(rc||!packet||bytes>AW_PACKET)goto done;
    bf_mp_reader_t r;bf_mp_value_t v;long long version=0,epoch=0,revision=0;char cursor[512],returned[512];
    bf_mp_reader_init(&r,packet,bytes);
    if(bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=5||!pds_uint(&r,&version)||version!=1||!pds_uint(&r,&epoch)||
       bf_mp_read_next(&r,&v)!=BF_MP_BOOL||v.v.b||!pds_text(&r,cursor,sizeof(cursor),1)||
       bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=1||bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=3||
       !pds_text(&r,returned,sizeof(returned),0)||strcmp(returned,key)||!pds_uint(&r,&revision)||revision<1||
       bf_mp_read_next(&r,&v)!=BF_MP_BIN||r.pos!=r.len)goto done;
    if(!pds_order_body(v.v.raw.ptr,v.v.raw.len,key,out))goto done;out->revision=revision;ok=1;
done: free(packet);return ok;
}
static int pds_known_commitment_state(const char *state){
    for(int n=BF_COMMITMENT_OPEN;n<=BF_COMMITMENT_COMPENSATED;n++)if(!strcmp(state,bf_commitment_state_name((BfCommitmentState)n)))return 1;
    return 0;
}
static int pds_status(const char *method,const char *query,const char *binding,const char *subject,const char *scope,const char *actor){
    if(strcmp(method,"GET")){aw_error(405,"Use GET to reconcile downstream procurement state.\n");return 1;}
    char key[512];if(!pds_query_key(query,key,sizeof(key))){aw_error(400,"Choose one exact procurement order-intent proposal.\n");return 1;}
    PdsOrder order;if(!pds_native_get(binding,subject,scope,actor,key,&order)){aw_error(404,"The retained order-intent proposal is unavailable or invalid.\n");return 1;}
    const char *commitment_path=getenv("BONFYRE_COMMITMENT_DB"),*commerce_path=getenv("BONFYRE_COMMERCE_DB");
    if(!commitment_path||!*commitment_path||!commerce_path||!*commerce_path){aw_error(503,"The downstream commitment or commerce owner is not configured.\n");return 1;}

    sqlite3 *cdb=NULL,*xdb=NULL;sqlite3_stmt *s=NULL;char commitment_state[32]="",commitment_actor[256]="",commitment_at[64]="",origin[96];
    char protocol_ids[64][BF_COMMITMENT_ID_MAX],protocol_enactments[64][BF_COMMITMENT_ID_MAX],protocol_states[64][32],protocol_actors[64][256],protocol_times[64][64];size_t protocol_count=0;
    int commitment_found=0,commitment_delivered=0;
    snprintf(origin,sizeof(origin),"procurement-order:%s",order.digest);
    if(sqlite3_open_v2(commitment_path,&cdb,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){if(cdb)sqlite3_close(cdb);aw_error(503,"The commitment owner could not be read.\n");return 1;}
    if(sqlite3_prepare_v2(cdb,"SELECT c.commitment_id,c.enactment,e.state,e.actor,e.at FROM commitments c JOIN commitment_events e ON e.seq=(SELECT max(e2.seq) FROM commitment_events e2 WHERE e2.commitment_id=c.commitment_id) WHERE c.origin=?1 ORDER BY c.commitment_id",-1,&s,NULL)!=SQLITE_OK){sqlite3_close(cdb);aw_error(503,"The commitment owner could not be read.\n");return 1;}
    sqlite3_bind_text(s,1,origin,-1,SQLITE_STATIC);
    while(sqlite3_step(s)==SQLITE_ROW){
        if(protocol_count>=64){sqlite3_finalize(s);sqlite3_close(cdb);aw_error(409,"The order intent has too many attributed protocol commitments.\n");return 1;}
        const char *id=(const char*)sqlite3_column_text(s,0),*enact=(const char*)sqlite3_column_text(s,1),*state=(const char*)sqlite3_column_text(s,2),*who=(const char*)sqlite3_column_text(s,3),*at=(const char*)sqlite3_column_text(s,4);
        if(!id||!enact||!state||!pds_known_commitment_state(state)||
           snprintf(protocol_ids[protocol_count],sizeof(protocol_ids[protocol_count]),"%s",id)>=(int)sizeof(protocol_ids[protocol_count])||
           snprintf(protocol_enactments[protocol_count],sizeof(protocol_enactments[protocol_count]),"%s",enact)>=(int)sizeof(protocol_enactments[protocol_count])||
           snprintf(protocol_states[protocol_count],sizeof(protocol_states[protocol_count]),"%s",state)>=(int)sizeof(protocol_states[protocol_count])||
           snprintf(protocol_actors[protocol_count],sizeof(protocol_actors[protocol_count]),"%s",who?who:"")>=(int)sizeof(protocol_actors[protocol_count])||
           snprintf(protocol_times[protocol_count],sizeof(protocol_times[protocol_count]),"%s",at?at:"")>=(int)sizeof(protocol_times[protocol_count])){sqlite3_finalize(s);sqlite3_close(cdb);aw_error(409,"The commitment owner returned unsupported attributed state.\n");return 1;}
        protocol_count++;
    }
    sqlite3_finalize(s);s=NULL;
    if(protocol_count){
        commitment_found=1;commitment_delivered=1;snprintf(commitment_state,sizeof(commitment_state),"%s",protocol_states[0]);
        for(size_t i=0;i<protocol_count;i++){if(strcmp(protocol_states[i],"discharged"))commitment_delivered=0;if(strcmp(commitment_state,protocol_states[i]))snprintf(commitment_state,sizeof(commitment_state),"mixed");if(strcmp(protocol_times[i],commitment_at)>0)snprintf(commitment_at,sizeof(commitment_at),"%s",protocol_times[i]);}
        snprintf(commitment_actor,sizeof(commitment_actor),"%s",protocol_count==1?protocol_actors[0]:"multiple");
    }else{
        if(sqlite3_prepare_v2(cdb,"SELECT state,actor,at FROM commitment_events WHERE commitment_id=?1 ORDER BY seq DESC LIMIT 1",-1,&s,NULL)!=SQLITE_OK){sqlite3_close(cdb);aw_error(503,"The commitment owner could not be read.\n");return 1;}
        sqlite3_bind_text(s,1,order.commitment,-1,SQLITE_STATIC);
        if(sqlite3_step(s)==SQLITE_ROW){
            const char *state=(const char*)sqlite3_column_text(s,0),*who=(const char*)sqlite3_column_text(s,1),*at=(const char*)sqlite3_column_text(s,2);
            if(!state||!pds_known_commitment_state(state)||snprintf(commitment_state,sizeof(commitment_state),"%s",state)>=(int)sizeof(commitment_state)||snprintf(commitment_actor,sizeof(commitment_actor),"%s",who?who:"")>=(int)sizeof(commitment_actor)||snprintf(commitment_at,sizeof(commitment_at),"%s",at?at:"")>=(int)sizeof(commitment_at)){sqlite3_finalize(s);sqlite3_close(cdb);aw_error(409,"The commitment owner returned an unsupported state.\n");return 1;}
            commitment_found=1;commitment_delivered=!strcmp(commitment_state,"discharged");
        }
        sqlite3_finalize(s);s=NULL;
    }
    sqlite3_close(cdb);

    if(sqlite3_open_v2(commerce_path,&xdb,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){if(xdb)sqlite3_close(xdb);aw_error(503,"The commerce owner could not be read.\n");return 1;}
    if(sqlite3_prepare_v2(xdb,"SELECT stage FROM commerce_progress WHERE exchange=?1",-1,&s,NULL)!=SQLITE_OK){sqlite3_close(xdb);aw_error(503,"The commerce owner could not be read.\n");return 1;}
    int reached[BF_COMMERCE_STAGE_COUNT]={0};BfCommerceStage furthest=BF_COMMERCE_PRODUCT;int furthest_found=0;
    sqlite3_bind_text(s,1,order.exchange,-1,SQLITE_STATIC);
    while(sqlite3_step(s)==SQLITE_ROW){
        const char *stage=(const char*)sqlite3_column_text(s,0);int known=0;
        for(int n=BF_COMMERCE_PRODUCT;n<BF_COMMERCE_STAGE_COUNT;n++)if(stage&&!strcmp(stage,bf_commerce_stage_name((BfCommerceStage)n))){reached[n]=1;if(!furthest_found||n>(int)furthest){furthest=(BfCommerceStage)n;furthest_found=1;}known=1;break;}
        if(!known){sqlite3_finalize(s);sqlite3_close(xdb);aw_error(409,"The commerce owner returned an unsupported stage.\n");return 1;}
    }
    sqlite3_finalize(s);sqlite3_close(xdb);

    bf_mp_writer_t w;bf_mp_writer_init(&w,2048);
    int outstanding=0,established=0;for(int n=0;n<BF_COMMERCE_STAGE_COUNT;n++){if(reached[n])established++;else outstanding++;}
    int ok=!bf_mp_pack_array(&w,2)&&!bf_mp_pack_uint(&w,1)&&!bf_mp_pack_map(&w,17)&&
      !bf_mp_pack_kv_str(&w,"state",commitment_found||furthest_found?"downstream_observed":"unmaterialized")&&
      !bf_mp_pack_kv_str(&w,"record_key",key)&&!bf_mp_pack_str_cstr(&w,"revision")&&!bf_mp_pack_uint(&w,(uint64_t)order.revision)&&
      !bf_mp_pack_kv_str(&w,"body_sha256",order.body_sha)&&!bf_mp_pack_kv_str(&w,"commitment_resource",order.commitment)&&
      !bf_mp_pack_str_cstr(&w,"commitment_found")&&!bf_mp_pack_bool(&w,commitment_found)&&
      !bf_mp_pack_kv_str(&w,"commitment_state",commitment_state)&&!bf_mp_pack_kv_str(&w,"commitment_actor",commitment_actor)&&
      !bf_mp_pack_kv_str(&w,"commitment_at",commitment_at)&&!bf_mp_pack_str_cstr(&w,"commitment_delivered")&&!bf_mp_pack_bool(&w,commitment_delivered)&&
      !bf_mp_pack_str_cstr(&w,"protocol_commitment_count")&&!bf_mp_pack_uint(&w,(uint64_t)protocol_count)&&!bf_mp_pack_str_cstr(&w,"protocol_commitments")&&!bf_mp_pack_array(&w,(uint32_t)protocol_count);
    for(size_t i=0;ok&&i<protocol_count;i++)ok=!bf_mp_pack_map(&w,5)&&!bf_mp_pack_kv_str(&w,"commitment_id",protocol_ids[i])&&!bf_mp_pack_kv_str(&w,"enactment",protocol_enactments[i])&&!bf_mp_pack_kv_str(&w,"state",protocol_states[i])&&!bf_mp_pack_kv_str(&w,"actor",protocol_actors[i])&&!bf_mp_pack_kv_str(&w,"at",protocol_times[i]);
    if(ok)ok=!bf_mp_pack_kv_str(&w,"commerce_exchange",order.exchange)&&!bf_mp_pack_kv_str(&w,"commerce_furthest",furthest_found?bf_commerce_stage_name(furthest):"")&&
      !bf_mp_pack_str_cstr(&w,"established")&&!bf_mp_pack_array(&w,(uint32_t)established);
    for(int n=0;ok&&n<BF_COMMERCE_STAGE_COUNT;n++)if(reached[n])ok=!bf_mp_pack_map(&w,3)&&!bf_mp_pack_kv_str(&w,"stage",bf_commerce_stage_name((BfCommerceStage)n))&&!bf_mp_pack_kv_str(&w,"owner",bf_commerce_stage_owner((BfCommerceStage)n))&&!bf_mp_pack_kv_str(&w,"does_not_establish",bf_commerce_stage_does_not_establish((BfCommerceStage)n));
    if(ok)ok=!bf_mp_pack_str_cstr(&w,"outstanding")&&!bf_mp_pack_array(&w,(uint32_t)outstanding);
    for(int n=0;ok&&n<BF_COMMERCE_STAGE_COUNT;n++)if(!reached[n])ok=!bf_mp_pack_map(&w,2)&&!bf_mp_pack_kv_str(&w,"stage",bf_commerce_stage_name((BfCommerceStage)n))&&!bf_mp_pack_kv_str(&w,"owner",bf_commerce_stage_owner((BfCommerceStage)n));
    if(ok)ok=!bf_mp_pack_kv_str(&w,"cannot_infer","Observed downstream state establishes only the exact CommitmentGraph state and CommerceChain stages shown. It does not establish quote acceptance, supplier acceptance, purchase order issuance, payment, settlement, fulfilment, delivery, satisfaction or ownership.");
    if(ok&&w.len<=16384)aw_response(200,"application/msgpack",w.buf,w.len,0);else aw_error(503,"Downstream procurement state could not be encoded.\n");
    bf_mp_writer_free(&w);return 1;
}
#endif
