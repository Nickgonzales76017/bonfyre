#ifndef BONFYRE_PROCUREMENT_ENACTMENT_H
#define BONFYRE_PROCUREMENT_ENACTMENT_H

#include "bf_json.h"
#include "bf_jsonw.h"

static int pde_mp_json_value(bf_mp_reader_t *r,bf_jsonw *w,int depth){
    if(depth>32)return 0;bf_mp_value_t v;bf_mp_type_t t=bf_mp_read_next(r,&v);
    if(t==BF_MP_NIL)bf_jsonw_null(w);
    else if(t==BF_MP_BOOL)bf_jsonw_bool(w,v.v.b);
    else if(t==BF_MP_INT)bf_jsonw_int(w,v.v.i);
    else if(t==BF_MP_UINT){if(v.v.u>INT64_MAX)return 0;bf_jsonw_int(w,(int64_t)v.v.u);}
    else if(t==BF_MP_FLOAT)bf_jsonw_num(w,(double)v.v.f);
    else if(t==BF_MP_DOUBLE)bf_jsonw_num(w,v.v.d);
    else if(t==BF_MP_STR)bf_jsonw_strn(w,(const char*)v.v.raw.ptr,v.v.raw.len);
    else if(t==BF_MP_ARRAY){
        if(v.v.count>4096)return 0;bf_jsonw_arr(w);
        for(uint32_t i=0;i<v.v.count;i++)if(!pde_mp_json_value(r,w,depth+1))return 0;
        bf_jsonw_arr_end(w);
    } else if(t==BF_MP_MAP){
        if(v.v.count>1024)return 0;bf_jsonw_obj(w);
        for(uint32_t i=0;i<v.v.count;i++){
            bf_mp_value_t key;if(bf_mp_read_next(r,&key)!=BF_MP_STR||!key.v.raw.len||key.v.raw.len>256)return 0;
            bf_jsonw_keyn(w,(const char*)key.v.raw.ptr,key.v.raw.len);
            if(!pde_mp_json_value(r,w,depth+1))return 0;
        }
        bf_jsonw_obj_end(w);
    } else return 0;
    return !w->oom;
}

static int pde_workspace_json(const uint8_t *body,size_t bytes,char **json,size_t *json_bytes){
    *json=NULL;*json_bytes=0;bf_mp_reader_t r;bf_mp_value_t v;long long version=0;bf_jsonw w;bf_jsonw_init(&w,0);
    bf_mp_reader_init(&r,body,bytes);
    if(bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=3||!pds_uint(&r,&version)||(version<2||version>4)||!pde_mp_json_value(&r,&w,0)||
       bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=0||r.pos!=r.len||w.oom||w.len>AW_PACKET){bf_jsonw_free(&w);return 0;}
    *json=malloc(w.len+1);if(!*json){bf_jsonw_free(&w);return 0;}memcpy(*json,w.buf,w.len);(*json)[w.len]=0;*json_bytes=w.len;bf_jsonw_free(&w);return 1;
}

static int pde_native_get_json(const char *binding,const char *subject,const char *scope,const char *actor,const char *key,long long *revision,char **json,size_t *json_bytes){
    const char *bin=page_cms_env("BONFYRE_CMS_BINARY","/usr/local/bin/bonfyre-cms"),*cms=page_cms_env("BONFYRE_CMS_DB","/var/lib/bonfyre/cms/bonfyre_cms.db");
    char *const argv[]={(char*)bin,"pack-workspace","--operation","get","--binding",(char*)binding,"--subject",(char*)subject,"--scope",(char*)scope,"--actor",(char*)actor,"--prefix",(char*)key,"--cursor","","--epoch","-1","--db",(char*)cms,NULL};
    unsigned char *packet=NULL;size_t bytes=0;int rc=aw_native_result(argv,NULL,0,&packet,&bytes,1),ok=0;
    if(rc||!packet||bytes>AW_PACKET)goto done;
    bf_mp_reader_t r;bf_mp_value_t v;long long wire=0,epoch=0,rev=0;char cursor[512],returned[512];
    bf_mp_reader_init(&r,packet,bytes);
    if(bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=5||!pds_uint(&r,&wire)||wire!=1||!pds_uint(&r,&epoch)||
       bf_mp_read_next(&r,&v)!=BF_MP_BOOL||v.v.b||!pds_text(&r,cursor,sizeof(cursor),1)||
       bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=1||bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=3||
       !pds_text(&r,returned,sizeof(returned),0)||strcmp(returned,key)||!pds_uint(&r,&rev)||rev<1||
       bf_mp_read_next(&r,&v)!=BF_MP_BIN||r.pos!=r.len||!pde_workspace_json(v.v.raw.ptr,v.v.raw.len,json,json_bytes))goto done;
    *revision=rev;ok=1;
done:free(packet);return ok;
}

static int pde_json_text(const bf_json_doc_t *doc,const bf_json_node_t *obj,const char *key,char *out,size_t cap){
    const bf_json_node_t *v=bf_json_obj_get(doc,obj,key);return v&&v->type==BF_JSON_STRING&&bf_json_get_str_copy(v,out,cap)>0;
}
static int pde_json_int(const bf_json_doc_t *doc,const bf_json_node_t *obj,const char *key,long long *out){
    const bf_json_node_t *v=bf_json_obj_get(doc,obj,key);if(!v||v->type!=BF_JSON_INT||v->val.i<0)return 0;*out=v->val.i;return 1;
}
static int pde_binary_path(char out[1024]){
    const char *configured=getenv("BONFYRE_PROCUREMENT_BINARY");if(configured&&*configured)return snprintf(out,1024,"%s",configured)<1024;
    const char *home=getenv("HOME");return home&&*home&&snprintf(out,1024,"%s/.bonfyre/bin/bonfyre-procurement",home)<1024;
}
static int pde_json_digest(const char *json,size_t bytes,char digest[65]){
    char err[256];bf_json_doc_t *doc=bf_json_parse(json,bytes,err,sizeof(err));if(!doc)return 0;const bf_json_node_t *root=bf_json_root(doc);int ok=root&&root->type==BF_JSON_OBJECT&&pde_json_text(doc,root,"digest",digest,65)&&strlen(digest)==64;bf_json_free(doc);return ok;
}

typedef struct PdeReady {
    char order_digest[65],approval_key[512],allocation_key[512];
    long long order_revision,approval_revision,allocation_revision;
    PasReconciliation reconciliation;
} PdeReady;

static int pde_regenerate(const char *binding,const char *subject,const char *scope,const char *actor,const char *auth_db,const char *key,PdeReady *ready){
    memset(ready,0,sizeof(*ready));PdsOrder order;if(!pds_native_get(binding,subject,scope,actor,key,&order))return 404;
    char *order_json=NULL,*approval_json=NULL,*allocation_json=NULL;size_t order_bytes=0,approval_bytes=0,allocation_bytes=0;long long order_revision=0,approval_revision=0,allocation_revision=0;
    if(!pde_native_get_json(binding,subject,scope,actor,key,&order_revision,&order_json,&order_bytes)||order_revision!=order.revision)goto unavailable;
    char err[256];bf_json_doc_t *odoc=bf_json_parse(order_json,order_bytes,err,sizeof(err));if(!odoc)goto unavailable;const bf_json_node_t *oroot=bf_json_root(odoc),*source=oroot?bf_json_obj_get(odoc,oroot,"source"):NULL;
    char id[160],prepared[64],approval_id[160],allocation_id[160],saved_digest[65];long long saved_approval_revision=0,saved_allocation_revision=0;
    if(!oroot||oroot->type!=BF_JSON_OBJECT||!source||source->type!=BF_JSON_OBJECT||!pde_json_text(odoc,oroot,"id",id,sizeof(id))||!pde_json_text(odoc,oroot,"prepared_at",prepared,sizeof(prepared))||
       !pde_json_text(odoc,oroot,"digest",saved_digest,sizeof(saved_digest))||strlen(saved_digest)!=64||
       !pde_json_text(odoc,source,"approval_id",approval_id,sizeof(approval_id))||!pde_json_text(odoc,source,"allocation_id",allocation_id,sizeof(allocation_id))||
       !pde_json_int(odoc,source,"approval_revision",&saved_approval_revision)||!pde_json_int(odoc,source,"allocation_revision",&saved_allocation_revision)){bf_json_free(odoc);goto unavailable;}
    if(snprintf(ready->approval_key,sizeof(ready->approval_key),"procurement-approval:%s",approval_id)>=(int)sizeof(ready->approval_key)||
       snprintf(ready->allocation_key,sizeof(ready->allocation_key),"procurement-allocation:%s",allocation_id)>=(int)sizeof(ready->allocation_key)){bf_json_free(odoc);goto unavailable;}
    bf_json_free(odoc);
    if(!pde_native_get_json(binding,subject,scope,actor,ready->approval_key,&approval_revision,&approval_json,&approval_bytes)||approval_revision!=saved_approval_revision)goto stale;
    if(!pde_native_get_json(binding,subject,scope,actor,ready->allocation_key,&allocation_revision,&allocation_json,&allocation_bytes)||allocation_revision!=saved_allocation_revision)goto stale;
    PasApproval approval;if(!pas_native_get(binding,subject,scope,actor,ready->approval_key,&approval)||approval.revision!=approval_revision)goto stale;
    int rr=pas_reconcile(ready->approval_key,&approval,auth_db,&ready->reconciliation);if(rr!=1)return rr;if(!ready->reconciliation.eligible)goto refused;
    bf_json_doc_t *adoc=bf_json_parse(allocation_json,allocation_bytes,err,sizeof(err)),*pdoc=bf_json_parse(approval_json,approval_bytes,err,sizeof(err));if(!adoc||!pdoc){bf_json_free(adoc);bf_json_free(pdoc);goto unavailable;}
    bf_jsonw w;bf_jsonw_init(&w,0);bf_jsonw_obj(&w);
    bf_jsonw_key(&w,"allocation");bf_jsonw_node(&w,adoc,bf_json_root(adoc),0);
    bf_jsonw_key(&w,"allocation_revision");bf_jsonw_int(&w,allocation_revision);
    bf_jsonw_key(&w,"approval");bf_jsonw_node(&w,pdoc,bf_json_root(pdoc),0);
    bf_jsonw_key(&w,"approval_revision");bf_jsonw_int(&w,approval_revision);
    bf_jsonw_key(&w,"authority_status");bf_jsonw_obj(&w);
    bf_jsonw_key(&w,"state");bf_jsonw_str(&w,"eligible");bf_jsonw_key(&w,"record_key");bf_jsonw_str(&w,ready->approval_key);bf_jsonw_key(&w,"revision");bf_jsonw_int(&w,approval_revision);
    bf_jsonw_key(&w,"body_sha256");bf_jsonw_str(&w,approval.body_sha);bf_jsonw_key(&w,"authority_subject");bf_jsonw_str(&w,approval.authority_subject);
    bf_jsonw_key(&w,"decision_id");bf_jsonw_str(&w,ready->reconciliation.decision);bf_jsonw_key(&w,"outcome");bf_jsonw_str(&w,ready->reconciliation.outcome);
    bf_jsonw_key(&w,"decided_by");bf_jsonw_str(&w,ready->reconciliation.decider);bf_jsonw_key(&w,"decided_at");bf_jsonw_str(&w,ready->reconciliation.decided_at);
    bf_jsonw_key(&w,"assent_present");bf_jsonw_bool(&w,1);bf_jsonw_key(&w,"approve_authority");bf_jsonw_bool(&w,1);bf_jsonw_key(&w,"spend_authority");bf_jsonw_bool(&w,1);bf_jsonw_key(&w,"eligible_for_order_intent");bf_jsonw_bool(&w,1);
    bf_jsonw_key(&w,"decision_receipt");bf_jsonw_str(&w,ready->reconciliation.receipt);bf_jsonw_obj_end(&w);
    bf_jsonw_key(&w,"id");bf_jsonw_str(&w,id);bf_jsonw_key(&w,"prepared_at");bf_jsonw_str(&w,prepared);bf_jsonw_obj_end(&w);
    bf_json_free(adoc);bf_json_free(pdoc);if(w.oom||w.len>AW_PACKET){bf_jsonw_free(&w);goto unavailable;}
    char binary[1024];if(!pde_binary_path(binary)){bf_jsonw_free(&w);goto owner_missing;}char *const argv[]={binary,"order-intent",NULL};unsigned char *generated=NULL;size_t generated_bytes=0;int rc=aw_native_result(argv,w.buf,w.len,&generated,&generated_bytes,0);bf_jsonw_free(&w);
    if(rc||!generated||generated_bytes>AW_PACKET){free(generated);goto owner_missing;}char regenerated[65];int digest_ok=pde_json_digest((char*)generated,generated_bytes,regenerated);free(generated);if(!digest_ok)goto unavailable;
    if(strcmp(saved_digest,regenerated))goto stale;
    snprintf(ready->order_digest,sizeof(ready->order_digest),"%s",saved_digest);ready->order_revision=order_revision;ready->approval_revision=approval_revision;ready->allocation_revision=allocation_revision;
    free(order_json);free(approval_json);free(allocation_json);return 1;
refused:free(order_json);free(approval_json);free(allocation_json);return 409;
stale:free(order_json);free(approval_json);free(allocation_json);return 412;
owner_missing:free(order_json);free(approval_json);free(allocation_json);return 503;
unavailable:free(order_json);free(approval_json);free(allocation_json);return 404;
}

static int pde_readiness(const char *method,const char *query,const char *binding,const char *subject,const char *scope,const char *actor,const char *auth_db){
    if(strcmp(method,"GET")){aw_error(405,"Use GET to inspect procurement enactment readiness.\n");return 1;}
    char key[512];if(!pds_query_key(query,key,sizeof(key))){aw_error(400,"Choose one exact procurement order-intent proposal.\n");return 1;}
    PdeReady ready;int rc=pde_regenerate(binding,subject,scope,actor,auth_db,key,&ready);
    if(rc!=1){
        const char *state=rc==503?"native_owner_unavailable":rc==412?"lineage_changed":rc==409?"approval_not_eligible":"record_unavailable";
        bf_mp_writer_t w;bf_mp_writer_init(&w,512);int ok=!bf_mp_pack_array(&w,2)&&!bf_mp_pack_uint(&w,1)&&!bf_mp_pack_map(&w,3)&&!bf_mp_pack_kv_str(&w,"state",state)&&!bf_mp_pack_str_cstr(&w,"ready")&&!bf_mp_pack_bool(&w,0)&&!bf_mp_pack_kv_str(&w,"cannot_infer","No quote acceptance, supplier acceptance, commitment, purchase order, payment, settlement or shipment is established by readiness.");
        if(ok)aw_response(200,"application/msgpack",w.buf,w.len,0);else aw_error(503,"Readiness could not be encoded.\n");bf_mp_writer_free(&w);return 1;
    }
    bf_mp_writer_t w;bf_mp_writer_init(&w,1024);int ok=!bf_mp_pack_array(&w,2)&&!bf_mp_pack_uint(&w,1)&&!bf_mp_pack_map(&w,10)&&
        !bf_mp_pack_kv_str(&w,"state","ready_for_supplier_acceptance")&&!bf_mp_pack_str_cstr(&w,"ready")&&!bf_mp_pack_bool(&w,1)&&
        !bf_mp_pack_kv_str(&w,"record_key",key)&&!bf_mp_pack_str_cstr(&w,"revision")&&!bf_mp_pack_uint(&w,(uint64_t)ready.order_revision)&&
        !bf_mp_pack_kv_str(&w,"order_digest",ready.order_digest)&&!bf_mp_pack_str_cstr(&w,"native_lineage_attested")&&!bf_mp_pack_bool(&w,1)&&
        !bf_mp_pack_str_cstr(&w,"approval_eligible")&&!bf_mp_pack_bool(&w,1)&&!bf_mp_pack_kv_str(&w,"decision_id",ready.reconciliation.decision)&&
        !bf_mp_pack_str_cstr(&w,"supplier_acceptance_present")&&!bf_mp_pack_bool(&w,0)&&
        !bf_mp_pack_kv_str(&w,"cannot_infer","Native lineage and buyer approval are current. Supplier acceptance, ProtocolContract enactment, CommitmentGraph opening, purchase order, payment, settlement and shipment remain unestablished.");
    if(ok&&w.len<=8192)aw_response(200,"application/msgpack",w.buf,w.len,0);else aw_error(503,"Readiness could not be encoded.\n");bf_mp_writer_free(&w);return 1;
}
#include "procurement_enactment_effect.h"
#endif
