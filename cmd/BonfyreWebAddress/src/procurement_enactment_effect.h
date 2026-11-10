#ifndef BONFYRE_PROCUREMENT_ENACTMENT_EFFECT_H
#define BONFYRE_PROCUREMENT_ENACTMENT_EFFECT_H

typedef struct PdeEnactRequest {
    long long revision;
    char order_digest[65];
    char quote_id[160];
    char exchange[96];
    char buyer[257];
    char seller[257];
    char settlement[257];
    char deliverer[257];
    char capability[513];
    char signature[129];
    char accepted_at[21];
} PdeEnactRequest;

static int pde_effect_hex(const char *s,size_t n){
    if(!s||strlen(s)!=n)return 0;
    for(size_t i=0;i<n;i++)if(!((s[i]>='0'&&s[i]<='9')||(s[i]>='a'&&s[i]<='f')))return 0;
    return 1;
}
static int pde_effect_utc(const char *s){
    if(!s||strlen(s)!=20||s[4]!='-'||s[7]!='-'||s[10]!='T'||s[13]!=':'||s[16]!=':'||s[19]!='Z')return 0;
    for(int i=0;i<20;i++)if(i!=4&&i!=7&&i!=10&&i!=13&&i!=16&&i!=19&&(s[i]<'0'||s[i]>'9'))return 0;
    return 1;
}
static int pde_effect_ed25519(const char *s){
    return s&&!strncmp(s,"ed25519:",8)&&strlen(s)==72&&pde_effect_hex(s+8,64);
}
static int pde_effect_packet(const void *body,size_t bytes,PdeEnactRequest *q){
    memset(q,0,sizeof(*q));
    bf_mp_reader_t r;bf_mp_value_t v;long long wire=0,revision=0;bf_mp_reader_init(&r,body,bytes);
    if(bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=12||!pds_uint(&r,&wire)||wire!=1||!pds_uint(&r,&revision)||revision<1||
       !pds_text(&r,q->order_digest,sizeof(q->order_digest),0)||!pds_text(&r,q->quote_id,sizeof(q->quote_id),0)||
       !pds_text(&r,q->exchange,sizeof(q->exchange),0)||!pds_text(&r,q->buyer,sizeof(q->buyer),0)||!pds_text(&r,q->seller,sizeof(q->seller),0)||
       !pds_text(&r,q->settlement,sizeof(q->settlement),0)||!pds_text(&r,q->deliverer,sizeof(q->deliverer),0)||!pds_text(&r,q->capability,sizeof(q->capability),0)||
       !pds_text(&r,q->signature,sizeof(q->signature),0)||!pds_text(&r,q->accepted_at,sizeof(q->accepted_at),0)||r.pos!=r.len)return 0;
    q->revision=revision;
    return pde_effect_hex(q->order_digest,64)&&pde_effect_hex(q->signature,128)&&pde_effect_utc(q->accepted_at);
}
static int pde_effect_json_true(const bf_json_doc_t *doc,const bf_json_node_t *obj,const char *key){
    const bf_json_node_t *v=bf_json_obj_get(doc,obj,key);
    return v&&v->type==BF_JSON_BOOL&&v->val.b;
}
static int pde_effect_order_lot(const char *json,size_t bytes,const PdeEnactRequest *q,char quote_digest[65],char supplier_id[257],char currency[4],long long *amount){
    char err[256];bf_json_doc_t *doc=bf_json_parse(json,bytes,err,sizeof(err));
    if(!doc)return 0;
    const bf_json_node_t *root=bf_json_root(doc),*lots=root?bf_json_obj_get(doc,root,"order_lots"):NULL;
    int ok=0,hits=0;
    if(!root||root->type!=BF_JSON_OBJECT||!lots||lots->type!=BF_JSON_ARRAY||lots->child_count<1||lots->child_count>32||
       !pde_json_text(doc,root,"currency",currency,4)||strlen(currency)!=3)goto done;
    for(int c=0;c<3;c++)if(currency[c]<'A'||currency[c]>'Z')goto done;
    for(int i=0;i<lots->child_count;i++){
        const bf_json_node_t *lot=bf_json_arr_get(doc,lots,i);char quote[160];
        if(!lot||lot->type!=BF_JSON_OBJECT||!pde_json_text(doc,lot,"quote_id",quote,sizeof(quote))||strcmp(quote,q->quote_id))continue;
        hits++;
        const bf_json_node_t *supplier=bf_json_obj_get(doc,lot,"supplier"),*valid=bf_json_obj_get(doc,lot,"valid_until");
        if(!supplier||supplier->type!=BF_JSON_OBJECT||!pde_json_text(doc,lot,"digest",quote_digest,65)||!pde_effect_hex(quote_digest,64)||
           !pde_json_text(doc,supplier,"id",supplier_id,257)||!pde_json_int(doc,lot,"known_total_cents",amount)||!pde_effect_json_true(doc,lot,"full_quote_consumed"))goto done;
        if(valid&&valid->type!=BF_JSON_NULL){
            char until[21];
            if(valid->type!=BF_JSON_STRING||bf_json_get_str_copy(valid,until,sizeof(until))<=0||!pde_effect_utc(until)||strcmp(q->accepted_at,until)>0)goto done;
        }
    }
    ok=hits==1&&pde_effect_ed25519(supplier_id)&&!strcmp(supplier_id,q->seller);
done:
    bf_json_free(doc);
    return ok;
}
static int pde_effect_receipt(const unsigned char *json,size_t bytes,const PdeEnactRequest *q,const char *supplier,int *replayed){
    char err[256],schema[96],exchange[96],order[65],quote[160],supplier_got[257];
    bf_json_doc_t *doc=bf_json_parse((const char*)json,bytes,err,sizeof(err));
    if(!doc)return 0;
    const bf_json_node_t *root=bf_json_root(doc),*replay=root?bf_json_obj_get(doc,root,"replayed"):NULL;
    int ok=root&&root->type==BF_JSON_OBJECT&&
      pde_json_text(doc,root,"schema",schema,sizeof(schema))&&!strcmp(schema,"bonfyre.procurement.enactment-receipt.v1")&&
      pde_json_text(doc,root,"exchange",exchange,sizeof(exchange))&&!strcmp(exchange,q->exchange)&&
      pde_json_text(doc,root,"order_intent_digest",order,sizeof(order))&&!strcmp(order,q->order_digest)&&
      pde_json_text(doc,root,"quote_id",quote,sizeof(quote))&&!strcmp(quote,q->quote_id)&&
      pde_json_text(doc,root,"supplier_id",supplier_got,sizeof(supplier_got))&&!strcmp(supplier_got,supplier)&&
      pde_effect_json_true(doc,root,"supplier_identity_binding_verified")&&pde_effect_json_true(doc,root,"supplier_acceptance_verified")&&
      pde_effect_json_true(doc,root,"protocol_enacted")&&pde_effect_json_true(doc,root,"commitment_opened")&&replay&&replay->type==BF_JSON_BOOL;
    if(ok)*replayed=replay->val.b;
    bf_json_free(doc);
    return ok;
}
static int pde_enact(const char *method,const char *query,const char *body,const char *raw,const char *host,const char *binding,const char *subject,const char *scope,const char *actor,const char *auth_db){
    if(strcmp(method,"POST")){aw_error(405,"Use POST for one signed supplier acceptance.\n");return 1;}
    char key[512],origin[512],content_type[128],length[32],expected[512];
    if(!pds_query_key(query,key,sizeof(key))){aw_error(400,"Choose one exact procurement order-intent proposal.\n");return 1;}
    snprintf(expected,sizeof(expected),"%s://%s",(host&&(!strncmp(host,"127.0.0.1:",10)||!strncmp(host,"localhost:",10)))?"http":"https",host?host:"");
    if(!aw_header(raw,"Origin",origin,sizeof(origin))||strcmp(origin,expected)||!aw_header(raw,"Content-Type",content_type,sizeof(content_type))||strcmp(content_type,"application/msgpack")||
       !aw_header(raw,"Content-Length",length,sizeof(length))||!*length||strspn(length,"0123456789")!=strlen(length)){
        aw_error(403,"Submit supplier acceptance from its current native workspace.\n");return 1;
    }
    errno=0;unsigned long long parsed=strtoull(length,NULL,10);
    if(errno||!parsed||parsed>16384){aw_error(413,"Supplier acceptance exceeds its bounded packet.\n");return 1;}
    PdeEnactRequest request;
    if(!pde_effect_packet(body,(size_t)parsed,&request)){aw_error(400,"Supplier acceptance packet is invalid.\n");return 1;}
    PdeReady ready;int rr=pde_regenerate(binding,subject,scope,actor,auth_db,key,&ready);
    if(rr!=1){
        aw_error(rr==503?503:409,rr==412?"The order intent lineage changed before supplier acceptance.\n":rr==409?"Buyer approval is no longer eligible for enactment.\n":"The exact order intent is unavailable for enactment.\n");
        return 1;
    }
    if(request.revision!=ready.order_revision||strcmp(request.order_digest,ready.order_digest)){aw_error(409,"The saved order intent changed before supplier acceptance.\n");return 1;}
    char *order_json=NULL;size_t order_bytes=0;long long order_revision=0;
    if(!pde_native_get_json(binding,subject,scope,actor,key,&order_revision,&order_json,&order_bytes)||order_revision!=ready.order_revision){
        free(order_json);aw_error(409,"The exact order intent changed before supplier acceptance.\n");return 1;
    }
    char quote_digest[65],supplier_id[257],currency[4];long long amount=0;
    int lot_ok=pde_effect_order_lot(order_json,order_bytes,&request,quote_digest,supplier_id,currency,&amount);free(order_json);
    if(!lot_ok){aw_error(409,"The signed supplier acceptance does not bind one current fully priced cryptographic supplier lot.\n");return 1;}
    const char *commitments=getenv("BONFYRE_COMMITMENT_DB");char binary[1024],amount_text[32];
    if(!commitments||!*commitments||!pde_binary_path(binary)){aw_error(503,"The native procurement effect owner is unavailable.\n");return 1;}
    snprintf(amount_text,sizeof(amount_text),"%lld",amount);
    char *const argv[]={binary,"enact","--commitments-db",(char*)commitments,"--authority-db",(char*)auth_db,"--exchange",request.exchange,"--order-intent-digest",request.order_digest,
      "--quote-id",request.quote_id,"--quote-digest",quote_digest,"--supplier-id",supplier_id,"--capability",request.capability,"--buyer",request.buyer,"--seller",request.seller,
      "--settlement",request.settlement,"--deliverer",request.deliverer,"--amount-cents",amount_text,"--currency",currency,"--signature",request.signature,"--at",request.accepted_at,NULL};
    unsigned char *receipt=NULL;size_t receipt_bytes=0;int rc=aw_native_result(argv,NULL,0,&receipt,&receipt_bytes,0);
    if(rc){
        free(receipt);
        aw_error(rc==1||rc==6||rc==127?503:rc==2?400:409,rc==5?"Required ProtocolContract role authority is not current.\n":"Signed supplier acceptance was refused; no new downstream effect was inferred.\n");
        return 1;
    }
    int replayed=0;
    if(!receipt||receipt_bytes>16384||!pde_effect_receipt(receipt,receipt_bytes,&request,supplier_id,&replayed)){
        free(receipt);aw_error(503,"The native enactment receipt could not be verified.\n");return 1;
    }
    free(receipt);
    bf_mp_writer_t w;bf_mp_writer_init(&w,1536);
    int ok=!bf_mp_pack_array(&w,2)&&!bf_mp_pack_uint(&w,1)&&!bf_mp_pack_map(&w,14)&&
      !bf_mp_pack_kv_str(&w,"state",replayed?"replayed":"enacted")&&!bf_mp_pack_kv_str(&w,"record_key",key)&&
      !bf_mp_pack_str_cstr(&w,"revision")&&!bf_mp_pack_uint(&w,(uint64_t)ready.order_revision)&&!bf_mp_pack_kv_str(&w,"order_digest",ready.order_digest)&&
      !bf_mp_pack_kv_str(&w,"quote_id",request.quote_id)&&!bf_mp_pack_kv_str(&w,"exchange",request.exchange)&&!bf_mp_pack_kv_str(&w,"supplier_id",supplier_id)&&
      !bf_mp_pack_str_cstr(&w,"supplier_identity_binding_verified")&&!bf_mp_pack_bool(&w,1)&&!bf_mp_pack_str_cstr(&w,"supplier_acceptance_verified")&&!bf_mp_pack_bool(&w,1)&&
      !bf_mp_pack_str_cstr(&w,"protocol_enacted")&&!bf_mp_pack_bool(&w,1)&&!bf_mp_pack_str_cstr(&w,"commitment_opened")&&!bf_mp_pack_bool(&w,1)&&
      !bf_mp_pack_str_cstr(&w,"replayed")&&!bf_mp_pack_bool(&w,replayed)&&!bf_mp_pack_kv_str(&w,"accepted_at",request.accepted_at)&&
      !bf_mp_pack_kv_str(&w,"cannot_infer","No purchase order, payable, payment presentation, settlement, fulfillment, shipment, receipt, satisfaction or ownership is established by this enactment.");
    if(ok&&w.len<=8192)aw_response(200,"application/msgpack",w.buf,w.len,0);else aw_error(503,"The enactment receipt could not be encoded.\n");
    bf_mp_writer_free(&w);
    return 1;
}
#endif
