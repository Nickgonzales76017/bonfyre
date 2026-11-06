#ifndef BONFYRE_RELATIONSHIP_OBSERVATION_H
#define BONFYRE_RELATIONSHIP_OBSERVATION_H
/* Called only after current native workspace and account admission. Source
 * verification precedes the CMS compare-and-swap; these owners do not share a
 * transaction. The retained fingerprint describes the exact reviewed bytes. */
typedef struct { char item[160],snapshot[65],actor[160],release[65]; } RoForm;
static int ro_form(const char *input,int post,RoForm *out) {
    unsigned seen=0;memset(out,0,sizeof(*out));
    if(!input||!*input||strlen(input)>2048)return 0;
    for(const char *p=input;*p;){
        const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));
        char key[24],*value=NULL;size_t cap=0;unsigned bit=0;
        if(!eq||!cwl_decode(p,(size_t)(eq-p),key,sizeof(key)))return 0;
        if(!strcmp(key,"item")){value=out->item;cap=sizeof(out->item);bit=1;}
        else if(post&&!strcmp(key,"snapshot")){value=out->snapshot;cap=sizeof(out->snapshot);bit=2;}
        else if(post&&!strcmp(key,"actor")){value=out->actor;cap=sizeof(out->actor);bit=4;}
        else if(post&&!strcmp(key,"release")){value=out->release;cap=sizeof(out->release);bit=8;}
        if(!value||(seen&bit)||!cwl_decode(eq+1,(size_t)(end-eq-1),value,cap)||!*value)return 0;
        seen|=bit;if(*end&&!end[1])return 0;p=*end?end+1:end;
    }
    char source[128],key[25];return seen==(post?15u:1u)&&ac_feed_ref(out->item,source,key)&&(!post||(aw_hex(out->snapshot,64)&&aw_hex(out->release,64)));
}
static int ro_declared(const char *pack,const char *family,const char *source_family,const char *kind,char prefix[128]) {
    sqlite3 *db=cms_open_ro();sqlite3_stmt *s=NULL;int ok=0;
    if(!db)return 0;
    if(sqlite3_prepare_v2(db,"SELECT json_extract(produces,'$.prefix') FROM capability_program d WHERE namespace='root' AND application_kind=?1 AND action='feed-intake' AND status='published' AND json_extract(capability_requirements,'$.pack')=?2 AND json_extract(produces,'$.mode')='relationship' AND json_extract(transition_conditions,'$.source_fingerprint_required')=1 AND EXISTS(SELECT 1 FROM json_each(d.consumes) WHERE value=?3 OR value=?4) ORDER BY id",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,family,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,pack,-1,SQLITE_STATIC);sqlite3_bind_text(s,3,source_family,-1,SQLITE_STATIC);sqlite3_bind_text(s,4,kind,-1,SQLITE_STATIC);
        ok=sqlite3_step(s)==SQLITE_ROW&&bfpg_text(s,0,prefix,128)&&sqlite3_step(s)==SQLITE_DONE;
    }
    sqlite3_finalize(s);sqlite3_close(db);return ok&&ac_collection_declared(pack,family,prefix);
}
/* A job's publisher credit is not the employer or an actor identity.
 * Jobs remain source-item observations even when the feed sets authorName. */
static int ro_actor_credit(const PageItem *item,const char *family){return item->by[0]&&strcmp(family,"jobs")!=0;}
static int ro_source(const char *ref,PageItem *item,char family[64],char kind[64],char snapshot[65],char identity[65]) {
    char source[128],key[25],twin[256],material[65];sqlite3 *db=NULL;bf_mp_writer_t w;bf_mp_writer_init(&w,1024);
    int ok=ac_feed_ref(ref,source,key)&&ac_feed_item(ref,item,family,kind)&&(item->by[0]||!strcmp(family,"jobs"))&&cwl_clean(item->by,strlen(item->by))&&(db=cms_open_ro())&&pr_source(db,source,twin,sizeof(twin))&&pr_item_fingerprint(item,source,twin,material);
    if(ok){ok=!bf_mp_pack_array(&w,4)&&!bf_mp_pack_str_cstr(&w,material)&&!bf_mp_pack_str_cstr(&w,item->by)&&!bf_mp_pack_str_cstr(&w,family[0]?family:"")&&!bf_mp_pack_str_cstr(&w,kind);if(ok)bf_sha256_hex(w.buf,w.len,snapshot);}
    bf_mp_writer_free(&w);bf_mp_writer_init(&w,1024);
    if(ok){if(ro_actor_credit(item,family)){ok=!bf_mp_pack_array(&w,2)&&!bf_mp_pack_str_cstr(&w,ref)&&!bf_mp_pack_str_cstr(&w,item->by);if(ok)bf_sha256_hex(w.buf,w.len,identity);}else bf_sha256_hex((const uint8_t*)ref,strlen(ref),identity);}
    bf_mp_writer_free(&w);if(db)sqlite3_close(db);return ok;
}
static int ro_record(bf_mp_writer_t *w,const PageItem *item,const char *ref,const char *family,const char *kind,const char *snapshot,const char *identity) {
    char path[192],dated[32];snprintf(path,sizeof(path),"/o/%s",ref);struct tm tm;time_t now=time(NULL);if(!gmtime_r(&now,&tm)||!strftime(dated,sizeof(dated),"%Y-%m-%dT%H:%M:%SZ",&tm))return 0;
#define RO(call) do { if((call))return 0; } while(0)
    RO(bf_mp_pack_array(w,3));RO(bf_mp_pack_int(w,2));RO(bf_mp_pack_map(w,4));
    RO(bf_mp_pack_kv_str(w,"name",ro_actor_credit(item,family)?item->by:item->title));RO(bf_mp_pack_kv_str(w,"saved_at",dated));RO(bf_mp_pack_kv_str(w,"save_id",identity));RO(bf_mp_pack_str_cstr(w,"state"));RO(bf_mp_pack_map(w,7));
    RO(bf_mp_pack_str_cstr(w,"reqs"));RO(bf_mp_pack_array(w,0));RO(bf_mp_pack_str_cstr(w,"options"));RO(bf_mp_pack_array(w,0));RO(bf_mp_pack_str_cstr(w,"weightOverrides"));RO(bf_mp_pack_map(w,0));RO(bf_mp_pack_str_cstr(w,"poc"));RO(bf_mp_pack_array(w,0));RO(bf_mp_pack_str_cstr(w,"ops"));RO(bf_mp_pack_map(w,0));RO(bf_mp_pack_str_cstr(w,"own"));RO(bf_mp_pack_map(w,0));
    RO(bf_mp_pack_str_cstr(w,"relationship"));RO(bf_mp_pack_map(w,8));RO(bf_mp_pack_kv_str(w,"schema","bonfyre.relationship.v1"));RO(bf_mp_pack_kv_str(w,"id",identity));RO(bf_mp_pack_kv_str(w,"state","observed"));RO(bf_mp_pack_kv_str(w,"label",ro_actor_credit(item,family)?item->by:item->title));RO(bf_mp_pack_kv_str(w,"createdAt",dated));
    RO(bf_mp_pack_str_cstr(w,"actor"));RO(bf_mp_pack_map(w,2));RO(bf_mp_pack_kv_str(w,"state","unresolved"));RO(bf_mp_pack_str_cstr(w,"subject"));RO(bf_mp_pack_nil(w));
    RO(bf_mp_pack_str_cstr(w,"source"));RO(bf_mp_pack_map(w,11));RO(bf_mp_pack_kv_str(w,"itemRef",ref));RO(bf_mp_pack_kv_str(w,"publicPath",path));RO(bf_mp_pack_kv_str(w,"source",item->source));RO(bf_mp_pack_kv_str(w,"family",family));RO(bf_mp_pack_kv_str(w,"kind",kind));RO(bf_mp_pack_kv_str(w,"title",item->title));RO(bf_mp_pack_kv_str(w,"summary",item->body));RO(bf_mp_pack_kv_str(w,"canonicalHref",item->href));RO(bf_mp_pack_kv_str(w,"authorName",item->by));RO(bf_mp_pack_kv_str(w,"snapshot",snapshot));RO(bf_mp_pack_kv_str(w,"evidenceKind",ro_actor_credit(item,family)?"retained-source-credit":"retained-source-item"));
    RO(bf_mp_pack_str_cstr(w,"opportunity"));RO(bf_mp_pack_map(w,2));RO(bf_mp_pack_kv_str(w,"kind",!strcmp(family,"jobs")?"job-opening":"observed-mention"));RO(bf_mp_pack_kv_str(w,"stage","observed"));RO(bf_mp_pack_array(w,0));
#undef RO
    return 1;
}
static int ro_owner(const char *operation,const char *binding,const char *subject,const char *scope,const char *actor,const char *key,const void *input,size_t bytes,unsigned char **answer,size_t *length) {
    const char *bin=page_cms_env("BONFYRE_CMS_BINARY","/usr/local/bin/bonfyre-cms"),*db=page_cms_env("BONFYRE_CMS_DB","/var/lib/bonfyre/cms/bonfyre_cms.db");
    char *const args[]={(char*)bin,"pack-workspace","--operation",(char*)operation,"--binding",(char*)binding,"--subject",(char*)subject,"--scope",(char*)scope,"--actor",(char*)actor,"--prefix",(char*)key,"--db",(char*)db,NULL};
    return aw_native(args,input,bytes,answer,length);
}
static int ro_receipt(const unsigned char *answer,size_t length,const char *key,const bf_mp_writer_t *record) {
    bf_mp_reader_t r;bf_mp_reader_init(&r,answer,length);bf_mp_value_t v;long long version,epoch,revision;char cursor[512],kept_key[512];
    if(!ac_array(&r,5)||!ac_uint(&r,&version)||version!=1||!ac_uint(&r,&epoch)||
       bf_mp_read_next(&r,&v)!=BF_MP_BOOL||v.v.b||!ac_text(&r,cursor,sizeof(cursor))||*cursor||
       !ac_array(&r,1)||!ac_array(&r,3)||!ac_text(&r,kept_key,sizeof(kept_key))||strcmp(kept_key,key)||
       !ac_uint(&r,&revision)||revision!=1||bf_mp_read_next(&r,&v)!=BF_MP_BIN||
       v.v.raw.len!=record->len||memcmp(v.v.raw.ptr,record->buf,record->len)||r.pos!=r.len)return 0;
    return 1;
}
static void ro_back(const char *context,const PageItem *item,const char *family) {
    fputs("/relationships?context=",stdout);url_query_value(context);if(ro_actor_credit(item,family)){fputs("&amp;who=",stdout);url_query_value(item->by);fputs("&amp;source=",stdout);url_query_value(item->source);}
}
static int ro_serve(const char *method,const char *query,const char *body,const char *raw,const char *host,const char *binding,const char *subject,const char *scope,const char *actor,const char *context,const char *pack,const char *family,const char *release,const char *entry) {
    int post=!strcmp(method,"POST");RoForm form;char prefix[128],source_family[64],kind[64],snapshot[65],identity[65],key[256];PageItem item;
    if(strcmp(method,"GET")&&!post){aw_error(405,"Use GET to review an observation or POST to save it.\n");return 1;}
    if(post){
        char headers[CWL_HEADER_MAX+1],origin[512],expected[512],type[128],length[32];
        if(query&&*query){aw_error(400,"Submit the reviewed observation in the form body.\n");return 1;}
        if(!cwl_headers(raw,headers)||!cwl_origin(host,expected,sizeof(expected))||!aw_header(headers,"Origin",origin,sizeof(origin))||strcmp(origin,expected)){aw_error(403,"Save this observation from its current workspace.\n");return 1;}
        if(!aw_header(headers,"Content-Type",type,sizeof(type))||cwl_media(type)!=2||!aw_header(headers,"Content-Length",length,sizeof(length))||!*length||strlen(length)>4||strspn(length,"0123456789")!=strlen(length)||!body||strlen(body)>2048||strtoul(length,NULL,10)!=strlen(body)){aw_error(400,"Submit one complete observation form.\n");return 1;}
    }
    if(!ro_form(post?body:query,post,&form)){aw_error(400,"Choose one exact retained source observation.\n");return 1;}
    if(post&&(strcmp(form.actor,actor)||strcmp(form.release,release))){aw_error(409,"The account or editor changed. Review this observation again.\n");return 1;}
    if(!ac_verified(binding,subject,scope,pack,release,entry,actor)||!workspace_package_available(release,entry)){aw_error(409,"Reopen the current relationship workspace.\n");return 1;}
    if(!ro_source(form.item,&item,source_family,kind,snapshot,identity)){aw_error(404,"The credited source observation is unavailable.\n");return 1;}
    if(!ro_declared(pack,family,source_family,kind,prefix)){aw_error(409,"This workspace has not admitted reviewed observations from this source family.\n");return 1;}
    if(post&&strcmp(form.snapshot,snapshot)){aw_error(409,"The source or its credit changed. Review the current observation before saving.\n");return 1;}
    if(snprintf(key,sizeof(key),"%s%s:%s",prefix,ro_actor_credit(&item,source_family)?"actor":"feed",identity)>=(int)sizeof(key)){aw_error(409,"The observation collection is unavailable.\n");return 1;}
    unsigned char *answer=NULL;size_t length=0;AcPage page;int exists=0;char retained_snapshot[65]="";long long retained_revision=0;
    if(ro_owner("get",binding,subject,scope,actor,key,NULL,0,&answer,&length)||!ac_page(answer,length,&page)||page.more||page.count>1||(page.count&&strcmp(page.rows[0].key,key))){free(answer);aw_error(503,"The relationship record could not be read. No observation was saved.\n");return 1;}
    if(page.count){retained_revision=page.rows[0].revision;ac_value(&page.rows[0],"state.relationship.source.snapshot",retained_snapshot,sizeof(retained_snapshot));char saved_ref[160],saved_by[512],schema[80];exists=page.rows[0].body&&ac_value(&page.rows[0],"state.relationship.schema",schema,sizeof(schema))&&!strcmp(schema,"bonfyre.relationship.v1")&&ac_value(&page.rows[0],"state.relationship.source.itemRef",saved_ref,sizeof(saved_ref))&&!strcmp(saved_ref,form.item)&&ac_value(&page.rows[0],"state.relationship.source.authorName",saved_by,sizeof(saved_by))&&!strcmp(saved_by,item.by);if(!exists){free(answer);aw_error(409,"This observation identity already has a different or removed record. Open the collection to review it.\n");return 1;}}
    free(answer);answer=NULL;
    if(post&&!exists){
        bf_mp_writer_t record,write;bf_mp_writer_init(&record,4096);bf_mp_writer_init(&write,4096);char request[65];int okay=ro_record(&record,&item,form.item,source_family,kind,snapshot,identity);if(okay)bf_sha256_hex(record.buf,record.len,request);
        okay=okay&&!bf_mp_pack_array(&write,3)&&!bf_mp_pack_int(&write,1)&&!bf_mp_pack_str_cstr(&write,request)&&!bf_mp_pack_array(&write,1)&&!bf_mp_pack_array(&write,4)&&!bf_mp_pack_str_cstr(&write,key)&&!bf_mp_pack_int(&write,0)&&!bf_mp_pack_int(&write,0)&&!bf_mp_pack_bin(&write,record.buf,(uint32_t)record.len);
        int rc=okay?ro_owner("write",binding,subject,scope,actor,key,write.buf,write.len,&answer,&length):6;
        okay=!rc&&ro_receipt(answer,length,key,&record);
        free(answer);bf_mp_writer_free(&write);bf_mp_writer_free(&record);
        if(!okay){aw_error(rc==4?409:503,"The save was not confirmed. Reopen this observation to check its retained record before retrying.\n");return 1;}exists=1;retained_revision=1;snprintf(retained_snapshot,sizeof(retained_snapshot),"%s",snapshot);
    }
    psetup_start(200,exists?"Saved relationship observation":"Review relationship observation");fputs("<h1>",stdout);fputs(exists?"Saved relationship observation":"Review relationship observation",stdout);fputs("</h1><p>The excerpt below is the current retained source item.</p><p>Source credit: <strong>",stdout);html(item.by[0]?item.by:"No credited name");fputs("</strong> · <code>",stdout);html(item.source);fputs("</code></p><h2>",stdout);html(item.title);fputs("</h2><p>",stdout);html(item.body);fputs("</p><p>Save this source item as an unresolved observation in this workspace. It does not identify a person or establish a business relationship.</p>",stdout);
    if(exists){fputs("<p data-observation-saved>This observation already has a retained record. Its saved evidence and later edits are unchanged.</p><p>Saved record version ",stdout);printf("%lld",retained_revision);fputs(" · evidence fingerprint <code>",stdout);html(retained_snapshot[0]?retained_snapshot:"not recorded by the earlier writer");fputs("</code></p><p>Current source fingerprint <code>",stdout);html(snapshot);fputs("</code></p><p><a data-observation-open href=\"",stdout);ro_back(context,&item,source_family);fputs("\">Open this source's relationship observations</a></p>",stdout);}else{
        fputs("<form method=\"post\" data-observation-form action=\"/apps/",stdout);html(binding);fputs("/_relationship-observation\">",stdout);psetup_hidden("item",form.item);psetup_hidden("snapshot",snapshot);psetup_hidden("actor",actor);psetup_hidden("release",release);fputs("<button type=\"submit\">Save unresolved observation</button></form>",stdout);
    }
    fputs("<p><a href=\"",stdout);ro_back(context,&item,source_family);fputs("\">Back to relationship workspace</a></p>",stdout);psetup_end();return 1;
}
#endif
