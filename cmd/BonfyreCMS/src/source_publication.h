#ifndef BONFYRE_SOURCE_PUBLICATION_H
#define BONFYRE_SOURCE_PUBLICATION_H
#include "bf_msgpack.h"
/* One transaction over the existing SourceCollection and World owners.
 * A request coordinate is stable across interruptions; it carries no authority. */
#define SP_BYTES 16384
#define SP_NOTICE "This World is a human-authored publication over one selected foreign source collection. It does not admit source material, assign contributors, establish an institution, licence material, or authorize an effect."
typedef struct { char request[65],subject[257],source[257],title[961],lede[7201],character[32]; } SpInput;
static int sp_text(const unsigned char *p,size_t n,char *out,size_t cap) {
    if(!p||!n||n>=cap||memchr(p,0,n))return 0;
    for(size_t i=0;i<n;i++)if((p[i]<32&&p[i]!='\n'&&p[i]!='\r'&&p[i]!='\t')||p[i]==127)return 0;
    memcpy(out,p,n);out[n]=0;return 1;
}
static int sp_string(bf_mp_reader_t *r,char *out,size_t cap) {
    bf_mp_value_t v;return bf_mp_read_next(r,&v)==BF_MP_STR&&sp_text(v.v.raw.ptr,v.v.raw.len,out,cap);
}
static int sp_valid(const SpInput *p) {
    return strlen(p->request)==64&&strspn(p->request,"0123456789abcdef")==64&&
      *p->source&&strspn(p->source,"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._:-/")==strlen(p->source)&&
      *p->subject&&strspn(p->subject,"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._:@/-")==strlen(p->subject)&&
      strspn(p->title," \t\r\n")<strlen(p->title)&&strspn(p->lede," \t\r\n")<strlen(p->lede)&&
      (!strcmp(p->character,"zine")||!strcmp(p->character,"atlas")||!strcmp(p->character,"scrapbook")||!strcmp(p->character,"dossier"));
}
static int sp_decode(const void *body,size_t bytes,int writing,SpInput *p) {
    bf_mp_reader_t r;bf_mp_value_t v;memset(p,0,sizeof(*p));bf_mp_reader_init(&r,body,bytes);
    if(!bytes||bytes>SP_BYTES||bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=(writing?7u:2u))return 0;
    bf_mp_type_t t=bf_mp_read_next(&r,&v);if(!((t==BF_MP_UINT&&v.v.u==1)||(t==BF_MP_INT&&v.v.i==1)))return 0;
    if(!sp_string(&r,p->request,sizeof(p->request))||strlen(p->request)!=64||strspn(p->request,"0123456789abcdef")!=64)return 0;
    if(writing&&(!sp_string(&r,p->subject,sizeof(p->subject))||!sp_string(&r,p->source,sizeof(p->source))||
      !sp_string(&r,p->title,sizeof(p->title))||!sp_string(&r,p->lede,sizeof(p->lede))||!sp_string(&r,p->character,sizeof(p->character))||!sp_valid(p)))return 0;
    return r.pos==r.len;
}
static void sp_coordinates(const char *ns,const char *request,char *collection,char *world) {
    char seed[200],digest[65];snprintf(seed,sizeof(seed),"%s\n%s",ns,request);
    bf_sha256_hex((const uint8_t*)seed,strlen(seed),digest);
    snprintf(collection,96,"foreign-collection-%s",digest);snprintf(world,96,"foreign-world-%s",digest);
}
static int sp_existing(sqlite3 *db,const char *type,const char *field,const char *coordinate,const char *ns,long long *id) {
    char sql[200];sqlite3_stmt *s=NULL;int ok=0,step;*id=0;
    snprintf(sql,sizeof(sql),"SELECT id FROM %s WHERE namespace=?1 AND %s=?2 LIMIT 2",type,field);
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,ns,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,coordinate,-1,SQLITE_STATIC);
    step=sqlite3_step(s);if(step==SQLITE_ROW){*id=sqlite3_column_int64(s,0);step=sqlite3_step(s);}ok=step==SQLITE_DONE;
done:sqlite3_finalize(s);return ok;
}
static int sp_payload(sqlite3 *db,const SpInput *p,const char *collection,const char *world,char **a,char **b) {
    sqlite3_stmt *s=NULL;int ok=0;*a=*b=NULL;
    const char *sql="SELECT json_object('collection_id',?1,'subject_address',?3,'source_selector',json_object('source_refs',json_array(?4)),'material_kind',json_array('PublicationOccurrence'),'refresh_policy','manual','status','published'),"
      "json_object('world',?2,'title',?5,'character',?7,'lede',?6,'subject',?3,'sources','source_collection:'||?1,'grammars','discover','cannot_infer',?8,'status','published')";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)goto done;
    const char *values[]={collection,world,p->subject,p->source,p->title,p->lede,p->character,SP_NOTICE};
    for(int i=0;i<8;i++)sqlite3_bind_text(s,i+1,values[i],-1,SQLITE_STATIC);
    if(sqlite3_step(s)==SQLITE_ROW){*a=strdup((const char*)sqlite3_column_text(s,0));*b=strdup((const char*)sqlite3_column_text(s,1));ok=*a&&*b;}
done:sqlite3_finalize(s);return ok;
}
static int sp_matches(sqlite3 *db,const char *type,long long id,const char *wanted) {
    ContentType *ct=find_type(type);char *row=ct?entry_row_json(db,ct,id):NULL;sqlite3_stmt *s=NULL;int ok=0;
    /* The CMS row snapshot retains structured fields in their TEXT columns. */
    if(row&&sqlite3_prepare_v2(db,"SELECT NOT EXISTS(SELECT 1 FROM json_each(?1) e WHERE NOT EXISTS(SELECT 1 FROM json_each(?2) a WHERE a.key=e.key AND CASE WHEN e.type IN ('object','array') THEN CASE WHEN a.type='text' AND json_valid(a.value) THEN json(a.value)=json(e.value) ELSE 0 END ELSE a.type=e.type AND a.value IS e.value END))",-1,&s,NULL)==SQLITE_OK){
      sqlite3_bind_text(s,1,wanted,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,row,-1,SQLITE_STATIC);ok=sqlite3_step(s)==SQLITE_ROW&&sqlite3_column_int(s,0)==1;
    }
    sqlite3_finalize(s);free(row);return ok;
}
static int sp_load(sqlite3 *db,long long a,long long b,SpInput *p) {
    sqlite3_stmt *s=NULL;int ok=0;
    const char *sql="SELECT c.subject_address,CASE WHEN json_valid(c.source_selector) THEN json_extract(c.source_selector,'$.source_refs[0]') END,w.title,w.lede,w.character FROM source_collection c,world w WHERE c.id=?1 AND w.id=?2 AND COALESCE(w.template,'')='' AND COALESCE(w.filter,'')=''";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)goto done;sqlite3_bind_int64(s,1,a);sqlite3_bind_int64(s,2,b);
    if(sqlite3_step(s)!=SQLITE_ROW)goto done;
    char *fields[]={p->subject,p->source,p->title,p->lede,p->character};size_t caps[]={sizeof(p->subject),sizeof(p->source),sizeof(p->title),sizeof(p->lede),sizeof(p->character)};
    for(int i=0;i<5;i++)if(sqlite3_column_type(s,i)!=SQLITE_TEXT||!sp_text(sqlite3_column_text(s,i),(size_t)sqlite3_column_bytes(s,i),fields[i],caps[i]))goto done;
    ok=sp_valid(p);
done:sqlite3_finalize(s);return ok;
}
static int sp_reply(const char *state,const SpInput *p,const char *collection,const char *world,long long a,long long b,int material,const char *delta) {
    bf_mp_writer_t w;bf_mp_writer_init(&w,1024);int ok=!bf_mp_pack_array(&w,9)&&!bf_mp_pack_uint(&w,1)&&!bf_mp_pack_str_cstr(&w,state)&&!bf_mp_pack_str_cstr(&w,p->request)&&!bf_mp_pack_str_cstr(&w,collection)&&!bf_mp_pack_str_cstr(&w,world)&&!bf_mp_pack_uint(&w,(uint64_t)a)&&!bf_mp_pack_uint(&w,(uint64_t)b);
    if(ok&&material){ok=!bf_mp_pack_array(&w,7)&&!bf_mp_pack_uint(&w,1);const char *v[]={p->request,p->subject,p->source,p->title,p->lede,p->character};for(int i=0;i<6&&ok;i++)ok=!bf_mp_pack_str_cstr(&w,v[i]);}
    else if(ok)ok=!bf_mp_pack_nil(&w);
    if(ok)ok=!bf_mp_pack_str_cstr(&w,delta);
    if(ok)ok=fwrite(w.buf,1,w.len,stdout)==w.len;bf_mp_writer_free(&w);return ok;
}
static int entry_source_publication(sqlite3 *db,const char *ns,const char *operation,const void *body,size_t bytes) {
    SpInput p={0},current={0};char collection[96]="",world[96]="";char *ca=NULL,*wa=NULL;long long a=0,b=0;int begun=0,committed=0,material=0,rc=3;
    const char *state="invalid",*delta="NOT_REQUESTED";int writing=operation&&!strcmp(operation,"publish");
    if(!ns||!*ns||strlen(ns)>127||strspn(ns,"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-")!=strlen(ns)||!operation||(!writing&&strcmp(operation,"read"))||!sp_decode(body,bytes,writing,&p))goto done;
    sp_coordinates(ns,p.request,collection,world);current=p;
    if(sqlite3_exec(db,writing?"BEGIN IMMEDIATE":"BEGIN",NULL,NULL,NULL)!=SQLITE_OK){state="unavailable";rc=6;goto done;}begun=1;
    if(!sp_existing(db,"source_collection","collection_id",collection,ns,&a)||!sp_existing(db,"world","world",world,ns,&b)){state="conflict";rc=4;goto done;}
    if(a||b){
      if(!a||!b||!sp_load(db,a,b,&current)||!sp_payload(db,writing?&p:&current,collection,world,&ca,&wa)||!sp_matches(db,"source_collection",a,ca)||!sp_matches(db,"world",b,wa)){state="conflict";rc=4;goto done;}
      state="committed";material=1;delta="UNCHANGED";rc=0;
    }else if(!writing){state="absent";rc=0;}
    else{
      sqlite3_stmt *source=NULL;int available=0;
      if(sqlite3_prepare_v2(db,"SELECT count(*) FROM internet_source WHERE source=?1 AND namespace IN ('root',?2) AND status='published'",-1,&source,NULL)==SQLITE_OK){sqlite3_bind_text(source,1,p.source,-1,SQLITE_STATIC);sqlite3_bind_text(source,2,ns,-1,SQLITE_STATIC);available=sqlite3_step(source)==SQLITE_ROW&&sqlite3_column_int(source,0)==1;}sqlite3_finalize(source);
      if(!available){state="source_unavailable";rc=4;goto done;}
      if(!sp_payload(db,&p,collection,world,&ca,&wa)){state="unavailable";rc=6;goto done;}
      defer_hooks=1;deferred_write_error=0;
      if(entry_create(db,"source_collection",ns,ca,&a)||entry_create(db,"world",ns,wa,&b)||deferred_write_error||!sp_matches(db,"source_collection",a,ca)||!sp_matches(db,"world",b,wa)){state="refused";rc=4;goto done;}
      if(sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK){state="unavailable";rc=6;goto done;}begun=0;committed=1;material=1;state="committed";current=p;
      finish_deferred_hooks(db,1);int first=emit_published_entry_delta("source_collection",ca,a),second=emit_published_entry_delta("world",wa,b);
      delta=!cms_fact_deltas_enabled()?"DISABLED":first&&second?"DELIVERED":"DELIVERY_FAILED";rc=first&&second?0:2;
    }
    if(begun){if(sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK){state="unavailable";rc=6;material=0;goto done;}begun=0;}
done:
    if(begun)sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);
    if(!committed)finish_deferred_hooks(db,0);
    if(rc&&rc!=2){a=b=0;material=0;}
    if(!sp_reply(state,material?&current:&p,collection,world,a,b,material,delta))rc=6;
    free(ca);free(wa);return rc;
}
#endif
