#ifndef BONFYRE_CMS_WORKSPACE_RECORDS_H
#define BONFYRE_CMS_WORKSPACE_RECORDS_H
static int cw_lookup(sqlite3 *db,const char *id,const char *subject,const char *scope,CwBinding *b) {
    sqlite3_stmt *s=NULL;CmsPackInstallation i={0};BfPackResolution plan={0};int ok=0,step;
    memset(b,0,sizeof(*b));
    if(!id || strlen(id)!=43 || strncmp(id,"ws-",3) || strspn(id+3,"abcdef0123456789")!=40)return 0;
    if(sqlite3_prepare_v2(db,"SELECT i.namespace,i.installed_for,i.pack,w.family,w.release,w.entry,w.installation_id,a.continuity_key FROM _application_workspace w JOIN application_binding a ON a.id=w.cms_id JOIN pack_installation i ON i.id=w.installation_id WHERE w.binding_id=?1 AND a.binding_id=?1 AND a.namespace=i.namespace AND a.status='active' AND a.site_id=i.installed_for AND a.slot_id=i.pack AND a.application_kind=w.family AND a.source_collection=w.family AND a.action_aperture='/apps/'||w.binding_id||'/_records'",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,id,-1,SQLITE_STATIC);
    if(sqlite3_step(s)!=SQLITE_ROW || !bfpg_text(s,0,b->ns,sizeof(b->ns)) || !bfpg_text(s,1,b->subject,sizeof(b->subject)) || !bfpg_text(s,2,b->pack,sizeof(b->pack)) ||
        !bfpg_text(s,3,b->family,sizeof(b->family)) || !bfpg_text(s,4,b->release,sizeof(b->release)) || !bfpg_text(s,5,b->entry,sizeof(b->entry)) || !bfpg_text(s,7,b->record_ns,sizeof(b->record_ns)))goto done;
    b->installation=sqlite3_column_int64(s,6);if(sqlite3_step(s)!=SQLITE_DONE)goto done;sqlite3_finalize(s);s=NULL;
    if(strcmp(subject,b->subject) || !cpack_read(db,b->ns,subject,scope,b->pack,&i) || i.id!=b->installation || strcmp(i.status,"active") ||
        !bfpg_resolve(db,b->pack,i.version,&i.lock,&plan) || strcmp(plan.digest,i.digest))goto done;
    char key[640],hash[65],expected[80];int declared=0;
    if(!cw_token(b->family,sizeof(b->family)) || !cw_entry(b->entry) || strlen(b->release)!=64 || strspn(b->release,"abcdef0123456789")!=64)goto done;
    snprintf(key,sizeof(key),"%s\n%s\n%s\n%s\n%s",subject,scope,b->pack,b->family,b->entry);bf_sha256_hex((uint8_t *)key,strlen(key),hash);snprintf(expected,sizeof(expected),"ws-%.40s",hash);
    if(strcmp(expected,id))goto done;
    snprintf(key,sizeof(key),"%s\n%s\n%s",subject,scope,b->family);bf_sha256_hex((uint8_t *)key,strlen(key),hash);snprintf(expected,sizeof(expected),"workspace-%.40s",hash);
    if(strcmp(expected,b->record_ns))goto done;
    for(size_t n=0;n<plan.count;n++){
        if(sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM pack_demand WHERE namespace='root' AND pack=?1 AND demand_kind='workspace' AND needs=?2 AND preferred_sources=?3 AND fields=?4 AND status='published'",-1,&s,NULL)!=SQLITE_OK)goto done;
        sqlite3_bind_text(s,1,plan.nodes[n].pack,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,b->family,-1,SQLITE_STATIC);sqlite3_bind_text(s,3,b->release,-1,SQLITE_STATIC);sqlite3_bind_text(s,4,b->entry,-1,SQLITE_STATIC);
        if(sqlite3_step(s)!=SQLITE_ROW)goto done;declared+=sqlite3_column_int(s,0);sqlite3_finalize(s);s=NULL;
    }
    if(declared!=1)goto done;
    snprintf(b->id,sizeof(b->id),"%s",id);snprintf(b->scope,sizeof(b->scope),"%s",scope);
    for(size_t n=0;n<plan.count;n++){
        if(sqlite3_prepare_v2(db,"SELECT fields FROM pack_demand WHERE namespace='root' AND pack=?1 AND demand_kind='workspace-record' AND needs=?2 AND status='published' ORDER BY id",-1,&s,NULL)!=SQLITE_OK)goto done;
        sqlite3_bind_text(s,1,plan.nodes[n].pack,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,b->family,-1,SQLITE_STATIC);
        while((step=sqlite3_step(s))==SQLITE_ROW){
            if(b->prefix_count>=32 || !bfpg_text(s,0,b->prefixes[b->prefix_count],128))goto done;
            const char *p=b->prefixes[b->prefix_count];if(!*p || strcspn(p,"\r\n\t")!=strlen(p))goto done;b->prefix_count++;
        }
        sqlite3_finalize(s);s=NULL;if(step!=SQLITE_DONE)goto done;
    }
    ok=b->prefix_count>0;
 done:sqlite3_finalize(s);return ok;
}
static int cw_key(const CwBinding *b,const char *key) {
    size_t len=key?strlen(key):0;if(!len || len>=512)return 0;
    for(size_t n=0;n<len;n++)if((unsigned char)key[n]<32 || (unsigned char)key[n]==127)return 0;
    for(size_t n=0;n<b->prefix_count;n++)if(!strncmp(key,b->prefixes[n],strlen(b->prefixes[n])))return 1;
    return 0;
}
static int cw_utf8(const uint8_t *s,size_t size) {
    for(size_t i=0;i<size;){
        uint32_t c=s[i++];unsigned n;uint32_t min;
        if(c<128)continue;
        if(c>=0xc2 && c<=0xdf){n=1;min=0x80;c&=31;}else if(c>=0xe0 && c<=0xef){n=2;min=0x800;c&=15;}else if(c>=0xf0 && c<=0xf4){n=3;min=0x10000;c&=7;}else return 0;
        if(i+n>size)return 0;while(n--){uint8_t b=s[i++];if((b&0xc0)!=0x80)return 0;c=(c<<6)|(b&63);}
        if(c<min || c>0x10ffff || (c>=0xd800 && c<=0xdfff))return 0;
    }
    return 1;
}
static int cw_text(bf_mp_reader_t *r,char *s,size_t cap) {
    bf_mp_value_t v;if(bf_mp_read_next(r,&v)!=BF_MP_STR || v.v.raw.len>=cap || memchr(v.v.raw.ptr,0,v.v.raw.len) || !cw_utf8(v.v.raw.ptr,v.v.raw.len))return 0;
    memcpy(s,v.v.raw.ptr,v.v.raw.len);s[v.v.raw.len]=0;return 1;
}
static int cw_uint(bf_mp_reader_t *r,long long *out) {
    bf_mp_value_t v;bf_mp_type_t t=bf_mp_read_next(r,&v);
    if(t==BF_MP_UINT && v.v.u<INT_MAX){*out=(long long)v.v.u;return 1;}
    if(t==BF_MP_INT && v.v.i>=0 && v.v.i<INT_MAX){*out=v.v.i;return 1;}return 0;
}
static int cw_array(bf_mp_reader_t *r,uint32_t *count) {
    bf_mp_value_t v;if(bf_mp_read_next(r,&v)!=BF_MP_ARRAY)return 0;*count=v.v.count;return 1;
}
/* No executable types, extension objects, duplicate map keys, nonfinite
 * numbers, unbounded recursion or trailing bytes enter a candidate record. */
static int cw_value(bf_mp_reader_t *r,unsigned depth,size_t *nodes) {
    if(depth>32 || ++*nodes>50000)return 0;
    bf_mp_value_t v;bf_mp_type_t t=bf_mp_read_next(r,&v);
    if(t==BF_MP_MAP){
        if(v.v.count>4096)return 0;
        uint32_t count=v.v.count;bf_mp_value_t *keys=calloc(count?count:1,sizeof(*keys));if(!keys)return 0;
        int ok=1;
        for(uint32_t n=0;n<count;n++){
            if(bf_mp_read_next(r,&keys[n])!=BF_MP_STR || keys[n].v.raw.len>512 || !cw_utf8(keys[n].v.raw.ptr,keys[n].v.raw.len)){ok=0;break;}
            for(uint32_t k=0;k<n;k++)if(keys[k].v.raw.len==keys[n].v.raw.len && !memcmp(keys[k].v.raw.ptr,keys[n].v.raw.ptr,keys[n].v.raw.len)){ok=0;break;}
            if(!ok || !cw_value(r,depth+1,nodes)){ok=0;break;}
        }
        free(keys);return ok;
    }
    if(t==BF_MP_ARRAY){if(v.v.count>20000)return 0;for(uint32_t n=0;n<v.v.count;n++)if(!cw_value(r,depth+1,nodes))return 0;return 1;}
    if(t==BF_MP_FLOAT)return isfinite(v.v.f);if(t==BF_MP_DOUBLE)return isfinite(v.v.d);
    if(t==BF_MP_INT)return v.v.i>=-9007199254740991LL && v.v.i<=9007199254740991LL;
    if(t==BF_MP_UINT)return v.v.u<=9007199254740991ULL;
    if(t==BF_MP_STR)return cw_utf8(v.v.raw.ptr,v.v.raw.len);
    return t==BF_MP_NIL || t==BF_MP_BOOL || t==BF_MP_BIN;
}
#include "workspace_blobs.h"
static long long cw_epoch(sqlite3 *db,const char *ns) {
    sqlite3_stmt *s=NULL;long long out=-1;
    if(sqlite3_prepare_v2(db,"SELECT revision FROM _workspace_epoch WHERE scope_key=?1",-1,&s,NULL)==SQLITE_OK){sqlite3_bind_text(s,1,ns,-1,SQLITE_STATIC);int rc=sqlite3_step(s);if(rc==SQLITE_ROW)out=sqlite3_column_int64(s,0);else if(rc==SQLITE_DONE)out=0;}
    sqlite3_finalize(s);return out;
}
static int cw_row(bf_mp_writer_t *w,sqlite3_stmt *s) {
    const char *key=(const char *)sqlite3_column_text(s,0);long long revision=sqlite3_column_int64(s,1);int deleted=sqlite3_column_int(s,3);
    const void *body=sqlite3_column_blob(s,2);int bytes=sqlite3_column_bytes(s,2);
    if(!key || revision<1 || revision>=INT_MAX || (!deleted && (!body || bytes<1 || bytes>(int)CW_MAX_VALUE)))return 0;
    if(!deleted){char hash[65];const char *stored=(const char *)sqlite3_column_text(s,4);bf_sha256_hex(body,(size_t)bytes,hash);if(!stored || strcmp(stored,hash))return 0;}
    return !bf_mp_pack_array(w,3) && !bf_mp_pack_str_cstr(w,key) && !bf_mp_pack_uint(w,(uint64_t)revision) && !(deleted?bf_mp_pack_nil(w):bf_mp_pack_bin(w,body,(uint32_t)bytes));
}
static int cw_emit(bf_mp_writer_t *rows,uint32_t count,long long epoch,int more,const char *cursor) {
    bf_mp_writer_t w;bf_mp_writer_init(&w,128);
    int ok=!bf_mp_pack_array(&w,5) && !bf_mp_pack_uint(&w,1) && !bf_mp_pack_uint(&w,(uint64_t)epoch) && !bf_mp_pack_bool(&w,more) && !bf_mp_pack_str_cstr(&w,cursor) && !bf_mp_pack_array(&w,count);
    if(ok)ok=fwrite(w.buf,1,w.len,stdout)==w.len && fwrite(rows->buf,1,rows->len,stdout)==rows->len;
    bf_mp_writer_free(&w);return ok;
}
static int cw_read(sqlite3 *db,const CwBinding *b,const char *operation,const char *prefix,const char *after,long long expected_epoch) {
    long long epoch=cw_epoch(db,b->record_ns);if(epoch<0)return 6;if(expected_epoch>=0 && epoch!=expected_epoch)return 4;
    if(!prefix || strlen(prefix)>=512 || !after || strlen(after)>=512)return 3;
    sqlite3_stmt *s=NULL;bf_mp_writer_t rows;bf_mp_writer_init(&rows,4096);uint32_t count=0;int rc=6,step,more=0;char cursor[512]="";
    const char *sql=!strcmp(operation,"get")?
        "SELECT r.record_key,r.revision,b.body,r.deleted,r.body_digest FROM _workspace_record r LEFT JOIN _workspace_body b ON b.scope_key=r.scope_key AND b.digest=r.body_digest WHERE r.scope_key=?1 AND r.record_key=?2":
        "SELECT r.record_key,r.revision,b.body,r.deleted,r.body_digest FROM _workspace_record r LEFT JOIN _workspace_body b ON b.scope_key=r.scope_key AND b.digest=r.body_digest WHERE r.scope_key=?1 AND r.record_key>=?2 AND substr(r.record_key,1,length(?2))=?2 AND r.record_key>?3 AND r.deleted=0 ORDER BY r.record_key";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,b->record_ns,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,prefix,-1,SQLITE_STATIC);if(strcmp(operation,"get"))sqlite3_bind_text(s,3,after,-1,SQLITE_STATIC);
    while((step=sqlite3_step(s))==SQLITE_ROW){
        const char *key=(const char *)sqlite3_column_text(s,0);if(!cw_key(b,key))continue;
        if(count>=CW_PAGE || (count && rows.len+(size_t)sqlite3_column_bytes(s,2)+strlen((const char *)sqlite3_column_text(s,0))+1024>CW_MAX_PACKET)){more=1;break;}
        if(!cw_row(&rows,s))goto done;snprintf(cursor,sizeof(cursor),"%s",key);count++;
    }
    if(step!=SQLITE_DONE && !more)goto done;
    rc=cw_emit(&rows,count,epoch,more,cursor)?0:6;
 done:sqlite3_finalize(s);bf_mp_writer_free(&rows);return rc;
}
static int cw_replay(sqlite3 *db,const CwBinding *b,const char *request) {
    sqlite3_stmt *s=NULL;bf_mp_writer_t rows;bf_mp_writer_init(&rows,4096);uint32_t count=0;int rc=6,step;
    if(sqlite3_prepare_v2(db,"SELECT r.record_key,r.revision,b.body,r.deleted,r.body_digest FROM _workspace_operation_row r LEFT JOIN _workspace_body b ON b.scope_key=r.scope_key AND b.digest=r.body_digest WHERE r.scope_key=?1 AND r.request_id=?2 ORDER BY r.ordinal",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,b->record_ns,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,request,-1,SQLITE_STATIC);
    while((step=sqlite3_step(s))==SQLITE_ROW){if(++count>CW_MAX_ROWS || !cw_key(b,(const char *)sqlite3_column_text(s,0)) || rows.len+(size_t)sqlite3_column_bytes(s,2)+strlen((const char *)sqlite3_column_text(s,0))+1024>CW_MAX_PACKET || !cw_row(&rows,s))goto done;}
    if(step!=SQLITE_DONE)goto done;long long epoch=cw_epoch(db,b->record_ns);if(epoch<0)goto done;
    rc=cw_emit(&rows,count,epoch,0,"")?0:6;
 done:sqlite3_finalize(s);bf_mp_writer_free(&rows);return rc;
}
static int cw_write(sqlite3 *db,const CwBinding *b,const char *actor,const uint8_t *packet,size_t bytes,int *committed) {
    bf_mp_reader_t r;bf_mp_reader_init(&r,packet,bytes);uint32_t count=0,nfields;long long version;char request[80],digest[65];CwChange *changes=NULL;sqlite3_stmt *s=NULL;int rc=3,step,changed=0;
    if(!cw_array(&r,&nfields) || nfields!=3 || !cw_uint(&r,&version) || version!=1 || !cw_text(&r,request,sizeof(request)) || !cw_token(request,sizeof(request)) || !cw_array(&r,&count) || !count || count>CW_MAX_ROWS)goto done;
    changes=calloc(count,sizeof(*changes));if(!changes){rc=6;goto done;}
    for(uint32_t n=0;n<count;n++){
        CwChange *c=&changes[n];long long mode;bf_mp_value_t value;
        if(!cw_array(&r,&nfields) || nfields!=4 || !cw_text(&r,c->key,sizeof(c->key)) || !cw_key(b,c->key) || !cw_uint(&r,&c->expected) || !cw_uint(&r,&mode) || mode>2)goto done;
        for(uint32_t k=0;k<n;k++)if(!strcmp(c->key,changes[k].key))goto done;c->mode=(int)mode;
        bf_mp_type_t type=bf_mp_read_next(&r,&value);
        if(mode==0){
            if(type!=BF_MP_BIN || !value.v.raw.len || value.v.raw.len>CW_MAX_VALUE)goto done;
            c->body=value.v.raw.ptr;c->bytes=value.v.raw.len;
            if(!cw_record(db,b,c->body,c->bytes))goto done;bf_sha256_hex(c->body,c->bytes,c->hash);
        }else if(type!=BF_MP_NIL)goto done;
    }
    if(r.pos!=r.len)goto done;bf_sha256_hex(packet,bytes,digest);rc=6;
    if(sqlite3_prepare_v2(db,"SELECT digest FROM _workspace_operation WHERE scope_key=?1 AND request_id=?2",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,b->record_ns,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,request,-1,SQLITE_STATIC);step=sqlite3_step(s);
    if(step==SQLITE_ROW){const char *old=(const char *)sqlite3_column_text(s,0);int same=old && !strcmp(old,digest);sqlite3_finalize(s);s=NULL;rc=same?cw_replay(db,b,request):4;goto done;}
    if(step!=SQLITE_DONE)goto done;sqlite3_finalize(s);s=NULL;
    size_t receipt_bytes=0;
    for(uint32_t n=0;n<count;n++){
        CwChange *c=&changes[n];long long revision=0;int deleted=1;
        if(sqlite3_prepare_v2(db,"SELECT revision,deleted,COALESCE((SELECT length(body) FROM _workspace_body b WHERE b.scope_key=_workspace_record.scope_key AND b.digest=_workspace_record.body_digest),0) FROM _workspace_record WHERE scope_key=?1 AND record_key=?2",-1,&s,NULL)!=SQLITE_OK)goto done;
        sqlite3_bind_text(s,1,b->record_ns,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,c->key,-1,SQLITE_STATIC);step=sqlite3_step(s);
        if(step==SQLITE_ROW){revision=sqlite3_column_int64(s,0);deleted=sqlite3_column_int(s,1);if(c->mode==1)receipt_bytes+=(size_t)sqlite3_column_int(s,2);}else if(step!=SQLITE_DONE)goto done;
        sqlite3_finalize(s);s=NULL;receipt_bytes+=strlen(c->key)+32+c->bytes;if(receipt_bytes>CW_MAX_PACKET){rc=5;goto done;}if(revision!=c->expected || revision>=INT_MAX-1 || (c->mode!=0 && deleted)){rc=4;goto done;}
    }
    if(sqlite3_prepare_v2(db,"INSERT INTO _workspace_operation VALUES(?1,?2,?3,?4)",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,b->record_ns,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,request,-1,SQLITE_STATIC);sqlite3_bind_text(s,3,digest,-1,SQLITE_STATIC);sqlite3_bind_text(s,4,actor,-1,SQLITE_STATIC);
    if(sqlite3_step(s)!=SQLITE_DONE)goto done;sqlite3_finalize(s);s=NULL;
    for(uint32_t n=0;n<count;n++){
        CwChange *c=&changes[n];
        if(c->mode==0){
            if(sqlite3_prepare_v2(db,"INSERT INTO _workspace_body VALUES(?1,?2,?3) ON CONFLICT(scope_key,digest) DO UPDATE SET body=excluded.body WHERE body=excluded.body",-1,&s,NULL)!=SQLITE_OK)goto done;
            sqlite3_bind_text(s,1,b->record_ns,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,c->hash,-1,SQLITE_STATIC);sqlite3_bind_blob(s,3,c->body,(int)c->bytes,SQLITE_STATIC);
            if(sqlite3_step(s)!=SQLITE_DONE || sqlite3_changes(db)!=1)goto done;sqlite3_finalize(s);s=NULL;
        }
        if(c->mode!=1){
            if(sqlite3_prepare_v2(db,"INSERT INTO _workspace_record VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(scope_key,record_key) DO UPDATE SET revision=excluded.revision,body_digest=excluded.body_digest,deleted=excluded.deleted,actor=excluded.actor",-1,&s,NULL)!=SQLITE_OK)goto done;
            sqlite3_bind_text(s,1,b->record_ns,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,c->key,-1,SQLITE_STATIC);sqlite3_bind_int64(s,3,c->expected+1);if(c->mode==0)sqlite3_bind_text(s,4,c->hash,-1,SQLITE_STATIC);else sqlite3_bind_null(s,4);sqlite3_bind_int(s,5,c->mode==2);sqlite3_bind_text(s,6,actor,-1,SQLITE_STATIC);
            if(sqlite3_step(s)!=SQLITE_DONE)goto done;sqlite3_finalize(s);s=NULL;changed=1;
        }
        if(sqlite3_prepare_v2(db,"INSERT INTO _workspace_operation_row SELECT scope_key,?1,?2,record_key,revision,body_digest,deleted FROM _workspace_record WHERE scope_key=?3 AND record_key=?4",-1,&s,NULL)!=SQLITE_OK)goto done;
        sqlite3_bind_text(s,1,request,-1,SQLITE_STATIC);sqlite3_bind_int(s,2,(int)n);sqlite3_bind_text(s,3,b->record_ns,-1,SQLITE_STATIC);sqlite3_bind_text(s,4,c->key,-1,SQLITE_STATIC);
        if(sqlite3_step(s)!=SQLITE_DONE || sqlite3_changes(db)!=1)goto done;sqlite3_finalize(s);s=NULL;
    }
    if(changed){
        if(cw_epoch(db,b->record_ns)>=INT_MAX-1){rc=5;goto done;}
        if(sqlite3_prepare_v2(db,"INSERT INTO _workspace_epoch VALUES(?1,1) ON CONFLICT(scope_key) DO UPDATE SET revision=revision+1",-1,&s,NULL)!=SQLITE_OK)goto done;
        sqlite3_bind_text(s,1,b->record_ns,-1,SQLITE_STATIC);if(sqlite3_step(s)!=SQLITE_DONE)goto done;sqlite3_finalize(s);s=NULL;
    }
    if(sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK)goto done;*committed=1;rc=cw_replay(db,b,request);
 done:sqlite3_finalize(s);free(changes);return rc;
}
#include "workspace_continuation.h"
static int cw_dispatch(sqlite3 *db,const char *operation,const char *binding,const char *subject,const char *scope,const char *actor,const char *prefix,const char *cursor,long long epoch) {
    int blob_put=operation && !strcmp(operation,"blob-put"),blob_get=operation && !strcmp(operation,"blob-get"),write=operation && (!strcmp(operation,"write") || blob_put),rc=3,committed=0,begun=0;uint8_t *packet=NULL;CwBinding b;
    if(!operation || (!write && !blob_get && strcmp(operation,"read") && strcmp(operation,"get") && strcmp(operation,"describe") && strcmp(operation,"context") && strcmp(operation,"continuation")) || !bfpg_token(subject,160) || !bfpg_token(actor,160) || !scope || (strcmp(scope,"self") && strcmp(scope,"institution") && strcmp(scope,"household")) || (!strcmp(scope,"self") && strcmp(subject,actor)))goto done;
    if(sqlite3_exec(db,write?"BEGIN IMMEDIATE":"BEGIN",NULL,NULL,NULL)!=SQLITE_OK){rc=6;goto done;}begun=1;
    if(!strcmp(scope,"household")){
        BfHouseholdAccess access;
        if(strncmp(subject,"household:",10) || !bhh_context(db,subject+10,actor,&access) || bhh_rank(access.role)<(write?2:1)){rc=4;goto done;}
    }
    if(!cw_lookup(db,binding,subject,scope,&b)){rc=4;goto done;}
    if(write){packet=malloc(CW_MAX_PACKET+1);if(!packet){rc=6;goto done;}size_t size=fread(packet,1,CW_MAX_PACKET+1,stdin);if(ferror(stdin)){rc=6;goto done;}if(size>CW_MAX_PACKET){rc=5;goto done;}rc=blob_put?cw_blob_put(db,&b,packet,size,&committed):cw_write(db,&b,actor,packet,size,&committed);}
    else if(!strcmp(operation,"context")){
        bf_mp_writer_t out;bf_mp_writer_init(&out,1024);
        int ok=cw_context_pack(&out,&b);
        rc=ok && fwrite(out.buf,1,out.len,stdout)==out.len?0:6;bf_mp_writer_free(&out);
    }
    else if(!strcmp(operation,"continuation"))rc=cw_continuation(db,&b,actor,prefix);
    else if(!strcmp(operation,"describe")){rc=printf("workspace\t%s\nsubject\t%s\nscope\t%s\npack\t%s\nrelease\t%s\nentry\t%s\n",b.id,b.subject,b.scope,b.pack,b.release,b.entry)<0?6:0;}
    else if(blob_get)rc=cw_blob_get(db,&b,prefix,cursor);
    else rc=cw_read(db,&b,operation,prefix?prefix:"",cursor?cursor:"",epoch);
 done:free(packet);if(begun && !committed)sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);
    if(rc)fputs(rc==4?"A draft changed in another tab or this workspace is no longer active. Nothing was overwritten.\n":rc==3?"The workspace request contains an unsupported record, type or duplicate key.\n":rc==5?"The workspace batch is too large. Nothing was saved.\n":"The native content owner could not confirm this operation.\n",stderr);
    return rc;
}
#endif
