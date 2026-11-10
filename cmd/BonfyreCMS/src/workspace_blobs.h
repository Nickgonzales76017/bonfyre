#ifndef BONFYRE_CMS_WORKSPACE_BLOBS_H
#define BONFYRE_CMS_WORKSPACE_BLOBS_H
#define CW_BLOB_CHUNK (256u * 1024u)
#define CW_BLOB_LIMIT (24u * 1024u * 1024u)
#define CW_BLOB_BUDGET (128u * 1024u * 1024u)
static int cw_blob_hash(const char *s){return s && strlen(s)==64 && strspn(s,"abcdef0123456789")==64;}
static int cw_blob_exists(sqlite3 *db,const CwBinding *b,const char *hash,long long bytes){
    sqlite3_stmt *s=NULL;int ok=0;
    if(sqlite3_prepare_v2(db,"SELECT 1 FROM _workspace_blob WHERE scope_key=?1 AND digest=?2 AND bytes=?3 AND complete=1",-1,&s,NULL)==SQLITE_OK){sqlite3_bind_text(s,1,b->record_ns,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,hash,-1,SQLITE_STATIC);sqlite3_bind_int64(s,3,bytes);ok=sqlite3_step(s)==SQLITE_ROW;}
    sqlite3_finalize(s);return ok;
}
/* A per-record value holds source-blob references, never another user's bytes.
 * v2 has six-field file references; v3 adds kind 0=file, 1=ArrayBuffer.
 * v4 also admits kind 2=Uint8Array, using the same scoped blob transport. */
static int cw_record(sqlite3 *db,const CwBinding *b,const uint8_t *body,size_t bytes){
    bf_mp_reader_t r;bf_mp_reader_init(&r,body,bytes);uint32_t n,count;long long version;size_t nodes=0;
    if(!cw_array(&r,&n) || n!=3 || !cw_uint(&r,&version) || (version!=2&&version!=3&&version!=4) || !cw_value(&r,0,&nodes) || !cw_array(&r,&count) || count>64)return 0;
    for(uint32_t k=0;k<count;k++){
        char hash[65],mime[256];long long size;bf_mp_value_t value;
        if(!cw_array(&r,&n) || n!=(version>=3?7u:6u) || !cw_array(&r,&n) || n>32)return 0;
        for(uint32_t j=0;j<n;j++){bf_mp_type_t type=bf_mp_read_next(&r,&value);if(type==BF_MP_STR){if(value.v.raw.len>512 || !cw_utf8(value.v.raw.ptr,value.v.raw.len))return 0;}else if(type!=BF_MP_UINT || value.v.u>20000)return 0;}
        if(!cw_text(&r,hash,sizeof(hash)) || !cw_blob_hash(hash) || !cw_uint(&r,&size) || size>CW_BLOB_LIMIT || !cw_text(&r,mime,sizeof(mime)) || !cw_blob_exists(db,b,hash,size))return 0;
        bf_mp_type_t type=bf_mp_read_next(&r,&value);if(type!=BF_MP_NIL && (type!=BF_MP_STR || value.v.raw.len>1024 || !cw_utf8(value.v.raw.ptr,value.v.raw.len)))return 0;
        int name_nil=type==BF_MP_NIL;
        type=bf_mp_read_next(&r,&value);if(type!=BF_MP_NIL && !((type==BF_MP_UINT && value.v.u<=9007199254740991ULL) || (type==BF_MP_INT && value.v.i>=0 && value.v.i<=9007199254740991LL)))return 0;
        int modified_nil=type==BF_MP_NIL;if(name_nil!=modified_nil)return 0;
        if(version>=3){
            long long kind;
            if(!cw_uint(&r,&kind)||kind>(version==4?2:1)||(kind>0&&(*mime||!name_nil||!modified_nil)))return 0;
        }
    }
    return r.pos==r.len;
}
static int cw_blob_response(const char *hash,long long size,long long offset,const void *body,size_t bytes,int complete){
    bf_mp_writer_t out;bf_mp_writer_init(&out,bytes+128);
    int ok=!bf_mp_pack_array(&out,6) && !bf_mp_pack_uint(&out,1) && !bf_mp_pack_str_cstr(&out,hash) && !bf_mp_pack_uint(&out,(uint64_t)size) && !bf_mp_pack_uint(&out,(uint64_t)offset) && !bf_mp_pack_bool(&out,complete) && !bf_mp_pack_bin(&out,body,(uint32_t)bytes);
    if(ok)ok=fwrite(out.buf,1,out.len,stdout)==out.len;
    bf_mp_writer_free(&out);return ok?0:6;
}
static int cw_blob_put(sqlite3 *db,const CwBinding *b,const uint8_t *packet,size_t bytes,int *committed){
    bf_mp_reader_t r;bf_mp_reader_init(&r,packet,bytes);uint32_t n;long long version,total,offset;char hash[65],piece_hash[65];bf_mp_value_t value;
    sqlite3_stmt *s=NULL;sqlite3_blob *blob=NULL;sqlite3_int64 row=0;int rc=3,step,complete=0;uint8_t *verify=NULL;
    if(!cw_array(&r,&n) || n!=5 || !cw_uint(&r,&version) || version!=1 || !cw_text(&r,hash,sizeof(hash)) || !cw_blob_hash(hash) || !cw_uint(&r,&total) || total>CW_BLOB_LIMIT || !cw_uint(&r,&offset) || offset>total || offset%CW_BLOB_CHUNK || bf_mp_read_next(&r,&value)!=BF_MP_BIN || r.pos!=r.len || value.v.raw.len!=(uint32_t)((total-offset)<CW_BLOB_CHUNK?(total-offset):CW_BLOB_CHUNK))goto done;
    rc=6;bf_sha256_hex(value.v.raw.ptr,value.v.raw.len,piece_hash);
    if(sqlite3_prepare_v2(db,"SELECT id,bytes,complete FROM _workspace_blob WHERE scope_key=?1 AND digest=?2",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,b->record_ns,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,hash,-1,SQLITE_STATIC);step=sqlite3_step(s);
    if(step==SQLITE_ROW){row=sqlite3_column_int64(s,0);complete=sqlite3_column_int(s,2);if(sqlite3_column_int64(s,1)!=total){rc=4;goto done;}}
    else if(step!=SQLITE_DONE)goto done;
    sqlite3_finalize(s);s=NULL;
    if(!row){
        if(offset){rc=4;goto done;}
        if(sqlite3_prepare_v2(db,"SELECT COALESCE(SUM(bytes),0) FROM _workspace_blob WHERE scope_key=?1",-1,&s,NULL)!=SQLITE_OK)goto done;
        sqlite3_bind_text(s,1,b->record_ns,-1,SQLITE_STATIC);if(sqlite3_step(s)!=SQLITE_ROW)goto done;if(sqlite3_column_int64(s,0)+total>CW_BLOB_BUDGET){rc=5;goto done;}sqlite3_finalize(s);s=NULL;
        if(sqlite3_prepare_v2(db,"INSERT INTO _workspace_blob(scope_key,digest,bytes,complete,body) VALUES(?1,?2,?3,0,zeroblob(?3))",-1,&s,NULL)!=SQLITE_OK)goto done;
        sqlite3_bind_text(s,1,b->record_ns,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,hash,-1,SQLITE_STATIC);sqlite3_bind_int64(s,3,total);if(sqlite3_step(s)!=SQLITE_DONE)goto done;row=sqlite3_last_insert_rowid(db);sqlite3_finalize(s);s=NULL;
    }
    if(sqlite3_prepare_v2(db,"SELECT digest FROM _workspace_blob_piece WHERE blob_id=?1 AND offset=?2",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_int64(s,1,row);sqlite3_bind_int64(s,2,offset);step=sqlite3_step(s);
    if(step==SQLITE_ROW){const char *old=(const char *)sqlite3_column_text(s,0);if(!old || strcmp(old,piece_hash)){rc=4;goto done;}}
    else if(step!=SQLITE_DONE)goto done;
    int have_piece=step==SQLITE_ROW;sqlite3_finalize(s);s=NULL;
    if(!have_piece){
        if(complete){rc=4;goto done;}
        if(total && (sqlite3_blob_open(db,"main","_workspace_blob","body",row,1,&blob)!=SQLITE_OK || sqlite3_blob_write(blob,value.v.raw.ptr,(int)value.v.raw.len,(int)offset)!=SQLITE_OK))goto done;
        if(blob){sqlite3_blob_close(blob);blob=NULL;}
        if(sqlite3_prepare_v2(db,"INSERT INTO _workspace_blob_piece VALUES(?1,?2,?3,?4)",-1,&s,NULL)!=SQLITE_OK)goto done;
        sqlite3_bind_int64(s,1,row);sqlite3_bind_int64(s,2,offset);sqlite3_bind_int(s,3,(int)value.v.raw.len);sqlite3_bind_text(s,4,piece_hash,-1,SQLITE_STATIC);if(sqlite3_step(s)!=SQLITE_DONE)goto done;sqlite3_finalize(s);s=NULL;
    }
    if(!complete){
        if(sqlite3_prepare_v2(db,"SELECT COALESCE(SUM(bytes),0) FROM _workspace_blob_piece WHERE blob_id=?1",-1,&s,NULL)!=SQLITE_OK)goto done;
        sqlite3_bind_int64(s,1,row);if(sqlite3_step(s)!=SQLITE_ROW)goto done;int ready=sqlite3_column_int64(s,0)==total;sqlite3_finalize(s);s=NULL;
        if(ready){
            char actual[65];verify=malloc(total?(size_t)total:1);if(!verify)goto done;
            if(total && (sqlite3_blob_open(db,"main","_workspace_blob","body",row,0,&blob)!=SQLITE_OK || sqlite3_blob_read(blob,verify,(int)total,0)!=SQLITE_OK))goto done;
            if(blob){sqlite3_blob_close(blob);blob=NULL;}bf_sha256_hex(verify,(size_t)total,actual);if(strcmp(hash,actual)){rc=3;goto done;}
            if(sqlite3_prepare_v2(db,"UPDATE _workspace_blob SET complete=1 WHERE id=?1",-1,&s,NULL)!=SQLITE_OK)goto done;sqlite3_bind_int64(s,1,row);if(sqlite3_step(s)!=SQLITE_DONE)goto done;sqlite3_finalize(s);s=NULL;complete=1;
        }
    }
    if(sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK)goto done;*committed=1;rc=cw_blob_response(hash,total,offset,NULL,0,complete);
 done:if(blob)sqlite3_blob_close(blob);sqlite3_finalize(s);free(verify);return rc;
}
static int cw_blob_get(sqlite3 *db,const CwBinding *b,const char *hash,const char *position){
    if(!cw_blob_hash(hash) || !position || !*position || strspn(position,"0123456789")!=strlen(position))return 3;
    char *end=NULL;errno=0;long long offset=strtoll(position,&end,10);if(errno || !end || *end || offset<0 || offset>CW_BLOB_LIMIT || offset%CW_BLOB_CHUNK)return 3;
    sqlite3_stmt *s=NULL;int rc=6;
    if(sqlite3_prepare_v2(db,"SELECT bytes,substr(body,?3+1,?4) FROM _workspace_blob WHERE scope_key=?1 AND digest=?2 AND complete=1",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,b->record_ns,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,hash,-1,SQLITE_STATIC);sqlite3_bind_int64(s,3,offset);sqlite3_bind_int(s,4,CW_BLOB_CHUNK);int step=sqlite3_step(s);
    if(step==SQLITE_DONE){rc=4;goto done;}if(step!=SQLITE_ROW)goto done;long long total=sqlite3_column_int64(s,0);if(total<offset || total>CW_BLOB_LIMIT){rc=3;goto done;}
    rc=cw_blob_response(hash,total,offset,sqlite3_column_blob(s,1),(size_t)sqlite3_column_bytes(s,1),1);
 done:sqlite3_finalize(s);return rc;
}
#endif
