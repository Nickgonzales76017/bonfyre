#ifndef BONFYRE_CMS_ENTRY_SNAPSHOT_H
#define BONFYRE_CMS_ENTRY_SNAPSHOT_H
#include "bf_msgpack.h"
/* Exact typed row baseline, shared by CMS writes and its native editor. This
 * adds no record store; changes to any field invalidate a reviewed baseline. */
static int cms_snapshot_statement(sqlite3_stmt *row,char out[65]){
    if(!row||!out)return 0;
    bf_mp_writer_t w;bf_mp_writer_init(&w,4096);int columns=sqlite3_column_count(row),ok=1;
    if(columns<1||columns>1024||bf_mp_pack_array(&w,(uint32_t)columns))ok=0;
    for(int i=0;i<columns&&ok;i++){
        int kind=sqlite3_column_type(row,i);const char *name=sqlite3_column_name(row,i);
        if(!name||bf_mp_pack_array(&w,3)||bf_mp_pack_str_cstr(&w,name)||bf_mp_pack_int(&w,kind)){ok=0;break;}
        if(kind==SQLITE_NULL)ok=!bf_mp_pack_nil(&w);
        else if(kind==SQLITE_INTEGER)ok=!bf_mp_pack_int(&w,sqlite3_column_int64(row,i));
        else if(kind==SQLITE_FLOAT)ok=!bf_mp_pack_double(&w,sqlite3_column_double(row,i));
        else {const void *bytes=sqlite3_column_blob(row,i);int n=sqlite3_column_bytes(row,i);
            if(n<0||n>1024*1024||(!bytes&&n))ok=0;else ok=!bf_mp_pack_bin(&w,bytes,(uint32_t)n);}
        if(w.len>2u*1024u*1024u)ok=0;
    }
    if(ok)bf_sha256_hex(w.buf,w.len,out);bf_mp_writer_free(&w);return ok;
}
static int cms_snapshot_row(sqlite3 *db,const char *type,int id,const char *ns,char out[65]){
    sqlite3_stmt *row=NULL;char *sql=sqlite3_mprintf("SELECT * FROM \"%w\" WHERE id=?1 AND namespace=?2",type);int ok=0;
    if(!sql)return 0;
    if(sqlite3_prepare_v2(db,sql,-1,&row,NULL)==SQLITE_OK){
        sqlite3_bind_int(row,1,id);sqlite3_bind_text(row,2,ns?ns:"root",-1,SQLITE_TRANSIENT);
        if(sqlite3_step(row)==SQLITE_ROW)ok=cms_snapshot_statement(row,out);
    }sqlite3_free(sql);sqlite3_finalize(row);return ok;
}
#endif
