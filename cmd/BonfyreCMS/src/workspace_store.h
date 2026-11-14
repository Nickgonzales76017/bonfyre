#ifndef BONFYRE_CMS_WORKSPACE_STORE_H
#define BONFYRE_CMS_WORKSPACE_STORE_H
#include "bf_msgpack.h"
#include <math.h>
/* Candidate records extend the CMS owner. Published material stays in its
 * source package; each destination retains exact changed record bodies once. */
#define CW_MAX_PACKET (512u * 1024u)
#define CW_MAX_VALUE (256u * 1024u)
#define CW_MAX_ROWS 1000
#define CW_PAGE 128

typedef struct {
    char id[64], ns[80], subject[160], scope[24], pack[96], family[64];
    char release[65], entry[128], record_ns[80], prefixes[32][128];
    size_t prefix_count;
    long long installation;
} CwBinding;
typedef struct {
    char key[512], hash[65];
    long long expected;
    int mode;
    const uint8_t *body;
    size_t bytes;
} CwChange;
static int cw_schema(sqlite3 *db) {
    return sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS _application_workspace(binding_id TEXT PRIMARY KEY,cms_id INTEGER NOT NULL UNIQUE,installation_id INTEGER NOT NULL,family TEXT NOT NULL,release TEXT NOT NULL,entry TEXT NOT NULL);"
        "CREATE INDEX IF NOT EXISTS _application_workspace_install ON _application_workspace(installation_id);"
        "CREATE TABLE IF NOT EXISTS _workspace_body(scope_key TEXT NOT NULL,digest TEXT NOT NULL,body BLOB NOT NULL,PRIMARY KEY(scope_key,digest));"
        "CREATE TABLE IF NOT EXISTS _workspace_record(scope_key TEXT NOT NULL,record_key TEXT NOT NULL,revision INTEGER NOT NULL,body_digest TEXT,deleted INTEGER NOT NULL,actor TEXT NOT NULL,PRIMARY KEY(scope_key,record_key));"
        "CREATE TABLE IF NOT EXISTS _workspace_blob(id INTEGER PRIMARY KEY,scope_key TEXT NOT NULL,digest TEXT NOT NULL,bytes INTEGER NOT NULL,complete INTEGER NOT NULL,body BLOB NOT NULL,UNIQUE(scope_key,digest));"
        "CREATE TABLE IF NOT EXISTS _workspace_blob_piece(blob_id INTEGER NOT NULL,offset INTEGER NOT NULL,bytes INTEGER NOT NULL,digest TEXT NOT NULL,PRIMARY KEY(blob_id,offset));"
        "CREATE TABLE IF NOT EXISTS _workspace_epoch(scope_key TEXT PRIMARY KEY,revision INTEGER NOT NULL);"
        "CREATE TABLE IF NOT EXISTS _workspace_operation(scope_key TEXT NOT NULL,request_id TEXT NOT NULL,digest TEXT NOT NULL,actor TEXT NOT NULL,PRIMARY KEY(scope_key,request_id));"
        "CREATE TABLE IF NOT EXISTS _workspace_operation_row(scope_key TEXT NOT NULL,request_id TEXT NOT NULL,ordinal INTEGER NOT NULL,record_key TEXT NOT NULL,revision INTEGER NOT NULL,body_digest TEXT,deleted INTEGER NOT NULL,PRIMARY KEY(scope_key,request_id,ordinal));",
        NULL,NULL,NULL)==SQLITE_OK;
}
static int cw_token(const char *s,size_t cap) {
    return s && *s && strlen(s)<cap && strspn(s,"abcdefghijklmnopqrstuvwxyz0123456789-_")==strlen(s);
}
static int cw_entry(const char *s) {
    return s && *s && strlen(s)<128 && s[0]!='/' && s[strlen(s)-1]=='/' &&
        strspn(s,"abcdefghijklmnopqrstuvwxyz0123456789-_/ ")==strlen(s) && !strchr(s,' ') && !strstr(s,"//");
}
static int cw_load_type(sqlite3 *db) {
    if(find_type("application_binding"))return 1;
    sqlite3_stmt *s=NULL;ContentType type={0};int ok=0;
    if(sqlite3_prepare_v2(db,"SELECT schema_json FROM _content_types WHERE name='application_binding'",-1,&s,NULL)==SQLITE_OK && sqlite3_step(s)==SQLITE_ROW && g_type_count<CMS_MAX_TYPES){
        const char *raw=(const char *)sqlite3_column_text(s,0);
        if(raw && parse_schema(raw,&type)==0 && !strcmp(type.name,"application_binding")){g_types[g_type_count++]=type;ok=1;}else free(type.fields);
    }
    sqlite3_finalize(s);return ok;
}
/* Called within the pack owner's existing immediate transaction and row hooks. */
static int cw_bind(sqlite3 *db,const char *operation,const char *ns,const char *subject,
                   const char *scope,const char *pack,const CmsPackInstallation *installation,
                   const BfPackResolution *plan) {
    sqlite3_stmt *s=NULL;int rc=0,step=SQLITE_DONE,count=0;char *body=NULL;
    char seen[16][64];size_t seen_count=0;
    if(!strcmp(operation,"install")){
        for(size_t n=0;n<plan->count;n++){
            if(sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM pack_demand WHERE namespace='root' AND pack=?1 AND demand_kind='workspace' AND status='published'",-1,&s,NULL)!=SQLITE_OK)goto done;
            sqlite3_bind_text(s,1,plan->nodes[n].pack,-1,SQLITE_STATIC);
            if(sqlite3_step(s)!=SQLITE_ROW)goto done;count+=sqlite3_column_int(s,0);sqlite3_finalize(s);s=NULL;
        }
    }
    if(!count && !cpack_exists(db,"_application_workspace"))return 1;
    if(count>16 || !cw_load_type(db) || !cw_schema(db))goto done;
    if(sqlite3_prepare_v2(db,"SELECT w.cms_id,a.status FROM _application_workspace w JOIN application_binding a ON a.id=w.cms_id WHERE w.installation_id=?1 ORDER BY w.cms_id",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_int64(s,1,installation->id);
    while((step=sqlite3_step(s))==SQLITE_ROW){
        if((!strcmp(operation,"activate") || !strcmp(operation,"deactivate")) && !strcmp((const char *)sqlite3_column_text(s,1),"withdrawn"))continue;
        const char *status=!strcmp(operation,"activate")?"active":!strcmp(operation,"deactivate")?"draft":"withdrawn";
        const char *v[]={status};body=cpack_json(db,"SELECT json_object('status',?1)",v,1);
        if(!body || entry_update(db,"application_binding",sqlite3_column_int(s,0),ns,body))goto done;
        free(body);body=NULL;
    }
    sqlite3_finalize(s);s=NULL;if(step!=SQLITE_DONE)goto done;
    if(strcmp(operation,"install")){rc=1;goto done;}
    for(size_t n=0;n<plan->count;n++){
        if(sqlite3_prepare_v2(db,"SELECT needs,preferred_sources,fields FROM pack_demand WHERE namespace='root' AND pack=?1 AND demand_kind='workspace' AND status='published' ORDER BY id",-1,&s,NULL)!=SQLITE_OK)goto done;
        sqlite3_bind_text(s,1,plan->nodes[n].pack,-1,SQLITE_STATIC);
        while((step=sqlite3_step(s))==SQLITE_ROW){
            char family[64],release[65],entry[128],key[640],hash[65],id[64],aperture[128],continuity[80];
            if(!bfpg_text(s,0,family,sizeof(family)) || !bfpg_text(s,1,release,sizeof(release)) || !bfpg_text(s,2,entry,sizeof(entry)) ||
                !cw_token(family,sizeof(family)) || strlen(release)!=64 || strspn(release,"abcdef0123456789")!=64 || !cw_entry(entry))goto done;
            snprintf(key,sizeof(key),"%s\n%s\n%s\n%s\n%s",subject,scope,pack,family,entry);bf_sha256_hex((uint8_t *)key,strlen(key),hash);
            snprintf(id,sizeof(id),"ws-%.40s",hash);
            for(size_t k=0;k<seen_count;k++)if(!strcmp(seen[k],id))goto done;
            if(seen_count>=16)goto done;snprintf(seen[seen_count++],64,"%s",id);
            snprintf(aperture,sizeof(aperture),"/apps/%s/_records",id);
            snprintf(key,sizeof(key),"%s\n%s\n%s",subject,scope,family);bf_sha256_hex((uint8_t *)key,strlen(key),hash);snprintf(continuity,sizeof(continuity),"workspace-%.40s",hash);
            sqlite3_stmt *q=NULL;long long row=0;
            if(sqlite3_prepare_v2(db,"SELECT cms_id,installation_id FROM _application_workspace WHERE binding_id=?1",-1,&q,NULL)!=SQLITE_OK)goto done;
            sqlite3_bind_text(q,1,id,-1,SQLITE_STATIC);int found=sqlite3_step(q);
            if(found==SQLITE_ROW){row=sqlite3_column_int64(q,0);if(sqlite3_column_int64(q,1)!=installation->id){sqlite3_finalize(q);goto done;}}
            else if(found!=SQLITE_DONE){sqlite3_finalize(q);goto done;}
            sqlite3_finalize(q);
            const char *v[]={id,subject,pack,family,aperture,continuity};
            body=cpack_json(db,"SELECT json_object('binding_id',?1,'site_id',?2,'slot_id',?3,'application_kind',?4,'material_query',json('{}'),'source_collection',?4,'action_aperture',?5,'continuity_key',?6,'status','draft')",v,6);
            if(!body || (row?entry_update(db,"application_binding",(int)row,ns,body):entry_create(db,"application_binding",ns,body,&row)))goto done;
            free(body);body=NULL;
            if(sqlite3_prepare_v2(db,"INSERT INTO _application_workspace VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(binding_id) DO UPDATE SET release=excluded.release,entry=excluded.entry",-1,&q,NULL)!=SQLITE_OK)goto done;
            sqlite3_bind_text(q,1,id,-1,SQLITE_STATIC);sqlite3_bind_int64(q,2,row);sqlite3_bind_int64(q,3,installation->id);sqlite3_bind_text(q,4,family,-1,SQLITE_STATIC);sqlite3_bind_text(q,5,release,-1,SQLITE_STATIC);sqlite3_bind_text(q,6,entry,-1,SQLITE_STATIC);
            found=sqlite3_step(q);sqlite3_finalize(q);if(found!=SQLITE_DONE)goto done;
        }
        sqlite3_finalize(s);s=NULL;if(step!=SQLITE_DONE)goto done;
    }
    rc=1;
 done:sqlite3_finalize(s);free(body);return rc;
}
/* Store protocol definitions follow in the same CMS translation unit. */
#include "workspace_records.h"
#endif
