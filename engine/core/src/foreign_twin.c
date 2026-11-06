#include "bf_foreign_twin.h"

#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>

struct BfForeignTwin { sqlite3 *db; int owns_db; };
static const char *SCHEMA =
    "CREATE TABLE IF NOT EXISTS foreign_twin_observations("
    " receipt_id TEXT PRIMARY KEY, subject TEXT NOT NULL, predicate TEXT NOT NULL,"
    " source_url TEXT NOT NULL, body_sha256 TEXT NOT NULL, observed_at TEXT NOT NULL, observer TEXT NOT NULL,"
    " UNIQUE(subject,predicate,body_sha256,observed_at,observer));"
    "CREATE INDEX IF NOT EXISTS foreign_twin_subject_predicate"
    " ON foreign_twin_observations(subject,predicate,observed_at);";
static int open_db(sqlite3 *db, int owns, BfForeignTwin **out, FILE *err) {
    BfForeignTwin *value; char *message = NULL;
    if (sqlite3_exec(db, SCHEMA, NULL, NULL, &message) != SQLITE_OK) { if (err) fprintf(err,"foreign_twin: schema failed: %s\n",message ? message : "?"); sqlite3_free(message); return -1; }
    value = calloc(1,sizeof(*value)); if (!value) return -1; value->db=db; value->owns_db=owns; *out=value; return 0;
}
int bf_foreign_twin_open(const char *path,BfForeignTwin **out,FILE *err) { sqlite3 *db=NULL; if (!path || !out || sqlite3_open(path,&db)!=SQLITE_OK) { if (db) sqlite3_close(db); return -1; } if (open_db(db,1,out,err)) { sqlite3_close(db); return -1; } return 0; }
int bf_foreign_twin_open_database(void *raw,BfForeignTwin **out,FILE *err) { return !raw || !out ? -1 : open_db((sqlite3 *)raw,0,out,err); }
void bf_foreign_twin_close(BfForeignTwin *value) { if (!value) return; if (value->owns_db) sqlite3_close(value->db); free(value); }
int bf_foreign_twin_record(BfForeignTwin *value,const BfForeignTwinObservation *o) {
    sqlite3_stmt *s=NULL; int rc; int changes;
    if (!value || !o || !o->receipt_id || !o->subject || !o->predicate || !o->source_url || !o->body_sha256 || !o->observed_at || !o->observer || !o->receipt_id[0] || !o->subject[0] || !o->predicate[0]) return -1;
    if (sqlite3_prepare_v2(value->db,"INSERT INTO foreign_twin_observations(receipt_id,subject,predicate,source_url,body_sha256,observed_at,observer) VALUES(?,?,?,?,?,?,?) ON CONFLICT(receipt_id) DO NOTHING",-1,&s,NULL)!=SQLITE_OK) return -1;
    sqlite3_bind_text(s,1,o->receipt_id,-1,SQLITE_TRANSIENT); sqlite3_bind_text(s,2,o->subject,-1,SQLITE_TRANSIENT); sqlite3_bind_text(s,3,o->predicate,-1,SQLITE_TRANSIENT); sqlite3_bind_text(s,4,o->source_url,-1,SQLITE_TRANSIENT); sqlite3_bind_text(s,5,o->body_sha256,-1,SQLITE_TRANSIENT); sqlite3_bind_text(s,6,o->observed_at,-1,SQLITE_TRANSIENT); sqlite3_bind_text(s,7,o->observer,-1,SQLITE_TRANSIENT);
    rc=sqlite3_step(s); sqlite3_finalize(s); if (rc != SQLITE_DONE) return -1;
    changes=sqlite3_changes(value->db); if (changes) return 0;
    if(sqlite3_prepare_v2(value->db,"SELECT 1 FROM foreign_twin_observations WHERE receipt_id=? AND subject=? AND predicate=? AND source_url=? AND body_sha256=? AND observed_at=? AND observer=?",-1,&s,NULL)!=SQLITE_OK)return -1;
    sqlite3_bind_text(s,1,o->receipt_id,-1,SQLITE_TRANSIENT); sqlite3_bind_text(s,2,o->subject,-1,SQLITE_TRANSIENT); sqlite3_bind_text(s,3,o->predicate,-1,SQLITE_TRANSIENT); sqlite3_bind_text(s,4,o->source_url,-1,SQLITE_TRANSIENT); sqlite3_bind_text(s,5,o->body_sha256,-1,SQLITE_TRANSIENT); sqlite3_bind_text(s,6,o->observed_at,-1,SQLITE_TRANSIENT); sqlite3_bind_text(s,7,o->observer,-1,SQLITE_TRANSIENT);
    rc=sqlite3_step(s); sqlite3_finalize(s); return rc==SQLITE_ROW ? 0 : -1;
}
int bf_foreign_twin_observed(BfForeignTwin *value,const char *subject,const char *predicate) { sqlite3_stmt *s=NULL; int answer; if(!value||!subject||!predicate) return -1; if(sqlite3_prepare_v2(value->db,"SELECT 1 FROM foreign_twin_observations WHERE subject=? AND predicate=? LIMIT 1",-1,&s,NULL)!=SQLITE_OK)return -1; sqlite3_bind_text(s,1,subject,-1,SQLITE_TRANSIENT); sqlite3_bind_text(s,2,predicate,-1,SQLITE_TRANSIENT); answer=sqlite3_step(s)==SQLITE_ROW; sqlite3_finalize(s); return answer; }
int bf_foreign_twin_latest_receipt(BfForeignTwin *value,const char *subject,const char *predicate,char *out,size_t out_size) {
    sqlite3_stmt *s=NULL; int found=0;
    if(!value||!subject||!predicate||!out||out_size<2)return -1;
    out[0]='\0';
    if(sqlite3_prepare_v2(value->db,"SELECT receipt_id FROM foreign_twin_observations WHERE subject=? AND predicate=? ORDER BY observed_at DESC,receipt_id DESC LIMIT 1",-1,&s,NULL)!=SQLITE_OK)return -1;
    sqlite3_bind_text(s,1,subject,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,predicate,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(s)==SQLITE_ROW){const char *receipt=(const char *)sqlite3_column_text(s,0);snprintf(out,out_size,"%s",receipt?receipt:"");found=out[0]!='\0';}
    sqlite3_finalize(s);return found;
}
