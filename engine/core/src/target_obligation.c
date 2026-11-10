#include "bf_target_obligation.h"

#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>

struct BfTargetObligation { sqlite3 *db; FILE *err; };

static const char *SCHEMA =
    "CREATE TABLE IF NOT EXISTS target_obligations("
    "target_id TEXT PRIMARY KEY,target TEXT NOT NULL,semantic_contract TEXT NOT NULL,"
    "source_frontier TEXT NOT NULL,preference_policy TEXT NOT NULL,state TEXT NOT NULL,"
    "terminal_reason TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),"
    "updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')));"
    "CREATE TABLE IF NOT EXISTS target_predicate_refs("
    "target_id TEXT NOT NULL,predicate_id TEXT NOT NULL,owner TEXT NOT NULL,subject TEXT NOT NULL,"
    "predicate TEXT NOT NULL,expected_state TEXT NOT NULL,valid_start TEXT NOT NULL DEFAULT '',valid_end TEXT NOT NULL DEFAULT '',"
    "evidence_requirement TEXT NOT NULL DEFAULT '',authority_requirement TEXT NOT NULL DEFAULT '',"
    "adapter INTEGER NOT NULL,native_state INTEGER NOT NULL,owner_query INTEGER NOT NULL DEFAULT 0,"
    "PRIMARY KEY(target_id,predicate_id),FOREIGN KEY(target_id) REFERENCES target_obligations(target_id));"
    "CREATE TABLE IF NOT EXISTS target_obligation_events("
    "event_id INTEGER PRIMARY KEY,target_id TEXT NOT NULL,event_kind TEXT NOT NULL,detail TEXT NOT NULL,"
    "created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')));";

static int text_ok(const char *s, size_t max) { return s && *s && strlen(s) < max; }
static int exec(BfTargetObligation *o, const char *sql) { char *e=NULL; int rc=sqlite3_exec(o->db,sql,NULL,NULL,&e); if(rc!=SQLITE_OK&&o->err)fprintf(o->err,"target-obligation: %s\n",e?e:"schema error");sqlite3_free(e);return rc==SQLITE_OK; }
static int bind_text(sqlite3_stmt *s, int n, const char *v) { return sqlite3_bind_text(s,n,v?v:"",-1,SQLITE_TRANSIENT)==SQLITE_OK; }
static int terminal(BfTargetState state) { return state==BF_TARGET_PROVEN||state==BF_TARGET_SUPERSEDED; }
static BfTargetState parse_state(const char *s) { if(!strcmp(s,"OPEN"))return BF_TARGET_OPEN; if(!strcmp(s,"WAITING_EXTERNAL_PREDICATE"))return BF_TARGET_WAITING_EXTERNAL_PREDICATE; if(!strcmp(s,"PROVEN"))return BF_TARGET_PROVEN; return BF_TARGET_SUPERSEDED; }
const char *bf_target_obligation_state_name(BfTargetState state) { switch(state){case BF_TARGET_OPEN:return "OPEN";case BF_TARGET_WAITING_EXTERNAL_PREDICATE:return "WAITING_EXTERNAL_PREDICATE";case BF_TARGET_PROVEN:return "PROVEN";case BF_TARGET_SUPERSEDED:return "SUPERSEDED";}return "INVALID"; }

int bf_target_obligation_open(const char *path, BfTargetObligation **out, FILE *err) {
    BfTargetObligation *o;
    if(!path||!out||(o=calloc(1,sizeof(*o)))==NULL)return -1;
    if(sqlite3_open(path,&o->db)!=SQLITE_OK){sqlite3_close(o->db);free(o);return -1;}
    o->err=err; sqlite3_exec(o->db,"PRAGMA foreign_keys=ON;PRAGMA journal_mode=WAL;",NULL,NULL,NULL);
    if(!exec(o,SCHEMA)){sqlite3_close(o->db);free(o);return -1;}*out=o;return 0;
}
void bf_target_obligation_close(BfTargetObligation *o){if(!o)return;sqlite3_close(o->db);free(o);}

int bf_target_obligation_require(BfTargetObligation *o,const BfTargetObligationSpec *spec,const BfTargetPredicateSpec *predicates,size_t count){
    sqlite3_stmt *s=NULL;size_t i;BfTargetObligationRecord old;
    if(!o||!spec||!text_ok(spec->target_id,128)||!text_ok(spec->target,512)||!text_ok(spec->semantic_contract,512)||!text_ok(spec->source_frontier,256)||!text_ok(spec->preference_policy,96)||!predicates||!count)return -1;
    if(bf_target_obligation_get(o,spec->target_id,&old)==0&&terminal(old.state))return -2;
    if(!exec(o,"BEGIN IMMEDIATE"))return -1;
    if(sqlite3_prepare_v2(o->db,"INSERT INTO target_obligations(target_id,target,semantic_contract,source_frontier,preference_policy,state,terminal_reason,updated_at) VALUES(?,?,?,?,?,'OPEN','',strftime('%Y-%m-%dT%H:%M:%SZ','now')) ON CONFLICT(target_id) DO UPDATE SET target=excluded.target,semantic_contract=excluded.semantic_contract,source_frontier=excluded.source_frontier,preference_policy=excluded.preference_policy,state='OPEN',terminal_reason='',updated_at=excluded.updated_at",-1,&s,NULL)!=SQLITE_OK)goto fail;
    if(!bind_text(s,1,spec->target_id)||!bind_text(s,2,spec->target)||!bind_text(s,3,spec->semantic_contract)||!bind_text(s,4,spec->source_frontier)||!bind_text(s,5,spec->preference_policy)||sqlite3_step(s)!=SQLITE_DONE)goto fail;sqlite3_finalize(s);s=NULL;
    if(sqlite3_prepare_v2(o->db,"DELETE FROM target_predicate_refs WHERE target_id=?",-1,&s,NULL)!=SQLITE_OK)goto fail;if(!bind_text(s,1,spec->target_id)||sqlite3_step(s)!=SQLITE_DONE)goto fail;sqlite3_finalize(s);s=NULL;
    for(i=0;i<count;i++){const BfTargetPredicateSpec *p=&predicates[i];if(!text_ok(p->predicate_id,128)||!text_ok(p->owner,96)||!text_ok(p->subject,512)||!text_ok(p->predicate,128)||!text_ok(p->expected_state,64)||!p->adapter||!p->native_state)goto fail;if(sqlite3_prepare_v2(o->db,"INSERT INTO target_predicate_refs(target_id,predicate_id,owner,subject,predicate,expected_state,valid_start,valid_end,evidence_requirement,authority_requirement,adapter,native_state,owner_query) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",-1,&s,NULL)!=SQLITE_OK)goto fail;if(!bind_text(s,1,spec->target_id)||!bind_text(s,2,p->predicate_id)||!bind_text(s,3,p->owner)||!bind_text(s,4,p->subject)||!bind_text(s,5,p->predicate)||!bind_text(s,6,p->expected_state)||!bind_text(s,7,p->valid_start)||!bind_text(s,8,p->valid_end)||!bind_text(s,9,p->evidence_requirement)||!bind_text(s,10,p->authority_requirement)||sqlite3_bind_int(s,11,p->adapter)!=SQLITE_OK||sqlite3_bind_int(s,12,p->native_state)!=SQLITE_OK||sqlite3_bind_int(s,13,p->owner_query)!=SQLITE_OK||sqlite3_step(s)!=SQLITE_DONE)goto fail;sqlite3_finalize(s);s=NULL;}
    return exec(o,"COMMIT")?0:-1;
fail:if(s)sqlite3_finalize(s);exec(o,"ROLLBACK");return -1;
}

static void copy_column(char *dst, size_t cap, sqlite3_stmt *stmt, int column) {
    const char *value = (const char *)sqlite3_column_text(stmt, column);
    snprintf(dst, cap, "%s", value ? value : "");
}

int bf_target_obligation_get(BfTargetObligation *o, const char *id,
                             BfTargetObligationRecord *out) {
    sqlite3_stmt *s = NULL;
    const char *state;
    if (!o || !id || !out || sqlite3_prepare_v2(o->db,
        "SELECT target_id,target,semantic_contract,source_frontier,preference_policy,state,terminal_reason "
        "FROM target_obligations WHERE target_id=?", -1, &s, NULL) != SQLITE_OK) return -1;
    bind_text(s, 1, id);
    if (sqlite3_step(s) != SQLITE_ROW) { sqlite3_finalize(s); return 1; }
    memset(out, 0, sizeof(*out));
    copy_column(out->target_id, sizeof(out->target_id), s, 0);
    copy_column(out->target, sizeof(out->target), s, 1);
    copy_column(out->semantic_contract, sizeof(out->semantic_contract), s, 2);
    copy_column(out->source_frontier, sizeof(out->source_frontier), s, 3);
    copy_column(out->preference_policy, sizeof(out->preference_policy), s, 4);
    state = (const char *)sqlite3_column_text(s, 5);
    out->state = parse_state(state ? state : "OPEN");
    copy_column(out->terminal_reason, sizeof(out->terminal_reason), s, 6);
    sqlite3_finalize(s);
    return 0;
}

int bf_target_obligation_list_predicates(BfTargetObligation *o, const char *id,
                                         BfTargetPredicateRecord **out, size_t *count) {
    sqlite3_stmt *s = NULL;
    BfTargetPredicateRecord *rows = NULL;
    size_t n = 0, cap = 0;
    if (!o || !id || !out || !count) return -1;
    *out = NULL; *count = 0;
    if (sqlite3_prepare_v2(o->db,
        "SELECT predicate_id,owner,subject,predicate,expected_state,valid_start,valid_end,"
        "evidence_requirement,authority_requirement,adapter,native_state,owner_query "
        "FROM target_predicate_refs WHERE target_id=? ORDER BY predicate_id", -1, &s, NULL) != SQLITE_OK) return -1;
    bind_text(s, 1, id);
    while (sqlite3_step(s) == SQLITE_ROW) {
        BfTargetPredicateRecord *r;
        if (n == cap) {
            size_t next = cap ? cap * 2 : 8;
            void *grown = realloc(rows, next * sizeof(*rows));
            if (!grown) { free(rows); sqlite3_finalize(s); return -1; }
            rows = grown; cap = next;
        }
        r = &rows[n++]; memset(r, 0, sizeof(*r));
        copy_column(r->predicate_id, sizeof(r->predicate_id), s, 0);
        copy_column(r->owner, sizeof(r->owner), s, 1);
        copy_column(r->subject, sizeof(r->subject), s, 2);
        copy_column(r->predicate, sizeof(r->predicate), s, 3);
        copy_column(r->expected_state, sizeof(r->expected_state), s, 4);
        copy_column(r->valid_start, sizeof(r->valid_start), s, 5);
        copy_column(r->valid_end, sizeof(r->valid_end), s, 6);
        copy_column(r->evidence_requirement, sizeof(r->evidence_requirement), s, 7);
        copy_column(r->authority_requirement, sizeof(r->authority_requirement), s, 8);
        r->adapter = (uint8_t)sqlite3_column_int(s, 9);
        r->native_state = (uint8_t)sqlite3_column_int(s, 10);
        r->owner_query = (uint16_t)sqlite3_column_int(s, 11);
    }
    sqlite3_finalize(s); *out = rows; *count = n;
    return n ? 0 : 1;
}
void bf_target_obligation_free_predicates(BfTargetPredicateRecord *p){free(p);}
static int close_target(BfTargetObligation *o,const char*id,BfTargetState wanted,const char*detail){sqlite3_stmt*s=NULL;BfTargetObligationRecord r;if(!o||!text_ok(id,128)||!text_ok(detail,512)||bf_target_obligation_get(o,id,&r)!=0||terminal(r.state))return -1;if(sqlite3_prepare_v2(o->db,"UPDATE target_obligations SET state=?,terminal_reason=?,updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE target_id=?",-1,&s,NULL)!=SQLITE_OK)return -1;bind_text(s,1,bf_target_obligation_state_name(wanted));bind_text(s,2,detail);bind_text(s,3,id);if(sqlite3_step(s)!=SQLITE_DONE){sqlite3_finalize(s);return -1;}sqlite3_finalize(s);if(sqlite3_prepare_v2(o->db,"INSERT INTO target_obligation_events(target_id,event_kind,detail) VALUES(?,?,?)",-1,&s,NULL)!=SQLITE_OK)return -1;bind_text(s,1,id);bind_text(s,2,wanted==BF_TARGET_PROVEN?"PROVEN":"SUPERSEDED");bind_text(s,3,detail);if(sqlite3_step(s)!=SQLITE_DONE){sqlite3_finalize(s);return -1;}sqlite3_finalize(s);return 0;}
int bf_target_obligation_prove(BfTargetObligation *o,const char*id,const char*receipt){return close_target(o,id,BF_TARGET_PROVEN,receipt);}
int bf_target_obligation_supersede(BfTargetObligation *o,const char*id,const char*reason){return close_target(o,id,BF_TARGET_SUPERSEDED,reason);}
