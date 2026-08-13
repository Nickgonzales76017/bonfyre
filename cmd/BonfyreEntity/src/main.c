/*
 * bonfyre-entity — universal identity resolution layer.
 *
 * Links cross-modal observations (face, voice, text mention, email, location)
 * into a unified entity graph with confidence scores and temporal evolution.
 * Every pipeline that produces names, faces, voices, or references feeds into
 * this graph; downstream stages query it to resolve ambiguous references.
 *
 * DB: ~/.local/share/bonfyre/entity.db  (override: $BONFYRE_ENTITY_DB)
 *
 * Commands:
 *   bonfyre-entity status                        — entity + link counts
 *   bonfyre-entity resolve <modal> <value>       — resolve observation to entity
 *   bonfyre-entity show <entity-id>              — full entity record + links
 *   bonfyre-entity link <e1> <e2> <confidence>   — manually link two entities
 *   bonfyre-entity unlink <e1> <e2>              — remove a link
 *   bonfyre-entity merge <e1> <e2>               — merge e2 into e1
 *   bonfyre-entity search <query>                — full-text search entities
 *   bonfyre-entity history <entity-id>           — show observation timeline
 *   bonfyre-entity add <file.json>               — ingest entity manifest
 *   bonfyre-entity help                          — this message
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <sqlite3.h>
#include <bonfyre.h>

#define VERSION    "1.0.0"
#define DB_ENV     "BONFYRE_ENTITY_DB"
#define DB_SUBPATH "/.local/share/bonfyre/entity.db"

static void db_path(char *buf, size_t len) {
    const char *e = getenv(DB_ENV);
    if (e) { snprintf(buf, len, "%s", e); return; }
    const char *h = getenv("HOME"); if (!h) h="/tmp";
    snprintf(buf, len, "%s%s", h, DB_SUBPATH);
}
static void ensure_dir(const char *p) { bf_ensure_parent_dir(p); }

static const char *SCHEMA =
    "PRAGMA journal_mode=WAL;"
    "PRAGMA foreign_keys=ON;"
    "CREATE TABLE IF NOT EXISTS entities ("
    "  id          TEXT PRIMARY KEY,"
    "  display     TEXT NOT NULL,"
    "  kind        TEXT NOT NULL DEFAULT 'person'," /* person|place|org|object|event */
    "  confidence  REAL NOT NULL DEFAULT 1.0,"
    "  obs_count   INTEGER NOT NULL DEFAULT 0,"
    "  first_seen  INTEGER NOT NULL,"
    "  last_seen   INTEGER NOT NULL"
    ");"
    "CREATE TABLE IF NOT EXISTS observations ("
    "  id        INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  entity    TEXT NOT NULL REFERENCES entities(id),"
    "  modal     TEXT NOT NULL," /* face|voice|text|email|location|tag */
    "  value     TEXT NOT NULL,"
    "  source    TEXT,"
    "  confidence REAL NOT NULL DEFAULT 1.0,"
    "  ts        INTEGER NOT NULL"
    ");"
    "CREATE TABLE IF NOT EXISTS links ("
    "  a           TEXT NOT NULL REFERENCES entities(id),"
    "  b           TEXT NOT NULL REFERENCES entities(id),"
    "  confidence  REAL NOT NULL DEFAULT 1.0,"
    "  reason      TEXT,"
    "  created     INTEGER NOT NULL,"
    "  PRIMARY KEY(a,b)"
    ");"
    "CREATE VIRTUAL TABLE IF NOT EXISTS entities_fts USING fts5("
    "  id, display, kind, content='entities', content_rowid='rowid'"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_obs_entity ON observations(entity);"
    "CREATE INDEX IF NOT EXISTS idx_obs_modal  ON observations(modal,value);";

static sqlite3 *open_db(void) {
    char path[4096]; db_path(path,sizeof(path)); ensure_dir(path);
    sqlite3 *db=NULL;
    if (bf_sqlite3_open(path,&db)!=SQLITE_OK){fprintf(stderr,"db error\n");exit(1);}
    char *err=NULL; sqlite3_exec(db,SCHEMA,NULL,NULL,&err);
    if (err){fprintf(stderr,"%s\n",err);sqlite3_free(err);exit(1);}
    return db;
}

static void cmd_status(sqlite3 *db) {
    int ne=0, no=0, nl=0;
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM entities",-1,&st,NULL);
    if(sqlite3_step(st)==SQLITE_ROW) ne=sqlite3_column_int(st,0); sqlite3_finalize(st);
    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM observations",-1,&st,NULL);
    if(sqlite3_step(st)==SQLITE_ROW) no=sqlite3_column_int(st,0); sqlite3_finalize(st);
    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM links",-1,&st,NULL);
    if(sqlite3_step(st)==SQLITE_ROW) nl=sqlite3_column_int(st,0); sqlite3_finalize(st);
    printf("bonfyre-entity %s\n",VERSION);
    printf("  entities    : %d\n",ne);
    printf("  observations: %d\n",no);
    printf("  links       : %d\n",nl);
}

/* resolve: find entity for modal+value, or create new */
static void cmd_resolve(sqlite3 *db, const char *modal, const char *value) {
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "SELECT e.id,e.display,o.confidence FROM observations o"
        " JOIN entities e ON e.id=o.entity"
        " WHERE o.modal=? AND o.value=? ORDER BY o.confidence DESC LIMIT 1",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,modal,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,value,-1,SQLITE_STATIC);
    if (sqlite3_step(st)==SQLITE_ROW) {
        printf("resolved: %s → entity=%s (%s) confidence=%.3f\n",
            value, (const char*)sqlite3_column_text(st,0),
            (const char*)sqlite3_column_text(st,1),
            sqlite3_column_double(st,2));
        sqlite3_finalize(st);
        return;
    }
    sqlite3_finalize(st);
    /* create new entity */
    time_t now=time(NULL);
    char eid[64]; snprintf(eid,sizeof(eid),"entity-%ld-%s",(long)now,modal);
    sqlite3_prepare_v2(db,
        "INSERT OR IGNORE INTO entities(id,display,kind,confidence,obs_count,first_seen,last_seen)"
        " VALUES(?,?,?,1.0,1,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,eid,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,value,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,"person",-1,SQLITE_STATIC);
    sqlite3_bind_int64(st,4,(sqlite3_int64)now);
    sqlite3_bind_int64(st,5,(sqlite3_int64)now);
    sqlite3_step(st); sqlite3_finalize(st);
    /* add observation */
    sqlite3_prepare_v2(db,
        "INSERT INTO observations(entity,modal,value,confidence,ts) VALUES(?,?,?,1.0,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,eid,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,modal,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,value,-1,SQLITE_STATIC);
    sqlite3_bind_int64(st,4,(sqlite3_int64)now);
    sqlite3_step(st); sqlite3_finalize(st);
    printf("new entity: %s → id=%s (first observation via %s)\n", value, eid, modal);
}

static void cmd_show(sqlite3 *db, const char *eid) {
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "SELECT id,display,kind,confidence,obs_count,first_seen,last_seen FROM entities WHERE id=?",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,eid,-1,SQLITE_STATIC);
    if (sqlite3_step(st)!=SQLITE_ROW) {
        fprintf(stderr,"entity not found: %s\n",eid); sqlite3_finalize(st); return;
    }
    char fs[20],ls[20];
    time_t t1=(time_t)sqlite3_column_int64(st,5),t2=(time_t)sqlite3_column_int64(st,6);
    strftime(fs,sizeof(fs),"%Y-%m-%d",localtime(&t1));
    strftime(ls,sizeof(ls),"%Y-%m-%d",localtime(&t2));
    printf("id          : %s\n",(const char*)sqlite3_column_text(st,0));
    printf("display     : %s\n",(const char*)sqlite3_column_text(st,1));
    printf("kind        : %s\n",(const char*)sqlite3_column_text(st,2));
    printf("confidence  : %.3f\n",sqlite3_column_double(st,3));
    printf("observations: %d\n",sqlite3_column_int(st,4));
    printf("first seen  : %s\n",fs);
    printf("last seen   : %s\n",ls);
    sqlite3_finalize(st);
    /* observations */
    sqlite3_prepare_v2(db,
        "SELECT modal,value,confidence,ts FROM observations WHERE entity=? ORDER BY ts DESC LIMIT 10",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,eid,-1,SQLITE_STATIC);
    printf("\nobservations (latest 10):\n");
    while (sqlite3_step(st)==SQLITE_ROW) {
        time_t ts=(time_t)sqlite3_column_int64(st,3);
        char tb[20]; strftime(tb,sizeof(tb),"%Y-%m-%d %H:%M",localtime(&ts));
        printf("  [%s] %-10s %-40s conf=%.3f\n",
            tb,(const char*)sqlite3_column_text(st,0),
            (const char*)sqlite3_column_text(st,1),
            sqlite3_column_double(st,2));
    }
    sqlite3_finalize(st);
}

static void cmd_link(sqlite3 *db, const char *a, const char *b, double conf) {
    time_t now=time(NULL);
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO links(a,b,confidence,created) VALUES(?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,a,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,b,-1,SQLITE_STATIC);
    sqlite3_bind_double(st,3,conf);
    sqlite3_bind_int64(st,4,(sqlite3_int64)now);
    sqlite3_step(st); sqlite3_finalize(st);
    printf("linked: %s ↔ %s (confidence=%.3f)\n",a,b,conf);
}

static void cmd_unlink(sqlite3 *db, const char *a, const char *b) {
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"DELETE FROM links WHERE (a=? AND b=?) OR (a=? AND b=?)",-1,&st,NULL);
    sqlite3_bind_text(st,1,a,-1,SQLITE_STATIC); sqlite3_bind_text(st,2,b,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,b,-1,SQLITE_STATIC); sqlite3_bind_text(st,4,a,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);
    printf("unlinked: %s ↔ %s\n",a,b);
}

static void cmd_merge(sqlite3 *db, const char *target, const char *source) {
    /* Re-point all observations from source to target, then delete source */
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"UPDATE observations SET entity=? WHERE entity=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,target,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,source,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);
    int moved = sqlite3_changes(db);
    sqlite3_prepare_v2(db,"DELETE FROM entities WHERE id=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,source,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);
    printf("merged: %s → %s (%d observations moved)\n", source, target, moved);
}

static void cmd_search(sqlite3 *db, const char *q) {
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "SELECT id,display,kind,confidence FROM entities"
        " WHERE id LIKE ? OR display LIKE ? ORDER BY confidence DESC LIMIT 20",
        -1,&st,NULL);
    char pat[256]; snprintf(pat,sizeof(pat),"%%%s%%",q);
    sqlite3_bind_text(st,1,pat,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,pat,-1,SQLITE_STATIC);
    printf("%-36s  %-30s  %-10s  %s\n","ID","DISPLAY","KIND","CONF");
    while (sqlite3_step(st)==SQLITE_ROW)
        printf("%-36s  %-30s  %-10s  %.3f\n",
            (const char*)sqlite3_column_text(st,0),
            (const char*)sqlite3_column_text(st,1),
            (const char*)sqlite3_column_text(st,2),
            sqlite3_column_double(st,3));
    sqlite3_finalize(st);
}

static void cmd_history(sqlite3 *db, const char *eid) {
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "SELECT modal,value,confidence,source,ts FROM observations"
        " WHERE entity=? ORDER BY ts",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,eid,-1,SQLITE_STATIC);
    printf("observation timeline for: %s\n",eid);
    while (sqlite3_step(st)==SQLITE_ROW) {
        time_t ts=(time_t)sqlite3_column_int64(st,4);
        char tb[20]; strftime(tb,sizeof(tb),"%Y-%m-%d %H:%M",localtime(&ts));
        printf("  [%s] %-8s %-40s conf=%.3f%s%s\n",
            tb,(const char*)sqlite3_column_text(st,0),
            (const char*)sqlite3_column_text(st,1),
            sqlite3_column_double(st,2),
            sqlite3_column_text(st,3)?" src=":"",
            sqlite3_column_text(st,3)?(const char*)sqlite3_column_text(st,3):"");
    }
    sqlite3_finalize(st);
}

static void cmd_help(void) {
    printf(
"bonfyre-entity %s — universal identity resolution layer\n\n"
"USAGE\n"
"  bonfyre-entity <command> [args]\n\n"
"COMMANDS\n"
"  status                        entity + observation + link counts\n"
"  resolve <modal> <value>       map observation to entity (or create new)\n"
"  show <entity-id>              full entity record + recent observations\n"
"  link <e1> <e2> <conf>         manually link two entities\n"
"  unlink <e1> <e2>              remove a link\n"
"  merge <target> <source>       merge source into target\n"
"  search <query>                search entities by name or id\n"
"  history <entity-id>           full observation timeline\n"
"  help                          this message\n\n"
"MODALITIES\n"
"  face · voice · text · email · location · tag · fingerprint\n\n"
"INTEGRATION\n"
"  bonfyre-transcribe feeds speaker labels → bonfyre-entity resolve voice <label>\n"
"  bonfyre-tag feeds tag values → bonfyre-entity resolve text <value>\n"
"  bonfyre-time fires entity_resolved trigger when confidence crosses threshold\n\n"
"ENVIRONMENT\n"
"  BONFYRE_ENTITY_DB   override DB path\n",
    VERSION);
}

int main(int argc, char **argv) {
    if (argc<2||strcmp(argv[1],"help")==0||strcmp(argv[1],"--help")==0){cmd_help();return 0;}
    sqlite3 *db=open_db();
    int rc=0; const char *cmd=argv[1];
    if (strcmp(cmd,"status")==0) cmd_status(db);
    else if (strcmp(cmd,"resolve")==0){
        if(argc<4){fprintf(stderr,"usage: bonfyre-entity resolve <modal> <value>\n");rc=1;}
        else cmd_resolve(db,argv[2],argv[3]);
    } else if (strcmp(cmd,"show")==0){
        if(argc<3){fprintf(stderr,"usage: bonfyre-entity show <id>\n");rc=1;}
        else cmd_show(db,argv[2]);
    } else if (strcmp(cmd,"link")==0){
        if(argc<5){fprintf(stderr,"usage: bonfyre-entity link <e1> <e2> <conf>\n");rc=1;}
        else cmd_link(db,argv[2],argv[3],atof(argv[4]));
    } else if (strcmp(cmd,"unlink")==0){
        if(argc<4){fprintf(stderr,"usage: bonfyre-entity unlink <e1> <e2>\n");rc=1;}
        else cmd_unlink(db,argv[2],argv[3]);
    } else if (strcmp(cmd,"merge")==0){
        if(argc<4){fprintf(stderr,"usage: bonfyre-entity merge <target> <source>\n");rc=1;}
        else cmd_merge(db,argv[2],argv[3]);
    } else if (strcmp(cmd,"search")==0){
        if(argc<3){fprintf(stderr,"usage: bonfyre-entity search <query>\n");rc=1;}
        else cmd_search(db,argv[2]);
    } else if (strcmp(cmd,"history")==0){
        if(argc<3){fprintf(stderr,"usage: bonfyre-entity history <id>\n");rc=1;}
        else cmd_history(db,argv[2]);
    } else {
        fprintf(stderr,"bonfyre-entity: unknown command: %s\n",cmd); rc=1;
    }
    sqlite3_close(db); return rc;
}
