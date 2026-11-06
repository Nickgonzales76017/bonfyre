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
 *   bonfyre-entity observe <id> <modal> <value>  — attach an observation to an entity
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
#include "../../../lib/libbonfyre/include/bf_help.h"
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
    /* Durable tier -- identity links: who was resolved to whom. */
    if (bf_sqlite3_open_tier(path, &db, BF_SQLITE_DURABLE) != SQLITE_OK){fprintf(stderr,"db error\n");exit(1);}
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

/* resolve: find entity for modal+value, or create new.
 *
 * Identity is derived from the observation, never from the clock. The previous
 * implementation built the id as "entity-<unix_seconds>-<modal>", so any two
 * distinct values resolved inside the same second produced the same id; the
 * INSERT OR IGNORE then silently dropped the second entity and attached its
 * observation to the first. Twelve distinct property-owner companies collapsed
 * into a single entity, and `search` could not find any of them because the
 * display name only ever held the first value observed.
 *
 * The lookup is by derived id rather than by raw value so that the read path
 * and the create path cannot disagree: both normalize, so "BETA TRUST" and
 * "beta trust" reach the same entity instead of forking one identity in two.
 * Every resolve records an observation, which is what makes `history` a real
 * timeline rather than a single first sighting.
 */
static void entity_id_for(char *out, size_t out_sz, const char *modal, const char *value) {
    char norm[192]; bf_normalize_token(norm, sizeof(norm), value);
    char key[288];  snprintf(key, sizeof(key), "%s|%s", modal, norm);
    char hex[65];   bf_sha256_hex((const uint8_t *)key, strlen(key), hex);
    snprintf(out, out_sz, "entity-%s-%.16s", modal, hex);
}

/* kind: the schema already allows person|place|org|object|event, but resolve
 * hardcoded "person" for everything. An organisation recorded as a person is
 * a false label in the graph, and every reader downstream inherits it, so the
 * caller states the kind and "person" is only the default. */

/* observe: attach a further observation to an entity already in the graph.
 *
 * resolve keys an entity by the modal and value that first named it, so a
 * second fact about the same thing -- the identifier a register holds for it,
 * where it is, what a source called it -- would otherwise mint a second
 * entity rather than describe the first. That is the difference between an
 * identity graph and a list of strings.
 *
 * The source is written down. The column existed and nothing ever filled it,
 * so every observation in this graph was unattributable: a reader could see
 * that something had been claimed and never who claimed it. Confidence is the
 * caller's, stated rather than assumed, because the caller is the only one who
 * knows how strong the evidence was.
 */
static void cmd_observe(sqlite3 *db, const char *eid, const char *modal,
                        const char *value, const char *source, double confidence) {
    sqlite3_stmt *st = NULL;
    time_t now = time(NULL);
    int known = 0;

    sqlite3_prepare_v2(db, "SELECT 1 FROM entities WHERE id=?", -1, &st, NULL);
    sqlite3_bind_text(st, 1, eid, -1, SQLITE_STATIC);
    known = (sqlite3_step(st) == SQLITE_ROW);
    sqlite3_finalize(st);
    if (!known) {
        /* An observation about nothing is not an observation. Saying so beats
         * inventing the entity it would have described. */
        fprintf(stderr, "bonfyre-entity: no entity %s; resolve it first\n", eid);
        return;
    }

    sqlite3_prepare_v2(db,
        "INSERT INTO observations(entity,modal,value,source,confidence,ts)"
        " VALUES(?,?,?,?,?,?)", -1, &st, NULL);
    sqlite3_bind_text(st, 1, eid, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, modal, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, value, -1, SQLITE_STATIC);
    if (source && *source) sqlite3_bind_text(st, 4, source, -1, SQLITE_STATIC);
    else sqlite3_bind_null(st, 4);
    sqlite3_bind_double(st, 5, confidence);
    sqlite3_bind_int64(st, 6, (sqlite3_int64)now);
    sqlite3_step(st); sqlite3_finalize(st);

    sqlite3_prepare_v2(db,
        "UPDATE entities SET obs_count=(SELECT COUNT(*) FROM observations WHERE entity=?1),"
        " last_seen=?2 WHERE id=?1", -1, &st, NULL);
    sqlite3_bind_text(st, 1, eid, -1, SQLITE_STATIC);
    sqlite3_bind_int64(st, 2, (sqlite3_int64)now);
    sqlite3_step(st); sqlite3_finalize(st);

    printf("{\"entity\":\"%s\",\"modal\":\"%s\",\"observed\":\"%s\",\"source\":\"%s\","
           "\"confidence\":%.3f}\n", eid, modal, value, source && *source ? source : "", confidence);
}

/* `confidence` inside this function is the entity's own, read from its row.
 * What the caller passes is how sure *this observation* is, which is a
 * different number and must not overwrite it. */
static void cmd_resolve(sqlite3 *db, const char *modal, const char *value,
                        const char *kind, const char *source, double observed) {
    sqlite3_stmt *st = NULL;
    time_t now = time(NULL);
    char eid[64]; entity_id_for(eid, sizeof(eid), modal, value);

    int existed = 0;
    char display[256] = {0};
    double confidence = 1.0;
    sqlite3_prepare_v2(db, "SELECT display,confidence FROM entities WHERE id=?", -1, &st, NULL);
    sqlite3_bind_text(st, 1, eid, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) {
        existed = 1;
        snprintf(display, sizeof(display), "%s", (const char *)sqlite3_column_text(st, 0));
        confidence = sqlite3_column_double(st, 1);
    }
    sqlite3_finalize(st);

    if (!existed) {
        sqlite3_prepare_v2(db,
            "INSERT INTO entities(id,display,kind,confidence,obs_count,first_seen,last_seen)"
            " VALUES(?,?,?,1.0,0,?,?)", -1, &st, NULL);
        sqlite3_bind_text(st, 1, eid, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, value, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 3, (kind && *kind) ? kind : "person", -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 4, (sqlite3_int64)now);
        sqlite3_bind_int64(st, 5, (sqlite3_int64)now);
        sqlite3_step(st); sqlite3_finalize(st);
    }

    sqlite3_prepare_v2(db,
        "INSERT INTO observations(entity,modal,value,source,confidence,ts) VALUES(?,?,?,?,?,?)",
        -1, &st, NULL);
    sqlite3_bind_text(st, 1, eid, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, modal, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, value, -1, SQLITE_STATIC);
    if (source && *source) sqlite3_bind_text(st, 4, source, -1, SQLITE_STATIC);
    else sqlite3_bind_null(st, 4);
    sqlite3_bind_double(st, 5, observed);
    sqlite3_bind_int64(st, 6, (sqlite3_int64)now);
    sqlite3_step(st); sqlite3_finalize(st);

    /* obs_count is maintained here rather than left at its inserted value; it
     * previously stayed at 1 while `show` listed every observation beneath it. */
    sqlite3_prepare_v2(db,
        "UPDATE entities SET obs_count=(SELECT COUNT(*) FROM observations WHERE entity=?),"
        " last_seen=? WHERE id=?", -1, &st, NULL);
    sqlite3_bind_text(st, 1, eid, -1, SQLITE_STATIC);
    sqlite3_bind_int64(st, 2, (sqlite3_int64)now);
    sqlite3_bind_text(st, 3, eid, -1, SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);

    if (existed)
        printf("resolved: %s \u2192 entity=%s (%s) confidence=%.3f\n",
               value, eid, display, confidence);
    else
        printf("new entity: %s \u2192 id=%s (first observation via %s)\n", value, eid, modal);
}

static int entity_exists(sqlite3 *db, const char *eid) {
    sqlite3_stmt *st=NULL; int found=0;
    if (sqlite3_prepare_v2(db,"SELECT 1 FROM entities WHERE id=?",-1,&st,NULL)!=SQLITE_OK) return 0;
    sqlite3_bind_text(st,1,eid,-1,SQLITE_STATIC);
    found = sqlite3_step(st)==SQLITE_ROW;
    sqlite3_finalize(st); return found;
}

static int cmd_show(sqlite3 *db, const char *eid) {
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "SELECT id,display,kind,confidence,obs_count,first_seen,last_seen FROM entities WHERE id=?",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,eid,-1,SQLITE_STATIC);
    if (sqlite3_step(st)!=SQLITE_ROW) {
        fprintf(stderr,"entity not found: %s\n",eid); sqlite3_finalize(st); return 1;
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
    return 0;
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

static int cmd_merge(sqlite3 *db, const char *target, const char *source) {
    /* Re-point observations and links from source to target, then delete
     * source, as one transaction. Links used to be left behind, so the
     * foreign key refused the delete and the source survived its merge. */
    if (!strcmp(target, source)) { fprintf(stderr,"bonfyre-entity: cannot merge an entity into itself\n"); return 1; }
    static const char *sql[] = {
        "UPDATE observations SET entity=?1 WHERE entity=?2",
        "INSERT OR IGNORE INTO links(a,b,confidence,reason,created)"
        " SELECT CASE WHEN a=?2 THEN ?1 ELSE a END, CASE WHEN b=?2 THEN ?1 ELSE b END, confidence, reason, created"
        " FROM links WHERE (a=?2 OR b=?2) AND NOT (a IN (?1,?2) AND b IN (?1,?2))",
        "DELETE FROM links WHERE a=?2 OR b=?2",
        "UPDATE entities SET obs_count=(SELECT COUNT(*) FROM observations WHERE entity=?1) WHERE id=?1",
        "DELETE FROM entities WHERE id=?2",
    };
    int moved = 0, ok = sqlite3_exec(db,"BEGIN IMMEDIATE",NULL,NULL,NULL)==SQLITE_OK;
    for (size_t i = 0; ok && i < sizeof sql / sizeof *sql; i++) {
        sqlite3_stmt *st=NULL;
        ok = sqlite3_prepare_v2(db,sql[i],-1,&st,NULL)==SQLITE_OK;
        if (ok) {
            sqlite3_bind_text(st,1,target,-1,SQLITE_STATIC);
            if (sqlite3_bind_parameter_count(st) >= 2) sqlite3_bind_text(st,2,source,-1,SQLITE_STATIC);
            ok = sqlite3_step(st)==SQLITE_DONE;
            if (ok && i == 0) moved = sqlite3_changes(db);
            if (ok && i == 4) ok = sqlite3_changes(db)==1;
        }
        sqlite3_finalize(st);
    }
    if (!ok) {
        fprintf(stderr,"bonfyre-entity: merge failed: %s\n", sqlite3_errmsg(db));
        sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);
        return 1;
    }
    sqlite3_exec(db,"COMMIT",NULL,NULL,NULL);
    printf("merged: %s → %s (%d observations moved)\n", source, target, moved);
    return 0;
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
    if (bf_help_requested(argc, argv)) { cmd_help(); return 0; }
    if (argc<2){cmd_help();return 0;}
    sqlite3 *db=open_db();
    int rc=0; const char *cmd=argv[1];
    if (strcmp(cmd,"status")==0) cmd_status(db);
    else if (strcmp(cmd,"resolve")==0){
        if(argc<4){fprintf(stderr,"usage: bonfyre-entity resolve <modal> <value> [--kind person|place|org|object|event] [--source S] [--confidence C]\n");rc=1;}
        else {
            const char *kind = NULL, *source = NULL;
            double confidence = 1.0;
            for (int i = 4; i + 1 < argc; i++) {
                if (strcmp(argv[i], "--kind") == 0) kind = argv[i + 1];
                else if (strcmp(argv[i], "--source") == 0) source = argv[i + 1];
                else if (strcmp(argv[i], "--confidence") == 0) confidence = atof(argv[i + 1]);
            }
            if (kind && strcmp(kind,"person") && strcmp(kind,"place") && strcmp(kind,"org") &&
                strcmp(kind,"object") && strcmp(kind,"event")) {
                fprintf(stderr,"bonfyre-entity: kind must be person|place|org|object|event\n"); rc=1;
            } else cmd_resolve(db,argv[2],argv[3],kind,source,confidence);
        }
    } else if (strcmp(cmd,"observe")==0){
        if(argc<5){fprintf(stderr,"usage: bonfyre-entity observe <entity-id> <modal> <value>"
                                  " [--source S] [--confidence C]\n");rc=1;}
        else {
            const char *source = NULL; double confidence = 1.0;
            for (int i = 5; i + 1 < argc; i++) {
                if (strcmp(argv[i], "--source") == 0) source = argv[i + 1];
                else if (strcmp(argv[i], "--confidence") == 0) confidence = atof(argv[i + 1]);
            }
            cmd_observe(db,argv[2],argv[3],argv[4],source,confidence);
        }
    } else if (strcmp(cmd,"show")==0){
        if(argc<3){fprintf(stderr,"usage: bonfyre-entity show <id>\n");rc=1;}
        else rc=cmd_show(db,argv[2]);
    } else if (strcmp(cmd,"link")==0){
        if(argc<5){fprintf(stderr,"usage: bonfyre-entity link <e1> <e2> <conf>\n");rc=1;}
        else if(!entity_exists(db,argv[2])||!entity_exists(db,argv[3])){fprintf(stderr,"bonfyre-entity: link needs two known entities\n");rc=1;}
        else if(!(atof(argv[4])>=0.0&&atof(argv[4])<=1.0)){fprintf(stderr,"bonfyre-entity: link confidence must be within [0,1]\n");rc=1;}
        else cmd_link(db,argv[2],argv[3],atof(argv[4]));
    } else if (strcmp(cmd,"unlink")==0){
        if(argc<4){fprintf(stderr,"usage: bonfyre-entity unlink <e1> <e2>\n");rc=1;}
        else cmd_unlink(db,argv[2],argv[3]);
    } else if (strcmp(cmd,"merge")==0){
        if(argc<4){fprintf(stderr,"usage: bonfyre-entity merge <target> <source>\n");rc=1;}
        else if(!entity_exists(db,argv[2])||!entity_exists(db,argv[3])){fprintf(stderr,"bonfyre-entity: merge needs two known entities\n");rc=1;}
        else rc=cmd_merge(db,argv[2],argv[3]);
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
