/*
 * bonfyre-space — shared memory substrate.
 *
 * Named memory spaces that pipeline stages can attach to and read/write
 * without writing intermediate files. A space is a key-value store backed by
 * SQLite WAL. Stages attach to a named space by run-id, write their outputs,
 * and downstream stages read directly — no temp files, no copies.
 *
 * DB: ~/.local/share/bonfyre/space.db  (override: $BONFYRE_SPACE_DB)
 *
 * Commands:
 *   bonfyre-space status              — list open spaces + sizes
 *   bonfyre-space open <name>         — open (or create) a named space
 *   bonfyre-space put <space> <key> <value>   — write a key
 *   bonfyre-space get <space> <key>   — read a key
 *   bonfyre-space list [space]        — list keys (optionally in a space)
 *   bonfyre-space stats <space>       — size, key count, age
 *   bonfyre-space gc [--older-than N] — remove stale spaces
 *   bonfyre-space attach <space> <stage>   — mark a stage as attached
 *   bonfyre-space detach <space> <stage>   — mark detached
 *   bonfyre-space help                — this message
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <sqlite3.h>
#include <bonfyre.h>

#define VERSION    "1.0.0"
#define DB_ENV     "BONFYRE_SPACE_DB"
#define DB_SUBPATH "/.local/share/bonfyre/space.db"

static void db_path(char *buf,size_t len){
    const char *e=getenv(DB_ENV);
    if(e){snprintf(buf,len,"%s",e);return;}
    const char *h=getenv("HOME");if(!h)h="/tmp";
    snprintf(buf,len,"%s%s",h,DB_SUBPATH);
}
static void ensure_dir(const char *p) { bf_ensure_parent_dir(p); }
static void ensure_parent_dir(const char *path) {
    char tmp[4096];
    char *slash;
    if (!path || !path[0]) return;
    snprintf(tmp, sizeof(tmp), "%s", path);
    slash = strrchr(tmp, '/');
    if (!slash) return;
    *slash = '\0';
    if (tmp[0]) ensure_dir(tmp);
}

static const char *SCHEMA=
    "PRAGMA journal_mode=WAL;"
    "CREATE TABLE IF NOT EXISTS spaces("
    "  name      TEXT PRIMARY KEY,"
    "  created   INTEGER NOT NULL,"
    "  accessed  INTEGER NOT NULL,"
    "  mode      TEXT NOT NULL DEFAULT 'rw'" /* rw|ro|append */
    ");"
    "CREATE TABLE IF NOT EXISTS entries("
    "  space     TEXT NOT NULL REFERENCES spaces(name) ON DELETE CASCADE,"
    "  key       TEXT NOT NULL,"
    "  value     BLOB NOT NULL,"
    "  written   INTEGER NOT NULL,"
    "  ttl_s     INTEGER,"
    "  PRIMARY KEY(space,key)"
    ");"
    "CREATE TABLE IF NOT EXISTS attachments("
    "  space     TEXT NOT NULL REFERENCES spaces(name) ON DELETE CASCADE,"
    "  stage     TEXT NOT NULL,"
    "  attached  INTEGER NOT NULL,"
    "  detached  INTEGER,"
    "  PRIMARY KEY(space,stage)"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_entries_space ON entries(space);";

static sqlite3 *open_db(void){
    char path[4096];db_path(path,sizeof(path));ensure_parent_dir(path);
    sqlite3 *db=NULL;
    if(bf_sqlite3_open(path,&db)!=SQLITE_OK){
        fprintf(stderr,"db error: %s (%s)\n", path, db ? sqlite3_errmsg(db) : "open failed");
        if (db) sqlite3_close(db);
        exit(1);
    }
    char *err=NULL;sqlite3_exec(db,SCHEMA,NULL,NULL,&err);
    if(err){fprintf(stderr,"%s\n",err);sqlite3_free(err);exit(1);}
    return db;
}

static void cmd_status(sqlite3 *db){
    sqlite3_stmt *st=NULL;
    printf("%-30s  %8s  %8s  %s\n","SPACE","SIZE(B)","KEYS","LAST ACCESS");
    sqlite3_prepare_v2(db,
        "SELECT s.name, COALESCE(SUM(LENGTH(e.value)),0), COUNT(e.key), s.accessed"
        " FROM spaces s LEFT JOIN entries e ON e.space=s.name GROUP BY s.name",
        -1,&st,NULL);
    while(sqlite3_step(st)==SQLITE_ROW){
        time_t ts=(time_t)sqlite3_column_int64(st,3);
        char tb[20];strftime(tb,sizeof(tb),"%Y-%m-%d %H:%M",localtime(&ts));
        printf("%-30s  %8lld  %8d  %s\n",
            (const char*)sqlite3_column_text(st,0),
            (long long)sqlite3_column_int64(st,1),sqlite3_column_int(st,2),tb);
    }
    sqlite3_finalize(st);
}

static void cmd_open(sqlite3 *db,const char *name){
    time_t now=time(NULL);
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "INSERT OR IGNORE INTO spaces(name,created,accessed) VALUES(?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,name,-1,SQLITE_STATIC);
    sqlite3_bind_int64(st,2,(sqlite3_int64)now);
    sqlite3_bind_int64(st,3,(sqlite3_int64)now);
    sqlite3_step(st);sqlite3_finalize(st);
    printf("space '%s' open\n",name);
}

static void cmd_put(sqlite3 *db,const char *space,const char *key,const char *val){
    /* auto-create space if needed */
    time_t now=time(NULL);
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO spaces(name,created,accessed) VALUES(?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,space,-1,SQLITE_STATIC);
    sqlite3_bind_int64(st,2,(sqlite3_int64)now);
    sqlite3_bind_int64(st,3,(sqlite3_int64)now);
    sqlite3_step(st);sqlite3_finalize(st);
    sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO entries(space,key,value,written) VALUES(?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,space,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,key,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,val,-1,SQLITE_STATIC);
    sqlite3_bind_int64(st,4,(sqlite3_int64)now);
    sqlite3_step(st);sqlite3_finalize(st);
    /* update accessed */
    sqlite3_prepare_v2(db,"UPDATE spaces SET accessed=? WHERE name=?",-1,&st,NULL);
    sqlite3_bind_int64(st,1,(sqlite3_int64)now);
    sqlite3_bind_text(st,2,space,-1,SQLITE_STATIC);
    sqlite3_step(st);sqlite3_finalize(st);
    printf("written: %s/%s (%zu bytes)\n",space,key,strlen(val));
}

static void cmd_get(sqlite3 *db,const char *space,const char *key){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"SELECT value FROM entries WHERE space=? AND key=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,space,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,key,-1,SQLITE_STATIC);
    if(sqlite3_step(st)!=SQLITE_ROW){
        fprintf(stderr,"not found: %s/%s\n",space,key);sqlite3_finalize(st);return;
    }
    printf("%s\n",(const char*)sqlite3_column_text(st,0));
    sqlite3_finalize(st);
}

static void cmd_list(sqlite3 *db,const char *space){
    sqlite3_stmt *st=NULL;
    if(space){
        sqlite3_prepare_v2(db,
            "SELECT key,LENGTH(value),written FROM entries WHERE space=? ORDER BY key",
            -1,&st,NULL);
        sqlite3_bind_text(st,1,space,-1,SQLITE_STATIC);
    } else {
        sqlite3_prepare_v2(db,
            "SELECT space||'/'||key,LENGTH(value),written FROM entries ORDER BY space,key",
            -1,&st,NULL);
    }
    printf("%-50s  %8s  %s\n","KEY","SIZE(B)","WRITTEN");
    while(sqlite3_step(st)==SQLITE_ROW){
        time_t ts=(time_t)sqlite3_column_int64(st,2);
        char tb[20];strftime(tb,sizeof(tb),"%Y-%m-%d %H:%M",localtime(&ts));
        printf("%-50s  %8d  %s\n",
            (const char*)sqlite3_column_text(st,0),sqlite3_column_int(st,1),tb);
    }
    sqlite3_finalize(st);
}

static void cmd_stats(sqlite3 *db,const char *space){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "SELECT COUNT(*),COALESCE(SUM(LENGTH(value)),0),MIN(written),MAX(written)"
        " FROM entries WHERE space=?",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,space,-1,SQLITE_STATIC);
    if(sqlite3_step(st)==SQLITE_ROW){
        time_t t1=(time_t)sqlite3_column_int64(st,2),t2=(time_t)sqlite3_column_int64(st,3);
        char a[20],b[20];
        strftime(a,sizeof(a),"%Y-%m-%d %H:%M",localtime(&t1));
        strftime(b,sizeof(b),"%Y-%m-%d %H:%M",localtime(&t2));
        printf("space     : %s\nkeys      : %d\nsize      : %lld bytes\nfirst     : %s\nlast      : %s\n",
            space,sqlite3_column_int(st,0),(long long)sqlite3_column_int64(st,1),a,b);
    }
    sqlite3_finalize(st);
}

static void cmd_gc(sqlite3 *db,int older_than_s){
    time_t cutoff=time(NULL)-(time_t)older_than_s;
    sqlite3_stmt *st=NULL;
    /* delete spaces with no recent entries */
    sqlite3_prepare_v2(db,"DELETE FROM spaces WHERE accessed<?",-1,&st,NULL);
    sqlite3_bind_int64(st,1,(sqlite3_int64)cutoff);
    sqlite3_step(st);sqlite3_finalize(st);
    int removed=sqlite3_changes(db);
    printf("gc: removed %d stale spaces (older than %d seconds)\n",removed,older_than_s);
}

static void cmd_attach(sqlite3 *db,const char *space,const char *stage){
    time_t now=time(NULL);
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO attachments(space,stage,attached) VALUES(?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,space,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,stage,-1,SQLITE_STATIC);
    sqlite3_bind_int64(st,3,(sqlite3_int64)now);
    sqlite3_step(st);sqlite3_finalize(st);
    printf("attached: stage '%s' → space '%s'\n",stage,space);
}

static void cmd_detach(sqlite3 *db,const char *space,const char *stage){
    time_t now=time(NULL);
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"UPDATE attachments SET detached=? WHERE space=? AND stage=?",-1,&st,NULL);
    sqlite3_bind_int64(st,1,(sqlite3_int64)now);
    sqlite3_bind_text(st,2,space,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,stage,-1,SQLITE_STATIC);
    sqlite3_step(st);sqlite3_finalize(st);
    printf("detached: stage '%s' from space '%s'\n",stage,space);
}

static void cmd_help(void){
    printf(
"bonfyre-space %s — shared memory substrate\n\n"
"USAGE\n"
"  bonfyre-space <command> [args]\n\n"
"COMMANDS\n"
"  status                        list all open spaces with size + access time\n"
"  open <name>                   open (or create) a named space\n"
"  put <space> <key> <value>     write a key to a space\n"
"  get <space> <key>             read a key from a space\n"
"  list [space]                  list keys (all, or within one space)\n"
"  stats <space>                 key count, total size, timestamps\n"
"  gc [--older-than <seconds>]   remove stale spaces (default: 86400)\n"
"  attach <space> <stage>        record that a pipeline stage is attached\n"
"  detach <space> <stage>        mark stage as detached\n"
"  help                          this message\n\n"
"INTEGRATION\n"
"  bonfyre-run uses spaces for inter-stage artifact passing\n"
"  bonfyre-flow reads branching state from named spaces\n"
"  bonfyre-compete writes variant outputs to separate space keys\n\n"
"ENVIRONMENT\n"
"  BONFYRE_SPACE_DB    override DB path\n",
    VERSION);
}

static int cmd_fixture(const char *path) {
    sqlite3 *db;
    sqlite3_stmt *st = NULL;
    int put = 0, isolated = 0, handoff = 0, restart = 0;
    if (!path || setenv(DB_ENV, path, 1) != 0) return 1;
    db = open_db();
    cmd_put(db, "fixture-project", "source", "governed-artifact");
    cmd_put(db, "fixture-project", "handoff", "downstream-stage");
    cmd_put(db, "other-project", "source", "isolated-artifact");
    cmd_attach(db, "fixture-project", "worker-a");
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM entries WHERE space='fixture-project' AND key='source' AND value='governed-artifact'", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW)
        put = sqlite3_column_int(st, 0) == 1;
    sqlite3_finalize(st); st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM entries WHERE space='other-project' AND key='source' AND value='governed-artifact'", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW)
        isolated = sqlite3_column_int(st, 0) == 0;
    sqlite3_finalize(st); st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM entries WHERE space='fixture-project' AND key='handoff' AND value='downstream-stage'", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW)
        handoff = sqlite3_column_int(st, 0) == 1;
    sqlite3_finalize(st); sqlite3_close(db);
    db = open_db();
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM entries WHERE space='fixture-project' AND key='source'", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW)
        restart = sqlite3_column_int(st, 0) == 1;
    sqlite3_finalize(st); sqlite3_close(db);
    printf("{\"put\":%s,\"get\":%s,\"isolation\":%s,\"handoff\":%s,\"restart_persisted\":%s}\n",
           put ? "true" : "false", put ? "true" : "false", isolated ? "true" : "false",
           handoff ? "true" : "false", restart ? "true" : "false");
    return put && isolated && handoff && restart ? 0 : 1;
}

int main(int argc,char **argv){
    if(argc<2||strcmp(argv[1],"help")==0||strcmp(argv[1],"--help")==0){cmd_help();return 0;}
    if (strcmp(argv[1], "fixture") == 0) {
        if (argc != 3) { fprintf(stderr, "usage: bonfyre-space fixture <db>\n"); return 1; }
        return cmd_fixture(argv[2]);
    }
    sqlite3 *db=open_db();
    int rc=0;const char *cmd=argv[1];
    if(strcmp(cmd,"status")==0) cmd_status(db);
    else if(strcmp(cmd,"open")==0){
        if(argc<3){fprintf(stderr,"usage: bonfyre-space open <name>\n");rc=1;}
        else cmd_open(db,argv[2]);
    } else if(strcmp(cmd,"put")==0){
        if(argc<5){fprintf(stderr,"usage: bonfyre-space put <space> <key> <value>\n");rc=1;}
        else cmd_put(db,argv[2],argv[3],argv[4]);
    } else if(strcmp(cmd,"get")==0){
        if(argc<4){fprintf(stderr,"usage: bonfyre-space get <space> <key>\n");rc=1;}
        else cmd_get(db,argv[2],argv[3]);
    } else if(strcmp(cmd,"list")==0){
        cmd_list(db,argc>2?argv[2]:NULL);
    } else if(strcmp(cmd,"stats")==0){
        if(argc<3){fprintf(stderr,"usage: bonfyre-space stats <space>\n");rc=1;}
        else cmd_stats(db,argv[2]);
    } else if(strcmp(cmd,"gc")==0){
        int older=86400;
        if(argc>3&&strcmp(argv[2],"--older-than")==0) older=atoi(argv[3]);
        cmd_gc(db,older);
    } else if(strcmp(cmd,"attach")==0){
        if(argc<4){fprintf(stderr,"usage: bonfyre-space attach <space> <stage>\n");rc=1;}
        else cmd_attach(db,argv[2],argv[3]);
    } else if(strcmp(cmd,"detach")==0){
        if(argc<4){fprintf(stderr,"usage: bonfyre-space detach <space> <stage>\n");rc=1;}
        else cmd_detach(db,argv[2],argv[3]);
    } else {
        fprintf(stderr,"bonfyre-space: unknown command: %s\n",cmd);rc=1;
    }
    sqlite3_close(db);return rc;
}
