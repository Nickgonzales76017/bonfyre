/*
 * bonfyre-tier — latency tier management and SLA enforcement.
 *
 * Every pipeline stage belongs to a latency tier: instant (<10ms),
 * fast (<100ms), batch (<10s), or deep (minutes). bonfyre-tier maintains
 * these assignments, validates actual observed latencies against the SLA,
 * and routes requests to the right execution path. bonfyre-control and
 * bonfyre-economy consult tier routing before dispatching.
 *
 * DB: ~/.local/share/bonfyre/tier.db  (override: $BONFYRE_TIER_DB)
 *
 * Commands:
 *   bonfyre-tier status                   — tier summary + SLA compliance
 *   bonfyre-tier list                     — all defined tiers
 *   bonfyre-tier show <tier>              — tier record + SLA definition
 *   bonfyre-tier set <recipe> <stage> <tier>  — assign stage to tier
 *   bonfyre-tier route <recipe> <stage>    — get tier for a stage
 *   bonfyre-tier record <recipe> <stage> <ms>   — log observed latency
 *   bonfyre-tier violations [recipe]      — stages exceeding SLA
 *   bonfyre-tier history [recipe]         — recent latency records
 *   bonfyre-tier help                     — this message
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sqlite3.h>
#include <bonfyre.h>

#define VERSION    "1.0.0"
#define DB_ENV     "BONFYRE_TIER_DB"
#define DB_SUBPATH "/.local/share/bonfyre/tier.db"

static void db_path(char *buf,size_t len){
    const char *e=getenv(DB_ENV);
    if(e){snprintf(buf,len,"%s",e);return;}
    const char *h=getenv("HOME");if(!h)h="/tmp";
    snprintf(buf,len,"%s%s",h,DB_SUBPATH);
}
static void ensure_dir(const char *p) { bf_ensure_dir(p); }

static const char *SCHEMA=
    "PRAGMA journal_mode=WAL;"
    "PRAGMA foreign_keys=ON;"
    "CREATE TABLE IF NOT EXISTS tiers("
    "  name        TEXT PRIMARY KEY,"
    "  max_ms      INTEGER NOT NULL,"
    "  description TEXT NOT NULL,"
    "  color       TEXT"
    ");"
    "CREATE TABLE IF NOT EXISTS assignments("
    "  recipe      TEXT NOT NULL,"
    "  stage       TEXT NOT NULL,"
    "  tier        TEXT NOT NULL REFERENCES tiers(name),"
    "  updated     INTEGER NOT NULL,"
    "  PRIMARY KEY(recipe,stage)"
    ");"
    "CREATE TABLE IF NOT EXISTS observations("
    "  id          INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  recipe      TEXT NOT NULL,"
    "  stage       TEXT NOT NULL,"
    "  latency_ms  INTEGER NOT NULL,"
    "  violated    INTEGER NOT NULL DEFAULT 0,"
    "  tier        TEXT,"
    "  ts          INTEGER NOT NULL"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_obs_recipe ON observations(recipe,stage);";

static const struct {const char *name;int max_ms;const char *desc;} TIER_DEFS[]={
    {"instant",   10,     "Sub-10ms: in-memory lookups, cache reads, metadata ops"},
    {"fast",      100,    "Sub-100ms: lightweight inference, local model, index queries"},
    {"batch",     10000,  "Sub-10s: cloud inference, file I/O, moderate compute"},
    {"deep",      600000, "Minutes: large model inference, batch transcription, GPU jobs"},
};
static const int NTIERS=4;

static sqlite3 *open_db(void){
    char path[4096];db_path(path,sizeof(path));ensure_dir(path);
    sqlite3 *db=NULL;
    if(bf_sqlite3_open(path,&db)!=SQLITE_OK){fprintf(stderr,"db error\n");exit(1);}
    char *err=NULL;sqlite3_exec(db,SCHEMA,NULL,NULL,&err);
    if(err){fprintf(stderr,"%s\n",err);sqlite3_free(err);exit(1);}
    /* seed tiers */
    for(int i=0;i<NTIERS;i++){
        sqlite3_stmt *st=NULL;
        sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO tiers(name,max_ms,description) VALUES(?,?,?)",-1,&st,NULL);
        sqlite3_bind_text(st,1,TIER_DEFS[i].name,-1,SQLITE_STATIC);
        sqlite3_bind_int(st,2,TIER_DEFS[i].max_ms);
        sqlite3_bind_text(st,3,TIER_DEFS[i].desc,-1,SQLITE_STATIC);
        sqlite3_step(st);sqlite3_finalize(st);
    }
    return db;
}

static void cmd_status(sqlite3 *db){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "SELECT t.name,t.max_ms,"
        " (SELECT COUNT(*) FROM assignments a WHERE a.tier=t.name) as n_assigned,"
        " (SELECT COUNT(*) FROM observations o WHERE o.tier=t.name AND o.violated=1) as n_viol"
        " FROM tiers t ORDER BY t.max_ms",
        -1,&st,NULL);
    printf("%-10s  %8s  %8s  %8s\n","TIER","MAX(ms)","ASSIGNED","VIOLAT");
    while(sqlite3_step(st)==SQLITE_ROW)
        printf("%-10s  %8d  %8d  %8d\n",
            (const char*)sqlite3_column_text(st,0),sqlite3_column_int(st,1),
            sqlite3_column_int(st,2),sqlite3_column_int(st,3));
    sqlite3_finalize(st);
}

static void cmd_list(sqlite3 *db){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"SELECT name,max_ms,description FROM tiers ORDER BY max_ms",-1,&st,NULL);
    printf("%-10s  %8s  %s\n","TIER","MAX(ms)","DESCRIPTION");
    while(sqlite3_step(st)==SQLITE_ROW)
        printf("%-10s  %8d  %s\n",
            (const char*)sqlite3_column_text(st,0),sqlite3_column_int(st,1),
            (const char*)sqlite3_column_text(st,2));
    sqlite3_finalize(st);
}

static void cmd_show(sqlite3 *db,const char *tier){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"SELECT name,max_ms,description FROM tiers WHERE name=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,tier,-1,SQLITE_STATIC);
    if(sqlite3_step(st)!=SQLITE_ROW){fprintf(stderr,"tier not found: %s\n",tier);sqlite3_finalize(st);return;}
    printf("tier        : %s\nmax_ms      : %d\ndescription : %s\n",
        (const char*)sqlite3_column_text(st,0),sqlite3_column_int(st,1),
        (const char*)sqlite3_column_text(st,2));
    sqlite3_finalize(st);
    /* assigned stages */
    sqlite3_prepare_v2(db,"SELECT recipe,stage FROM assignments WHERE tier=? ORDER BY recipe,stage",-1,&st,NULL);
    sqlite3_bind_text(st,1,tier,-1,SQLITE_STATIC);
    printf("\nassigned stages:\n");
    int any=0;
    while(sqlite3_step(st)==SQLITE_ROW){
        printf("  %s/%s\n",(const char*)sqlite3_column_text(st,0),(const char*)sqlite3_column_text(st,1));
        any=1;
    }
    if(!any) printf("  (none)\n");
    sqlite3_finalize(st);
}

static void cmd_set(sqlite3 *db,const char *recipe,const char *stage,const char *tier){
    time_t now=time(NULL);
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO assignments(recipe,stage,tier,updated) VALUES(?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,recipe,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,stage,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,tier,-1,SQLITE_STATIC);
    sqlite3_bind_int64(st,4,(sqlite3_int64)now);
    sqlite3_step(st);sqlite3_finalize(st);
    printf("assigned: %s/%s → tier=%s\n",recipe,stage,tier);
}

static void cmd_route(sqlite3 *db,const char *recipe,const char *stage){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "SELECT a.tier,t.max_ms FROM assignments a JOIN tiers t ON t.name=a.tier"
        " WHERE a.recipe=? AND a.stage=?",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,recipe,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,stage,-1,SQLITE_STATIC);
    if(sqlite3_step(st)==SQLITE_ROW){
        printf("%s/%s → tier=%s (max=%dms)\n",recipe,stage,
            (const char*)sqlite3_column_text(st,0),sqlite3_column_int(st,1));
    } else {
        printf("%s/%s → tier=batch (default, no assignment found)\n",recipe,stage);
    }
    sqlite3_finalize(st);
}

static void cmd_record(sqlite3 *db,const char *recipe,const char *stage,int ms){
    time_t now=time(NULL);
    /* look up tier SLA */
    sqlite3_stmt *st=NULL;
    int max_ms=10000;char tier_name[64]="batch";
    sqlite3_prepare_v2(db,
        "SELECT a.tier,t.max_ms FROM assignments a JOIN tiers t ON t.name=a.tier"
        " WHERE a.recipe=? AND a.stage=?",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,recipe,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,stage,-1,SQLITE_STATIC);
    if(sqlite3_step(st)==SQLITE_ROW){
        snprintf(tier_name,sizeof(tier_name),"%s",(const char*)sqlite3_column_text(st,0));
        max_ms=sqlite3_column_int(st,1);
    }
    sqlite3_finalize(st);

    int violated=(ms>max_ms)?1:0;
    sqlite3_stmt *ins=NULL;
    sqlite3_prepare_v2(db,
        "INSERT INTO observations(recipe,stage,latency_ms,violated,tier,ts) VALUES(?,?,?,?,?,?)",
        -1,&ins,NULL);
    sqlite3_bind_text(ins,1,recipe,-1,SQLITE_STATIC);
    sqlite3_bind_text(ins,2,stage,-1,SQLITE_STATIC);
    sqlite3_bind_int(ins,3,ms);
    sqlite3_bind_int(ins,4,violated);
    sqlite3_bind_text(ins,5,tier_name,-1,SQLITE_STATIC);
    sqlite3_bind_int64(ins,6,(sqlite3_int64)now);
    sqlite3_step(ins);sqlite3_finalize(ins);
    printf("recorded: %s/%s %dms tier=%s%s\n",recipe,stage,ms,tier_name,violated?" [SLA VIOLATION]":"");
}

static void cmd_violations(sqlite3 *db,const char *recipe){
    sqlite3_stmt *st=NULL;
    if(recipe){
        sqlite3_prepare_v2(db,
            "SELECT recipe,stage,latency_ms,tier,ts FROM observations WHERE violated=1 AND recipe=?"
            " ORDER BY ts DESC LIMIT 20",
            -1,&st,NULL);
        sqlite3_bind_text(st,1,recipe,-1,SQLITE_STATIC);
    } else {
        sqlite3_prepare_v2(db,
            "SELECT recipe,stage,latency_ms,tier,ts FROM observations WHERE violated=1"
            " ORDER BY ts DESC LIMIT 20",
            -1,&st,NULL);
    }
    printf("%-20s  %-20s  %8s  %-8s  %s\n","RECIPE","STAGE","MS","TIER","WHEN");
    while(sqlite3_step(st)==SQLITE_ROW){
        time_t ts=(time_t)sqlite3_column_int64(st,4);
        char tb[20];strftime(tb,sizeof(tb),"%Y-%m-%d %H:%M",localtime(&ts));
        printf("%-20s  %-20s  %8d  %-8s  %s\n",
            (const char*)sqlite3_column_text(st,0),(const char*)sqlite3_column_text(st,1),
            sqlite3_column_int(st,2),(const char*)sqlite3_column_text(st,3),tb);
    }
    sqlite3_finalize(st);
}

static void cmd_history(sqlite3 *db,const char *recipe){
    sqlite3_stmt *st=NULL;
    if(recipe){
        sqlite3_prepare_v2(db,
            "SELECT recipe,stage,latency_ms,violated,tier,ts FROM observations WHERE recipe=?"
            " ORDER BY ts DESC LIMIT 30",
            -1,&st,NULL);
        sqlite3_bind_text(st,1,recipe,-1,SQLITE_STATIC);
    } else {
        sqlite3_prepare_v2(db,
            "SELECT recipe,stage,latency_ms,violated,tier,ts FROM observations"
            " ORDER BY ts DESC LIMIT 30",
            -1,&st,NULL);
    }
    printf("%-20s  %-20s  %8s  %3s  %-8s  %s\n","RECIPE","STAGE","MS","SLA","TIER","WHEN");
    while(sqlite3_step(st)==SQLITE_ROW){
        time_t ts=(time_t)sqlite3_column_int64(st,5);
        char tb[20];strftime(tb,sizeof(tb),"%Y-%m-%d %H:%M",localtime(&ts));
        printf("%-20s  %-20s  %8d  %3s  %-8s  %s\n",
            (const char*)sqlite3_column_text(st,0),(const char*)sqlite3_column_text(st,1),
            sqlite3_column_int(st,2),sqlite3_column_int(st,3)?"✗":"✓",
            (const char*)sqlite3_column_text(st,4),tb);
    }
    sqlite3_finalize(st);
}

static void cmd_help(void){
    printf(
"bonfyre-tier %s — latency tier management and SLA enforcement\n\n"
"USAGE\n"
"  bonfyre-tier <command> [args]\n\n"
"COMMANDS\n"
"  status                        tier counts + SLA compliance\n"
"  list                          all tier definitions\n"
"  show <tier>                   tier SLA + assigned stages\n"
"  set <recipe> <stage> <tier>   assign a stage to a tier\n"
"  route <recipe> <stage>        get tier assignment for a stage\n"
"  record <recipe> <stage> <ms>  log observed latency\n"
"  violations [recipe]           show SLA violations\n"
"  history [recipe]              recent latency records\n"
"  layer <artifact_id>           recommended layer tier\n"
"  help                          this message\n\n"
"BUILT-IN TIERS\n"
"  instant  <10ms    in-memory, cache, metadata ops\n"
"  fast     <100ms   local inference, index reads\n"
"  batch    <10s     cloud inference, file I/O\n"
"  deep     <600s    large model, GPU batch jobs\n\n"
"INTEGRATION\n"
"  bonfyre-control reads tier assignments for SLA-aware routing\n"
"  bonfyre-economy combines tier + cost for optimal dispatch\n"
"  bonfyre-run records actual latency: bonfyre-tier record ...\n\n"
"ENVIRONMENT\n"
"  BONFYRE_TIER_DB   override DB path\n",
    VERSION);
}

int main(int argc,char **argv){
    if(argc<2||strcmp(argv[1],"help")==0||strcmp(argv[1],"--help")==0){cmd_help();return 0;}
    if(strcmp(argv[1],"layer")==0 && argc>=3){
        const char *root = NULL;
        char *json = NULL, *out = NULL;
        for(int i=1;i<argc-1;i++) if(strcmp(argv[i],"--root")==0){ root=argv[i+1]; break; }
        if (bf_layer_load_json(root, argv[2], &json) != 0) return 1;
        if (bf_layer_tier_json(json, &out) != 0) { free(json); return 1; }
        puts(out);
        free(out);
        free(json);
        return 0;
    }
    sqlite3 *db=open_db();
    int rc=0;const char *cmd=argv[1];
    if(strcmp(cmd,"status")==0) cmd_status(db);
    else if(strcmp(cmd,"list")==0) cmd_list(db);
    else if(strcmp(cmd,"show")==0){
        if(argc<3){fprintf(stderr,"usage: bonfyre-tier show <tier>\n");rc=1;}
        else cmd_show(db,argv[2]);
    } else if(strcmp(cmd,"set")==0){
        if(argc<5){fprintf(stderr,"usage: bonfyre-tier set <recipe> <stage> <tier>\n");rc=1;}
        else cmd_set(db,argv[2],argv[3],argv[4]);
    } else if(strcmp(cmd,"route")==0){
        if(argc<4){fprintf(stderr,"usage: bonfyre-tier route <recipe> <stage>\n");rc=1;}
        else cmd_route(db,argv[2],argv[3]);
    } else if(strcmp(cmd,"record")==0){
        if(argc<5){fprintf(stderr,"usage: bonfyre-tier record <recipe> <stage> <ms>\n");rc=1;}
        else cmd_record(db,argv[2],argv[3],atoi(argv[4]));
    } else if(strcmp(cmd,"violations")==0) cmd_violations(db,argc>2?argv[2]:NULL);
    else if(strcmp(cmd,"history")==0) cmd_history(db,argc>2?argv[2]:NULL);
    else {fprintf(stderr,"bonfyre-tier: unknown command: %s\n",cmd);rc=1;}
    sqlite3_close(db);return rc;
}
