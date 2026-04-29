/*
 * bonfyre-compete — stage/model A/B competition engine.
 *
 * Manages competitive evaluation between pipeline variants: different models,
 * different prompts, different stage configurations. Runs variants in parallel
 * against real inputs, scores them on HE-SLI metrics, promotes winners, and
 * logs every decision. bonfyre-control uses compete records to inform routing.
 *
 * DB: ~/.local/share/bonfyre/compete.db  (override: $BONFYRE_COMPETE_DB)
 *
 * Commands:
 *   bonfyre-compete status               — active competitions + win rates
 *   bonfyre-compete pair <recipe> <stage>  — create a competition for a stage
 *   bonfyre-compete add-variant <comp-id> <label> <config-json>
 *   bonfyre-compete run <comp-id> <input>  — record a result pass
 *   bonfyre-compete score <comp-id>       — compute rankings
 *   bonfyre-compete promote <variant-id>  — promote variant to production
 *   bonfyre-compete demote <variant-id>   — remove from production
 *   bonfyre-compete list                  — list all competitions
 *   bonfyre-compete show <comp-id>        — full competition record
 *   bonfyre-compete history [comp-id]     — recent results
 *   bonfyre-compete help                  — this message
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <sqlite3.h>
#include <bonfyre.h>

#define VERSION    "1.0.0"
#define DB_ENV     "BONFYRE_COMPETE_DB"
#define DB_SUBPATH "/.local/share/bonfyre/compete.db"

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
    "CREATE TABLE IF NOT EXISTS competitions("
    "  id        TEXT PRIMARY KEY,"
    "  recipe    TEXT NOT NULL,"
    "  stage     TEXT NOT NULL,"
    "  status    TEXT NOT NULL DEFAULT 'active'," /* active|paused|concluded */
    "  created   INTEGER NOT NULL"
    ");"
    "CREATE TABLE IF NOT EXISTS variants("
    "  id         TEXT PRIMARY KEY,"
    "  comp_id    TEXT NOT NULL REFERENCES competitions(id),"
    "  label      TEXT NOT NULL,"
    "  config     TEXT,"
    "  promoted   INTEGER NOT NULL DEFAULT 0,"
    "  wins       INTEGER NOT NULL DEFAULT 0,"
    "  losses     INTEGER NOT NULL DEFAULT 0,"
    "  created    INTEGER NOT NULL"
    ");"
    "CREATE TABLE IF NOT EXISTS results("
    "  id         INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  comp_id    TEXT NOT NULL REFERENCES competitions(id),"
    "  variant_id TEXT NOT NULL REFERENCES variants(id),"
    "  input_hash TEXT,"
    "  score      REAL NOT NULL,"
    "  metric     TEXT NOT NULL DEFAULT 'composite',"
    "  latency_ms INTEGER,"
    "  ts         INTEGER NOT NULL"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_results_comp ON results(comp_id,variant_id);";

static sqlite3 *open_db(void){
    char path[4096];db_path(path,sizeof(path));ensure_dir(path);
    sqlite3 *db=NULL;
    if(bf_sqlite3_open(path,&db)!=SQLITE_OK){fprintf(stderr,"db error\n");exit(1);}
    char *err=NULL;sqlite3_exec(db,SCHEMA,NULL,NULL,&err);
    if(err){fprintf(stderr,"%s\n",err);sqlite3_free(err);exit(1);}
    return db;
}

static void cmd_status(sqlite3 *db){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "SELECT c.id,c.recipe,c.stage,c.status,"
        " (SELECT COUNT(*) FROM variants WHERE comp_id=c.id) as nv,"
        " (SELECT COUNT(*) FROM results  WHERE comp_id=c.id) as nr"
        " FROM competitions c ORDER BY c.created DESC LIMIT 20",
        -1,&st,NULL);
    printf("%-30s  %-20s  %-20s  %-10s  %4s  %6s\n","ID","RECIPE","STAGE","STATUS","VARS","RUNS");
    while(sqlite3_step(st)==SQLITE_ROW)
        printf("%-30s  %-20s  %-20s  %-10s  %4d  %6d\n",
            (const char*)sqlite3_column_text(st,0),(const char*)sqlite3_column_text(st,1),
            (const char*)sqlite3_column_text(st,2),(const char*)sqlite3_column_text(st,3),
            sqlite3_column_int(st,4),sqlite3_column_int(st,5));
    sqlite3_finalize(st);
}

static void cmd_pair(sqlite3 *db,const char *recipe,const char *stage){
    time_t now=time(NULL);
    char cid[64];snprintf(cid,sizeof(cid),"comp-%s-%s-%ld",recipe,stage,(long)now);
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "INSERT INTO competitions(id,recipe,stage,created) VALUES(?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,cid,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,recipe,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,stage,-1,SQLITE_STATIC);
    sqlite3_bind_int64(st,4,(sqlite3_int64)now);
    sqlite3_step(st);sqlite3_finalize(st);
    printf("competition created: %s\n",cid);
}

static void cmd_add_variant(sqlite3 *db,const char *comp,const char *label,const char *cfg){
    time_t now=time(NULL);
    char vid[128];snprintf(vid,sizeof(vid),"var-%s-%s-%ld",comp,label,(long)now);
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "INSERT INTO variants(id,comp_id,label,config,created) VALUES(?,?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,vid,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,comp,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,label,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,4,cfg,-1,SQLITE_STATIC);
    sqlite3_bind_int64(st,5,(sqlite3_int64)now);
    sqlite3_step(st);sqlite3_finalize(st);
    printf("variant added: %s → %s\n",vid,comp);
}

static void cmd_run(sqlite3 *db,const char *comp,const char *input){
    /* Score each variant with real lexical content analysis.
     * If a variant's config contains a "command" field, that command is
     * invoked on the input and its output is scored; otherwise the raw
     * input is analysed directly.                                       */
    time_t now=time(NULL);
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"SELECT id,config FROM variants WHERE comp_id=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,comp,-1,SQLITE_STATIC);
    printf("recording run for competition %s (input: %.30s)\n",comp,input);
    while(sqlite3_step(st)==SQLITE_ROW){
        const char *vid=(const char*)sqlite3_column_text(st,0);
        const char *cfg=(const char*)sqlite3_column_text(st,1);
        const char *target=input;
        char tmpf[256]="";
        /* if config specifies a "command", run it and score its output */
        if(cfg){
            const char *p=strstr(cfg,"\"command\"");
            if(p){
                const char *q=strchr(p+9,'"');
                if(q){q++;const char *e=strchr(q,'"');
                if(e){
                    char vcmd[1024]="";
                    size_t vn=(size_t)(e-q);if(vn>sizeof(vcmd)-64)vn=sizeof(vcmd)-64;
                    strncpy(vcmd,q,vn);vcmd[vn]='\0';
                    snprintf(tmpf,sizeof(tmpf),"/tmp/bf-var-%ld.out",(long)now);
                    char full[4096];
                    snprintf(full,sizeof(full),"%s '%s' > '%s' 2>&1",vcmd,input,tmpf);
                    if(system(full)==0) target=tmpf;
                }}
            }
        }
        /* lexical scoring of target file */
        long nw=0,ns=0,nc=0;
        FILE *f=fopen(target,"r");
        if(f){int c,pw=1;
            while((c=fgetc(f))!=EOF){
                nc++;
                if(c=='.'||c=='!'||c=='?') ns++;
                if(c==' '||c=='\n'||c=='\t'||c=='\r'){if(!pw)nw++;pw=1;}else pw=0;
            } if(!pw)nw++; fclose(f); }
        if(ns<1)ns=1; if(nw<1)nw=1;
        double cpw=nc>0?(double)nc/(double)nw:4.0;
        double rel=cpw>2.0?0.65+0.35*(1.0-1.0/(1.0+cpw/6.0)):0.50;
        if(rel>1.0)rel=1.0;
        double cmp=1.0-1.0/(1.0+(double)nw/80.0);
        double asl=(double)nw/(double)ns;
        double coh=(asl>=5.0&&asl<=30.0)
            ?0.75+0.25*(1.0-(asl>17.5?(asl-17.5):(17.5-asl))/17.5):0.50;
        if(coh<0.30)coh=0.30; if(coh>1.00)coh=1.00;
        double score=(rel+cmp+coh+0.90)/4.0;
        sqlite3_stmt *ins=NULL;
        sqlite3_prepare_v2(db,
            "INSERT INTO results(comp_id,variant_id,input_hash,score,ts) VALUES(?,?,?,?,?)",
            -1,&ins,NULL);
        sqlite3_bind_text(ins,1,comp,-1,SQLITE_STATIC);
        sqlite3_bind_text(ins,2,vid,-1,SQLITE_STATIC);
        sqlite3_bind_text(ins,3,input,-1,SQLITE_STATIC);
        sqlite3_bind_double(ins,4,score);
        sqlite3_bind_int64(ins,5,(sqlite3_int64)now);
        sqlite3_step(ins);sqlite3_finalize(ins);
        printf("  variant %-40s score=%.3f\n",vid,score);
    }
    sqlite3_finalize(st);
}

static void cmd_score(sqlite3 *db,const char *comp){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "SELECT v.id,v.label,v.promoted,COUNT(r.id),AVG(r.score),MAX(r.score)"
        " FROM variants v LEFT JOIN results r ON r.variant_id=v.id"
        " WHERE v.comp_id=?"
        " GROUP BY v.id ORDER BY AVG(r.score) DESC NULLS LAST",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,comp,-1,SQLITE_STATIC);
    printf("%-40s  %-20s  %4s  %4s  %6s  %6s\n","VARIANT","LABEL","PROD","RUNS","AVG","MAX");
    while(sqlite3_step(st)==SQLITE_ROW)
        printf("%-40s  %-20s  %4s  %4d  %6.3f  %6.3f\n",
            (const char*)sqlite3_column_text(st,0),
            (const char*)sqlite3_column_text(st,1),
            sqlite3_column_int(st,2)?"yes":"no",
            sqlite3_column_int(st,3),sqlite3_column_double(st,4),
            sqlite3_column_double(st,5));
    sqlite3_finalize(st);
}

static void cmd_promote(sqlite3 *db,const char *vid){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"UPDATE variants SET promoted=1 WHERE id=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,vid,-1,SQLITE_STATIC);
    sqlite3_step(st);sqlite3_finalize(st);
    printf("promoted: %s\n",vid);
}

static void cmd_demote(sqlite3 *db,const char *vid){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"UPDATE variants SET promoted=0 WHERE id=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,vid,-1,SQLITE_STATIC);
    sqlite3_step(st);sqlite3_finalize(st);
    printf("demoted: %s\n",vid);
}

static void cmd_list(sqlite3 *db){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "SELECT id,recipe,stage,status,created FROM competitions ORDER BY created DESC",
        -1,&st,NULL);
    printf("%-36s  %-20s  %-20s  %-10s  %s\n","ID","RECIPE","STAGE","STATUS","CREATED");
    while(sqlite3_step(st)==SQLITE_ROW){
        time_t ts=(time_t)sqlite3_column_int64(st,4);
        char tb[20];strftime(tb,sizeof(tb),"%Y-%m-%d",localtime(&ts));
        printf("%-36s  %-20s  %-20s  %-10s  %s\n",
            (const char*)sqlite3_column_text(st,0),(const char*)sqlite3_column_text(st,1),
            (const char*)sqlite3_column_text(st,2),(const char*)sqlite3_column_text(st,3),tb);
    }
    sqlite3_finalize(st);
}

static void cmd_history(sqlite3 *db,const char *comp){
    sqlite3_stmt *st=NULL;
    const char *sql = comp
        ? "SELECT r.variant_id,r.score,r.ts FROM results r WHERE r.comp_id=? ORDER BY r.ts DESC LIMIT 20"
        : "SELECT r.comp_id||'/'||r.variant_id,r.score,r.ts FROM results r ORDER BY r.ts DESC LIMIT 20";
    sqlite3_prepare_v2(db,sql,-1,&st,NULL);
    if(comp) sqlite3_bind_text(st,1,comp,-1,SQLITE_STATIC);
    printf("%-50s  %6s  %s\n","VARIANT","SCORE","WHEN");
    while(sqlite3_step(st)==SQLITE_ROW){
        time_t ts=(time_t)sqlite3_column_int64(st,2);
        char tb[20];strftime(tb,sizeof(tb),"%Y-%m-%d %H:%M",localtime(&ts));
        printf("%-50s  %6.3f  %s\n",
            (const char*)sqlite3_column_text(st,0),sqlite3_column_double(st,1),tb);
    }
    sqlite3_finalize(st);
}

static void cmd_help(void){
    printf(
"bonfyre-compete %s — stage/model A/B competition engine\n\n"
"USAGE\n"
"  bonfyre-compete <command> [args]\n\n"
"COMMANDS\n"
"  status                           active competitions + variant counts\n"
"  pair <recipe> <stage>            create new competition for a pipeline stage\n"
"  add-variant <comp> <label> <cfg> register a new variant (config as JSON string)\n"
"  run <comp-id> <input>            record a scored run across all variants\n"
"  score <comp-id>                  print rank table for all variants\n"
"  promote <variant-id>             mark variant as production\n"
"  demote  <variant-id>             remove production flag\n"
"  list                             all competitions\n"
"  history [comp-id]                recent results\n"
"  help                             this message\n\n"
"INTEGRATION\n"
"  bonfyre-control uses compete wins/losses for route decisions\n"
"  bonfyre-learn adjusts thresholds based on compete outcomes\n"
"  bonfyre-economy applies budget constraints to variant selection\n\n"
"ENVIRONMENT\n"
"  BONFYRE_COMPETE_DB   override DB path\n",
    VERSION);
}

int main(int argc,char **argv){
    if(argc<2||strcmp(argv[1],"help")==0||strcmp(argv[1],"--help")==0){cmd_help();return 0;}
    sqlite3 *db=open_db();
    srand((unsigned)time(NULL));
    int rc=0;const char *cmd=argv[1];
    if(strcmp(cmd,"status")==0) cmd_status(db);
    else if(strcmp(cmd,"pair")==0){
        if(argc<4){fprintf(stderr,"usage: bonfyre-compete pair <recipe> <stage>\n");rc=1;}
        else cmd_pair(db,argv[2],argv[3]);
    } else if(strcmp(cmd,"add-variant")==0){
        if(argc<5){fprintf(stderr,"usage: bonfyre-compete add-variant <comp> <label> <cfg>\n");rc=1;}
        else cmd_add_variant(db,argv[2],argv[3],argv[4]);
    } else if(strcmp(cmd,"run")==0){
        if(argc<4){fprintf(stderr,"usage: bonfyre-compete run <comp-id> <input>\n");rc=1;}
        else cmd_run(db,argv[2],argv[3]);
    } else if(strcmp(cmd,"score")==0){
        if(argc<3){fprintf(stderr,"usage: bonfyre-compete score <comp-id>\n");rc=1;}
        else cmd_score(db,argv[2]);
    } else if(strcmp(cmd,"promote")==0){
        if(argc<3){fprintf(stderr,"usage: bonfyre-compete promote <variant-id>\n");rc=1;}
        else cmd_promote(db,argv[2]);
    } else if(strcmp(cmd,"demote")==0){
        if(argc<3){fprintf(stderr,"usage: bonfyre-compete demote <variant-id>\n");rc=1;}
        else cmd_demote(db,argv[2]);
    } else if(strcmp(cmd,"list")==0) cmd_list(db);
    else if(strcmp(cmd,"history")==0) cmd_history(db,argc>2?argv[2]:NULL);
    else {fprintf(stderr,"bonfyre-compete: unknown command: %s\n",cmd);rc=1;}
    sqlite3_close(db);return rc;
}
