/*
 * bonfyre-learn — artifact-level feedback and threshold tuning.
 *
 * Collects outcome signals (user ratings, downstream errors, downstream
 * improvements) and tunes per-stage quality thresholds to minimize waste:
 * if a stage's outputs consistently score well, loosen its latency budget;
 * if they score poorly, tighten the quality gate or trigger a rerun.
 * bonfyre-compete produces wins/losses; bonfyre-learn converts them to
 * threshold updates and shares them with bonfyre-control.
 *
 * DB: ~/.local/share/bonfyre/learn.db  (override: $BONFYRE_LEARN_DB)
 *
 * Commands:
 *   bonfyre-learn status               — stage thresholds + recent tuning
 *   bonfyre-learn feedback <run-id> <score> [label]   — record outcome
 *   bonfyre-learn tune <stage>         — run threshold update for a stage
 *   bonfyre-learn list                 — all tracked stages
 *   bonfyre-learn export               — dump thresholds as JSON
 *   bonfyre-learn reset <stage>        — restore default thresholds
 *   bonfyre-learn history [stage]      — recent feedback records
 *   bonfyre-learn help                 — this message
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <sqlite3.h>
#include <bonfyre.h>

#define VERSION    "1.0.0"
#define DB_ENV     "BONFYRE_LEARN_DB"
#define DB_SUBPATH "/.local/share/bonfyre/learn.db"

/* Default quality threshold: if avg score below this, tighten gate */
#define THRESHOLD_DEFAULT 0.75
#define THRESHOLD_MIN     0.50
#define THRESHOLD_MAX     0.99

static void db_path(char *buf,size_t len){
    const char *e=getenv(DB_ENV);
    if(e){snprintf(buf,len,"%s",e);return;}
    const char *h=getenv("HOME");if(!h)h="/tmp";
    snprintf(buf,len,"%s%s",h,DB_SUBPATH);
}
static void ensure_dir(const char *p) { bf_ensure_parent_dir(p); }

static const char *SCHEMA=
    "PRAGMA journal_mode=WAL;"
    "PRAGMA foreign_keys=ON;"
    "CREATE TABLE IF NOT EXISTS feedback("
    "  id       INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  run_id   TEXT NOT NULL,"
    "  stage    TEXT,"
    "  score    REAL NOT NULL,"
    "  label    TEXT," /* good|bad|neutral */
    "  ts       INTEGER NOT NULL"
    ");"
    "CREATE TABLE IF NOT EXISTS thresholds("
    "  stage    TEXT PRIMARY KEY,"
    "  quality  REAL NOT NULL DEFAULT 0.75,"
    "  latency_ms INTEGER NOT NULL DEFAULT 5000,"
    "  updated  INTEGER NOT NULL"
    ");"
    "CREATE TABLE IF NOT EXISTS optimizations("
    "  id       INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  stage    TEXT NOT NULL,"
    "  metric   TEXT NOT NULL,"
    "  old_val  REAL NOT NULL,"
    "  new_val  REAL NOT NULL,"
    "  reason   TEXT,"
    "  ts       INTEGER NOT NULL"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_feedback_run   ON feedback(run_id);"
    "CREATE INDEX IF NOT EXISTS idx_feedback_stage ON feedback(stage);";

/* seed default thresholds for known stages */
static const char *SEED_STAGES[]={"intake","transcribe","diarize","tag","summarize","translate","qa"};
static const int NSEED=7;

static sqlite3 *open_db(void){
    char path[4096];db_path(path,sizeof(path));ensure_dir(path);
    sqlite3 *db=NULL;
    if(bf_sqlite3_open(path,&db)!=SQLITE_OK){fprintf(stderr,"db error\n");exit(1);}
    char *err=NULL;sqlite3_exec(db,SCHEMA,NULL,NULL,&err);
    if(err){fprintf(stderr,"%s\n",err);sqlite3_free(err);exit(1);}
    /* seed known stages */
    time_t now=time(NULL);
    for(int i=0;i<NSEED;i++){
        sqlite3_stmt *st=NULL;
        sqlite3_prepare_v2(db,
            "INSERT OR IGNORE INTO thresholds(stage,quality,latency_ms,updated) VALUES(?,0.75,5000,?)",
            -1,&st,NULL);
        sqlite3_bind_text(st,1,SEED_STAGES[i],-1,SQLITE_STATIC);
        sqlite3_bind_int64(st,2,(sqlite3_int64)now);
        sqlite3_step(st);sqlite3_finalize(st);
    }
    return db;
}

static void cmd_status(sqlite3 *db){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "SELECT t.stage,t.quality,t.latency_ms,"
        " (SELECT AVG(f.score) FROM feedback f WHERE f.stage=t.stage AND f.ts>strftime('%s','now','-7 days'))"
        " FROM thresholds t ORDER BY t.stage",
        -1,&st,NULL);
    printf("%-20s  %8s  %8s  %8s\n","STAGE","QUALITY","LAT(ms)","AVG7D");
    while(sqlite3_step(st)==SQLITE_ROW){
        const char *avg_s="     —";
        char avg_buf[16];
        if(sqlite3_column_type(st,3)!=SQLITE_NULL){
            snprintf(avg_buf,sizeof(avg_buf)," %6.3f",sqlite3_column_double(st,3));
            avg_s=avg_buf;
        }
        printf("%-20s  %8.3f  %8d  %s\n",
            (const char*)sqlite3_column_text(st,0),sqlite3_column_double(st,1),
            sqlite3_column_int(st,2),avg_s);
    }
    sqlite3_finalize(st);
}

static void cmd_feedback(sqlite3 *db,const char *run_id,double score,const char *label){
    time_t now=time(NULL);
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "INSERT INTO feedback(run_id,score,label,ts) VALUES(?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,run_id,-1,SQLITE_STATIC);
    sqlite3_bind_double(st,2,score);
    if(label) sqlite3_bind_text(st,3,label,-1,SQLITE_STATIC);
    else sqlite3_bind_null(st,3);
    sqlite3_bind_int64(st,4,(sqlite3_int64)now);
    sqlite3_step(st);sqlite3_finalize(st);
    printf("feedback: run=%s score=%.3f%s%s\n",run_id,score,label?" label=":"",label?label:"");
}

static void cmd_tune(sqlite3 *db,const char *stage){
    /* compute rolling avg over last 50 samples, adjust threshold */
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "SELECT AVG(score),COUNT(*) FROM ("
        "  SELECT score FROM feedback WHERE stage=? ORDER BY ts DESC LIMIT 50"
        ")",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,stage,-1,SQLITE_STATIC);
    if(sqlite3_step(st)!=SQLITE_ROW||sqlite3_column_type(st,0)==SQLITE_NULL){
        printf("not enough data to tune stage '%s'\n",stage);
        sqlite3_finalize(st);return;
    }
    double avg=sqlite3_column_double(st,0);
    int n=sqlite3_column_int(st,1);
    sqlite3_finalize(st);

    /* get current threshold */
    double cur=THRESHOLD_DEFAULT;
    sqlite3_prepare_v2(db,"SELECT quality FROM thresholds WHERE stage=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,stage,-1,SQLITE_STATIC);
    if(sqlite3_step(st)==SQLITE_ROW) cur=sqlite3_column_double(st,0);
    sqlite3_finalize(st);

    /* simple adjustment: if avg significantly above threshold → loosen slightly */
    double next=cur;
    const char *reason="stable";
    if(avg<cur-0.05){
        next=cur-0.05; if(next<THRESHOLD_MIN)next=THRESHOLD_MIN;
        reason="avg_below_threshold";
    } else if(avg>cur+0.10){
        next=cur+0.02; if(next>THRESHOLD_MAX)next=THRESHOLD_MAX;
        reason="avg_well_above_threshold";
    }

    time_t now=time(NULL);
    sqlite3_stmt *upd=NULL;
    sqlite3_prepare_v2(db,"INSERT OR REPLACE INTO thresholds(stage,quality,latency_ms,updated) VALUES(?,?,5000,?)",-1,&upd,NULL);
    sqlite3_bind_text(upd,1,stage,-1,SQLITE_STATIC);
    sqlite3_bind_double(upd,2,next);
    sqlite3_bind_int64(upd,3,(sqlite3_int64)now);
    sqlite3_step(upd);sqlite3_finalize(upd);

    if(next!=cur){
        sqlite3_stmt *log=NULL;
        sqlite3_prepare_v2(db,"INSERT INTO optimizations(stage,metric,old_val,new_val,reason,ts) VALUES(?,?,?,?,?,?)",-1,&log,NULL);
        sqlite3_bind_text(log,1,stage,-1,SQLITE_STATIC);
        sqlite3_bind_text(log,2,"quality_threshold",-1,SQLITE_STATIC);
        sqlite3_bind_double(log,3,cur);
        sqlite3_bind_double(log,4,next);
        sqlite3_bind_text(log,5,reason,-1,SQLITE_STATIC);
        sqlite3_bind_int64(log,6,(sqlite3_int64)now);
        sqlite3_step(log);sqlite3_finalize(log);
    }

    printf("tune %s: samples=%d avg=%.3f threshold %.3f→%.3f (%s)\n",
        stage,n,avg,cur,next,reason);
}

static void cmd_list(sqlite3 *db){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "SELECT stage,quality,latency_ms,updated FROM thresholds ORDER BY stage",
        -1,&st,NULL);
    printf("%-20s  %8s  %8s  %s\n","STAGE","QUALITY","LAT(ms)","UPDATED");
    while(sqlite3_step(st)==SQLITE_ROW){
        time_t ts=(time_t)sqlite3_column_int64(st,3);
        char tb[20];strftime(tb,sizeof(tb),"%Y-%m-%d",localtime(&ts));
        printf("%-20s  %8.3f  %8d  %s\n",
            (const char*)sqlite3_column_text(st,0),sqlite3_column_double(st,1),
            sqlite3_column_int(st,2),tb);
    }
    sqlite3_finalize(st);
}

static void cmd_export(sqlite3 *db){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"SELECT stage,quality,latency_ms FROM thresholds ORDER BY stage",-1,&st,NULL);
    printf("{\n  \"thresholds\": [\n");
    int first=1;
    while(sqlite3_step(st)==SQLITE_ROW){
        if(!first) printf(",\n");
        printf("    {\"stage\":\"%s\",\"quality\":%.3f,\"latency_ms\":%d}",
            (const char*)sqlite3_column_text(st,0),sqlite3_column_double(st,1),
            sqlite3_column_int(st,2));
        first=0;
    }
    printf("\n  ]\n}\n");
    sqlite3_finalize(st);
}

static void cmd_reset(sqlite3 *db,const char *stage){
    time_t now=time(NULL);
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"INSERT OR REPLACE INTO thresholds(stage,quality,latency_ms,updated) VALUES(?,0.75,5000,?)",-1,&st,NULL);
    sqlite3_bind_text(st,1,stage,-1,SQLITE_STATIC);
    sqlite3_bind_int64(st,2,(sqlite3_int64)now);
    sqlite3_step(st);sqlite3_finalize(st);
    printf("reset: %s → quality=0.750 latency_ms=5000\n",stage);
}

static void cmd_history(sqlite3 *db,const char *stage){
    sqlite3_stmt *st=NULL;
    if(stage){
        sqlite3_prepare_v2(db,
            "SELECT run_id,score,label,ts FROM feedback WHERE stage=? ORDER BY ts DESC LIMIT 30",
            -1,&st,NULL);
        sqlite3_bind_text(st,1,stage,-1,SQLITE_STATIC);
    } else {
        sqlite3_prepare_v2(db,
            "SELECT run_id,score,label,ts FROM feedback ORDER BY ts DESC LIMIT 30",
            -1,&st,NULL);
    }
    printf("%-36s  %6s  %-8s  %s\n","RUN ID","SCORE","LABEL","WHEN");
    while(sqlite3_step(st)==SQLITE_ROW){
        time_t ts=(time_t)sqlite3_column_int64(st,3);
        char tb[20];strftime(tb,sizeof(tb),"%Y-%m-%d %H:%M",localtime(&ts));
        printf("%-36s  %6.3f  %-8s  %s\n",
            (const char*)sqlite3_column_text(st,0),sqlite3_column_double(st,1),
            sqlite3_column_text(st,2)?(const char*)sqlite3_column_text(st,2):"—",tb);
    }
    sqlite3_finalize(st);
}

static void cmd_help(void){
    printf(
"bonfyre-learn %s — artifact feedback and threshold tuning\n\n"
"USAGE\n"
"  bonfyre-learn <command> [args]\n\n"
"COMMANDS\n"
"  status                    current stage thresholds + 7-day averages\n"
"  feedback <run> <score> [label]   record outcome (score 0.0–1.0)\n"
"  tune <stage>              compute rolling avg and adjust threshold\n"
"  list                      all tracked stages and thresholds\n"
"  export                    dump thresholds as JSON\n"
"  reset <stage>             restore default thresholds (0.75 quality)\n"
"  history [stage]           recent feedback records\n"
"  help                      this message\n\n"
"LABELS\n"
"  good · bad · neutral · repeat · escalate\n\n"
"TUNING LOGIC\n"
"  - Rolling 50-sample average compared to current threshold\n"
"  - avg < threshold - 0.05 → lower gate by 0.05 (min 0.50)\n"
"  - avg > threshold + 0.10 → raise gate by 0.02 (max 0.99)\n\n"
"INTEGRATION\n"
"  bonfyre-compete → bonfyre-learn feedback (win/loss signals)\n"
"  bonfyre-control reads exported thresholds for routing gates\n\n"
"ENVIRONMENT\n"
"  BONFYRE_LEARN_DB   override DB path\n",
    VERSION);
}

int main(int argc,char **argv){
    if(argc<2||strcmp(argv[1],"help")==0||strcmp(argv[1],"--help")==0){cmd_help();return 0;}
    sqlite3 *db=open_db();
    int rc=0;const char *cmd=argv[1];
    if(strcmp(cmd,"status")==0) cmd_status(db);
    else if(strcmp(cmd,"feedback")==0){
        if(argc<4){fprintf(stderr,"usage: bonfyre-learn feedback <run-id> <score> [label]\n");rc=1;}
        else cmd_feedback(db,argv[2],atof(argv[3]),argc>4?argv[4]:NULL);
    } else if(strcmp(cmd,"tune")==0){
        if(argc<3){fprintf(stderr,"usage: bonfyre-learn tune <stage>\n");rc=1;}
        else cmd_tune(db,argv[2]);
    } else if(strcmp(cmd,"list")==0) cmd_list(db);
    else if(strcmp(cmd,"export")==0) cmd_export(db);
    else if(strcmp(cmd,"reset")==0){
        if(argc<3){fprintf(stderr,"usage: bonfyre-learn reset <stage>\n");rc=1;}
        else cmd_reset(db,argv[2]);
    } else if(strcmp(cmd,"history")==0) cmd_history(db,argc>2?argv[2]:NULL);
    else {fprintf(stderr,"bonfyre-learn: unknown command: %s\n",cmd);rc=1;}
    sqlite3_close(db);return rc;
}
