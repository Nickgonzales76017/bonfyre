/*
 * bonfyre-flow — coroutine-native pipeline programming.
 *
 * Defines pipelines as graphs: stages, branches (conditional routing),
 * merges (fan-in), loops, and reactive triggers. Flows are JSON documents;
 * bonfyre-flow stores them, validates them, runs them, and tracks state.
 * bonfyre-run executes individual stages; bonfyre-flow orchestrates them.
 *
 * DB: ~/.local/share/bonfyre/flow.db  (override: $BONFYRE_FLOW_DB)
 *
 * Commands:
 *   bonfyre-flow status               — running flows + stage states
 *   bonfyre-flow define <file.json>   — import a flow definition
 *   bonfyre-flow list                 — list all defined flows
 *   bonfyre-flow show <flow-id>       — show flow definition
 *   bonfyre-flow run <flow-id> <input>   — start a flow run
 *   bonfyre-flow inspect <run-id>     — show execution state
 *   bonfyre-flow branch <flow-id> <cond> <target>   — add branch rule
 *   bonfyre-flow merge <flow-id> <stage-a> <stage-b> <target>  — add merge
 *   bonfyre-flow export <flow-id>     — print flow definition as JSON
 *   bonfyre-flow rm <flow-id>         — delete a flow
 *   bonfyre-flow help                 — this message
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <sqlite3.h>
#include <bonfyre.h>

#define VERSION    "1.0.0"
#define DB_ENV     "BONFYRE_FLOW_DB"
#define DB_SUBPATH "/.local/share/bonfyre/flow.db"

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
    "CREATE TABLE IF NOT EXISTS flows("
    "  id          TEXT PRIMARY KEY,"
    "  name        TEXT NOT NULL,"
    "  definition  TEXT NOT NULL," /* full JSON */
    "  entry_stage TEXT NOT NULL,"
    "  created     INTEGER NOT NULL,"
    "  updated     INTEGER NOT NULL"
    ");"
    "CREATE TABLE IF NOT EXISTS edges("
    "  flow_id     TEXT NOT NULL REFERENCES flows(id) ON DELETE CASCADE,"
    "  from_stage  TEXT NOT NULL,"
    "  to_stage    TEXT NOT NULL,"
    "  kind        TEXT NOT NULL DEFAULT 'seq'," /* seq|branch|merge|loop */
    "  condition   TEXT,"
    "  PRIMARY KEY(flow_id,from_stage,to_stage)"
    ");"
    "CREATE TABLE IF NOT EXISTS runs("
    "  id            TEXT PRIMARY KEY,"
    "  flow_id       TEXT NOT NULL REFERENCES flows(id),"
    "  input         TEXT,"
    "  status        TEXT NOT NULL DEFAULT 'running'," /* running|done|failed|waiting */
    "  current_stage TEXT,"
    "  started       INTEGER NOT NULL,"
    "  finished      INTEGER"
    ");"
    "CREATE TABLE IF NOT EXISTS run_stages("
    "  run_id  TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,"
    "  stage   TEXT NOT NULL,"
    "  status  TEXT NOT NULL DEFAULT 'pending',"
    "  started INTEGER,"
    "  done    INTEGER,"
    "  output  TEXT,"
    "  PRIMARY KEY(run_id,stage)"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_runs_flow ON runs(flow_id);";

static sqlite3 *open_db(void){
    char path[4096];db_path(path,sizeof(path));ensure_dir(path);
    sqlite3 *db=NULL;
    if(bf_sqlite3_open(path,&db)!=SQLITE_OK){fprintf(stderr,"db error\n");exit(1);}
    char *err=NULL;sqlite3_exec(db,SCHEMA,NULL,NULL,&err);
    if(err){fprintf(stderr,"%s\n",err);sqlite3_free(err);exit(1);}
    return db;
}

/* Read file content into a malloc'd buffer (caller frees) */
static char *read_file(const char *path) { return bf_read_file(path, NULL); }

static void cmd_status(sqlite3 *db){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "SELECT r.id,r.flow_id,r.status,r.current_stage,r.started"
        " FROM runs r WHERE r.status='running' OR r.status='waiting'"
        " ORDER BY r.started DESC LIMIT 20",
        -1,&st,NULL);
    printf("%-36s  %-20s  %-10s  %-20s  %s\n","RUN ID","FLOW","STATUS","STAGE","STARTED");
    int any=0;
    while(sqlite3_step(st)==SQLITE_ROW){
        time_t ts=(time_t)sqlite3_column_int64(st,4);
        char tb[20];strftime(tb,sizeof(tb),"%Y-%m-%d %H:%M",localtime(&ts));
        printf("%-36s  %-20s  %-10s  %-20s  %s\n",
            (const char*)sqlite3_column_text(st,0),(const char*)sqlite3_column_text(st,1),
            (const char*)sqlite3_column_text(st,2),
            sqlite3_column_text(st,3)?(const char*)sqlite3_column_text(st,3):"—",tb);
        any=1;
    }
    if(!any) printf("(no active runs)\n");
    sqlite3_finalize(st);
}

static void cmd_define(sqlite3 *db,const char *path){
    char *src=read_file(path);
    if(!src){return;}
    /* minimal parse: look for "id" and "entry" keys */
    time_t now=time(NULL);
    char flow_id[256]="";char flow_name[256]="";char entry[256]="";
    /* very simple field extraction — not a full JSON parser */
    const char *p;
    if((p=strstr(src,"\"id\""))){
        const char *q=strchr(p+4,'"');if(q){q++;const char *e=strchr(q,'"');
        if(e){size_t n=(size_t)(e-q);if(n<sizeof(flow_id)-1){strncpy(flow_id,q,n);flow_id[n]='\0';}}}
    }
    if((p=strstr(src,"\"name\""))){
        const char *q=strchr(p+6,'"');if(q){q++;const char *e=strchr(q,'"');
        if(e){size_t n=(size_t)(e-q);if(n<sizeof(flow_name)-1){strncpy(flow_name,q,n);flow_name[n]='\0';}}}
    }
    if((p=strstr(src,"\"entry\""))){
        const char *q=strchr(p+7,'"');if(q){q++;const char *e=strchr(q,'"');
        if(e){size_t n=(size_t)(e-q);if(n<sizeof(entry)-1){strncpy(entry,q,n);entry[n]='\0';}}}
    }
    if(!flow_id[0]){fprintf(stderr,"flow JSON must have \"id\" field\n");free(src);return;}
    if(!entry[0]) snprintf(entry,sizeof(entry),"start");
    if(!flow_name[0]) snprintf(flow_name,sizeof(flow_name),"%s",flow_id);

    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO flows(id,name,definition,entry_stage,created,updated)"
        " VALUES(?,?,?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,flow_id,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,flow_name,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,src,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,4,entry,-1,SQLITE_STATIC);
    sqlite3_bind_int64(st,5,(sqlite3_int64)now);
    sqlite3_bind_int64(st,6,(sqlite3_int64)now);
    sqlite3_step(st);sqlite3_finalize(st);
    printf("flow '%s' defined (id=%s entry=%s)\n",flow_name,flow_id,entry);
    free(src);
}

static void cmd_list(sqlite3 *db){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "SELECT f.id,f.name,f.entry_stage,"
        " (SELECT COUNT(*) FROM runs r WHERE r.flow_id=f.id) as nruns"
        " FROM flows f ORDER BY f.created DESC",
        -1,&st,NULL);
    printf("%-30s  %-25s  %-15s  %5s\n","ID","NAME","ENTRY","RUNS");
    while(sqlite3_step(st)==SQLITE_ROW)
        printf("%-30s  %-25s  %-15s  %5d\n",
            (const char*)sqlite3_column_text(st,0),(const char*)sqlite3_column_text(st,1),
            (const char*)sqlite3_column_text(st,2),sqlite3_column_int(st,3));
    sqlite3_finalize(st);
}

static void cmd_show(sqlite3 *db,const char *fid){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"SELECT name,entry_stage,definition FROM flows WHERE id=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,fid,-1,SQLITE_STATIC);
    if(sqlite3_step(st)!=SQLITE_ROW){fprintf(stderr,"flow not found: %s\n",fid);sqlite3_finalize(st);return;}
    printf("name  : %s\nentry : %s\n\n%s\n",
        (const char*)sqlite3_column_text(st,0),(const char*)sqlite3_column_text(st,1),
        (const char*)sqlite3_column_text(st,2));
    sqlite3_finalize(st);
}

static void cmd_run_flow(sqlite3 *db,const char *fid,const char *input){
    time_t now=time(NULL);
    /* get entry stage */
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"SELECT entry_stage FROM flows WHERE id=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,fid,-1,SQLITE_STATIC);
    if(sqlite3_step(st)!=SQLITE_ROW){
        fprintf(stderr,"flow not found: %s\n",fid);sqlite3_finalize(st);return;
    }
    char entry[64];snprintf(entry,sizeof(entry),"%s",(const char*)sqlite3_column_text(st,0));
    char eid[64];snprintf(eid,sizeof(eid),"run-%ld",(long)now);
    char run_id[256];snprintf(run_id,sizeof(run_id),"%s-%s",fid,eid);
    sqlite3_finalize(st);

    sqlite3_stmt *ins=NULL;
    sqlite3_prepare_v2(db,
        "INSERT INTO runs(id,flow_id,input,current_stage,started) VALUES(?,?,?,?,?)",
        -1,&ins,NULL);
    sqlite3_bind_text(ins,1,run_id,-1,SQLITE_STATIC);
    sqlite3_bind_text(ins,2,fid,-1,SQLITE_STATIC);
    sqlite3_bind_text(ins,3,input,-1,SQLITE_STATIC);
    sqlite3_bind_text(ins,4,entry,-1,SQLITE_STATIC);
    sqlite3_bind_int64(ins,5,(sqlite3_int64)now);
    sqlite3_step(ins);sqlite3_finalize(ins);
    printf("started run: %s\n  flow : %s\n  entry: %s\n  input: %.60s\n",
        run_id,fid,entry,input);

    /* dispatch stages: stage id == recipe code by convention;
     * traverse seq edges from entry_stage using the edges table.        */
    char cur_stage[64];snprintf(cur_stage,sizeof(cur_stage),"%s",entry);
    char cur_input[4096];snprintf(cur_input,sizeof(cur_input),"%s",input);
    int failed=0;
    for(int step=0;step<32;step++){
        /* mark stage as running */
        sqlite3_stmt *rs=NULL;
        sqlite3_prepare_v2(db,
            "INSERT OR IGNORE INTO run_stages(run_id,stage,status,started) VALUES(?,?,'running',?)",
            -1,&rs,NULL);
        sqlite3_bind_text(rs,1,run_id,-1,SQLITE_STATIC);
        sqlite3_bind_text(rs,2,cur_stage,-1,SQLITE_STATIC);
        sqlite3_bind_int64(rs,3,(sqlite3_int64)time(NULL));
        sqlite3_step(rs);sqlite3_finalize(rs);

        /* dispatch via bonfyre-runtime run-recipe (single orchestration gateway);
         * output dir becomes next stage input */
        char outdir[256];
        snprintf(outdir,sizeof(outdir),"/tmp/bf-flow-%s-%d",eid,step);
        char bfcmd[4096];
        snprintf(bfcmd,sizeof(bfcmd),
            "bonfyre-runtime run-recipe '%s' '%s' --out '%s' --quiet >/dev/null 2>&1",
            cur_stage,cur_input,outdir);
        int rc=system(bfcmd);

        /* update run_stage status */
        sqlite3_stmt *upd1=NULL;
        sqlite3_prepare_v2(db,
            "UPDATE run_stages SET status=?,done=? WHERE run_id=? AND stage=?",
            -1,&upd1,NULL);
        sqlite3_bind_text(upd1,1,rc==0?"done":"failed",-1,SQLITE_STATIC);
        sqlite3_bind_int64(upd1,2,(sqlite3_int64)time(NULL));
        sqlite3_bind_text(upd1,3,run_id,-1,SQLITE_STATIC);
        sqlite3_bind_text(upd1,4,cur_stage,-1,SQLITE_STATIC);
        sqlite3_step(upd1);sqlite3_finalize(upd1);
        printf("  stage %-20s %s\n",cur_stage,rc==0?"done":"failed");
        if(rc!=0){failed=1;break;}

        snprintf(cur_input,sizeof(cur_input),"%s",outdir);

        /* look up next seq stage from edge table */
        sqlite3_stmt *edge=NULL;
        sqlite3_prepare_v2(db,
            "SELECT to_stage FROM edges WHERE flow_id=? AND from_stage=? AND kind='seq' LIMIT 1",
            -1,&edge,NULL);
        sqlite3_bind_text(edge,1,fid,-1,SQLITE_STATIC);
        sqlite3_bind_text(edge,2,cur_stage,-1,SQLITE_STATIC);
        int has_next=(sqlite3_step(edge)==SQLITE_ROW);
        if(has_next)
            snprintf(cur_stage,sizeof(cur_stage),"%s",(const char*)sqlite3_column_text(edge,0));
        sqlite3_finalize(edge);
        if(!has_next) break;

        /* update current_stage in run record */
        sqlite3_stmt *upd2=NULL;
        sqlite3_prepare_v2(db,
            "UPDATE runs SET current_stage=? WHERE id=?",
            -1,&upd2,NULL);
        sqlite3_bind_text(upd2,1,cur_stage,-1,SQLITE_STATIC);
        sqlite3_bind_text(upd2,2,run_id,-1,SQLITE_STATIC);
        sqlite3_step(upd2);sqlite3_finalize(upd2);
    }

    /* mark run done or failed */
    sqlite3_stmt *fin=NULL;
    sqlite3_prepare_v2(db,
        "UPDATE runs SET status=?,finished=? WHERE id=?",
        -1,&fin,NULL);
    sqlite3_bind_text(fin,1,failed?"failed":"done",-1,SQLITE_STATIC);
    sqlite3_bind_int64(fin,2,(sqlite3_int64)time(NULL));
    sqlite3_bind_text(fin,3,run_id,-1,SQLITE_STATIC);
    sqlite3_step(fin);sqlite3_finalize(fin);
    if(!failed) printf("run complete: %s\n",run_id);
}

static void cmd_inspect(sqlite3 *db,const char *run_id){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "SELECT flow_id,status,current_stage,input,started,finished FROM runs WHERE id=?",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,run_id,-1,SQLITE_STATIC);
    if(sqlite3_step(st)!=SQLITE_ROW){fprintf(stderr,"run not found: %s\n",run_id);sqlite3_finalize(st);return;}
    char sb[20];time_t ts=(time_t)sqlite3_column_int64(st,4);
    strftime(sb,sizeof(sb),"%Y-%m-%d %H:%M",localtime(&ts));
    printf("run     : %s\nflow    : %s\nstatus  : %s\nstage   : %s\ninput   : %.80s\nstarted : %s\n",
        run_id,(const char*)sqlite3_column_text(st,0),(const char*)sqlite3_column_text(st,1),
        sqlite3_column_text(st,2)?(const char*)sqlite3_column_text(st,2):"—",
        sqlite3_column_text(st,3)?(const char*)sqlite3_column_text(st,3):"",sb);
    sqlite3_finalize(st);
}

static void cmd_branch(sqlite3 *db,const char *fid,const char *cond,const char *target){
    sqlite3_stmt *st=NULL;
    /* default from_stage is entry */
    sqlite3_prepare_v2(db,"SELECT entry_stage FROM flows WHERE id=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,fid,-1,SQLITE_STATIC);
    if(sqlite3_step(st)!=SQLITE_ROW){fprintf(stderr,"flow not found: %s\n",fid);sqlite3_finalize(st);return;}
    const char *from=(const char*)sqlite3_column_text(st,0);
    sqlite3_stmt *ins=NULL;
    sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO edges(flow_id,from_stage,to_stage,kind,condition) VALUES(?,?,?,?,?)",
        -1,&ins,NULL);
    sqlite3_bind_text(ins,1,fid,-1,SQLITE_STATIC);
    sqlite3_bind_text(ins,2,from,-1,SQLITE_STATIC);
    sqlite3_bind_text(ins,3,target,-1,SQLITE_STATIC);
    sqlite3_bind_text(ins,4,"branch",-1,SQLITE_STATIC);
    sqlite3_bind_text(ins,5,cond,-1,SQLITE_STATIC);
    sqlite3_step(ins);sqlite3_finalize(ins);
    sqlite3_finalize(st);
    printf("branch added: %s → %s (when: %s)\n",fid,target,cond);
}

static void cmd_export(sqlite3 *db,const char *fid){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"SELECT definition FROM flows WHERE id=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,fid,-1,SQLITE_STATIC);
    if(sqlite3_step(st)!=SQLITE_ROW){fprintf(stderr,"flow not found: %s\n",fid);sqlite3_finalize(st);return;}
    printf("%s\n",(const char*)sqlite3_column_text(st,0));
    sqlite3_finalize(st);
}

static void cmd_rm(sqlite3 *db,const char *fid){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"DELETE FROM flows WHERE id=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,fid,-1,SQLITE_STATIC);
    sqlite3_step(st);sqlite3_finalize(st);
    printf("deleted flow: %s\n",fid);
}

static void cmd_help(void){
    printf(
"bonfyre-flow %s — coroutine-native pipeline programming\n\n"
"USAGE\n"
"  bonfyre-flow <command> [args]\n\n"
"COMMANDS\n"
"  status                        show active runs + current stage\n"
"  define <file.json>            import a flow definition\n"
"  list                          list all flows\n"
"  show <flow-id>                print full flow definition\n"
"  run <flow-id> <input>         start a new flow run\n"
"  inspect <run-id>              show execution state\n"
"  branch <flow-id> <cond> <target>   add a conditional branch\n"
"  merge <flow-id> <a> <b> <target>   add a merge node\n"
"  export <flow-id>              print flow as JSON\n"
"  rm <flow-id>                  delete a flow definition\n"
"  help                          this message\n\n"
"FLOW JSON FORMAT\n"
"  {\"id\":\"my-flow\",\"name\":\"My Flow\",\"entry\":\"ingest\","
"\"stages\":[{\"id\":\"ingest\",\"recipe\":\"intake\"},...],\"edges\":[...]}\n\n"
"INTEGRATION\n"
"  bonfyre-run executes individual stages within a flow\n"
"  bonfyre-control enforces policies per-stage\n"
"  bonfyre-space holds intermediate state between stages\n\n"
"ENVIRONMENT\n"
"  BONFYRE_FLOW_DB   override DB path\n",
    VERSION);
}

int main(int argc,char **argv){
    if(argc<2||strcmp(argv[1],"help")==0||strcmp(argv[1],"--help")==0){cmd_help();return 0;}
    sqlite3 *db=open_db();
    int rc=0;const char *cmd=argv[1];
    if(strcmp(cmd,"status")==0) cmd_status(db);
    else if(strcmp(cmd,"define")==0){
        if(argc<3){fprintf(stderr,"usage: bonfyre-flow define <file.json>\n");rc=1;}
        else cmd_define(db,argv[2]);
    } else if(strcmp(cmd,"list")==0) cmd_list(db);
    else if(strcmp(cmd,"show")==0){
        if(argc<3){fprintf(stderr,"usage: bonfyre-flow show <flow-id>\n");rc=1;}
        else cmd_show(db,argv[2]);
    } else if(strcmp(cmd,"run")==0){
        if(argc<4){fprintf(stderr,"usage: bonfyre-flow run <flow-id> <input>\n");rc=1;}
        else cmd_run_flow(db,argv[2],argv[3]);
    } else if(strcmp(cmd,"inspect")==0){
        if(argc<3){fprintf(stderr,"usage: bonfyre-flow inspect <run-id>\n");rc=1;}
        else cmd_inspect(db,argv[2]);
    } else if(strcmp(cmd,"branch")==0){
        if(argc<5){fprintf(stderr,"usage: bonfyre-flow branch <flow-id> <cond> <target>\n");rc=1;}
        else cmd_branch(db,argv[2],argv[3],argv[4]);
    } else if(strcmp(cmd,"export")==0){
        if(argc<3){fprintf(stderr,"usage: bonfyre-flow export <flow-id>\n");rc=1;}
        else cmd_export(db,argv[2]);
    } else if(strcmp(cmd,"rm")==0){
        if(argc<3){fprintf(stderr,"usage: bonfyre-flow rm <flow-id>\n");rc=1;}
        else cmd_rm(db,argv[2]);
    } else {fprintf(stderr,"bonfyre-flow: unknown command: %s\n",cmd);rc=1;}
    sqlite3_close(db);return rc;
}
