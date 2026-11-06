/*
 * bonfyre-economy — cost-aware routing and budget enforcement.
 *
 * Tracks spend per recipe, enforces budget caps, routes inference to the
 * cheapest model that meets latency and quality thresholds, and produces
 * cost reports. bonfyre-control consults economy before routing any request.
 *
 * DB: ~/.local/share/bonfyre/economy.db  (override: $BONFYRE_ECONOMY_DB)
 *
 * Commands:
 *   bonfyre-economy status               — current spend vs budgets
 *   bonfyre-economy budget set <recipe> <usd>   — set/update budget cap
 *   bonfyre-economy budget show [recipe]         — show budgets
 *   bonfyre-economy route <recipe>        — recommend cheapest valid path
 *   bonfyre-economy cost estimate <recipe>  — estimate cost for a run
 *   bonfyre-economy cost record <recipe> <stage> <model> <usd>
 *   bonfyre-economy report [--last N]     — spending breakdown
 *   bonfyre-economy reset <recipe>        — zero the spent counter
 *   bonfyre-economy help                  — this message
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
#include "verification_planner.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

#define VERSION    "1.0.0"
#define DB_ENV     "BONFYRE_ECONOMY_DB"
#define DB_SUBPATH "/.local/share/bonfyre/economy.db"

static void db_path(char *buf,size_t len){
    const char *e=getenv(DB_ENV);
    if(e){snprintf(buf,len,"%s",e);return;}
    const char *h=getenv("HOME");if(!h)h="/tmp";
    snprintf(buf,len,"%s%s",h,DB_SUBPATH);
}
static void ensure_dir(const char *p) { bf_ensure_parent_dir(p); }

/* Economy must never choose a provider merely because a static price table
 * says it is cheap.  Provider/model reachability belongs to Model Commons;
 * the caller supplies this narrow readiness fact only after probing a local
 * runtime.  Without a measured route, the honest recommendation is unresolved
 * rather than silently re-entering an exhausted remote provider. */
static int local_model_ready(void) {
    const char *value = getenv("BONFYRE_ECONOMY_LOCAL_MODEL_READY");
    return value && (!strcmp(value, "1") || !strcmp(value, "true"));
}

static const char *SCHEMA=
    "PRAGMA journal_mode=WAL;"
    "PRAGMA foreign_keys=ON;"
    "CREATE TABLE IF NOT EXISTS budgets("
    "  recipe       TEXT PRIMARY KEY,"
    "  max_usd      REAL NOT NULL,"
    "  spent        REAL NOT NULL DEFAULT 0.0,"
    "  reset_period TEXT NOT NULL DEFAULT 'monthly',"
    "  updated      INTEGER NOT NULL"
    ");"
    "CREATE TABLE IF NOT EXISTS costs("
    "  id       INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  recipe   TEXT NOT NULL,"
    "  stage    TEXT NOT NULL,"
    "  model    TEXT NOT NULL,"
    "  usd      REAL NOT NULL,"
    "  tokens   INTEGER,"
    "  ts       INTEGER NOT NULL"
    ");"
    "CREATE TABLE IF NOT EXISTS routes("
    "  id           INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  recipe       TEXT NOT NULL,"
    "  chosen_model TEXT NOT NULL,"
    "  reason       TEXT,"
    "  ts           INTEGER NOT NULL"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_costs_recipe ON costs(recipe);";

static sqlite3 *open_db(void){
    char path[4096];db_path(path,sizeof(path));ensure_dir(path);
    sqlite3 *db=NULL;
    /* What this store holds is costs and money: a commit that returned stays. */
    if(bf_sqlite3_open_tier(path,&db,BF_SQLITE_DURABLE)!=SQLITE_OK){fprintf(stderr,"db error: the ledger could not be opened at full durability\n");exit(1);}
    char *err=NULL;sqlite3_exec(db,SCHEMA,NULL,NULL,&err);
    if(err){fprintf(stderr,"%s\n",err);sqlite3_free(err);exit(1);}
    return db;
}

static void cmd_status(sqlite3 *db){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "SELECT recipe,max_usd,spent,(spent/max_usd)*100.0 FROM budgets ORDER BY recipe",
        -1,&st,NULL);
    printf("%-30s  %10s  %10s  %6s\n","RECIPE","BUDGET","SPENT","% USED");
    while(sqlite3_step(st)==SQLITE_ROW)
        printf("%-30s  %10.4f  %10.4f  %5.1f%%\n",
            (const char*)sqlite3_column_text(st,0),sqlite3_column_double(st,1),
            sqlite3_column_double(st,2),sqlite3_column_double(st,3));
    sqlite3_finalize(st);
}

static void cmd_budget_set(sqlite3 *db,const char *recipe,double usd){
    time_t now=time(NULL);
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO budgets(recipe,max_usd,updated) VALUES(?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,recipe,-1,SQLITE_STATIC);
    sqlite3_bind_double(st,2,usd);
    sqlite3_bind_int64(st,3,(sqlite3_int64)now);
    sqlite3_step(st);sqlite3_finalize(st);
    printf("budget set: %s → $%.4f\n",recipe,usd);
}

static void cmd_budget_show(sqlite3 *db,const char *recipe){
    sqlite3_stmt *st=NULL;
    if(recipe){
        sqlite3_prepare_v2(db,"SELECT recipe,max_usd,spent,reset_period FROM budgets WHERE recipe=?",-1,&st,NULL);
        sqlite3_bind_text(st,1,recipe,-1,SQLITE_STATIC);
    } else {
        sqlite3_prepare_v2(db,"SELECT recipe,max_usd,spent,reset_period FROM budgets ORDER BY recipe",-1,&st,NULL);
    }
    printf("%-30s  %10s  %10s  %s\n","RECIPE","MAX_USD","SPENT","RESET");
    while(sqlite3_step(st)==SQLITE_ROW)
        printf("%-30s  %10.4f  %10.4f  %s\n",
            (const char*)sqlite3_column_text(st,0),sqlite3_column_double(st,1),
            sqlite3_column_double(st,2),(const char*)sqlite3_column_text(st,3));
    sqlite3_finalize(st);
}

static void cmd_route(sqlite3 *db,const char *recipe){
    /* Prefer an observed, within-budget realization.  A historical cost is a
     * price observation, not availability; an explicit local readiness probe
     * is the only zero-price path this organ may recommend. */
    sqlite3_stmt *st=NULL;
    double remaining=9999.0;
    sqlite3_prepare_v2(db,"SELECT max_usd-spent FROM budgets WHERE recipe=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,recipe,-1,SQLITE_STATIC);
    if(sqlite3_step(st)==SQLITE_ROW) remaining=sqlite3_column_double(st,0);
    sqlite3_finalize(st);

    printf("budget remaining for '%s': $%.4f\n",recipe,remaining);

    /* First: check DB cost history for this recipe — prefer the cheapest model
     * that has been actually observed, rather than the static cost table. */
    char chosen_buf[256]; chosen_buf[0]='\0';
    const char *chosen="unresolved"; double chosen_cost=-1.0;
    const char *reason="no observed affordable realization and no probed local runtime";
    {
        sqlite3_stmt *hs=NULL;
        sqlite3_prepare_v2(db,
            "SELECT model,AVG(usd) AS avg_usd FROM costs WHERE recipe=?"
            " GROUP BY model ORDER BY avg_usd ASC LIMIT 1",
            -1,&hs,NULL);
        sqlite3_bind_text(hs,1,recipe,-1,SQLITE_STATIC);
        if(sqlite3_step(hs)==SQLITE_ROW){
            const char *m=(const char*)sqlite3_column_text(hs,0);
            double c=sqlite3_column_double(hs,1);
            if(m&&c<=remaining){
                snprintf(chosen_buf,sizeof(chosen_buf),"%s",m);
                chosen=chosen_buf; chosen_cost=c;
                reason="cheapest observed cost within this recipe budget";
            }
        }
        sqlite3_finalize(hs);
    }
    if(chosen_cost < 0.0 && local_model_ready()){
        chosen="bonfyre-local";
        chosen_cost=0.0;
        reason="local runtime was explicitly probed ready; provider-price basis is zero, physical carrying cost remains outside this estimate";
    }
    if(chosen_cost < 0.0)
        printf("recommended model : unresolved (no lawful route recommendation)\n");
    else
        printf("recommended model : %s (est. provider cost/call: $%.6f)\n",chosen,chosen_cost);
    printf("route basis       : %s\n",reason);

    time_t now=time(NULL);
    sqlite3_stmt *ins=NULL;
    sqlite3_prepare_v2(db,"INSERT INTO routes(recipe,chosen_model,reason,ts) VALUES(?,?,?,?)",-1,&ins,NULL);
    sqlite3_bind_text(ins,1,recipe,-1,SQLITE_STATIC);
    sqlite3_bind_text(ins,2,chosen,-1,SQLITE_STATIC);
    sqlite3_bind_text(ins,3,reason,-1,SQLITE_STATIC);
    sqlite3_bind_int64(ins,4,(sqlite3_int64)now);
    sqlite3_step(ins);sqlite3_finalize(ins);
}

static void cmd_cost_estimate(sqlite3 *db,const char *recipe){
    /* average of last 10 runs for this recipe */
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "SELECT model,AVG(usd),COUNT(*) FROM costs WHERE recipe=?"
        " GROUP BY model ORDER BY AVG(usd) ASC",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,recipe,-1,SQLITE_STATIC);
    printf("cost estimate for '%s':\n",recipe);
    printf("  %-20s  %10s  %6s\n","MODEL","AVG USD","RUNS");
    int any=0;
    while(sqlite3_step(st)==SQLITE_ROW){
        printf("  %-20s  %10.6f  %6d\n",
            (const char*)sqlite3_column_text(st,0),sqlite3_column_double(st,1),
            sqlite3_column_int(st,2));
        any=1;
    }
    if(!any) printf("  (no cost history for recipe '%s')\n",recipe);
    sqlite3_finalize(st);
}

static void cmd_cost_record(sqlite3 *db,const char *recipe,const char *stage,const char *model,double usd){
    time_t now=time(NULL);
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,
        "INSERT INTO costs(recipe,stage,model,usd,ts) VALUES(?,?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,recipe,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,stage,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,model,-1,SQLITE_STATIC);
    sqlite3_bind_double(st,4,usd);
    sqlite3_bind_int64(st,5,(sqlite3_int64)now);
    sqlite3_step(st);sqlite3_finalize(st);
    /* update spent */
    sqlite3_prepare_v2(db,"UPDATE budgets SET spent=spent+? WHERE recipe=?",-1,&st,NULL);
    sqlite3_bind_double(st,1,usd);
    sqlite3_bind_text(st,2,recipe,-1,SQLITE_STATIC);
    sqlite3_step(st);sqlite3_finalize(st);
    printf("recorded: %s/%s/%s $%.6f\n",recipe,stage,model,usd);
}

static void cmd_report(sqlite3 *db,int last_n){
    sqlite3_stmt *st=NULL;
    char sql[512];
    snprintf(sql,sizeof(sql),
        "SELECT recipe,stage,model,usd,ts FROM costs ORDER BY ts DESC LIMIT %d",last_n>0?last_n:50);
    sqlite3_prepare_v2(db,sql,-1,&st,NULL);
    printf("%-20s  %-20s  %-15s  %10s  %s\n","RECIPE","STAGE","MODEL","USD","WHEN");
    while(sqlite3_step(st)==SQLITE_ROW){
        time_t ts=(time_t)sqlite3_column_int64(st,4);
        char tb[20];strftime(tb,sizeof(tb),"%Y-%m-%d %H:%M",localtime(&ts));
        printf("%-20s  %-20s  %-15s  %10.6f  %s\n",
            (const char*)sqlite3_column_text(st,0),(const char*)sqlite3_column_text(st,1),
            (const char*)sqlite3_column_text(st,2),sqlite3_column_double(st,3),tb);
    }
    sqlite3_finalize(st);
}

static void cmd_reset(sqlite3 *db,const char *recipe){
    sqlite3_stmt *st=NULL;
    sqlite3_prepare_v2(db,"UPDATE budgets SET spent=0 WHERE recipe=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,recipe,-1,SQLITE_STATIC);
    sqlite3_step(st);sqlite3_finalize(st);
    printf("reset: %s spent=$0.00\n",recipe);
}

static void cmd_help(void){
    printf(
"bonfyre-economy %s — cost-aware routing and budget enforcement\n\n"
"USAGE\n"
"  bonfyre-economy <command> [args]\n\n"
"COMMANDS\n"
"  status                        current spend vs caps\n"
"  budget set <recipe> <usd>     set/update budget cap for a recipe\n"
"  budget show [recipe]          show budget(s)\n"
"  route <recipe>                recommend cheapest model within budget\n"
"  cost estimate <recipe>        estimate cost based on history\n"
"  cost record <recipe> <stage> <model> <usd>   record actual cost\n"
"  report [--last N]             cost history (default: 50 rows)\n"
"  reset <recipe>                zero the spent counter\n"
"  verify case|need|offer|covers|plan  bounded verification cost planning\n"
"  layer <artifact_id> [--op verify|materialize|compose|run]\n"
"  help                          this message\n\n"
"ROUTING BASIS\n"
"  Reuses observed within-budget recipe costs. A local route is emitted only\n"
"  when BONFYRE_ECONOMY_LOCAL_MODEL_READY=1 was supplied after a runtime probe.\n"
"  Otherwise the route remains unresolved; this command never invents a remote fallback.\n\n"
"INTEGRATION\n"
"  bonfyre-control calls: bonfyre-economy route <recipe>\n"
"  bonfyre-run calls:     bonfyre-economy cost record ...\n\n"
"ENVIRONMENT\n"
"  BONFYRE_ECONOMY_DB   override DB path\n",
    VERSION);
}

int main(int argc,char **argv){
    /* --db FILE before the command names the store, like the other organs
     * that keep one; it takes the place of the environment variable. */
    if (argc >= 3 && !strcmp(argv[1], "--db")) { setenv(DB_ENV, argv[2], 1); argv += 2; argc -= 2; }
    if(argc<2){cmd_help();return 0;}
    enum {
        EC_HELP, EC_BUDGET, EC_COST, EC_LAYER, EC_REPORT, EC_RESET, EC_ROUTE, EC_STATUS, EC_VERIFY,
    };
    static const BfCommandEntry economy_commands[] = {
        {"--help", EC_HELP, 0, NULL},
        {"budget", EC_BUDGET, 0, NULL},
        {"cost", EC_COST, 0, NULL},
        {"help", EC_HELP, 0, NULL},
        {"layer", EC_LAYER, 0, NULL},
        {"report", EC_REPORT, 0, NULL},
        {"reset", EC_RESET, 0, NULL},
        {"route", EC_ROUTE, 0, NULL},
        {"status", EC_STATUS, 0, NULL},
        {"verify", EC_VERIFY, 0, NULL},
    };
    const BfCommandEntry *command = bf_command_lookup(economy_commands,
        sizeof(economy_commands) / sizeof(economy_commands[0]), argv[1]);
    if(command && command->id == EC_HELP){cmd_help();return 0;}
    if(command && command->id == EC_LAYER && argc>=3){
        const char *root = NULL, *op = "verify";
        char *json = NULL, *out = NULL;
        for(int i=1;i<argc-1;i++){
            if(strcmp(argv[i],"--root")==0){ root=argv[i+1]; }
            if(strcmp(argv[i],"--op")==0){ op=argv[i+1]; }
        }
        if (bf_layer_load_json(root, argv[2], &json) != 0) return 1;
        if (bf_layer_economy_json(json, op, &out) != 0) { free(json); return 1; }
        puts(out);
        free(out);
        free(json);
        return 0;
    }
    sqlite3 *db=open_db();
    int rc=0;const char *cmd=argv[1];
    if(command && command->id == EC_VERIFY) rc=bf_verification_command(db,argc,argv);
    else if(command && command->id == EC_STATUS) cmd_status(db);
    else if(command && command->id == EC_BUDGET){
        if(argc<3){fprintf(stderr,"budget: set|show\n");rc=1;}
        else if(strcmp(argv[2],"set")==0){
            if(argc<5){fprintf(stderr,"usage: budget set <recipe> <usd>\n");rc=1;}
            else cmd_budget_set(db,argv[3],atof(argv[4]));
        } else if(strcmp(argv[2],"show")==0)
            cmd_budget_show(db,argc>3?argv[3]:NULL);
        else {fprintf(stderr,"budget: unknown sub-command %s\n",argv[2]);rc=1;}
    } else if(command && command->id == EC_ROUTE){
        if(argc<3){fprintf(stderr,"usage: bonfyre-economy route <recipe>\n");rc=1;}
        else cmd_route(db,argv[2]);
    } else if(command && command->id == EC_COST){
        if(argc<3){fprintf(stderr,"cost: estimate|record\n");rc=1;}
        else if(strcmp(argv[2],"estimate")==0){
            if(argc<4){fprintf(stderr,"usage: cost estimate <recipe>\n");rc=1;}
            else cmd_cost_estimate(db,argv[3]);
        } else if(strcmp(argv[2],"record")==0){
            if(argc<7){fprintf(stderr,"usage: cost record <recipe> <stage> <model> <usd>\n");rc=1;}
            else cmd_cost_record(db,argv[3],argv[4],argv[5],atof(argv[6]));
        } else {fprintf(stderr,"cost: unknown sub-command %s\n",argv[2]);rc=1;}
    } else if(command && command->id == EC_REPORT){
        int n=50;
        if(argc>3&&strcmp(argv[2],"--last")==0) n=atoi(argv[3]);
        cmd_report(db,n);
    } else if(command && command->id == EC_RESET){
        if(argc<3){fprintf(stderr,"usage: bonfyre-economy reset <recipe>\n");rc=1;}
        else cmd_reset(db,argv[2]);
    } else {fprintf(stderr,"bonfyre-economy: unknown command: %s\n",cmd);rc=1;}
    sqlite3_close(db);return rc;
}
