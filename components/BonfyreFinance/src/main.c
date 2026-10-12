/*
 * BonfyreFinance — service arbitrage, labor pipeline, and bundle pricing engine.
 *
 * Replaces: ServiceArbitrageHub + AIOverseasLaborPipeline + RepackagedServiceMarketplace
 *
 * Three interconnected sub-systems in one binary:
 *   services  — track buy/sell spread, log jobs, margin reports, find best arbitrage
 *   jobs      — hybrid fulfillment (AI + human QA), cost tracking, handoff to pipeline
 *   bundles   — component-based pricing, bundle design, buyer-facing export
 *
 * Storage: SQLite (services, service_jobs, pipeline_jobs, bundles, components).
 *
 * Usage:
 *   bonfyre-finance service add --name N --buy B --sell S --source SRC [--category C]
 *   bonfyre-finance service list
 *   bonfyre-finance service opportunities
 *   bonfyre-finance job create --input FILE --type TYPE [--sell-price P]
 *   bonfyre-finance job list [--status S]
 *   bonfyre-finance job review --id ID --result pass|fail --reviewer R [--cost C]
 *   bonfyre-finance job handoff --id ID [--type T]
 *   bonfyre-finance margin
 *   bonfyre-finance bundle create --name N --components A,B,C --price P [--target T]
 *   bonfyre-finance bundle list
 *   bonfyre-finance bundle compare --name N
 *   bonfyre-finance bundle export --name N
 *   bonfyre-finance status
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <sqlite3.h>

#define MAX_PATH   2048
#define MAX_COMPS  32
#define MAX_NAME   256

/* ── Built-in service components ──────────────────────────────────────── */

typedef struct {
    const char *name;
    double cost;
    double price;
    const char *desc;
} Component;

static const Component COMPONENTS[] = {
    {"transcription",  1.50, 12.00, "Clean formatted transcript"},
    {"summary",        0.50,  5.00, "Executive summary with key points"},
    {"action-items",   0.30,  3.00, "Extracted action items"},
    {"speaker-labels", 0.80,  4.00, "Speaker identification and labeling"},
    {"timestamps",     0.20,  2.00, "Timestamp markers throughout"},
    {"human-qa",       2.00,  5.00, "Human quality review pass"},
    {"rush-delivery",  0.00,  8.00, "Same-hour delivery"},
    {"multi-format",   0.30,  3.00, "Delivery in TXT + DOCX + PDF"},
};
#define NUM_COMPONENTS (int)(sizeof(COMPONENTS)/sizeof(COMPONENTS[0]))

static const Component *find_component(const char *name) {
    for (int i=0;i<NUM_COMPONENTS;i++)
        if (strcmp(COMPONENTS[i].name,name)==0) return &COMPONENTS[i];
    return NULL;
}

/* ── Utility ──────────────────────────────────────────────────────────── */

static void iso_timestamp(char *buf, size_t sz) {
    time_t now=time(NULL); struct tm t; gmtime_r(&now,&t);
    strftime(buf,sz,"%Y-%m-%dT%H:%M:%SZ",&t);
}

static long file_size(const char *path) {
    struct stat st;
    if (stat(path,&st)!=0) return -1;
    return (long)st.st_size;
}

/* ── Database ─────────────────────────────────────────────────────────── */

static const char *SCHEMA_SQL =
    /* Services (arbitrage tracking) */
    "CREATE TABLE IF NOT EXISTS services ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  name TEXT NOT NULL UNIQUE,"
    "  category TEXT NOT NULL DEFAULT 'general',"
    "  buy_price REAL NOT NULL,"
    "  sell_price REAL NOT NULL,"
    "  source TEXT NOT NULL,"
    "  created_at TEXT NOT NULL"
    ");"
    /* Service jobs (completed fulfillment) */
    "CREATE TABLE IF NOT EXISTS service_jobs ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  service_name TEXT NOT NULL,"
    "  actual_cost REAL NOT NULL,"
    "  sold_for REAL NOT NULL,"
    "  quality TEXT NOT NULL DEFAULT 'pass',"
    "  notes TEXT DEFAULT '',"
    "  completed_at TEXT NOT NULL,"
    "  handoff_job_id INTEGER"
    ");"
    /* Pipeline jobs (AI + human QA) */
    "CREATE TABLE IF NOT EXISTS pipeline_jobs ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  input_file TEXT NOT NULL,"
    "  job_type TEXT NOT NULL DEFAULT 'transcription',"
    "  created_at TEXT NOT NULL,"
    "  status TEXT NOT NULL DEFAULT 'pending',"
    "  ai_cost REAL DEFAULT 0.0,"
    "  review_cost REAL DEFAULT 0.0,"
    "  sell_price REAL DEFAULT 0.0,"
    "  reviewer TEXT,"
    "  qa_result TEXT,"
    "  qa_notes TEXT,"
    "  reviewed_at TEXT,"
    "  output_file TEXT,"
    "  source_system TEXT,"
    "  source_job_id INTEGER,"
    "  source_service TEXT"
    ");"
    /* Bundles (packaged offerings) */
    "CREATE TABLE IF NOT EXISTS bundles ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  name TEXT NOT NULL UNIQUE,"
    "  components TEXT NOT NULL,"
    "  sell_price REAL NOT NULL,"
    "  total_cost REAL NOT NULL,"
    "  alacarte_total REAL NOT NULL,"
    "  target TEXT DEFAULT 'general',"
    "  created_at TEXT NOT NULL"
    ");";

static sqlite3 *open_db(const char *path) {
    sqlite3 *db;
    if (sqlite3_open(path,&db)!=SQLITE_OK){
        fprintf(stderr,"Cannot open %s: %s\n",path,sqlite3_errmsg(db));
        return NULL;
    }
    char *err=NULL;
    if (sqlite3_exec(db,SCHEMA_SQL,NULL,NULL,&err)!=SQLITE_OK){
        fprintf(stderr,"Schema error: %s\n",err);
        sqlite3_free(err); sqlite3_close(db); return NULL;
    }
    return db;
}

/* ── Default DB path ──────────────────────────────────────────────────── */

static const char *default_db(void) {
    static char path[MAX_PATH];
    const char *home=getenv("HOME");
    if (home) snprintf(path,sizeof(path),"%s/.local/share/bonfyre/finance.db",home);
    else snprintf(path,sizeof(path),"finance.db");
    /* Ensure directory exists */
    char dir[MAX_PATH];
    snprintf(dir,sizeof(dir),"%s/.local/share/bonfyre",home?home:".");
    mkdir(dir,0755);
    return path;
}

/* ── Service commands ─────────────────────────────────────────────────── */

static int cmd_service_add(sqlite3 *db, const char *name, double buy, double sell,
                           const char *source, const char *category) {
    char ts[64]; iso_timestamp(ts,sizeof(ts));
    if (!category) category="general";
    double margin=sell-buy;
    double pct=sell>0?(margin/sell*100):0;

    sqlite3_stmt *st;
    int rc=sqlite3_prepare_v2(db,
        "INSERT INTO services (name,category,buy_price,sell_price,source,created_at) VALUES (?,?,?,?,?,?)",
        -1,&st,NULL);
    if (rc!=SQLITE_OK){ fprintf(stderr,"SQL error\n"); return 1; }
    sqlite3_bind_text(st,1,name,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,category,-1,SQLITE_STATIC);
    sqlite3_bind_double(st,3,buy);
    sqlite3_bind_double(st,4,sell);
    sqlite3_bind_text(st,5,source,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,6,ts,-1,SQLITE_STATIC);

    if (sqlite3_step(st)!=SQLITE_DONE) {
        /* Unique constraint — update instead */
        sqlite3_finalize(st);
        sqlite3_prepare_v2(db,
            "UPDATE services SET category=?,buy_price=?,sell_price=?,source=? WHERE name=?",
            -1,&st,NULL);
        sqlite3_bind_text(st,1,category,-1,SQLITE_STATIC);
        sqlite3_bind_double(st,2,buy);
        sqlite3_bind_double(st,3,sell);
        sqlite3_bind_text(st,4,source,-1,SQLITE_STATIC);
        sqlite3_bind_text(st,5,name,-1,SQLITE_STATIC);
        sqlite3_step(st);
        sqlite3_finalize(st);
        printf("Updated: %s\n",name);
        return 0;
    }
    sqlite3_finalize(st);
    printf("Added: %s\n",name);
    printf("  Buy:    $%.2f\n",buy);
    printf("  Sell:   $%.2f\n",sell);
    printf("  Margin: $%.2f (%.0f%%)\n",margin,pct);
    printf("  Source: %s\n",source);
    return 0;
}

static int cmd_service_list(sqlite3 *db) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT name,buy_price,sell_price,source FROM services ORDER BY sell_price-buy_price DESC",
        -1,&st,NULL);
    printf("%-28s %-8s %-8s %-8s %-6s %s\n","Service","Buy","Sell","Margin","%","Source");
    printf("------------------------------------------------------------------------\n");
    int count=0;
    while (sqlite3_step(st)==SQLITE_ROW) {
        const char *name=(const char*)sqlite3_column_text(st,0);
        double buy=sqlite3_column_double(st,1);
        double sell=sqlite3_column_double(st,2);
        const char *source=(const char*)sqlite3_column_text(st,3);
        double margin=sell-buy;
        double pct=sell>0?(margin/sell*100):0;
        printf("%-28s $%-7.2f $%-7.2f $%-7.2f %-5.0f%% %s\n",name,buy,sell,margin,pct,source);
        count++;
    }
    sqlite3_finalize(st);
    if (!count) printf("No services tracked. Use 'service add' to create one.\n");
    return 0;
}

static int cmd_service_opportunities(sqlite3 *db) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT name,buy_price,sell_price,source FROM services ORDER BY (sell_price-buy_price)/sell_price DESC",
        -1,&st,NULL);
    printf("## Best Arbitrage Opportunities\n\n");
    printf("%-4s %-28s %-10s %-10s %s\n","#","Service","Spread","Margin%","Source");
    printf("------------------------------------------------------------\n");
    int i=1;
    while (sqlite3_step(st)==SQLITE_ROW) {
        const char *name=(const char*)sqlite3_column_text(st,0);
        double buy=sqlite3_column_double(st,1);
        double sell=sqlite3_column_double(st,2);
        const char *source=(const char*)sqlite3_column_text(st,3);
        double spread=sell-buy;
        double pct=sell>0?(spread/sell*100):0;
        printf("%-4d %-28s $%-9.2f %-9.0f%% %s\n",i++,name,spread,pct,source);
    }
    sqlite3_finalize(st);
    return 0;
}

/* ── Service job commands ─────────────────────────────────────────────── */

static int cmd_job_log(sqlite3 *db, const char *service, double cost,
                       double sold, const char *quality, const char *notes) {
    /* Verify service exists */
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,"SELECT id FROM services WHERE name=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,service,-1,SQLITE_STATIC);
    if (sqlite3_step(st)!=SQLITE_ROW){
        fprintf(stderr,"Service not found: %s\n",service);
        sqlite3_finalize(st); return 1;
    }
    sqlite3_finalize(st);

    char ts[64]; iso_timestamp(ts,sizeof(ts));
    if (!quality) quality="pass";
    if (!notes) notes="";
    double margin=sold-cost;
    double pct=sold>0?(margin/sold*100):0;

    sqlite3_prepare_v2(db,
        "INSERT INTO service_jobs (service_name,actual_cost,sold_for,quality,notes,completed_at) VALUES (?,?,?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,service,-1,SQLITE_STATIC);
    sqlite3_bind_double(st,2,cost);
    sqlite3_bind_double(st,3,sold);
    sqlite3_bind_text(st,4,quality,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,5,notes,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,6,ts,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);

    printf("Job logged: %s\n",service);
    printf("  Cost:    $%.2f\n",cost);
    printf("  Sold:    $%.2f\n",sold);
    printf("  Margin:  $%.2f (%.0f%%)\n",margin,pct);
    printf("  Quality: %s\n",quality);
    return 0;
}

/* ── Pipeline job commands ────────────────────────────────────────────── */

static int cmd_pipeline_create(sqlite3 *db, const char *input, const char *type,
                               double sell_price_override) {
    long sz=file_size(input);
    if (sz<0){ fprintf(stderr,"Input not found: %s\n",input); return 1; }

    double ai_cost=(double)sz/1024.0*0.01;
    if (ai_cost<0.50) ai_cost=0.50;

    /* Default sell prices */
    double sell;
    if (sell_price_override>0) sell=sell_price_override;
    else if (strcmp(type,"summary")==0) sell=20.0;
    else if (strcmp(type,"full-deliverable")==0) sell=35.0;
    else sell=15.0;

    char ts[64]; iso_timestamp(ts,sizeof(ts));
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "INSERT INTO pipeline_jobs (input_file,job_type,created_at,ai_cost,sell_price) VALUES (?,?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,input,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,type,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,ts,-1,SQLITE_STATIC);
    sqlite3_bind_double(st,4,ai_cost);
    sqlite3_bind_double(st,5,sell);
    sqlite3_step(st); sqlite3_finalize(st);

    int job_id=(int)sqlite3_last_insert_rowid(db);
    printf("Created job #%d: %s\n",job_id,type);
    printf("  AI cost estimate: $%.2f\n",ai_cost);
    printf("  Sell price: $%.2f\n",sell);
    printf("  Status: pending review\n");
    return 0;
}

static int cmd_pipeline_list(sqlite3 *db, const char *status_filter) {
    sqlite3_stmt *st;
    if (status_filter)
        sqlite3_prepare_v2(db,
            "SELECT id,job_type,status,qa_result,reviewer,ai_cost,review_cost,sell_price,input_file "
            "FROM pipeline_jobs WHERE status=? ORDER BY created_at DESC",-1,&st,NULL),
        sqlite3_bind_text(st,1,status_filter,-1,SQLITE_STATIC);
    else
        sqlite3_prepare_v2(db,
            "SELECT id,job_type,status,qa_result,reviewer,ai_cost,review_cost,sell_price,input_file "
            "FROM pipeline_jobs ORDER BY created_at DESC",-1,&st,NULL);

    printf("%-5s %-18s %-10s %-8s %-12s %-10s %s\n","ID","Type","Status","QA","Reviewer","Margin","File");
    printf("-------------------------------------------------------------------------------------\n");
    int count=0;
    while (sqlite3_step(st)==SQLITE_ROW) {
        int id=sqlite3_column_int(st,0);
        const char *type=(const char*)sqlite3_column_text(st,1);
        const char *status=(const char*)sqlite3_column_text(st,2);
        const char *qa=sqlite3_column_text(st,3)?(const char*)sqlite3_column_text(st,3):"-";
        const char *reviewer=sqlite3_column_text(st,4)?(const char*)sqlite3_column_text(st,4):"-";
        double ai=sqlite3_column_double(st,5);
        double review=sqlite3_column_double(st,6);
        double sell=sqlite3_column_double(st,7);
        const char *file=(const char*)sqlite3_column_text(st,8);
        /* Extract filename */
        const char *slash=strrchr(file,'/');
        const char *fname=slash?slash+1:file;
        double margin=sell-ai-review;
        printf("%-5d %-18s %-10s %-8s %-12s $%-9.2f %s\n",id,type,status,qa,reviewer,margin,fname);
        count++;
    }
    sqlite3_finalize(st);
    if (!count) printf("No jobs found.\n");
    return 0;
}

static int cmd_pipeline_review(sqlite3 *db, int job_id, const char *result,
                               const char *reviewer, const char *notes, double cost) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,"SELECT id FROM pipeline_jobs WHERE id=?",-1,&st,NULL);
    sqlite3_bind_int(st,1,job_id);
    if (sqlite3_step(st)!=SQLITE_ROW){
        fprintf(stderr,"Job #%d not found.\n",job_id);
        sqlite3_finalize(st); return 1;
    }
    sqlite3_finalize(st);

    char ts[64]; iso_timestamp(ts,sizeof(ts));
    if (cost<=0) cost=2.0;
    if (!notes) notes="";
    const char *new_status=strcmp(result,"pass")==0?"completed":"needs-rework";

    sqlite3_prepare_v2(db,
        "UPDATE pipeline_jobs SET status=?,qa_result=?,qa_notes=?,reviewer=?,review_cost=?,reviewed_at=? WHERE id=?",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,new_status,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,result,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,notes,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,4,reviewer,-1,SQLITE_STATIC);
    sqlite3_bind_double(st,5,cost);
    sqlite3_bind_text(st,6,ts,-1,SQLITE_STATIC);
    sqlite3_bind_int(st,7,job_id);
    sqlite3_step(st); sqlite3_finalize(st);

    printf("Job #%d: %s by %s\n",job_id,result,reviewer);
    if (notes[0]) printf("  Notes: %s\n",notes);
    printf("  Review cost: $%.2f\n",cost);
    printf("  Status: %s\n",new_status);
    return 0;
}

static int cmd_handoff(sqlite3 *db, int job_id, const char *type) {
    if (!type) type="full-deliverable";
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT id,service_name,actual_cost,sold_for,quality,notes,handoff_job_id FROM service_jobs WHERE id=?",
        -1,&st,NULL);
    sqlite3_bind_int(st,1,job_id);
    if (sqlite3_step(st)!=SQLITE_ROW){
        fprintf(stderr,"Service job #%d not found.\n",job_id);
        sqlite3_finalize(st); return 1;
    }
    const char *svc=(const char*)sqlite3_column_text(st,1);
    double cost=sqlite3_column_double(st,2);
    double sold=sqlite3_column_double(st,3);
    if (sqlite3_column_type(st,6)!=SQLITE_NULL){
        int existing=sqlite3_column_int(st,6);
        fprintf(stderr,"Job #%d already handed off as pipeline job #%d.\n",job_id,existing);
        sqlite3_finalize(st); return 1;
    }

    char svc_copy[MAX_NAME];
    snprintf(svc_copy,sizeof(svc_copy),"%s",svc);
    sqlite3_finalize(st);

    /* Create pipeline job from service job */
    char ts[64]; iso_timestamp(ts,sizeof(ts));
    char input_ref[MAX_PATH];
    snprintf(input_ref,sizeof(input_ref),"arbitrage://%s/job-%d",svc_copy,job_id);

    sqlite3_prepare_v2(db,
        "INSERT INTO pipeline_jobs (input_file,job_type,created_at,ai_cost,sell_price,"
        "source_system,source_job_id,source_service) VALUES (?,?,?,?,?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,input_ref,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,type,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,ts,-1,SQLITE_STATIC);
    sqlite3_bind_double(st,4,cost);
    sqlite3_bind_double(st,5,sold);
    sqlite3_bind_text(st,6,"ServiceArbitrage",-1,SQLITE_STATIC);
    sqlite3_bind_int(st,7,job_id);
    sqlite3_bind_text(st,8,svc_copy,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);

    int pipeline_id=(int)sqlite3_last_insert_rowid(db);

    /* Update handoff pointer */
    sqlite3_prepare_v2(db,"UPDATE service_jobs SET handoff_job_id=? WHERE id=?",-1,&st,NULL);
    sqlite3_bind_int(st,1,pipeline_id);
    sqlite3_bind_int(st,2,job_id);
    sqlite3_step(st); sqlite3_finalize(st);

    printf("Handed off service job #%d -> pipeline job #%d\n",job_id,pipeline_id);
    printf("  Service: %s\n",svc_copy);
    printf("  Type: %s\n",type);
    printf("  Sell price: $%.2f\n",sold);
    return 0;
}

/* ── Margin report ────────────────────────────────────────────────────── */

static int cmd_margin(sqlite3 *db) {
    /* Service jobs margin */
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT DISTINCT service_name FROM service_jobs",-1,&st,NULL);
    int has_svc=0;
    printf("## Service Margins\n\n");
    printf("%-28s %-6s %-10s %-10s %-10s %-8s %s\n","Service","Jobs","Revenue","Cost","Margin","Avg %","QA Fail");
    printf("-------------------------------------------------------------------------------------\n");
    double total_rev=0,total_cost=0;
    while (sqlite3_step(st)==SQLITE_ROW) {
        has_svc=1;
        const char *name=(const char*)sqlite3_column_text(st,0);
        sqlite3_stmt *j;
        sqlite3_prepare_v2(db,
            "SELECT actual_cost,sold_for,quality FROM service_jobs WHERE service_name=?",-1,&j,NULL);
        sqlite3_bind_text(j,1,name,-1,SQLITE_STATIC);
        int count=0,fails=0; double rev=0,cost=0;
        while (sqlite3_step(j)==SQLITE_ROW){
            cost+=sqlite3_column_double(j,0);
            rev+=sqlite3_column_double(j,1);
            const char *q=(const char*)sqlite3_column_text(j,2);
            if (q && strcmp(q,"fail")==0) fails++;
            count++;
        }
        sqlite3_finalize(j);
        double margin=rev-cost;
        double pct=rev>0?(margin/rev*100):0;
        total_rev+=rev; total_cost+=cost;
        printf("%-28s %-6d $%-9.2f $%-9.2f $%-9.2f %-7.0f%% %d\n",name,count,rev,cost,margin,pct,fails);
    }
    sqlite3_finalize(st);
    if (has_svc){
        double tm=total_rev-total_cost;
        double tp=total_rev>0?(tm/total_rev*100):0;
        printf("-------------------------------------------------------------------------------------\n");
        printf("%-35s $%-9.2f $%-9.2f $%-9.2f %.0f%%\n","TOTAL",total_rev,total_cost,tm,tp);
    } else {
        printf("No service jobs logged.\n");
    }

    /* Pipeline jobs margin */
    printf("\n## Pipeline Margins\n\n");
    sqlite3_prepare_v2(db,
        "SELECT id,job_type,sell_price,ai_cost,review_cost FROM pipeline_jobs WHERE status='completed' ORDER BY created_at DESC",
        -1,&st,NULL);
    printf("%-5s %-18s %-8s %-8s %-8s %-8s %s\n","ID","Type","Sell","AI","Review","Margin","Margin%");
    printf("-----------------------------------------------------------------\n");
    double p_rev=0,p_ai=0,p_review=0;
    int p_count=0;
    while (sqlite3_step(st)==SQLITE_ROW) {
        int id=sqlite3_column_int(st,0);
        const char *type=(const char*)sqlite3_column_text(st,1);
        double sell=sqlite3_column_double(st,2);
        double ai=sqlite3_column_double(st,3);
        double review=sqlite3_column_double(st,4);
        double margin=sell-ai-review;
        double pct=sell>0?(margin/sell*100):0;
        p_rev+=sell; p_ai+=ai; p_review+=review; p_count++;
        printf("%-5d %-18s $%-7.2f $%-7.2f $%-7.2f $%-7.2f %.0f%%\n",id,type,sell,ai,review,margin,pct);
    }
    sqlite3_finalize(st);
    if (p_count){
        double pm=p_rev-p_ai-p_review;
        double pp=p_rev>0?(pm/p_rev*100):0;
        printf("-----------------------------------------------------------------\n");
        printf("%-24s $%-7.2f $%-7.2f $%-7.2f $%-7.2f %.0f%%\n","TOTAL",p_rev,p_ai,p_review,pm,pp);
        printf("\n  Jobs: %d | Avg margin: $%.2f/job\n",p_count,pm/p_count);
    } else {
        printf("No completed pipeline jobs.\n");
    }
    return 0;
}

/* ── Bundle commands ──────────────────────────────────────────────────── */

static int cmd_bundle_create(sqlite3 *db, const char *name, const char *components_csv,
                             double sell_price, const char *target) {
    if (!target) target="general";

    /* Parse and validate components */
    char csv_copy[MAX_PATH];
    snprintf(csv_copy,sizeof(csv_copy),"%s",components_csv);
    char *parts[MAX_COMPS];
    int nparts=0;
    char *tok=strtok(csv_copy,",");
    while (tok && nparts<MAX_COMPS){
        while (*tok==' ') tok++;
        char *end=tok+strlen(tok)-1;
        while (end>tok && *end==' ') *end--='\0';
        parts[nparts++]=tok;
        tok=strtok(NULL,",");
    }

    double total_cost=0, alacarte=0;
    for (int i=0;i<nparts;i++){
        const Component *c=find_component(parts[i]);
        if (!c){ fprintf(stderr,"Unknown component: %s\n",parts[i]); return 1; }
        total_cost+=c->cost;
        alacarte+=c->price;
    }

    double margin=sell_price-total_cost;
    double margin_pct=sell_price>0?(margin/sell_price*100):0;
    double discount=alacarte>0?((alacarte-sell_price)/alacarte*100):0;

    char ts[64]; iso_timestamp(ts,sizeof(ts));
    sqlite3_stmt *st;
    /* Delete existing bundle with same name */
    sqlite3_prepare_v2(db,"DELETE FROM bundles WHERE name=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,name,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);

    sqlite3_prepare_v2(db,
        "INSERT INTO bundles (name,components,sell_price,total_cost,alacarte_total,target,created_at) VALUES (?,?,?,?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,name,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,components_csv,-1,SQLITE_STATIC);
    sqlite3_bind_double(st,3,sell_price);
    sqlite3_bind_double(st,4,total_cost);
    sqlite3_bind_double(st,5,alacarte);
    sqlite3_bind_text(st,6,target,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,7,ts,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);

    printf("Bundle created: %s\n\n",name);
    printf("  Components: %s\n",components_csv);
    printf("  Cost:       $%.2f\n",total_cost);
    printf("  A la carte: $%.2f\n",alacarte);
    printf("  Sell price: $%.2f\n",sell_price);
    printf("  Margin:     $%.2f (%.0f%%)\n",margin,margin_pct);
    printf("  Buyer saves: %.0f%% vs individual pricing\n",discount);
    printf("  Target:     %s\n",target);
    return 0;
}

static int cmd_bundle_list(sqlite3 *db) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT name,sell_price,total_cost,target FROM bundles ORDER BY sell_price-total_cost DESC",
        -1,&st,NULL);
    printf("%-30s %-8s %-8s %-8s %-6s %s\n","Bundle","Price","Cost","Margin","%","Target");
    printf("---------------------------------------------------------------------------\n");
    int count=0;
    while (sqlite3_step(st)==SQLITE_ROW) {
        const char *name=(const char*)sqlite3_column_text(st,0);
        double price=sqlite3_column_double(st,1);
        double cost=sqlite3_column_double(st,2);
        const char *target=(const char*)sqlite3_column_text(st,3);
        double margin=price-cost;
        double pct=price>0?(margin/price*100):0;
        printf("%-30s $%-7.2f $%-7.2f $%-7.2f %-5.0f%% %s\n",name,price,cost,margin,pct,target);
        count++;
    }
    sqlite3_finalize(st);
    if (!count) printf("No bundles created. Use 'bundle create' to create one.\n");
    return 0;
}

static int cmd_bundle_compare(sqlite3 *db, const char *name) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT components,sell_price,total_cost,alacarte_total FROM bundles WHERE name=?",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,name,-1,SQLITE_STATIC);
    if (sqlite3_step(st)!=SQLITE_ROW){
        fprintf(stderr,"Bundle not found: %s\n",name);
        sqlite3_finalize(st); return 1;
    }
    const char *comps_str=(const char*)sqlite3_column_text(st,0);
    double sell=sqlite3_column_double(st,1);
    double alacarte=sqlite3_column_double(st,3);

    printf("## %s — Bundle vs A La Carte\n\n",name);
    printf("%-18s %-12s %s\n","Component","Individual","In Bundle");
    printf("------------------------------------------\n");

    /* Parse components */
    char csv[MAX_PATH];
    snprintf(csv,sizeof(csv),"%s",comps_str);
    char *tok=strtok(csv,",");
    while (tok) {
        while (*tok==' ') tok++;
        char *end=tok+strlen(tok)-1;
        while (end>tok && *end==' ') *end--='\0';
        const Component *c=find_component(tok);
        if (c) printf("%-18s $%-11.2f $%.2f\n",tok,c->price,c->cost);
        tok=strtok(NULL,",");
    }
    printf("------------------------------------------\n");
    printf("%-18s $%-11.2f $%.2f\n","Total",alacarte,sell);
    printf("\nBuyer saves: $%.2f (%.0f%%)\n",alacarte-sell,alacarte>0?((alacarte-sell)/alacarte*100):0);

    sqlite3_finalize(st);
    return 0;
}

static int cmd_bundle_export(sqlite3 *db, const char *name) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT components,sell_price,alacarte_total,target FROM bundles WHERE name=?",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,name,-1,SQLITE_STATIC);
    if (sqlite3_step(st)!=SQLITE_ROW){
        fprintf(stderr,"Bundle not found: %s\n",name);
        sqlite3_finalize(st); return 1;
    }
    const char *comps_str=(const char*)sqlite3_column_text(st,0);
    double sell=sqlite3_column_double(st,1);
    double alacarte=sqlite3_column_double(st,2);
    const char *target=(const char*)sqlite3_column_text(st,3);

    printf("# %s\n\n",name);
    printf("**$%.0f** — one price, everything included.\n\n",sell);
    printf("## What You Get\n\n");

    char csv[MAX_PATH];
    snprintf(csv,sizeof(csv),"%s",comps_str);
    char *tok=strtok(csv,",");
    while (tok) {
        while (*tok==' ') tok++;
        char *end=tok+strlen(tok)-1;
        while (end>tok && *end==' ') *end--='\0';
        const Component *c=find_component(tok);
        if (c) printf("- %s\n",c->desc);
        tok=strtok(NULL,",");
    }

    double savings=alacarte-sell;
    printf("\n## Why This Bundle\n");
    printf("Buying these separately would cost $%.0f. ",alacarte);
    printf("This package saves you $%.0f and delivers everything in one clean handoff.\n",savings);
    printf("\n*Target: %s*\n",target);

    sqlite3_finalize(st);
    return 0;
}

/* ── Status ───────────────────────────────────────────────────────────── */

static int cmd_status(sqlite3 *db) {
    sqlite3_stmt *st;
    int svc_count=0, svc_job_count=0, pipe_count=0, bundle_count=0;
    int pipe_pending=0, pipe_complete=0;

    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM services",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) svc_count=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM service_jobs",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) svc_job_count=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM pipeline_jobs",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) pipe_count=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM pipeline_jobs WHERE status='pending'",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) pipe_pending=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM pipeline_jobs WHERE status='completed'",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) pipe_complete=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM bundles",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) bundle_count=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    /* Best service by margin % */
    char best_svc[MAX_NAME]="(none)";
    double best_pct=0;
    sqlite3_prepare_v2(db,
        "SELECT name,(sell_price-buy_price)/sell_price*100 AS pct FROM services ORDER BY pct DESC LIMIT 1",
        -1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW){
        snprintf(best_svc,sizeof(best_svc),"%s",(const char*)sqlite3_column_text(st,0));
        best_pct=sqlite3_column_double(st,1);
    }
    sqlite3_finalize(st);

    /* Total margin across all pipeline jobs */
    double total_margin=0;
    sqlite3_prepare_v2(db,
        "SELECT SUM(sell_price-ai_cost-review_cost) FROM pipeline_jobs WHERE status='completed'",
        -1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) total_margin=sqlite3_column_double(st,0);
    sqlite3_finalize(st);

    printf("BonfyreFinance Status\n");
    printf("  Services:       %d tracked | %d jobs logged\n",svc_count,svc_job_count);
    printf("  Pipeline:       %d jobs | %d pending | %d completed\n",pipe_count,pipe_pending,pipe_complete);
    printf("  Bundles:        %d configured\n",bundle_count);
    printf("  Best spread:    %s (%.1f%%)\n",best_svc,best_pct);
    printf("  Pipeline margin: $%.2f total\n",total_margin);
    return 0;
}

/* ── CLI routing ──────────────────────────────────────────────────────── */

static const char *arg_get(int argc, char **argv, const char *flag) {
    for (int i=0;i<argc-1;i++)
        if (strcmp(argv[i],flag)==0) return argv[i+1];
    return NULL;
}

static int arg_has(int argc, char **argv, const char *flag) {
    for (int i=0;i<argc;i++)
        if (strcmp(argv[i],flag)==0) return 1;
    return 0;
}

static void usage(void) {
    fprintf(stderr,
        "BonfyreFinance — service arbitrage, labor pipeline, and bundle pricing\n\n"
        "Usage:\n"
        "  bonfyre-finance [--db FILE] service add --name N --buy B --sell S --source SRC\n"
        "  bonfyre-finance [--db FILE] service list\n"
        "  bonfyre-finance [--db FILE] service opportunities\n"
        "  bonfyre-finance [--db FILE] job log --service S --cost C --sold S [--quality Q]\n"
        "  bonfyre-finance [--db FILE] job create --input FILE --type TYPE [--sell-price P]\n"
        "  bonfyre-finance [--db FILE] job list [--status S]\n"
        "  bonfyre-finance [--db FILE] job review --id ID --result pass|fail --reviewer R\n"
        "  bonfyre-finance [--db FILE] job handoff --id ID [--type T]\n"
        "  bonfyre-finance [--db FILE] margin\n"
        "  bonfyre-finance [--db FILE] bundle create --name N --components A,B --price P\n"
        "  bonfyre-finance [--db FILE] bundle list\n"
        "  bonfyre-finance [--db FILE] bundle compare --name N\n"
        "  bonfyre-finance [--db FILE] bundle export --name N\n"
        "  bonfyre-finance [--db FILE] components\n"
        "  bonfyre-finance [--db FILE] status\n");
}

int main(int argc, char **argv) {
    if (argc<2){ usage(); return 1; }

    /* Find --db flag or use default */
    const char *db_path=arg_get(argc,argv,"--db");
    if (!db_path) db_path=default_db();

    /* Skip --db and its arg for command parsing */
    int cmd_argc=0;
    char *cmd_argv[128];
    for (int i=0;i<argc && cmd_argc<128;i++){
        if (strcmp(argv[i],"--db")==0){ i++; continue; }
        cmd_argv[cmd_argc++]=argv[i];
    }

    if (cmd_argc<2){ usage(); return 1; }
    const char *cmd=cmd_argv[1];

    sqlite3 *db=open_db(db_path);
    if (!db) return 1;

    int rc=0;

    if (strcmp(cmd,"service")==0) {
        if (cmd_argc<3){ fprintf(stderr,"Usage: bonfyre-finance service <add|list|opportunities>\n"); rc=1; goto done; }
        const char *sub=cmd_argv[2];
        if (strcmp(sub,"add")==0) {
            const char *name=arg_get(cmd_argc,cmd_argv,"--name");
            const char *buy_s=arg_get(cmd_argc,cmd_argv,"--buy");
            const char *sell_s=arg_get(cmd_argc,cmd_argv,"--sell");
            const char *source=arg_get(cmd_argc,cmd_argv,"--source");
            const char *cat=arg_get(cmd_argc,cmd_argv,"--category");
            if (!name||!buy_s||!sell_s||!source){ fprintf(stderr,"Missing --name, --buy, --sell, --source\n"); rc=1; goto done; }
            rc=cmd_service_add(db,name,atof(buy_s),atof(sell_s),source,cat);
        } else if (strcmp(sub,"list")==0) {
            rc=cmd_service_list(db);
        } else if (strcmp(sub,"opportunities")==0) {
            rc=cmd_service_opportunities(db);
        } else {
            fprintf(stderr,"Unknown service command: %s\n",sub); rc=1;
        }
    } else if (strcmp(cmd,"job")==0) {
        if (cmd_argc<3){ fprintf(stderr,"Usage: bonfyre-finance job <create|list|review|log|handoff>\n"); rc=1; goto done; }
        const char *sub=cmd_argv[2];
        if (strcmp(sub,"log")==0) {
            const char *svc=arg_get(cmd_argc,cmd_argv,"--service");
            const char *cost_s=arg_get(cmd_argc,cmd_argv,"--cost");
            const char *sold_s=arg_get(cmd_argc,cmd_argv,"--sold");
            const char *quality=arg_get(cmd_argc,cmd_argv,"--quality");
            const char *notes=arg_get(cmd_argc,cmd_argv,"--notes");
            if (!svc||!cost_s||!sold_s){ fprintf(stderr,"Missing --service, --cost, --sold\n"); rc=1; goto done; }
            rc=cmd_job_log(db,svc,atof(cost_s),atof(sold_s),quality,notes);
        } else if (strcmp(sub,"create")==0) {
            const char *input=arg_get(cmd_argc,cmd_argv,"--input");
            const char *type=arg_get(cmd_argc,cmd_argv,"--type");
            const char *sell_s=arg_get(cmd_argc,cmd_argv,"--sell-price");
            if (!input){ fprintf(stderr,"Missing --input\n"); rc=1; goto done; }
            if (!type) type="transcription";
            rc=cmd_pipeline_create(db,input,type,sell_s?atof(sell_s):0);
        } else if (strcmp(sub,"list")==0) {
            const char *status=arg_get(cmd_argc,cmd_argv,"--status");
            rc=cmd_pipeline_list(db,status);
        } else if (strcmp(sub,"review")==0) {
            const char *id_s=arg_get(cmd_argc,cmd_argv,"--id");
            const char *result=arg_get(cmd_argc,cmd_argv,"--result");
            const char *reviewer=arg_get(cmd_argc,cmd_argv,"--reviewer");
            const char *notes=arg_get(cmd_argc,cmd_argv,"--notes");
            const char *cost_s=arg_get(cmd_argc,cmd_argv,"--cost");
            if (!id_s||!result||!reviewer){ fprintf(stderr,"Missing --id, --result, --reviewer\n"); rc=1; goto done; }
            rc=cmd_pipeline_review(db,atoi(id_s),result,reviewer,notes,cost_s?atof(cost_s):0);
        } else if (strcmp(sub,"handoff")==0) {
            const char *id_s=arg_get(cmd_argc,cmd_argv,"--id");
            const char *type=arg_get(cmd_argc,cmd_argv,"--type");
            if (!id_s){ fprintf(stderr,"Missing --id\n"); rc=1; goto done; }
            rc=cmd_handoff(db,atoi(id_s),type);
        } else {
            fprintf(stderr,"Unknown job command: %s\n",sub); rc=1;
        }
    } else if (strcmp(cmd,"margin")==0) {
        rc=cmd_margin(db);
    } else if (strcmp(cmd,"bundle")==0) {
        if (cmd_argc<3){ fprintf(stderr,"Usage: bonfyre-finance bundle <create|list|compare|export>\n"); rc=1; goto done; }
        const char *sub=cmd_argv[2];
        if (strcmp(sub,"create")==0) {
            const char *name=arg_get(cmd_argc,cmd_argv,"--name");
            const char *comps=arg_get(cmd_argc,cmd_argv,"--components");
            const char *price_s=arg_get(cmd_argc,cmd_argv,"--price");
            const char *target=arg_get(cmd_argc,cmd_argv,"--target");
            if (!name||!comps||!price_s){ fprintf(stderr,"Missing --name, --components, --price\n"); rc=1; goto done; }
            rc=cmd_bundle_create(db,name,comps,atof(price_s),target);
        } else if (strcmp(sub,"list")==0) {
            rc=cmd_bundle_list(db);
        } else if (strcmp(sub,"compare")==0) {
            const char *name=arg_get(cmd_argc,cmd_argv,"--name");
            if (!name){ fprintf(stderr,"Missing --name\n"); rc=1; goto done; }
            rc=cmd_bundle_compare(db,name);
        } else if (strcmp(sub,"export")==0) {
            const char *name=arg_get(cmd_argc,cmd_argv,"--name");
            if (!name){ fprintf(stderr,"Missing --name\n"); rc=1; goto done; }
            rc=cmd_bundle_export(db,name);
        } else {
            fprintf(stderr,"Unknown bundle command: %s\n",sub); rc=1;
        }
    } else if (strcmp(cmd,"components")==0) {
        printf("%-18s %-8s %-8s %s\n","Component","Cost","Price","Description");
        printf("-----------------------------------------------------------------\n");
        for (int i=0;i<NUM_COMPONENTS;i++)
            printf("%-18s $%-7.2f $%-7.2f %s\n",COMPONENTS[i].name,COMPONENTS[i].cost,COMPONENTS[i].price,COMPONENTS[i].desc);
    } else if (strcmp(cmd,"status")==0) {
        rc=cmd_status(db);
    } else {
        fprintf(stderr,"Unknown command: %s\n",cmd);
        usage(); rc=1;
    }

done:
    sqlite3_close(db);
    return rc;
}
