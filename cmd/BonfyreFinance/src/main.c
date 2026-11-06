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
#include <ctype.h>
#include "../../../lib/libbonfyre/include/bf_help.h"
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <sqlite3.h>
#include <bonfyre.h>

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
    /* The routing line. Everything above is the transcript pipeline, which is
     * the only product this catalogue knew about, so a bundle could be composed
     * for one product and for nothing else.
     *
     * Both numbers are measured rather than chosen. The cost is zero because
     * the route resolves to a source the estate has proven it can read under a
     * published permission -- robots allowed, the fetch returned 200, and the
     * retained bytes still hash to the receipt. The price is the median of what
     * the metered sources serving the same coordinate publish, taken across
     * markets of four sellers or more; the mean is 5.99 and is one 149-dollar
     * outlier, which is why it is not the number here. */
    {"route-resolution", 0.000, 0.005, "Cheapest proven-readable source for a need, with digest"},
    {"liveness-proof",   0.000, 0.005, "Re-observation confirming the routed source still answers"},
};
#define NUM_COMPONENTS (int)(sizeof(COMPONENTS)/sizeof(COMPONENTS[0]))

static const Component *find_component(const char *name) {
    for (int i=0;i<NUM_COMPONENTS;i++)
        if (strcmp(COMPONENTS[i].name,name)==0) return &COMPONENTS[i];
    return NULL;
}

/* ── Utility ──────────────────────────────────────────────────────────── */

static void iso_timestamp(char *buf, size_t sz) { bf_iso_timestamp(buf, sz); }

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


/* Generic economic opportunity closure. Sources project into these same
 * records; a source such as classifieds does not become a new product owner. */
static const char *ZERO_CLOSE_SCHEMA_SQL =
    "PRAGMA foreign_keys=ON;"
    "CREATE TABLE IF NOT EXISTS opportunities ("
    " id TEXT PRIMARY KEY, record_digest TEXT NOT NULL,"
    " source_kind TEXT NOT NULL, source_ref TEXT NOT NULL, source_digest TEXT NOT NULL,"
    " observed_at TEXT NOT NULL, title TEXT NOT NULL, desired_state TEXT NOT NULL,"
    " compensation_cents INTEGER, currency TEXT NOT NULL, deadline TEXT NOT NULL DEFAULT '',"
    " contact_boundary TEXT NOT NULL, expected_compute_cents INTEGER NOT NULL DEFAULT 0,"
    " expected_external_cents INTEGER NOT NULL DEFAULT 0, expected_human_minutes INTEGER NOT NULL DEFAULT 0,"
    " reuse_points INTEGER NOT NULL DEFAULT 0, risk_points INTEGER NOT NULL DEFAULT 0,"
    " stage TEXT NOT NULL DEFAULT 'observed',"
    " proposal_ref TEXT, authority_ref TEXT, customer_ref TEXT, execution_ref TEXT,"
    " verification_ref TEXT, delivery_ref TEXT, payment_ref TEXT, retention_ref TEXT,"
    " actual_revenue_cents INTEGER, actual_cost_cents INTEGER, actual_human_minutes INTEGER,"
    " created_at TEXT NOT NULL, updated_at TEXT NOT NULL"
    ");"
    "CREATE TABLE IF NOT EXISTS opportunity_requirements ("
    " opportunity_id TEXT NOT NULL REFERENCES opportunities(id) ON DELETE CASCADE,"
    " kind TEXT NOT NULL, requirement_key TEXT NOT NULL, owner_ref TEXT NOT NULL,"
    " state TEXT NOT NULL, evidence_ref TEXT NOT NULL DEFAULT '', row_digest TEXT NOT NULL,"
    " updated_at TEXT NOT NULL, PRIMARY KEY(opportunity_id,kind,requirement_key)"
    ") WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS opportunity_events ("
    " id INTEGER PRIMARY KEY AUTOINCREMENT,"
    " opportunity_id TEXT NOT NULL REFERENCES opportunities(id) ON DELETE CASCADE,"
    " kind TEXT NOT NULL, from_stage TEXT NOT NULL, to_stage TEXT NOT NULL,"
    " evidence_ref TEXT NOT NULL, created_at TEXT NOT NULL"
    ");"
    "CREATE INDEX IF NOT EXISTS opportunity_stage_rank ON opportunities(stage,compensation_cents);"
    "CREATE INDEX IF NOT EXISTS opportunity_requirement_state ON opportunity_requirements(opportunity_id,kind,state);"
    "CREATE INDEX IF NOT EXISTS opportunity_event_subject ON opportunity_events(opportunity_id,id);";


static sqlite3 *open_db(const char *path) {
    sqlite3 *db=NULL;
    /* Finance owns durable proposals, requirements, jobs and cost evidence.
     * Refuse a store that cannot read back WAL + synchronous=FULL. */
    int rc=bf_sqlite3_open_tier(path,&db,BF_SQLITE_DURABLE);
    if (rc!=SQLITE_OK){
        fprintf(stderr,"Finance store cannot open at full durability (SQLite %d)\n",rc);
        return NULL;
    }
    char *err=NULL;
    if (sqlite3_exec(db,SCHEMA_SQL,NULL,NULL,&err)!=SQLITE_OK){
        fprintf(stderr,"Schema error: %s\n",err);
        sqlite3_free(err); sqlite3_close(db); return NULL;
    }
    if (sqlite3_exec(db,ZERO_CLOSE_SCHEMA_SQL,NULL,NULL,&err)!=SQLITE_OK){
        fprintf(stderr,"Zero-close schema error: %s\n",err);
        sqlite3_free(err); sqlite3_close(db); return NULL;
    }
    return db;
}

/* ── Default DB path ──────────────────────────────────────────────────── */

static const char *default_db(void) {
    static char path[MAX_PATH];
    const char *home=getenv("HOME");
    /* Every sibling organ honours an explicit DB override; this one did not, so
     * a service running with ProtectHome=true (which masks $HOME) could never
     * open its book at all -- it failed with "Cannot open /home/.../finance.db"
     * rather than reading the shared state copy its readers use. */
    const char *override=getenv("BONFYRE_FINANCE_DB");
    if (override && *override) { snprintf(path,sizeof(path),"%s",override); return path; }
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


#define ZC_MAX_LINE 65536

static int zc_text_ok(const char *text, size_t max) {
    if (!text || strlen(text)>max) return 0;
    for (const unsigned char *p=(const unsigned char *)text; *p; ++p)
        if (*p<32 || *p==127) return 0;
    return 1;
}
static int zc_atom_ok(const char *text, size_t max) {
    if (!text || !*text || strlen(text)>max) return 0;
    for (const unsigned char *p=(const unsigned char *)text; *p; ++p)
        if (!(isalnum(*p)||*p=='-'||*p=='_'||*p=='.'||*p==':')) return 0;
    return 1;
}
static int zc_hex64(const char *text) {
    if (!text || strlen(text)!=64) return 0;
    for (int i=0;i<64;i++) if (!isxdigit((unsigned char)text[i])) return 0;
    return 1;
}
static int zc_currency(const char *text) {
    return text && strlen(text)==3 && isupper((unsigned char)text[0]) &&
           isupper((unsigned char)text[1]) && isupper((unsigned char)text[2]);
}
static int zc_parse_nonnegative(const char *text, sqlite3_int64 *value, int nullable, int *is_null) {
    char *end=NULL; long long parsed;
    if (is_null) *is_null=0;
    if (nullable && text && (!strcmp(text,"-") || !*text)) {
        if (is_null) *is_null=1; *value=0; return 1;
    }
    if (!text || !*text || *text=='-' || *text=='+') return 0;
    errno=0; parsed=strtoll(text,&end,10);
    if (errno || !end || *end || parsed<0) return 0;
    *value=(sqlite3_int64)parsed; return 1;
}
static int zc_split_tsv(char *line, char **fields, int expected) {
    int count=1; fields[0]=line;
    for (char *p=line; *p; ++p) if (*p=='\t') {
        *p='\0'; if (count>=expected) return 0; fields[count++]=p+1;
    }
    return count==expected;
}
static int zc_stage_depth(const char *stage) {
    static const char *stages[]={"observed","specified","solvable","proposal_ready",
        "outreach_approved","accepted","executing","verified","delivered","paid","retained"};
    for (int i=0;i<11;i++) if (stage && !strcmp(stage,stages[i])) return i;
    return -1;
}
static int zc_exec(sqlite3 *db, const char *sql) {
    char *error=NULL;
    if (sqlite3_exec(db,sql,NULL,NULL,&error)==SQLITE_OK) return 1;
    fprintf(stderr,"zero-close SQL: %s\n",error?error:"unknown error");
    sqlite3_free(error); return 0;
}
static int zc_refresh(sqlite3 *db, const char *id) {
    sqlite3_stmt *st=NULL;
    const char *sql=
        "UPDATE opportunities SET stage=CASE "
        " WHEN proposal_ref IS NOT NULL AND EXISTS(SELECT 1 FROM opportunity_requirements r WHERE r.opportunity_id=opportunities.id AND r.kind<>'authority') "
        "  AND NOT EXISTS(SELECT 1 FROM opportunity_requirements r WHERE r.opportunity_id=opportunities.id AND r.kind<>'authority' AND r.state<>'satisfied') THEN 'proposal_ready' "
        " WHEN EXISTS(SELECT 1 FROM opportunity_requirements r WHERE r.opportunity_id=opportunities.id AND r.kind<>'authority') "
        "  AND NOT EXISTS(SELECT 1 FROM opportunity_requirements r WHERE r.opportunity_id=opportunities.id AND r.kind<>'authority' AND r.state<>'satisfied') THEN 'solvable' "
        " WHEN title<>'' AND desired_state<>'' AND contact_boundary<>'' THEN 'specified' ELSE 'observed' END,"
        " updated_at=?1 WHERE stage IN ('observed','specified','solvable','proposal_ready') AND (?2 IS NULL OR id=?2)";
    char ts[64]; iso_timestamp(ts,sizeof(ts));
    if (sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK) return 0;
    sqlite3_bind_text(st,1,ts,-1,SQLITE_TRANSIENT);
    if (id) sqlite3_bind_text(st,2,id,-1,SQLITE_TRANSIENT); else sqlite3_bind_null(st,2);
    int ok=sqlite3_step(st)==SQLITE_DONE; sqlite3_finalize(st); return ok;
}

static int zc_import_opportunities(sqlite3 *db, const char *path) {
    static const char *header="id\tsource_kind\tsource_ref\tsource_digest\tobserved_at\ttitle\tdesired_state\tcompensation_cents\tcurrency\tdeadline\tcontact_boundary\texpected_compute_cents\texpected_external_cents\thuman_minutes\treuse_points\trisk_points";
    FILE *file=fopen(path,"r"); if (!file) { fprintf(stderr,"Cannot open opportunity TSV: %s\n",path); return 1; }
    char *line=NULL; size_t cap=0; ssize_t len; int row=0,inserted=0,duplicates=0,rc=1;
    sqlite3_stmt *insert=NULL,*existing=NULL;
    if (!zc_exec(db,"BEGIN IMMEDIATE")) goto done;
    const char *sql="INSERT INTO opportunities(id,record_digest,source_kind,source_ref,source_digest,observed_at,title,desired_state,compensation_cents,currency,deadline,contact_boundary,expected_compute_cents,expected_external_cents,expected_human_minutes,reuse_points,risk_points,stage,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING";
    if (sqlite3_prepare_v2(db,sql,-1,&insert,NULL)!=SQLITE_OK ||
        sqlite3_prepare_v2(db,"SELECT record_digest FROM opportunities WHERE id=?",-1,&existing,NULL)!=SQLITE_OK) goto rollback;
    while ((len=getline(&line,&cap,file))>=0) {
        row++; while (len>0 && (line[len-1]=='\n'||line[len-1]=='\r')) line[--len]='\0';
        if (len<=0 || len>ZC_MAX_LINE) { fprintf(stderr,"Invalid opportunity row %d\n",row); goto rollback; }
        if (row==1) { if (strcmp(line,header)) { fprintf(stderr,"Unexpected opportunity TSV header\n"); goto rollback; } continue; }
        char digest[65]; bf_sha256_hex((const uint8_t *)line,(size_t)len,digest);
        char *f[16];
        if (!zc_split_tsv(line,f,16) || !zc_atom_ok(f[0],160) || !zc_atom_ok(f[1],64) ||
            !zc_text_ok(f[2],2048) || !zc_hex64(f[3]) || !zc_text_ok(f[4],64) ||
            !zc_text_ok(f[5],512) || !zc_text_ok(f[6],4096) || !zc_currency(f[8]) ||
            !zc_text_ok(f[9],64) || !zc_atom_ok(f[10],128)) {
            fprintf(stderr,"Malformed opportunity row %d\n",row); goto rollback;
        }
        sqlite3_int64 compensation=0,compute=0,external=0,human=0,reuse=0,risk=0; int unpriced=0;
        if (!zc_parse_nonnegative(f[7],&compensation,1,&unpriced) ||
            !zc_parse_nonnegative(f[11],&compute,0,NULL) ||
            !zc_parse_nonnegative(f[12],&external,0,NULL) ||
            !zc_parse_nonnegative(f[13],&human,0,NULL) ||
            !zc_parse_nonnegative(f[14],&reuse,0,NULL) ||
            !zc_parse_nonnegative(f[15],&risk,0,NULL) || reuse>1000 || risk>1000) {
            fprintf(stderr,"Invalid opportunity economics row %d\n",row); goto rollback;
        }
        const char *stage=(*f[5]&&*f[6]&&*f[10])?"specified":"observed"; char ts[64]; iso_timestamp(ts,sizeof(ts));
        sqlite3_reset(insert); sqlite3_clear_bindings(insert);
        sqlite3_bind_text(insert,1,f[0],-1,SQLITE_TRANSIENT); sqlite3_bind_text(insert,2,digest,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(insert,3,f[1],-1,SQLITE_TRANSIENT); sqlite3_bind_text(insert,4,f[2],-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(insert,5,f[3],-1,SQLITE_TRANSIENT); sqlite3_bind_text(insert,6,f[4],-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(insert,7,f[5],-1,SQLITE_TRANSIENT); sqlite3_bind_text(insert,8,f[6],-1,SQLITE_TRANSIENT);
        if (unpriced) sqlite3_bind_null(insert,9); else sqlite3_bind_int64(insert,9,compensation);
        sqlite3_bind_text(insert,10,f[8],-1,SQLITE_TRANSIENT); sqlite3_bind_text(insert,11,f[9],-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(insert,12,f[10],-1,SQLITE_TRANSIENT); sqlite3_bind_int64(insert,13,compute);
        sqlite3_bind_int64(insert,14,external); sqlite3_bind_int64(insert,15,human);
        sqlite3_bind_int64(insert,16,reuse); sqlite3_bind_int64(insert,17,risk); sqlite3_bind_text(insert,18,stage,-1,SQLITE_STATIC);
        sqlite3_bind_text(insert,19,ts,-1,SQLITE_TRANSIENT); sqlite3_bind_text(insert,20,ts,-1,SQLITE_TRANSIENT);
        if (sqlite3_step(insert)!=SQLITE_DONE) goto rollback;
        if (sqlite3_changes(db)>0) inserted++;
        else {
            sqlite3_reset(existing); sqlite3_clear_bindings(existing); sqlite3_bind_text(existing,1,f[0],-1,SQLITE_TRANSIENT);
            if (sqlite3_step(existing)!=SQLITE_ROW || strcmp((const char *)sqlite3_column_text(existing,0),digest)) {
                fprintf(stderr,"Opportunity identity collision at row %d\n",row); goto rollback;
            }
            duplicates++;
        }
    }
    if (ferror(file) || row<2 || !zc_refresh(db,NULL) || !zc_exec(db,"COMMIT")) goto rollback_no_tx;
    printf("imported\t%d\nduplicates\t%d\n",inserted,duplicates); rc=0; goto done;
rollback:
    zc_exec(db,"ROLLBACK");
rollback_no_tx:
done:
    sqlite3_finalize(insert); sqlite3_finalize(existing); free(line); fclose(file); return rc;
}

static int zc_import_requirements(sqlite3 *db, const char *path) {
    static const char *header="opportunity_id\tkind\trequirement_key\towner_ref\tstate\tevidence_ref";
    FILE *file=fopen(path,"r"); if (!file) { fprintf(stderr,"Cannot open requirement TSV: %s\n",path); return 1; }
    char *line=NULL; size_t cap=0; ssize_t len; int row=0,inserted=0,duplicates=0,rc=1;
    sqlite3_stmt *insert=NULL,*existing=NULL;
    if (!zc_exec(db,"BEGIN IMMEDIATE")) goto done;
    const char *sql="INSERT INTO opportunity_requirements(opportunity_id,kind,requirement_key,owner_ref,state,evidence_ref,row_digest,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(opportunity_id,kind,requirement_key) DO NOTHING";
    if (sqlite3_prepare_v2(db,sql,-1,&insert,NULL)!=SQLITE_OK ||
        sqlite3_prepare_v2(db,"SELECT row_digest FROM opportunity_requirements WHERE opportunity_id=? AND kind=? AND requirement_key=?",-1,&existing,NULL)!=SQLITE_OK) goto rollback;
    while ((len=getline(&line,&cap,file))>=0) {
        row++; while (len>0 && (line[len-1]=='\n'||line[len-1]=='\r')) line[--len]='\0';
        if (len<=0 || len>ZC_MAX_LINE) goto rollback;
        if (row==1) { if (strcmp(line,header)) { fprintf(stderr,"Unexpected requirement TSV header\n"); goto rollback; } continue; }
        char digest[65]; bf_sha256_hex((const uint8_t *)line,(size_t)len,digest); char *f[6];
        if (!zc_split_tsv(line,f,6) || !zc_atom_ok(f[0],160) || !zc_atom_ok(f[2],160) ||
            !zc_text_ok(f[3],1024) || !zc_text_ok(f[5],2048) ||
            (strcmp(f[1],"capability")&&strcmp(f[1],"material")&&strcmp(f[1],"external")&&strcmp(f[1],"authority")) ||
            (strcmp(f[4],"unresolved")&&strcmp(f[4],"available")&&strcmp(f[4],"satisfied")&&strcmp(f[4],"blocked"))) {
            fprintf(stderr,"Malformed requirement row %d\n",row); goto rollback;
        }
        char ts[64]; iso_timestamp(ts,sizeof(ts)); sqlite3_reset(insert); sqlite3_clear_bindings(insert);
        for (int i=0;i<6;i++) sqlite3_bind_text(insert,i+1,f[i],-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(insert,7,digest,-1,SQLITE_TRANSIENT); sqlite3_bind_text(insert,8,ts,-1,SQLITE_TRANSIENT);
        if (sqlite3_step(insert)!=SQLITE_DONE) { fprintf(stderr,"Requirement insert failed row %d: %s\n",row,sqlite3_errmsg(db)); goto rollback; }
        if (sqlite3_changes(db)>0) inserted++;
        else {
            sqlite3_reset(existing); sqlite3_clear_bindings(existing);
            sqlite3_bind_text(existing,1,f[0],-1,SQLITE_TRANSIENT); sqlite3_bind_text(existing,2,f[1],-1,SQLITE_TRANSIENT); sqlite3_bind_text(existing,3,f[2],-1,SQLITE_TRANSIENT);
            if (sqlite3_step(existing)!=SQLITE_ROW || strcmp((const char *)sqlite3_column_text(existing,0),digest)) {
                fprintf(stderr,"Requirement identity collision at row %d\n",row); goto rollback;
            }
            duplicates++;
        }
    }
    if (ferror(file) || row<2 || !zc_refresh(db,NULL) || !zc_exec(db,"COMMIT")) goto rollback_no_tx;
    printf("requirements_imported\t%d\nrequirements_duplicates\t%d\n",inserted,duplicates); rc=0; goto done;
rollback:
    zc_exec(db,"ROLLBACK");
rollback_no_tx:
done:
    sqlite3_finalize(insert); sqlite3_finalize(existing); free(line); fclose(file); return rc;
}



static int zc_event(sqlite3 *db, const char *id, const char *kind,
                    const char *from, const char *to, const char *ref) {
    sqlite3_stmt *st=NULL; char ts[64]; iso_timestamp(ts,sizeof(ts));
    if (sqlite3_prepare_v2(db,"INSERT INTO opportunity_events(opportunity_id,kind,from_stage,to_stage,evidence_ref,created_at) VALUES(?,?,?,?,?,?)",-1,&st,NULL)!=SQLITE_OK) return 0;
    sqlite3_bind_text(st,1,id,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,2,kind,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,3,from,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,4,to,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,5,ref,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,6,ts,-1,SQLITE_TRANSIENT);
    int ok=sqlite3_step(st)==SQLITE_DONE; sqlite3_finalize(st); return ok;
}

static int zc_requirement_set(sqlite3 *db, const char *id, const char *kind, const char *key,
                              const char *state, const char *evidence) {
    if (!zc_atom_ok(id,160)||!zc_atom_ok(key,160)||!zc_text_ok(evidence,2048) ||
        (strcmp(kind,"capability")&&strcmp(kind,"material")&&strcmp(kind,"external")&&strcmp(kind,"authority")) ||
        (strcmp(state,"unresolved")&&strcmp(state,"available")&&strcmp(state,"satisfied")&&strcmp(state,"blocked"))) return 1;
    sqlite3_stmt *read=NULL,*update=NULL; int rc=1; char old[32]="",stage[32]="";
    if (!zc_exec(db,"BEGIN IMMEDIATE")) return 1;
    if (sqlite3_prepare_v2(db,"SELECT r.state,o.stage FROM opportunity_requirements r JOIN opportunities o ON o.id=r.opportunity_id WHERE r.opportunity_id=? AND r.kind=? AND r.requirement_key=?",-1,&read,NULL)!=SQLITE_OK) goto rollback;
    sqlite3_bind_text(read,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(read,2,kind,-1,SQLITE_TRANSIENT);sqlite3_bind_text(read,3,key,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(read)!=SQLITE_ROW) { fprintf(stderr,"Unknown opportunity requirement\n"); goto rollback; }
    snprintf(old,sizeof(old),"%s",(const char *)sqlite3_column_text(read,0));
    snprintf(stage,sizeof(stage),"%s",(const char *)sqlite3_column_text(read,1));
    sqlite3_finalize(read); read=NULL;
    if (zc_stage_depth(stage)>=6 && strcmp(state,"satisfied")) {
        fprintf(stderr,"Cannot invalidate a requirement after execution began\n"); goto rollback;
    }
    if (!strcmp(old,state)) { zc_exec(db,"ROLLBACK"); printf("requirement_unchanged\t%s\n",state); return 0; }
    char ts[64]; iso_timestamp(ts,sizeof(ts));
    if (sqlite3_prepare_v2(db,"UPDATE opportunity_requirements SET state=?,evidence_ref=?,updated_at=? WHERE opportunity_id=? AND kind=? AND requirement_key=?",-1,&update,NULL)!=SQLITE_OK) goto rollback;
    sqlite3_bind_text(update,1,state,-1,SQLITE_TRANSIENT);sqlite3_bind_text(update,2,evidence,-1,SQLITE_TRANSIENT);sqlite3_bind_text(update,3,ts,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(update,4,id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(update,5,kind,-1,SQLITE_TRANSIENT);sqlite3_bind_text(update,6,key,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(update)!=SQLITE_DONE || !zc_event(db,id,"requirement",stage,stage,evidence) ||
        !zc_refresh(db,id) || !zc_exec(db,"COMMIT")) goto rollback_no_tx;
    printf("requirement\t%s\t%s\t%s\n",kind,key,state); rc=0; goto done;
rollback:
    zc_exec(db,"ROLLBACK");
rollback_no_tx:
done:
    sqlite3_finalize(read); sqlite3_finalize(update); return rc;
}

static int zc_all_requirements_satisfied(sqlite3 *db, const char *id) {
    sqlite3_stmt *st=NULL; int ok=0;
    if (sqlite3_prepare_v2(db,"SELECT count(*)=0 AND (SELECT count(*) FROM opportunity_requirements WHERE opportunity_id=?1)>0 FROM opportunity_requirements WHERE opportunity_id=?1 AND state<>'satisfied'",-1,&st,NULL)!=SQLITE_OK) return 0;
    sqlite3_bind_text(st,1,id,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(st)==SQLITE_ROW) ok=sqlite3_column_int(st,0);
    sqlite3_finalize(st); return ok;
}

static int zc_attest(sqlite3 *db, const char *id, const char *kind, const char *ref,
                     sqlite3_int64 revenue, sqlite3_int64 cost, sqlite3_int64 human, int has_actuals) {
    if (!zc_atom_ok(id,160)||!zc_text_ok(ref,2048)||!*ref) return 1;
    const char *next=NULL,*column=NULL; int need=0;
    if (!strcmp(kind,"proposal")) { next="proposal_ready";column="proposal_ref";need=2; }
    else if (!strcmp(kind,"authority")) { next="outreach_approved";column="authority_ref";need=3; }
    else if (!strcmp(kind,"customer")) { next="accepted";column="customer_ref";need=4; }
    else if (!strcmp(kind,"execution")) { next="executing";column="execution_ref";need=5; }
    else if (!strcmp(kind,"verification")) { next="verified";column="verification_ref";need=6; }
    else if (!strcmp(kind,"delivery")) { next="delivered";column="delivery_ref";need=7; }
    else if (!strcmp(kind,"payment")) { next="paid";column="payment_ref";need=8; }
    else if (!strcmp(kind,"retention")) { next="retained";column="retention_ref";need=9; }
    else return 1;
    sqlite3_stmt *st=NULL; char stage[32]=""; int current=-1,existing_col=0,comp_null=1,rc=1;
    char query[256]; snprintf(query,sizeof(query),"SELECT stage,%s,compensation_cents FROM opportunities WHERE id=?",column);
    if (sqlite3_prepare_v2(db,query,-1,&st,NULL)!=SQLITE_OK) return 1;
    sqlite3_bind_text(st,1,id,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(st)!=SQLITE_ROW) { sqlite3_finalize(st); fprintf(stderr,"Opportunity not found\n"); return 1; }
    snprintf(stage,sizeof(stage),"%s",(const char *)sqlite3_column_text(st,0));
    current=zc_stage_depth(stage); existing_col=sqlite3_column_type(st,1)!=SQLITE_NULL;
    comp_null=sqlite3_column_type(st,2)==SQLITE_NULL;
    if (existing_col) {
        const char *old=(const char *)sqlite3_column_text(st,1);
        if (!strcmp(old,ref) && current>=zc_stage_depth(next)) { sqlite3_finalize(st); printf("attestation_unchanged\t%s\n",next); return 0; }
        sqlite3_finalize(st); fprintf(stderr,"Conflicting attestation already exists\n"); return 1;
    }
    sqlite3_finalize(st); st=NULL;
    if (current!=need) { fprintf(stderr,"Stage %s cannot accept %s attestation\n",stage,kind); return 1; }
    if (!strcmp(kind,"proposal") && comp_null) { fprintf(stderr,"Unpriced opportunity cannot become proposal-ready\n"); return 1; }
    if (!strcmp(kind,"execution") && !zc_all_requirements_satisfied(db,id)) {
        fprintf(stderr,"Execution blocked by unresolved requirements\n"); return 1;
    }
    if (!strcmp(kind,"payment") && !has_actuals) {
        fprintf(stderr,"Payment attestation requires actual revenue, cost, and human minutes\n"); return 1;
    }
    if (!zc_exec(db,"BEGIN IMMEDIATE")) return 1;
    char ts[64]; iso_timestamp(ts,sizeof(ts)); char update_sql[512];
    if (!strcmp(kind,"payment"))
        snprintf(update_sql,sizeof(update_sql),"UPDATE opportunities SET %s=?,stage=?,actual_revenue_cents=?,actual_cost_cents=?,actual_human_minutes=?,updated_at=? WHERE id=?",column);
    else
        snprintf(update_sql,sizeof(update_sql),"UPDATE opportunities SET %s=?,stage=?,updated_at=? WHERE id=?",column);
    if (sqlite3_prepare_v2(db,update_sql,-1,&st,NULL)!=SQLITE_OK) goto rollback;
    sqlite3_bind_text(st,1,ref,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,2,next,-1,SQLITE_STATIC);
    if (!strcmp(kind,"payment")) {
        sqlite3_bind_int64(st,3,revenue);sqlite3_bind_int64(st,4,cost);sqlite3_bind_int64(st,5,human);
        sqlite3_bind_text(st,6,ts,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,7,id,-1,SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_text(st,3,ts,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,4,id,-1,SQLITE_TRANSIENT);
    }
    if (sqlite3_step(st)!=SQLITE_DONE) goto rollback;
    sqlite3_finalize(st); st=NULL;
    if (!zc_event(db,id,kind,stage,next,ref) || !zc_exec(db,"COMMIT")) goto rollback_no_tx;
    printf("stage\t%s\tZ%d\n",next,zc_stage_depth(next)); rc=0; goto done;
rollback:
    zc_exec(db,"ROLLBACK");
rollback_no_tx:
done:
    sqlite3_finalize(st); return rc;
}

static int zc_rank(sqlite3 *db, int limit) {
    if (limit<1) limit=25; if (limit>100000) limit=100000;
    sqlite3_stmt *st=NULL;
    const char *sql=
        "WITH r AS (SELECT opportunity_id,count(*) total,sum(state='satisfied') satisfied,"
        "sum(kind<>'authority') nonauth,sum(kind<>'authority' AND state='satisfied') nonauth_satisfied "
        "FROM opportunity_requirements GROUP BY opportunity_id) "
        "SELECT o.id,o.stage,o.compensation_cents,"
        "CASE WHEN cost_basis.opportunity_id IS NULL THEN NULL ELSE o.expected_compute_cents+o.expected_external_cents END,"
        "o.expected_human_minutes,o.reuse_points,o.risk_points,coalesce(r.satisfied,0),coalesce(r.total,0),"
        "CASE WHEN o.compensation_cents IS NULL OR cost_basis.opportunity_id IS NULL THEN NULL "
        "ELSE o.compensation_cents-o.expected_compute_cents-o.expected_external_cents END margin "
        "FROM opportunities o LEFT JOIN r ON r.opportunity_id=o.id "
        "LEFT JOIN opportunity_requirements cost_basis ON cost_basis.opportunity_id=o.id "
        "AND cost_basis.kind='material' AND cost_basis.requirement_key='cost_basis_confirmed' "
        "AND cost_basis.state='satisfied' AND cost_basis.evidence_ref NOT IN ('','-') "
        "ORDER BY (o.stage IN ('paid','retained')) ASC,(o.compensation_cents IS NULL) ASC,"
        "(margin>0) DESC,(coalesce(r.nonauth,0)>0 AND coalesce(r.nonauth,0)=coalesce(r.nonauth_satisfied,0)) DESC,"
        "CASE WHEN margin IS NULL THEN -9e18 WHEN o.expected_human_minutes>0 THEN margin*1.0/o.expected_human_minutes ELSE margin END DESC,"
        "o.reuse_points DESC,o.risk_points ASC,o.observed_at ASC LIMIT ?";
    if (sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK) return 1;
    sqlite3_bind_int(st,1,limit);
    puts("id\tdepth\tstage\tcompensation_cents\texpected_cost_cents\tmargin_cents\thuman_minutes\tmargin_per_human_minute\treuse_points\trisk_points\trequirements_closed");
    while (sqlite3_step(st)==SQLITE_ROW) {
        const char *id=(const char *)sqlite3_column_text(st,0),*stage=(const char *)sqlite3_column_text(st,1);
        int comp_null=sqlite3_column_type(st,2)==SQLITE_NULL,cost_null=sqlite3_column_type(st,3)==SQLITE_NULL,margin_null=sqlite3_column_type(st,9)==SQLITE_NULL;
        sqlite3_int64 comp=sqlite3_column_int64(st,2),cost=sqlite3_column_int64(st,3),human=sqlite3_column_int64(st,4),margin=sqlite3_column_int64(st,9);
        printf("%s\t%d\t%s\t",id,zc_stage_depth(stage),stage);
        if (comp_null) printf("-"); else printf("%lld",(long long)comp);
        if (cost_null) printf("\t-\t"); else printf("\t%lld\t",(long long)cost);
        if (margin_null) printf("-"); else printf("%lld",(long long)margin);
        printf("\t%lld\t",(long long)human);
        if (margin_null || human<=0) printf("-"); else printf("%.2f",(double)margin/(double)human);
        printf("\t%lld\t%lld\t%d/%d\n",(long long)sqlite3_column_int64(st,5),(long long)sqlite3_column_int64(st,6),
               sqlite3_column_int(st,7),sqlite3_column_int(st,8));
    }
    sqlite3_finalize(st); return 0;
}

static int zc_stats(sqlite3 *db) {
    sqlite3_stmt *st=NULL;
    const char *sql="SELECT stage,count(*),coalesce(sum(compensation_cents),0),coalesce(sum(actual_revenue_cents),0),coalesce(sum(actual_cost_cents),0),coalesce(sum(actual_human_minutes),0) FROM opportunities GROUP BY stage ORDER BY CASE stage WHEN 'observed' THEN 0 WHEN 'specified' THEN 1 WHEN 'solvable' THEN 2 WHEN 'proposal_ready' THEN 3 WHEN 'outreach_approved' THEN 4 WHEN 'accepted' THEN 5 WHEN 'executing' THEN 6 WHEN 'verified' THEN 7 WHEN 'delivered' THEN 8 WHEN 'paid' THEN 9 WHEN 'retained' THEN 10 ELSE 99 END";
    if (sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK) return 1;
    puts("depth\tstage\tcount\tlisted_compensation_cents\tactual_revenue_cents\tactual_cost_cents\tactual_human_minutes");
    while (sqlite3_step(st)==SQLITE_ROW) {
        const char *stage=(const char *)sqlite3_column_text(st,0);
        printf("%d\t%s\t%d\t%lld\t%lld\t%lld\t%lld\n",zc_stage_depth(stage),stage,sqlite3_column_int(st,1),
            (long long)sqlite3_column_int64(st,2),(long long)sqlite3_column_int64(st,3),
            (long long)sqlite3_column_int64(st,4),(long long)sqlite3_column_int64(st,5));
    }
    sqlite3_finalize(st); return 0;
}

static int zc_queue_batch(sqlite3 *db, const char *path, int limit) {
    if (!path||!*path) return 1; if (limit<1) limit=1000; if (limit>100000) limit=100000;
    FILE *out=fopen(path,"wx"); if (!out) { fprintf(stderr,"Refusing queue batch output: %s\n",path); return 1; }
    sqlite3_stmt *st=NULL; int count=0,rc=1;
    /* A stated price is not a margin: a source-backed cost basis must be independently satisfied. */
    const char *sql="SELECT o.id,o.record_digest FROM opportunities o WHERE o.stage IN ('specified','solvable') "
        "AND o.compensation_cents IS NOT NULL AND o.compensation_cents>o.expected_compute_cents+o.expected_external_cents "
        "AND EXISTS (SELECT 1 FROM opportunity_requirements r WHERE r.opportunity_id=o.id "
        "AND r.kind='material' AND r.requirement_key='cost_basis_confirmed' "
        "AND r.state='satisfied' AND r.evidence_ref NOT IN ('','-')) "
        "ORDER BY (o.compensation_cents-o.expected_compute_cents-o.expected_external_cents)*1.0/"
        "CASE WHEN o.expected_human_minutes>0 THEN o.expected_human_minutes ELSE 1 END DESC,"
        "o.reuse_points DESC,o.risk_points ASC LIMIT ?";
    if (sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK) goto done;
    sqlite3_bind_int(st,1,limit);
    while (sqlite3_step(st)==SQLITE_ROW) {
        const char *id=(const char *)sqlite3_column_text(st,0),*digest=(const char *)sqlite3_column_text(st,1);
        if (fprintf(out,"zc-%.16s\tfinance.zero-close.prepare\tfinance://opportunity/%s\tqueue\t100\t1\t1000\n",digest,id)<0) goto done;
        count++;
    }
    if (fflush(out)||ferror(out)) goto done;
    printf("queueable\t%d\nbatch\t%s\nhuman_line\toutreach_not_authorized\n",count,path); rc=0;
done:
    sqlite3_finalize(st); if (fclose(out)) rc=1; if (rc) unlink(path); return rc;
}

/* Read-only view over the real Finance, CRM and Outreach stores. */
#include "opportunity_inspect.inc"

static int cmd_opportunity_dispatch(sqlite3 *db, int argc, char **argv) {
    if (argc<3) { fprintf(stderr,"Usage: bonfyre-finance opportunity <import|requirements-import|requirement-set|refresh|rank|inspect|attest|queue-batch|stats>\n"); return 1; }
    const char *sub=argv[2];
    if (!strcmp(sub,"import")) {
        const char *file=arg_get(argc,argv,"--file"); if (!file) return 1; return zc_import_opportunities(db,file);
    } else if (!strcmp(sub,"requirements-import")) {
        const char *file=arg_get(argc,argv,"--file"); if (!file) return 1; return zc_import_requirements(db,file);
    } else if (!strcmp(sub,"requirement-set")) {
        const char *id=arg_get(argc,argv,"--id"),*kind=arg_get(argc,argv,"--kind"),*key=arg_get(argc,argv,"--key"),*state=arg_get(argc,argv,"--state"),*evidence=arg_get(argc,argv,"--evidence");
        if (!id||!kind||!key||!state||!evidence) return 1; return zc_requirement_set(db,id,kind,key,state,evidence);
    } else if (!strcmp(sub,"refresh")) {
        const char *id=arg_get(argc,argv,"--id"); if (!zc_refresh(db,id)) return 1; printf("refreshed\t%s\n",id?id:"all"); return 0;
    } else if (!strcmp(sub,"rank")) {
        const char *n=arg_get(argc,argv,"--limit"); return zc_rank(db,n?atoi(n):25);
    } else if (!strcmp(sub,"inspect")) {
        const char *id=arg_get(argc,argv,"--id"),*cms=arg_get(argc,argv,"--cms-db");
        const char *outreach=arg_get(argc,argv,"--outreach-db");
        return zc_inspect_opportunity(db,id,cms,outreach);
    } else if (!strcmp(sub,"stats")) return zc_stats(db);
    else if (!strcmp(sub,"queue-batch")) {
        const char *out=arg_get(argc,argv,"--out"),*n=arg_get(argc,argv,"--limit"); return zc_queue_batch(db,out,n?atoi(n):1000);
    } else if (!strcmp(sub,"attest")) {
        const char *id=arg_get(argc,argv,"--id"),*kind=arg_get(argc,argv,"--kind"),*ref=arg_get(argc,argv,"--ref");
        if (!id||!kind||!ref) return 1;
        sqlite3_int64 rev=0,cost=0,human=0; int has=0;
        if (!strcmp(kind,"payment")) {
            const char *a=arg_get(argc,argv,"--actual-revenue-cents"),*b=arg_get(argc,argv,"--actual-cost-cents"),*c=arg_get(argc,argv,"--actual-human-minutes");
            if (!a||!b||!c||!zc_parse_nonnegative(a,&rev,0,NULL)||!zc_parse_nonnegative(b,&cost,0,NULL)||!zc_parse_nonnegative(c,&human,0,NULL)) return 1;
            has=1;
            /* An opportunity is paid when Pay holds the payment. The
             * reference attested here has to be one Pay recorded, for the
             * amount claimed, and not one another opportunity already
             * counted; otherwise anyone could type an opportunity to paid. */
            const char *pay_path=arg_get(argc,argv,"--pay-db");
            if (!pay_path) pay_path=getenv("BONFYRE_PAY_DB");
            if (!pay_path||!*pay_path) {
                fprintf(stderr,"Payment attestation needs --pay-db: the Pay store that holds the payment being attested\n");
                return 1;
            }
            sqlite3 *pay=NULL; sqlite3_stmt *q=NULL; sqlite3_int64 recorded=-1; int payments=0;
            if (sqlite3_open_v2(pay_path,&pay,SQLITE_OPEN_READONLY,NULL)==SQLITE_OK &&
                sqlite3_prepare_v2(pay,"SELECT COUNT(*),COALESCE(SUM(amount_cents),0) FROM payments WHERE reference=?",-1,&q,NULL)==SQLITE_OK) {
                sqlite3_bind_text(q,1,ref,-1,SQLITE_STATIC);
                if (sqlite3_step(q)==SQLITE_ROW) { payments=sqlite3_column_int(q,0); recorded=sqlite3_column_int64(q,1); }
            }
            if (q) sqlite3_finalize(q);
            if (pay) sqlite3_close(pay);
            if (recorded<0) { fprintf(stderr,"Cannot read payments from %s\n",pay_path); return 1; }
            if (payments==0) { fprintf(stderr,"Pay holds no payment with reference %s; nothing was received to attest\n",ref); return 1; }
            if (recorded!=rev) {
                fprintf(stderr,"Pay recorded %lld cents under reference %s, not the %lld cents claimed\n",
                        (long long)recorded,ref,(long long)rev);
                return 1;
            }
            sqlite3_stmt *used=NULL; int taken=0;
            if (sqlite3_prepare_v2(db,"SELECT 1 FROM opportunities WHERE payment_ref=? AND id<>?",-1,&used,NULL)==SQLITE_OK) {
                sqlite3_bind_text(used,1,ref,-1,SQLITE_STATIC); sqlite3_bind_text(used,2,id,-1,SQLITE_STATIC);
                taken=sqlite3_step(used)==SQLITE_ROW;
            }
            if (used) sqlite3_finalize(used);
            if (taken) { fprintf(stderr,"Payment reference %s is already attested for another opportunity\n",ref); return 1; }
        }
        return zc_attest(db,id,kind,ref,rev,cost,human,has);
    }
    fprintf(stderr,"Unknown opportunity command: %s\n",sub); return 1;
}


/* Existing finance owner delegates the quantitative calculus to its pinned provider. */
static int cmd_payment_options(int argc, char **argv) {
    const char *root = NULL, *input = NULL, *output = NULL, *bun = NULL;
    for (int i = 2; i < argc; i++) {
        if (i + 1 >= argc) { fprintf(stderr, "Missing payment-options argument\n"); return 2; }
        const char *key = argv[i++], *value = argv[i];
        if (!strcmp(key, "--root") && !root) root = value;
        else if (!strcmp(key, "--input") && !input) input = value;
        else if (!strcmp(key, "--output") && !output) output = value;
        else if (!strcmp(key, "--bun") && !bun) bun = value;
        else { fprintf(stderr, "Unknown or duplicate payment-options option: %s\n", key); return 2; }
    }
    if (!root) root = getenv("BONFYRE_ROOT");
    if (!root || !input) { fprintf(stderr, "payment-options requires --root DIR and --input JSON\n"); return 2; }
    char script[MAX_PATH], local_bun[MAX_PATH];
    int n = snprintf(script, sizeof(script), "%s/integrations/finance/quantlib.mjs", root);
    if (n < 0 || (size_t)n >= sizeof(script) || access(script, R_OK) != 0) {
        fprintf(stderr, "QuantLib provider boundary not readable under the supplied root\n"); return 2;
    }
    if (!bun) bun = getenv("BONFYRE_BUN");
    if (!bun || !*bun) {
        const char *home = getenv("HOME");
        n = snprintf(local_bun, sizeof(local_bun), "%s/.bun/bin/bun", home ? home : "");
        bun = n > 0 && (size_t)n < sizeof(local_bun) && access(local_bun, X_OK) == 0 ? local_bun : "bun";
    }
    char *args[] = {(char *)bun, script, (char *)input, (char *)output, NULL};
    execvp(bun, args); /* argv, not shell interpolation; no finance database opened */
    perror("Unable to execute quantitative provider");
    return 1;
}

static void usage(void) {
    fprintf(stderr,
        "BonfyreFinance — service arbitrage, labor pipeline, and bundle pricing\n\n"
        "Usage:\n"
        "  bonfyre-finance payment-options --root DIR --input JSON [--output JSON] [--bun PATH]\n"
        "  bonfyre-finance [--db FILE] opportunity <import|requirements-import|requirement-set|refresh|rank|attest|queue-batch|stats> ...\n"
        "  bonfyre-finance [--db FILE] service add --name N --buy B --sell S --source SRC\n"
        "  bonfyre-finance [--db FILE] service list\n"
        "  bonfyre-finance [--db FILE] service opportunities\n"
        "  bonfyre-finance [--db FILE] job log --service S --cost C --sold S [--quality Q]\n"
        "  bonfyre-finance [--db FILE] job create --input FILE --type TYPE [--sell-price P]\n"
        "  bonfyre-finance [--db FILE] job list [--status S]\n"
        "  bonfyre-finance [--db FILE] job review --id ID --result pass|fail --reviewer R\n"
        "  bonfyre-finance [--db FILE] job handoff --id ID [--type T]\n"
        "  bonfyre-finance [--db FILE] margin\n"
        "  bonfyre-finance books [--pay-db FILE] [--economy-db FILE] [--journal]   derive a journal and trial balance from recorded invoices, payments and costs\n"
        "  bonfyre-finance [--db FILE] bundle create --name N --components A,B --price P\n"
        "  bonfyre-finance [--db FILE] bundle list\n"
        "  bonfyre-finance [--db FILE] bundle compare --name N\n"
        "  bonfyre-finance [--db FILE] bundle export --name N\n"
        "  bonfyre-finance [--db FILE] components\n"
        "  bonfyre-finance [--db FILE] status\n"
        "  bonfyre-finance layer <artifact_id> [--root DIR]\n");
}

#include "books.inc"
#include "relationships.inc"

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
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
    if (!strcmp(cmd,"--help") || !strcmp(cmd,"-h")) { usage(); return 0; }
    if (!strcmp(cmd,"payment-options")) return cmd_payment_options(cmd_argc,cmd_argv);
    if (!strcmp(cmd,"books")) return cmd_books(cmd_argc,cmd_argv);

    if (strcmp(cmd,"layer")==0 && cmd_argc>=3) {
        const char *root = arg_get(cmd_argc, cmd_argv, "--root");
        char *out = NULL;
        if (bf_layer_finance_json(root, cmd_argv[2], &out) != 0 || !out) return 1;
        puts(out);
        free(out);
        return 0;
    }

    sqlite3 *db=open_db(db_path);
    if (!db) return 1;

    int rc=0;

    if (strcmp(cmd,"relationships")==0) {
        rc=cmd_relationships(db,cmd_argc,cmd_argv);
    } else
    if (strcmp(cmd,"opportunity")==0) {
        rc=cmd_opportunity_dispatch(db,cmd_argc,cmd_argv);
    } else if (strcmp(cmd,"service")==0) {
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
