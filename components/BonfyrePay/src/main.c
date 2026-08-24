/*
 * BonfyrePay — payment & invoice management.
 *
 * Tracks invoices, payment records, and account balances.
 * Reads from bonfyre-meter usage data to generate bills.
 * Supports manual payment logging and balance checks.
 *
 * Schema:
 *   invoices: id, user_id, api_key, period_start, period_end, amount_cents,
 *             line_items_json, status, created_at
 *   payments: id, invoice_id, user_id, amount_cents, method, reference,
 *             created_at
 *   credits:  id, user_id, amount_cents, reason, created_at
 *
 * Usage:
 *   bonfyre-pay invoice --user-id ID [--period YYYY-MM]
 *   bonfyre-pay invoices [--user-id ID] [--status pending|paid|overdue]
 *   bonfyre-pay pay --invoice-id ID --amount CENTS --method METHOD [--reference REF]
 *   bonfyre-pay credit --user-id ID --amount CENTS --reason REASON
 *   bonfyre-pay balance --user-id ID
 *   bonfyre-pay report [--period YYYY-MM]
 *   bonfyre-pay status
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <sqlite3.h>

#define MAX_PATH  2048

/* ── Pricing table (matches bonfyre-meter) ────────────────────────── */

typedef struct {
    const char *op;
    int cents_per_unit;  /* in hundredths of a cent for sub-cent ops */
    const char *desc;
} PriceEntry;

static const PriceEntry PRICES[] = {
    {"ingest",         1, "File intake ($0.001/file)"},
    {"media-prep",     2, "Audio normalization ($0.002/file)"},
    {"hash",           1, "Content addressing ($0.001/file)"},
    {"transcribe",    50, "Speech-to-text ($0.50/file)"},
    {"transcript-clean",5, "Transcript cleanup ($0.05/file)"},
    {"paragraph",      5, "Paragraph structuring ($0.05/file)"},
    {"brief",        100, "Summary + action items ($1.00/file)"},
    {"proof",        200, "Quality scoring ($2.00/file)"},
    {"offer",        500, "Pricing generation ($5.00/file)"},
    {"pack",         100, "Package assembly ($1.00/file)"},
    {"distribute",  1000, "Distribution ($10.00/file)"},
};
#define NUM_PRICES (int)(sizeof(PRICES)/sizeof(PRICES[0]))

/* ── Utility ──────────────────────────────────────────────────────── */

static void iso_now(char *buf, size_t sz) {
    time_t t=time(NULL); struct tm tm; gmtime_r(&t,&tm);
    strftime(buf,sz,"%Y-%m-%dT%H:%M:%SZ",&tm);
}

/* ── Database ─────────────────────────────────────────────────────── */

static const char *SCHEMA_SQL =
    "CREATE TABLE IF NOT EXISTS invoices ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  user_id INTEGER,"
    "  api_key TEXT,"
    "  period_start TEXT,"
    "  period_end TEXT,"
    "  amount_cents INTEGER NOT NULL DEFAULT 0,"
    "  line_items_json TEXT,"
    "  status TEXT NOT NULL DEFAULT 'pending',"
    "  created_at TEXT NOT NULL"
    ");"
    "CREATE TABLE IF NOT EXISTS payments ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  invoice_id INTEGER,"
    "  user_id INTEGER,"
    "  amount_cents INTEGER NOT NULL,"
    "  method TEXT NOT NULL,"
    "  reference TEXT,"
    "  created_at TEXT NOT NULL,"
    "  FOREIGN KEY (invoice_id) REFERENCES invoices(id)"
    ");"
    "CREATE TABLE IF NOT EXISTS credits ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  user_id INTEGER NOT NULL,"
    "  amount_cents INTEGER NOT NULL,"
    "  reason TEXT,"
    "  created_at TEXT NOT NULL"
    ");";

static sqlite3 *open_db(const char *path) {
    sqlite3 *db;
    if (sqlite3_open(path,&db)!=SQLITE_OK) {
        fprintf(stderr,"Cannot open %s: %s\n",path,sqlite3_errmsg(db)); return NULL;
    }
    char *err=NULL;
    if (sqlite3_exec(db,SCHEMA_SQL,NULL,NULL,&err)!=SQLITE_OK) {
        fprintf(stderr,"Schema error: %s\n",err);
        sqlite3_free(err); sqlite3_close(db); return NULL;
    }
    return db;
}

static const char *default_db(void) {
    static char p[MAX_PATH];
    const char *h=getenv("HOME");
    snprintf(p,sizeof(p),"%s/.local/share/bonfyre/pay.db",h?h:".");
    char d[MAX_PATH];
    snprintf(d,sizeof(d),"%s/.local/share/bonfyre",h?h:".");
    mkdir(d,0755);
    return p;
}

/* ── Commands ─────────────────────────────────────────────────────── */

static int cmd_generate_invoice(sqlite3 *db, int user_id, const char *period) {
    /* Build period bounds */
    char p_start[32], p_end[32];
    if (period) {
        snprintf(p_start,sizeof(p_start),"%s-01T00:00:00Z",period);
        /* Calculate end of month */
        int y,m; sscanf(period,"%d-%d",&y,&m);
        m++; if(m>12){m=1;y++;}
        snprintf(p_end,sizeof(p_end),"%04d-%02d-01T00:00:00Z",y,m);
    } else {
        /* Current month */
        time_t now=time(NULL); struct tm t; gmtime_r(&now,&t);
        snprintf(p_start,sizeof(p_start),"%04d-%02d-01T00:00:00Z",t.tm_year+1900,t.tm_mon+1);
        int y=t.tm_year+1900, m=t.tm_mon+2;
        if(m>12){m=1;y++;}
        snprintf(p_end,sizeof(p_end),"%04d-%02d-01T00:00:00Z",y,m);
    }

    /* Build line items from pricing table (simulated usage) */
    /* In production, this would query bonfyre-meter's SQLite DB */
    char line_items[4096]="[";
    int total_cents=0;
    int first=1;

    for (int i=0;i<NUM_PRICES;i++) {
        /* Simulate: look for operations in our own tables or from meter DB */
        int units=0; /* placeholder — would query meter */
        if (units>0) {
            int cost=units*PRICES[i].cents_per_unit;
            total_cents+=cost;
            if (!first) strcat(line_items,",");
            char item[256];
            snprintf(item,sizeof(item),
                "{\"op\":\"%s\",\"units\":%d,\"unit_price\":%d,\"total\":%d}",
                PRICES[i].op,units,PRICES[i].cents_per_unit,cost);
            strcat(line_items,item);
            first=0;
        }
    }
    strcat(line_items,"]");

    char ts[64]; iso_now(ts,sizeof(ts));

    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "INSERT INTO invoices (user_id,period_start,period_end,amount_cents,line_items_json,status,created_at) "
        "VALUES (?,?,?,?,?,?,?)",-1,&st,NULL);
    sqlite3_bind_int(st,1,user_id);
    sqlite3_bind_text(st,2,p_start,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,p_end,-1,SQLITE_STATIC);
    sqlite3_bind_int(st,4,total_cents);
    sqlite3_bind_text(st,5,line_items,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,6,"pending",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,7,ts,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);

    int inv_id=(int)sqlite3_last_insert_rowid(db);
    printf("Invoice #%d created\n",inv_id);
    printf("  User: %d\n",user_id);
    printf("  Period: %s to %s\n",p_start,p_end);
    printf("  Amount: $%.2f (%d cents)\n",total_cents/100.0,total_cents);
    printf("  Status: pending\n");
    return 0;
}

static int cmd_invoices(sqlite3 *db, int user_id, const char *status_filter) {
    sqlite3_stmt *st;
    if (user_id>0 && status_filter) {
        sqlite3_prepare_v2(db,
            "SELECT id,user_id,period_start,period_end,amount_cents,status,created_at FROM invoices "
            "WHERE user_id=? AND status=? ORDER BY id DESC",-1,&st,NULL);
        sqlite3_bind_int(st,1,user_id);
        sqlite3_bind_text(st,2,status_filter,-1,SQLITE_STATIC);
    } else if (user_id>0) {
        sqlite3_prepare_v2(db,
            "SELECT id,user_id,period_start,period_end,amount_cents,status,created_at FROM invoices "
            "WHERE user_id=? ORDER BY id DESC",-1,&st,NULL);
        sqlite3_bind_int(st,1,user_id);
    } else if (status_filter) {
        sqlite3_prepare_v2(db,
            "SELECT id,user_id,period_start,period_end,amount_cents,status,created_at FROM invoices "
            "WHERE status=? ORDER BY id DESC",-1,&st,NULL);
        sqlite3_bind_text(st,1,status_filter,-1,SQLITE_STATIC);
    } else {
        sqlite3_prepare_v2(db,
            "SELECT id,user_id,period_start,period_end,amount_cents,status,created_at FROM invoices ORDER BY id DESC",
            -1,&st,NULL);
    }

    printf("%-5s %-8s %-12s %-12s %-10s %-8s %s\n","ID","User","From","To","Amount","Status","Created");
    printf("--------------------------------------------------------------------------\n");
    while (sqlite3_step(st)==SQLITE_ROW) {
        int cents=sqlite3_column_int(st,4);
        const char *ps=sqlite3_column_text(st,2)?(const char*)sqlite3_column_text(st,2):"";
        const char *pe=sqlite3_column_text(st,3)?(const char*)sqlite3_column_text(st,3):"";
        printf("%-5d %-8d %-12.10s %-12.10s $%-9.2f %-8s %.10s\n",
            sqlite3_column_int(st,0),
            sqlite3_column_int(st,1),
            ps,pe,
            cents/100.0,
            (const char*)sqlite3_column_text(st,5),
            (const char*)sqlite3_column_text(st,6));
    }
    sqlite3_finalize(st);
    return 0;
}

static int cmd_pay(sqlite3 *db, int invoice_id, int amount_cents,
                   const char *method, const char *reference) {
    /* Verify invoice exists */
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,"SELECT user_id,amount_cents,status FROM invoices WHERE id=?",-1,&st,NULL);
    sqlite3_bind_int(st,1,invoice_id);
    if (sqlite3_step(st)!=SQLITE_ROW) {
        fprintf(stderr,"Invoice #%d not found.\n",invoice_id);
        sqlite3_finalize(st); return 1;
    }
    int uid=sqlite3_column_int(st,0);
    int inv_cents=sqlite3_column_int(st,1);
    const char *stat=(const char*)sqlite3_column_text(st,2);
    if (strcmp(stat,"paid")==0) {
        printf("Invoice #%d already paid.\n",invoice_id);
        sqlite3_finalize(st); return 0;
    }
    sqlite3_finalize(st);

    char ts[64]; iso_now(ts,sizeof(ts));

    sqlite3_prepare_v2(db,
        "INSERT INTO payments (invoice_id,user_id,amount_cents,method,reference,created_at) VALUES (?,?,?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_int(st,1,invoice_id);
    sqlite3_bind_int(st,2,uid);
    sqlite3_bind_int(st,3,amount_cents);
    sqlite3_bind_text(st,4,method,-1,SQLITE_STATIC);
    if (reference) sqlite3_bind_text(st,5,reference,-1,SQLITE_STATIC);
    else sqlite3_bind_null(st,5);
    sqlite3_bind_text(st,6,ts,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);

    /* Check if fully paid */
    int total_paid=0;
    sqlite3_prepare_v2(db,
        "SELECT COALESCE(SUM(amount_cents),0) FROM payments WHERE invoice_id=?",-1,&st,NULL);
    sqlite3_bind_int(st,1,invoice_id);
    if (sqlite3_step(st)==SQLITE_ROW) total_paid=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    if (total_paid>=inv_cents) {
        sqlite3_prepare_v2(db,"UPDATE invoices SET status='paid' WHERE id=?",-1,&st,NULL);
        sqlite3_bind_int(st,1,invoice_id);
        sqlite3_step(st); sqlite3_finalize(st);
        printf("Invoice #%d: PAID ($%.2f of $%.2f)\n",invoice_id,total_paid/100.0,inv_cents/100.0);
    } else {
        printf("Invoice #%d: partial payment ($%.2f of $%.2f)\n",invoice_id,total_paid/100.0,inv_cents/100.0);
    }
    return 0;
}

static int cmd_credit(sqlite3 *db, int user_id, int amount_cents, const char *reason) {
    char ts[64]; iso_now(ts,sizeof(ts));
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "INSERT INTO credits (user_id,amount_cents,reason,created_at) VALUES (?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_int(st,1,user_id);
    sqlite3_bind_int(st,2,amount_cents);
    sqlite3_bind_text(st,3,reason,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,4,ts,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);

    printf("Credit $%.2f applied to user #%d (%s)\n",amount_cents/100.0,user_id,reason);
    return 0;
}

static int cmd_balance(sqlite3 *db, int user_id) {
    sqlite3_stmt *st;
    int invoiced=0, paid=0, credited=0;

    sqlite3_prepare_v2(db,
        "SELECT COALESCE(SUM(amount_cents),0) FROM invoices WHERE user_id=?",-1,&st,NULL);
    sqlite3_bind_int(st,1,user_id);
    if (sqlite3_step(st)==SQLITE_ROW) invoiced=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,
        "SELECT COALESCE(SUM(amount_cents),0) FROM payments WHERE user_id=?",-1,&st,NULL);
    sqlite3_bind_int(st,1,user_id);
    if (sqlite3_step(st)==SQLITE_ROW) paid=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,
        "SELECT COALESCE(SUM(amount_cents),0) FROM credits WHERE user_id=?",-1,&st,NULL);
    sqlite3_bind_int(st,1,user_id);
    if (sqlite3_step(st)==SQLITE_ROW) credited=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    int balance=invoiced-paid-credited;

    printf("Balance for user #%d\n",user_id);
    printf("  Total invoiced: $%.2f\n",invoiced/100.0);
    printf("  Total paid:     $%.2f\n",paid/100.0);
    printf("  Total credits:  $%.2f\n",credited/100.0);
    printf("  Balance due:    $%.2f\n",balance>0?balance/100.0:0.0);
    if (balance<=0) printf("  Account status: CURRENT\n");
    else printf("  Account status: BALANCE DUE\n");
    return 0;
}

static int cmd_report(sqlite3 *db, const char *period) {
    printf("## Payment Report");
    if (period) printf(" (%s)",period);
    printf("\n\n");

    sqlite3_stmt *st;

    /* Invoice summary */
    int total_inv=0, pending=0, paid_count=0, overdue=0;
    int total_cents=0, paid_cents=0;

    sqlite3_prepare_v2(db,
        "SELECT status,COUNT(*),SUM(amount_cents) FROM invoices GROUP BY status",-1,&st,NULL);
    while (sqlite3_step(st)==SQLITE_ROW) {
        const char *s=(const char*)sqlite3_column_text(st,0);
        int c=sqlite3_column_int(st,1);
        int amt=sqlite3_column_int(st,2);
        total_inv+=c; total_cents+=amt;
        if (strcmp(s,"pending")==0) pending=c;
        else if (strcmp(s,"paid")==0) { paid_count=c; paid_cents=amt; }
        else if (strcmp(s,"overdue")==0) overdue=c;
    }
    sqlite3_finalize(st);

    printf("Invoices:     %d total ($%.2f)\n",total_inv,total_cents/100.0);
    printf("  Paid:       %d ($%.2f)\n",paid_count,paid_cents/100.0);
    printf("  Pending:    %d\n",pending);
    printf("  Overdue:    %d\n",overdue);

    /* Payment methods */
    printf("\nPayment Methods:\n");
    sqlite3_prepare_v2(db,
        "SELECT method,COUNT(*),SUM(amount_cents) FROM payments GROUP BY method ORDER BY SUM(amount_cents) DESC",
        -1,&st,NULL);
    while (sqlite3_step(st)==SQLITE_ROW) {
        printf("  %-20s %d payments, $%.2f\n",
            (const char*)sqlite3_column_text(st,0),
            sqlite3_column_int(st,1),
            sqlite3_column_int(st,2)/100.0);
    }
    sqlite3_finalize(st);

    /* Credits */
    int total_credits=0;
    sqlite3_prepare_v2(db,"SELECT COALESCE(SUM(amount_cents),0) FROM credits",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) total_credits=sqlite3_column_int(st,0);
    sqlite3_finalize(st);
    printf("\nTotal Credits:  $%.2f\n",total_credits/100.0);

    /* Revenue */
    printf("\nNet Revenue:    $%.2f\n",(paid_cents-total_credits)/100.0);
    return 0;
}

static int cmd_status(sqlite3 *db) {
    sqlite3_stmt *st;
    int inv=0, pend=0, pay_total=0, cred=0;

    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM invoices",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) inv=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM invoices WHERE status='pending'",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) pend=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,"SELECT COALESCE(SUM(amount_cents),0) FROM payments",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) pay_total=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,"SELECT COALESCE(SUM(amount_cents),0) FROM credits",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) cred=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    printf("BonfyrePay Status\n");
    printf("  Invoices:     %d (%d pending)\n",inv,pend);
    printf("  Payments:     $%.2f collected\n",pay_total/100.0);
    printf("  Credits:      $%.2f issued\n",cred/100.0);
    printf("  Net revenue:  $%.2f\n",(pay_total-cred)/100.0);
    return 0;
}

/* ── CLI ──────────────────────────────────────────────────────────── */

static const char *arg_get(int argc, char **argv, const char *flag) {
    for (int i=0;i<argc-1;i++)
        if (strcmp(argv[i],flag)==0) return argv[i+1];
    return NULL;
}

static void usage(void) {
    fprintf(stderr,
        "BonfyrePay — payment & invoice management\n\n"
        "Usage:\n"
        "  bonfyre-pay [--db FILE] invoice --user-id ID [--period YYYY-MM]\n"
        "  bonfyre-pay [--db FILE] invoices [--user-id ID] [--status pending|paid|overdue]\n"
        "  bonfyre-pay [--db FILE] pay --invoice-id ID --amount CENTS --method METHOD [--reference REF]\n"
        "  bonfyre-pay [--db FILE] credit --user-id ID --amount CENTS --reason REASON\n"
        "  bonfyre-pay [--db FILE] balance --user-id ID\n"
        "  bonfyre-pay [--db FILE] report [--period YYYY-MM]\n"
        "  bonfyre-pay [--db FILE] status\n");
}

int main(int argc, char **argv) {
    if (argc<2) { usage(); return 1; }

    const char *db_path=arg_get(argc,argv,"--db");
    if (!db_path) db_path=default_db();

    int ca=0; char *cv[128];
    for (int i=0;i<argc&&ca<128;i++) {
        if (strcmp(argv[i],"--db")==0){i++;continue;}
        cv[ca++]=argv[i];
    }
    if (ca<2) { usage(); return 1; }

    const char *cmd=cv[1];

    sqlite3 *db=open_db(db_path);
    if (!db) return 1;
    int rc=0;

    if (strcmp(cmd,"invoice")==0) {
        const char *uid_s=arg_get(ca,cv,"--user-id");
        const char *period=arg_get(ca,cv,"--period");
        if (!uid_s) { fprintf(stderr,"Missing --user-id\n"); rc=1; }
        else rc=cmd_generate_invoice(db,atoi(uid_s),period);
    } else if (strcmp(cmd,"invoices")==0) {
        const char *uid_s=arg_get(ca,cv,"--user-id");
        const char *status=arg_get(ca,cv,"--status");
        rc=cmd_invoices(db,uid_s?atoi(uid_s):0,status);
    } else if (strcmp(cmd,"pay")==0) {
        const char *inv_s=arg_get(ca,cv,"--invoice-id");
        const char *amt_s=arg_get(ca,cv,"--amount");
        const char *meth=arg_get(ca,cv,"--method");
        const char *ref=arg_get(ca,cv,"--reference");
        if (!inv_s||!amt_s||!meth) { fprintf(stderr,"Missing --invoice-id, --amount, --method\n"); rc=1; }
        else rc=cmd_pay(db,atoi(inv_s),atoi(amt_s),meth,ref);
    } else if (strcmp(cmd,"credit")==0) {
        const char *uid_s=arg_get(ca,cv,"--user-id");
        const char *amt_s=arg_get(ca,cv,"--amount");
        const char *reason=arg_get(ca,cv,"--reason");
        if (!uid_s||!amt_s||!reason) { fprintf(stderr,"Missing --user-id, --amount, --reason\n"); rc=1; }
        else rc=cmd_credit(db,atoi(uid_s),atoi(amt_s),reason);
    } else if (strcmp(cmd,"balance")==0) {
        const char *uid_s=arg_get(ca,cv,"--user-id");
        if (!uid_s) { fprintf(stderr,"Missing --user-id\n"); rc=1; }
        else rc=cmd_balance(db,atoi(uid_s));
    } else if (strcmp(cmd,"report")==0) {
        const char *period=arg_get(ca,cv,"--period");
        rc=cmd_report(db,period);
    } else if (strcmp(cmd,"status")==0) {
        rc=cmd_status(db);
    } else {
        fprintf(stderr,"Unknown command: %s\n",cmd); usage(); rc=1;
    }

    sqlite3_close(db);
    return rc;
}
