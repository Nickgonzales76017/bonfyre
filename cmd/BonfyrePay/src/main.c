/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

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
 *   bonfyre-pay invoice --user-id ID --api-key KEY [--meter-db FILE] [--period YYYY-MM]
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
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <sqlite3.h>
#include <bonfyre.h>
#include <bf_commerce_stage.h>
#include "../../../lib/libbonfyre/include/bf_help.h"

#define MAX_PATH  2048

/* ── Utility ──────────────────────────────────────────────────────── */

static void iso_now(char *buf, size_t sz) { bf_iso_timestamp(buf, sz); }

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
    sqlite3 *db=NULL;
    /* Invoices, credits and recorded payments cannot use cache durability.
     * Refuse if WAL + FULL is not applied and read back. */
    int rc=bf_sqlite3_open_tier(path,&db,BF_SQLITE_DURABLE);
    if (rc!=SQLITE_OK) {
        fprintf(stderr,"Payment store cannot open at full durability (SQLite %d)\n",rc); return NULL;
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

static int cmd_generate_invoice(sqlite3 *db, int user_id, const char *api_key,
                                const char *meter_path, const char *period) {
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

    /* Line items are the metered events for this key in the period. A price
     * is whatever admitted rate the meter applied when the event was recorded;
     * an event the meter left unpriced is reported and not billed. Nothing is
     * priced here by operation name. */
    sqlite3 *meter=NULL;
    if (sqlite3_open_v2(meter_path,&meter,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK) {
        fprintf(stderr,"Cannot read meter store %s: %s\n",meter_path,
                meter?sqlite3_errmsg(meter):"open failed");
        if (meter) sqlite3_close(meter);
        return 1;
    }
    sqlite3_stmt *ms=NULL;
    if (sqlite3_prepare_v2(meter,
            "SELECT op, COUNT(*), SUM(cost IS NOT NULL), SUM(cost IS NULL), "
            "       COALESCE(SUM(cost),0.0) "
            "FROM events WHERE key_id=? AND timestamp>=? AND timestamp<? "
            "GROUP BY op ORDER BY op",-1,&ms,NULL)!=SQLITE_OK) {
        fprintf(stderr,"Meter store %s has no readable events: %s\n",
                meter_path,sqlite3_errmsg(meter));
        sqlite3_close(meter);
        return 1;
    }
    sqlite3_bind_text(ms,1,api_key,-1,SQLITE_STATIC);
    sqlite3_bind_text(ms,2,p_start,-1,SQLITE_STATIC);
    sqlite3_bind_text(ms,3,p_end,-1,SQLITE_STATIC);

    char line_items[16384]="[";
    size_t used=1;
    double total_usd=0.0;
    long events=0, unpriced=0;
    int first=1, truncated=0;
    while (sqlite3_step(ms)==SQLITE_ROW) {
        const char *op=(const char*)sqlite3_column_text(ms,0);
        long n=(long)sqlite3_column_int64(ms,1);
        long np=(long)sqlite3_column_int64(ms,2);
        long nu=(long)sqlite3_column_int64(ms,3);
        double usd=sqlite3_column_double(ms,4);
        events+=n; unpriced+=nu; total_usd+=usd;
        /* No priced event means the amount is unknown, which is not zero. */
        char amount[32]="null";
        if (np>0) snprintf(amount,sizeof(amount),"%.6f",usd);
        char item[512];
        int w=snprintf(item,sizeof(item),
            "%s{\"op\":\"%s\",\"events\":%ld,\"priced_events\":%ld,"
            "\"unpriced_events\":%ld,\"amount_usd\":%s}",
            first?"":",",op?op:"",n,np,nu,amount);
        if (w<0 || used+(size_t)w+2>=sizeof(line_items)) { truncated=1; break; }
        memcpy(line_items+used,item,(size_t)w); used+=(size_t)w; line_items[used]=0;
        first=0;
    }
    sqlite3_finalize(ms);
    sqlite3_close(meter);
    if (truncated) {
        fprintf(stderr,"Too many distinct operations to itemise; no invoice written.\n");
        return 1;
    }
    line_items[used++]=']'; line_items[used]=0;

    if (events==0) {
        fprintf(stderr,"No metered events for key %s between %s and %s; no invoice written.\n",
                api_key,p_start,p_end);
        return 2;
    }
    if (events==unpriced) {
        fprintf(stderr,"%ld metered events for key %s, none priced by an admitted rate; "
                       "no invoice written.\n",events,api_key);
        return 2;
    }
    int total_cents=(int)(total_usd*100.0+0.5);

    char ts[64]; iso_now(ts,sizeof(ts));

    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "INSERT INTO invoices (user_id,api_key,period_start,period_end,amount_cents,line_items_json,status,created_at) "
        "VALUES (?,?,?,?,?,?,?,?)",-1,&st,NULL);
    sqlite3_bind_int(st,1,user_id);
    sqlite3_bind_text(st,2,api_key,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,p_start,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,4,p_end,-1,SQLITE_STATIC);
    sqlite3_bind_int(st,5,total_cents);
    sqlite3_bind_text(st,6,line_items,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,7,"pending",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,8,ts,-1,SQLITE_STATIC);
    if (sqlite3_step(st)!=SQLITE_DONE) {
        fprintf(stderr,"Invoice not written: %s\n",sqlite3_errmsg(db));
        sqlite3_finalize(st); return 1;
    }
    sqlite3_finalize(st);

    int inv_id=(int)sqlite3_last_insert_rowid(db);
    printf("Invoice #%d created\n",inv_id);
    printf("  User: %d\n",user_id);
    printf("  Period: %s to %s\n",p_start,p_end);
    printf("  Key: %s\n",api_key);
    printf("  Metered events: %ld (%ld unpriced, not billed)\n",events,unpriced);
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

    /* A payment is money that arrived. It is recorded against whatever shows
     * that it did: the processor's transaction id, a bank line, a receipt
     * number. Without one this would mark an invoice paid on someone's say-so,
     * and the same reference twice would count the same money twice. */
    if (amount_cents<=0) {
        fprintf(stderr,"A payment is a positive amount in cents; got %d.\n",amount_cents);
        return 2;
    }
    if (!reference||!*reference) {
        fprintf(stderr,"A payment needs --reference: what shows the money arrived "
                       "(a transaction id, a bank line, a receipt number).\n");
        return 2;
    }
    sqlite3_prepare_v2(db,"SELECT id FROM payments WHERE reference=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,reference,-1,SQLITE_STATIC);
    if (sqlite3_step(st)==SQLITE_ROW) {
        fprintf(stderr,"Reference %s is already recorded as payment #%d; the same money is not counted twice.\n",
                reference,sqlite3_column_int(st,0));
        sqlite3_finalize(st); return 2;
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
    sqlite3_bind_text(st,5,reference,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,6,ts,-1,SQLITE_STATIC);
    if (sqlite3_step(st)!=SQLITE_DONE) {
        fprintf(stderr,"Payment not recorded: %s\n",sqlite3_errmsg(db));
        sqlite3_finalize(st); return 1;
    }
    sqlite3_finalize(st);

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
        "  bonfyre-pay [--db FILE] invoice --user-id ID --api-key KEY [--meter-db FILE] [--period YYYY-MM]\n"
        "  bonfyre-pay [--db FILE] invoices [--user-id ID] [--status pending|paid|overdue]\n"
        "  bonfyre-pay [--db FILE] pay --invoice-id ID --amount CENTS --method METHOD [--reference REF]\n"
        "  bonfyre-pay [--db FILE] credit --user-id ID --amount CENTS --reason REASON\n"
        "  bonfyre-pay [--db FILE] balance --user-id ID\n"
        "  bonfyre-pay [--db FILE] report [--period YYYY-MM]\n"
        "  bonfyre-pay [--db FILE] status\n"
        "  bonfyre-pay layer <artifact_id> [--op verify|materialize|compose|run] [--root DIR]\n");
}

/* Ids and cents are whole positive numbers. atoi read "u1" as user 0 and
 * "-50" as a negative credit; both were stored. */
static int pos_int(const char *s) {
    if (!s || !*s) return -1;
    char *end = NULL;
    long v = strtol(s, &end, 10);
    if (*end || v <= 0 || v > 2147483647L) return -1;
    return (int)v;
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
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

    if (strcmp(cmd,"layer")==0 && ca>=3) {
        const char *op = arg_get(ca, cv, "--op");
        char *out = NULL;
        if (!op) op = "verify";
        if (bf_layer_pay_json(cv[2], op, &out) != 0 || !out) return 1;
        puts(out);
        free(out);
        return 0;
    }

    sqlite3 *db=open_db(db_path);
    if (!db) return 1;
    int rc=0;

    if (strcmp(cmd,"invoice")==0) {
        const char *uid_s=arg_get(ca,cv,"--user-id");
        const char *period=arg_get(ca,cv,"--period");
        const char *api_key=arg_get(ca,cv,"--api-key");
        static char meter_resolved[4096];
        const char *meter_path=bf_state_path(meter_resolved,sizeof(meter_resolved),
            "BONFYRE_METER_DB","state/bonfyre-meter.db",arg_get(ca,cv,"--meter-db"));
        if (!uid_s||!api_key) { fprintf(stderr,"Missing --user-id or --api-key\n"); rc=1; }
        else if (!meter_path) {
            fprintf(stderr,"No meter store coordinate: pass --meter-db or set BONFYRE_METER_DB.\n"); rc=1;
        }
        else {
            /* "auto" for the key or the period is answered by the meter store
             * itself, and only when it has one answer: a store holding a
             * single key, or events in a single month. More than one is a
             * choice somebody has to make, so it is refused. */
            static char auto_key[256], auto_period[16];
            int want_key=!strcmp(api_key,"auto"), want_period=period&&!strcmp(period,"auto");
            if (want_key||want_period) {
                sqlite3 *meter=NULL; sqlite3_stmt *q=NULL;
                if (sqlite3_open_v2(meter_path,&meter,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK ||
                    sqlite3_prepare_v2(meter,"SELECT COUNT(DISTINCT key_id),MIN(key_id),"
                        "COUNT(DISTINCT substr(timestamp,1,7)),MIN(substr(timestamp,1,7)) FROM events",-1,&q,NULL)!=SQLITE_OK ||
                    sqlite3_step(q)!=SQLITE_ROW) {
                    fprintf(stderr,"Cannot read meter store %s\n",meter_path); rc=1;
                } else {
                    int keys=sqlite3_column_int(q,0), months=sqlite3_column_int(q,2);
                    if (want_key && keys!=1) {
                        fprintf(stderr,"--api-key auto needs a meter store with one key; this one has %d\n",keys); rc=2;
                    } else if (want_period && months!=1) {
                        fprintf(stderr,"--period auto needs events in one month; this store spans %d\n",months); rc=2;
                    } else {
                        if (want_key) { snprintf(auto_key,sizeof(auto_key),"%s",(const char*)sqlite3_column_text(q,1)); api_key=auto_key; }
                        if (want_period) { snprintf(auto_period,sizeof(auto_period),"%s",(const char*)sqlite3_column_text(q,3)); period=auto_period; }
                    }
                }
                if (q) sqlite3_finalize(q);
                if (meter) sqlite3_close(meter);
            }
            if (!rc && pos_int(uid_s)<0) { fprintf(stderr,"--user-id must be a positive whole number\n"); rc=1; }
            if (!rc) rc=cmd_generate_invoice(db,pos_int(uid_s),api_key,meter_path,period);
        }
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
        else if (pos_int(inv_s)<0||pos_int(amt_s)<0) { fprintf(stderr,"--invoice-id and --amount must be positive whole numbers\n"); rc=1; }
        else {
            /* A payment made for an exchange is that exchange's payment
             * stage. The chain decides whether it can be, and requires a
             * commitment first; when it refuses, the payment is not kept. */
            const char *exchange=arg_get(ca,cv,"--exchange");
            const char *commerce_db=arg_get(ca,cv,"--commerce-db");
            if ((exchange&&!commerce_db)||(commerce_db&&!exchange)) {
                fprintf(stderr,"--exchange and --commerce-db go together.\n"); rc=2;
            } else if (!exchange) {
                rc=cmd_pay(db,atoi(inv_s),atoi(amt_s),meth,ref);
            } else {
                /* Asked before anything is said about the invoice, so a
                 * payment the chain will refuse is never reported as paid. */
                sqlite3 *chain=NULL; sqlite3_stmt *has=NULL; int committed=0;
                if (sqlite3_open_v2(commerce_db,&chain,SQLITE_OPEN_READONLY,NULL)==SQLITE_OK &&
                    sqlite3_prepare_v2(chain,"SELECT 1 FROM commerce_progress WHERE exchange=? AND stage='commitment'",
                                       -1,&has,NULL)==SQLITE_OK) {
                    sqlite3_bind_text(has,1,exchange,-1,SQLITE_STATIC);
                    committed=sqlite3_step(has)==SQLITE_ROW;
                }
                if (has) sqlite3_finalize(has);
                if (chain) sqlite3_close(chain);
                if (!committed) {
                    fprintf(stderr,"Payment not kept: %s has no commitment in the commerce chain, "
                                   "and a payment follows a commitment.\n",exchange);
                    rc=3;
                    sqlite3_close(db);
                    return rc;
                }
                sqlite3_exec(db,"BEGIN IMMEDIATE",NULL,NULL,NULL);
                rc=cmd_pay(db,atoi(inv_s),atoi(amt_s),meth,ref);
                if (rc==0) {
                    char detail[256];
                    snprintf(detail,sizeof(detail),"invoice %s, %s cents by %s, ref %s",inv_s,amt_s,meth,ref?ref:"");
                    int stage=bf_commerce_stage_record(commerce_db,exchange,"payment","payment rail",detail);
                    if (stage!=BF_COMMERCE_STAGE_RECORDED) {
                        fprintf(stderr,"Payment not kept: the commerce chain did not record a payment for %s%s\n",exchange,
                                stage==BF_COMMERCE_STAGE_REFUSED?"; a commitment has to be established for it first.":".");
                        rc=3;
                    }
                }
                sqlite3_exec(db,rc==0?"COMMIT":"ROLLBACK",NULL,NULL,NULL);
            }
        }
    } else if (strcmp(cmd,"credit")==0) {
        const char *uid_s=arg_get(ca,cv,"--user-id");
        const char *amt_s=arg_get(ca,cv,"--amount");
        const char *reason=arg_get(ca,cv,"--reason");
        if (!uid_s||!amt_s||!reason) { fprintf(stderr,"Missing --user-id, --amount, --reason\n"); rc=1; }
        else if (pos_int(uid_s)<0||pos_int(amt_s)<0) { fprintf(stderr,"--user-id and --amount must be positive whole numbers\n"); rc=1; }
        else rc=cmd_credit(db,pos_int(uid_s),pos_int(amt_s),reason);
    } else if (strcmp(cmd,"balance")==0) {
        const char *uid_s=arg_get(ca,cv,"--user-id");
        if (!uid_s) { fprintf(stderr,"Missing --user-id\n"); rc=1; }
        else if (pos_int(uid_s)<0) { fprintf(stderr,"--user-id must be a positive whole number\n"); rc=1; }
        else rc=cmd_balance(db,pos_int(uid_s));
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
