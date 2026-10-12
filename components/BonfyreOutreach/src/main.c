/*
 * BonfyreOutreach — quiet distribution engine.
 *
 * Replaces: QuietDistributionEngine (distribute.py)
 *
 * Tracks outreach sends across channels (dm, listing, post, landing),
 * logs replies, generates follow-up copy based on age rules, and
 * produces channel performance reports.
 *
 * Follow-up logic:
 *   ≤1 day  → wait
 *   ≤3 days → soft follow-up
 *   ≤7 days → proof angle
 *   >7 days → close loop / archive
 *
 * Storage: SQLite (sends table with channel, target, offer, reply tracking).
 *
 * Usage:
 *   bonfyre-outreach channels
 *   bonfyre-outreach templates --channel dm|listing|post
 *   bonfyre-outreach send --channel CH --target WHO [--offer NAME] [--message MSG]
 *   bonfyre-outreach reply --id ID --type positive|negative|neutral [--notes N]
 *   bonfyre-outreach pending
 *   bonfyre-outreach followup [--id ID]
 *   bonfyre-outreach report
 *   bonfyre-outreach status
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <sqlite3.h>

#define MAX_PATH  2048
#define MAX_NAME  256
#define MAX_MSG   4096

/* ── Channels ─────────────────────────────────────────────────────────── */

typedef struct {
    const char *id;
    const char *desc;
} Channel;

static const Channel CHANNELS[] = {
    {"dm",      "Direct messages (Twitter, LinkedIn, email)"},
    {"listing", "Marketplace listings (Fiverr, Upwork, etc.)"},
    {"post",    "Content posts (Twitter, Reddit, Indie Hackers)"},
    {"landing", "Landing page / link-in-bio"},
};
#define NUM_CHANNELS (int)(sizeof(CHANNELS)/sizeof(CHANNELS[0]))

/* ── Templates ────────────────────────────────────────────────────────── */

typedef struct {
    const char *channel;
    const char *name;
    const char *body;
} Template;

static const Template TEMPLATES[] = {
    {"dm", "founder-cold",
     "Hey [NAME] -- I run a small transcription service built on local AI. "
     "I turn interview recordings into clean transcripts + summaries + action items, "
     "usually same-day. Would you want to try one for free so you can see the quality?"},
    {"dm", "operator-cold",
     "Hi [NAME] -- I noticed you're doing a lot of calls/interviews. "
     "I offer fast AI transcription with human QA -- transcripts, summaries, and action items "
     "delivered same-day. Want me to run a sample on one of your recordings?"},
    {"listing", "service-listing",
     "# Professional AI Transcription Service\n\n"
     "Fast, accurate transcription powered by local AI with human quality checks.\n\n"
     "## What You Get\n"
     "- Clean, formatted transcript\n"
     "- Executive summary with key points\n"
     "- Action items extracted automatically\n\n"
     "## Pricing\n"
     "- Standard (< 30 min): $12\n"
     "- Extended (30-60 min): $20\n"
     "- Deep (60+ min, multi-speaker): $35\n\n"
     "Same-day delivery. Satisfaction guaranteed."},
    {"post", "value-post",
     "I built a local AI transcription pipeline that turns messy recordings into "
     "clean transcripts + summaries + action items.\n\n"
     "No cloud. No subscriptions. Just send a file, get a deliverable.\n\n"
     "Happy to run a free sample if anyone wants to test it."},
};
#define NUM_TEMPLATES (int)(sizeof(TEMPLATES)/sizeof(TEMPLATES[0]))

/* ── Utility ──────────────────────────────────────────────────────────── */

static void iso_timestamp(char *buf, size_t sz) {
    time_t now=time(NULL); struct tm t; gmtime_r(&now,&t);
    strftime(buf,sz,"%Y-%m-%dT%H:%M:%SZ",&t);
}

static time_t parse_iso(const char *s) {
    if (!s || !*s) return 0;
    struct tm t; memset(&t,0,sizeof(t));
    /* Parse YYYY-MM-DDTHH:MM:SSZ */
    if (sscanf(s,"%d-%d-%dT%d:%d:%d",
        &t.tm_year,&t.tm_mon,&t.tm_mday,&t.tm_hour,&t.tm_min,&t.tm_sec)==6) {
        t.tm_year-=1900; t.tm_mon-=1;
        return timegm(&t);
    }
    return 0;
}

static int age_days(const char *iso_str) {
    time_t sent=parse_iso(iso_str);
    if (!sent) return 0;
    time_t now=time(NULL);
    return (int)((now-sent)/(60*60*24));
}

static const char *followup_move(int days) {
    if (days<=1) return "wait";
    if (days<=3) return "soft follow-up";
    if (days<=7) return "send proof angle";
    return "close loop or archive";
}

/* ── Database ─────────────────────────────────────────────────────────── */

static const char *SCHEMA_SQL =
    "CREATE TABLE IF NOT EXISTS sends ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  channel TEXT NOT NULL,"
    "  target TEXT NOT NULL,"
    "  offer_name TEXT,"
    "  template TEXT,"
    "  message_preview TEXT,"
    "  sent_at TEXT NOT NULL,"
    "  reply_type TEXT,"
    "  reply_notes TEXT,"
    "  replied_at TEXT"
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

static const char *default_db(void) {
    static char path[MAX_PATH];
    const char *home=getenv("HOME");
    if (home) snprintf(path,sizeof(path),"%s/.local/share/bonfyre/outreach.db",home);
    else snprintf(path,sizeof(path),"outreach.db");
    char dir[MAX_PATH];
    snprintf(dir,sizeof(dir),"%s/.local/share/bonfyre",home?home:".");
    mkdir(dir,0755);
    return path;
}

/* ── Commands ─────────────────────────────────────────────────────────── */

static int cmd_channels(void) {
    printf("## Channels\n\n");
    for (int i=0;i<NUM_CHANNELS;i++){
        printf("  %-10s %s\n",CHANNELS[i].id,CHANNELS[i].desc);
        /* List templates for this channel */
        for (int j=0;j<NUM_TEMPLATES;j++)
            if (strcmp(TEMPLATES[j].channel,CHANNELS[i].id)==0)
                printf("    > %s\n",TEMPLATES[j].name);
    }
    return 0;
}

static int cmd_templates(const char *channel) {
    printf("## %s Templates\n\n",channel);
    int found=0;
    for (int i=0;i<NUM_TEMPLATES;i++){
        if (strcmp(TEMPLATES[i].channel,channel)==0){
            printf("### %s\n%s\n\n",TEMPLATES[i].name,TEMPLATES[i].body);
            found=1;
        }
    }
    if (!found) printf("No templates for channel: %s\n",channel);
    return 0;
}

static int cmd_send(sqlite3 *db, const char *channel, const char *target,
                    const char *offer, const char *message) {
    char ts[64]; iso_timestamp(ts,sizeof(ts));

    /* Build preview from template or offer */
    char preview[MAX_MSG]="";
    if (message) {
        for (int i=0;i<NUM_TEMPLATES;i++){
            if (strcmp(TEMPLATES[i].channel,channel)==0 &&
                strcmp(TEMPLATES[i].name,message)==0){
                snprintf(preview,sizeof(preview),"%.80s...",TEMPLATES[i].body);
                break;
            }
        }
    }

    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "INSERT INTO sends (channel,target,offer_name,template,message_preview,sent_at) VALUES (?,?,?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,channel,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,target,-1,SQLITE_STATIC);
    if (offer) sqlite3_bind_text(st,3,offer,-1,SQLITE_STATIC);
    else sqlite3_bind_null(st,3);
    if (message) sqlite3_bind_text(st,4,message,-1,SQLITE_STATIC);
    else sqlite3_bind_null(st,4);
    if (preview[0]) sqlite3_bind_text(st,5,preview,-1,SQLITE_STATIC);
    else sqlite3_bind_null(st,5);
    sqlite3_bind_text(st,6,ts,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);

    int send_id=(int)sqlite3_last_insert_rowid(db);
    printf("Logged send #%d: %s -> %s\n",send_id,channel,target);
    if (offer) printf("  Offer: %s\n",offer);
    return 0;
}

static int cmd_reply(sqlite3 *db, int send_id, const char *type, const char *notes) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,"SELECT id FROM sends WHERE id=?",-1,&st,NULL);
    sqlite3_bind_int(st,1,send_id);
    if (sqlite3_step(st)!=SQLITE_ROW){
        fprintf(stderr,"Send #%d not found.\n",send_id);
        sqlite3_finalize(st); return 1;
    }
    sqlite3_finalize(st);

    char ts[64]; iso_timestamp(ts,sizeof(ts));
    if (!notes) notes="";

    sqlite3_prepare_v2(db,
        "UPDATE sends SET reply_type=?,reply_notes=?,replied_at=? WHERE id=?",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,type,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,notes,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,ts,-1,SQLITE_STATIC);
    sqlite3_bind_int(st,4,send_id);
    sqlite3_step(st); sqlite3_finalize(st);

    printf("Send #%d: reply logged (%s)\n",send_id,type);
    return 0;
}

static int cmd_pending(sqlite3 *db) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT id,channel,target,offer_name,sent_at FROM sends WHERE reply_type IS NULL ORDER BY sent_at DESC",
        -1,&st,NULL);
    printf("%-5s %-10s %-25s %-36s %s\n","ID","Channel","Target","Offer","Age");
    printf("--------------------------------------------------------------------------------------------\n");
    int count=0;
    while (sqlite3_step(st)==SQLITE_ROW) {
        int id=sqlite3_column_int(st,0);
        const char *ch=(const char*)sqlite3_column_text(st,1);
        const char *tgt=(const char*)sqlite3_column_text(st,2);
        const char *offer=sqlite3_column_text(st,3)?(const char*)sqlite3_column_text(st,3):"-";
        const char *sent=(const char*)sqlite3_column_text(st,4);
        int days=age_days(sent);
        printf("%-5d %-10s %-25s %-36s %dd\n",id,ch,tgt,offer,days);
        count++;
    }
    sqlite3_finalize(st);
    if (!count) printf("No pending sends.\n");
    return 0;
}

static int cmd_followup(sqlite3 *db, int specific_id) {
    sqlite3_stmt *st;
    if (specific_id>0) {
        sqlite3_prepare_v2(db,
            "SELECT id,channel,target,offer_name,sent_at,reply_type FROM sends WHERE id=?",
            -1,&st,NULL);
        sqlite3_bind_int(st,1,specific_id);
    } else {
        sqlite3_prepare_v2(db,
            "SELECT id,channel,target,offer_name,sent_at,reply_type FROM sends WHERE reply_type IS NULL ORDER BY sent_at ASC",
            -1,&st,NULL);
    }

    if (specific_id>0) {
        if (sqlite3_step(st)!=SQLITE_ROW){
            fprintf(stderr,"Send #%d not found.\n",specific_id);
            sqlite3_finalize(st); return 1;
        }
        if (sqlite3_column_text(st,5)){
            printf("Send #%d already has reply: %s\n",specific_id,(const char*)sqlite3_column_text(st,5));
            sqlite3_finalize(st); return 0;
        }
        const char *tgt=(const char*)sqlite3_column_text(st,2);
        const char *offer=sqlite3_column_text(st,3)?(const char*)sqlite3_column_text(st,3):"-";
        const char *sent=(const char*)sqlite3_column_text(st,4);
        int days=age_days(sent);
        const char *move=followup_move(days);

        printf("## Follow-Up For Send #%d\n\n",specific_id);
        printf("- Target: %s\n",tgt);
        printf("- Offer: %s\n",offer);
        printf("- Age: %dd\n",days);
        printf("- Recommended move: %s\n\n",move);

        /* Generate copy */
        if (days<=1)
            printf("No follow-up yet. Give this send a little more time.\n");
        else if (days<=3)
            printf("Hi %s -- following up in case this got buried. "
                   "I put together this offer and can still run a quick sample if helpful.\n",tgt);
        else if (days<=7)
            printf("Hi %s -- one more nudge. I already have a proof-backed example, "
                   "and the current offer is ready. If you want, I can run one file "
                   "and let the output speak for itself.\n",tgt);
        else
            printf("Hi %s -- closing the loop for now. "
                   "If transcript + summary + action-item cleanup becomes useful later, "
                   "I can pick this back up quickly.\n",tgt);
    } else {
        printf("%-5s %-25s %-36s %-6s %s\n","ID","Target","Offer","Age","Next Move");
        printf("-------------------------------------------------------------------------------\n");
        int count=0;
        while (sqlite3_step(st)==SQLITE_ROW) {
            int id=sqlite3_column_int(st,0);
            const char *tgt=(const char*)sqlite3_column_text(st,2);
            const char *offer=sqlite3_column_text(st,3)?(const char*)sqlite3_column_text(st,3):"-";
            const char *sent=(const char*)sqlite3_column_text(st,4);
            int days=age_days(sent);
            printf("%-5d %-25s %-36s %-4dd  %s\n",id,tgt,offer,days,followup_move(days));
            count++;
        }
        if (!count) printf("No follow-ups needed.\n");
    }
    sqlite3_finalize(st);
    return 0;
}

static int cmd_report(sqlite3 *db) {
    /* Channel performance */
    printf("## Channel Performance\n\n");
    printf("%-12s %-6s %-6s %-6s %-8s %s\n","Channel","Sent","+","-","Silent","Rate");
    printf("------------------------------------------------\n");

    for (int i=0;i<NUM_CHANNELS;i++) {
        sqlite3_stmt *st;
        sqlite3_prepare_v2(db,
            "SELECT reply_type FROM sends WHERE channel=?",-1,&st,NULL);
        sqlite3_bind_text(st,1,CHANNELS[i].id,-1,SQLITE_STATIC);
        int sent=0,pos=0,neg=0,silent=0;
        while (sqlite3_step(st)==SQLITE_ROW) {
            sent++;
            const char *rt=sqlite3_column_text(st,0)?(const char*)sqlite3_column_text(st,0):NULL;
            if (!rt) silent++;
            else if (strcmp(rt,"positive")==0) pos++;
            else if (strcmp(rt,"negative")==0) neg++;
        }
        sqlite3_finalize(st);
        if (sent==0) continue;
        char rate[16];
        if (sent>0) snprintf(rate,sizeof(rate),"%d%%",pos*100/sent);
        else snprintf(rate,sizeof(rate),"-");
        printf("%-12s %-6d %-6d %-6d %-8d %s\n",CHANNELS[i].id,sent,pos,neg,silent,rate);
    }

    /* Recent sends */
    printf("\n## Recent Sends\n\n");
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT id,channel,target,offer_name,reply_type,sent_at FROM sends ORDER BY sent_at DESC LIMIT 10",
        -1,&st,NULL);
    while (sqlite3_step(st)==SQLITE_ROW) {
        int id=sqlite3_column_int(st,0);
        const char *ch=(const char*)sqlite3_column_text(st,1);
        const char *tgt=(const char*)sqlite3_column_text(st,2);
        const char *offer=sqlite3_column_text(st,3)?(const char*)sqlite3_column_text(st,3):"";
        const char *reply=sqlite3_column_text(st,4)?(const char*)sqlite3_column_text(st,4):"waiting";
        const char *sent=(const char*)sqlite3_column_text(st,5);
        char date[11]; snprintf(date,sizeof(date),"%.10s",sent?sent:"");
        printf("  #%-4d %-10s -> %-25s [%s]",id,ch,tgt,reply);
        if (offer[0]) printf(" [%s]",offer);
        printf("  %s\n",date);
    }
    sqlite3_finalize(st);

    /* Offer performance */
    printf("\n## Offer Performance\n\n");
    sqlite3_prepare_v2(db,
        "SELECT COALESCE(offer_name,'unlinked') as oname,"
        "COUNT(*) as total,"
        "SUM(CASE WHEN reply_type='positive' THEN 1 ELSE 0 END) as pos,"
        "SUM(CASE WHEN reply_type='negative' THEN 1 ELSE 0 END) as neg,"
        "SUM(CASE WHEN reply_type IS NULL THEN 1 ELSE 0 END) as waiting "
        "FROM sends GROUP BY oname ORDER BY total DESC",
        -1,&st,NULL);
    printf("%-42s %-6s %-6s %-6s %s\n","Offer","Sent","+","-","Waiting");
    printf("----------------------------------------------------------------------\n");
    while (sqlite3_step(st)==SQLITE_ROW) {
        const char *offer=(const char*)sqlite3_column_text(st,0);
        int total=sqlite3_column_int(st,1);
        int pos=sqlite3_column_int(st,2);
        int neg=sqlite3_column_int(st,3);
        int waiting=sqlite3_column_int(st,4);
        printf("%-42s %-6d %-6d %-6d %d\n",offer,total,pos,neg,waiting);
    }
    sqlite3_finalize(st);
    return 0;
}

static int cmd_status(sqlite3 *db) {
    sqlite3_stmt *st;
    int total=0, replied=0, positive=0, pending=0, followup_due=0;

    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM sends",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) total=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM sends WHERE reply_type IS NOT NULL",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) replied=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM sends WHERE reply_type='positive'",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) positive=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM sends WHERE reply_type IS NULL",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) pending=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    /* Count sends older than 2 days with no reply */
    sqlite3_prepare_v2(db,
        "SELECT sent_at FROM sends WHERE reply_type IS NULL",-1,&st,NULL);
    while (sqlite3_step(st)==SQLITE_ROW){
        const char *sent=(const char*)sqlite3_column_text(st,0);
        if (age_days(sent)>=2) followup_due++;
    }
    sqlite3_finalize(st);

    /* Best channel */
    char best_ch[MAX_NAME]="(none)";
    int best_rate=0;
    for (int i=0;i<NUM_CHANNELS;i++){
        sqlite3_prepare_v2(db,
            "SELECT COUNT(*),SUM(CASE WHEN reply_type='positive' THEN 1 ELSE 0 END) FROM sends WHERE channel=?",
            -1,&st,NULL);
        sqlite3_bind_text(st,1,CHANNELS[i].id,-1,SQLITE_STATIC);
        if (sqlite3_step(st)==SQLITE_ROW){
            int s=sqlite3_column_int(st,0);
            int p=sqlite3_column_int(st,1);
            if (s>0 && p*100/s>best_rate){
                best_rate=p*100/s;
                snprintf(best_ch,sizeof(best_ch),"%s",CHANNELS[i].id);
            }
        }
        sqlite3_finalize(st);
    }

    double resp_rate=total>0?(double)replied/total:0;
    double pos_rate=replied>0?(double)positive/replied:0;

    printf("BonfyreOutreach Status\n");
    printf("  Total sends:    %d\n",total);
    printf("  Replies:        %d | Response rate: %.0f%%\n",replied,resp_rate*100);
    printf("  Positive:       %d | Positive rate: %.0f%%\n",positive,pos_rate*100);
    printf("  Pending:        %d\n",pending);
    printf("  Follow-up due:  %d\n",followup_due);
    printf("  Best channel:   %s (%d%%)\n",best_ch,best_rate);
    return 0;
}

/* ── CLI routing ──────────────────────────────────────────────────────── */

static const char *arg_get(int argc, char **argv, const char *flag) {
    for (int i=0;i<argc-1;i++)
        if (strcmp(argv[i],flag)==0) return argv[i+1];
    return NULL;
}

static void usage(void) {
    fprintf(stderr,
        "BonfyreOutreach — quiet distribution engine\n\n"
        "Usage:\n"
        "  bonfyre-outreach [--db FILE] channels\n"
        "  bonfyre-outreach [--db FILE] templates --channel CH\n"
        "  bonfyre-outreach [--db FILE] send --channel CH --target WHO [--offer NAME] [--message TPL]\n"
        "  bonfyre-outreach [--db FILE] reply --id ID --type positive|negative|neutral [--notes N]\n"
        "  bonfyre-outreach [--db FILE] pending\n"
        "  bonfyre-outreach [--db FILE] followup [--id ID]\n"
        "  bonfyre-outreach [--db FILE] report\n"
        "  bonfyre-outreach [--db FILE] status\n");
}

int main(int argc, char **argv) {
    if (argc<2){ usage(); return 1; }

    const char *db_path=arg_get(argc,argv,"--db");
    if (!db_path) db_path=default_db();

    /* Strip --db and its arg */
    int cmd_argc=0;
    char *cmd_argv[128];
    for (int i=0;i<argc && cmd_argc<128;i++){
        if (strcmp(argv[i],"--db")==0){ i++; continue; }
        cmd_argv[cmd_argc++]=argv[i];
    }

    if (cmd_argc<2){ usage(); return 1; }
    const char *cmd=cmd_argv[1];

    /* channels and templates don't need DB */
    if (strcmp(cmd,"channels")==0) return cmd_channels();
    if (strcmp(cmd,"templates")==0) {
        const char *ch=arg_get(cmd_argc,cmd_argv,"--channel");
        if (!ch){ fprintf(stderr,"Missing --channel\n"); return 1; }
        return cmd_templates(ch);
    }

    sqlite3 *db=open_db(db_path);
    if (!db) return 1;
    int rc=0;

    if (strcmp(cmd,"send")==0) {
        const char *ch=arg_get(cmd_argc,cmd_argv,"--channel");
        const char *tgt=arg_get(cmd_argc,cmd_argv,"--target");
        const char *offer=arg_get(cmd_argc,cmd_argv,"--offer");
        const char *msg=arg_get(cmd_argc,cmd_argv,"--message");
        if (!ch||!tgt){ fprintf(stderr,"Missing --channel, --target\n"); rc=1; goto done; }
        rc=cmd_send(db,ch,tgt,offer,msg);
    } else if (strcmp(cmd,"reply")==0) {
        const char *id_s=arg_get(cmd_argc,cmd_argv,"--id");
        const char *type=arg_get(cmd_argc,cmd_argv,"--type");
        const char *notes=arg_get(cmd_argc,cmd_argv,"--notes");
        if (!id_s||!type){ fprintf(stderr,"Missing --id, --type\n"); rc=1; goto done; }
        rc=cmd_reply(db,atoi(id_s),type,notes);
    } else if (strcmp(cmd,"pending")==0) {
        rc=cmd_pending(db);
    } else if (strcmp(cmd,"followup")==0) {
        const char *id_s=arg_get(cmd_argc,cmd_argv,"--id");
        rc=cmd_followup(db,id_s?atoi(id_s):0);
    } else if (strcmp(cmd,"report")==0) {
        rc=cmd_report(db);
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
