/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

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
#include "bonfyre.h"
#include "../../../lib/libbonfyre/include/bf_help.h"

#define MAX_PATH  2048
#define MAX_NAME  256
#define MAX_MSG   4096

/* ── Channels ─────────────────────────────────────────────────────────── */

typedef struct {
    const char *id;
    const char *desc;
} Channel;

static const Channel CHANNELS[] = {
    {"dm",      "Direct messages (social/in-app)"},
    {"email",   "Email correspondence (source-bound threads)"},
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
     "Hi [NAME], I build and maintain local-first document and business workflow tools. "
     "If there is a specific task your team needs to complete, I can share relevant completed work "
     "and discuss the scope before proposing terms."},
    {"dm", "operator-cold",
     "Hi [NAME], I work on reliable processing and handoff for documents, recordings and "
     "operational records. What output and deadline would be useful for your team? "
     "I can review a real example and confirm feasibility before quoting."},
    {"listing", "service-listing",
     "INTERNAL DRAFT ONLY. Scope, pricing, turnaround, capacity and review criteria "
     "must be based on the actual buyer request and confirmed supplier capability. "
     "An earlier proof score or internal benchmark does not establish sale terms."},
    {"post", "value-post",
     "Aurekai has working examples of source-backed document, catalogue and workflow "
     "processing. Product previews are available for evaluation; scope, permissions "
     "and commercial terms depend on the actual project."},
};
#define NUM_TEMPLATES (int)(sizeof(TEMPLATES)/sizeof(TEMPLATES[0]))

/* ── Utility ──────────────────────────────────────────────────────────── */

static void iso_timestamp(char *buf, size_t sz) { bf_iso_timestamp(buf, sz); }

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
    "  replied_at TEXT,"
    "  source_message_id TEXT,"
    "  source_sent_at TEXT,"
    "  reply_message_id TEXT"
    ");";

/* Source evidence stays separate from offer identity; add nullable columns
 * without changing pre-existing sends or their recorded timestamps. */
static int ensure_evidence_columns(sqlite3 *db) {
    const char *names[]={"source_message_id","source_sent_at","reply_message_id"};
    for(size_t i=0;i<3;i++) {
        sqlite3_stmt *st=NULL;
        if(sqlite3_prepare_v2(db,"PRAGMA table_info(sends)",-1,&st,NULL)!=SQLITE_OK)return 1;
        int found=0;
        while(sqlite3_step(st)==SQLITE_ROW) {
            const char *column=(const char*)sqlite3_column_text(st,1);
            if(column && strcmp(column,names[i])==0) found=1;
        }
        sqlite3_finalize(st);
        if(!found) {
            char sql[128];
            snprintf(sql,sizeof(sql),"ALTER TABLE sends ADD COLUMN %s TEXT",names[i]);
            if(sqlite3_exec(db,sql,NULL,NULL,NULL)!=SQLITE_OK)return 1;
        }
    }
    return sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS outreach_responses ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "send_id INTEGER NOT NULL REFERENCES sends(id),"
        "source_message_id TEXT NOT NULL UNIQUE,"
        "received_at TEXT NOT NULL,"
        "classification TEXT NOT NULL,"
        "notes TEXT);"
        "CREATE INDEX IF NOT EXISTS ix_outreach_responses_send "
        "ON outreach_responses(send_id,received_at);"
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_outreach_source ON sends(source_message_id) "
        "WHERE source_message_id IS NOT NULL;"
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_outreach_reply ON sends(reply_message_id) "
        "WHERE reply_message_id IS NOT NULL;",NULL,NULL,NULL)!=SQLITE_OK;
}
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
    if (ensure_evidence_columns(db)!=0) {
        fprintf(stderr,"Outreach evidence migration refused: %s\n",sqlite3_errmsg(db));
        sqlite3_close(db); return NULL;
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
    printf("Recorded outbound entry #%d locally: %s -> %s (delivery NOT verified by this command)\n",send_id,channel,target);
    if (offer) printf("  Offer: %s\n",offer);
    return 0;
}

/* Record already-sent correspondence with its original source and timestamp.
 * This operation never dispatches a message. */
static int cmd_backfill(sqlite3 *db,const char *channel,const char *target,
                        const char *offer,const char *source,const char *sent_at) {
    if(!channel || !*channel || !target || !*target || !source ||
       strncmp(source,"gmail:",6)!=0 || strlen(source)<8 || !sent_at ||
       strlen(sent_at)<19) {
        fprintf(stderr,"backfill requires --channel --target --source gmail:ID --sent-at ISO\n");
        return 2;
    }
    sqlite3_stmt *st=NULL;
    if(sqlite3_prepare_v2(db,
      "SELECT id,source_sent_at FROM sends WHERE source_message_id=? OR "
      "substr(COALESCE(offer_name,''),-length(?))=? LIMIT 1",
      -1,&st,NULL)!=SQLITE_OK)return 1;
    sqlite3_bind_text(st,1,source,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,source,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,source,-1,SQLITE_STATIC);
    if(sqlite3_step(st)==SQLITE_ROW) {
        int id=sqlite3_column_int(st,0);
        const char *prior=(const char*)sqlite3_column_text(st,1);
        int conflict=prior && strcmp(prior,sent_at)!=0;
        sqlite3_finalize(st);
        if(conflict) {
            fprintf(stderr,"backfill: source %s has a different original timestamp; refusing overwrite\n",source);
            return 2;
        }
        if(sqlite3_prepare_v2(db,
          "UPDATE sends SET source_message_id=?,source_sent_at=? WHERE id=? "
          "AND (source_message_id IS NULL OR source_message_id=?)",
          -1,&st,NULL)!=SQLITE_OK)return 1;
        sqlite3_bind_text(st,1,source,-1,SQLITE_STATIC);
        sqlite3_bind_text(st,2,sent_at,-1,SQLITE_STATIC);
        sqlite3_bind_int(st,3,id);
        sqlite3_bind_text(st,4,source,-1,SQLITE_STATIC);
        int ok=sqlite3_step(st)==SQLITE_DONE;
        sqlite3_finalize(st);
        if(!ok || sqlite3_changes(db)!=1)return 1;
        printf("Already linked source %s as #%d; original timestamp retained; not resent\n",source,id);
        return 0;
    }
    sqlite3_finalize(st);
    if(sqlite3_prepare_v2(db,
      "INSERT INTO sends(channel,target,offer_name,sent_at,source_message_id,source_sent_at) "
      "VALUES(?,?,?,?,?,?)",-1,&st,NULL)!=SQLITE_OK)return 1;
    sqlite3_bind_text(st,1,channel,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,target,-1,SQLITE_STATIC);
    if(offer && *offer)sqlite3_bind_text(st,3,offer,-1,SQLITE_STATIC);
    else sqlite3_bind_null(st,3);
    sqlite3_bind_text(st,4,sent_at,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,5,source,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,6,sent_at,-1,SQLITE_STATIC);
    int ok=sqlite3_step(st)==SQLITE_DONE;
    sqlite3_finalize(st);
    if(!ok) { fprintf(stderr,"backfill: %s\n",sqlite3_errmsg(db));return 1; }
    printf("Backfilled source %s as #%lld (NO dispatch)\n",source,
           (long long)sqlite3_last_insert_rowid(db));
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

/* Reconcile a real external reply; do not use the local observation time,
 * replace an earlier classification, or count automated replies as humans. */
/* Every counterparty message is its own source-linked fact. Earlier requests
 * remain visible after a later decline; sends.reply_type is the current view. */
static int cmd_reply_evidence(sqlite3 *db,int id,const char *type,
                              const char *source,const char *received_at,
                              const char *notes) {
    if(id<1 || !source || strncmp(source,"gmail:",6)!=0 || strlen(source)<8 ||
       !received_at || strlen(received_at)<19 || !type ||
       (strcmp(type,"positive") && strcmp(type,"negative") &&
        strcmp(type,"neutral") && strcmp(type,"automated"))) {
        fprintf(stderr,"reply-evidence: --id --type positive|negative|neutral|automated --source gmail:ID --at ISO required\n");
        return 2;
    }
    sqlite3_stmt *st=NULL;
    int result=1;
    if(sqlite3_exec(db,"BEGIN IMMEDIATE",NULL,NULL,NULL)!=SQLITE_OK)return 1;
    if(sqlite3_prepare_v2(db,
       "SELECT send_id,classification FROM outreach_responses WHERE source_message_id=?",
       -1,&st,NULL)!=SQLITE_OK)goto fail;
    sqlite3_bind_text(st,1,source,-1,SQLITE_STATIC);
    if(sqlite3_step(st)==SQLITE_ROW) {
        int same=sqlite3_column_int(st,0)==id;
        const char *prior=(const char*)sqlite3_column_text(st,1);
        same=same && prior && strcmp(prior,type)==0;
        sqlite3_finalize(st);st=NULL;
        if(same) {
            sqlite3_exec(db,"COMMIT",NULL,NULL,NULL);
            printf("Reply %s already linked; no change\n",source);
            return 0;
        }
        fprintf(stderr,"reply-evidence: source %s belongs to another classification or outreach\n",source);
        result=2;goto fail;
    }
    sqlite3_finalize(st);st=NULL;
    if(sqlite3_prepare_v2(db,
       "SELECT reply_type,reply_message_id,replied_at FROM sends WHERE id=?",
       -1,&st,NULL)!=SQLITE_OK)goto fail;
    sqlite3_bind_int(st,1,id);
    if(sqlite3_step(st)!=SQLITE_ROW) {
        fprintf(stderr,"reply-evidence: outreach #%d not found\n",id);
        result=2;goto fail;
    }
    const char *prior_type=(const char*)sqlite3_column_text(st,0);
    const char *prior_ref=(const char*)sqlite3_column_text(st,1);
    const char *prior_at=(const char*)sqlite3_column_text(st,2);
    int unresolved_legacy=prior_type && !prior_ref;
    int newer=!prior_at || strcmp(received_at,prior_at)>=0;
    sqlite3_finalize(st);st=NULL;
    if(unresolved_legacy) {
        fprintf(stderr,"reply-evidence: legacy manual response on #%d needs source reconciliation\n",id);
        result=2;goto fail;
    }
    if(sqlite3_prepare_v2(db,
       "INSERT INTO outreach_responses(send_id,source_message_id,received_at,classification,notes) "
       "VALUES(?,?,?,?,?)",-1,&st,NULL)!=SQLITE_OK)goto fail;
    sqlite3_bind_int(st,1,id);
    sqlite3_bind_text(st,2,source,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,received_at,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,4,type,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,5,notes?notes:"",-1,SQLITE_STATIC);
    if(sqlite3_step(st)!=SQLITE_DONE)goto fail;
    sqlite3_finalize(st);st=NULL;
    if(newer) {
        if(sqlite3_prepare_v2(db,
           "UPDATE sends SET reply_type=?,reply_message_id=?,replied_at=?,reply_notes=? "
           "WHERE id=?",-1,&st,NULL)!=SQLITE_OK)goto fail;
        sqlite3_bind_text(st,1,type,-1,SQLITE_STATIC);
        sqlite3_bind_text(st,2,source,-1,SQLITE_STATIC);
        sqlite3_bind_text(st,3,received_at,-1,SQLITE_STATIC);
        sqlite3_bind_text(st,4,notes?notes:"",-1,SQLITE_STATIC);
        sqlite3_bind_int(st,5,id);
        if(sqlite3_step(st)!=SQLITE_DONE || sqlite3_changes(db)!=1)goto fail;
        sqlite3_finalize(st);st=NULL;
    }
    if(sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK)goto fail;
    printf("Outreach #%d observed %s from %s; current=%s; not a sale\n",
           id,type,source,newer?"updated":"newer response retained");
    return 0;
fail:
    sqlite3_finalize(st);
    sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);
    return result;
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

        printf("## Follow-Up Review For Outbound Record #%d (not a dispatch)\n\n",specific_id);
        printf("- Target: %s\n",tgt);
        printf("- Offer: %s\n",offer);
        printf("- Age: %dd\n",days);
        printf("- Recommended move: %s\n\n",move);

        /* Generate copy */
        if (days<=1)
            printf("No follow-up yet. Give this send a little more time.\n");
        else if (days<=3)
            printf("REVIEW ONLY: %s. Check the real recipient, previous delivery, and currently approved scope before any follow-up.\n",tgt);
        else if (days<=7)
            printf("REVIEW ONLY: %s. Evidence or an offer may have changed; verify the thread before drafting another request.\n",tgt);
        else
            printf("REVIEW ONLY: %s. Preserve the contact record and current response status; do not assume repeated outreach is welcome.\n",tgt);
    } else {
        printf("%-5s %-25s %-36s %-6s %s\n","ID","Target","Offer","Age","Review Condition");
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
    printf("## Recorded Outreach (this store does not verify external delivery)\n\n");
    printf("%-12s %-6s %-6s %-6s %-8s %s\n","Channel","Rows","+","-","Unknown","Rate");
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
    printf("\n## Recent Recorded Outbound Entries\n\n");
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
    printf("\n## Linked Offer Records (not verified sales)\n\n");
    sqlite3_prepare_v2(db,
        "SELECT COALESCE(offer_name,'unlinked') as oname,"
        "COUNT(*) as total,"
        "SUM(CASE WHEN reply_type='positive' THEN 1 ELSE 0 END) as pos,"
        "SUM(CASE WHEN reply_type='negative' THEN 1 ELSE 0 END) as neg,"
        "SUM(CASE WHEN reply_type IS NULL THEN 1 ELSE 0 END) as waiting "
        "FROM sends GROUP BY oname ORDER BY total DESC",
        -1,&st,NULL);
    printf("%-42s %-6s %-6s %-6s %s\n","Offer","Rows","+","-","Unknown");
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
    int total=0, replied=0, positive=0, pending=0, automated=0, followup_due=0;

    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM sends",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) total=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM sends WHERE reply_type IN ('positive','negative','neutral')",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) replied=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM sends WHERE reply_type='positive'",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) positive=sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM sends WHERE reply_type='automated'",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) automated=sqlite3_column_int(st,0);
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
    printf("  Logged entries (external delivery unverified): %d\n",total);
    printf("  Classified responses: %d | Share of records: %.0f%%\n",replied,resp_rate*100);
    printf("  Recorded positive: %d | Share of classified: %.0f%%\n",positive,pos_rate*100);
    printf("  Automated acknowledgments (not human responses): %d\n",automated);
    printf("  Unclassified: %d\n",pending);
    printf("  Follow-up candidates (verify externally): %d\n",followup_due);
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
        "  bonfyre-outreach [--db FILE] reply-evidence --id ID --type positive|negative|neutral|automated --source gmail:ID --at ISO [--notes N]\n"
        "  bonfyre-outreach [--db FILE] backfill --channel CH --target WHO --source gmail:ID --sent-at ISO [--offer KEY]\n"
        "  bonfyre-outreach [--db FILE] pending\n"
        "  bonfyre-outreach [--db FILE] followup [--id ID]\n"
        "  bonfyre-outreach [--db FILE] report\n"
        "  bonfyre-outreach [--db FILE] status\n");
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
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
    } else if (strcmp(cmd,"reply-evidence")==0) {
        const char *id=arg_get(cmd_argc,cmd_argv,"--id");
        rc=cmd_reply_evidence(db,id?atoi(id):0,
            arg_get(cmd_argc,cmd_argv,"--type"),
            arg_get(cmd_argc,cmd_argv,"--source"),
            arg_get(cmd_argc,cmd_argv,"--at"),
            arg_get(cmd_argc,cmd_argv,"--notes"));
    } else if (strcmp(cmd,"backfill")==0) {
        rc=cmd_backfill(db,arg_get(cmd_argc,cmd_argv,"--channel"),
            arg_get(cmd_argc,cmd_argv,"--target"),arg_get(cmd_argc,cmd_argv,"--offer"),
            arg_get(cmd_argc,cmd_argv,"--source"),arg_get(cmd_argc,cmd_argv,"--sent-at"));
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
