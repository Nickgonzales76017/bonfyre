#ifndef BONFYRE_QUEUE_ACCOUNT_WORK_H
#define BONFYRE_QUEUE_ACCOUNT_WORK_H

/* The account's work as it is actually recorded: operator goals and runs, fabric
 * tasks and their agent runs, and workgraph missions. Each owner store is read
 * through a read-only ATTACH and copied into one derived index, rebuilt whole,
 * then served in the exact task packet the activity rail already verifies.
 * Fleming's queue store belongs to one client and is never read here. The
 * index is a cache: it decides nothing, dispatches nothing and owns no work. */
#include "account_activity.h"
#include <limits.h>

#define AWK_STALE_MS 60000
#define AWK_SOURCE_LIMIT 16
#define AWK_CREDIT_ATTENTION "OpenAI API credits are exhausted. Add credits before retrying this work."

typedef struct { const char *kind, *path; } AwkSource;

static const char *const awk_schema =
    "CREATE TABLE IF NOT EXISTS account_work(seq INTEGER PRIMARY KEY,id TEXT NOT NULL UNIQUE,subject TEXT NOT NULL,"
    "scope TEXT NOT NULL,source TEXT NOT NULL,title TEXT NOT NULL,state TEXT NOT NULL,created_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL,attention TEXT NOT NULL DEFAULT '');"
    "CREATE INDEX IF NOT EXISTS account_work_page ON account_work(subject,scope,seq DESC);"
    "CREATE TABLE IF NOT EXISTS account_work_run(work_id TEXT NOT NULL,run_id TEXT NOT NULL,worker_id TEXT NOT NULL,"
    "state TEXT NOT NULL,created_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL,agent TEXT NOT NULL DEFAULT '',role TEXT NOT NULL DEFAULT '',"
    "session_kind TEXT NOT NULL DEFAULT '',duration_ms INTEGER,limit_signal INTEGER NOT NULL DEFAULT 0,provider_session_ref TEXT NOT NULL DEFAULT '',"
    "handoff_ref TEXT NOT NULL DEFAULT '');"
    "CREATE INDEX IF NOT EXISTS account_work_run_work ON account_work_run(work_id,updated_ms DESC);"
    "CREATE TABLE IF NOT EXISTS account_work_usage(work_id TEXT NOT NULL,run_id TEXT NOT NULL,"
    "input_tokens INTEGER,output_tokens INTEGER,estimated_micro_usd INTEGER,cache_read_tokens INTEGER,cache_write_tokens INTEGER,PRIMARY KEY(work_id,run_id));"
    "CREATE INDEX IF NOT EXISTS account_work_usage_work ON account_work_usage(work_id);"
    "CREATE TABLE IF NOT EXISTS account_work_estimate(work_id TEXT NOT NULL,run_id TEXT NOT NULL,input_tokens INTEGER NOT NULL,output_tokens INTEGER NOT NULL,estimated_micro_usd INTEGER NOT NULL,source_digest TEXT NOT NULL,PRIMARY KEY(work_id,run_id));"
    "CREATE TABLE IF NOT EXISTS account_work_step(work_id TEXT NOT NULL,node_id TEXT NOT NULL,operator_id TEXT NOT NULL,"
    "state TEXT NOT NULL,attempt INTEGER NOT NULL,updated_ms INTEGER NOT NULL,output_ref TEXT NOT NULL DEFAULT '');"
    "CREATE INDEX IF NOT EXISTS account_work_step_work ON account_work_step(work_id,node_id);"
    "CREATE TABLE IF NOT EXISTS account_work_meta(key TEXT PRIMARY KEY,value INTEGER NOT NULL);";

/* Each owner's vocabulary mapped onto the workgraph node and worker states the
 * rail knows. Blocked operator goals stay blocked; their failed runs are what
 * raise attention. */
/* Wall time only where the owner recorded both ends; an open run has none. */
#define AWK_WALL "CASE WHEN completed_at IS NOT NULL AND completed_at>=started_at THEN CAST(round((completed_at-started_at)*1000) AS INTEGER) END"
#define AWK_WALL_R "CASE WHEN r.completed_at IS NOT NULL AND r.completed_at>=r.started_at THEN CAST(round((r.completed_at-r.started_at)*1000) AS INTEGER) END"
static const char *const awk_operator =
    "INSERT INTO temp.w SELECT 'account-operator-'||id,'Aurekai assistant',title,"
    "CASE state WHEN 'resolved' THEN 'complete' WHEN 'superseded' THEN 'cancelled' WHEN 'retry' THEN 'retry_wait' "
    "WHEN 'blocked' THEN 'blocked' WHEN 'running' THEN 'running' ELSE 'ready' END,"
    "CAST(created_at*1000 AS INTEGER),CAST(updated_at*1000 AS INTEGER),'' FROM src.goals;"
    "DELETE FROM temp.w WHERE source='Aurekai assistant' AND id IN("
    "SELECT 'account-operator-'||g.id FROM src.goals g WHERE g.state='superseded' "
    "AND NOT EXISTS(SELECT 1 FROM src.runs r WHERE r.goal_id=g.id));"
    "INSERT INTO temp.r(work_id,run_id,worker_id,state,created_ms,updated_ms,agent,session_kind,duration_ms) SELECT 'account-operator-'||goal_id,id,'Aurekai assistant',"
    "CASE state WHEN 'resolved' THEN 'completed' WHEN 'failed' THEN 'failed' WHEN 'running' THEN 'running' ELSE 'waiting' END,"
    "CAST(started_at*1000 AS INTEGER),CAST(COALESCE(completed_at,started_at)*1000 AS INTEGER),'Aurekai assistant','assistant'," AWK_WALL " FROM src.runs;"
    "INSERT INTO temp.s(work_id,node_id,operator_id,state,attempt,updated_ms) SELECT 'account-operator-'||r.goal_id,'action-'||printf('%012d',a.seq),a.tool,"
    "CASE a.state WHEN 'passed' THEN 'complete' WHEN 'completed' THEN 'complete' WHEN 'reverted' THEN 'cancelled' "
    "WHEN 'failed' THEN 'dead_letter' WHEN 'outcome_uncertain' THEN 'cancel_requested' WHEN 'refused' THEN 'blocked' "
    "WHEN 'started' THEN 'running' ELSE 'blocked' END,1,"
    "CAST(a.started_at*1000 AS INTEGER)+CASE WHEN a.duration_ms>0 THEN a.duration_ms ELSE 0 END FROM src.actions a JOIN src.runs r ON r.id=a.run_id;";
static const char *const awk_operator_context =
    "INSERT INTO temp.w SELECT 'account-operator-'||id,'Aurekai assistant',title,"
    "CASE state WHEN 'resolved' THEN 'complete' WHEN 'superseded' THEN 'cancelled' WHEN 'retry' THEN 'retry_wait' "
    "WHEN 'blocked' THEN 'blocked' WHEN 'running' THEN 'running' ELSE 'ready' END,"
    "CAST(created_at*1000 AS INTEGER),CAST(updated_at*1000 AS INTEGER),'' FROM src.goals;"
    "DELETE FROM temp.w WHERE source='Aurekai assistant' AND id IN("
    "SELECT 'account-operator-'||g.id FROM src.goals g WHERE g.state='superseded' "
    "AND NOT EXISTS(SELECT 1 FROM src.runs r WHERE r.goal_id=g.id));"
    "INSERT INTO temp.r(work_id,run_id,worker_id,state,created_ms,updated_ms,agent,role,session_kind,duration_ms) SELECT 'account-operator-'||r.goal_id,r.id,"
    "COALESCE(NULLIF(TRIM(c.agent||CASE WHEN TRIM(c.role)='' THEN '' ELSE ' · '||c.role END),''),'Aurekai assistant'),"
    "CASE r.state WHEN 'resolved' THEN 'completed' WHEN 'failed' THEN 'failed' WHEN 'running' THEN 'running' ELSE 'waiting' END,"
    "CAST(r.started_at*1000 AS INTEGER),CAST(COALESCE(r.completed_at,r.started_at)*1000 AS INTEGER),"
    "COALESCE(NULLIF(TRIM(c.agent),''),'Aurekai assistant'),COALESCE(TRIM(c.role),''),'assistant'," AWK_WALL_R " FROM src.runs r LEFT JOIN src.operator_run_context c ON c.run_id=r.id;"
    "INSERT INTO temp.s(work_id,node_id,operator_id,state,attempt,updated_ms) SELECT 'account-operator-'||r.goal_id,'action-'||printf('%012d',a.seq),a.tool,"
    "CASE a.state WHEN 'passed' THEN 'complete' WHEN 'completed' THEN 'complete' WHEN 'reverted' THEN 'cancelled' "
    "WHEN 'failed' THEN 'dead_letter' WHEN 'outcome_uncertain' THEN 'cancel_requested' WHEN 'refused' THEN 'blocked' "
    "WHEN 'started' THEN 'running' ELSE 'blocked' END,1,"
    "CAST(a.started_at*1000 AS INTEGER)+CASE WHEN a.duration_ms>0 THEN a.duration_ms ELSE 0 END FROM src.actions a JOIN src.runs r ON r.id=a.run_id;";

static const char *const awk_fabric =
    "INSERT INTO temp.w SELECT 'account-fabric-'||id,'fabric',title,"
    "CASE state WHEN 'completed' THEN 'complete' WHEN 'failed' THEN 'dead_letter' WHEN 'cancelled' THEN 'cancelled' "
    "WHEN 'running' THEN 'running' ELSE 'ready' END,"
    "CAST(created_at*1000 AS INTEGER),CAST(COALESCE(completed_at,started_at,created_at)*1000 AS INTEGER),'' FROM src.tasks;"
    "INSERT INTO temp.r(work_id,run_id,worker_id,state,created_ms,updated_ms,agent,role,duration_ms) SELECT 'account-fabric-'||task_id,id,agent||' '||role,"
    "CASE state WHEN 'passed' THEN 'completed' WHEN 'failed' THEN 'failed' WHEN 'running' THEN 'running' ELSE 'waiting' END,"
    "CAST(started_at*1000 AS INTEGER),CAST(COALESCE(completed_at,started_at)*1000 AS INTEGER),agent,role," AWK_WALL " FROM src.runs WHERE state<>'cancelled';";
/* Optional Fabric columns refine the run without becoming required: the task
 * kind names the session (duel, relay, review, verify), duration_ms is the
 * runner's own wall time, and a short failed final that says a provider limit
 * was hit raises the one fixed limit flag. The final text itself never leaves. */
static const char *const awk_fabric_kind =
    "UPDATE temp.r SET session_kind=COALESCE((SELECT t.kind FROM src.tasks t WHERE t.id=substr(temp.r.work_id,16)),'') WHERE work_id GLOB 'account-fabric-*';";
static const char *const awk_fabric_duration =
    "UPDATE temp.r SET duration_ms=COALESCE((SELECT x.duration_ms FROM src.runs x WHERE x.id=temp.r.run_id AND x.task_id=substr(temp.r.work_id,16) "
    "AND typeof(x.duration_ms)='integer' AND x.duration_ms>=0),duration_ms) WHERE work_id GLOB 'account-fabric-*';";
/* An assistant run the provider refused for credit or quota carries the same
 * limit flag, so the Inbox can say the assistant is stopped and why. Only the
 * provider's own refusal words count, read from the run's short summary. */
static const char *const awk_operator_limit =
    "UPDATE temp.r SET limit_signal=1 WHERE work_id GLOB 'account-operator-*' AND state='failed' AND EXISTS(SELECT 1 FROM src.runs x WHERE x.id=temp.r.run_id "
    "AND x.goal_id=substr(temp.r.work_id,18) AND length(x.summary)<800 AND (instr(x.summary,'insufficient_quota')>0 OR "
    "instr(x.summary,'credit_balance_exhausted')>0 OR instr(lower(x.summary),'no credits remaining')>0));";
static const char *const awk_fabric_limit =
    "UPDATE temp.r SET limit_signal=1 WHERE work_id GLOB 'account-fabric-*' AND state='failed' AND EXISTS(SELECT 1 FROM src.runs x WHERE x.id=temp.r.run_id "
    "AND x.task_id=substr(temp.r.work_id,16) AND json_valid(x.result_json) AND length(json_extract(x.result_json,'$.final'))<400 AND ("
    "instr(lower(json_extract(x.result_json,'$.final')),'session limit')>0 OR instr(lower(json_extract(x.result_json,'$.final')),'usage limit')>0 OR "
    "instr(lower(json_extract(x.result_json,'$.final')),'rate limit')>0 OR instr(lower(json_extract(x.result_json,'$.final')),'hit your limit')>0 OR "
    "instr(lower(json_extract(x.result_json,'$.final')),'insufficient_quota')>0 OR instr(lower(json_extract(x.result_json,'$.final')),'credit balance')>0));";
/* The provider session the Fabric runner captured at launch (Claude session_id,
 * Codex thread_id). It is a join key only: Meter attributes observed token
 * receipts to a task through this exact identifier, never by time or name. */
static const char *const awk_fabric_session =
    "UPDATE temp.r SET provider_session_ref=COALESCE((SELECT x.session_id FROM src.runs x WHERE x.id=temp.r.run_id AND x.task_id=substr(temp.r.work_id,16) "
    "AND typeof(x.session_id)='text' AND length(x.session_id) BETWEEN 8 AND 64 AND x.session_id NOT GLOB '*[^0-9A-Za-z-]*'),'') WHERE work_id GLOB 'account-fabric-*';";
static const char *const awk_workgraph =
    "INSERT INTO temp.w SELECT 'account-workgraph-'||m.id,'workgraph',"
    "COALESCE((SELECT operator_id FROM src.workgraph_nodes n WHERE n.mission_id=m.id ORDER BY node_id LIMIT 1),m.id),"
    "CASE m.status WHEN 'complete' THEN 'complete' WHEN 'completed' THEN 'complete' WHEN 'failed' THEN 'dead_letter' "
    "WHEN 'cancelled' THEN 'cancelled' WHEN 'running' THEN 'running' ELSE 'ready' END,"
    "CAST(strftime('%s',m.created_at) AS INTEGER)*1000,CAST(strftime('%s',m.updated_at) AS INTEGER)*1000,'' FROM src.missions m;"
    "INSERT INTO temp.s(work_id,node_id,operator_id,state,attempt,updated_ms,output_ref) SELECT 'account-workgraph-'||mission_id,node_id,operator_id,status,attempt,"
    "COALESCE(last_heartbeat_at_ms,claim_acquired_at_ms,0),COALESCE(output_uri,'') FROM src.workgraph_nodes "
    "WHERE (node_id<>'job' OR (status='complete' AND COALESCE(output_uri,'')<>'')) AND status IN('defined','blocked','ready','retry_wait','running','complete','dead_letter','cancel_requested','cancelled');";

/* A harness session that ran a claimed WorkGraph attempt names itself with
 * `run-attest`, inside the node's chained transition receipts. Each attested
 * attempt becomes one run of the mission's Work: the agent and provider session
 * are Meter's exact join key, and the handoff is the context-verify receipt the
 * worker declared at that attempt. Outcome is the attempt's own; nothing is
 * matched by time or title, and unattested attempts stay steps, not runs. */
static const char *const awk_workgraph_runs =
    "INSERT INTO temp.r(work_id,run_id,worker_id,state,created_ms,updated_ms,agent,role,session_kind,duration_ms,provider_session_ref,handoff_ref) "
    "SELECT 'account-workgraph-'||t.mission_id,t.node_id||'#'||t.attempt,t.actor,"
    "CASE WHEN a.outcome IS NULL THEN 'running' WHEN a.outcome='complete' THEN 'completed' ELSE 'failed' END,"
    "COALESCE(a.started_at_ms,t.created_at_ms),COALESCE(a.finished_at_ms,t.created_at_ms),"
    "json_extract(r.payload,'$.agent'),'','workgraph',"
    "CASE WHEN a.finished_at_ms>=a.started_at_ms THEN a.finished_at_ms-a.started_at_ms END,"
    "json_extract(r.payload,'$.provider_session_ref'),COALESCE(json_extract(r.payload,'$.handoff_receipt'),'') "
    "FROM src.workgraph_transitions t JOIN src.receipts r ON r.id=t.receipt_id "
    "LEFT JOIN src.workgraph_attempts a ON a.mission_id=t.mission_id AND a.node_id=t.node_id AND a.attempt=t.attempt "
    "WHERE r.subject_kind='workgraph-transition' AND json_valid(r.payload) AND json_extract(r.payload,'$.transition')='run_attested' "
    "AND json_extract(r.payload,'$.agent') IN('claude','codex') "
    "AND length(json_extract(r.payload,'$.provider_session_ref')) BETWEEN 8 AND 64 "
    "AND json_extract(r.payload,'$.provider_session_ref') NOT GLOB '*[^0-9A-Za-z-]*' "
    "AND (COALESCE(json_extract(r.payload,'$.handoff_receipt'),'')='' OR (length(json_extract(r.payload,'$.handoff_receipt'))=71 "
    "AND json_extract(r.payload,'$.handoff_receipt') GLOB 'sha256:*' AND substr(json_extract(r.payload,'$.handoff_receipt'),8) NOT GLOB '*[^0-9a-f]*'));";

/* Only source-reported integer tokens are projected. Absent usage stays unknown. */
static const char *const awk_operator_usage =
    "INSERT INTO temp.u(work_id,run_id,input_tokens,output_tokens,estimated_micro_usd,cache_read_tokens) SELECT 'account-operator-'||goal_id,id,"
    "CAST(json_extract(usage_json,'$.input_tokens') AS INTEGER),"
    "CAST(json_extract(usage_json,'$.output_tokens') AS INTEGER),NULL,"
    "CASE WHEN json_type(usage_json,'$.input_tokens_details.cached_tokens')='integer' AND json_extract(usage_json,'$.input_tokens_details.cached_tokens')>=0 "
    "THEN json_extract(usage_json,'$.input_tokens_details.cached_tokens') END FROM src.runs "
    "WHERE json_valid(usage_json) AND CASE WHEN json_valid(usage_json) THEN "
    "json_type(usage_json,'$.input_tokens')='integer' AND "
    "json_type(usage_json,'$.output_tokens')='integer' AND "
    "json_extract(usage_json,'$.input_tokens')>=0 AND "
    "json_extract(usage_json,'$.output_tokens')>=0 ELSE 0 END;";

static int awk_src_column(sqlite3 *db,const char *table,const char *column){
    sqlite3_stmt *q=NULL;int found=0;
    char *sql=sqlite3_mprintf("PRAGMA src.table_info(%Q)",table);
    if(!sql)return 0;
    if(sqlite3_prepare_v2(db,sql,-1,&q,NULL)!=SQLITE_OK){sqlite3_free(sql);return 0;}
    sqlite3_free(sql);
    while(sqlite3_step(q)==SQLITE_ROW){
        const char *n=(const char *)sqlite3_column_text(q,1);
        if(n&&!strcmp(n,column)){found=1;break;}
    }
    sqlite3_finalize(q);return found;
}
static int awk_src_usage_column(sqlite3 *db){return awk_src_column(db,"runs","usage_json");}
static int awk_src_last_error_column(sqlite3 *db){return awk_src_column(db,"goals","last_error");}

/* Provider diagnostics are owner-only. The account projection recognizes one
 * stable recovery state and substitutes a fixed public next step. */
static const char *const awk_operator_attention =
    "UPDATE temp.w SET attention=(SELECT CASE WHEN g.state IN('retry','blocked') AND "
    "(instr(lower(g.last_error),'insufficient_quota')>0 OR instr(lower(g.last_error),'no credits remaining')>0 "
    "OR instr(lower(g.last_error),'credit_balance_exhausted')>0) THEN '" AWK_CREDIT_ATTENTION "' ELSE '' END "
    "FROM src.goals g WHERE 'account-operator-'||g.id=temp.w.id) "
    "WHERE id IN(SELECT 'account-operator-'||id FROM src.goals);";

static int awk_exec(sqlite3 *db, const char *sql) { return sqlite3_exec(db, sql, NULL, NULL, NULL) == SQLITE_OK; }
static int awk_src_table(sqlite3 *db,const char *name){sqlite3_stmt *q=NULL;int found=0;if(sqlite3_prepare_v2(db,"SELECT 1 FROM src.sqlite_master WHERE type='table' AND name=?1",-1,&q,NULL)==SQLITE_OK){sqlite3_bind_text(q,1,name,-1,SQLITE_STATIC);found=sqlite3_step(q)==SQLITE_ROW;}sqlite3_finalize(q);return found;}

/* A completed Fabric run owns its retained stream. Only a single terminal
 * usage record with all required integer counters is a complete measurement.
 * No transcript, path or inferred price enters the account index. */
static int awk_fabric_usage(sqlite3 *db) {
    sqlite3_stmt *runs=NULL,*parse=NULL,*insert=NULL;
    const char *sql="SELECT task_id,id,agent,stdout_path FROM src.runs WHERE state='passed' AND agent IN('codex','claude')";
    const char *json="SELECT json_valid(?1),CASE WHEN json_valid(?1) THEN json_extract(?1,'$.type') END,"
      "CASE WHEN json_valid(?1) THEN json_type(?1,'$.usage.input_tokens') END,"
      "CASE WHEN json_valid(?1) THEN json_extract(?1,'$.usage.input_tokens') END,"
      "CASE WHEN json_valid(?1) THEN json_type(?1,'$.usage.output_tokens') END,"
      "CASE WHEN json_valid(?1) THEN json_extract(?1,'$.usage.output_tokens') END,"
      "CASE WHEN json_valid(?1) THEN json_type(?1,'$.usage.cache_read_input_tokens') END,"
      "CASE WHEN json_valid(?1) THEN json_extract(?1,'$.usage.cache_read_input_tokens') END,"
      "CASE WHEN json_valid(?1) THEN json_type(?1,'$.usage.cache_creation_input_tokens') END,"
      "CASE WHEN json_valid(?1) THEN json_extract(?1,'$.usage.cache_creation_input_tokens') END,"
      "CASE WHEN json_valid(?1) THEN json_type(?1,'$.total_cost_usd') END,"
      "CASE WHEN json_valid(?1) THEN json_extract(?1,'$.total_cost_usd') END,"
      "CASE WHEN json_valid(?1) THEN json_type(?1,'$.usage.cached_input_tokens') END,"
      "CASE WHEN json_valid(?1) THEN json_extract(?1,'$.usage.cached_input_tokens') END,"
      "CASE WHEN json_valid(?1) THEN json_type(?1,'$.usage.cache_write_input_tokens') END,"
      "CASE WHEN json_valid(?1) THEN json_extract(?1,'$.usage.cache_write_input_tokens') END";
    int ok=0;
    if(sqlite3_prepare_v2(db,sql,-1,&runs,NULL)!=SQLITE_OK || sqlite3_prepare_v2(db,json,-1,&parse,NULL)!=SQLITE_OK ||
       sqlite3_prepare_v2(db,"INSERT INTO temp.u(work_id,run_id,input_tokens,output_tokens,estimated_micro_usd,cache_read_tokens,cache_write_tokens) VALUES('account-fabric-'||?1,?2,?3,?4,?5,?6,?7)",-1,&insert,NULL)!=SQLITE_OK) goto done;
    while(sqlite3_step(runs)==SQLITE_ROW) {
        const char *agent=(const char*)sqlite3_column_text(runs,2),*path=(const char*)sqlite3_column_text(runs,3);
        FILE *f=path?fopen(path,"rb"):NULL; char *line=NULL; size_t cap=0,total=0; ssize_t n; int bad=0,seen=0,priced=0,has_read=0,has_write=0; sqlite3_int64 in=0,out=0,cost=0,cache_read=0,cache_write=0;
        if(!f) continue;
        while((n=getline(&line,&cap,f))>=0) {
            total+=(size_t)n; if(total>8*1024*1024 || n>1000000) {bad=1;break;}
            sqlite3_reset(parse); sqlite3_clear_bindings(parse); sqlite3_bind_text(parse,1,line,(int)n,SQLITE_TRANSIENT);
            if(sqlite3_step(parse)!=SQLITE_ROW || !sqlite3_column_int(parse,0)) {bad=1;break;}
            const char *type=(const char*)sqlite3_column_text(parse,1);
            if(!type || strcmp(type,!strcmp(agent,"codex")?"turn.completed":"result")) continue;
            seen++;
            const char *it=(const char*)sqlite3_column_text(parse,2),*ot=(const char*)sqlite3_column_text(parse,4);
            if(!it || strcmp(it,"integer") || !ot || strcmp(ot,"integer")) {bad=1;break;}
            in=sqlite3_column_int64(parse,3);out=sqlite3_column_int64(parse,5);
            if(in<0 || out<0) {bad=1;break;}
            if(!strcmp(agent,"claude")) {
                const char *rt=(const char*)sqlite3_column_text(parse,6),*wt=(const char*)sqlite3_column_text(parse,8);
                sqlite3_int64 read=sqlite3_column_int64(parse,7),write=sqlite3_column_int64(parse,9);
                if(!rt || strcmp(rt,"integer") || !wt || strcmp(wt,"integer") || read<0 || write<0 || in>LLONG_MAX-read || in+read>LLONG_MAX-write) {bad=1;break;}
                in+=read+write; cache_read=read; cache_write=write; has_read=has_write=1;
                const char *ct=(const char*)sqlite3_column_text(parse,10); double dollars=sqlite3_column_double(parse,11);
                if(ct && (!strcmp(ct,"integer")||!strcmp(ct,"real")) && dollars>=0 && dollars<=1000000) {cost=(sqlite3_int64)(dollars*1000000.0+0.5);priced=1;}
            } else {
                /* Codex counts cached reads inside input_tokens; keep them as a breakdown only. */
                const char *rt=(const char*)sqlite3_column_text(parse,12),*wt=(const char*)sqlite3_column_text(parse,14);
                has_read=rt && !strcmp(rt,"integer") && (cache_read=sqlite3_column_int64(parse,13))>=0 && cache_read<=in;
                has_write=wt && !strcmp(wt,"integer") && (cache_write=sqlite3_column_int64(parse,15))>=0;
            }
        }
        if(ferror(f)) bad=1; free(line); fclose(f);
        if(bad || seen!=1) continue;
        sqlite3_reset(insert);sqlite3_clear_bindings(insert);
        sqlite3_bind_text(insert,1,(const char*)sqlite3_column_text(runs,0),-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(insert,2,(const char*)sqlite3_column_text(runs,1),-1,SQLITE_TRANSIENT);
        sqlite3_bind_int64(insert,3,in);sqlite3_bind_int64(insert,4,out);
        if(priced) sqlite3_bind_int64(insert,5,cost); else sqlite3_bind_null(insert,5);
        if(has_read) sqlite3_bind_int64(insert,6,cache_read); else sqlite3_bind_null(insert,6);
        if(has_write) sqlite3_bind_int64(insert,7,cache_write); else sqlite3_bind_null(insert,7);
        if(sqlite3_step(insert)!=SQLITE_DONE) goto done;
    }
    if(sqlite3_errcode(db)!=SQLITE_DONE && sqlite3_errcode(db)!=SQLITE_OK) goto done;
    ok=1;
done:
    sqlite3_finalize(runs);sqlite3_finalize(parse);sqlite3_finalize(insert);return ok;
}

/* A completed Fabric task's receipts become its outputs only when the bytes
 * the owner retained still hash to the digest the owner recorded, and only
 * from the owner's own receipts directory beside its store. The index keeps
 * the digest, never the path, so an output stays a reference: nothing here
 * turns a file into a link or grants anyone the right to open it. */
#define AWK_RECEIPT_LIMIT (16 * 1024 * 1024)
static int awk_file_digest(const char *path, char hex[65]) {
    FILE *f = fopen(path, "rb"); unsigned char buf[65536]; size_t n, total = 0; BfSha256 h; uint8_t d[32];
    if (!f) return 0;
    bf_sha256_init(&h);
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0) {
        total += n; if (total > AWK_RECEIPT_LIMIT) { fclose(f); return 0; }
        bf_sha256_update(&h, buf, n);
    }
    int bad = ferror(f); fclose(f);
    if (bad) return 0;
    bf_sha256_final(&h, d);
    for (int i = 0; i < 32; i++) snprintf(hex + i * 2, 3, "%02x", d[i]);
    return 1;
}
static int awk_fabric_receipts(sqlite3 *db, const char *store) {
    sqlite3_stmt *q = NULL, *ins = NULL; int ok = 0;
    char dir[PATH_MAX]; const char *slash = strrchr(store, '/');
    if (!slash || (size_t)(slash - store) + sizeof("/receipts/") >= sizeof(dir)) return 0;
    snprintf(dir, sizeof(dir), "%.*s/receipts/", (int)(slash - store), store);
    size_t dlen = strlen(dir);
    if (sqlite3_prepare_v2(db, "SELECT r.task_id,r.id,r.kind,r.path,lower(r.digest),CAST(r.created_at*1000 AS INTEGER) FROM src.receipts r "
            "JOIN src.tasks t ON t.id=r.task_id WHERE t.state='completed' AND typeof(r.digest)='text' AND length(r.digest)=64 "
            "AND r.digest NOT GLOB '*[^0-9A-Fa-f]*' ORDER BY r.created_at,r.id", -1, &q, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, "INSERT INTO temp.s(work_id,node_id,operator_id,state,attempt,updated_ms,output_ref) "
            "VALUES('account-fabric-'||?1,'receipt-'||?2,?3,'complete',1,?4,'sha256:'||?5)", -1, &ins, NULL) != SQLITE_OK) goto done;
    int rc;
    while ((rc = sqlite3_step(q)) == SQLITE_ROW) {
        const char *path = (const char *)sqlite3_column_text(q, 3), *digest = (const char *)sqlite3_column_text(q, 4);
        char hex[65];
        if (!path || !digest || strncmp(path, dir, dlen) || strstr(path + dlen, "/") || strstr(path, "..")) continue;
        if (!awk_file_digest(path, hex) || strcmp(hex, digest)) continue;
        sqlite3_reset(ins); sqlite3_clear_bindings(ins);
        for (int c = 0; c < 3; c++) sqlite3_bind_text(ins, c + 1, (const char *)sqlite3_column_text(q, c), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(ins, 4, sqlite3_column_int64(q, 5));
        sqlite3_bind_text(ins, 5, digest, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(ins) != SQLITE_DONE) goto done;
    }
    ok = rc == SQLITE_DONE;
done:
    sqlite3_finalize(q); sqlite3_finalize(ins); return ok;
}

static int awk_attach(sqlite3 *db, const char *path) {
    sqlite3_stmt *q = NULL;
    char *uri = sqlite3_mprintf("file:%s?mode=ro", path);
    int ok = uri && sqlite3_prepare_v2(db, "ATTACH DATABASE ? AS src", -1, &q, NULL) == SQLITE_OK &&
        sqlite3_bind_text(q, 1, uri, -1, SQLITE_TRANSIENT) == SQLITE_OK && sqlite3_step(q) == SQLITE_DONE;
    sqlite3_finalize(q); sqlite3_free(uri);
    return ok;
}

/* Rebuilds the whole account index only from a complete owner snapshot.
 * A missing or invalid declared source refuses the rebuild; retaining the
 * previous index is safer than silently dropping another owner's work. */
static int awk_index(const char *path, const char *subject, const char *scope, const AwkSource *sources, size_t count) {
    sqlite3 *db = NULL;
    sqlite3_stmt *q = NULL;
    int ok = 0;
    size_t readable = 0;
    const char *meter_path = NULL;
    if (!qact_scope_valid(subject, scope) || count > AWK_SOURCE_LIMIT) return 1;
    if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) goto done;
    sqlite3_busy_timeout(db, 2000);
    if (!awk_exec(db, awk_schema)) goto done;
    sqlite3_exec(db, "ALTER TABLE account_work_usage ADD COLUMN estimated_micro_usd INTEGER", NULL, NULL, NULL);
    sqlite3_exec(db, "ALTER TABLE account_work_step ADD COLUMN output_ref TEXT NOT NULL DEFAULT ''", NULL, NULL, NULL);
    sqlite3_exec(db, "ALTER TABLE account_work ADD COLUMN attention TEXT NOT NULL DEFAULT ''", NULL, NULL, NULL);
    /* Same order as the schema so a migrated index copies temp rows positionally. */
    static const char *const awk_columns[] = {
        "ALTER TABLE account_work_usage ADD COLUMN cache_read_tokens INTEGER", "ALTER TABLE account_work_usage ADD COLUMN cache_write_tokens INTEGER",
        "ALTER TABLE account_work_run ADD COLUMN agent TEXT NOT NULL DEFAULT ''", "ALTER TABLE account_work_run ADD COLUMN role TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE account_work_run ADD COLUMN session_kind TEXT NOT NULL DEFAULT ''", "ALTER TABLE account_work_run ADD COLUMN duration_ms INTEGER",
        "ALTER TABLE account_work_run ADD COLUMN limit_signal INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE account_work_run ADD COLUMN provider_session_ref TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE account_work_run ADD COLUMN handoff_ref TEXT NOT NULL DEFAULT ''"};
    for (size_t i = 0; i < sizeof(awk_columns) / sizeof(awk_columns[0]); i++) sqlite3_exec(db, awk_columns[i], NULL, NULL, NULL);
    if (!awk_exec(db, "CREATE TEMP TABLE w(id,source,title,state,created_ms,updated_ms,attention);"
                      "CREATE TEMP TABLE r(work_id,run_id,worker_id,state,created_ms,updated_ms,agent DEFAULT '',role DEFAULT '',session_kind DEFAULT '',duration_ms DEFAULT NULL,limit_signal DEFAULT 0,provider_session_ref DEFAULT '',handoff_ref DEFAULT '');"
                      "CREATE TEMP TABLE s(work_id,node_id,operator_id,state,attempt,updated_ms,output_ref TEXT NOT NULL DEFAULT '');"
                      "CREATE TEMP TABLE u(work_id,run_id,input_tokens,output_tokens,estimated_micro_usd,cache_read_tokens DEFAULT NULL,cache_write_tokens DEFAULT NULL);"
                      "CREATE INDEX temp.r_work ON r(work_id);CREATE INDEX temp.s_work ON s(work_id);")) goto done;
    for (size_t i = 0; i < count; i++) {
        if (!strcmp(sources[i].kind, "meter")) {
            if (meter_path) goto done;
            meter_path = sources[i].path;
            continue;
        }
        const char *sql = !strcmp(sources[i].kind, "operator") ? awk_operator : !strcmp(sources[i].kind, "fabric") ? awk_fabric :
            !strcmp(sources[i].kind, "workgraph") ? awk_workgraph : NULL;
        if (!sql) { fprintf(stderr, "account-work: unknown source kind %s\n", sources[i].kind); goto done; }
        if (!awk_attach(db, sources[i].path)) { fprintf(stderr, "account-work: cannot read %s\n", sources[i].path); goto done; }
        if (!strcmp(sources[i].kind, "operator") && awk_src_table(db, "operator_run_context")) sql = awk_operator_context;
        readable++;
        if (!awk_exec(db, sql)) {
            /* Fail closed; a present but invalid source must not erase Work. */
            fprintf(stderr, "account-work: %s is not a %s store: %s\n", sources[i].path, sources[i].kind, sqlite3_errmsg(db));
            awk_exec(db, "DETACH DATABASE src");
            goto done;
        }
        if (!strcmp(sources[i].kind,"operator") && awk_src_last_error_column(db) && !awk_exec(db,awk_operator_attention)) {
            fprintf(stderr,"account-work: attention projection failed: %s\n",sqlite3_errmsg(db));goto done;
        }
        if (!strcmp(sources[i].kind,"operator") && awk_src_column(db,"runs","summary") && !awk_exec(db,awk_operator_limit)) {
            fprintf(stderr,"account-work: operator limit projection failed: %s\n",sqlite3_errmsg(db));goto done;
        }
        if (!strcmp(sources[i].kind,"operator") && awk_src_usage_column(db) && !awk_exec(db,awk_operator_usage)) {
            fprintf(stderr,"account-work: usage projection failed: %s\n",sqlite3_errmsg(db));goto done;
        }
        if (!strcmp(sources[i].kind,"fabric") && ((awk_src_column(db,"tasks","kind") && !awk_exec(db,awk_fabric_kind)) ||
            (awk_src_column(db,"runs","duration_ms") && !awk_exec(db,awk_fabric_duration)) ||
            (awk_src_column(db,"runs","result_json") && !awk_exec(db,awk_fabric_limit)) ||
            (awk_src_column(db,"runs","session_id") && !awk_exec(db,awk_fabric_session)))) {
            fprintf(stderr,"account-work: fabric agent projection failed: %s\n",sqlite3_errmsg(db));goto done;
        }
        if (!strcmp(sources[i].kind,"fabric") && awk_src_column(db,"runs","stdout_path") && !awk_fabric_usage(db)) {
            fprintf(stderr,"account-work: fabric usage projection failed: %s\n",sqlite3_errmsg(db));goto done;
        }
        if (!strcmp(sources[i].kind,"fabric") && awk_src_table(db,"receipts") && awk_src_column(db,"receipts","digest") &&
            !awk_fabric_receipts(db,sources[i].path)) {
            fprintf(stderr,"account-work: fabric receipt projection failed: %s\n",sqlite3_errmsg(db));goto done;
        }
        if (!strcmp(sources[i].kind,"workgraph") && awk_src_table(db,"workgraph_transitions") && awk_src_table(db,"workgraph_attempts") &&
            awk_src_column(db,"receipts","payload") && !awk_exec(db,awk_workgraph_runs)) {
            fprintf(stderr,"account-work: workgraph run projection failed: %s\n",sqlite3_errmsg(db));goto done;
        }
        if (!awk_exec(db, "DETACH DATABASE src")) goto done;
    }
    if (!readable || readable + (meter_path != NULL) != count) goto done;
    /* Reconciled archive estimates apply only while both reported token totals
     * still match the exact run; a terminal provider estimate takes priority. */
    if (!awk_exec(db,"UPDATE temp.u SET estimated_micro_usd=(SELECT e.estimated_micro_usd FROM account_work_estimate e WHERE e.work_id=u.work_id AND e.run_id=u.run_id AND e.input_tokens=u.input_tokens AND e.output_tokens=u.output_tokens) WHERE estimated_micro_usd IS NULL;")) goto done;
    /* A Meter session is a run measurement only if exactly one run in the
     * entire index names it. Existing terminal usage wins; snapshots cannot
     * establish complete run usage. No event-time prices are projected. */
    if (meter_path) {
        if (!awk_attach(db, meter_path)) {
            fprintf(stderr,"account-work: warning: Meter unavailable; Meter token evidence omitted\n");
        } else {
            if (!awk_exec(db,
                "CREATE INDEX temp.r_session ON r(provider_session_ref);"
                "CREATE INDEX temp.u_run ON u(work_id,run_id);"
                "CREATE TEMP TABLE meter_foreign(ref TEXT PRIMARY KEY) WITHOUT ROWID;")) goto done;
            if (sqlite3_prepare_v2(db,
                "INSERT OR IGNORE INTO temp.meter_foreign SELECT x.provider_session_ref "
                "FROM account_work_run x JOIN account_work a ON a.id=x.work_id "
                "WHERE x.provider_session_ref<>'' AND NOT(a.subject=?1 AND a.scope=?2)", -1, &q, NULL)!=SQLITE_OK) goto done;
            sqlite3_bind_text(q,1,subject,-1,SQLITE_TRANSIENT); sqlite3_bind_text(q,2,scope,-1,SQLITE_TRANSIENT);
            if (sqlite3_step(q)!=SQLITE_DONE) goto done;
            sqlite3_finalize(q); q=NULL;
            /* Fix the join order and expose the predicate of Meter's partial
             * session index; otherwise SQLite scans all account events per run. */
            const char *meter_sql =
                "INSERT INTO temp.u(work_id,run_id,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens) "
                "SELECT r.work_id,r.run_id,sum(e.uncached_input_tokens+e.cached_input_tokens+e.cache_write_tokens),"
                "sum(e.output_tokens),sum(e.cached_input_tokens),sum(e.cache_write_tokens) "
                "FROM temp.r r JOIN temp.w w ON w.id=r.work_id CROSS JOIN src.events e "
                "WHERE r.provider_session_ref<>'' AND e.session_ref<>'' AND e.session_ref=r.provider_session_ref AND e.key_id=?1 AND e.op='model.tokens' AND e.provider=r.agent "
                "AND (SELECT count(*) FROM temp.r x WHERE x.provider_session_ref=r.provider_session_ref)=1 "
                "AND NOT EXISTS(SELECT 1 FROM temp.meter_foreign x WHERE x.ref=r.provider_session_ref) "
                "AND NOT EXISTS(SELECT 1 FROM temp.u u WHERE u.work_id=r.work_id AND u.run_id=r.run_id) "
                "GROUP BY r.work_id,r.run_id,r.provider_session_ref HAVING "
                "sum(CASE WHEN e.counter_basis IS NULL OR e.counter_basis NOT IN ('message_usage','counter_delta') OR "
                "typeof(e.uncached_input_tokens)<>'integer' OR typeof(e.cached_input_tokens)<>'integer' OR "
                "typeof(e.cache_write_tokens)<>'integer' OR typeof(e.output_tokens)<>'integer' OR "
                "e.uncached_input_tokens<0 OR e.cached_input_tokens<0 OR e.cache_write_tokens<0 OR e.output_tokens<0 "
                "THEN 1 ELSE 0 END)=0 AND typeof(sum(e.uncached_input_tokens+e.cached_input_tokens+e.cache_write_tokens))='integer'";
            if (!awk_exec(db,"SAVEPOINT meter_projection")) goto done;
            int meter_ok = sqlite3_prepare_v2(db,meter_sql,-1,&q,NULL)==SQLITE_OK;
            if (meter_ok) {
                sqlite3_bind_text(q,1,subject,-1,SQLITE_TRANSIENT);
                meter_ok = sqlite3_step(q)==SQLITE_DONE;
            }
            sqlite3_finalize(q); q=NULL;
            if (!meter_ok) {
                if (!awk_exec(db,"ROLLBACK TO meter_projection")) goto done;
                fprintf(stderr,"account-work: warning: Meter unavailable; Meter token evidence omitted\n");
            }
            if (!awk_exec(db,"RELEASE meter_projection") || !awk_exec(db,"DETACH DATABASE src")) goto done;
        }
    }
    /* Rows the rail could not name are dropped, never repaired into a guess. */
    if (!awk_exec(db,
        "DELETE FROM temp.w WHERE length(id)>127 OR id GLOB '*[^A-Za-z0-9._:-]*' OR typeof(created_ms)<>'integer' OR typeof(updated_ms)<>'integer' "
        "OR created_ms<0 OR updated_ms<0 OR trim(COALESCE(title,''))='';"
        "DELETE FROM temp.r WHERE typeof(created_ms)<>'integer' OR typeof(updated_ms)<>'integer' OR created_ms<0 OR length(run_id)>127 OR trim(run_id)='';"
        "UPDATE temp.r SET updated_ms=created_ms WHERE updated_ms<created_ms;"
        "DELETE FROM temp.s WHERE length(node_id)>127 OR trim(node_id)='' OR trim(operator_id)='' OR length(operator_id)>127;"
        "UPDATE temp.w SET updated_ms=max(updated_ms,created_ms,"
        "COALESCE((SELECT max(updated_ms) FROM temp.r WHERE work_id=w.id),0),COALESCE((SELECT max(updated_ms) FROM temp.s WHERE work_id=w.id),0));"
        "BEGIN IMMEDIATE;")) goto done;
    if (sqlite3_prepare_v2(db,"DELETE FROM account_work_usage WHERE work_id IN(SELECT id FROM account_work WHERE subject=?1 AND scope=?2);",-1,&q,NULL)!=SQLITE_OK)goto rollback;
    sqlite3_bind_text(q,1,subject,-1,SQLITE_TRANSIENT);sqlite3_bind_text(q,2,scope,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(q)!=SQLITE_DONE)goto rollback;sqlite3_finalize(q);q=NULL;
    if (sqlite3_prepare_v2(db, "DELETE FROM account_work_run WHERE work_id IN(SELECT id FROM account_work WHERE subject=?1 AND scope=?2);"
            , -1, &q, NULL) != SQLITE_OK) goto rollback;
    sqlite3_bind_text(q, 1, subject, -1, SQLITE_TRANSIENT); sqlite3_bind_text(q, 2, scope, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(q) != SQLITE_DONE) goto rollback;
    sqlite3_finalize(q); q = NULL;
    if (sqlite3_prepare_v2(db, "DELETE FROM account_work_step WHERE work_id IN(SELECT id FROM account_work WHERE subject=?1 AND scope=?2);"
            , -1, &q, NULL) != SQLITE_OK || (sqlite3_bind_text(q, 1, subject, -1, SQLITE_TRANSIENT), sqlite3_bind_text(q, 2, scope, -1, SQLITE_TRANSIENT),
            sqlite3_step(q)) != SQLITE_DONE) goto rollback;
    sqlite3_finalize(q); q = NULL;
    if (sqlite3_prepare_v2(db, "DELETE FROM account_work WHERE subject=?1 AND scope=?2", -1, &q, NULL) != SQLITE_OK ||
        (sqlite3_bind_text(q, 1, subject, -1, SQLITE_TRANSIENT), sqlite3_bind_text(q, 2, scope, -1, SQLITE_TRANSIENT), sqlite3_step(q)) != SQLITE_DONE) goto rollback;
    sqlite3_finalize(q); q = NULL;
    /* Sequence follows recency, so the rail's newest-first page is newest work. */
    if (sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO account_work(id,subject,scope,source,title,state,created_ms,updated_ms,attention) "
            "SELECT id,?1,?2,source,title,state,min(created_ms,updated_ms),updated_ms,attention FROM temp.w ORDER BY updated_ms,id", -1, &q, NULL) != SQLITE_OK ||
        (sqlite3_bind_text(q, 1, subject, -1, SQLITE_TRANSIENT), sqlite3_bind_text(q, 2, scope, -1, SQLITE_TRANSIENT), sqlite3_step(q)) != SQLITE_DONE) goto rollback;
    sqlite3_finalize(q); q = NULL;
    if (!awk_exec(db,
        "INSERT INTO account_work_run SELECT r.* FROM temp.r r JOIN account_work a ON a.id=r.work_id;"
        "INSERT INTO account_work_usage SELECT u.* FROM temp.u u JOIN account_work a ON a.id=u.work_id;"
        "INSERT INTO account_work_step SELECT s.* FROM temp.s s JOIN account_work a ON a.id=s.work_id;"
        "INSERT OR REPLACE INTO account_work_meta VALUES('indexed_ms',CAST((julianday('now')-2440587.5)*86400000 AS INTEGER));"
        "COMMIT;")) goto rollback;
    ok = 1;
    goto done;
rollback:
    sqlite3_finalize(q); q = NULL;
    awk_exec(db, "ROLLBACK");
done:
    sqlite3_finalize(q);
    if (!ok && db) fprintf(stderr, "account-work: index failed: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return ok ? 0 : 1;
}

static int awk_stale(const char *path) {
    sqlite3 *db = NULL;
    sqlite3_stmt *q = NULL;
    int stale = 1;
    if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK &&
        sqlite3_prepare_v2(db, "SELECT value FROM account_work_meta WHERE key='indexed_ms'", -1, &q, NULL) == SQLITE_OK &&
        sqlite3_step(q) == SQLITE_ROW) stale = qact_now() - sqlite3_column_int64(q, 0) > AWK_STALE_MS;
    sqlite3_finalize(q); sqlite3_close(db);
    return stale;
}

/* Titles come from owners that never promised the rail's 160-byte public text:
 * control characters become spaces and the cut lands on a UTF-8 boundary. */
static void awk_title(const char *in, char out[161]) {
    size_t n = 0;
    for (const unsigned char *p = (const unsigned char *)(in ? in : ""); *p && n < 160; p++) out[n++] = *p < 32 || *p == 127 ? ' ' : (char)*p;
    while (n && ((unsigned char)out[n - 1] & 0xC0) == 0x80) n--;
    if (n && (unsigned char)out[n - 1] >= 0xC0) n--;
    out[n] = 0;
    if (!bf_utf8_valid(out, n, NULL)) snprintf(out, 161, "Untitled work");
    char *start = out; while (*start == ' ') start++;
    if (!*start) snprintf(out, 161, "Untitled work"); else if (start != out) memmove(out, start, strlen(start) + 1);
}

static void awk_atom(const char *in, char *out, size_t cap) {
    size_t n = 0;
    for (const unsigned char *p = (const unsigned char *)(in ? in : ""); *p && n + 1 < cap && n < 127; p++)
        out[n++] = ((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') || (*p >= '0' && *p <= '9') || strchr("._:- ", *p)) ? (char)*p : '-';
    out[n] = 0;
    if (!n) snprintf(out, cap, "unknown");
}

static void awk_attention(const char *in,char out[241]) {
    out[0]=0;
    if(in&&!strcmp(in,AWK_CREDIT_ATTENTION))snprintf(out,241,"%s",AWK_CREDIT_ATTENTION);
}

static int awk_load(sqlite3 *db, sqlite3_stmt *row, QactTask *t) {
    sqlite3_stmt *q = NULL;
    const char *id = (const char *)sqlite3_column_text(row, 1), *source = (const char *)sqlite3_column_text(row, 3);
    memset(t, 0, sizeof(*t));
    if (!id || !source || !qact_atom(id, 127)) return 0;
    t->notice.sequence = sqlite3_column_int64(row, 0);
    snprintf(t->notice.id, sizeof(t->notice.id), "%s", id);
    awk_title((const char *)sqlite3_column_text(row, 2), t->notice.title);
    snprintf(t->state, sizeof(t->state), "%s", (const char *)sqlite3_column_text(row, 4));
    t->notice.created = sqlite3_column_int64(row, 5);
    t->notice.updated = sqlite3_column_int64(row, 6);
    awk_attention((const char *)sqlite3_column_text(row,7),t->notice.attention);
    int root = qact_phase(t->state, 0, 0);
    if (root < 0) return 0;
    /* A blocked assistant goal is waiting on the person, not on another step,
     * so the rail files it under Needs you rather than Queued. */
    if (root == 0 && !strcmp(t->state, "blocked") && !strncmp(id, "account-operator-", 17)) root = 2;
    QactNode *job = &t->nodes[0];
    snprintf(job->id, sizeof(job->id), "job"); awk_atom(source, job->operator_id, sizeof(job->operator_id));
    snprintf(job->family, sizeof(job->family), "%s", job->operator_id); snprintf(job->state, sizeof(job->state), "%s", t->state);
    snprintf(job->phase, sizeof(job->phase), "%s", qact_phase_name(root)); job->updated = t->notice.updated;
    t->node_count = 1; t->total_nodes = 1; t->phases[root]++;
    if (sqlite3_prepare_v2(db, "SELECT node_id,operator_id,state,attempt,updated_ms,COALESCE(output_ref,'') FROM account_work_step WHERE work_id=? ORDER BY node_id", -1, &q, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(q, 1, id, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(q) == SQLITE_ROW) {
        QactNode node = {0};
        const char *state = (const char *)sqlite3_column_text(q, 2);
        int phase = state ? qact_phase(state, 0, 0) : -1;
        if (phase < 0) continue;
        awk_atom((const char *)sqlite3_column_text(q, 0), node.id, sizeof(node.id));
        int is_root = !strcmp(node.id, "job");
        snprintf(node.parent, sizeof(node.parent), "job");
        awk_atom((const char *)sqlite3_column_text(q, 1), node.operator_id, sizeof(node.operator_id));
        snprintf(node.state, sizeof(node.state), "%s", state); snprintf(node.phase, sizeof(node.phase), "%s", qact_phase_name(phase));
        node.attempt = sqlite3_column_int64(q, 3); node.updated = sqlite3_column_int64(q, 4);
        if (node.updated > t->notice.updated) node.updated = t->notice.updated;
        if (!is_root) { t->total_nodes++; t->phases[phase]++; }
        const char *output = (const char *)sqlite3_column_text(q, 5);
        node.has_output = output && *output && sqlite3_column_bytes(q, 5) == (int)strlen(output) && !strcmp(node.state, "complete");
        if (node.has_output) {
            t->total_outputs++;
            if (t->output_count < QACT_OUTPUT_LIMIT) {
                QactOutput *o = &t->outputs[t->output_count++];
                snprintf(o->node, sizeof(o->node), "%s", node.id);
                snprintf(o->label, sizeof(o->label), "%s output reference", node.operator_id);
                bf_sha256_hex((const uint8_t *)output, strlen(output), o->reference);
                o->updated = node.updated;
            }
        }
        if (is_root) job->has_output = node.has_output;
        else if (t->node_count < QACT_NODE_LIMIT) t->nodes[t->node_count++] = node;
    }
    sqlite3_finalize(q); q = NULL;
    int64_t combined[5]; memcpy(combined, t->phases, sizeof(combined));
    if (sqlite3_prepare_v2(db, "SELECT run_id,worker_id,state,created_ms,updated_ms FROM account_work_run WHERE work_id=? ORDER BY updated_ms DESC,run_id", -1, &q, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(q, 1, id, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(q) == SQLITE_ROW) {
        const char *state = (const char *)sqlite3_column_text(q, 2);
        int phase = state ? qact_phase(state, 1, 0) : -1;
        if (phase < 0) continue;
        t->total_workers++; combined[phase]++;
        if (phase == 1) t->active_workers++;
        if (t->worker_count >= QACT_WORKER_LIMIT) continue;
        QactWorker *v = &t->workers[t->worker_count++];
        awk_atom((const char *)sqlite3_column_text(q, 0), v->id, sizeof(v->id));
        awk_atom((const char *)sqlite3_column_text(q, 1), v->worker, sizeof(v->worker));
        snprintf(v->state, sizeof(v->state), "%s", state); snprintf(v->phase, sizeof(v->phase), "%s", qact_phase_name(phase));
        v->created = sqlite3_column_int64(q, 3); v->updated = sqlite3_column_int64(q, 4);
        if (v->updated > t->notice.updated) v->updated = t->notice.updated;
        if (v->created > v->updated) v->created = v->updated;
    }
    sqlite3_finalize(q);
    /* Exact indexed run coverage from the same scoped account-work row. */
    if (sqlite3_prepare_v2(db,
        "SELECT (SELECT count(*) FROM account_work_run WHERE work_id=?1),count(*),COALESCE(sum(input_tokens),0),COALESCE(sum(output_tokens),0),count(estimated_micro_usd),COALESCE(sum(estimated_micro_usd),0) "
        "FROM account_work_usage WHERE work_id=?1",-1,&q,NULL)!=SQLITE_OK) return 0;
    sqlite3_bind_text(q,1,id,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(q)!=SQLITE_ROW) { sqlite3_finalize(q); return 0; }
    t->has_account_usage=1;
    t->usage_runs=sqlite3_column_int64(q,0);
    t->usage_reported_runs=sqlite3_column_int64(q,1);
    t->usage_input_tokens=sqlite3_column_int64(q,2);
    t->usage_output_tokens=sqlite3_column_int64(q,3);
    t->usage_priced_runs=sqlite3_column_int64(q,4);t->usage_estimated_micro_usd=sqlite3_column_int64(q,5);
    sqlite3_finalize(q); q=NULL;
    if (t->usage_reported_runs>t->usage_runs || t->usage_priced_runs>t->usage_reported_runs || t->usage_runs<0 || t->usage_input_tokens<0 || t->usage_output_tokens<0 || t->usage_estimated_micro_usd<0) return 0;
    /* The rail's caps: 512 runs and 4096 steps are the most it will count. */
    if (t->total_workers > 512) t->total_workers = 512;
    if (t->active_workers > t->total_workers) t->active_workers = t->total_workers;
    if (t->total_nodes > 4096) return 0;
    int phase = combined[2] ? 2 : combined[1] ? 1 : combined[0] ? 0 : combined[4] ? 4 : 3;
    snprintf(t->notice.phase, sizeof(t->notice.phase), "%s", qact_phase_name(phase));
    BfSha256 hash; uint8_t digest[32];
    bf_sha256_init(&hash);
    bf_sha256_update(&hash, (const uint8_t *)"account-work-v2", 15);
    bf_sha256_update(&hash, (const uint8_t *)id, strlen(id) + 1);
    bf_sha256_update(&hash, (const uint8_t *)t->state, strlen(t->state) + 1);
    bf_sha256_update(&hash, (const uint8_t *)t->notice.attention, strlen(t->notice.attention) + 1);
    int64_t cells[] = {t->notice.updated, t->total_nodes, t->total_workers, t->active_workers, t->total_outputs, phase};
    bf_sha256_update(&hash, (const uint8_t *)cells, sizeof(cells));
    for (size_t i = 0; i < t->output_count; i++)
        bf_sha256_update(&hash, (const uint8_t *)t->outputs[i].reference, strlen(t->outputs[i].reference) + 1);
    bf_sha256_final(&hash, digest);
    for (int i = 0; i < 32; i++) snprintf(t->notice.version + i * 2, 3, "%02x", digest[i]);
    return 1;
}

/* The Inbox over account work: work that moved in the last week (running,
 * finished, stopped or waiting on a retry), newest first. Queued goals stay in
 * Activity; an owner re-touching a queue never becomes a thousand notices. */
#define AWK_NOTICE_WINDOW_MS INT64_C(604800000)
static int awk_notice(sqlite3 *db, const char *subject, const char *scope, const char *id, QactNotice *notice) {
    sqlite3_stmt *q = NULL;
    QactTask *task = calloc(1, sizeof(*task));
    int answer = -1;
    if (!task || !qact_scope_valid(subject, scope) || !qact_atom(id, 127) ||
        sqlite3_prepare_v2(db, "SELECT seq,id,title,source,state,created_ms,updated_ms,attention FROM account_work "
            "WHERE subject=?1 AND scope=?2 AND id=?3 AND updated_ms>=?4 AND state NOT IN('ready','defined') AND NOT (source='Aurekai assistant' AND state='cancelled')", -1, &q, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(q, 1, subject, -1, SQLITE_TRANSIENT); sqlite3_bind_text(q, 2, scope, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(q, 3, id, -1, SQLITE_TRANSIENT); sqlite3_bind_int64(q, 4, qact_now() - AWK_NOTICE_WINDOW_MS);
    int rc = sqlite3_step(q);
    if (rc == SQLITE_DONE) answer = 0;
    else if (rc == SQLITE_ROW && awk_load(db, q, task)) { *notice = task->notice; answer = 1; }
done:
    sqlite3_finalize(q); free(task);
    return answer;
}
static int awk_notice_ids(sqlite3 *db, const char *subject, const char *scope, int64_t cursor,
                          char ids[51][128], int64_t sequences[51], size_t *count) {
    sqlite3_stmt *q = NULL;
    int rc;
    *count = 0;
    if (!qact_scope_valid(subject, scope) || cursor < 0 || cursor > QACT_MAX_TIME ||
        sqlite3_prepare_v2(db, "SELECT id,seq FROM account_work WHERE subject=?1 AND scope=?2 AND updated_ms>=?3 AND seq<?4 AND state NOT IN('ready','defined') AND NOT (source='Aurekai assistant' AND state='cancelled') "
            "ORDER BY seq DESC LIMIT 51", -1, &q, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(q, 1, subject, -1, SQLITE_TRANSIENT); sqlite3_bind_text(q, 2, scope, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(q, 3, qact_now() - AWK_NOTICE_WINDOW_MS); sqlite3_bind_int64(q, 4, cursor ? cursor : INT64_MAX);
    while ((rc = sqlite3_step(q)) == SQLITE_ROW) {
        if (!qact_cell(q, 0, ids[*count], 128, 0) || !qact_atom(ids[*count], 127)) continue;
        sequences[(*count)++] = sqlite3_column_int64(q, 1);
    }
    sqlite3_finalize(q);
    return rc == SQLITE_DONE ? 1 : -1;
}
/* The Inbox asks whichever owner the store holds: account work when it is the
 * account work index, the queue's own scoped missions otherwise. */
static inline int awk_source_notice(sqlite3 *db, const char *subject, const char *scope, const char *id, QactNotice *notice) {
    int indexed = qact_table(db, "account_work");
    return indexed < 0 ? -1 : indexed ? awk_notice(db, subject, scope, id, notice) : qact_notice(db, subject, scope, id, notice);
}
static inline int awk_source_ids(sqlite3 *db, const char *subject, const char *scope, int64_t cursor,
                                 char ids[51][128], int64_t sequences[51], size_t *count) {
    int indexed = qact_table(db, "account_work");
    return indexed < 0 ? -1 : indexed ? awk_notice_ids(db, subject, scope, cursor, ids, sequences, count)
                                      : qact_notice_ids(db, subject, scope, cursor, ids, sequences, count);
}

/* Same envelope as qact_read: [1,true,{tasks,more,cursor}], newest first. */
static int awk_read(const char *path, const char *subject, const char *scope, const char *mission, int64_t cursor, int outputs_only, FILE *out) {
    sqlite3 *db = NULL;
    sqlite3_stmt *q = NULL;
    bf_mp_writer_t w;
    QactTask *tasks = calloc(QACT_PAGE, sizeof(*tasks));
    int ok = 0, listed = 0, more = 0, rc;
    bf_mp_writer_init(&w, 4096);
    if (!tasks || !qact_scope_valid(subject, scope) || cursor < 0 || (mission && *mission && (!qact_atom(mission, 127) || cursor))) goto done;
    if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK || !awk_exec(db, "BEGIN")) goto done;
    if (sqlite3_prepare_v2(db, mission && *mission
            ? "SELECT seq,id,title,source,state,created_ms,updated_ms,attention FROM account_work WHERE subject=?1 AND scope=?2 AND id=?3"
            : outputs_only
            ? "SELECT seq,id,title,source,state,created_ms,updated_ms,attention FROM account_work WHERE subject=?1 AND scope=?2 AND (?4=0 OR seq<?4) "
              "AND EXISTS(SELECT 1 FROM account_work_step s WHERE s.work_id=account_work.id AND s.state='complete' AND s.output_ref<>'') ORDER BY seq DESC"
            : "SELECT seq,id,title,source,state,created_ms,updated_ms,attention FROM account_work WHERE subject=?1 AND scope=?2 AND (?4=0 OR seq<?4) "
              "ORDER BY seq DESC", -1, &q, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(q, 1, subject, -1, SQLITE_TRANSIENT); sqlite3_bind_text(q, 2, scope, -1, SQLITE_TRANSIENT);
    if (mission && *mission) sqlite3_bind_text(q, 3, mission, -1, SQLITE_TRANSIENT); else sqlite3_bind_int64(q, 4, cursor);
    /* A row the rail could not verify is skipped, so the page reads on past it. */
    while ((rc = sqlite3_step(q)) == SQLITE_ROW) {
        if (listed == QACT_PAGE) { more = 1; break; }
        if (awk_load(db, q, &tasks[listed])) listed++;
    }
    if ((rc != SQLITE_ROW && rc != SQLITE_DONE) || (mission && *mission && !listed)) goto done;
    if (bf_mp_pack_array(&w, 3) || bf_mp_pack_int(&w, 1) || bf_mp_pack_bool(&w, 1) || bf_mp_pack_map(&w, 3) ||
        bf_mp_pack_str_cstr(&w, "tasks") || bf_mp_pack_array(&w, (uint32_t)listed)) goto done;
    for (int i = 0; i < listed; i++) if (!qact_pack_task(&w, &tasks[i]) || bf_mp_writer_size(&w) > QACT_PACKET_LIMIT) goto done;
    if (bf_mp_pack_kv_bool(&w, "more", more) || bf_mp_pack_kv_int(&w, "cursor", more ? tasks[listed - 1].notice.sequence : 0)) goto done;
    ok = fwrite(bf_mp_writer_data(&w), 1, bf_mp_writer_size(&w), out) == bf_mp_writer_size(&w) && !ferror(out);
done:
    sqlite3_finalize(q);
    if (db) { awk_exec(db, "ROLLBACK"); sqlite3_close(db); }
    bf_mp_writer_free(&w);
    free(tasks);
    return ok ? 0 : 1;
}

/* The Agents lane: one row per recorded run (Claude, Codex, the assistant's
 * model runs) under this account's own work, running runs first, then newest.
 * Usage appears only where a run's own terminal record reported it; a missing
 * counter is nil, never zero. Wall time is the owner's recorded span. A run
 * still marked running under finished work is unconfirmed, so it needs you.
 * Counts cover every running run and the last week of finished ones. */
#define AWK_AGENT_PAGE 20
static int awk_pack_opt(bf_mp_writer_t *w, const char *key, sqlite3_stmt *q, int col) {
    if (bf_mp_pack_str_cstr(w, key)) return 0;
    if (sqlite3_column_type(q, col) == SQLITE_INTEGER && sqlite3_column_int64(q, col) >= 0) return !bf_mp_pack_int(w, sqlite3_column_int64(q, col));
    return !bf_mp_pack_nil(w);
}
static int awk_agents(const char *path, const char *subject, const char *scope, FILE *out) {
    sqlite3 *db = NULL; sqlite3_stmt *q = NULL; bf_mp_writer_t w; int ok = 0, listed = 0, more = 0, rc;
    int64_t counts[3] = {0, 0, 0}, since = qact_now() - AWK_NOTICE_WINDOW_MS;
    bf_mp_writer_init(&w, 4096);
    if (!qact_scope_valid(subject, scope) || sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK || !awk_exec(db, "BEGIN")) goto done;
    sqlite3_busy_timeout(db, 1000);
    const char *phase = "CASE WHEN r.state='running' AND a.state IN('complete','dead_letter','cancelled') THEN 2 WHEN r.state='running' THEN 1 "
                        "WHEN r.state='failed' THEN 2 WHEN r.state='completed' THEN 3 ELSE 0 END";
    char *sql = sqlite3_mprintf("SELECT %s AS p,count(*) FROM account_work_run r JOIN account_work a ON a.id=r.work_id "
                                "WHERE a.subject=?1 AND a.scope=?2 AND (r.state='running' OR r.updated_ms>=?3) GROUP BY p", phase);
    if (!sql || sqlite3_prepare_v2(db, sql, -1, &q, NULL) != SQLITE_OK) { sqlite3_free(sql); goto done; }
    sqlite3_free(sql);
    sqlite3_bind_text(q, 1, subject, -1, SQLITE_TRANSIENT); sqlite3_bind_text(q, 2, scope, -1, SQLITE_TRANSIENT); sqlite3_bind_int64(q, 3, since);
    while ((rc = sqlite3_step(q)) == SQLITE_ROW) { int p = sqlite3_column_int(q, 0); if (p >= 1 && p <= 3) counts[p - 1] = sqlite3_column_int64(q, 1); }
    sqlite3_finalize(q); q = NULL;
    if (rc != SQLITE_DONE) goto done;
    sql = sqlite3_mprintf("SELECT r.run_id,r.work_id,a.title,r.agent,r.role,r.session_kind,r.state,%s,r.created_ms,r.updated_ms,r.duration_ms,"
        "r.limit_signal OR (r.state='failed' AND a.attention<>''),r.state='running' AND a.state IN('complete','dead_letter','cancelled'),"
        "u.run_id IS NOT NULL,u.input_tokens,u.output_tokens,u.cache_read_tokens,u.cache_write_tokens,u.estimated_micro_usd "
        "FROM account_work_run r JOIN account_work a ON a.id=r.work_id LEFT JOIN account_work_usage u ON u.work_id=r.work_id AND u.run_id=r.run_id "
        "WHERE a.subject=?1 AND a.scope=?2 ORDER BY r.state='running' DESC,r.updated_ms DESC,r.run_id LIMIT %d", phase, AWK_AGENT_PAGE + 1);
    if (!sql || sqlite3_prepare_v2(db, sql, -1, &q, NULL) != SQLITE_OK) { sqlite3_free(sql); goto done; }
    sqlite3_free(sql);
    sqlite3_bind_text(q, 1, subject, -1, SQLITE_TRANSIENT); sqlite3_bind_text(q, 2, scope, -1, SQLITE_TRANSIENT);
    /* First pass sizes the page, second packs it; ids were validated at index time. */
    while ((rc = sqlite3_step(q)) == SQLITE_ROW) { if (listed == AWK_AGENT_PAGE) { more = 1; break; } listed++; }
    if ((rc != SQLITE_ROW && rc != SQLITE_DONE) || sqlite3_reset(q) != SQLITE_OK || bf_mp_pack_array(&w, 3) || bf_mp_pack_int(&w, 1) ||
        bf_mp_pack_bool(&w, 1) || bf_mp_pack_map(&w, 3) || bf_mp_pack_str_cstr(&w, "agents") || bf_mp_pack_array(&w, (uint32_t)listed)) goto done;
    for (int i = 0; i < listed; i++) {
        if (sqlite3_step(q) != SQLITE_ROW) goto done;
        char id[128], title[161], agent[128], role[128], session[128], state[32];
        const char *task = (const char *)sqlite3_column_text(q, 1), *r = (const char *)sqlite3_column_text(q, 4), *k = (const char *)sqlite3_column_text(q, 5);
        if (!task || !qact_atom(task, 127)) goto done;
        awk_atom((const char *)sqlite3_column_text(q, 0), id, sizeof(id)); awk_title((const char *)sqlite3_column_text(q, 2), title);
        awk_atom((const char *)sqlite3_column_text(q, 3), agent, sizeof(agent)); awk_atom(r, role, sizeof(role)); awk_atom(k, session, sizeof(session));
        snprintf(state, sizeof(state), "%s", (const char *)sqlite3_column_text(q, 6));
        int64_t started = sqlite3_column_int64(q, 8), updated = sqlite3_column_int64(q, 9);
        if (bf_mp_pack_map(&w, 14) || bf_mp_pack_kv_str(&w, "id", id) || bf_mp_pack_kv_str(&w, "taskId", task) ||
            bf_mp_pack_kv_str(&w, "title", title) || bf_mp_pack_kv_str(&w, "agent", agent) || bf_mp_pack_str_cstr(&w, "role") ||
            (r && *r ? bf_mp_pack_str_cstr(&w, role) : bf_mp_pack_nil(&w)) || bf_mp_pack_str_cstr(&w, "session") ||
            (k && *k ? bf_mp_pack_str_cstr(&w, session) : bf_mp_pack_nil(&w)) || bf_mp_pack_kv_str(&w, "state", state) ||
            bf_mp_pack_kv_str(&w, "phase", qact_phase_name(sqlite3_column_int(q, 7))) ||
            bf_mp_pack_kv_int(&w, "started", started) || bf_mp_pack_kv_int(&w, "updated", updated < started ? started : updated) ||
            !awk_pack_opt(&w, "wallMs", q, 10) || bf_mp_pack_kv_bool(&w, "limit", sqlite3_column_int(q, 11)) ||
            bf_mp_pack_kv_bool(&w, "unconfirmed", sqlite3_column_int(q, 12)) || bf_mp_pack_str_cstr(&w, "usage")) goto done;
        if (!sqlite3_column_int(q, 13)) { if (bf_mp_pack_nil(&w)) goto done; }
        else if (bf_mp_pack_map(&w, 5) || !awk_pack_opt(&w, "inputTokens", q, 14) || !awk_pack_opt(&w, "outputTokens", q, 15) ||
                 !awk_pack_opt(&w, "cacheReadTokens", q, 16) || !awk_pack_opt(&w, "cacheWriteTokens", q, 17) ||
                 !awk_pack_opt(&w, "estimatedMicroUsd", q, 18)) goto done;
        if (bf_mp_writer_size(&w) > QACT_PACKET_LIMIT) goto done;
    }
    if (bf_mp_pack_str_cstr(&w, "counts") || bf_mp_pack_map(&w, 3) || bf_mp_pack_kv_int(&w, "active", counts[0]) ||
        bf_mp_pack_kv_int(&w, "attention", counts[1]) || bf_mp_pack_kv_int(&w, "completed", counts[2]) || bf_mp_pack_kv_bool(&w, "more", more)) goto done;
    ok = fwrite(bf_mp_writer_data(&w), 1, bf_mp_writer_size(&w), out) == bf_mp_writer_size(&w) && !ferror(out);
done:
    sqlite3_finalize(q);
    if (db) { awk_exec(db, "ROLLBACK"); sqlite3_close(db); }
    bf_mp_writer_free(&w);
    return ok ? 0 : 1;
}

/* bonfyre-queue account-work index|read --db INDEX --subject S --scope SCOPE
 *   [--owner SUBJECT --source KIND=PATH]... [--mission ID] [--cursor N]
 * The sources are this machine's own work, so they index only under --owner;
 * `read` refreshes a stale index only when the reader is that owner. */
/* Scoped, read-only provider-reported token coverage: no invented pricing. */
static int awk_usage_read(const char *path,const char *subject,const char *scope,const char *mission){
    sqlite3 *db=NULL;sqlite3_stmt *q=NULL;int ok=0;
    if(!qact_scope_valid(subject,scope)||!qact_atom(mission,127))return 2;
    if(sqlite3_open_v2(path,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)goto done;
    sqlite3_busy_timeout(db,1000);
    if(sqlite3_prepare_v2(db,
        "SELECT (SELECT count(*) FROM account_work_run WHERE work_id=a.id),"
        "(SELECT count(*) FROM account_work_usage WHERE work_id=a.id),"
        "(SELECT sum(input_tokens) FROM account_work_usage WHERE work_id=a.id),"
        "(SELECT sum(output_tokens) FROM account_work_usage WHERE work_id=a.id)"
        " FROM account_work a WHERE a.id=?1 AND a.subject=?2 AND a.scope=?3",
        -1,&q,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(q,1,mission,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(q,2,subject,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(q,3,scope,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(q)!=SQLITE_ROW)goto done;
    long long runs=sqlite3_column_int64(q,0),observed=sqlite3_column_int64(q,1);
    printf("task\t%s\nruns\t%lld\nreported_runs\t%lld\ncoverage\t%s\n",
           mission,runs,observed,!runs?"no-runs":!observed?"unmeasured":runs==observed?"complete":"partial");
    if(observed)printf("reported_input_tokens\t%lld\nreported_output_tokens\t%lld\n",
                       (long long)sqlite3_column_int64(q,2),(long long)sqlite3_column_int64(q,3));
    else fputs("reported_input_tokens\tunknown\nreported_output_tokens\tunknown\n",stdout);
    ok=1;
done:
    sqlite3_finalize(q);sqlite3_close(db);return ok?0:1;
}

/* Read-only account coverage: exact scoped aggregates, never titles or bodies. */
static int awk_audit_read(const char *path,const char *subject,const char *scope) {
    sqlite3 *db=NULL;sqlite3_stmt *q=NULL;int ok=0;
    if(!qact_scope_valid(subject,scope))return 2;
    if(sqlite3_open_v2(path,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)goto done;
    sqlite3_busy_timeout(db,1000);
    const char *sql=
        "SELECT (SELECT value FROM account_work_meta WHERE key='indexed_ms'),"
        "(SELECT count(*) FROM account_work WHERE subject=?1 AND scope=?2),"
        "(SELECT count(*) FROM account_work WHERE subject=?1 AND scope=?2 AND state='complete'),"
        "(SELECT count(*) FROM account_work WHERE subject=?1 AND scope=?2 AND state IN('running','retry_wait','blocked')),"
        "(SELECT count(*) FROM account_work WHERE subject=?1 AND scope=?2 AND source='Aurekai assistant'),"
        "(SELECT count(*) FROM account_work WHERE subject=?1 AND scope=?2 AND source='fabric'),"
        "(SELECT count(*) FROM account_work WHERE subject=?1 AND scope=?2 AND source='workgraph'),"
        "(SELECT count(*) FROM account_work_run r JOIN account_work a ON a.id=r.work_id WHERE a.subject=?1 AND a.scope=?2),"
        "(SELECT count(*) FROM account_work_usage u JOIN account_work a ON a.id=u.work_id WHERE a.subject=?1 AND a.scope=?2),"
        "(SELECT count(*) FROM account_work_step s JOIN account_work a ON a.id=s.work_id "
        "WHERE a.subject=?1 AND a.scope=?2 AND s.state='complete' AND s.output_ref<>''),"
        "(SELECT count(*) FROM account_work WHERE subject=?1 AND scope=?2 AND "
        "updated_ms>=?3 AND state NOT IN('ready','defined') AND NOT (source='Aurekai assistant' AND state='cancelled'))";
    if(sqlite3_prepare_v2(db,sql,-1,&q,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(q,1,subject,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(q,2,scope,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int64(q,3,qact_now()-INT64_C(604800000));
    if(sqlite3_step(q)!=SQLITE_ROW || sqlite3_column_type(q,0)==SQLITE_NULL)goto done;
    static const char *keys[]={"work","completed_work","work_needing_attention",
        "operator_work","fabric_work","workgraph_work","runs",
        "reported_token_runs","completed_output_references","inbox_eligible"};
    long long indexed=(long long)sqlite3_column_int64(q,0);
    printf("account\t%s\nscope\t%s\nindexed_ms\t%lld\n",subject,scope,indexed);
    printf("freshness\t%s\n",qact_now()-indexed>AWK_STALE_MS?"stale":"fresh");
    for(size_t i=0;i<sizeof(keys)/sizeof(keys[0]);i++)
        printf("%s\t%lld\n",keys[i],(long long)sqlite3_column_int64(q,(int)i+1));
    /* Report missing Meter coverage explicitly; absent usage is not zero cost. */
    sqlite3_int64 runs=sqlite3_column_int64(q,7),reported=sqlite3_column_int64(q,8);
    if(reported>runs || runs<0 || reported<0)goto done;
    printf("unreported_token_runs\t%lld\n",(long long)(runs-reported));
    printf("token_coverage\t%s\n",runs==0?"no_runs":
        reported==runs?"complete":reported==0?"unmeasured":"partial");
    ok=1;
done:
    sqlite3_finalize(q);if(db)sqlite3_close(db);
    if(!ok)fputs("account-work: scoped audit unavailable\n",stderr);
    return ok?0:1;
}

#include "account_compute.h"

static inline int awk_cli(int argc, char **argv) {
    const char *db = NULL, *subject = NULL, *scope = NULL, *mission = NULL, *cursor = NULL, *owner = NULL;
    AwkSource sources[AWK_SOURCE_LIMIT];
    char kinds[AWK_SOURCE_LIMIT][16];
    size_t count = 0;
    if (argc < 3 || (strcmp(argv[2], "index") && strcmp(argv[2], "read") && strcmp(argv[2], "outputs") && strcmp(argv[2], "usage") && strcmp(argv[2], "audit") && strcmp(argv[2], "agents") && strcmp(argv[2], "compute"))) return 2;
    for (int i = 3; i + 1 < argc; i += 2) {
        const char *key = argv[i], *value = argv[i + 1];
        if (!strcmp(key, "--db")) db = value; else if (!strcmp(key, "--subject")) subject = value;
        else if (!strcmp(key, "--scope")) scope = value; else if (!strcmp(key, "--mission")) mission = value;
        else if (!strcmp(key, "--cursor")) cursor = value; else if (!strcmp(key, "--owner")) owner = value;
        else if (!strcmp(key, "--source")) {
            const char *eq = strchr(value, '=');
            if (!eq || eq == value || (size_t)(eq - value) >= sizeof(kinds[0]) || eq[1] != '/' || count == AWK_SOURCE_LIMIT) return 2;
            memcpy(kinds[count], value, (size_t)(eq - value)); kinds[count][eq - value] = 0;
            sources[count].kind = kinds[count]; sources[count].path = eq + 1; count++;
        } else return 2;
    }
    if (!db || !subject || !scope || (count && !owner)) return 2;
    if (!strcmp(argv[2],"audit")) return count||owner||mission||cursor?2:awk_audit_read(db,subject,scope);
    if (!strcmp(argv[2], "index")) return owner && strcmp(owner, subject) ? 2 : awk_index(db, subject, scope, sources, count);
    int64_t value = 0;
    if (cursor) {
        if (!*cursor || strlen(cursor) > 16) return 2;
        for (const char *p = cursor; *p; p++) { if (*p < '0' || *p > '9') return 2; value = value * 10 + (*p - '0'); }
    }
    if (!strcmp(argv[2],"compute")) {
        const char *meter=NULL;
        if (mission||cursor||!owner||strcmp(owner,subject)) return 2;
        for(size_t i=0;i<count;i++)if(!strcmp(sources[i].kind,"meter")){
            if(meter)return 2;
            meter=sources[i].path;
        }
        return meter?awk_compute_read(meter,subject,scope,stdout):2;
    }
    if (count && !strcmp(owner, subject) && !strcmp(scope, "self") && awk_stale(db) && awk_index(db, subject, scope, sources, count)) return 1;
    if (!strcmp(argv[2],"usage")) return mission ? awk_usage_read(db,subject,scope,mission) : 2;
    if (!strcmp(argv[2],"agents")) return mission || cursor ? 2 : awk_agents(db, subject, scope, stdout);
    if (!strcmp(argv[2],"outputs") && mission) return 2;
    return awk_read(db, subject, scope, mission, value, !strcmp(argv[2],"outputs"), stdout);
}

#endif
