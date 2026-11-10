#ifndef BONFYRE_WORK_PROVENANCE_H
#define BONFYRE_WORK_PROVENANCE_H
/* One account task told as evidence, and the account's kept outputs.
 *
 * The task view under /work-board?task= reads the account work index the
 * activity rail already uses: every run (agent, role, session, wall time,
 * limit, reported usage including cache, whether a session key exists),
 * every recorded step split into
 * verified, failed and still open, and every output the owner kept as a
 * digest. Token records come from Meter's work-tokens read, which joins only
 * on the exact provider session a run names. /work-board/outputs lists the
 * same digest references across the whole account.
 *
 * Nothing here starts, retries or closes work, opens a receipt file, or turns
 * a digest into a link. A task outside the signed-in account's index shows
 * nothing. A missing counter is "not reported", never zero. */

#define WP_RUNS 40
#define WP_STEPS 120
#define WP_OUTPUTS 80

static void wp_num(long long n){
    char digits[32],out[48];int len=snprintf(digits,sizeof(digits),"%lld",n<0?-n:n),o=0;
    if(n<0)out[o++]='-';
    for(int i=0;i<len;i++){out[o++]=digits[i];if((len-i-1)%3==0&&i<len-1)out[o++]=',';}
    out[o]=0;fputs(out,stdout);
}
static void wp_wall(long long ms){
    long long s=ms/1000;
    if(ms<1000)fputs("under a second",stdout);
    else if(s<60)printf("%lld s",s);
    else if(s<3600)printf("%lld min %lld s",s/60,s%60);
    else printf("%lld h %lld min",s/3600,(s%3600)/60);
}
static void wp_when(long long ms){
    time_t t=(time_t)(ms/1000);struct tm tm;char day[40];if(ms<=0||!gmtime_r(&t,&tm))return;
    strftime(day,sizeof(day),"%d %b %Y, %H:%M UTC",&tm);printf("<time datetime=\"%lld\">",ms);html(day);fputs("</time>",stdout);
}
/* Agent names as people say them; anything else is shown as the owner wrote it. */
static void wp_agent(const char *agent){
    if(!agent||!*agent)fputs("Unnamed worker",stdout);
    else if(!strcmp(agent,"claude"))fputs("Claude",stdout);
    else if(!strcmp(agent,"codex"))fputs("Codex",stdout);
    else if(!strcmp(agent,"fabric"))fputs("Agent fabric",stdout);
    else if(!strcmp(agent,"workgraph"))fputs("Pipeline",stdout);
    else html(agent);
}
static void wp_words(const char *s){
    char out[160];size_t n=0;int first=1;
    for(const char *p=s?s:"";*p&&n+1<sizeof(out);p++){char c=*p;if(c=='_'||c=='-'||c==':'||c=='.')c=' ';if(first&&c>='a'&&c<='z')c=(char)(c-'a'+'A');first=0;out[n++]=c;}
    out[n]=0;html(out);
}
static const char *wp_run_words(const char *state,int unconfirmed){
    if(unconfirmed)return "Still marked running after the work ended";
    if(!strcmp(state,"completed"))return "Finished";
    if(!strcmp(state,"failed"))return "Failed";
    if(!strcmp(state,"running"))return "Running now";
    return "Waiting";
}
static const char *wp_task_words(const char *state){
    if(!strcmp(state,"complete"))return "Finished";
    if(!strcmp(state,"running"))return "Being worked on now";
    if(!strcmp(state,"retry_wait"))return "Waiting to try again";
    if(!strcmp(state,"blocked"))return "Stopped until someone looks at it";
    if(!strcmp(state,"dead_letter"))return "Failed";
    if(!strcmp(state,"cancelled"))return "Cancelled";
    return "Waiting its turn";
}
/* verified, failed, open: how a step's recorded state counts. */
static int wp_step_kind(const char *state){
    if(!strcmp(state,"complete"))return 0;
    if(!strcmp(state,"dead_letter")||!strcmp(state,"blocked")||!strcmp(state,"cancel_requested"))return 1;
    return 2;
}
static const char *wp_step_words(const char *state){
    if(!strcmp(state,"complete"))return "verified";
    if(!strcmp(state,"dead_letter"))return "failed";
    if(!strcmp(state,"blocked"))return "refused or stopped";
    if(!strcmp(state,"cancel_requested"))return "outcome uncertain";
    if(!strcmp(state,"cancelled"))return "undone";
    if(!strcmp(state,"running"))return "running";
    return "not started";
}
/* An output reference as a fingerprint. Fabric receipts arrive as sha256:HEX;
 * any other owner reference (a workgraph output address) is hashed so its
 * location never reaches the page. */
static void wp_digest(const char *ref,char hex[65]){
    if(!strncmp(ref,"sha256:",7)&&strlen(ref+7)==64&&strspn(ref+7,"0123456789abcdef")==64){memcpy(hex,ref+7,65);return;}
    bf_sha256_hex((const uint8_t *)ref,strlen(ref),hex);
}
static void wp_fingerprint(const char *hex){
    fputs("<code class=\"wp-digest\" title=\"sha256:",stdout);html(hex);fputs("\" data-digest=\"",stdout);html(hex);fputs("\">sha256:",stdout);
    printf("%.12s…%.6s",hex,hex+58);fputs("</code>",stdout);
}
static sqlite3 *wp_index(void){
    const char *index=getenv("BONFYRE_ACCOUNT_WORK_DB");sqlite3 *db=NULL;
    if(!index||*index!='/'||sqlite3_open_v2(index,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){sqlite3_close(db);return NULL;}
    sqlite3_busy_timeout(db,1000);return db;
}
static void wp_style(void){
    fputs("<style id=\"work-provenance\">"
    ".wp{--wp-ink:#203023;--wp-forest:#173d33;--wp-lime:#b5df32;--wp-signal:#c2412a;--wp-muted:#5d6b58;--wp-rule:#2030232b;margin:34px 0 10px;max-width:1100px;color:var(--wp-ink)}"
    ".wp>header h2{font:400 clamp(30px,3.4vw,44px)/1.02 'Instrument Serif',Georgia,serif;letter-spacing:-.03em;margin:0 0 8px}"
    ".wp-kicker{font:500 11px/1.4 'DM Mono',monospace;letter-spacing:.14em;text-transform:uppercase;color:var(--wp-muted);margin:0 0 8px}"
    ".wp-sum{display:flex;flex-wrap:wrap;gap:0;margin:16px 0 0;padding:14px 0 0;border-top:2px solid var(--wp-ink)}"
    ".wp-sum>div{padding:0 24px 0 0;margin:0 24px 8px 0;border-right:1px solid var(--wp-rule)}.wp-sum>div:last-child{border:0}"
    ".wp-sum dt{font:500 10px/1.4 'DM Mono',monospace;letter-spacing:.12em;text-transform:uppercase;color:var(--wp-muted)}.wp-sum dd{margin:2px 0 0;font:400 30px/1 'Instrument Serif',Georgia,serif}"
    ".wp-sum dd[data-tone=failed]{color:var(--wp-signal)}"
    ".wp h3{display:flex;align-items:center;gap:9px;font:600 1.05rem/1.3 'DM Sans',system-ui,sans-serif;margin:34px 0 4px}.wp h3 .ak-icon{width:19px;height:19px}"
    ".wp-note{margin:0 0 10px;color:var(--wp-muted);max-width:68ch;line-height:1.5;font-size:.93rem}"
    ".wp-list{list-style:none;margin:0;padding:0;border-top:1px solid var(--wp-ink)}"
    ".wp-run{display:grid;grid-template-columns:minmax(150px,1.1fr) minmax(0,2fr) minmax(120px,.8fr);gap:4px 22px;padding:14px 0;border-bottom:1px solid var(--wp-rule);align-items:baseline}"
    ".wp-run strong{font-size:1.02rem}.wp-run small{display:block;color:var(--wp-muted);font-size:.84rem;margin-top:2px}"
    ".wp-run [data-run-state]{font:500 11px/1.4 'DM Mono',monospace;letter-spacing:.08em;text-transform:uppercase}"
    ".wp-run[data-state=failed] [data-run-state],.wp-run[data-unconfirmed] [data-run-state]{color:var(--wp-signal)}.wp-run[data-state=completed] [data-run-state]{color:var(--wp-forest)}"
    ".wp-tokens{margin:0;font-size:.9rem;line-height:1.55}.wp-tokens span{white-space:nowrap}.wp-tokens span:not(:last-child):after{content:' ·';color:var(--wp-muted)}"
    ".wp-limit{display:inline-block;margin-top:4px;color:var(--wp-signal);font-size:.86rem}"
    ".wp-wall{text-align:right;font-variant-numeric:tabular-nums}.wp-wall small{text-align:right}"
    ".wp-steps{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:0 28px;list-style:none;margin:0;padding:0;border-top:1px solid var(--wp-ink)}"
    ".wp-steps li{display:flex;justify-content:space-between;gap:12px;padding:9px 0;border-bottom:1px solid var(--wp-rule);font-size:.92rem}"
    ".wp-steps li span:last-child{font:500 11px/1.6 'DM Mono',monospace;letter-spacing:.06em;text-transform:uppercase;white-space:nowrap}"
    ".wp-steps li[data-step=failed] span:last-child{color:var(--wp-signal)}.wp-steps li[data-step=verified] span:last-child{color:var(--wp-forest)}"
    ".wp-out{display:grid;grid-template-columns:minmax(0,1.4fr) minmax(0,1fr) auto;gap:4px 22px;padding:13px 0;border-bottom:1px solid var(--wp-rule);align-items:baseline}"
    ".wp-out a{font-weight:600;text-decoration:none;overflow-wrap:anywhere}.wp-out a:hover,.wp-out a:focus-visible{text-decoration:underline}.wp-out small{display:block;color:var(--wp-muted);font-size:.84rem}"
    ".wp-digest{font:500 12px/1.5 'DM Mono',monospace;overflow-wrap:anywhere;user-select:all}"
    ".wp-models{width:100%;min-width:600px;border-collapse:collapse;font-size:.88rem;font-variant-numeric:tabular-nums;border-top:1px solid var(--wp-ink)}"
    ".wp-models th,.wp-models td{text-align:right;padding:9px 8px;border-bottom:1px solid var(--wp-rule);white-space:nowrap}.wp-models th:first-child,.wp-models td:first-child{text-align:left;white-space:normal;min-width:10em}"
    ".wp-models th{font:500 10px/1.4 'DM Mono',monospace;letter-spacing:.1em;text-transform:uppercase;color:var(--wp-muted)}"
    ".wp-scroll{overflow-x:auto}"
    ".wp-next{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0 0}.wp-next a{display:inline-flex;align-items:center;gap:7px;min-height:44px;padding:0 16px;border-radius:999px;border:1px solid var(--wp-ink);color:var(--wp-ink);text-decoration:none;font-size:.92rem}"
    ".wp-next a:first-child{background:var(--wp-forest);border-color:var(--wp-forest);color:#f5f7ee}.wp-next a:hover,.wp-next a:focus-visible{background:var(--wp-lime);border-color:var(--wp-lime);color:#111913}.wp-next .ak-icon{width:16px;height:16px}"
    ".wp-empty{padding:12px 0;color:var(--wp-muted);border-bottom:1px solid var(--wp-rule);margin:0}"
    "@media(max-width:720px){.wp-run,.wp-out{grid-template-columns:1fr}.wp-wall,.wp-wall small{text-align:left}.wp-sum>div{padding-right:16px;margin-right:16px}.wp-sum dd{font-size:25px}}"
    "</style>",stdout);
}

/* ---------- Meter token records for this task ---------- */

typedef struct {char provider[48],model[80];long long records,sessions,uncached,cached,write,output,reasoning;int reasoning_known;} WpModel;
static long long wp_field_num(const char *s){return s&&strspn(s,"0123456789")==strlen(s)&&*s?strtoll(s,NULL,10):-1;}
static void wp_meter(const char *task,const char *subject,const char *scope){
    const char *meter_db=getenv("BONFYRE_METER_DB"),*index=getenv("BONFYRE_ACCOUNT_WORK_DB"),*bin=getenv("BONFYRE_METER_BINARY"),*bindir=getenv("BONFYRE_BIN_DIR");
    char binary[PATH_MAX];unsigned char *out=NULL;size_t size=0;
    fputs("<h3 id=\"tokens\">",stdout);page_glyph("cost");fputs("Tokens the meter recorded</h3>",stdout);
    if(bin&&*bin=='/')snprintf(binary,sizeof(binary),"%s",bin);
    else if(snprintf(binary,sizeof(binary),"%s/bonfyre-meter",bindir&&*bindir=='/'?bindir:"/usr/local/bin")>=(int)sizeof(binary))binary[0]=0;
    if(!meter_db||*meter_db!='/'||!index||*index!='/'||!*binary||access(binary,X_OK)){
        fputs("<p class=\"wp-note\" data-meter=\"unavailable\">Token records are not connected on this site, so the counts above are only what each run reported itself.</p>",stdout);return;}
    char *args[]={binary,"work-tokens","--db",(char*)meter_db,"--work-db",(char*)index,"--subject",(char*)subject,"--scope",(char*)scope,"--mission",(char*)task,NULL};
    int rc=aw_native_result(args,NULL,0,&out,&size,1);
    if(rc||!out){
        fputs(rc==2?"<p class=\"wp-note\" data-meter=\"refused\">Token records could not be tied to this work. Check account access and try again.</p>"
                   :"<p class=\"wp-note\" data-meter=\"unavailable\">Token records could not be read. Try again; the available run counts are shown above.</p>",stdout);
        free(out);return;}
    out[size]=0;
    long long runs=-1,nokey=0,exact=0,shared=0,evidenced=0,without=0,failed_used=0,records=0,valued=0,unvalued=0;char coverage[32]="",price[32]="";
    WpModel models[12];int model_count=0;
    for(char *save=NULL,*line=strtok_r((char*)out,"\n",&save);line;line=strtok_r(NULL,"\n",&save)){
        char *cell[14];int n=0;for(char *p=line;n<14;){cell[n++]=p;char *tab=strchr(p,'\t');if(!tab)break;*tab=0;p=tab+1;}
        if(n<2)continue;const char *k=cell[0],*v=cell[1];
        if(!strcmp(k,"model")&&n>=12&&model_count<12){WpModel *m=&models[model_count++];snprintf(m->provider,sizeof(m->provider),"%s",cell[1]);snprintf(m->model,sizeof(m->model),"%s",cell[2]);
            m->records=wp_field_num(cell[3]);m->sessions=wp_field_num(cell[4]);m->uncached=wp_field_num(cell[5]);m->cached=wp_field_num(cell[6]);m->write=wp_field_num(cell[7]);m->output=wp_field_num(cell[8]);
            m->reasoning=wp_field_num(cell[9]);m->reasoning_known=m->reasoning>=0;continue;}
        if(!strcmp(k,"runs"))runs=wp_field_num(v);else if(!strcmp(k,"runs_without_session_key"))nokey=wp_field_num(v);else if(!strcmp(k,"sessions_exact"))exact=wp_field_num(v);
        else if(!strcmp(k,"sessions_shared_refused"))shared=wp_field_num(v);else if(!strcmp(k,"sessions_with_token_evidence"))evidenced=wp_field_num(v);
        else if(!strcmp(k,"sessions_without_token_evidence"))without=wp_field_num(v);else if(!strcmp(k,"failed_runs_with_token_evidence"))failed_used=wp_field_num(v);
        else if(!strcmp(k,"records"))records=wp_field_num(v);else if(!strcmp(k,"valued_records"))valued=wp_field_num(v);else if(!strcmp(k,"unvalued_records"))unvalued=wp_field_num(v);
        else if(!strcmp(k,"valuation_coverage"))snprintf(coverage,sizeof(coverage),"%s",v);else if(!strcmp(k,"provisional_valuation_usd"))snprintf(price,sizeof(price),"%s",v);
    }
    free(out);
    if(runs<0){fputs("<p class=\"wp-note\" data-meter=\"unavailable\">The token record response could not be read. Try again.</p>",stdout);return;}
    fputs("<p class=\"wp-note\" data-meter=\"read\">Matched only by the exact session each run names, never by time or title. ",stdout);
    if(exact){wp_num(evidenced);fputs(" of ",stdout);wp_num(exact);fputs(exact==1?" session on this work has token records.":" sessions on this work have token records.",stdout);}
    else fputs("No run on this work names a session the meter can match.",stdout);
    if(nokey>0){fputs(" ",stdout);wp_num(nokey);fputs(nokey==1?" run started without a session key, so nothing is matched to it.":" runs started without a session key, so nothing is matched to them.",stdout);}
    if(shared>0){fputs(" ",stdout);wp_num(shared);fputs(shared==1?" session also appears on other work and is left out rather than split.":" sessions also appear on other work and are left out rather than split.",stdout);}
    if(failed_used>0){fputs(" ",stdout);wp_num(failed_used);fputs(failed_used==1?" failed run still used tokens.":" failed runs still used tokens.",stdout);}
    (void)without;fputs("</p>",stdout);
    if(model_count){
        fputs("<div class=\"wp-scroll\"><table class=\"wp-models\" data-meter-models><thead><tr><th scope=\"col\">Model</th><th scope=\"col\">Records</th><th scope=\"col\">Fresh input</th><th scope=\"col\">Cached input</th><th scope=\"col\">Cache writes</th><th scope=\"col\">Output</th><th scope=\"col\">Reasoning</th></tr></thead><tbody>",stdout);
        for(int i=0;i<model_count;i++){const WpModel *m=&models[i];fputs("<tr><th scope=\"row\">",stdout);wp_agent(m->provider);fputs(" <small>",stdout);html(m->model);fputs("</small></th>",stdout);
            long long cells[]={m->records,m->uncached,m->cached,m->write,m->output};for(size_t c=0;c<5;c++){fputs("<td>",stdout);if(cells[c]>=0)wp_num(cells[c]);else fputs("not reported",stdout);fputs("</td>",stdout);}
            fputs("<td>",stdout);if(m->reasoning_known)wp_num(m->reasoning);else fputs("not reported",stdout);fputs("</td></tr>",stdout);}
        fputs("</tbody></table></div>",stdout);
    }
    fputs("<p class=\"wp-note\" data-valuation=\"",stdout);html(coverage);fputs("\">",stdout);
    if(!records)fputs("No token records, so nothing to price.",stdout);
    else if(valued>0&&*price&&strcmp(price,"unknown")){fputs("At the rates admitted for these models, ",stdout);wp_num(valued);fputs(" of ",stdout);wp_num(valued+unvalued);double usd=strtod(price,NULL);if(usd<0.005)fputs(" records come to less than one cent",stdout);else{fputs(" records come to about $",stdout);printf("%.2f",usd);}fputs(". That is an estimate, not a charge.",stdout);}
    else fputs("No admitted rate covers these records, so no price is shown. Unknown is not zero.",stdout);
    fputs(" Nothing here is a bill.</p>",stdout);
}

/* ---------- the task view ---------- */

static void ac_work_provenance(const char *context,const char *task,const char *subject,const char *scope){
    sqlite3 *db;sqlite3_stmt *s=NULL;char title[256]="",state[32]="",source[64]="";long long updated=0;
    if(!task||!*task||!ac_word(task)||!(db=wp_index()))return;
    if(sqlite3_prepare_v2(db,"SELECT title,state,source,updated_ms FROM account_work WHERE id=?1 AND subject=?2 AND scope=?3",-1,&s,NULL)!=SQLITE_OK){sqlite3_close(db);return;}
    sqlite3_bind_text(s,1,task,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,subject,-1,SQLITE_STATIC);sqlite3_bind_text(s,3,scope,-1,SQLITE_STATIC);
    if(sqlite3_step(s)!=SQLITE_ROW){sqlite3_finalize(s);sqlite3_close(db);return;}
    snprintf(title,sizeof(title),"%s",sqlite3_column_text(s,0)?(const char*)sqlite3_column_text(s,0):"");snprintf(state,sizeof(state),"%s",sqlite3_column_text(s,1)?(const char*)sqlite3_column_text(s,1):"");
    snprintf(source,sizeof(source),"%s",sqlite3_column_text(s,2)?(const char*)sqlite3_column_text(s,2):"");updated=sqlite3_column_int64(s,3);
    sqlite3_finalize(s);s=NULL;
    long long total_runs=0,failed_runs=0,wall=0,steps[3]={0,0,0},outputs=0;int walls=0;
    if(sqlite3_prepare_v2(db,"SELECT count(*),COALESCE(sum(state='failed'),0),COALESCE(sum(duration_ms),0),count(duration_ms) FROM account_work_run WHERE work_id=?1",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,task,-1,SQLITE_STATIC);if(sqlite3_step(s)==SQLITE_ROW){total_runs=sqlite3_column_int64(s,0);failed_runs=sqlite3_column_int64(s,1);wall=sqlite3_column_int64(s,2);walls=sqlite3_column_int(s,3);}}
    sqlite3_finalize(s);s=NULL;
    if(sqlite3_prepare_v2(db,"SELECT state,count(*),COALESCE(sum(state='complete' AND output_ref<>''),0) FROM account_work_step WHERE work_id=?1 GROUP BY state",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,task,-1,SQLITE_STATIC);
        while(sqlite3_step(s)==SQLITE_ROW){const char *st=(const char*)sqlite3_column_text(s,0);if(!st)continue;steps[wp_step_kind(st)]+=sqlite3_column_int64(s,1);outputs+=sqlite3_column_int64(s,2);}}
    sqlite3_finalize(s);s=NULL;
    wp_style();
    fputs("<section class=\"wp\" id=\"provenance\" data-work-provenance data-task=\"",stdout);html(task);fputs("\" data-task-state=\"",stdout);html(state);fputs("\" aria-labelledby=\"wp-title\"><header><p class=\"wp-kicker\">",stdout);
    html(wp_task_words(state));if(updated>0){fputs(" · last change ",stdout);wp_when(updated);}fputs("</p><h2 id=\"wp-title\">",stdout);
    /* Assistant goals already carry their own heading in the focus above. */
    if(strncmp(task,"account-operator-",17)&&*title){html(title);}else fputs("How this work was done",stdout);
    fputs("</h2><dl class=\"wp-sum\"><div><dt>Runs</dt><dd data-sum=\"runs\">",stdout);wp_num(total_runs);fputs("</dd></div><div><dt>Failed runs</dt><dd data-sum=\"failed-runs\"",stdout);if(failed_runs)fputs(" data-tone=\"failed\"",stdout);fputs(">",stdout);wp_num(failed_runs);
    fputs("</dd></div><div><dt>Agent time</dt><dd data-sum=\"wall\">",stdout);if(walls)wp_wall(wall);else fputs("not recorded",stdout);
    fputs("</dd></div><div><dt>Verified steps</dt><dd data-sum=\"verified\">",stdout);wp_num(steps[0]);fputs("</dd></div><div><dt>Failed steps</dt><dd data-sum=\"failed-steps\"",stdout);if(steps[1])fputs(" data-tone=\"failed\"",stdout);fputs(">",stdout);wp_num(steps[1]);
    fputs("</dd></div><div><dt>Outputs kept</dt><dd data-sum=\"outputs\">",stdout);wp_num(outputs);fputs("</dd></div></dl></header>",stdout);

    /* Who worked on it. */
    fputs("<h3 id=\"runs\">",stdout);page_glyph("robot");fputs("Who worked on it</h3><p class=\"wp-note\">Each run as its owner recorded it. Tokens are what the run itself reported when it ended.</p>",stdout);
    if(sqlite3_prepare_v2(db,"SELECT r.run_id,r.agent,r.worker_id,r.role,r.session_kind,r.state,r.created_ms,r.duration_ms,r.limit_signal,r.provider_session_ref,"
        "r.state='running' AND a.state IN('complete','dead_letter','cancelled'),u.run_id IS NOT NULL,u.input_tokens,u.output_tokens,u.cache_read_tokens,u.cache_write_tokens,u.estimated_micro_usd "
        "FROM account_work_run r JOIN account_work a ON a.id=r.work_id LEFT JOIN account_work_usage u ON u.work_id=r.work_id AND u.run_id=r.run_id "
        "WHERE a.id=?1 AND a.subject=?2 AND a.scope=?3 ORDER BY r.created_ms,r.run_id LIMIT ?4",-1,&s,NULL)!=SQLITE_OK){
        fputs("<p class=\"wp-empty\" data-runs=\"unavailable\">Run details are not in this index yet.</p>",stdout);
    }else{
        int shown=0;sqlite3_bind_text(s,1,task,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,subject,-1,SQLITE_STATIC);sqlite3_bind_text(s,3,scope,-1,SQLITE_STATIC);sqlite3_bind_int(s,4,WP_RUNS);
        while(sqlite3_step(s)==SQLITE_ROW){
            const char *agent=(const char*)sqlite3_column_text(s,1),*worker=(const char*)sqlite3_column_text(s,2),*role=(const char*)sqlite3_column_text(s,3),*kind=(const char*)sqlite3_column_text(s,4),*rs=(const char*)sqlite3_column_text(s,5),*session=(const char*)sqlite3_column_text(s,9);
            int unconfirmed=sqlite3_column_int(s,10),reported=sqlite3_column_int(s,11);if(!rs)continue;
            if(!shown)fputs("<ol class=\"wp-list\" data-runs>",stdout);shown++;
            fputs("<li class=\"wp-run\" data-state=\"",stdout);html(rs);fputs("\"",stdout);if(unconfirmed)fputs(" data-unconfirmed",stdout);fputs("><div><strong>",stdout);wp_agent(agent&&*agent?agent:worker);fputs("</strong>",stdout);
            if((role&&*role)||(kind&&*kind)){fputs("<small>",stdout);if(role&&*role)wp_words(role);if(role&&*role&&kind&&*kind)fputs(" · ",stdout);if(kind&&*kind){wp_words(kind);fputs(" session",stdout);}fputs("</small>",stdout);}
            /* The session id is a Meter join key only; the page says it exists, never what it is. */
            if(session&&*session)fputs("<small data-session>Session recorded for token matching</small>",stdout);
            fputs("</div><div><span data-run-state>",stdout);fputs(wp_run_words(rs,unconfirmed),stdout);fputs("</span>",stdout);
            if(reported){
                fputs("<p class=\"wp-tokens\" data-usage>",stdout);static const char *const names[]={" in"," out"," read from cache"," written to cache"};int any=0;
                for(int c=0;c<4;c++)if(sqlite3_column_type(s,12+c)==SQLITE_INTEGER){fputs("<span>",stdout);wp_num(sqlite3_column_int64(s,12+c));fputs(names[c],stdout);fputs("</span> ",stdout);any=1;}
                if(sqlite3_column_type(s,16)==SQLITE_INTEGER){fputs("<span>about $",stdout);printf("%.2f",(double)sqlite3_column_int64(s,16)/1000000.0);fputs(" estimated</span> ",stdout);}
                if(!any)fputs("<span>Tokens not reported</span>",stdout);fputs("</p>",stdout);
            }else fputs("<p class=\"wp-tokens\" data-usage=\"unreported\"><span>Tokens not reported</span></p>",stdout);
            if(sqlite3_column_int(s,8))fputs("<span class=\"wp-limit\" data-limit>Stopped at the provider's usage limit</span>",stdout);
            fputs("</div><div class=\"wp-wall\">",stdout);if(sqlite3_column_type(s,7)==SQLITE_INTEGER){fputs("<span data-wall>",stdout);wp_wall(sqlite3_column_int64(s,7));fputs("</span>",stdout);}else fputs("<span>time not recorded</span>",stdout);
            fputs("<small>",stdout);wp_when(sqlite3_column_int64(s,6));fputs("</small></div></li>",stdout);
        }
        if(shown)fputs("</ol>",stdout);else fputs("<p class=\"wp-empty\" data-runs=\"none\">No runs are recorded for this work.</p>",stdout);
        if(total_runs>shown&&shown){fputs("<p class=\"wp-note\">Showing the first ",stdout);wp_num(shown);fputs(" of ",stdout);wp_num(total_runs);fputs(" runs.</p>",stdout);}
    }
    sqlite3_finalize(s);s=NULL;

    /* What it did: failed steps first, then the rest in order. */
    fputs("<h3 id=\"steps\">",stdout);page_glyph("needs");fputs("What it did</h3>",stdout);
    if(sqlite3_prepare_v2(db,"SELECT node_id,operator_id,state,attempt FROM account_work_step WHERE work_id=?1 ORDER BY state NOT IN('dead_letter','blocked','cancel_requested'),updated_ms,node_id LIMIT ?2",-1,&s,NULL)==SQLITE_OK){
        int shown=0;sqlite3_bind_text(s,1,task,-1,SQLITE_STATIC);sqlite3_bind_int(s,2,WP_STEPS);
        while(sqlite3_step(s)==SQLITE_ROW){const char *op=(const char*)sqlite3_column_text(s,1),*st=(const char*)sqlite3_column_text(s,2);int attempt=sqlite3_column_int(s,3);if(!st)continue;
            if(!shown){fputs("<p class=\"wp-note\">Verified means the step's owner recorded it as done. A failed step names what to look at first.</p><ul class=\"wp-steps\" data-steps>",stdout);}shown++;
            int k=wp_step_kind(st);fputs("<li data-step=\"",stdout);fputs(k==0?"verified":k==1?"failed":"open",stdout);fputs("\"><span>",stdout);wp_words(op&&*op?op:(const char*)sqlite3_column_text(s,0));
            if(attempt>1)printf(" <small>(try %d)</small>",attempt);fputs("</span><span>",stdout);fputs(wp_step_words(st),stdout);fputs("</span></li>",stdout);}
        if(shown){fputs("</ul>",stdout);long long all=steps[0]+steps[1]+steps[2];if(all>shown){fputs("<p class=\"wp-note\">Showing ",stdout);wp_num(shown);fputs(" of ",stdout);wp_num(all);fputs(" steps.</p>",stdout);}}
        else fputs("<p class=\"wp-empty\" data-steps=\"none\">No steps are recorded for this work.</p>",stdout);
    }
    sqlite3_finalize(s);s=NULL;

    /* What it kept. */
    fputs("<h3 id=\"outputs\">",stdout);page_glyph("receipt");fputs("What it kept</h3>",stdout);
    if(sqlite3_prepare_v2(db,"SELECT operator_id,output_ref,updated_ms FROM account_work_step WHERE work_id=?1 AND state='complete' AND output_ref<>'' ORDER BY updated_ms,node_id LIMIT ?2",-1,&s,NULL)==SQLITE_OK){
        int shown=0;sqlite3_bind_text(s,1,task,-1,SQLITE_STATIC);sqlite3_bind_int(s,2,WP_OUTPUTS);
        while(sqlite3_step(s)==SQLITE_ROW){const char *op=(const char*)sqlite3_column_text(s,0),*ref=(const char*)sqlite3_column_text(s,1);char hex[65];if(!ref)continue;
            if(!shown)fputs("<p class=\"wp-note\">Each output is held by the owner that made it. Its fingerprint is here so anyone holding the file can check it is the same one; this page does not open or share the file.</p><ol class=\"wp-list\" data-outputs>",stdout);shown++;
            wp_digest(ref,hex);fputs("<li class=\"wp-out\"><div><strong>",stdout);wp_words(op);fputs("</strong><small>",stdout);fputs(!strncmp(ref,"sha256:",7)?"Receipt checked against the owner's copy when indexed":"Pipeline output",stdout);fputs("</small></div><div>",stdout);
            wp_fingerprint(hex);fputs("</div><div>",stdout);wp_when(sqlite3_column_int64(s,2));fputs("</div></li>",stdout);}
        if(shown)fputs("</ol>",stdout);else fputs("<p class=\"wp-empty\" data-outputs=\"none\">This work has not kept an output yet. Work that fails or is still running keeps none.</p>",stdout);
    }
    sqlite3_finalize(s);s=NULL;sqlite3_close(db);

    wp_meter(task,subject,scope);

    /* Where to go from here: existing owners only. */
    int stopped=!strcmp(state,"blocked")||!strcmp(state,"dead_letter")||!strcmp(state,"retry_wait");
    fputs("<h3 id=\"next\">",stdout);page_glyph("play");fputs("Pick up from here</h3><p class=\"wp-note\" data-next=\"",stdout);fputs(stopped?"stopped":!strcmp(state,"complete")?"finished":"open",stdout);fputs("\">",stdout);
    if(stopped&&steps[1])fputs("It stopped on the failed step listed first above. The agent that owns it picks it up again; this page does not restart it.",stdout);
    else if(stopped)fputs("It is stopped. The agent that owns it picks it up again; this page does not restart it.",stdout);
    else if(!strcmp(state,"complete"))fputs("It finished. What it kept stays with its owner and is listed with the rest of this account's outputs.",stdout);
    else fputs("It is still open. This page updates when its owner records the next run.",stdout);
    (void)source;fputs("</p><nav class=\"wp-next\" aria-label=\"Continue this work\">",stdout);
    if(stopped){fputs("<a href=\"/inbox?context=",stdout);url_query_value(context);fputs("\">",stdout);page_glyph("bell");fputs("See what needs you</a>",stdout);}
    fputs("<a href=\"/work-board/outputs?context=",stdout);url_query_value(context);fputs("\">",stdout);page_glyph("receipt");fputs("All outputs</a><a href=\"/s/models/all?context=",stdout);url_query_value(context);fputs("\">",stdout);page_glyph("robot");fputs("Try another model</a><a href=\"/programs?context=",stdout);url_query_value(context);fputs("\">",stdout);page_glyph("apps");fputs("Programs</a></nav></section>",stdout);
}

/* ---------- /work-board/outputs ---------- */

static int ac_work_outputs(const char *method,const char *target,const char *query,const char *bearer,const char *auth_db){
    if(strcmp(target,"/work-board/outputs"))return 0;
    if(strcmp(method,"GET")){aw_error(405,"Use GET to read this account's outputs.\n");return 1;}
    char context[96],task[128],actor[160],destination[160],back[800];const char *scope=NULL;
    if(!ac_work_board_query(query,context,task)||*task){aw_error(400,"Choose one account destination.\n");return 1;}
    int access=psetup_context(bearer,auth_db,context,0,actor,sizeof(actor),destination,sizeof(destination),&scope);
    if(access==0){snprintf(back,sizeof(back),"/work-board/outputs%s%s",*query?"?":"",query);fputs("HTTP/1.1 303 See Other\r\nLocation: /login?next=",stdout);url_query_value(back);fputs("\r\nCache-Control: private, no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);return 1;}
    if(access!=1){aw_error(403,"This account cannot open those outputs.\n");return 1;}
    psetup_start(200,"Account outputs");wp_style();
    fputs("<section class=\"wp\" data-work-outputs data-context=\"",stdout);html(context);fputs("\"><header><p class=\"wp-kicker\">Account work</p><h2>Outputs</h2><p class=\"wp-note\">What this account's agents finished and kept, newest first. Each one opens the work that made it, with who ran it, what failed along the way and the tokens it used.</p></header>",stdout);
    ac_work_board_picker(context,"",bearer,auth_db);
    sqlite3 *db=wp_index();sqlite3_stmt *s=NULL;int shown=0;
    if(!db||sqlite3_prepare_v2(db,"SELECT a.id,a.title,s.operator_id,s.output_ref,s.updated_ms FROM account_work_step s JOIN account_work a ON a.id=s.work_id "
        "WHERE a.subject=?1 AND a.scope=?2 AND s.state='complete' AND s.output_ref<>'' ORDER BY s.updated_ms DESC,a.id,s.node_id LIMIT ?3",-1,&s,NULL)!=SQLITE_OK){
        fputs("<p class=\"wp-empty\" data-outputs=\"unavailable\" role=\"status\">The account output index is not connected here, so this is not an empty list. <a href=\"/work-board?context=",stdout);url_query_value(context);fputs("\">Open the work board</a>.</p>",stdout);
    }else{
        sqlite3_bind_text(s,1,destination,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,scope?scope:"",-1,SQLITE_STATIC);sqlite3_bind_int(s,3,WP_OUTPUTS);
        while(sqlite3_step(s)==SQLITE_ROW){const char *id=(const char*)sqlite3_column_text(s,0),*title=(const char*)sqlite3_column_text(s,1),*op=(const char*)sqlite3_column_text(s,2),*ref=(const char*)sqlite3_column_text(s,3);char hex[65];
            if(!id||!ref||!ac_word(id))continue;if(!shown)fputs("<ol class=\"wp-list\" data-outputs>",stdout);shown++;wp_digest(ref,hex);
            fputs("<li class=\"wp-out\"><div><a href=\"/work-board?context=",stdout);url_query_value(context);fputs("&amp;task=",stdout);url_query_value(id);fputs("#provenance\">",stdout);html(title&&*title?title:"Untitled work");fputs("</a><small>",stdout);wp_words(op);fputs("</small></div><div>",stdout);
            wp_fingerprint(hex);fputs("</div><div>",stdout);wp_when(sqlite3_column_int64(s,4));fputs("</div></li>",stdout);}
        if(shown)fputs("</ol>",stdout);else fputs("<p class=\"wp-empty\" data-outputs=\"none\">No work on this account has kept an output yet.</p>",stdout);
    }
    sqlite3_finalize(s);sqlite3_close(db);
    fputs("<nav class=\"wp-next\" aria-label=\"Account work\"><a href=\"/work-board?context=",stdout);url_query_value(context);fputs("\">",stdout);page_glyph("board");fputs("Work board</a><a href=\"/inbox?context=",stdout);url_query_value(context);fputs("\">",stdout);page_glyph("bell");fputs("Inbox</a></nav></section>",stdout);
    psetup_end();return 1;
}
#endif
