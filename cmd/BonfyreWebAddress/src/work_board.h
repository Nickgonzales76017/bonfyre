#ifndef BONFYRE_WORK_BOARD_H
#define BONFYRE_WORK_BOARD_H

/* Account work remains a read-only projection until a separately guarded
 * person-owned action is posted. This support layer owns no account state. */
static int ac_work_board_query(const char *query,char context[96],char task[128]){
    unsigned seen=0;if(!query||strlen(query)>512)return 0;context[0]=task[0]=0;if(!*query){snprintf(context,96,"self");return 1;}
    for(const char *p=query;*p;){const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char key[16],*out=NULL;size_t cap=0;unsigned bit=0;if(!eq||!psetup_decode(p,(size_t)(eq-p),key,sizeof(key)))return 0;
        if(!strcmp(key,"context")){out=context;cap=96;bit=1;}else if(!strcmp(key,"task")){out=task;cap=128;bit=2;}else return 0;
        if((seen&bit)||!ac_decode(eq+1,(size_t)(end-eq-1),out,cap)||!*out)return 0;seen|=bit;p=*end?end+1:end;
    }
    return (seen&1)&&(!(seen&2)||ac_word(task));
}
static void ac_work_board_picker(const char *context,const char *task,const char *bearer,const char *auth_db){
    (void)task; /* A selected task belongs to the current workspace, not the next one. */
    PackSetupDestination rows[PSETUP_DESTINATIONS_MAX];size_t count=0;int result=psetup_destinations(bearer,auth_db,rows,&count);
    if(result<0){fputs("<p role=\"alert\">The complete destination list is unavailable.</p>",stdout);return;}
    fputs("<form class=\"work-board-picker\" method=\"get\" action=\"/work-board\"><label>Work board <select name=\"context\">",stdout);if(!result)fputs("<option value=\"self\">My personal workspace</option>",stdout);
    for(size_t i=0;i<count;i++){fputs("<option value=\"",stdout);html(rows[i].context);fputs(!strcmp(rows[i].context,context)?"\" selected>":"\">",stdout);html(rows[i].label);fputs("</option>",stdout);}fputs("</select></label> <button>Change workspace</button></form>",stdout);
}
/* The operator store named in the account work sources, if any. */
static int ac_work_operator_path(char *out,size_t cap){
    const char *list=getenv("BONFYRE_ACCOUNT_WORK_SOURCES"),*p=list;if(!list)return 0;
    while(p&&*p){const char *end=strchr(p,';');size_t n=end?(size_t)(end-p):strlen(p);
        if(n>9&&!strncmp(p,"operator=",9)&&p[9]=='/'&&n-9<cap){memcpy(out,p+9,n-9);out[n-9]=0;return 1;}
        p=end?end+1:NULL;}
    return 0;
}
static const char *ac_work_state_words(const char *state){
    if(!strcmp(state,"blocked"))return "Stopped until someone looks at it";
    if(!strcmp(state,"running"))return "Being worked on now";
    if(!strcmp(state,"retry"))return "Waiting to try again";
    if(!strcmp(state,"resolved"))return "Finished";
    if(!strcmp(state,"superseded"))return "Replaced by newer work";
    return "Waiting its turn";
}
static void ac_work_when(double at){
    time_t t=(time_t)at;struct tm tm;char day[32];if(t<=0||!gmtime_r(&t,&tm))return;
    strftime(day,sizeof(day),"%a %d %b %Y",&tm);html(day);
}
/* Goals that wait on the person themselves, which the person may settle from
 * the board. The operator's own goals are never closed from a browser. */
static int ac_work_person_source(const char *source){return !strcmp(source,"provider-estate")||!strcmp(source,"release-approval");}
/* Everything that waits on the person: the goals above, plus the provider
 * checklist, whose next steps are sign-ins only the account holder can make.
 * Any other stopped goal is agent work, however long it has been stopped. */
static int ac_work_needs_person(const char *source){return ac_work_person_source(source)||!strcmp(source,"provider-estate-checklist");}
/* The account work index lists this exact assistant goal for this account. */
static int ac_work_listed(const char *task,const char *subject,const char *scope){
    const char *index=getenv("BONFYRE_ACCOUNT_WORK_DB");sqlite3 *db=NULL;sqlite3_stmt *s=NULL;int listed=0;
    if(!task||strncmp(task,"account-operator-",17)||!index||*index!='/')return 0;
    if(sqlite3_open_v2(index,&db,SQLITE_OPEN_READONLY,NULL)==SQLITE_OK){sqlite3_busy_timeout(db,1000);
        if(sqlite3_prepare_v2(db,"SELECT 1 FROM account_work WHERE id=?1 AND subject=?2 AND scope=?3",-1,&s,NULL)==SQLITE_OK){
            sqlite3_bind_text(s,1,task,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,subject,-1,SQLITE_STATIC);sqlite3_bind_text(s,3,scope,-1,SQLITE_STATIC);listed=sqlite3_step(s)==SQLITE_ROW;}
        sqlite3_finalize(s);}
    sqlite3_close(db);return listed;
}
/* One open goal that waits on the person, as the operator store holds it now
 * (not as the index last copied it). */
#define AC_PERSON_MAX 40
typedef struct {char id[150],source[64],title[256],release[96];char *details,*error;double at;} AcPersonGoal;
static void ac_person_free(AcPersonGoal *rows,size_t count){for(size_t i=0;i<count;i++){free(rows[i].details);free(rows[i].error);}}
/* Open person-waiting goals this account's work index lists. Returns -1 when
 * no operator store is configured, else the count (newest priority first). */
static int ac_person_goals(const char *subject,const char *scope,AcPersonGoal *rows,size_t cap){
    char operator_db[PATH_MAX];sqlite3 *db=NULL;sqlite3_stmt *s=NULL;size_t n=0;
    if(!ac_work_operator_path(operator_db,sizeof(operator_db)))return -1;
    if(sqlite3_open_v2(operator_db,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){sqlite3_close(db);return -1;}sqlite3_busy_timeout(db,1000);
    if(sqlite3_prepare_v2(db,"SELECT id,source,title,details,last_error,updated_at,dedupe_key FROM goals WHERE source IN('release-approval','provider-estate','provider-estate-checklist') "
        "AND state NOT IN('resolved','superseded') ORDER BY priority DESC,updated_at DESC LIMIT 200",-1,&s,NULL)==SQLITE_OK){
        while(n<cap&&sqlite3_step(s)==SQLITE_ROW){
            const char *id=(const char*)sqlite3_column_text(s,0),*source=(const char*)sqlite3_column_text(s,1),*title=(const char*)sqlite3_column_text(s,2),*key=(const char*)sqlite3_column_text(s,6);
            AcPersonGoal *g=&rows[n];memset(g,0,sizeof(*g));if(!id||!source||!title||!ac_work_needs_person(source))continue;
            snprintf(g->id,sizeof(g->id),"account-operator-%s",id);if(!ac_word(g->id)||!ac_work_listed(g->id,subject,scope))continue;
            snprintf(g->source,sizeof(g->source),"%s",source);snprintf(g->title,sizeof(g->title),"%s",title);g->at=sqlite3_column_double(s,5);
            if(!strcmp(source,"release-approval")&&key&&!strncmp(key,"release-approval:",17)&&strlen(key+17)<sizeof(g->release)&&strspn(key+17,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-")==strlen(key+17))snprintf(g->release,sizeof(g->release),"%s",key+17);
            const char *d=(const char*)sqlite3_column_text(s,3),*e=(const char*)sqlite3_column_text(s,4);g->details=strdup(d?d:"");g->error=strdup(e?e:"");
            if(!g->details||!g->error){free(g->details);free(g->error);break;}n++;
        }
    }
    sqlite3_finalize(s);sqlite3_close(db);return (int)n;
}

#endif
