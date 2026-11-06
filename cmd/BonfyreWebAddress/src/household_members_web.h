#ifndef BONFYRE_HOUSEHOLD_MEMBERS_WEB_H
#define BONFYRE_HOUSEHOLD_MEMBERS_WEB_H
/* Household access uses native forms and CMS membership revisions. */
typedef struct { char household[64],email[320],member[64],role[8],revision[24],epoch[24],expires[24],after[24]; } HhmForm;
static int hhm_form(const char *input,HhmForm *out){
    memset(out,0,sizeof(*out));unsigned seen=0;
    if(input&&strlen(input)>4096)return 0;
    for(const char *p=input?input:"";*p;){
        const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char name[32];
        if(!eq||!psetup_decode(p,(size_t)(eq-p),name,sizeof(name)))return 0;
        char *dest=NULL;size_t cap=0;unsigned bit=0;
#define HHM_FIELD(n,b) if(!strcmp(name,#n)){dest=out->n;cap=sizeof(out->n);bit=b;}
        HHM_FIELD(household,1) else HHM_FIELD(email,2) else HHM_FIELD(member,4) else HHM_FIELD(role,8)
        else HHM_FIELD(revision,16) else HHM_FIELD(epoch,32) else HHM_FIELD(expires,64) else HHM_FIELD(after,128)
#undef HHM_FIELD
        if(!dest||(seen&bit)||!psetup_decode(eq+1,(size_t)(end-eq-1),dest,cap))return 0;seen|=bit;p=*end?end+1:end;
    }
    if(!bhh_slug(out->household)||(*out->member&&!bhh_actor(out->member))||(*out->role&&bhh_rank(out->role)<0))return 0;
    const char *numbers[]={out->revision,out->epoch,out->expires,out->after};
    for(size_t i=0;i<4;i++)if(*numbers[i]){char *end=NULL;errno=0;long long n=strtoll(numbers[i],&end,10);if(strspn(numbers[i],"0123456789")!=strlen(numbers[i])||errno||!end||*end||n<0||((i==0||i==3)&&n>=INT_MAX))return 0;}
    return 1;
}
static int hhm_account(const char *auth_db,const char *email,const char *member,char *principal,size_t size,int active){
    sqlite3 *db=NULL;sqlite3_stmt *s=NULL;int ok=0;principal[0]=0;
    if(!auth_db||sqlite3_open_v2(auth_db,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)goto done;
    const char *sql=email&&*email?"SELECT id,active FROM users WHERE email=?1":"SELECT id,active FROM users WHERE id=?1";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)goto done;
    if(email&&*email)sqlite3_bind_text(s,1,email,-1,SQLITE_STATIC);else if(bhh_actor(member))sqlite3_bind_int(s,1,atoi(member+5));else goto done;
    if(sqlite3_step(s)==SQLITE_ROW && (!active||sqlite3_column_int(s,1))){snprintf(principal,size,"user:%d",sqlite3_column_int(s,0));ok=sqlite3_step(s)==SQLITE_DONE;}
 done:sqlite3_finalize(s);if(db)sqlite3_close(db);return ok;
}
static int serve_household_members(const char *method,const char *target,const char *query,const char *body,const char *raw,const char *host,const char *bearer,const char *auth_db){
    if(strcmp(target,"/households/access"))return 0;
    int post=!strcmp(method,"POST");if(!post&&strcmp(method,"GET")){aw_error(405,"Use GET to review or POST to change household access.\n");return 1;}
    HhmForm input;if(!hhm_form(post?body:query,&input)){aw_error(400,"Invalid household access form or repeated field.\n");return 1;}
    StudioViewer viewer;char actor[64];BfHouseholdAccess access;sqlite3 *db=NULL;sqlite3_stmt *s=NULL;
    if(!session_identity(bearer,auth_db,&viewer)||viewer.id<1){aw_error(403,"Sign in before opening household access.\n");return 1;}
    snprintf(actor,sizeof(actor),"user:%ld",viewer.id);db=cms_open_ro();
    if(!bhh_context(db,input.household,actor,&access)||bhh_rank(access.role)<1){if(db)sqlite3_close(db);aw_error(403,"This account does not have current household access.\n");return 1;}
    int steward=!strcmp(actor,access.steward);
    if(post){
        sqlite3_close(db);db=NULL;char origin[512],expected[512],type[128],member[64];
        snprintf(expected,sizeof(expected),"%s://%s",host&&(!strncmp(host,"127.0.0.1:",10)||!strncmp(host,"localhost:",10))?"http":"https",host?host:"");
        if(!steward||!session_self_authoring(bearer,auth_db)||!aw_header(raw,"Origin",origin,sizeof(origin))||strcmp(origin,expected)||!aw_header(raw,"Content-Type",type,sizeof(type))||strcmp(type,"application/x-www-form-urlencoded")||!*input.member||!*input.role||!*input.revision||!*input.epoch||!hhm_account(auth_db,NULL,input.member,member,sizeof(member),strcmp(input.role,"none")!=0)){
            aw_error(403,"The current steward must submit a reviewed change for an existing account from this page.\n");return 1;
        }
        const char *bin=page_cms_env("BONFYRE_CMS_BINARY","/usr/local/bin/bonfyre-cms"),*cms=page_cms_env("BONFYRE_CMS_DB","/var/lib/bonfyre/cms/bonfyre_cms.db");
        char *const args[]={(char *)bin,"household-members","--operation","set","--household",input.household,"--actor",actor,"--member",member,"--role",input.role,"--expected-revision",input.revision,"--epoch",input.epoch,"--expires-at",*input.expires?input.expires:"0","--db",(char *)cms,NULL};
        unsigned char *output=NULL;size_t bytes=0;int rc=aw_native(args,NULL,0,&output,&bytes);free(output);
        if(rc){aw_error(rc==4?409:rc==3?400:503,"Household access was not changed. Refresh the member review before trying again.\n");return 1;}
        fputs("HTTP/1.1 303 See Other\r\nLocation: /households/access?household=",stdout);url_query_value(input.household);fputs("&member=",stdout);url_query_value(member);fputs("\r\nCache-Control: private, no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);return 1;
    }
    psetup_start(200,"Household access");fputs("<h1>Household access</h1><p>Household: ",stdout);html(input.household);fputs(". Your access: ",stdout);html(access.role);fputs(".</p><p><a href=\"/packs/setup\">Open configurations</a></p>",stdout);
    if(!steward){fputs("<p>The recorded steward manages membership. Your access applies to this household only; it does not share personal training history or grant supplier purchasing authority.</p>",stdout);sqlite3_close(db);psetup_end();return 1;}
    fputs("<p>Review an existing account before changing access. Read permits inspection, write permits draft editing, and admin permits configuration changes. Only the steward manages membership. Removing access preserves saved household work.</p><form method=\"get\" action=\"/households/access\">",stdout);psetup_hidden("household",input.household);fputs("<label>Member account email <input type=\"email\" name=\"email\" required maxlength=\"300\"></label><button>Review member</button></form>",stdout);
    char member[64];int requested=*input.email||*input.member;
    if(requested&&hhm_account(auth_db,input.email,input.member,member,sizeof(member),0)){
        long long revision=0,expires=0;char role[8]="none";int effective=0;
        if(access.epoch&&sqlite3_prepare_v2(db,"SELECT role,revision,expires_at,authority_epoch=?3 AND (expires_at=0 OR expires_at>CAST(strftime('%s','now') AS INTEGER)) FROM _household_membership WHERE household_row=?1 AND member=?2",-1,&s,NULL)==SQLITE_OK){sqlite3_bind_int64(s,1,access.id);sqlite3_bind_text(s,2,member,-1,SQLITE_STATIC);sqlite3_bind_int64(s,3,access.epoch);if(sqlite3_step(s)==SQLITE_ROW){const char *r=(const char *)sqlite3_column_text(s,0);if(bhh_rank(r)>=0)snprintf(role,sizeof(role),"%s",r);revision=sqlite3_column_int64(s,1);expires=sqlite3_column_int64(s,2);effective=sqlite3_column_int(s,3)&&bhh_rank(role)>0;}sqlite3_finalize(s);s=NULL;}
        fputs("<section class=\"pack-panel\" data-household-member><h2>Review ",stdout);html(member);fputs("</h2><p>Current access: ",stdout);html(effective?role:"none");fputs(". The change below applies only to this household.</p>",stdout);
        if(strcmp(member,actor)){
            char rev[24],epoch[24],expiry[24];snprintf(rev,sizeof(rev),"%lld",revision);snprintf(epoch,sizeof(epoch),"%lld",(long long)access.epoch);snprintf(expiry,sizeof(expiry),"%lld",expires);
            fputs("<form method=\"post\" action=\"/households/access\" data-household-access-form>",stdout);psetup_hidden("household",input.household);psetup_hidden("member",member);psetup_hidden("revision",rev);psetup_hidden("epoch",epoch);psetup_hidden("expires",expiry);
            fputs("<label>Access <select name=\"role\">",stdout);const char *roles[]={"none","read","write","admin"};for(size_t i=0;i<4;i++){fputs("<option value=\"",stdout);html(roles[i]);fputs(!strcmp(role,roles[i])?"\" selected>":"\">",stdout);html(roles[i]);fputs("</option>",stdout);}fputs("</select></label><button>Apply reviewed access</button></form>",stdout);
            if(expires)fputs("<p>This form preserves the existing expiry. An expired membership needs a separately reviewed renewal.</p>",stdout);
        }else fputs("<p>The steward is established by the household record; this form cannot transfer stewardship.</p>",stdout);
        fputs("</section>",stdout);
    }else if(requested)fputs("<p>No existing account was found. This page does not send invitations or create accounts.</p>",stdout);
    if(access.epoch&&sqlite3_prepare_v2(db,"SELECT member,role FROM _household_membership WHERE household_row=?1 AND CAST(substr(member,6) AS INTEGER)>?2 ORDER BY CAST(substr(member,6) AS INTEGER) LIMIT 51",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_int64(s,1,access.id);sqlite3_bind_int(s,2,atoi(input.after));int count=0,last=0,more=0;fputs("<h2>Recorded memberships</h2><ul>",stdout);
        while(sqlite3_step(s)==SQLITE_ROW){if(count==50){more=1;break;}const char *m=(const char *)sqlite3_column_text(s,0);if(!bhh_actor(m))continue;fputs("<li><a href=\"/households/access?household=",stdout);url_query_value(input.household);fputs("&amp;member=",stdout);url_query_value(m);fputs("\">Review ",stdout);html(m);fputs("</a></li>",stdout);last=atoi(m+5);count++;}
        fputs("</ul>",stdout);if(more){printf("<a href=\"/households/access?after=%d&amp;household=",last);url_query_value(input.household);fputs("\">Next members</a>",stdout);}
    }
    sqlite3_finalize(s);sqlite3_close(db);psetup_end();return 1;
}
#endif
