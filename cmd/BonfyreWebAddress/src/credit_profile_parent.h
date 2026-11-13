#ifndef BONFYRE_CREDIT_PROFILE_PARENT_H
#define BONFYRE_CREDIT_PROFILE_PARENT_H
/* Every unclaimed publication byline is a subprofile in an established
 * institution scope, not a third actor kind. A published publisher presentation
 * wins only through the exact source's retained publisher name. Otherwise
 * the installed tenant supplies a managing institution; no\n * institutional owner is inferred from an unclaimed byline.
 * A verified person-account binding must be a separate explicit admission. */
#include <sqlite3.h>
#include <stdio.h>
#include <string.h>
#include "tenant_host_binding.h"
#include "compat/aurekai_legacy.h"

typedef struct {
    char subject[128];
    char host[128];
    char label[256];
    int publisher_site;
    char account_id[96];
    char subaccount_id[96];
    char subaccount_subject[96];
    int canonical_subaccount;
} BfCreditProfileParent;

static void bf_credit_profile_parent(sqlite3 *cms, sqlite3 *archive,
                                     const char *by, const char *source,
                                     BfCreditProfileParent *parent) {
    sqlite3_stmt *st = NULL;
    char publisher[256] = "", slug[64] = "", subject[128];
    size_t n = 0;
    if (!parent) return;
    memset(parent, 0, sizeof(*parent));
    /* A configured installation never inherits Aurekai's managing
     * institution. Use the exact owner-bound account/site record instead.
     * Only the historical installation retains this fallback. */
    if(bf_tenant_hosts_status()==0) {
        snprintf(parent->subject,sizeof(parent->subject),"institution:aurekai:work");
        snprintf(parent->host,sizeof(parent->host),"%s",bf_legacy_aurekai_public_host("work"));
        snprintf(parent->label,sizeof(parent->label),"Aurekai");
    }
    /* Canonical source-parented subordinate account takes precedence over
     * an inferred publisher slug. It is a public presentation, not a login. */
    if(cms && by && source){
        char expected_host[128]={0};
        sqlite3_stmt *bound=NULL;
        if(bf_credit_profile_host(by,source,expected_host) &&
           sqlite3_prepare_v2(cms,
             "SELECT a.account_id,a.account_subject,a.name,"
             " COALESCE(a.publisher_subject,''),COALESCE(a.publisher_profile_url,''),"
             " s.subaccount_id,s.account_subject"
             " FROM publisher_subaccount s JOIN publisher_account a"
             " ON a.account_id=s.publisher_account_id AND a.namespace=s.namespace"
             " WHERE s.namespace='root' AND a.namespace='root'"
             " AND s.status='published' AND a.status='published'"
             " AND s.source=?1 AND s.display_name=?2 AND s.host=?3"
             " AND s.identity_state='unclaimed' AND a.source=s.source"
             " ORDER BY s.id DESC LIMIT 1",-1,&bound,NULL)==SQLITE_OK){
            sqlite3_bind_text(bound,1,source,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(bound,2,by,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(bound,3,expected_host,-1,SQLITE_TRANSIENT);
            if(sqlite3_step(bound)==SQLITE_ROW){
                const char *id=(const char*)sqlite3_column_text(bound,0);
                const char *subject=(const char*)sqlite3_column_text(bound,1);
                const char *name=(const char*)sqlite3_column_text(bound,2);
                const char *published=(const char*)sqlite3_column_text(bound,3);
                const char *url=(const char*)sqlite3_column_text(bound,4);
                const char *sub=(const char*)sqlite3_column_text(bound,5);
                const char *actor=(const char*)sqlite3_column_text(bound,6);
                if(id&&subject&&name&&sub&&actor){
                    snprintf(parent->account_id,sizeof(parent->account_id),"%s",id);
                    snprintf(parent->subaccount_id,sizeof(parent->subaccount_id),"%s",sub);
                    snprintf(parent->subaccount_subject,sizeof(parent->subaccount_subject),"%s",actor);
                    snprintf(parent->subject,sizeof(parent->subject),"%s",published&&*published?published:subject);
                    snprintf(parent->label,sizeof(parent->label),"%s",name);
                    if(url&&!strncmp(url,"https://",8)){
                        const char *h=url+8,*slash=strchr(h,'/');
                        size_t n=slash?(size_t)(slash-h):0;
                        if(n>0&&n<sizeof(parent->host)&&slash[1]=='\0') {
                            char candidate[128];
                            memcpy(candidate,h,n);candidate[n]='\0';
                            int state=bf_tenant_hosts_status();
                            if((state==1&&bf_tenant_is_member_site_host(candidate))||
                               (state==0&&bf_legacy_aurekai_subhost(candidate))) {
                                snprintf(parent->host,sizeof(parent->host),"%s",candidate);
                                parent->publisher_site=1;
                            }
                        }
                    }
                    parent->canonical_subaccount=1;
                }
            }
        }
        sqlite3_finalize(bound);
        if(parent->canonical_subaccount)return;
    }
    if (!archive || !cms || !source || !*source || !by || !*by) return;
    /* Only a publisher observed on this exact source and byline is eligible. */
    if (sqlite3_prepare_v2(archive,
        "SELECT publisher FROM item WHERE source=?1 AND by=?2"
        " AND COALESCE(publisher,'')<>'' GROUP BY publisher"
        " ORDER BY COUNT(*) DESC,publisher LIMIT 1",-1,&st,NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st,1,source,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,2,by,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(st)==SQLITE_ROW) {
        const char *p=(const char*)sqlite3_column_text(st,0);
        if (p && strlen(p)<sizeof(publisher)) snprintf(publisher,sizeof(publisher),"%s",p);
    }
    sqlite3_finalize(st);st=NULL;
    if (!publisher[0]) return;
    /* This is the same canonical publisher slug as address_every_publisher. */
    for (const unsigned char *p=(const unsigned char*)publisher;*p && n<48;p++){
        unsigned char c=*p;
        if (c>='A'&&c<='Z')c=(unsigned char)(c+32);
        if (!((c>='a'&&c<='z')||(c>='0'&&c<='9')))c='-';
        if (c=='-' && (!n||slug[n-1]=='-'))continue;
        slug[n++]=(char)c;
    }
    while(n&&slug[n-1]=='-')n--;slug[n]=0;
    if(!n) return;
    snprintf(subject,sizeof(subject),"institution:publisher:%s",slug);
    if(sqlite3_prepare_v2(cms,
       "SELECT s.host FROM site_presentation s WHERE s.namespace='root'"
       " AND s.status='published' AND s.profile='newsroom'"
       " AND s.subject_address=?1"
       " AND NOT EXISTS(SELECT 1 FROM site_presentation other"
       " WHERE other.namespace='root' AND other.status='published'"
       " AND other.host=s.host AND other.subject_address<>s.subject_address)"
       " ORDER BY s.host LIMIT 2",-1,&st,NULL)!=SQLITE_OK)return;
    sqlite3_bind_text(st,1,subject,-1,SQLITE_TRANSIENT);
    int matches=0;char host[128]="";
    while(sqlite3_step(st)==SQLITE_ROW){
        const char *candidate=(const char*)sqlite3_column_text(st,0);
        int state=bf_tenant_hosts_status();
        if(!candidate || strlen(candidate)>=sizeof(host) ||
           !((state==1&&bf_tenant_is_member_site_host(candidate))||
             (state==0&&bf_legacy_aurekai_subhost(candidate))))continue;
        if(++matches==1)snprintf(host,sizeof(host),"%s",candidate);
        if(matches>1)break; /* Two admitted published sites are ambiguous. */
    }
    sqlite3_finalize(st);
    if(matches!=1||!host[0])return;
    snprintf(parent->subject,sizeof(parent->subject),"%s",subject);
    snprintf(parent->host,sizeof(parent->host),"%s",host);
    snprintf(parent->label,sizeof(parent->label),"%s",publisher);
    parent->publisher_site=1;
}
#endif
