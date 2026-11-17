/* Published article credits belong to the site's own CMS page and the
 * admitted public person profile. They never come from a filename, narration
 * prompt, unreviewed foreign byline or the action aperture. */
#ifndef BONFYRE_SITE_EDITORIAL_H
#define BONFYRE_SITE_EDITORIAL_H
#include "ui_tree.h"
typedef struct {
    char media_subject[256];
    char kind[24];
    char author_credit[160];
    char author_subject[160];
    char author_host[256];
    char published_at[16];
    char updated_at[16];
    int article;
} SiteEditorial;

static void site_editorial_copy(char *dst,size_t cap,const unsigned char *value) {
    if(value&&strlen((const char *)value)<cap)
        snprintf(dst,cap,"%s",(const char *)value);
}
static int site_editorial_date(const char *s) {
    if(!s||strlen(s)!=10||s[4]!='-'||s[7]!='-')return 0;
    for(int i=0;i<10;i++)if(i!=4&&i!=7&&!isdigit((unsigned char)s[i]))return 0;
    int year=atoi(s),month=atoi(s+5),day=atoi(s+8);
    if(year<1800||year>2200||month<1||month>12||day<1)return 0;
    const int lengths[]={0,31,28,31,30,31,30,31,31,30,31,30,31};
    int leap=(year%4==0&&(year%100!=0||year%400==0));
    return day<=lengths[month]+(month==2&&leap);
}
static void site_editorial_read(sqlite3 *db,const McpSite *site,
                                const char *route,SiteEditorial *e) {
    sqlite3_stmt *st=NULL;
    memset(e,0,sizeof(*e));
    if(!db||!site||!route)return;
    if(sqlite3_prepare_v2(db,
       "SELECT COALESCE(media_subject_ref,''),COALESCE(editorial_kind,''),"
       "COALESCE(author_credit,''),COALESCE(author_subject_ref,''),"
       "COALESCE(editorial_published_on,''),COALESCE(editorial_updated_on,'') FROM site_page "
       "WHERE namespace=?1 AND route=?2 AND status='published' ORDER BY id DESC LIMIT 1",
       -1,&st,NULL)!=SQLITE_OK)return;
    sqlite3_bind_text(st,1,site->id,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,route,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)==SQLITE_ROW){
        site_editorial_copy(e->media_subject,sizeof(e->media_subject),sqlite3_column_text(st,0));
        site_editorial_copy(e->kind,sizeof(e->kind),sqlite3_column_text(st,1));
        site_editorial_copy(e->author_credit,sizeof(e->author_credit),sqlite3_column_text(st,2));
        site_editorial_copy(e->author_subject,sizeof(e->author_subject),sqlite3_column_text(st,3));
        site_editorial_copy(e->published_at,sizeof(e->published_at),sqlite3_column_text(st,4));
        site_editorial_copy(e->updated_at,sizeof(e->updated_at),sqlite3_column_text(st,5));
    }
    sqlite3_finalize(st);st=NULL;
    e->article=!strcmp(e->kind,"article")||!strcmp(e->kind,"essay")||
                !strcmp(e->kind,"report")||!strcmp(e->kind,"feature")||
                !strcmp(e->kind,"longform");
    if(!site_editorial_date(e->published_at))e->published_at[0]=0;
    if(!site_editorial_date(e->updated_at)||!e->published_at[0]||
       strcmp(e->updated_at,e->published_at)<0)e->updated_at[0]=0;
    if(!e->article||!e->author_credit[0]||
       strncmp(e->author_subject,"person:",7))return;
    const char *slug=e->author_subject+7;
    size_t n=strlen(slug);
    if(!n||n>70)return;
    for(size_t i=0;i<n;i++)if(!(slug[i]>='a'&&slug[i]<='z')&&
        !(slug[i]>='0'&&slug[i]<='9')&&slug[i]!='-')return;
    char host[256];
    snprintf(host,sizeof(host),"%s.aurekai.work",slug);
    if(sqlite3_prepare_v2(db,
       "SELECT host FROM site_presentation WHERE namespace='root' AND "
       "status='published' AND profile='member-profile-v1' AND "
       "subject_address=?1 AND host=?2 AND host NOT IN "
       "(SELECT host FROM site_presentation WHERE namespace='root' AND status='published' "
       "GROUP BY host HAVING COUNT(DISTINCT subject_address)>1) ORDER BY id DESC LIMIT 1",
       -1,&st,NULL)!=SQLITE_OK)return;
    sqlite3_bind_text(st,1,e->author_subject,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,2,host,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)==SQLITE_ROW)
        site_editorial_copy(e->author_host,sizeof(e->author_host),sqlite3_column_text(st,0));
    sqlite3_finalize(st);
}
static void site_editorial_credit(FILE *out,const SiteEditorial *e) {
    if(!out||!e||!e->article||(!e->author_credit[0]&&!e->published_at[0]))return;
    ui_start(out,"div");ui_attr(out,"class","site-article-credit");ui_attr(out,"aria-label","Article details");ui_gt(out);
    if(e->author_credit[0]){
        ui_open(out,"span",NULL);ui_copy(out,"By ");
        if(e->author_host[0]){ui_start(out,"a");ui_attr(out,"rel","author");ui_attr_open(out,"href");ui_copy(out,"https://");
            htext(out,e->author_host);fputs("/\">",out);}
        htext(out,e->author_credit);
        if(e->author_host[0])ui_close(out,"a");
        ui_close(out,"span");
    }
    if(e->published_at[0]){ui_start(out,"time");ui_attr_open(out,"datetime");htext(out,e->published_at);
        fputs("\">",out);htext(out,e->published_at);ui_close(out,"time");}
    if(e->updated_at[0]&&strcmp(e->updated_at,e->published_at)>0){
        ui_open(out,"span",NULL);ui_copy(out,"Updated ");ui_start(out,"time");ui_attr_open(out,"datetime");htext(out,e->updated_at);
        fputs("\">",out);htext(out,e->updated_at);ui_close(out,"time");ui_close(out,"span");}
    ui_close(out,"div");
}
/* Authored citations are one ordered list of published source fragments.
 * The prose may link to them with [1](#source-1); the source list does not
 * create a citation, infer evidence, or claim a foreign work was reviewed. */
static int site_editorial_source_href(const char *href) {
    if(!href||!*href||strlen(href)>1200||strpbrk(href,"\"'<>\\@\r\n\t")||
       strstr(href,"..")||(strncmp(href,"https://",8)==0 ?
       strstr(href+8,"//")!=NULL : strstr(href,"//")!=NULL))return 0;
    return (!strncmp(href,"https://",8)&&strchr(href+8,'.')!=NULL&&href[8]&&href[8]!='/')||
           (href[0]=='/'&&href[1]&&href[1]!='/');
}
static void site_editorial_sources(FILE *out,sqlite3 *db,const McpSite *site,
                                   const char *route,const SiteEditorial *e) {
    sqlite3_stmt *st=NULL;int index=0;
    if(!out||!db||!site||!route||!e||!e->article)return;
    if(sqlite3_prepare_v2(db,
       "SELECT COALESCE(heading,''),COALESCE(body,''),COALESCE(href,''),"
       "COALESCE(source_ref,''),COALESCE(source_digest,'') FROM site_fragment "
       "WHERE namespace=?1 AND page=?2 AND kind IN ('citation','primary-reference') AND status='published' AND audience='public' "
       "ORDER BY position,id LIMIT 100",-1,&st,NULL)!=SQLITE_OK)return;
    sqlite3_bind_text(st,1,site->id,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,route,-1,SQLITE_TRANSIENT);
    while(sqlite3_step(st)==SQLITE_ROW){
        const char *heading=(const char*)sqlite3_column_text(st,0);
        const char *body=(const char*)sqlite3_column_text(st,1);
        const char *href=(const char*)sqlite3_column_text(st,2);
        const char *ref=(const char*)sqlite3_column_text(st,3);
        const char *digest=(const char*)sqlite3_column_text(st,4);
        if(!index){ui_start(out,"section");ui_attr(out,"class","site-article-sources");ui_attr(out,"aria-labelledby","article-sources-title");ui_gt(out);ui_start(out,"h2");ui_attr(out,"id","article-sources-title");ui_gt(out);ui_copy(out,"Sources and notes");ui_close(out,"h2");ui_open(out,"ol",NULL);}
        ui_start(out,"li");ui_attr_open(out,"id");ui_copy(out,"source-");fprintf(out,"%d",++index);ui_attr_close(out);ui_gt(out);
        if(heading&&*heading){ui_open(out,"strong",NULL);htext(out,heading);ui_close(out,"strong");}
        if(body&&*body){ui_open(out,"p",NULL);htext(out,body);ui_close(out,"p");}
        if(site_editorial_source_href(href)){ui_start(out,"a");ui_attr(out,"rel","noopener noreferrer");ui_attr_open(out,"href");
            htext(out,href);fputs("\">Read the source</a>",out);}
        if(ref&&*ref){ui_section_open(out,"source-record","Source record");
            ui_open(out,"p",NULL);ui_copy(out,"Identifier: ");ui_open(out,"code",NULL);htext(out,ref);ui_close(out,"code");ui_close(out,"p");
            if(digest&&*digest){ui_open(out,"p",NULL);ui_copy(out,"Supplied digest: ");ui_open(out,"code",NULL);
                htext(out,digest);ui_close(out,"code");ui_close(out,"p");}
            ui_section_close(out);}
        ui_close(out,"li");
    }
    if(index){ui_close(out,"ol");ui_close(out,"section");}
    sqlite3_finalize(st);
}

#endif
