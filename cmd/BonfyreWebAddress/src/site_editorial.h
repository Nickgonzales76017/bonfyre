/* Published article credits belong to the site's own CMS page and the
 * admitted public person profile. They never come from a filename, narration
 * prompt, unreviewed foreign byline or the action aperture. */
#ifndef BONFYRE_SITE_EDITORIAL_H
#define BONFYRE_SITE_EDITORIAL_H
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
    fputs("<div class=\"site-article-credit\" aria-label=\"Article details\">",out);
    if(e->author_credit[0]){
        fputs("<span>By ",out);
        if(e->author_host[0]){fputs("<a rel=\"author\" href=\"https://",out);
            htext(out,e->author_host);fputs("/\">",out);}
        htext(out,e->author_credit);
        if(e->author_host[0])fputs("</a>",out);
        fputs("</span>",out);
    }
    if(e->published_at[0]){fputs("<time datetime=\"",out);htext(out,e->published_at);
        fputs("\">",out);htext(out,e->published_at);fputs("</time>",out);}
    if(e->updated_at[0]&&strcmp(e->updated_at,e->published_at)>0){
        fputs("<span>Updated <time datetime=\"",out);htext(out,e->updated_at);
        fputs("\">",out);htext(out,e->updated_at);fputs("</time></span>",out);}
    fputs("</div>",out);
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
        if(!index)fputs("<section class=\"site-article-sources\" aria-labelledby=\"article-sources-title\">"
                         "<h2 id=\"article-sources-title\">Sources and notes</h2><ol>",out);
        fprintf(out,"<li id=\"source-%d\">",++index);
        if(heading&&*heading){fputs("<strong>",out);htext(out,heading);fputs("</strong>",out);}
        if(body&&*body){fputs("<p>",out);htext(out,body);fputs("</p>",out);}
        if(site_editorial_source_href(href)){fputs("<a rel=\"noopener noreferrer\" href=\"",out);
            htext(out,href);fputs("\">Read the source</a>",out);}
        if(ref&&*ref){fputs("<details class=\"source-record\"><summary>Source record</summary>"
                             "<p>Identifier: <code>",out);htext(out,ref);fputs("</code></p>",out);
            if(digest&&*digest){fputs("<p>Supplied digest: <code>",out);
                htext(out,digest);fputs("</code></p>",out);}
            fputs("</details>",out);}
        fputs("</li>",out);
    }
    if(index)fputs("</ol></section>",out);
    sqlite3_finalize(st);
}

#endif
