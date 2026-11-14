#ifndef BONFYRE_CREDIT_PROFILE_PROJECTION_H
#define BONFYRE_CREDIT_PROFILE_PROJECTION_H
#include "credit_profile_host.h"
#include "credit_profile_parent.h"
/* Reuse the published member profile shell for read-only source-attributed
 * makers, without granting them a member identity or any mutation endpoint. */
/* Source-owned archive projection into the same profile card vocabulary used
 * by native member posts. Nothing here creates an authored Aurekai post. */
/* Only item families admitted by the public People credit reader may fan out
 * to a source profile. Future schedules are not public credit publications. */
#define BF_CREDIT_PUBLICATION_BOUND \
  " AND COALESCE(at,0)<=CAST(strftime('%s','now') AS INTEGER)+86400" \
  " AND family IN('news','today','local-news','podcasts','books','culture','music','entertainment','sports','gaming','food','home','style','shopping','health','travel','small-business','commons','nature','local-nature','vulnerabilities')"

static void bf_credit_publications(sqlite3 *db,const char *by,const char *source,int media_only){
 sqlite3_stmt *s=NULL;int shown=0;
 const char *sql=media_only?
  "SELECT key,title,COALESCE(publisher,''),COALESCE(kind,''),COALESCE(strftime('%Y-%m-%d',NULLIF(COALESCE(at,0),0),'unixepoch'),''),COALESCE(image,'') FROM item WHERE by=?1 AND source=?2" BF_CREDIT_PUBLICATION_BOUND " AND COALESCE(title,'')<>'' AND COALESCE(image,'')<>'' ORDER BY COALESCE(at,0) DESC LIMIT 60":
  "SELECT key,title,COALESCE(publisher,''),COALESCE(kind,''),COALESCE(strftime('%Y-%m-%d',NULLIF(COALESCE(at,0),0),'unixepoch'),''),COALESCE(image,'') FROM item WHERE by=?1 AND source=?2" BF_CREDIT_PUBLICATION_BOUND " AND COALESCE(title,'')<>'' ORDER BY COALESCE(at,0) DESC LIMIT 60";
 if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)==SQLITE_OK){
  sqlite3_bind_text(s,1,by,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,source,-1,SQLITE_TRANSIENT);
  while(sqlite3_step(s)==SQLITE_ROW){
   const char *key=(const char*)sqlite3_column_text(s,0),*title=(const char*)sqlite3_column_text(s,1);
   if(!key||!title||!*key||!*title)continue;
    const char *image=(const char*)sqlite3_column_text(s,5);
    if(media_only && (!page_image_url_safe(image) || page_picture_withheld(image)))continue;
    fputs("<article class=\"ph-post\" data-credit-publication>",stdout);
    if(image&&page_image_url_safe(image)&&!page_picture_withheld(image))page_emit_picture(image,"ph-credit-media");
    /* Absolute: the profile also answers at its own credited host, where the
     * item does not; the publication opens on the territory that admitted it. */
    fputs("<h3><a href=\"https://aurekai.work/o/",stdout);
   url_query_value(source);fputc('/',stdout);url_query_value(key);fputs("\">",stdout);html(title);
   fputs("</a></h3><p>",stdout);
   const char *publisher=(const char*)sqlite3_column_text(s,2),*kind=(const char*)sqlite3_column_text(s,3),*date=(const char*)sqlite3_column_text(s,4);
   if(publisher&&*publisher){html(publisher);fputs(" · ",stdout);}
   if(kind&&*kind){html(kind);fputs(" · ",stdout);}
   if(date&&*date){fputs("<time datetime=\"",stdout);html(date);fputs("\">",stdout);html(date);fputs("</time>",stdout);}
    fputs("</p>",stdout);
    /* A credited publication is not a post. Reuse the same exact-item app intake
     * as Discover and A2A, and preserve the source-qualified relationship. */
    if(strlen(source)<128 && strlen(key)==24 && strspn(key,"0123456789abcdef")==24){
      fputs("<nav class=\"ph-source-continuations\" data-publication-continuation aria-label=\"Use this retained publication\">",stdout);
      fputs("<a href=\"https://signin.aurekai.work/feed-use?context=self&amp;item=",stdout);url_query_value(source);fputs("%2F",stdout);url_query_value(key);
      fputs("\">" BF_ICON_SPARKLES " Use in installed apps</a>",stdout);
      fputs("<a href=\"https://signin.aurekai.work/relationships?context=self&amp;who=",stdout);url_query_value(by);
      fputs("&amp;source=",stdout);url_query_value(source);
      fputs("\">" BF_ICON_BOOKMARK " Keep track of this source credit</a></nav>",stdout);
    }
    fputs("</article>",stdout);shown++;
  }
 }
 sqlite3_finalize(s);
 if(!shown)fputs("<p class=\"ph-empty\">No matching public archive items in this source.</p>",stdout);
}

static int serve_credit_profile(const char *method,const char *target,const char *query){
    char flag[8]={0},by[513]={0},source[4097]={0},tab[24]="overview",host[128]={0};
    const char *archive=getenv("BONFYRE_ITEM_ARCHIVE_DB");
    sqlite3 *db=NULL;sqlite3_stmt *st=NULL;int found=0;
    /* The exported /people?who&source leaf is the public, read-only profile
     * when this same exact-source evidence gate admits it. Explicit profile=1
     * retains strict 404 semantics for unadmitted source credits. */
    int explicit_profile=query_value(query,"profile",flag,sizeof(flag));
    if(strcmp(method,"GET")||strcmp(target,"/people")||
       (explicit_profile&&strcmp(flag,"1")))return 0;
    if(!query_value(query,"who",by,sizeof(by))||
       !query_value(query,"source",source,sizeof(source))||
       !bf_people_name_plausible(by)||!bf_credit_profile_host(by,source,host)){
        if(!explicit_profile)return 0;
        ph_error(404,"No exact source-attributed profile exists for this coordinate.\n");
        return 1;
    }
    char chosen[24]={0};
    if(query_value(query,"tab",chosen,sizeof(chosen))){
        static const char *const allowed[]={"overview","posts","gallery","about","communities","organizations"};
        int valid=0;
        for(size_t i=0;i<sizeof(allowed)/sizeof(allowed[0]);i++)
            if(!strcmp(chosen,allowed[i]))valid=1;
        if(!valid){if(!explicit_profile)return 0;ph_error(404,"This public profile section is unavailable.\n");return 1;}
        snprintf(tab,sizeof(tab),"%s",chosen);
    }
    if(!archive||!(db=bf_sqlite_open_readonly_path(archive,1500))){
        ph_error(503,"The retained publication archive is unavailable.\n");return 1;
    }
    if(sqlite3_prepare_v2(db,
      "SELECT 1 FROM item WHERE by=?1 AND source=?2"
      " AND COALESCE(at,0)<=CAST(strftime('%s','now') AS INTEGER)+86400"
      " AND family IN('news','today','local-news','podcasts','books','culture','music','entertainment','sports','gaming','food','home','style','shopping','health','travel','small-business','commons','nature','local-nature','vulnerabilities')"
      " AND by NOT IN(SELECT publisher FROM item WHERE COALESCE(publisher,'')<>'') LIMIT 1",
      -1,&st,NULL)==SQLITE_OK){
        sqlite3_bind_text(st,1,by,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,2,source,-1,SQLITE_TRANSIENT);
        found=sqlite3_step(st)==SQLITE_ROW;
    }
    sqlite3_finalize(st);
    const char *cms_path=getenv("BONFYRE_CMS_DB");
    if(found&&cms_path&&*cms_path){
        sqlite3 *cms=bf_sqlite_open_readonly_path(cms_path,1500);
        sqlite3_stmt *source_decl=NULL;
        if(!cms||sqlite3_prepare_v2(cms,
            "SELECT 1 FROM internet_source WHERE source=?1 AND status='published'"
            " AND (lower(COALESCE(label,''))=lower(?2) OR lower(COALESCE(publisher_name,''))=lower(?2)) LIMIT 1",
            -1,&source_decl,NULL)!=SQLITE_OK){
            sqlite3_finalize(source_decl);if(cms)sqlite3_close(cms);sqlite3_close(db);
            /* No declarations to check means no admitted credit: the ordinary
             * people page answers, and only an explicit profile asks for more. */
            if(!explicit_profile)return 0;
            ph_error(503,"Publisher evidence declarations are unavailable.\n");return 1;
        }
        sqlite3_bind_text(source_decl,1,source,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(source_decl,2,by,-1,SQLITE_TRANSIENT);
        if(sqlite3_step(source_decl)==SQLITE_ROW)found=0;
        sqlite3_finalize(source_decl);sqlite3_close(cms);
    }
    if(!found){sqlite3_close(db);if(!explicit_profile)return 0;ph_error(404,"No retained publication credits that name and source.\n");return 1;}
    BfCreditProfileParent parent;
    sqlite3 *parent_cms=cms_path&&*cms_path?bf_sqlite_open_readonly_path(cms_path,1500):NULL;
    bf_credit_profile_parent(parent_cms,db,by,source,&parent);
    if(parent_cms)sqlite3_close(parent_cms);
    PhSite profile={0};
    snprintf(profile.id,sizeof(profile.id),"%s",host);
    snprintf(profile.host,sizeof(profile.host),"%s",host);
    snprintf(profile.territory,sizeof(profile.territory),"work");
    snprintf(profile.name,sizeof(profile.name),"%s",by);
    /* No claim, account, follow authority or editor is inferred. */
    ph_begin(NULL,&profile,by,0,tab,NULL);
    ph_nav(&profile,1,&profile,tab,0);
    fputs("<nav class=\"ph-parent\" data-subprofile-parent-subject=\"",stdout);html(parent.subject);
    fputs("\" data-subprofile-parent-account=\"",stdout);html(parent.account_id);
    fputs("\" data-subprofile-account=\"",stdout);html(parent.subaccount_id);
    fputs("\" data-subprofile-account-subject=\"",stdout);html(parent.subaccount_subject);
    fputs("\" data-subprofile-parent-host=\"",stdout);html(parent.host);
    fputs("\" aria-label=\"Profile hierarchy\"><a href=\"https://",stdout);html(parent.host);
    fputs("/\">",stdout);html(parent.label);
    fputs("</a><span aria-hidden=\"true\"> / </span><span>Contributor subaccount</span></nav>",stdout);
    fputs("<p class=\"ph-privacy\" data-credit-observation>Publisher account contributor profile. Publications remain attached to the exact source organization. This subordinate public account is not a verified person, employment record, login, consent or editorial permission.</p>",stdout);
    /* One real profile document, seven anchored sections. The original
     * /o source detail pages own the article/media bodies and their rights. */
    fputs("<section id='overview' data-profile-overview><h2>Publisher contributor account</h2><p>",stdout);
    html(by);fputs(" appears in the source-backed publication collection of ",stdout);html(parent.label);
    fputs(". This subordinate profile preserves the original source and does not assert a personal or organizational grant.</p></section>",stdout);
    fputs("<section id='posts' data-credit-posts data-observed-publications><h2>Publications</h2>"
          "<p>Original source publications under this publisher account; not invented first-person posts.</p>",stdout);
    bf_credit_publications(db,by,source,0);
    fputs("</section>",stdout);
    fputs("<section id='gallery' data-credit-gallery data-observed-media><h2>Media from credited publications</h2>",stdout);
    bf_credit_publications(db,by,source,1);
    sqlite3_close(db);
    fputs("</section><section id='about'><h2>About this public credit</h2><p>Printed as ",stdout);
    html(by);fputs(" by ",stdout);html(source);
    fputs(". This is not a claimed biography or identity verification.</p></section>",stdout);
    fputs("<section id='communities'><h2>Communities</h2>"
          "<p>No public community ownership is established by a publisher byline.</p>"
          "<a href='/community'>Explore published communities</a></section>",stdout);
    fputs("<section id='organizations'><h2>Organizations</h2>"
          "<p>A publishing source is not evidence of organizational membership.</p>"
          "<a href='/organisations'>Explore source-backed organizations</a></section>",stdout);
    fputs("</section></div></main>" BF_ESTATE_FOOT "</body></html>",stdout); /* credited pages carry no member posts, so no post filter script */
    return 1;
}
#endif
