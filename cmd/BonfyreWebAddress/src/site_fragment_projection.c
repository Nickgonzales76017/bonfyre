#include "site_fragment_projection.h"

#include <ctype.h>
#include <stdio.h>
#include <string.h>

typedef struct { char *buf; size_t cap, used; int failed; } Writer;

static void raw(Writer *w, const char *s) {
    size_t n;
    if (!w || w->failed || !s) return;
    n = strlen(s);
    if (n >= w->cap - w->used) { w->failed = 1; return; }
    memcpy(w->buf + w->used, s, n);
    w->used += n; w->buf[w->used] = '\0';
}
static void esc(Writer *w, const char *s) {
    if (!w || !s) return;
    for (const unsigned char *p=(const unsigned char *)s; *p; ++p) {
        const char *r=NULL; char one[2]={(char)*p,'\0'};
        if (*p=='&') r="&amp;"; else if (*p=='<') r="&lt;";
        else if (*p=='>') r="&gt;"; else if (*p=='\"') r="&quot;";
        else if (*p=='\'') r="&#39;"; else r=one;
        raw(w,r); if (w->failed) return;
    }
}
static int ident(const char *s) {
    size_t n;
    if (!s || !(n=strlen(s)) || n>128) return 0;
    for (size_t i=0;i<n;i++) {
        unsigned char c=(unsigned char)s[i];
        if (!(isalnum(c)||c=='-'||c=='_'||c=='.'||c==':')) return 0;
    }
    return 1;
}
static int href_ok(const char *s) {
    return s && *s && (s[0]=='/' || s[0]=='#' ||
           !strncmp(s,"https://",8) || !strncmp(s,"http://",7));
}
static const char *repr_key(BfSiteFragmentRepresentation r) {
    return r==BF_SITE_FRAGMENT_HTML ? "html" :
           r==BF_SITE_FRAGMENT_HTMX ? "htmx" : NULL;
}
static int count_rows(sqlite3 *db, const char *sql,
                      const BfSiteFragmentProjectionRequest *r) {
    sqlite3_stmt *st=NULL; int n=-1;
    if (sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK) return -1;
    sqlite3_bind_text(st,1,r->namespace_name,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,2,r->page,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,3,r->slot,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,4,r->audience,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(st)==SQLITE_ROW) n=sqlite3_column_int(st,0);
    sqlite3_finalize(st); return n;
}

int bf_site_fragment_project(sqlite3 *db,
                             const BfSiteFragmentProjectionRequest *r,
                             char *out, size_t cap,
                             BfSiteFragmentProjectionStats *stats) {
    sqlite3_stmt *st=NULL; Writer w={out,cap,0,0}; char sql[1024];
    const char *key; int rows=0;
    if (stats) memset(stats,0,sizeof(*stats));
    if (!db||!r||!out||cap<2||!ident(r->namespace_name)||!ident(r->page)||
        !ident(r->slot)||!ident(r->audience)||!(key=repr_key(r->representation)))
        return -1;
    out[0]='\0';
    if (stats) {
        stats->exact_coordinate_rows=count_rows(db,
          "SELECT count(*) FROM site_fragment WHERE namespace=?1 AND page=?2 AND slot=?3 AND audience=?4",r);
        stats->published_rows=count_rows(db,
          "SELECT count(*) FROM site_fragment WHERE namespace=?1 AND page=?2 AND slot=?3 AND audience=?4 AND status='published'",r);
        if (stats->exact_coordinate_rows<0||stats->published_rows<0) return -1;
    }
    /* projection_contract is typed JSON; formatting must not become semantics. */
    if (snprintf(sql,sizeof(sql),
        "SELECT coordinate,kind,eyebrow,heading,body,label,href FROM site_fragment "
        "WHERE namespace=?1 AND page=?2 AND slot=?3 AND audience=?4 "
        "AND status='published' AND COALESCE(json_extract(projection_contract,'$.%s'),0)=1 "
        "ORDER BY position,id",key)>=(int)sizeof(sql)) return -1;
    if (sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK) return -1;
    sqlite3_bind_text(st,1,r->namespace_name,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,2,r->page,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,3,r->slot,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,4,r->audience,-1,SQLITE_TRANSIENT);
    while (sqlite3_step(st)==SQLITE_ROW) {
        const char *coordinate=(const char *)sqlite3_column_text(st,0);
        const char *kind=(const char *)sqlite3_column_text(st,1);
        const char *eyebrow=(const char *)sqlite3_column_text(st,2);
        const char *heading=(const char *)sqlite3_column_text(st,3);
        const char *body=(const char *)sqlite3_column_text(st,4);
        const char *label=(const char *)sqlite3_column_text(st,5);
        const char *href=(const char *)sqlite3_column_text(st,6);
        if (!coordinate||!kind||!ident(coordinate)||!ident(kind)) continue;
        raw(&w,"<article class=\"site-fragment kind-"); esc(&w,kind);
        raw(&w,"\" data-coordinate=\""); esc(&w,coordinate); raw(&w,"\">");
        if (eyebrow&&*eyebrow) { raw(&w,"<p class=\"eyebrow\"><i></i>"); esc(&w,eyebrow); raw(&w,"</p>"); }
        if (heading&&*heading) { raw(&w,"<h3>"); esc(&w,heading); raw(&w,"</h3>"); }
        if (body&&*body) { raw(&w,"<p>"); esc(&w,body); raw(&w,"</p>"); }
        if (label&&*label&&href_ok(href)) {
            raw(&w,"<a class=\"way\" href=\""); esc(&w,href); raw(&w,"\">");
            esc(&w,label); raw(&w,"</a>");
        }
        raw(&w,"</article>");
        if (w.failed) { sqlite3_finalize(st); out[0]='\0'; return -1; }
        rows++;
    }
    sqlite3_finalize(st);
    if (stats) { stats->rows=rows; stats->contract_rows=rows; }
    return rows ? 1 : 0;
}
