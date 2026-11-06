/* The people and the institutions, which this estate had already read and was
 * not showing.
 *
 * Everything public here is somebody's work. The archive credits a maker on
 * nearly every thing it kept, and the ledger records the publisher each
 * channel belongs to -- twenty-four thousand of them carrying an encyclopedia's
 * identity for the institution itself. None of that had an address: a person
 * could read a story and never ask who wrote it, or what else they wrote.
 *
 * So this is a directory of makers and of institutions, built only from what
 * the estate actually read. A person here is a name a publisher printed beside
 * something; it is not an account, not a verified identity, and not a claim
 * that two identical names are one person. An institution here is a publisher
 * whose channels this estate reads, which says nothing about what it is like.
 */
#include "people_projection.h"
#include "bf_sqlite_path.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include "bf_lex.h"
#include "credit_profile_host.h"
#include "glyph_set.h"
#include "people_name_filter.h"

static void esc(FILE *out, const char *s) {
    bf_lex_html_emit(out, s);
}

/* Where this estate addresses an institution, when it has given it an address.
 *
 * The directory listed publishers and the estate answered on their hosts, and
 * neither knew about the other: a reader could see that this estate reads the
 * Guardian and have no way to reach the page standing at that publisher's own
 * address. The two halves are the same fact, so the row carries both.
 *
 * Only a published presentation produces a link. An address this estate has
 * not published is not guessed into existence from a hostname. */
static int institution_address(sqlite3 *cms, const char *host, char *out, size_t cap) {
    sqlite3_stmt *st = NULL;
    char label[96]; size_t n = 0;
    const char *p = host;
    if (!cms || !host || !*host || !out || cap < 8) return 0;
    if (!strncmp(p, "www.", 4)) p += 4;
    for (; *p && n + 1 < sizeof(label); p++) {
        char c = *p;
        if (c >= 'A' && c <= 'Z') c = (char)(c - 'A' + 'a');
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'))) c = '-';
        if (c == '-' && (n == 0 || label[n - 1] == '-')) continue;
        label[n++] = c;
    }
    while (n && label[n - 1] == '-') n--;
    if (!n) return 0;
    label[n] = '\0';
    if (n > 48) label[48] = '\0';
    if (sqlite3_prepare_v2(cms,
          "SELECT host FROM site_presentation WHERE status='published'"
          " AND host LIKE ?1 || '.aurekai.%' ORDER BY id DESC LIMIT 1", -1, &st, NULL) != SQLITE_OK)
        return 0;
    sqlite3_bind_text(st, 1, label, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        snprintf(out, cap, "%s", (const char *)sqlite3_column_text(st, 0));
        sqlite3_finalize(st);
        return out[0] != '\0';
    }
    sqlite3_finalize(st);
    return 0;
}

static void url(FILE *out, const char *s) {
    static const char *hex = "0123456789ABCDEF";
    for (; s && *s; s++) {
        unsigned char c = (unsigned char)*s;
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~')
            fputc(c, out);
        else { fputc('%', out); fputc(hex[c >> 4], out); fputc(hex[c & 15], out); }
    }
}

static sqlite3 *open_read_only(const char *path) {
    return bf_sqlite_open_readonly_path(path, 1500);
}

/* Only a published, destination-owned member presentation can establish a
 * person host. A publisher byline (including an identical display name) cannot. */
static int people_member_host(sqlite3 *cms,const char *subject,char *out,size_t cap) {
    const char *slug;
    static const char *territories[]={"work","life","live","shop"};
    sqlite3_stmt *st=NULL;
    if(out&&cap)out[0]=0;
    if(!cms||!subject||strncmp(subject,"person:",7)||!out||cap<32)return 0;
    slug=subject+7;
    size_t n=strlen(slug);
    if(n<1||n>63||slug[0]=='-'||slug[n-1]=='-')return 0;
    for(size_t i=0;i<n;i++){
        char c=slug[i];
        if(!((c>='a'&&c<='z')||(c>='0'&&c<='9')||c=='-'))return 0;
    }
    const char *sql="SELECT 1 FROM site_presentation s"
      " WHERE s.namespace='root' AND s.status='published'"
      " AND s.profile='member-profile-v1' AND s.subject_address=?1 AND s.host=?2"
      " AND NOT EXISTS(SELECT 1 FROM site_presentation other"
      " WHERE other.namespace='root' AND other.status='published'"
      " AND other.host=s.host AND other.subject_address<>s.subject_address)"
      " LIMIT 1";
    if(sqlite3_prepare_v2(cms,sql,-1,&st,NULL)!=SQLITE_OK)return 0;
    for(size_t i=0;i<sizeof(territories)/sizeof(territories[0]);i++){
        char host[96];
        int len=snprintf(host,sizeof(host),"%s.aurekai.%s",slug,territories[i]);
        if(len<=0||(size_t)len>=sizeof(host)||(size_t)len>=cap)continue;
        sqlite3_reset(st);sqlite3_clear_bindings(st);
        sqlite3_bind_text(st,1,subject,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,2,host,-1,SQLITE_TRANSIENT);
        if(sqlite3_step(st)==SQLITE_ROW){snprintf(out,cap,"%s",host);sqlite3_finalize(st);return 1;}
    }
    sqlite3_finalize(st);return 0;
}
int bf_public_person_home(const char *subject,char *out,size_t cap){
    const char *cms_path=getenv("BONFYRE_CMS_DB");
    sqlite3 *cms=cms_path&&*cms_path?open_read_only(cms_path):NULL;
    if(!cms){if(out&&cap)out[0]=0;return 0;}
    int ok=people_member_host(cms,subject,out,cap);
    sqlite3_close(cms);return ok;
}
static void people_presentation(FILE *out) {
    fputs("<style id='people-presentation-v3'>"
          ".people-summary{margin:6px 0 30px;font-size:15px;line-height:1.6;color:var(--dim);max-width:70ch}.people-summary strong{color:var(--ink)}"
          ".people-jump{display:flex;gap:16px;flex-wrap:wrap;margin:-18px 0 30px;font-size:14px}"
          ".region-people,.region-members,.region-person{margin-top:clamp(28px,4vw,56px)}"
          ".people-section-head h2,.people-credit-head h2{margin:0 0 6px;font:400 clamp(30px,4vw,48px)/1.02 'Instrument Serif',Georgia,serif;letter-spacing:-.04em}"
          ".people-section-head p,.people-credit-head p{margin:0;color:var(--dim);line-height:1.6;max-width:68ch}"
          ".people-credit-head{display:grid;grid-template-columns:minmax(0,1fr) minmax(260px,380px);gap:24px;align-items:end;margin-bottom:12px}"
          ".people-search{display:flex;gap:8px;border-bottom:2px solid var(--ink);padding:4px 0}.people-search label{flex:1;min-width:0}.people-search input{width:100%;border:0!important;background:transparent!important;padding:8px 2px!important;font-size:16px}.people-search button{white-space:nowrap}"
          ".people-kicker{font:600 10px/1.4 'DM Mono',monospace;text-transform:uppercase;letter-spacing:.1em;color:var(--dim);margin:0}"
          ".people-members,.people-list,.people-source-list,.people-work-list{list-style:none;margin:0;padding:0}"
          ".people-members li,.people-list>li,.people-source-list li,.people-work-list li{padding:14px 0;border-bottom:1px solid var(--rule);min-width:0}"
          ".people-members li{display:flex;flex-wrap:wrap;gap:6px 14px;align-items:baseline}.people-members strong{font:400 26px/1.1 'Instrument Serif',Georgia,serif}"
          ".people-members strong a[data-profile-primary]{display:inline-flex;gap:9px;align-items:center;color:var(--ink);text-decoration:none}.people-members strong a[data-profile-primary]:hover{text-decoration:underline}"
          ".people-members strong a[data-profile-primary] svg{width:23px;height:23px;flex:none}"
          "[data-person-profile-buckets]{display:flex;align-items:center;flex-wrap:wrap;gap:7px;margin-top:6px}"
          ".person-content-bucket{display:inline-flex;gap:5px;align-items:center;border:1px solid var(--rule);border-radius:999px;padding:5px 9px;font:500 11px/1.3 system-ui;color:var(--dim)}.person-content-bucket svg{width:14px;height:14px}"
          ".people-list h3{margin:0;font:400 24px/1.15 'Instrument Serif',Georgia,serif;letter-spacing:-.02em}.people-list h3 a,.people-source-list h3 a{color:var(--ink);text-decoration:none}.people-list h3 a:hover{text-decoration:underline}"
          ".people-list p,.people-source-list p,.people-work-list p{margin:4px 0 0;font-size:14px;line-height:1.55;color:var(--dim);overflow-wrap:anywhere}.people-list p a,.people-work-list a{color:var(--ink)}"
          ".people-where a+span{margin-right:6px}.relationship-link{font-size:13px;color:var(--ink)}"
          ".people-boundary{margin-top:18px;font-size:12px;line-height:1.6;color:var(--dim);max-width:72ch}"
          ".people-dossier-head h2{margin:0;font:400 clamp(40px,6vw,72px)/.98 'Instrument Serif',Georgia,serif;letter-spacing:-.05em}.people-dossier-head p{margin:8px 0 0;color:var(--dim)}"
          ".people-work-list p>*+*::before{content:' \\00b7  ';color:var(--dim)}"
          ".people-work-list a{font-weight:600;text-decoration:none}.people-work-list a:hover{text-decoration:underline}"
          ".people-pager{display:flex;gap:16px;margin-top:18px}"
          "@media(max-width:760px){.people-credit-head{grid-template-columns:1fr}}"
          "</style>", out);
    fputs("<style id='people-stitch-20261104'>"
          "/* 2026-11-04 exact Stitch People index design: translated to native"
          " * subaccount/source records and existing published profile surfaces. */"
          "body.route-people[data-aurekai-pattern=directory]{"
          "  --paper:#0f1511;--ink:#dee4dd;--dim:#c5c9af;"
          "  --rule:#444935;--panel:#171d19;--accent:#d0fc4f;"
          "  background:#0f1511;color:#dee4dd;color-scheme:dark"
          "}"
          "body.route-people :is(.wrap,.people-list,.people-members){min-width:0}"
          "body.route-people .hero{padding-block:clamp(34px,5vw,72px) 36px;border-bottom:0}"
          "body.route-people .hero h1{"
          "  color:#dee4dd;font:600 clamp(38px,4.2vw,56px)/1.08 'DM Sans',system-ui,sans-serif;"
          "  letter-spacing:-.035em;margin:4px 0 18px"
          "}"
          "body.route-people .hero p:not(.eyebrow){"
          "  font:400 clamp(18px,1.8vw,24px)/1.52 Literata,Georgia,serif;"
          "  color:#c5c9af;max-width:58ch"
          "}"
          "body.route-people .people-summary{"
          "  display:block;max-width:none;margin:6px 0 22px;padding:18px 22px;"
          "  background:#171d19;border:1px solid #252b27;border-radius:2px;"
          "  color:#c5c9af;font:13px/1.75 'DM Sans',sans-serif"
          "}"
          "body.route-people .people-summary::before{content:\"●\";color:#d0fc4f;margin-right:10px}"
          "body.route-people .people-summary strong{color:#d0fc4f;font-weight:700}"
          "body.route-people .people-jump{"
          "  margin:0 0 28px;display:flex;gap:6px;align-items:center;flex-wrap:wrap;"
          "  padding:0;font:600 12px/1.3 'DM Sans',sans-serif"
          "}"
          "body.route-people .people-jump a{"
          "  padding:9px 17px;background:#252b27;color:#dee4dd;"
          "  border-radius:999px;text-decoration:none;border:1px solid transparent"
          "}"
          "body.route-people .people-jump a[href=\"#people-credits\"]{"
          "  background:#d0fc4f;color:#283500"
          "}"
          "body.route-people:has(#people-addressed:target) .people-jump a[href=\"#people-credits\"]{"
          "  background:#252b27;color:#dee4dd"
          "}"
          "body.route-people:has(#people-addressed:target) .people-jump a[href=\"#people-addressed\"]{"
          "  background:#d0fc4f;color:#283500"
          "}"
          "body.route-people .people-jump a:hover,body.route-people .people-jump a:focus-visible{"
          "  outline:2px solid #d0fc4f;outline-offset:2px"
          "}"
          "body.route-people .region-members,body.route-people .region-people{margin-top:40px}"
          "body.route-people .people-section-head h2,body.route-people .people-credit-head h2{"
          "  font:600 clamp(25px,2.5vw,32px)/1.15 'DM Sans',sans-serif;"
          "  color:#dee4dd;letter-spacing:-.025em;margin:0 0 8px"
          "}"
          "body.route-people .people-section-head p,body.route-people .people-credit-head p{"
          "  font:400 15px/1.65 Literata,Georgia,serif;color:#c5c9af;max-width:72ch"
          "}"
          "body.route-people .people-members{"
          "  display:grid!important;grid-template-columns:minmax(0,1fr)!important;"
          "  gap:12px;padding:0;margin:24px 0"
          "}"
          "body.route-people .people-members>li{"
          "  border-radius:24px;background:#171d19;border:1px solid #252b27;"
          "  display:flex;align-items:center;gap:10px 22px;flex-wrap:wrap;"
          "  padding:clamp(18px,3vw,30px);min-height:120px"
          "}"
          "body.route-people .people-members strong{"
          "  font:650 23px/1.1 'DM Sans',sans-serif;letter-spacing:-.02em"
          "}"
          "body.route-people .people-members strong a{color:#dee4dd!important}"
          "body.route-people [data-person-profile-buckets]{margin-top:0;gap:6px}"
          "body.route-people .person-content-bucket{"
          "  color:#dee4dd;border-color:#303631;padding:7px 10px;"
          "  border-radius:999px;font:500 11px 'DM Sans',sans-serif"
          "}"
          "body.route-people .people-members [data-profile-surface]{"
          "  font:500 12px/1.35 'DM Sans',sans-serif;color:#c5c9af;"
          "  overflow-wrap:anywhere;text-decoration:none"
          "}"
          "body.route-people .people-members .relationship-link{"
          "  margin-left:auto;display:inline-flex;align-items:center;min-height:40px;"
          "  background:#d0fc4f;color:#283500!important;border-radius:999px;"
          "  padding:10px 18px;font:700 12px/1.2 'DM Sans',sans-serif;text-decoration:none"
          "}"
          "body.route-people .people-credit-head{"
          "  display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:end;"
          "  gap:14px 24px;margin:34px 0 18px"
          "}"
          "body.route-people .people-credit-head>div:first-child{"
          "  grid-column:1/-1"
          "}"
          "body.route-people .people-search{"
          "  display:flex;align-items:center;gap:6px;padding:7px;"
          "  border:1px solid #303631;border-radius:999px;background:#171d19;"
          "  min-width:0;margin:0"
          "}"
          "body.route-people .people-search label{flex:1}"
          "body.route-people .people-search input{"
          "  background:transparent!important;border:0!important;"
          "  color:#dee4dd!important;min-width:0;width:100%;min-height:38px;"
          "  font:500 14px/1.2 'DM Sans',sans-serif"
          "}"
          "body.route-people .people-search input::placeholder{color:#a6b1a4}"
          "body.route-people .people-search button,body.route-people .people-sort button{"
          "  border:0;border-radius:999px;background:#d0fc4f;color:#283500;"
          "  min-height:38px;padding:7px 16px;cursor:pointer;font:700 12px 'DM Sans',sans-serif"
          "}"
          "body.route-people .people-sort{display:flex;align-items:center;gap:9px;flex-wrap:wrap}"
          "body.route-people .people-sort label{"
          "  font:600 11px 'DM Sans',sans-serif;letter-spacing:.05em;text-transform:uppercase;color:#c5c9af"
          "}"
          "body.route-people .people-sort select{"
          "  background:#252b27;border:1px solid #444935;border-radius:999px;"
          "  padding:9px 14px;color:#dee4dd;min-height:40px;font:12px 'DM Sans',sans-serif"
          "}"
          "body.route-people .people-list{margin:20px 0 0;padding:0;list-style:none}"
          "body.route-people .people-list>li{"
          "  display:grid;grid-template-columns:38px minmax(0,1fr) minmax(160px,.42fr);"
          "  grid-template-rows:auto auto;column-gap:20px;row-gap:8px;"
          "  align-items:center;margin:0;padding:24px clamp(15px,2vw,30px);"
          "  border-bottom:1px solid #252b27;background:#0a100c;"
          "  transition:background-color .15s linear"
          "}"
          "body.route-people .people-list>li::before{"
          "  counter-increment:stitch-maker;content:counter(stitch-maker,decimal-leading-zero);"
          "  grid-column:1;grid-row:1/3;align-self:center;color:#d0fc4f;"
          "  font:650 11px/1 'DM Sans',monospace;letter-spacing:.07em"
          "}"
          "body.route-people .people-list>li:hover{background:#171d19}"
          "body.route-people .people-list h3{"
          "  grid-column:2;grid-row:1;font:600 clamp(16px,1.65vw,19px)/1.25 'DM Sans',sans-serif;"
          "  letter-spacing:-.015em;min-width:0"
          "}"
          "body.route-people .people-list h3 a{color:#dee4dd}"
          "body.route-people .people-list .people-latest{"
          "  grid-column:2;grid-row:2;margin:0;font:12px/1.6 'DM Sans',sans-serif;color:#c5c9af"
          "}"
          "body.route-people .people-list .people-where{"
          "  grid-column:3;grid-row:1/3;max-width:none;margin:0;text-align:right;"
          "  font:12px/1.6 'DM Sans',sans-serif;color:#c5c9af"
          "}"
          "body.route-people .people-list .people-where a:not(.relationship-link){color:#dee4dd}"
          "body.route-people .people-list .relationship-link{"
          "  display:inline-flex;align-items:center;min-height:30px;white-space:nowrap;"
          "  padding:5px 11px;margin:4px 0 0 10px;background:#252b27;color:#dee4dd;"
          "  border-radius:999px;text-decoration:none;font:600 11px/1.2 'DM Sans',sans-serif"
          "}"
          "body.route-people .people-list .relationship-link:hover{background:#d0fc4f;color:#283500}"
          "body.route-people .people-boundary{"
          "  max-width:80ch;text-align:center;margin:32px auto;color:#c5c9af;"
          "  font:12px/1.75 'DM Sans',sans-serif"
          "}"
          "body.route-people .people-pager{"
          "  display:flex;flex-wrap:wrap;justify-content:center;gap:10px;margin:32px 0"
          "}"
          "body.route-people .people-pager a,body.route-people .people-member-pager a{"
          "  display:inline-flex;align-items:center;border:1px solid #444935;"
          "  border-radius:999px;background:#252b27;color:#dee4dd;"
          "  padding:10px 20px;font:600 12px/1.2 'DM Sans',sans-serif;text-decoration:none"
          "}"
          "body.route-people .people-pager a:hover,body.route-people .people-member-pager a:hover{"
          "  background:#d0fc4f;color:#283500"
          "}"
          "@media(max-width:800px){"
          " body.route-people .people-credit-head{grid-template-columns:1fr}"
          " body.route-people .people-list>li{"
          "   grid-template-columns:28px minmax(0,1fr);"
          "   padding:18px 12px;column-gap:10px"
          " }"
          " body.route-people .people-list h3,body.route-people .people-list .people-latest{grid-column:2}"
          " body.route-people .people-list .people-where{"
          "   grid-column:2;grid-row:3;text-align:left"
          " }"
          " body.route-people .people-list>li::before{grid-row:1/4}"
          " body.route-people .people-sort{justify-content:flex-start}"
          "}"
          "@media(max-width:540px){"
          " body.route-people .people-search{flex-wrap:wrap;border-radius:16px}"
          " body.route-people .people-search button{flex:1}"
          " body.route-people .people-members>li{border-radius:16px}"
          " body.route-people .people-members .relationship-link{margin-left:0}"
          "}"
          "@media(prefers-reduced-motion:reduce){"
          " body.route-people .people-list>li{transition:none}"
          "}"
          "</style>", out);
}

/* The same filter, for the detail continuations that offer a maker. */
int bf_people_name_plausible(const char *s) { return people_name_plausible(s); }
static void people_name_fn(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    (void)argc; sqlite3_result_int(ctx, people_name_plausible((const char *)sqlite3_value_text(argv[0])));
}

static void member_counts(sqlite3 *cms, int *subjects, int *surfaces) {
    sqlite3_stmt *st = NULL;
    *subjects = 0; *surfaces = 0;
    if (!cms || sqlite3_prepare_v2(cms,
          "SELECT COUNT(DISTINCT subject_address), COUNT(*) FROM site_presentation"
          " WHERE namespace='root' AND profile='member-profile-v1' AND status='published' AND subject_address LIKE 'person:%'"
          " AND host LIKE '%.aurekai.%'", -1, &st, NULL) != SQLITE_OK)
        return;
    if (sqlite3_step(st) == SQLITE_ROW) {
        *subjects = sqlite3_column_int(st, 0);
        *surfaces = sqlite3_column_int(st, 1);
    }
    sqlite3_finalize(st);
}


/* The people standing in this territory, with the addresses they stand at.
 *
 * A directory of makers is a directory of names a publisher printed; a member
 * is a different thing entirely -- someone this estate gave an address and a
 * set of surfaces. Both belong on the same page for the same reason anyone
 * can read a profile before making an account: the public half has to be
 * visible without the private half.
 *
 * Only a published presentation appears. Nothing here is a claim that the
 * member made any of the material the estate reads at that address. */
static int members_standing(FILE *out, sqlite3 *cms, const char *needle, unsigned offset, unsigned page_size) {
    sqlite3_stmt *st = NULL; int shown = 0, open = 0, has_next = 0;
    char current[512] = "";
    if (!page_size || page_size > 30) page_size = 30;
    if (offset > 1000000U) return 0;
    if (!cms || sqlite3_prepare_v2(cms,
          "WITH picked AS (SELECT subject_address FROM site_presentation"
          " WHERE namespace='root' AND profile='member-profile-v1' AND status='published'"
          " AND subject_address LIKE 'person:%'"
          " AND host IN (substr(subject_address,8)||'.aurekai.work',substr(subject_address,8)||'.aurekai.life',substr(subject_address,8)||'.aurekai.live',substr(subject_address,8)||'.aurekai.shop')"
          " AND (?1='' OR subject_address LIKE '%'||?1||'%' OR host LIKE '%'||?1||'%')"
          " GROUP BY subject_address ORDER BY subject_address LIMIT ?2 OFFSET ?3)"
          " SELECT s.subject_address,s.host FROM site_presentation s JOIN picked p ON p.subject_address=s.subject_address"
          " WHERE s.namespace='root' AND s.profile='member-profile-v1' AND s.status='published'"
          " AND s.host IN (substr(s.subject_address,8)||'.aurekai.work',substr(s.subject_address,8)||'.aurekai.life',substr(s.subject_address,8)||'.aurekai.live',substr(s.subject_address,8)||'.aurekai.shop')"
          " AND s.host NOT IN (SELECT host FROM site_presentation WHERE namespace='root' AND status='published' AND host IS NOT NULL GROUP BY host HAVING COUNT(DISTINCT subject_address)>1)"
          " ORDER BY s.subject_address,CASE WHEN s.host=substr(s.subject_address,8)||'.aurekai.work' THEN 0 ELSE 1 END,s.host", -1, &st, NULL) != SQLITE_OK)
        return 0;
    sqlite3_bind_text(st,1,needle?needle:"",-1,SQLITE_TRANSIENT);
    sqlite3_bind_int(st,2,(int)page_size+1);
    sqlite3_bind_int(st,3,(int)offset);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *subject = (const char *)sqlite3_column_text(st, 0);
        const char *host = (const char *)sqlite3_column_text(st, 1);
        const char *label;
        if (!subject || !*subject || !host || !*host) continue;
        if (strcmp(current, subject)) {
            if ((unsigned)shown >= page_size) {has_next=1;break;}
            if (open) { fputs("<a class='relationship-link' data-relationship-subject href='https://signin.aurekai.work/relationships?context=self&amp;subject=", out); url(out, current); fputs("'>Open relationship</a></li>", out); }
            if (!shown)
                fputs("<section id='people-addressed' class='region region-members' aria-label='People on Aurekai'>"
                      "<div class='people-section-head'><h2>On Aurekai</h2>"
                      "<p>People who have their own address here, with every page they publish under it.</p></div>"
                      "<ul class='people-members'>", out);
            snprintf(current, sizeof(current), "%s", subject);
            label = strchr(subject, ':');
            label = label && label[1] ? label + 1 : subject;
            char canonical[128] = "";
            people_member_host(cms,subject,canonical,sizeof(canonical));
            fputs("<li data-person-subject='", out); esc(out, subject); fputs("'><strong>", out);
            if(canonical[0]){fputs("<a data-profile-primary href='https://",out);esc(out,canonical);fputs("/'>",out);}
            if (*label >= 'a' && *label <= 'z') { fputc(*label - 32, out); esc(out, label + 1); } else esc(out, label);
            if(canonical[0])fputs("</a>",out);
            fputs("</strong>", out);
            if(canonical[0]){
                fputs("<div data-person-profile-buckets aria-label='Existing public profile content'>",out);
                static const char *const labels[]={"Posts","Gallery","Communities","Organizations"};
                static const char *const glyphs[]={BF_ICON_FILE_TEXT,BF_ICON_PHOTO,BF_ICON_GROUP,BF_ICON_BRIEFCASE};
                for(size_t i=0;i<4;i++){fputs("<span class='person-content-bucket' aria-label='",out);esc(out,labels[i]);fputs("'><span aria-hidden='true'>",out);fputs(glyphs[i],out);fputs("</span>",out);esc(out,labels[i]);fputs("</span>",out);}
                fputs("</div>",out);
            }
            shown++; open = 1;
        }
        fputs("<a data-profile-surface href='https://", out); esc(out, host); fputs("'>", out);
        esc(out, host); fputs("</a>", out);
    }
    sqlite3_finalize(st);
    if (open) { fputs("<a class='relationship-link' data-relationship-subject href='https://signin.aurekai.work/relationships?context=self&amp;subject=", out); url(out, current); fputs("'>Open relationship</a></li>", out); }
    if (shown) {
        fputs("</ul><nav class='people-member-pager' aria-label='More published people'>",out);
        if(offset){fputs("<a href='/people?from=",out);fprintf(out,"%u",offset>page_size?offset-page_size:0);
            if(needle&&*needle){fputs("&amp;q=",out);url(out,needle);}
            fputs("#people-addressed'>Previous people</a>",out);}
        if(has_next){fputs("<a href='/people?from=",out);fprintf(out,"%u",offset+page_size);
            if(needle&&*needle){fputs("&amp;q=",out);url(out,needle);}
            fputs("#people-addressed'>More people</a>",out);}
        fputs("</nav></section>",out);
    }
    return shown;
}

int bf_people_index_ordered(FILE *out, const char *archive_path, const char *route,
                            const char *needle, unsigned offset, unsigned page_size, int recent_first) {
    sqlite3 *db = open_read_only(archive_path); sqlite3_stmt *st = NULL, *where = NULL, *latest = NULL;
    const char *cms_path = getenv("BONFYRE_CMS_DB");
    sqlite3 *cms = cms_path && *cms_path ? open_read_only(cms_path) : NULL;
    int shown = 0, member_shown = 0, total = 0, sources = 0, subjects = 0, surfaces = 0, cms_attached = 0;
    /* Names equal to a publisher's own name are the outlet crediting itself. */
    static const char named_base[] =
        " FROM item WHERE COALESCE(by,'')<>'' AND COALESCE(source,'')<>'' AND (?1='' OR by LIKE '%'||?1||'%')"
        " AND by NOT IN (SELECT publisher FROM item WHERE COALESCE(publisher,'')<>'')"
        /* A dated schedule (a race weekend, a broadcast slot) is not a byline. */
        " AND COALESCE(at,0)<=CAST(strftime('%s','now') AS INTEGER)+86400"
        /* Families whose credit field names an author. Elsewhere the same field
         * holds circuits, recalled firms, agencies, platforms and bands. */
        " AND family IN('news','today','local-news','podcasts','books','culture','music','entertainment','sports','gaming','food','home',"
        "'style','shopping','health','travel','small-business','commons','nature','local-nature','vulnerabilities')";
    char named[2048], sql[4096];
    /* Public member homes belong to CMS, not the foreign reading archive. */
    if (!db) {
        people_presentation(out);
        member_counts(cms,&subjects,&surfaces);
        if(subjects)fprintf(out,"<p class='people-summary' data-people-overview><strong>%d people</strong> with %d published profile pages.</p>",subjects,surfaces);
        shown=members_standing(out,cms,needle,offset,page_size);
        if(cms)sqlite3_close(cms);
        return shown;
    }
    /*
     * When the exact source calls itself by the same string it placed in its
     * maker field, that is a source self-credit rather than evidence of a
     * person. Use only the source owner's exact label/publisher declaration:
     * never fuzzy-match a brand and never join equal names across sources.
     */
    if (cms_path && *cms_path) {
        char uri[PATH_MAX * 3 + 32];
        if (bf_sqlite_readonly_uri(uri, sizeof(uri), cms_path) < (int)sizeof(uri)) {
            char *attach = sqlite3_mprintf("ATTACH DATABASE %Q AS cms_source", uri);
            if (attach && sqlite3_exec(db, attach, NULL, NULL, NULL) == SQLITE_OK) {
                sqlite3_stmt *probe = NULL;
                if (sqlite3_prepare_v2(db,
                      "SELECT 1 FROM cms_source.sqlite_master WHERE type='table' AND name='internet_source'",
                      -1, &probe, NULL) == SQLITE_OK && sqlite3_step(probe) == SQLITE_ROW)
                    cms_attached = 1;
                sqlite3_finalize(probe);
            }
            sqlite3_free(attach);
        }
    }
    /* Materialize the exact source labels once. A correlated scan of the CMS
     * for every archived item saturated all six public workers on /people. */
    const char *source_cte = cms_attached ?
        "WITH source_credits AS MATERIALIZED ("
        "SELECT source,lower(COALESCE(label,'')) AS name FROM cms_source.internet_source WHERE status='published'"
        " UNION ALL SELECT source,lower(COALESCE(publisher_name,'')) FROM cms_source.internet_source WHERE status='published') " : "";
    snprintf(named, sizeof(named), "%s%s", named_base, cms_attached ?
        " AND NOT EXISTS (SELECT 1 FROM source_credits s"
        " WHERE s.source=item.source AND s.name=lower(item.by))" : "");
    if (sqlite3_create_function(db, "person_name", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL, people_name_fn, NULL, NULL) != SQLITE_OK) {
        sqlite3_close(db); if (cms) sqlite3_close(cms); return 0;
    }
    if (page_size > 30) page_size = 30;
    people_presentation(out);

    snprintf(sql, sizeof(sql), "%sSELECT COUNT(*), (SELECT COUNT(DISTINCT source)%s AND person_name(by)) FROM (SELECT by,source%s GROUP BY by,source HAVING person_name(by))", source_cte, named, named);
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, needle ? needle : "", -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) { total = sqlite3_column_int(st, 0); sources = sqlite3_column_int(st, 1); }
    }
    sqlite3_finalize(st); st = NULL;

    if (!offset && !(needle && *needle)) {
        member_counts(cms, &subjects, &surfaces);
        fputs("<p class='people-summary' data-people-overview>", out);
        if (subjects) fprintf(out, "<strong>%d %s</strong> on Aurekai with %d page%s of their own. ", subjects, subjects == 1 ? "person" : "people", surfaces, surfaces == 1 ? "" : "s");
        fprintf(out, "<strong>%d publisher contributor subaccounts</strong> from what this estate reads, across %d source%s.</p>", total, sources, sources == 1 ? "" : "s");
        fputs("<nav class='people-jump' aria-label='People sections'>", out);
        if (subjects) fputs("<a href='#people-addressed'>On Aurekai</a>", out);
        fputs("<a href='#people-credits'>Publisher subaccounts</a><a href='/organisations'>Publishers</a>"
              "<a href='https://signin.aurekai.work/relationships?context=self'>Your relationships</a></nav>", out);
        member_shown=members_standing(out,cms,needle,offset,page_size);
    }
    else member_shown=members_standing(out,cms,needle,offset,page_size);
    if (cms) sqlite3_close(cms);

    snprintf(sql, sizeof(sql), "%sSELECT by, COUNT(*), 1, source%s GROUP BY by,source HAVING person_name(by)"
             " ORDER BY %s, by,source LIMIT ?2 OFFSET ?3", source_cte, named,
             recent_first ? "MAX(COALESCE(at,0)) DESC, COUNT(*) DESC" :
                            "COUNT(*) DESC, MAX(COALESCE(at,0)) DESC");
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, "SELECT source, COALESCE(MAX(NULLIF(publisher,'')),source), COUNT(*) FROM item WHERE by=?1 AND source=?2 AND COALESCE(source,'')<>''"
                               " GROUP BY source ORDER BY COUNT(*) DESC, source LIMIT 4", -1, &where, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, "SELECT source, key, title, COALESCE(strftime('%Y-%m-%d', NULLIF(COALESCE(at,0),0), 'unixepoch'),'')"
                               " FROM item WHERE by=?1 AND source=?2 AND COALESCE(title,'')<>'' ORDER BY COALESCE(at,0) DESC LIMIT 1", -1, &latest, NULL) != SQLITE_OK) {
        sqlite3_finalize(st); sqlite3_finalize(where); sqlite3_close(db); return 0;
    }
    sqlite3_bind_text(st, 1, needle ? needle : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 2, (int)page_size);
    sqlite3_bind_int(st, 3, (int)offset);

    fputs("<section id='people-credits' class='region region-people' aria-label='Publisher subaccounts'>"
          "<div class='people-credit-head'><div><h2>Publisher subaccounts</h2><p>Contributors belong beneath the publisher account that supplied their publication source. "
          "Each source and byline remains a distinct subordinate account until explicitly linked.</p></div>", out);
    fputs("<form class='people-search' role='search' method='get' action='", out); esc(out, route);
    fputs("'><label><input type='search' name='q' value='", out);
    esc(out, needle ? needle : "");
    fputs("' placeholder='Find a name' aria-label='Find a contributor'></label>"
          "<button class='button primary' type='submit'>Find</button></form>", out);
    fputs("<form class='people-sort' method='get' action='",out);esc(out,route);
    fputs("'><label for='people-sort-order'>Order</label><select id='people-sort-order' name='sort'>",out);
    fputs(recent_first ? "<option value='credit'>Most credited</option><option value='recent' selected>Latest observed</option>"
                      : "<option value='credit' selected>Most credited</option><option value='recent'>Latest observed</option>",out);
    fputs("</select>",out);
    if(needle&&*needle){fputs("<input type='hidden' name='q' value='",out);esc(out,needle);fputs("'>",out);}
    fputs("<button type='submit'>Apply order</button></form></div>",out);
    if (needle && *needle) fprintf(out, "<p class='source-count'>%d name%s match.</p>", total, total == 1 ? "" : "s");
    sqlite3_stmt *publisher_account=NULL;
    if(cms_attached)sqlite3_prepare_v2(db,
       "SELECT a.account_id,a.name,s.subaccount_id FROM cms_source.publisher_subaccount s"
       " JOIN cms_source.publisher_account a ON a.account_id=s.publisher_account_id"
       " AND a.namespace=s.namespace WHERE s.namespace='root' AND a.namespace='root'"
       " AND a.status='published' AND s.status='published' AND s.source=?1"
       " AND s.display_name=?2 AND a.source=s.source LIMIT 1",-1,&publisher_account,NULL);
    fprintf(out,"<ol class='people-list' style='counter-reset:stitch-maker %u'>",offset);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(st, 0);
        int made = sqlite3_column_int(st, 1), at = sqlite3_column_int(st, 2), listed = 0;
        if (!name || !*name) continue;
        /* Each exact source credit leads to its own host; equal printed names
         * from different sources remain separate observations. */
        char primary_credit[128]={0};
        const char *sole_source=(const char*)sqlite3_column_text(st,3);
        if(at==1&&sole_source)bf_credit_profile_host(name,sole_source,primary_credit);
        char publisher_name[256]="",publisher_id[96]="",subaccount_id[96]="";
        if(publisher_account && sole_source){
            sqlite3_reset(publisher_account);sqlite3_clear_bindings(publisher_account);
            sqlite3_bind_text(publisher_account,1,sole_source,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(publisher_account,2,name,-1,SQLITE_TRANSIENT);
            if(sqlite3_step(publisher_account)==SQLITE_ROW){
                const char *a=(const char*)sqlite3_column_text(publisher_account,0);
                const char *n=(const char*)sqlite3_column_text(publisher_account,1);
                const char *c=(const char*)sqlite3_column_text(publisher_account,2);
                if(a&&n&&c){snprintf(publisher_id,sizeof(publisher_id),"%s",a);
                    snprintf(publisher_name,sizeof(publisher_name),"%s",n);
                    snprintf(subaccount_id,sizeof(subaccount_id),"%s",c);}
            }
        }
        fputs("<li data-credit-card data-credit-name='", out); esc(out, name);
        fputs("' data-publisher-account='",out);esc(out,publisher_id);
        fputs("' data-publisher-subaccount='",out);esc(out,subaccount_id);
        fputs("'><h3><a href='", out);if(primary_credit[0]){fputs("https://",out);esc(out,primary_credit);fputs("/",out);}else{esc(out,route);fputs("?who=",out);url(out,name);}fputs("' data-profile-host='",out);esc(out,primary_credit);fputs("'>",out);esc(out,name);fputs("</a></h3>",out);
        if(publisher_name[0]){fputs("<p class='people-publisher-parent' data-publisher-name>Under ",out);
            esc(out,publisher_name);fputs(" <span>· publisher account</span></p>",out);}
        sqlite3_reset(latest); sqlite3_bind_text(latest, 1, name, -1, SQLITE_TRANSIENT); sqlite3_bind_text(latest, 2, sole_source, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(latest) == SQLITE_ROW) {
            const char *date = (const char *)sqlite3_column_text(latest, 3);
            fputs("<p class='people-latest'>Latest: <a href='/o/", out); url(out, (const char *)sqlite3_column_text(latest, 0)); fputs("/", out);
            esc(out, (const char *)sqlite3_column_text(latest, 1)); fputs("'>", out); esc(out, (const char *)sqlite3_column_text(latest, 2)); fputs("</a>", out);
            if (date && *date) { fputs(" · <time datetime='", out); esc(out, date); fputs("'>", out); esc(out, date); fputs("</time>", out); }
            fputs("</p>", out);
        }
        fprintf(out, "<p class='people-where'>%d piece%s at ", made, made == 1 ? "" : "s");
        sqlite3_reset(where); sqlite3_bind_text(where, 1, name, -1, SQLITE_TRANSIENT); sqlite3_bind_text(where, 2, sole_source, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(where) == SQLITE_ROW) {
            const char *source = (const char *)sqlite3_column_text(where, 0), *label = (const char *)sqlite3_column_text(where, 1);
            if (listed++) fputs("; ", out);
            if(primary_credit[0]){fputs("<a data-credit-evidence data-credit-source='",out);esc(out,source);fputs("' href='https://",out);esc(out,primary_credit);fputs("/'>",out);}
            esc(out, label && *label ? label : source);if(primary_credit[0])fputs("</a>",out);
            char credited_host[128]={0};
            if(bf_credit_profile_host(name,source,credited_host)){
                fputs(" <a data-credit-profile href='https://",out);esc(out,credited_host);fputs("/' data-credit-host='",out);esc(out,credited_host);
                fputs("' aria-label='Open exact source-attributed profile'>",out);
                fputs(BF_ICON_USER,out);fputs(" Public profile</a>",out);
            }
            fprintf(out, " <span>(%d)</span> <a class='relationship-link' data-relationship-review href='https://signin.aurekai.work/relationships?context=self&amp;who=", sqlite3_column_int(where, 2));
            url(out, name); fputs("&amp;source=", out); url(out, source); fputs("'>keep track</a>", out);
        }
        if (at > listed) fprintf(out, " and %d more", at - listed);
        fputs("</p></li>", out);
        shown++;
    }
    fputs("</ol>", out);
    if (offset || total > (int)(offset + (unsigned)shown)) {
        fputs("<p class='people-pager'>", out);
        if (offset) {
            fputs("<a href='", out); esc(out, route); fputs("?", out);
            if (needle && *needle) { fputs("q=", out); url(out, needle); fputs("&amp;", out); }
            if(recent_first)fputs("sort=recent&amp;",out);fprintf(out, "from=%u'>Previous makers</a>", offset > page_size ? offset - page_size : 0);
        }
        if (total > (int)(offset + (unsigned)shown)) {
            fputs("<a href='", out); esc(out, route); fputs("?", out);
            if (needle && *needle) { fputs("q=", out); url(out, needle); fputs("&amp;", out); }
            if(recent_first)fputs("sort=recent&amp;",out);fprintf(out, "from=%u'>More makers (%d)</a>", offset + (unsigned)shown, total - (int)(offset + (unsigned)shown));
        }
        fputs("</p>", out);
    }
    fputs("<p class='people-boundary' data-identity-boundary>This subordinate publisher account is a public collection, not an authenticated login, verified identity or consent; "
          "and the same name at two sources is not taken to be one person.</p></section>", out);
    sqlite3_finalize(publisher_account);sqlite3_finalize(st); sqlite3_finalize(where); sqlite3_finalize(latest);
    sqlite3_close(db);
    return shown ? shown : member_shown;
}
int bf_people_index(FILE *out, const char *archive_path, const char *route,
                    const char *needle, unsigned offset, unsigned page_size) {
    return bf_people_index_ordered(out, archive_path, route, needle, offset, page_size, 0);
}

int bf_person_page(FILE *out, const char *archive_path, const char *who,
                   const char *source_filter, const char *route) {
    sqlite3 *db = open_read_only(archive_path); sqlite3_stmt *st = NULL;
    int shown = 0, made = 0;
    if (!db || !who || !*who) { if (db) sqlite3_close(db); return 0; }
    people_presentation(out);

    /* Old name-only links cannot denote a person: show the exact source
     * observations available to choose from instead of making an identity
     * assertion by joining equal strings. */
    if (!source_filter || !*source_filter) {
        if (sqlite3_prepare_v2(db,
                "SELECT source, COUNT(*) FROM item WHERE by=?1 AND COALESCE(source,'')<>''"
                " AND COALESCE(at,0)<=CAST(strftime('%s','now') AS INTEGER)+86400"
                " AND by NOT IN(SELECT publisher FROM item WHERE COALESCE(publisher,'')<>'')"
                " AND family IN('news','today','local-news','podcasts','books','culture','music','entertainment','sports','gaming','food','home','style','shopping','health','travel','small-business','commons','nature','local-nature','vulnerabilities')"
                " GROUP BY source ORDER BY COUNT(*) DESC, source LIMIT 60", -1, &st, NULL) != SQLITE_OK) {
            sqlite3_close(db); return 0;
        }
        sqlite3_bind_text(st, 1, who, -1, SQLITE_TRANSIENT);
        fputs("<section class='region region-person' aria-label='Source choices' data-source-chooser>"
              "<div class='people-dossier-head'><p class='people-kicker'>Printed at more than one source</p><h2>", out);
        esc(out, who);
        fputs("</h2><p>Pick the source you mean. Each one is shown on its own; equal spellings are not merged into a person.</p></div>"
              "<ol class='people-source-list'>", out);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *source = (const char *)sqlite3_column_text(st, 0);
            int count = sqlite3_column_int(st, 1);
            if (!source || !*source) continue;
            char public_host[128]={0};
            int qualified=people_name_plausible(who)&&bf_credit_profile_host(who,source,public_host);
            fputs("<li data-source-observation><h3><a",out);
            if(qualified){fputs(" data-credit-profile href='https://",out);esc(out,public_host);fputs("/",out);}
            else{fputs(" href='",out);esc(out,route);fputs("?who=",out);url(out,who);fputs("&amp;source=",out);url(out,source);}
            fputs("'>",out);esc(out,source);
            fprintf(out, "</a></h3><p><span>%d published item%s</span></p>"
                    "<a class='relationship-link' data-relationship-review href='https://signin.aurekai.work/relationships?context=self&amp;who=",
                    count, count == 1 ? "" : "s");
            url(out,who); fputs("&amp;source=",out); url(out,source); fputs("'>Keep track</a></li>",out);
            shown++;
        }
        sqlite3_finalize(st);
        fputs("</ol><p class='people-boundary' data-identity-boundary>This chooser preserves publisher boundaries. "
              "Nothing here asserts that the same spelling at two sources is the same person. <a href='", out); esc(out, route);
        fputs("'>Back to People</a>.</p></section>", out);
        sqlite3_close(db);
        return shown;
    }

    if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM item WHERE by=?1 AND source=?2", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, who, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, source_filter, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) made = sqlite3_column_int(st, 0);
    }
    sqlite3_finalize(st); st = NULL;
    if (!made) { sqlite3_close(db); return 0; }

    fputs("<section class='region region-person people-dossier' aria-label='Source-bound public credit' data-source-dossier>"
          "<div class='people-dossier-head'><p class='people-kicker'>Credited maker</p><h2>", out);
    esc(out, who);
    fputs("</h2><p class='people-credit-source'>Named by <strong>", out); esc(out, source_filter);
    fprintf(out, "</strong> on %d piece%s this estate kept.</p><a class='relationship-link' data-relationship-review href='https://signin.aurekai.work/relationships?context=self&amp;who=",
            made, made == 1 ? "" : "s");
    url(out,who);fputs("&amp;source=",out);url(out,source_filter);fputs("'>Keep track of them</a></div>", out);

    if (sqlite3_prepare_v2(db,
            "SELECT source, key, title, kind, COALESCE(publisher,''),"
            " COALESCE(strftime('%Y-%m-%d', NULLIF(COALESCE(at,0),0), 'unixepoch'),'')"
            " FROM item WHERE by=?1 AND source=?2 ORDER BY COALESCE(at,0) DESC LIMIT 60",
            -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, who, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, source_filter, -1, SQLITE_TRANSIENT);
        fputs("<ol class='people-work-list' aria-label='Their work at this source'>", out);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *source = (const char *)sqlite3_column_text(st, 0);
            const char *key = (const char *)sqlite3_column_text(st, 1);
            const char *title = (const char *)sqlite3_column_text(st, 2);
            const char *kind = (const char *)sqlite3_column_text(st, 3);
            const char *publisher = (const char *)sqlite3_column_text(st, 4);
            const char *date = (const char *)sqlite3_column_text(st, 5);
            if (!title || !*title) continue;
            fputs("<li><a href='/o/", out);
            url(out, source); fputs("/", out); esc(out, key); fputs("'>", out); esc(out, title); fputs("</a><p>", out);
            if (publisher && *publisher) { fputs("<span>", out); esc(out, publisher); fputs("</span>", out); }
            if (kind && *kind) { fputs("<span>", out); esc(out, kind); fputs("</span>", out); }
            if (date && *date) { fputs("<time datetime='", out); esc(out, date); fputs("'>", out); esc(out, date); fputs("</time>", out); }
            fputs("</p></li>", out);
            shown++;
        }
        fputs("</ol>", out);
    }
    sqlite3_finalize(st);
    fputs("<p class='people-boundary' data-identity-boundary>Everything here was published under this exact name by this exact source. "
          "This is publication evidence, not a profile submitted to Aurekai, a verified identity, consent, or an established relationship.</p></section>", out);
    sqlite3_close(db);
    return shown;
}

int bf_organisations_index(FILE *out, const char *ledger_path, const char *route,
                           const char *needle, unsigned offset, unsigned page_size) {
    sqlite3 *db = open_read_only(ledger_path); sqlite3_stmt *st = NULL;
    /* The directory and the addresses are the same fact read twice: the store
     * that publishes a subject's presentation is the only thing that can say
     * whether this estate gave that institution an address. */
    const char *cms_path = getenv("BONFYRE_CMS_DB");
    sqlite3 *cms = cms_path && *cms_path ? open_read_only(cms_path) : NULL;
    int shown = 0, total = 0;
    if (!db) { if (cms) sqlite3_close(cms); return 0; }

    if (sqlite3_prepare_v2(db,
            "SELECT COUNT(*) FROM root r WHERE EXISTS"
            " (SELECT 1 FROM channel c WHERE c.root_id=r.root_id AND c.state IN ('admitted','active'))"
            " AND (?1='' OR r.host LIKE '%'||?1||'%' OR r.name LIKE '%'||?1||'%')",
            -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, needle ? needle : "", -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) total = sqlite3_column_int(st, 0);
    }
    sqlite3_finalize(st); st = NULL;

    if (sqlite3_prepare_v2(db,
            "SELECT r.host, COALESCE(NULLIF(r.name,''), r.host), COALESCE(r.identity,''),"
            " (SELECT COUNT(*) FROM channel c WHERE c.root_id=r.root_id AND c.state IN ('admitted','active')),"
            " (SELECT COUNT(*) FROM channel c WHERE c.root_id=r.root_id)"
            " FROM root r WHERE EXISTS"
            " (SELECT 1 FROM channel c WHERE c.root_id=r.root_id AND c.state IN ('admitted','active'))"
            " AND (?1='' OR r.host LIKE '%'||?1||'%' OR r.name LIKE '%'||?1||'%')"
            " ORDER BY 4 DESC, r.host LIMIT ?2 OFFSET ?3", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db); return 0;
    }
    sqlite3_bind_text(st, 1, needle ? needle : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 2, (int)page_size);
    sqlite3_bind_int(st, 3, (int)offset);

    fputs("<section class='region region-orgs' aria-label='Institutions'>"
          "<h2 class='region-label'>The institutions this estate reads</h2>", out);
    fprintf(out, "<p class='source-count'>%d publishers</p>", total);
    fputs("<form class='ledger-find' method='get' action='", out); esc(out, route);
    fputs("'><label>Find an institution <input type='search' name='q' value='", out);
    esc(out, needle ? needle : "");
    fputs("' placeholder='a name, or a host'></label><button class='button primary' type='submit'>Look</button></form>", out);
    fputs("<ol class='source-roots'>", out);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *host = (const char *)sqlite3_column_text(st, 0);
        const char *name = (const char *)sqlite3_column_text(st, 1);
        const char *identity = (const char *)sqlite3_column_text(st, 2);
        int read = sqlite3_column_int(st, 3), listed = sqlite3_column_int(st, 4);
        char address[160] = {0};
        fputs("<li><a href='/channels?root=", out); url(out, host); fputs("'>", out);
        esc(out, name); fputs("</a><span class='by'>", out);
        fprintf(out, "<span>%d read here</span><span>%d listed</span>", read, listed);
        if (identity && *identity) {
            fputs("<a href='/about/", out); esc(out, identity); fputs("'>About</a>", out);
        }
        if (cms && institution_address(cms, host, address, sizeof(address))) {
            fputs("<a href='https://", out); esc(out, address); fputs("'>", out);
            esc(out, address); fputs("</a>", out);
        }
        fputs("</span></li>", out);
        shown++;
    }
    fputs("</ol>", out);
    if (offset)
        fprintf(out, "<p class='pager'><a href='%s?from=%u'>Back</a></p>", route,
                offset > page_size ? offset - page_size : 0);
    if (total > (int)(offset + (unsigned)shown))
        fprintf(out, "<p class='pager'><a href='%s?from=%u'>More (%d left)</a></p>", route,
                offset + (unsigned)shown, total - (int)(offset + (unsigned)shown));
    fputs("</section>", out);
    if (cms) sqlite3_close(cms);
    sqlite3_finalize(st);
    sqlite3_close(db);
    return shown;
}
