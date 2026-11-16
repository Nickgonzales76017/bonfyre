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
#include "ui_tree.h"
#include "../generated/native_estate_hosts.h"
#include "people_projection.h"
#include "web_store.h"
#include "bf_sqlite_path.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
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

static sqlite3 *open_read_only(const char *path, const BfStoreDurability *policy) {
    return bf_store_open_ro(path, 1500, policy);
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
    sqlite3 *cms=cms_path&&*cms_path?open_read_only(cms_path, &bf_store_policy_cms):NULL;
    if(!cms){if(out&&cap)out[0]=0;return 0;}
    int ok=people_member_host(cms,subject,out,cap);
    sqlite3_close(cms);return ok;
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
            if (open) { fputs("</p></div><a class='ak-btn is-primary' data-relationship-subject href='" BF_ORIGIN_SIGNIN "/relationships?context=self&amp;subject=", out); url(out, current); fputs("'>Open relationship</a></li>", out); }
            if (!shown)
                {ui_start(out,"section");ui_attr_sq(out,"id","people-addressed");ui_attr_sq(out,"class","people-sec");ui_attr_sq(out,"aria-label","People on Aurekai");ui_gt(out);ui_start(out,"header");ui_attr_sq(out,"class","people-head");ui_gt(out);ui_open(out,"h2",NULL);ui_copy(out,"On Aurekai ");ui_open(out,"small",NULL);ui_copy(out,"Estate addresses");ui_close(out,"small");ui_close(out,"h2");ui_open(out,"p",NULL);ui_copy(out,"People who keep their own address here, with every page they publish under it.");ui_close(out,"p");ui_close(out,"header");ui_start(out,"ul");ui_attr_sq(out,"class","people-members");ui_gt(out);}
            snprintf(current, sizeof(current), "%s", subject);
            label = strchr(subject, ':');
            label = label && label[1] ? label + 1 : subject;
            char canonical[128] = "";
            people_member_host(cms,subject,canonical,sizeof(canonical));
            ui_start(out,"li");ui_attr_open_sq(out,"data-person-subject"); esc(out, subject); fputs("'><span class='people-avatar' aria-hidden='true'>", out);
            fputs(BF_ICON_USER, out); ui_close(out,"span");ui_start(out,"div");ui_attr_sq(out,"class","people-member");ui_gt(out);ui_open(out,"strong",NULL);
            if(canonical[0]){ui_start(out,"a");ui_flag(out,"data-profile-primary");ui_attr_open_sq(out,"href");ui_copy(out,"https://");esc(out,canonical);fputs("/'>",out);}
            if (*label >= 'a' && *label <= 'z') { fputc(*label - 32, out); esc(out, label + 1); } else esc(out, label);
            if(canonical[0])ui_close(out,"a");
            ui_close(out,"strong");ui_copy(out," ");ui_start(out,"span");ui_attr_sq(out,"class","ak-tag is-on");ui_gt(out);ui_copy(out,"Resident");ui_close(out,"span");
            if(canonical[0]){
                ui_start(out,"nav");ui_flag(out,"data-person-profile-buckets");ui_attr_sq(out,"aria-label","Public profile");ui_gt(out);
                static const char *const labels[]={"Posts","Gallery","Communities","Organizations"};
                static const char *const tabs[]={"posts","gallery","communities","organizations"};
                for(size_t i=0;i<4;i++){ui_start(out,"a");ui_attr_sq(out,"class","person-content-bucket");ui_flag(out,"data-person-bucket");ui_attr_open_sq(out,"href");ui_copy(out,"https://");esc(out,canonical);fputs("/?tab=",out);fputs(tabs[i],out);fputs("'>",out);esc(out,labels[i]);ui_close(out,"a");}
                ui_close(out,"nav");
            }
            ui_start(out,"p");ui_attr_sq(out,"class","people-hosts");ui_gt(out);
            shown++; open = 1;
        }
        ui_start(out,"a");ui_flag(out,"data-profile-surface");ui_attr_open_sq(out,"href");ui_copy(out,"https://"); esc(out, host); fputs("'>", out);
        esc(out, host); ui_close(out,"a");
    }
    sqlite3_finalize(st);
    if (open) { fputs("</p></div><a class='ak-btn is-primary' data-relationship-subject href='" BF_ORIGIN_SIGNIN "/relationships?context=self&amp;subject=", out); url(out, current); fputs("'>Open relationship</a></li>", out); }
    if (shown) {
        ui_close(out,"ul");ui_start(out,"nav");ui_attr_sq(out,"class","people-pager");ui_attr_sq(out,"aria-label","More published people");ui_gt(out);
        if(offset){ui_start(out,"a");ui_attr_open_sq(out,"href");ui_copy(out,"/people?from=");fprintf(out,"%u",offset>page_size?offset-page_size:0);
            if(needle&&*needle){fputs("&amp;q=",out);url(out,needle);}
            fputs("#people-addressed' class='ak-btn'>Previous people</a>",out);}
        if(has_next){ui_start(out,"a");ui_attr_open_sq(out,"href");ui_copy(out,"/people?from=");fprintf(out,"%u",offset+page_size);
            if(needle&&*needle){fputs("&amp;q=",out);url(out,needle);}
            fputs("#people-addressed' class='ak-btn'>More people</a>",out);}
        ui_close(out,"nav");ui_close(out,"section");
    }
    return shown;
}

/* The directory's aggregate (every credited name across the reading archive,
 * grouped by source) is a pure function of the archive and the CMS it is
 * checked against. It cost a second of SQL on every /people request; each
 * worker now keeps the last few answers, keyed by those stores' identities
 * (main file and WAL), the query, the page and a ten-minute clock (the
 * filter reads "now"). A changed store is a different key. */
typedef struct { char by[512]; char source[256]; int made; } PeopleMemoRow;
typedef struct { char key[1600]; int used, total, sources, rows; PeopleMemoRow row[30]; } PeopleMemo;
static PeopleMemo people_memo[8];
static unsigned people_memo_next;
static size_t people_memo_stat(char *out, size_t cap, size_t used, const char *path) {
    static const char *const tails[] = {"", "-wal"};
    for (size_t i = 0; path && *path && i < 2; i++) {
        char full[1100]; struct stat sb; int n;
        memset(&sb, 0, sizeof(sb));
        if (snprintf(full, sizeof(full), "%s%s", path, tails[i]) >= (int)sizeof(full)) return cap;
        (void)stat(full, &sb);
        n = snprintf(out + used, cap - used, "%s|%lld|%lld|%lld;", full, (long long)sb.st_mtime,
                     (long long)sb.st_size, (long long)sb.st_ino);
        if (n < 0 || (size_t)n >= cap - used) return cap;
        used += (size_t)n;
    }
    return used;
}
static PeopleMemo *people_memo_find(const char *key) {
    for (size_t i = 0; i < sizeof(people_memo) / sizeof(people_memo[0]); i++)
        if (people_memo[i].used && !strcmp(people_memo[i].key, key)) return &people_memo[i];
    return NULL;
}

int bf_people_index_ordered(FILE *out, const char *archive_path, const char *route,
                            const char *needle, unsigned offset, unsigned page_size, int recent_first) {
    sqlite3 *db = open_read_only(archive_path, &bf_store_policy_item_archive); sqlite3_stmt *st = NULL, *where = NULL, *latest = NULL;
    const char *cms_path = getenv("BONFYRE_CMS_DB");
    sqlite3 *cms = cms_path && *cms_path ? open_read_only(cms_path, &bf_store_policy_cms) : NULL;
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
        member_counts(cms,&subjects,&surfaces);
        if(subjects){ui_start(out,"dl");ui_attr_sq(out,"class","ak-stats is-row");ui_flag(out,"data-people-overview");ui_gt(out);ui_open(out,"div",NULL);ui_open(out,"dt",NULL);ui_copy(out,"Estate residents");ui_close(out,"dt");ui_start(out,"dd");ui_attr_sq(out,"class","is-on");ui_gt(out);fprintf(out,"%d",subjects);ui_close(out,"dd");ui_open(out,"small",NULL);fprintf(out,"%d",surfaces);ui_copy(out," published pages");ui_close(out,"small");ui_close(out,"div");ui_close(out,"dl");}
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
    if (cms_path && *cms_path && bf_store_attach(db, cms_path, BF_STORE_WEB_PEOPLE_CMS_ATTACH, BF_STORE_WEB_PEOPLE_CMS_ATTACH_READONLY))
        cms_attached = bf_store_schema_has(db, "cms_source", "internet_source");
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

    char memo_key[1600]; size_t memo_used; PeopleMemo *memo = NULL, fill;
    memo_used = (size_t)snprintf(memo_key, sizeof(memo_key), "%u|%u|%d|%d|%lld|%s|",
                                 offset, page_size, recent_first, cms_attached, (long long)(time(NULL) / 600),
                                 needle ? needle : "");
    if (memo_used < sizeof(memo_key)) memo_used = people_memo_stat(memo_key, sizeof(memo_key), memo_used, archive_path);
    if (memo_used < sizeof(memo_key)) memo_used = people_memo_stat(memo_key, sizeof(memo_key), memo_used, cms_path);
    if (memo_used >= sizeof(memo_key)) memo_key[0] = '\0';
    if (memo_key[0]) memo = people_memo_find(memo_key);
    memset(&fill, 0, sizeof(fill));
    fill.used = memo_key[0] != '\0';

    /* One pass: the sources that printed a plausible name are exactly the
     * sources of the (name, source) groups that pass, so the second scan of
     * the archive the old subquery made counted the same set again. */
    snprintf(sql, sizeof(sql), "%sSELECT COUNT(*), COUNT(DISTINCT source) FROM (SELECT by,source%s GROUP BY by,source HAVING person_name(by))", source_cte, named);
    if (memo) { total = memo->total; sources = memo->sources; }
    else if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, needle ? needle : "", -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) { total = sqlite3_column_int(st, 0); sources = sqlite3_column_int(st, 1); }
    }
    sqlite3_finalize(st); st = NULL;

    if (!offset && !(needle && *needle)) {
        member_counts(cms, &subjects, &surfaces);
        ui_start(out,"dl");ui_attr_sq(out,"class","ak-stats is-row");ui_flag(out,"data-people-overview");ui_gt(out);ui_open(out,"div",NULL);ui_open(out,"dt",NULL);ui_copy(out,"Estate residents");ui_close(out,"dt");ui_start(out,"dd");ui_attr_sq(out,"class","is-on");ui_gt(out);fprintf(out,"%d",subjects);ui_close(out,"dd");ui_open(out,"small",NULL);fprintf(out,"%d",surfaces);ui_copy(out," page");ui_copy(out,surfaces == 1 ? "" : "s");ui_copy(out," of their own");ui_close(out,"small");ui_close(out,"div");ui_open(out,"div",NULL);ui_open(out,"dt",NULL);ui_copy(out,"Credited makers");ui_close(out,"dt");ui_open(out,"dd",NULL);fprintf(out,"%d",total);ui_close(out,"dd");ui_open(out,"small",NULL);ui_copy(out,"names printed by a publisher");ui_close(out,"small");ui_close(out,"div");ui_open(out,"div",NULL);ui_open(out,"dt",NULL);ui_copy(out,"Sources");ui_close(out,"dt");ui_open(out,"dd",NULL);fprintf(out,"%d",sources);ui_close(out,"dd");ui_open(out,"small",NULL);ui_copy(out,"feeds that printed them");ui_close(out,"small");ui_close(out,"div");ui_close(out,"dl");
    }
    ui_start(out,"nav");ui_attr_sq(out,"class","ak-chips");ui_attr_sq(out,"aria-label","People");ui_gt(out);
    ui_start(out,"a");ui_attr_sq(out,"class","ak-chip");ui_attr_sq(out,"href","#people-addressed");ui_gt(out);ui_copy(out,"On Aurekai");ui_close(out,"a");
    ui_start(out,"a");ui_attr_sq(out,"class","ak-chip");ui_attr_sq(out,"aria-current","page");ui_attr_sq(out,"href","#people-credits");ui_gt(out);ui_copy(out,"Credited makers ");ui_open(out,"b",NULL);fprintf(out,"%d",total);ui_close(out,"b");ui_close(out,"a");
    fputs("<a class='ak-chip' href='/organisations'>Publishers</a>"
          "<a class='ak-chip' href='" BF_ORIGIN_SIGNIN "/relationships?context=self'>Your relationships</a></nav>", out);
    member_shown=members_standing(out,cms,needle,offset,page_size);
    if (cms) sqlite3_close(cms);

    snprintf(sql, sizeof(sql), "%sSELECT by, COUNT(*), 1, source%s GROUP BY by,source HAVING person_name(by)"
             " ORDER BY %s, by,source LIMIT ?2 OFFSET ?3", source_cte, named,
             recent_first ? "MAX(COALESCE(at,0)) DESC, COUNT(*) DESC" :
                            "COUNT(*) DESC, MAX(COALESCE(at,0)) DESC");
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, "SELECT COALESCE(MAX(NULLIF(publisher,'')),source) FROM item WHERE by=?1 AND source=?2", -1, &where, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, "SELECT source, key, title, COALESCE(strftime('%Y-%m-%d', NULLIF(COALESCE(at,0),0), 'unixepoch'),''),"
                               " COALESCE(href,'') FROM item WHERE by=?1 AND source=?2 AND COALESCE(title,'')<>'' ORDER BY COALESCE(at,0) DESC LIMIT 1", -1, &latest, NULL) != SQLITE_OK) {
        sqlite3_finalize(st); sqlite3_finalize(where); sqlite3_close(db); return 0;
    }
    sqlite3_bind_text(st, 1, needle ? needle : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 2, (int)page_size);
    sqlite3_bind_int(st, 3, (int)offset);

    ui_start(out,"section");ui_attr_sq(out,"id","people-credits");ui_attr_sq(out,"class","people-sec");ui_attr_sq(out,"aria-label","Credited makers");ui_gt(out);ui_start(out,"header");ui_attr_sq(out,"class","people-head");ui_gt(out);ui_open(out,"div",NULL);ui_open(out,"h2",NULL);ui_copy(out,"Credited makers");ui_close(out,"h2");ui_open(out,"p",NULL);ui_copy(out,"Writers, hosts and makers named on what this estate reads, most credited first. Each source that printed a name is listed and linked on its own.");ui_close(out,"p");ui_close(out,"div");
    fputs("<form class='people-sort' method='get' action='",out);esc(out,route);
    fputs("'><label for='people-sort-order'>Sort</label><select id='people-sort-order' name='sort' onchange='this.form.submit()'>",out);
    fputs(recent_first ? "<option value='credit'>Most credited first</option><option value='recent' selected>Latest first</option>"
                      : "<option value='credit' selected>Most credited first</option><option value='recent'>Latest first</option>",out);
    fputs("</select>",out);
    if(needle&&*needle){fputs("<input type='hidden' name='q' value='",out);esc(out,needle);fputs("'>",out);}
    fputs("<noscript><button type='submit'>Sort</button></noscript></form></header>",out);
    fputs("<form class='people-search' role='search' method='get' action='", out); esc(out, route);
    fputs("'><label>", out); fputs(BF_ICON_SEARCH, out); fputs("<input type='search' name='q' value='", out);
    esc(out, needle ? needle : "");
    fprintf(out, "' placeholder='Find a credited maker or source' aria-label='Find a credited maker'></label>"
            "<span class='people-total'><b>%d</b> %s</span></form>", total, needle && *needle ? "match" : "total");
    sqlite3_stmt *publisher_account=NULL;
    if(cms_attached)sqlite3_prepare_v2(db,
       "SELECT a.account_id,a.name,s.subaccount_id FROM cms_source.publisher_subaccount s"
       " JOIN cms_source.publisher_account a ON a.account_id=s.publisher_account_id"
       " AND a.namespace=s.namespace WHERE s.namespace='root' AND a.namespace='root'"
       " AND a.status='published' AND s.status='published' AND s.source=?1"
       " AND s.display_name=?2 AND a.source=s.source LIMIT 1",-1,&publisher_account,NULL);
    ui_start(out,"ol");ui_attr_sq(out,"class","people-list");ui_gt(out);
    fill.total = total; fill.sources = sources;
    for (int memo_row = 0;; memo_row++) {
        const char *name, *source; int made;
        if (memo) {
            if (memo_row >= memo->rows) break;
            name = memo->row[memo_row].by; source = memo->row[memo_row].source; made = memo->row[memo_row].made;
        } else {
            int rc = sqlite3_step(st);
            if (rc != SQLITE_ROW) { if (rc != SQLITE_DONE) fill.used = 0; break; }
            name = (const char *)sqlite3_column_text(st, 0);
            source = (const char *)sqlite3_column_text(st, 3);
            made = sqlite3_column_int(st, 1);
            if (fill.used && fill.rows < 30 && name && source &&
                strlen(name) < sizeof(fill.row[0].by) && strlen(source) < sizeof(fill.row[0].source)) {
                PeopleMemoRow *r = &fill.row[fill.rows++];
                snprintf(r->by, sizeof(r->by), "%s", name); snprintf(r->source, sizeof(r->source), "%s", source); r->made = made;
            } else fill.used = 0;
        }
        if (!name || !*name || !source) continue;
        /* Each exact source credit leads to its own host; equal printed names
         * from different sources remain separate observations. */
        char primary_credit[128]={0}, label[256]="", initials[8]="";
        bf_credit_profile_host(name,source,primary_credit);
        char publisher_name[256]="",publisher_id[96]="",subaccount_id[96]="";
        if(publisher_account){
            sqlite3_reset(publisher_account);sqlite3_clear_bindings(publisher_account);
            sqlite3_bind_text(publisher_account,1,source,-1,SQLITE_TRANSIENT);
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
        sqlite3_reset(where); sqlite3_bind_text(where, 1, name, -1, SQLITE_TRANSIENT); sqlite3_bind_text(where, 2, source, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(where) == SQLITE_ROW && sqlite3_column_text(where, 0))
            snprintf(label, sizeof(label), "%s", (const char *)sqlite3_column_text(where, 0));
        if (publisher_name[0]) snprintf(label, sizeof(label), "%s", publisher_name);
        {   /* First letters of the first two words, ASCII only. */
            int k = 0, at_word = 1;
            for (const char *c = name; *c && k < 2; c++) {
                if (*c == ' ') { at_word = 1; continue; }
                if (at_word && ((*c >= 'A' && *c <= 'Z') || (*c >= 'a' && *c <= 'z'))) initials[k++] = (char)(*c & ~32);
                at_word = 0;
            }
            initials[k] = 0;
        }
        ui_start(out,"li");ui_flag(out,"data-credit-card");ui_attr_open_sq(out,"data-credit-name"); esc(out, name);
        fputs("' data-publisher-account='",out);esc(out,publisher_id);
        fputs("' data-publisher-subaccount='",out);esc(out,subaccount_id);
        fprintf(out, "'><span class='people-n'>%02u</span><span class='people-avatar' aria-hidden='true'>%s</span><div class='people-who'><h3><a href='",
                offset + (unsigned)shown + 1, initials);
        /* A byline first opens the exact source-cut the static estate exports. */
        esc(out,route);fputs("?who=",out);url(out,name);fputs("&amp;source=",out);url(out,source);
        fputs("' data-profile-host='",out);esc(out,primary_credit);fputs("'>",out);esc(out,name);ui_close(out,"a");
        if (label[0] && strcmp(label, name)) { fputs(" <small>", out); esc(out, label); ui_close(out,"small"); }
        ui_close(out,"h3");
        char domain[128] = "";
        sqlite3_reset(latest); sqlite3_bind_text(latest, 1, name, -1, SQLITE_TRANSIENT); sqlite3_bind_text(latest, 2, source, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(latest) == SQLITE_ROW) {
            const char *href = (const char *)sqlite3_column_text(latest, 4), *h;
            /* The host the source's own link points at, without www. */
            if (href && (h = strstr(href, "://"))) {
                size_t n; h += 3; if (!strncmp(h, "www.", 4)) h += 4;
                n = strcspn(h, "/?#:"); if (n && n < sizeof(domain)) { memcpy(domain, h, n); domain[n] = 0; }
            }
            const char *date = (const char *)sqlite3_column_text(latest, 3);
            ui_start(out,"p");ui_attr_sq(out,"class","people-latest");ui_gt(out);ui_copy(out,"Latest: ");ui_start(out,"a");ui_attr_open_sq(out,"href");ui_copy(out,"/o/"); url(out, (const char *)sqlite3_column_text(latest, 0)); fputs("/", out);
            esc(out, (const char *)sqlite3_column_text(latest, 1)); fputs("'>&ldquo;", out); esc(out, (const char *)sqlite3_column_text(latest, 2)); fputs("&rdquo;</a>", out);
            if (date && *date) { fputs(" <time datetime='", out); esc(out, date); fputs("'>(", out); esc(out, date); fputs(")</time>", out); }
            ui_close(out,"p");
        }
        ui_close(out,"div");ui_start(out,"p");ui_attr_sq(out,"class","people-count");ui_gt(out);ui_open(out,"b",NULL);fprintf(out,"%d",made);ui_copy(out," piece");ui_copy(out,made == 1 ? "" : "s");ui_close(out,"b");ui_open(out,"small",NULL);ui_copy(out,"at ");
        esc(out, domain[0] ? domain : source); ui_close(out,"small");ui_close(out,"p");
        fputs("<a class='people-keep' data-relationship-review href='" BF_ORIGIN_SIGNIN "/relationships?context=self&amp;who=", out);
        url(out, name); fputs("&amp;source=", out); url(out, source); fputs("'>", out); fputs(BF_ICON_BOOKMARK, out); ui_open(out,"span",NULL);ui_copy(out,"keep track");ui_close(out,"span");ui_close(out,"a");ui_close(out,"li");
        shown++;
    }
    ui_close(out,"ol");
    if (offset || total > (int)(offset + (unsigned)shown)) {
        ui_start(out,"nav");ui_attr_sq(out,"class","people-pager");ui_attr_sq(out,"aria-label","More makers");ui_gt(out);
        if (offset) {
            ui_start(out,"a");ui_attr_sq(out,"class","ak-btn");ui_attr_open_sq(out,"href"); esc(out, route); fputs("?", out);
            if (needle && *needle) { fputs("q=", out); url(out, needle); fputs("&amp;", out); }
            if(recent_first)fputs("sort=recent&amp;",out);fprintf(out, "from=%u#people-credits'>Newer</a>", offset > page_size ? offset - page_size : 0);
        }
        if (total > (int)(offset + (unsigned)shown)) {
            ui_start(out,"a");ui_attr_sq(out,"class","ak-btn");ui_attr_open_sq(out,"href"); esc(out, route); fputs("?", out);
            if (needle && *needle) { fputs("q=", out); url(out, needle); fputs("&amp;", out); }
            if(recent_first)fputs("sort=recent&amp;",out);fprintf(out, "from=%u#people-credits'>Older (%d more)</a>", offset + (unsigned)shown, total - (int)(offset + (unsigned)shown));
        }
        ui_close(out,"nav");
    }
    if (!shown) {ui_start(out,"p");ui_attr_sq(out,"class","ak-empty");ui_gt(out);ui_copy(out,"No credited maker matches this view.");ui_close(out,"p");}
    ui_start(out,"p");ui_attr_sq(out,"class","people-boundary");ui_flag(out,"data-identity-boundary");ui_gt(out);ui_copy(out,"A name here is what a publisher printed. It is not an account, a verified identity or consent, and the same name at two sources is not taken to be one person.");ui_close(out,"p");ui_close(out,"section");
    if (!memo && fill.used) {
        snprintf(fill.key, sizeof(fill.key), "%s", memo_key);
        people_memo[people_memo_next++ % (sizeof(people_memo) / sizeof(people_memo[0]))] = fill;
    }
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
    sqlite3 *db = open_read_only(archive_path, &bf_store_policy_item_archive); sqlite3_stmt *st = NULL;
    int shown = 0, made = 0;
    if (!db || !who || !*who) { if (db) sqlite3_close(db); return 0; }

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
        ui_start(out,"section");ui_attr_sq(out,"class","region region-person");ui_attr_sq(out,"aria-label","Source choices");ui_flag(out,"data-source-chooser");ui_gt(out);ui_start(out,"div");ui_attr_sq(out,"class","people-dossier-head");ui_gt(out);ui_start(out,"p");ui_attr_sq(out,"class","people-kicker");ui_gt(out);ui_copy(out,"Printed at more than one source");ui_close(out,"p");ui_open(out,"h2",NULL);
        esc(out, who);
        ui_close(out,"h2");ui_open(out,"p",NULL);ui_copy(out,"Pick the source you mean. Each one is shown on its own; equal spellings are not merged into a person.");ui_close(out,"p");ui_close(out,"div");ui_start(out,"ol");ui_attr_sq(out,"class","people-source-list");ui_gt(out);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *source = (const char *)sqlite3_column_text(st, 0);
            int count = sqlite3_column_int(st, 1);
            if (!source || !*source) continue;
            char public_host[128]={0};
            int qualified=people_name_plausible(who)&&bf_credit_profile_host(who,source,public_host);
            ui_start(out,"li");ui_flag(out,"data-source-observation");ui_gt(out);ui_open(out,"h3",NULL);ui_start(out,"a");
            if(qualified){fputs(" data-credit-profile href='https://",out);esc(out,public_host);fputs("/",out);}
            else{fputs(" href='",out);esc(out,route);fputs("?who=",out);url(out,who);fputs("&amp;source=",out);url(out,source);}
            fputs("'>",out);esc(out,source);
            fprintf(out, "</a></h3><p><span>%d published item%s</span></p>"
                    "<a class='relationship-link' data-relationship-review href='" BF_ORIGIN_SIGNIN "/relationships?context=self&amp;who=",
                    count, count == 1 ? "" : "s");
            url(out,who); fputs("&amp;source=",out); url(out,source); fputs("'>Keep track</a></li>",out);
            shown++;
        }
        sqlite3_finalize(st);
        ui_close(out,"ol");ui_start(out,"p");ui_attr_sq(out,"class","people-boundary");ui_flag(out,"data-identity-boundary");ui_gt(out);ui_copy(out,"This chooser preserves publisher boundaries. Nothing here asserts that the same spelling at two sources is the same person. ");ui_start(out,"a");ui_attr_open_sq(out,"href"); esc(out, route);
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

    ui_start(out,"section");ui_attr_sq(out,"class","region region-person people-dossier");ui_attr_sq(out,"aria-label","Source-bound public credit");ui_flag(out,"data-source-dossier");ui_gt(out);ui_start(out,"div");ui_attr_sq(out,"class","people-dossier-head");ui_gt(out);ui_start(out,"p");ui_attr_sq(out,"class","people-kicker");ui_gt(out);ui_copy(out,"Credited maker");ui_close(out,"p");ui_open(out,"h2",NULL);
    esc(out, who);
    ui_close(out,"h2");ui_start(out,"p");ui_attr_sq(out,"class","people-credit-source");ui_gt(out);ui_copy(out,"Named by ");ui_open(out,"strong",NULL); esc(out, source_filter);
    fprintf(out, "</strong> on %d piece%s this estate kept.</p><a class='relationship-link' data-relationship-review href='" BF_ORIGIN_SIGNIN "/relationships?context=self&amp;who=",
            made, made == 1 ? "" : "s");
    url(out,who);fputs("&amp;source=",out);url(out,source_filter);fputs("'>Keep track of them</a></div>", out);

    if (sqlite3_prepare_v2(db,
            "SELECT source, key, title, kind, COALESCE(publisher,''),"
            " COALESCE(strftime('%Y-%m-%d', NULLIF(COALESCE(at,0),0), 'unixepoch'),'')"
            " FROM item WHERE by=?1 AND source=?2 ORDER BY COALESCE(at,0) DESC LIMIT 60",
            -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, who, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, source_filter, -1, SQLITE_TRANSIENT);
        ui_start(out,"ol");ui_attr_sq(out,"class","people-work-list");ui_attr_sq(out,"aria-label","Their work at this source");ui_gt(out);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *source = (const char *)sqlite3_column_text(st, 0);
            const char *key = (const char *)sqlite3_column_text(st, 1);
            const char *title = (const char *)sqlite3_column_text(st, 2);
            const char *kind = (const char *)sqlite3_column_text(st, 3);
            const char *publisher = (const char *)sqlite3_column_text(st, 4);
            const char *date = (const char *)sqlite3_column_text(st, 5);
            if (!title || !*title) continue;
            ui_open(out,"li",NULL);ui_start(out,"a");ui_attr_open_sq(out,"href");ui_copy(out,"/o/");
            url(out, source); fputs("/", out); esc(out, key); fputs("'>", out); esc(out, title); ui_close(out,"a");ui_open(out,"p",NULL);
            if (publisher && *publisher) { ui_open(out,"span",NULL); esc(out, publisher); ui_close(out,"span"); }
            if (kind && *kind) { ui_open(out,"span",NULL); esc(out, kind); ui_close(out,"span"); }
            if (date && *date) { ui_start(out,"time");ui_attr_open_sq(out,"datetime"); esc(out, date); fputs("'>", out); esc(out, date); ui_close(out,"time"); }
            ui_close(out,"p");ui_close(out,"li");
            shown++;
        }
        ui_close(out,"ol");
    }
    sqlite3_finalize(st);
    ui_start(out,"p");ui_attr_sq(out,"class","people-boundary");ui_flag(out,"data-identity-boundary");ui_gt(out);ui_copy(out,"Everything here was published under this exact name by this exact source. This is publication evidence, not a profile submitted to Aurekai, a verified identity, consent, or an established relationship.");ui_close(out,"p");ui_close(out,"section");
    sqlite3_close(db);
    return shown;
}

int bf_organisations_index(FILE *out, const char *ledger_path, const char *route,
                           const char *needle, unsigned offset, unsigned page_size) {
    sqlite3 *db = open_read_only(ledger_path, &bf_store_policy_source_frontier); sqlite3_stmt *st = NULL;
    /* The directory and the addresses are the same fact read twice: the store
     * that publishes a subject's presentation is the only thing that can say
     * whether this estate gave that institution an address. */
    const char *cms_path = getenv("BONFYRE_CMS_DB");
    sqlite3 *cms = cms_path && *cms_path ? open_read_only(cms_path, &bf_store_policy_cms) : NULL;
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

    ui_start(out,"section");ui_attr_sq(out,"class","region region-orgs");ui_attr_sq(out,"aria-label","Institutions");ui_gt(out);ui_start(out,"h2");ui_attr_sq(out,"class","region-label");ui_gt(out);ui_copy(out,"The institutions this estate reads");ui_close(out,"h2");
    ui_start(out,"p");ui_attr_sq(out,"class","source-count");ui_gt(out);fprintf(out,"%d",total);ui_copy(out," publishers");ui_close(out,"p");
    fputs("<form class='ledger-find' method='get' action='", out); esc(out, route);
    fputs("'><label>Find an institution <input type='search' name='q' value='", out);
    esc(out, needle ? needle : "");
    fputs("' placeholder='a name, or a host'></label><button class='button primary' type='submit'>Look</button></form>", out);
    ui_start(out,"ol");ui_attr_sq(out,"class","source-roots");ui_gt(out);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *host = (const char *)sqlite3_column_text(st, 0);
        const char *name = (const char *)sqlite3_column_text(st, 1);
        const char *identity = (const char *)sqlite3_column_text(st, 2);
        int read = sqlite3_column_int(st, 3), listed = sqlite3_column_int(st, 4);
        char address[160] = {0};
        ui_open(out,"li",NULL);ui_start(out,"a");ui_attr_open_sq(out,"href");ui_copy(out,"/channels?root="); url(out, host); fputs("'>", out);
        esc(out, name); ui_close(out,"a");ui_start(out,"span");ui_attr_sq(out,"class","by");ui_gt(out);
        ui_open(out,"span",NULL);fprintf(out,"%d",read);ui_copy(out," read here");ui_close(out,"span");ui_open(out,"span",NULL);fprintf(out,"%d",listed);ui_copy(out," listed");ui_close(out,"span");
        if (identity && *identity) {
            ui_start(out,"a");ui_attr_open_sq(out,"href");ui_copy(out,"/about/"); esc(out, identity); fputs("'>About</a>", out);
        }
        if (cms && institution_address(cms, host, address, sizeof(address))) {
            ui_start(out,"a");ui_attr_open_sq(out,"href");ui_copy(out,"https://"); esc(out, address); fputs("'>", out);
            esc(out, address); ui_close(out,"a");
        }
        ui_close(out,"span");ui_close(out,"li");
        shown++;
    }
    ui_close(out,"ol");
    if (offset)
        {ui_start(out,"p");ui_attr_sq(out,"class","pager");ui_gt(out);ui_start(out,"a");ui_attr_open_sq(out,"href");ui_copy(out,route);ui_copy(out,"?from=");fprintf(out,"%u",offset > page_size ? offset - page_size : 0);ui_attr_close_sq(out);ui_gt(out);ui_copy(out,"Back");ui_close(out,"a");ui_close(out,"p");}
    if (total > (int)(offset + (unsigned)shown))
        {ui_start(out,"p");ui_attr_sq(out,"class","pager");ui_gt(out);ui_start(out,"a");ui_attr_open_sq(out,"href");ui_copy(out,route);ui_copy(out,"?from=");fprintf(out,"%u",offset + (unsigned)shown);ui_attr_close_sq(out);ui_gt(out);ui_copy(out,"More (");fprintf(out,"%d",total - (int)(offset + (unsigned)shown));ui_copy(out," left)");ui_close(out,"a");ui_close(out,"p");}
    ui_close(out,"section");
    if (cms) sqlite3_close(cms);
    sqlite3_finalize(st);
    sqlite3_close(db);
    return shown;
}
