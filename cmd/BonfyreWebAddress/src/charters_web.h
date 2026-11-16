#ifndef BONFYRE_CHARTERS_WEB_H
#define BONFYRE_CHARTERS_WEB_H
#include "ui_tree.h"
/* Community charters and SOPs are wiki_page rows in the root namespace whose
 * route is charters/<community>/<slug>. A public local community's active
 * members write and revise them; everyone reads them. Revisions are the CMS
 * operation log (_ops) for that row, so the history is the store's own, not a
 * copy kept by this page. */

typedef struct { char op[16], request[33], community[97], target[161], title[161], text[16001], note[401]; } ChForm;

static int ch_slug_ok(const char *s) {
    return s && *s && strlen(s) <= 60 && strspn(s, "abcdefghijklmnopqrstuvwxyz0123456789-") == strlen(s);
}
static void ch_slug(const char *title, const char *request, char out[64]) {
    size_t n = 0; int dash = 0;
    for (const unsigned char *p = (const unsigned char *)title; *p && n < 48; p++) {
        if (isalnum(*p)) { out[n++] = (char)tolower(*p); dash = 0; }
        else if (n && !dash) { out[n++] = '-'; dash = 1; }
    }
    while (n && out[n - 1] == '-') n--;
    if (!n) { snprintf(out, 64, "charter-%.8s", request); return; }
    out[n] = 0;
}
static void ch_body(const char *content) {
    /* Paragraphs split on blank lines; a line beginning "# " or "## " is a
     * section heading. Text only: nothing in a charter is executed. */
    const char *p = content; char line[4096];
    int open = 0;
    while (p && *p) {
        const char *end = strchr(p, '\n'); size_t n = end ? (size_t)(end - p) : strlen(p);
        if (n >= sizeof(line)) n = sizeof(line) - 1;
        memcpy(line, p, n); line[n] = 0;
        if (n && line[n - 1] == '\r') line[--n] = 0;
        if (!n) { if (open) { ui_close(stdout,"p"); open = 0; } }
        else if (!strncmp(line, "## ", 3) || !strncmp(line, "# ", 2)) {
            if (open) { ui_close(stdout,"p"); open = 0; }
            ui_text(stdout, "h2", NULL, line + (line[1] == '#' ? 3 : 2));
        } else {
            if (!open) { ui_open(stdout,"p",NULL); open = 1; } else ui_open(stdout,"br",NULL);
            html(line);
        }
        p = end ? end + 1 : NULL;
    }
    if (open) ui_close(stdout,"p");
}
static void ch_open(const char *title, int personal) {
    ak_app_http(200, personal);
    ak_app_open(title, "workbench", "route-charters");
    ui_start(stdout,"nav");ui_attr(stdout,"class","ak-wb-strip");ui_attr(stdout,"aria-label","Commons");ui_gt(stdout);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Knowledge commons");ui_close(stdout,"span");ui_start(stdout,"a");ui_attr(stdout,"href","/charters");ui_gt(stdout);ui_copy(stdout,"All charters");ui_close(stdout,"a");ui_start(stdout,"a");ui_attr(stdout,"href","/guilds");ui_gt(stdout);ui_copy(stdout,"Guilds");ui_close(stdout,"a");ui_start(stdout,"a");ui_attr(stdout,"href","/communities");ui_gt(stdout);ui_copy(stdout,"Communities");ui_close(stdout,"a");ui_open(stdout,"span","ak-live");ui_copy(stdout,"Versioned · revisions kept by the store");ui_close(stdout,"span");ui_close(stdout,"nav");
}
static void ch_index(sqlite3 *db, const char *actor, const char *community, const char *q) {
    char nonce[33];
    ch_open("Charters and SOPs", *actor != 0);
    ui_open(stdout,"header","ak-wb-hero is-solo");ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Living wiki");ui_close(stdout,"span");ui_open(stdout,"h1",NULL);ui_copy(stdout,"Charters, covenants and working procedures");ui_close(stdout,"h1");ui_open(stdout,"p","ak-lede");ui_copy(stdout,"What each community has written down about how it works: its charter, its standing procedures, its recipes. Members revise them; every revision is kept.");ui_close(stdout,"p");ui_close(stdout,"div");ui_close(stdout,"header");
    {
        BfAuthoringTree *tree = calloc(1, sizeof(*tree));
        FtField hidden[1] = {{"community", community}};
        if (tree && ft_action(tree, "ch-search", NULL, 0, "Search", "get", "/charters", hidden, *community ? 1 : 0) &&
            ft_field(tree, "ch-q", "ch-search", 0, "Search the commons", "search", "q", q, "", 0, "", "120", 0, ""))
            ft_render(tree, stdout);
        else if (tree) { bf_authoring_tree_clear(tree); free(tree); }
    }
    sqlite3_stmt *s = NULL;
    ui_start(stdout,"nav");ui_attr(stdout,"class","ak-chips");ui_attr(stdout,"aria-label","Communities");ui_gt(stdout);ui_start(stdout,"a");ui_attr(stdout,"class","ak-chip");ui_attr(stdout,"href","/charters");
    if (!*community) fputs(" aria-current=\"page\"", stdout);
    printf("><b>%.0f</b> All charters</a>", gr_number(db, "SELECT count(*) FROM wiki_page WHERE namespace='root' AND route LIKE 'charters/%' AND published=1", NULL, NULL));
    if (sqlite3_prepare_v2(db, "SELECT w.community,c.title,count(*) FROM wiki_page w JOIN community_body c ON c.namespace='root' AND c.community=w.community AND c.status='published' AND c.visibility='public' WHERE w.namespace='root' AND w.route LIKE 'charters/%' AND w.published=1 GROUP BY w.community ORDER BY 3 DESC LIMIT 12", -1, &s, NULL) == SQLITE_OK) {
        while (sqlite3_step(s) == SQLITE_ROW) {
            const char *k = (const char *)sqlite3_column_text(s, 0);
            ui_start(stdout,"a");ui_attr(stdout,"class","ak-chip");ui_attr_open(stdout,"href");ui_copy(stdout,"/charters?community="); url_query_value(k); fputc('"', stdout);
            if (!strcmp(k, community)) fputs(" aria-current=\"page\"", stdout);
            printf("><b>%d</b> ", sqlite3_column_int(s, 2)); html((const char *)sqlite3_column_text(s, 1)); ui_close(stdout,"a");
        }
    }
    sqlite3_finalize(s);
    ui_close(stdout,"nav");

    char like[200]; snprintf(like, sizeof(like), "%%%s%%", q);
    const char *sql = "SELECT w.id,w.title,w.route,substr(w.content,1,600),COALESCE(c.title,''),(SELECT count(*) FROM _ops o WHERE o.target='wiki_page' AND o.target_id=w.id),COALESCE(w.updated_at,w.created_at) FROM wiki_page w LEFT JOIN community_body c ON c.namespace='root' AND c.community=w.community WHERE w.namespace='root' AND w.route LIKE 'charters/%' AND w.published=1 AND (?1='' OR w.community=?1) AND (?2='%%' OR w.title LIKE ?2 OR w.content LIKE ?2) ORDER BY COALESCE(w.updated_at,w.created_at) DESC,w.id DESC LIMIT 40";
    int shown = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, community, -1, SQLITE_TRANSIENT); sqlite3_bind_text(s, 2, like, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            const char *route = (const char *)sqlite3_column_text(s, 2);
            if (!shown++) {
                ui_open(stdout,"section","ak-sec");ui_open(stdout,"div","ak-panel is-raised");ui_open(stdout,"header",NULL);ui_open(stdout,"span","ak-tag is-on");ui_copy(stdout,"Most recently revised");ui_close(stdout,"span");
                ui_open(stdout,"span","ak-kicker");fprintf(stdout,"%d",sqlite3_column_int(s, 5));ui_copy(stdout," revisions");ui_close(stdout,"span");ui_close(stdout,"header");ui_open(stdout,"span","ak-kicker");
                html((const char *)sqlite3_column_text(s, 4)); ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/"); html(route); fputs("\">", stdout);
                html((const char *)sqlite3_column_text(s, 1)); ui_close(stdout,"a");ui_close(stdout,"h2");ui_open(stdout,"div","ak-lede");
                ch_body((const char *)sqlite3_column_text(s, 3));
                ui_close(stdout,"div");ui_open(stdout,"p",NULL);ui_start(stdout,"a");ui_attr(stdout,"class","ak-btn");ui_attr_open(stdout,"href");ui_copy(stdout,"/"); html(route); fputs("\">Read and revise</a></p></div></section>", stdout);
                ui_open(stdout,"section","ak-sec");ui_open(stdout,"header",NULL);ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Index");ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Knowledge tree");ui_close(stdout,"h2");ui_close(stdout,"div");ui_close(stdout,"header");ui_open(stdout,"ul","ak-rows");
                continue;
            }
            ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/"); html(route); fputs("\"><strong>", stdout); html((const char *)sqlite3_column_text(s, 1));
            ui_close(stdout,"strong");ui_open(stdout,"small",NULL); html((const char *)sqlite3_column_text(s, 4));
            printf(" · %d revisions · ", sqlite3_column_int(s, 5)); html((const char *)sqlite3_column_text(s, 6));
            ui_close(stdout,"small");ui_open(stdout,"span","ak-go");ui_copy(stdout,"Open");ui_close(stdout,"span");ui_close(stdout,"a");
        }
    }
    sqlite3_finalize(s);
    if (shown) { ui_close(stdout,"ul"); if (shown == 1) ui_text(stdout, "p", "ak-empty", "No other charters here yet."); ui_close(stdout,"section"); }
    else ui_text(stdout, "p", "ak-empty", "No community has written a charter here yet.");

    ui_open(stdout,"section","ak-sec");ui_open(stdout,"header",NULL);ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Lineage");ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Recent revisions");ui_close(stdout,"h2");ui_close(stdout,"div");ui_close(stdout,"header");ui_open(stdout,"ul","ak-rows");
    shown = 0;
    if (sqlite3_prepare_v2(db, "SELECT w.title,w.route,o.ts,COALESCE(json_extract(o.payload,'$.edited_by'),''),COALESCE(json_extract(o.payload,'$.revision_note'),'') FROM _ops o JOIN wiki_page w ON w.id=o.target_id AND w.namespace='root' AND w.route LIKE 'charters/%' AND w.published=1 WHERE o.target='wiki_page' AND o.namespace='root' AND (?1='' OR w.community=?1) ORDER BY o.ts DESC LIMIT 12", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, community, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            shown++;
            ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/"); html((const char *)sqlite3_column_text(s, 1)); fputs("#revisions\"><strong>", stdout);
            html((const char *)sqlite3_column_text(s, 0)); ui_close(stdout,"strong");ui_open(stdout,"small",NULL);
            html(*(const char *)sqlite3_column_text(s, 4) ? (const char *)sqlite3_column_text(s, 4) : "Revised");
            fputs(" · ", stdout); gr_person((const char *)sqlite3_column_text(s, 3), actor, NULL);
            ui_close(stdout,"small");ui_open(stdout,"span","ak-go"); html((const char *)sqlite3_column_text(s, 2)); ui_close(stdout,"span");ui_close(stdout,"a");
        }
    }
    sqlite3_finalize(s);
    ui_close(stdout,"ul");
    if (!shown) ui_text(stdout, "p", "ak-empty", "No revisions yet.");
    ui_close(stdout,"section");

    CjrCommunity c;
    if (*community && cjr_community(db, community, &c) == 1 && cjr_public(&c) && gr_active(db, &c, actor)) {
        ph_nonce(nonce);
        FtField f[3] = {{"op", "write"}, {"request", nonce}, {"community", c.key}};
        GrIn in[] = {
            {"Title", "text", "title", "", "", 0, "160", 1, ""},
            {"Text (a line starting with # is a section heading)", "text", "text", "", "", 1, "16000", 1, ""},
            {"What this first version says", "text", "note", "First version", "", 0, "400", 0, ""},
        };
        ui_start(stdout,"section");ui_attr(stdout,"class","ak-sec");ui_attr(stdout,"id","write");ui_gt(stdout);ui_open(stdout,"div","ak-ask");ui_open(stdout,"span","ak-kicker"); html(c.title);
        ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Write a charter or procedure");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Members of this community can write and revise it. It is public, like the community.");ui_close(stdout,"p");
        gr_form("ch-write", "Publish charter", "/charters/act", f, 3, in, 3, NULL, NULL, NULL, 0);
        ui_close(stdout,"div");ui_close(stdout,"section");
    } else if (*actor) {
        ui_open(stdout,"section","ak-band");ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Contribute");ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Write your community's charter");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Choose a community you are an active member of.");ui_close(stdout,"p");ui_close(stdout,"div");ui_open(stdout,"div","ak-actions");
        int any = 0;
        if (sqlite3_prepare_v2(db, "SELECT c.community,c.title FROM community_body c WHERE c.namespace='root' AND c.status='published' AND c.visibility='public' AND (c.created_by_subject=?1 OR EXISTS(SELECT 1 FROM community_membership m WHERE m.namespace='root' AND m.community=c.community AND m.member_subject=?1 AND m.state='active' AND m.status='published')) ORDER BY c.id DESC LIMIT 6", -1, &s, NULL) == SQLITE_OK) {
            sqlite3_bind_text(s, 1, actor, -1, SQLITE_TRANSIENT);
            while (sqlite3_step(s) == SQLITE_ROW) {
                any++;
                ui_start(stdout,"a");ui_attr(stdout,"class","ak-btn");ui_attr_open(stdout,"href");ui_copy(stdout,"/charters?community="); url_query_value((const char *)sqlite3_column_text(s, 0));
                fputs("#write\">", stdout); html((const char *)sqlite3_column_text(s, 1)); ui_close(stdout,"a");
            }
        }
        sqlite3_finalize(s);
        if (!any) {ui_start(stdout,"a");ui_attr(stdout,"class","ak-btn");ui_attr(stdout,"href","/communities");ui_gt(stdout);ui_copy(stdout,"Find a community");ui_close(stdout,"a");}
        ui_close(stdout,"div");ui_close(stdout,"section");
    } else {
        ui_open(stdout,"section","ak-band");ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Contribute");ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Write your community's charter");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Members write and revise their community's charters.");ui_close(stdout,"p");ui_close(stdout,"div");ui_open(stdout,"div","ak-actions");ui_start(stdout,"a");ui_attr(stdout,"class","ak-btn");ui_attr(stdout,"href","/login?next=%2Fcharters");ui_gt(stdout);ui_copy(stdout,"Sign in");ui_close(stdout,"a");ui_close(stdout,"div");ui_close(stdout,"section");
    }
    ak_app_close();
}
static void ch_page(sqlite3 *db, const char *actor, const char *route) {
    sqlite3_stmt *s = NULL; char nonce[33];
    if (sqlite3_prepare_v2(db, "SELECT w.id,w.title,w.content,COALESCE(w.community,''),COALESCE(c.title,''),COALESCE(w.updated_at,w.created_at),COALESCE(w.edited_by,'') FROM wiki_page w LEFT JOIN community_body c ON c.namespace='root' AND c.community=w.community WHERE w.namespace='root' AND w.route=?1 AND w.published=1 LIMIT 1", -1, &s, NULL) != SQLITE_OK ||
        (sqlite3_bind_text(s, 1, route, -1, SQLITE_TRANSIENT), sqlite3_step(s)) != SQLITE_ROW) {
        sqlite3_finalize(s); ph_error(404, "There is no charter at this address.\n"); return;
    }
#define CHC(i) ((const char *)sqlite3_column_text(s, i))
    long long id = sqlite3_column_int64(s, 0);
    char idtext[24]; snprintf(idtext, sizeof(idtext), "%lld", id);
    int revisions = (int)gr_number(db, "SELECT count(*) FROM _ops WHERE target='wiki_page' AND target_id=CAST(?1 AS INTEGER)", idtext, NULL);
    ch_open(CHC(1), *actor != 0);
    ui_open(stdout,"header","ak-wb-hero");ui_open(stdout,"div",NULL);ui_open(stdout,"p","ak-actions");ui_open(stdout,"span","ak-tag is-on");ui_copy(stdout,"Community charter");ui_close(stdout,"span");ui_open(stdout,"span","ak-tag");
    html(CHC(4)); ui_close(stdout,"span");ui_open(stdout,"span","ak-tag");fprintf(stdout,"%d",revisions);ui_copy(stdout," revisions");ui_close(stdout,"span");ui_close(stdout,"p");ui_open(stdout,"h1",NULL); html(CHC(1));
    ui_close(stdout,"h1");ui_open(stdout,"p","ak-lede");ui_copy(stdout,"Last revised "); html(CHC(5));
    if (*CHC(6)) { fputs(" by ", stdout); gr_person(CHC(6), actor, NULL); }
    fputs(".</p></div><aside class=\"ak-panel\"><span class=\"ak-kicker\">Kept by</span><h3>", stdout); html(CHC(4));
    ui_close(stdout,"h3");ui_open(stdout,"p",NULL);ui_copy(stdout,"Its active members can revise this text. Every revision stays in the record below.");ui_close(stdout,"p");ui_open(stdout,"p",NULL);ui_start(stdout,"a");ui_attr(stdout,"class","ak-btn");ui_attr_open(stdout,"href");ui_copy(stdout,"/community?community=");
    url_query_value(CHC(3)); fputs("\">Open the community</a></p></aside></header>", stdout);
    ui_start(stdout,"article");ui_attr(stdout,"class","ak-panel");ui_attr(stdout,"style","max-width:78ch");ui_gt(stdout); ch_body(CHC(2)); ui_close(stdout,"article");
    ui_start(stdout,"section");ui_attr(stdout,"class","ak-sec");ui_attr(stdout,"id","revisions");ui_gt(stdout);ui_open(stdout,"header",NULL);ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Lineage ledger");ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Revisions");ui_close(stdout,"h2");ui_close(stdout,"div");ui_close(stdout,"header");ui_open(stdout,"ul","ak-rows");
    sqlite3_stmt *o = NULL;
    if (sqlite3_prepare_v2(db, "SELECT ts,op_type,COALESCE(json_extract(payload,'$.edited_by'),''),COALESCE(json_extract(payload,'$.revision_note'),''),substr(hash,1,12) FROM _ops WHERE target='wiki_page' AND target_id=?1 ORDER BY ts DESC,id DESC LIMIT 50", -1, &o, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(o, 1, id);
        while (sqlite3_step(o) == SQLITE_ROW) {
            ui_open(stdout,"li",NULL);ui_open(stdout,"strong",NULL);
            const char *note = (const char *)sqlite3_column_text(o, 3);
            html(*note ? note : !strcmp((const char *)sqlite3_column_text(o, 1), "create") ? "First version" : "Revised");
            ui_close(stdout,"strong");ui_open(stdout,"small",NULL); gr_person((const char *)sqlite3_column_text(o, 2), actor, NULL);
            fputs(" · ", stdout); html((const char *)sqlite3_column_text(o, 0)); ui_close(stdout,"small");ui_open(stdout,"span","ak-go");ui_open(stdout,"code",NULL);
            html((const char *)sqlite3_column_text(o, 4)); ui_close(stdout,"code");ui_close(stdout,"span");ui_close(stdout,"li");
        }
    }
    sqlite3_finalize(o);
    ui_close(stdout,"ul");ui_close(stdout,"section");
    CjrCommunity c;
    if (*CHC(3) && cjr_community(db, CHC(3), &c) == 1 && cjr_public(&c) && gr_active(db, &c, actor)) {
        ph_nonce(nonce);
        FtField f[4] = {{"op", "revise"}, {"request", nonce}, {"community", c.key}, {"target", route}};
        GrIn in[] = {
            {"Title", "text", "title", CHC(1), "", 0, "160", 1, ""},
            {"Text", "text", "text", CHC(2), "", 1, "16000", 1, ""},
            {"What you changed", "text", "note", "", "", 0, "400", 1, ""},
        };
        ui_open(stdout,"section","ak-sec");ui_open(stdout,"div","ak-ask");ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Members only");ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Revise this charter");ui_close(stdout,"h2");
        gr_form("ch-revise", "Save revision", "/charters/act", f, 4, in, 3, NULL, NULL, NULL, 0);
        ui_close(stdout,"div");ui_close(stdout,"section");
    }
#undef CHC
    sqlite3_finalize(s);
    ak_app_close();
}
static char *ch_json(const ChForm *p, const char *route, const char *actor, int creating) {
    bf_jsonw w; char *out = NULL;
    bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "title"); bf_jsonw_str(&w, p->title);
    if (creating) { bf_jsonw_key(&w, "route"); bf_jsonw_str(&w, route); bf_jsonw_key(&w, "community"); bf_jsonw_str(&w, p->community);
                    bf_jsonw_key(&w, "published"); bf_jsonw_bool(&w, 1); bf_jsonw_key(&w, "allow_guest"); bf_jsonw_bool(&w, 1); }
    bf_jsonw_key(&w, "content"); bf_jsonw_str(&w, p->text);
    bf_jsonw_key(&w, "edited_by"); bf_jsonw_str(&w, actor);
    bf_jsonw_key(&w, "revision_note"); bf_jsonw_str(&w, *p->note ? p->note : creating ? "First version" : "Revised");
    bf_jsonw_obj_end(&w);
    if (!w.oom && w.buf) out = strdup(w.buf);
    bf_jsonw_free(&w); return out;
}
static int ch_commit(sqlite3 *db, const char *actor, const ChForm *p, char back[256]) {
    CjrCommunity c; char route[200], slug[64];
    if (!cjr_key(p->community) || cjr_community(db, p->community, &c) != 1 || !cjr_public(&c)) return 404;
    if (!gr_active(db, &c, actor)) return 403;
    if (!gr_words(p->title) || !gr_words(p->text)) return 400;
    if (!strcmp(p->op, "write")) {
        ch_slug(p->title, p->request, slug);
        snprintf(route, sizeof(route), "charters/%s/%s", c.key, slug);
        if (gr_row(db, "SELECT id FROM wiki_page WHERE namespace='root' AND route=?1 AND ?2=''", route, "", NULL, 0, NULL, 0, NULL, 0)) {
            snprintf(slug + (strlen(slug) > 50 ? 50 : strlen(slug)), 14, "-%.8s", p->request);
            snprintf(route, sizeof(route), "charters/%s/%s", c.key, slug);
        }
        char *json = ch_json(p, route, actor, 1);
        int ok = json && ph_cms("create", "wiki_page", NULL, json, "root", NULL, NULL, NULL, NULL);
        free(json);
        snprintf(back, 256, "/%s", route);
        return ok ? 303 : 503;
    }
    if (!strcmp(p->op, "revise")) {
        char community[97] = "";
        long long id = gr_row(db, "SELECT id,COALESCE(community,'') FROM wiki_page WHERE namespace='root' AND route=?1 AND ?2=''", p->target, "", community, sizeof(community), NULL, 0, NULL, 0);
        if (!id || strncmp(p->target, "charters/", 9)) return 404;
        if (strcmp(community, c.key)) return 403;
        if (!gr_words(p->note)) return 400;
        char *json = ch_json(p, NULL, actor, 0), idtext[24];
        snprintf(idtext, sizeof(idtext), "%lld", id);
        int ok = json && ph_cms("update", "wiki_page", idtext, json, "root", "community", c.key, NULL, NULL);
        free(json);
        snprintf(back, 256, "/%s#revisions", p->target);
        return ok ? 303 : 409;
    }
    return 400;
}
static int serve_charters(const BfRequest *r) {
    const char *method=r->method,*path=r->target,*query=r->query,*body=r->body,*raw=r->raw,*host=r->host,*bearer=r->bearer,*auth_db=r->auth_db,*accept=r->accept;
    (void)method;(void)query;(void)body;(void)raw;(void)host;(void)bearer;(void)auth_db;(void)accept;
    int post = !strcmp(method, "POST");
    if (!post && !wants_html(accept)) return 0;
    StudioViewer who = {0}; char actor[64] = "";
    if (session_identity(bearer, auth_db, &who) && who.id > 0) snprintf(actor, sizeof(actor), "user:%ld", who.id);
    sqlite3 *db = cms_open_ro();
    if (!db) { ph_error(503, "The charters are unavailable.\n"); return 1; }
    if (post) {
        ChForm p; memset(&p, 0, sizeof(p)); char back[256];
        PhField f[] = {{"op", p.op, sizeof(p.op), 0}, {"request", p.request, sizeof(p.request), 0}, {"community", p.community, sizeof(p.community), 0},
                       {"target", p.target, sizeof(p.target), 0}, {"title", p.title, sizeof(p.title), 0}, {"text", p.text, sizeof(p.text), 1},
                       {"note", p.note, sizeof(p.note), 0}};
        if (strcmp(path, "/charters/act")) { sqlite3_close(db); ph_error(405, "Use the charter form.\n"); return 1; }
        if (!*actor) { sqlite3_close(db); ph_error(401, "Sign in before writing a charter.\n"); return 1; }
        if (!ph_origin(raw, host) || !ph_fields(body, f, 7, 40000) || strlen(p.request) != 32 || strspn(p.request, "0123456789abcdef") != 32) {
            sqlite3_close(db); ph_error(403, "Submit one charter form from this site.\n"); return 1;
        }
        int code = ch_commit(db, actor, &p, back); sqlite3_close(db);
        switch (code) {
        case 303: page_redirect(back); break;
        case 403: ph_error(403, "Only this community's active members can write its charters.\n"); break;
        case 404: ph_error(404, "That community or charter is not here.\n"); break;
        case 409: ph_error(409, "The charter changed. Reopen it.\n"); break;
        case 503: ph_error(503, "The record owner did not accept the charter. Nothing was saved.\n"); break;
        default: ph_error(400, "Give the charter a title, text and a note of what changed.\n"); break;
        }
        return 1;
    }
    if (!strcmp(path, "/charters") || !strcmp(path, "/charters/")) {
        char community[97] = "", q[121] = "";
        PhField f[] = {{"community", community, sizeof(community), 0}, {"q", q, sizeof(q), 0}};
        if (!ph_fields(query, f, 2, 400) || (*community && !cjr_key(community)) || strchr(q, '%') || strchr(q, '_')) { sqlite3_close(db); ph_error(400, "Search the charters by words or community.\n"); return 1; }
        ch_index(db, actor, community, q);
    } else {
        char route[200], community[97] = "", slug[64] = "";
        const char *rest = path + 10, *slash = strchr(rest, '/');
        if (!slash || (size_t)(slash - rest) >= sizeof(community)) { sqlite3_close(db); return 0; }
        memcpy(community, rest, (size_t)(slash - rest)); community[slash - rest] = 0;
        snprintf(slug, sizeof(slug), "%s", slash + 1);
        size_t n = strlen(slug); if (n && slug[n - 1] == '/') slug[n - 1] = 0;
        if (!cjr_key(community) || !ch_slug_ok(slug)) { sqlite3_close(db); return 0; }
        snprintf(route, sizeof(route), "charters/%s/%s", community, slug);
        ch_page(db, actor, route);
    }
    sqlite3_close(db);
    return 1;
}
#endif
