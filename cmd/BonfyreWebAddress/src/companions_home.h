#ifndef BONFYRE_COMPANIONS_HOME_H
#define BONFYRE_COMPANIONS_HOME_H
#include "affinity_projection.h"
#include "club_projection.h"
#include "dateplan_projection.h"
#include "route_presentation.h"
#include "form_tree.h"
/* Companion discovery: the signed-in member directory with affinity.
 * GET /companions lists published member people, minus the viewer and
 * the viewer's active passes, annotated with affinity status and
 * mutuals. POST /companions/affinity records a like, pass or superlike
 * as a person_affinity edge through the CMS owner. A like is private
 * until mutual; a pass only excludes discovery; a duplicate active edge
 * is refused rather than duplicated. Cannot infer: a listed companion
 * -> availability, interest, or permission to contact. */
static void cp_head(const char *title) {
    fputs("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: private, no-store\r\n"
          "Vary: Host, Cookie, Authorization\r\nReferrer-Policy: same-origin\r\nX-Content-Type-Options: nosniff\r\n"
          "Connection: close\r\n\r\n<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
          "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>", stdout);
    html(title);
    fputs(" · Aurekai</title>"
          "<link rel=\"stylesheet\" href=\"https://fonts.googleapis.com/css2?family=Literata:ital,opsz,wght@0,7..72,400;1,7..72,400&amp;display=swap\">"
          "<link rel=\"stylesheet\" href=\"/assets/aurekai-native.css?v=4\">"
          "<link rel=\"stylesheet\" href=\"/assets/authoring-reader.css\">", stdout);
}
/* Every companion page closes the shared way: the one footer, the one player. */
static void cp_end(void) { fputs("</main>" BF_ESTATE_FOOT "</body></html>", stdout); }
static void cp_theme_edit(sqlite3 *db, const char *actor) {
    BfAuthoringTree *tree = calloc(1, sizeof(*tree));
    sqlite3_stmt *st = NULL, *cur = NULL;
    char current[65] = "", family[65] = "", gid[32] = "";
    char oval[200][65], olabel[200][128];
    FtOption opts[200];
    unsigned nopts = 0, fam = 0;
    int ok = 0;
    if (!db || !actor || !*actor || !tree) goto done;
    if (sqlite3_prepare_v2(db, "SELECT template FROM scope_presentation WHERE namespace='root' AND scope='account' AND ref=?1 AND status='published' ORDER BY id DESC LIMIT 1",
                           -1, &cur, NULL) == SQLITE_OK) {
        sqlite3_bind_text(cur, 1, actor, -1, SQLITE_STATIC);
        if (sqlite3_step(cur) == SQLITE_ROW) {
            const char *t = (const char *)sqlite3_column_text(cur, 0);
            if (t) snprintf(current, sizeof(current), "%s", t);
        }
    }
    sqlite3_finalize(cur);
    cur = NULL;
    ok = ft_card(tree, "theme", NULL, 0, "Your design",
                 "Surfaces whose owners name nothing wear your account default.", NULL, 0);
    if (!ok) goto done;
    if (!ft_action(tree, "theme-save", "theme", 0, "Save design", "post", "/companions/theme", NULL, 0)) goto done;
    ok = 0;
    if (sqlite3_prepare_v2(db, "SELECT template,COALESCE(family,''),COALESCE(reference_name,'') FROM presentation_template WHERE namespace='root' AND status='published' ORDER BY family,template LIMIT 200",
                           -1, &st, NULL) != SQLITE_OK) goto done;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *t = (const char *)sqlite3_column_text(st, 0);
        const char *f = (const char *)sqlite3_column_text(st, 1);
        const char *n = (const char *)sqlite3_column_text(st, 2);
        if (!t || !*t) continue;
        if (strcmp(f ? f : "", family)) {
            if (nopts) {
                snprintf(gid, sizeof(gid), "theme-f%u", fam++);
                if (!ft_choice(tree, gid, "theme-save", (int)fam - 1, family[0] ? family : "Design",
                               "template", opts, nopts)) goto done;
                nopts = 0;
            }
            snprintf(family, sizeof(family), "%s", f ? f : "");
        }
        if (nopts >= 200) continue;
        snprintf(oval[nopts], sizeof(oval[nopts]), "%s", t);
        snprintf(olabel[nopts], sizeof(olabel[nopts]), "%s", (n && *n) ? n : t);
        opts[nopts].value = oval[nopts];
        opts[nopts].label = olabel[nopts];
        opts[nopts].checked = !strcmp(current, t);
        nopts++;
    }
    sqlite3_finalize(st);
    st = NULL;
    if (nopts) {
        snprintf(gid, sizeof(gid), "theme-f%u", fam++);
        if (!ft_choice(tree, gid, "theme-save", (int)fam - 1, family[0] ? family : "Design",
                       "template", opts, nopts)) goto done;
    }
    ok = 1;
done:
    if (st) sqlite3_finalize(st);
    if (tree) {
        if (ok) ft_render(tree, stdout);
        else {
            bf_authoring_tree_clear(tree);
            free(tree);
        }
    }
}
/* The companions masthead, from the Stitch "degrees of proximity" screen: a
 * hero with the real count of people who keep an address here, then the
 * switchboard of every companion view. Layout lives in aurekai-native.css
 * under [data-companions]. */
static void cp_masthead(sqlite3 *db, const char *actor, const char *kicker, const char *title, const char *lede) {
    static const char *const hrefs[] = {"/companions", "/companions?view=matches", "/companions?view=attention",
        "/companions/notices", "/companions/clubs", "/companions/events", "/companions/plans"};
    static const char *const names[] = {"People", "Matches", "Attention", "Notices", "Clubs", "Events", "Plans"};
    static const char *const notes[] = {"Everyone with an address here", "Likes returned both ways", "Who noticed you",
        "Matches, superlikes and RSVPs", "Interest groups, open doors", "Gatherings you can join", "Outings drafted for two"};
    char pattern[65] = {0}, tmpl[65] = {0};
    int people = 0;
    sqlite3_stmt *st = NULL;
    bf_presentation_resolve(NULL, NULL, NULL, NULL, NULL, actor && *actor ? actor : NULL, pattern, tmpl);
    if (tmpl[0] && db) bf_site_presentation_emit_scoped(stdout, db, tmpl, "body[data-bf-template]");
    if (db && sqlite3_prepare_v2(db, "SELECT count(DISTINCT subject_address) FROM site_presentation WHERE namespace='root' "
            "AND status='published' AND profile='member-profile-v1' AND subject_address LIKE 'person:%'", -1, &st, NULL) == SQLITE_OK &&
        sqlite3_step(st) == SQLITE_ROW) people = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    fputs("</head><body class=\"ak-app cp-app\"", stdout);
    if (tmpl[0]) {
        fputs(" data-bf-template=\"", stdout);
        html(tmpl);
        fputc('"', stdout);
    }
    fputs(">" BF_ESTATE_BAR "<main class=\"cp-shell\" data-companions><section class=\"cp-hero\"><div><p class=\"cp-kicker\">", stdout);
    html(kicker);
    fputs(" · Degrees of proximity</p><h1>Never walk or sit alone. <em>", stdout);
    html(title);
    fputs("</em></h1><p class=\"cp-lede\">", stdout);
    html(lede);
    fputs("</p></div><aside class=\"cp-count\"><p>People here</p><strong>", stdout);
    printf("%d", people);
    fputs("</strong><span>", stdout);
    fputs(people == 1 ? "person keeps" : "people keep", stdout);
    fputs(" a public address on Aurekai</span></aside></section>"
          "<section class=\"cp-board\" aria-labelledby=\"cp-board-h\"><header><h2 id=\"cp-board-h\">Ways to find each other</h2>"
          "<p>Each view widens or narrows who you see. Nothing here contacts anyone for you.</p></header>"
          "<nav class=\"cp-switch\" aria-label=\"Companions\">", stdout);
    for (int i = 0; i < 7; i++) {
        fputs("<a href=\"", stdout);
        fputs(hrefs[i], stdout);
        fputc('"', stdout);
        if (!strcmp(title, names[i])) fputs(" aria-current=\"page\"", stdout);
        printf("><small>0%d</small><b>", i + 1);
        fputs(names[i], stdout);
        fputs("</b><span>", stdout);
        fputs(notes[i], stdout);
        fputs("</span></a>", stdout);
    }
    fputs("</nav></section>", stdout);
}
static int cp_member_subject(sqlite3 *db, const char *subject) {
    sqlite3_stmt *st = NULL;
    int ok = 0;
    if (!db || !subject || strncmp(subject, "person:", 7) || strlen(subject) >= 192) return 0;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM site_presentation WHERE namespace='root'"
                           " AND status='published' AND profile='member-profile-v1'"
                           " AND subject_address=?1 LIMIT 1", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, subject, -1, SQLITE_STATIC);
    ok = sqlite3_step(st) == SQLITE_ROW;
    sqlite3_finalize(st);
    return ok;
}
static int cp_budget(const char *s, long *out) {
    char *end = NULL;
    long v;
    if (!s || !*s || strlen(s) > 6 || strspn(s, "0123456789") != strlen(s)) return 0;
    errno = 0;
    v = strtol(s, &end, 10);
    if (errno || !end || *end || v < 0 || v > 100000) return 0;
    *out = v;
    return 1;
}
static void cp_meter(const char *actor, const char *model, long bytes, long ms,
                     const char *receipt, const char *attempt) {
    const char *bin, *db;
    char msbuf[24], bytesbuf[24], date[40];
    char *argv[24];
    int n = 0;
    char out[512];
    if (!actor || !model || !receipt || !attempt) return;
    bin = getenv("BONFYRE_METER_BINARY");
    if (!bin || !*bin) bin = "bonfyre-meter";
    db = getenv("BONFYRE_METER_DB");
    if (!db || !*db) return;
    snprintf(msbuf, sizeof(msbuf), "%ld", ms);
    snprintf(bytesbuf, sizeof(bytesbuf), "%ld", bytes);
    iso_utc_now(date, sizeof(date));
    argv[n++] = (char *)bin;
    argv[n++] = "record-packet";
    argv[n++] = "--db";
    argv[n++] = (char *)db;
    argv[n++] = "--key";
    argv[n++] = (char *)actor;
    argv[n++] = "--op";
    argv[n++] = "dateplan.generate";
    argv[n++] = "--provider";
    argv[n++] = (char *)model;
    argv[n++] = "--bytes";
    argv[n++] = bytesbuf;
    argv[n++] = "--duration";
    argv[n++] = msbuf;
    argv[n++] = "--receipt";
    argv[n++] = (char *)receipt;
    argv[n++] = "--attempt-ref";
    argv[n++] = (char *)attempt;
    argv[n++] = "--realization-ref";
    argv[n++] = (char *)attempt;
    argv[n++] = "--at";
    argv[n++] = date;
    argv[n] = NULL;
    if (auth_call(argv, NULL, out, sizeof(out)) != 0)
        fprintf(stderr, "companions: meter record failed for %s\n", attempt);
}
static int serve_companions(const char *method, const char *target, const char *query,
                            const char *body, const char *raw, const char *host,
                            const char *bearer, const char *auth_db, const char *accept) {
    int listing = !strcmp(target, "/companions") || !strcmp(target, "/companions/");
    int mutate = !strcmp(target, "/companions/affinity");
    int plans = !strcmp(target, "/companions/plans") || !strcmp(target, "/companions/plans/");
    int accept_plan = !strcmp(target, "/companions/plans/accept");
    int report = !strcmp(target, "/companions/report");
    int clubs = !strcmp(target, "/companions/clubs") || !strcmp(target, "/companions/clubs/");
    int join = !strcmp(target, "/companions/clubs/join");
    int events = !strcmp(target, "/companions/events") || !strcmp(target, "/companions/events/");
    int rsvp = !strcmp(target, "/companions/events/rsvp");
    int notices = !strcmp(target, "/companions/notices") || !strcmp(target, "/companions/notices/");
    int theme = !strcmp(target, "/companions/theme");
    if (!listing && !mutate && !plans && !accept_plan && !report && !clubs && !join &&
        !events && !rsvp && !notices && !theme) return 0;
    char normalized[256];
    if (!lower_host(host, normalized)) return 0;
    sqlite3 *db = cms_open_ro();
    StudioViewer who = {0};
    char actor[192] = "";
    int signed_in = session_identity(bearer, auth_db, &who) && who.id > 0;
    if (signed_in && !ph_actor(who.id, auth_db, actor)) signed_in = 0;
    if (mutate) {
        int wrote = 0;
        char target_param[192] = "", kind[16] = "";
        PhField fields[] = {{"target", target_param, sizeof(target_param), 0},
                            {"kind", kind, sizeof(kind), 0}};
        if (strcmp(method, "POST")) {
            ph_error(405, "Record affinity with POST.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (!signed_in) {
            ph_error(401, "Sign in before recording affinity.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (!ph_origin(raw, host) || !ph_fields(body, fields, 2, 2048) ||
            (strcmp(kind, "like") && strcmp(kind, "pass") && strcmp(kind, "superlike") &&
             strcmp(kind, "block")) ||
            !cp_member_subject(db, target_param) || !strcmp(actor, target_param)) {
            ph_error(400, "Choose a listed companion and like, pass, superlike or block.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        {
            char latest[16] = "";
            int status = bf_affinity_status(db, actor, target_param, latest, sizeof(latest));
            if (status < 0) {
                ph_error(503, "The affinity owner is unavailable.\n");
                if (db) sqlite3_close(db);
                return 1;
            }
            if (status && !strcmp(latest, kind)) {
                ph_error(409, "This affinity is already recorded.\n");
                if (db) sqlite3_close(db);
                return 1;
            }
        }
        if (!strcmp(kind, "superlike")) {
            int left = 0;
            if (bf_quota_superlike(db, actor, &left) != 0 || left == 0) {
                if (db) sqlite3_close(db);
                ph_error(429, "Today's superlike is spent; Gold removes the limit.\n");
                return 1;
            }
        }
        {
            char payload[4096], nonce[33], affinity[40], date[40];
            size_t used = 1;
            ph_nonce(nonce);
            snprintf(affinity, sizeof(affinity), "aff-%s", nonce);
            iso_utc_now(date, sizeof(date));
            payload[0] = '{';
            page_json_field(payload, sizeof(payload), &used, "affinity", affinity, 0);
            page_json_field(payload, sizeof(payload), &used, "actor_subject", actor, 0);
            page_json_field(payload, sizeof(payload), &used, "target_subject", target_param, 0);
            page_json_field(payload, sizeof(payload), &used, "kind", kind, 0);
            page_json_field(payload, sizeof(payload), &used, "state", "active", 0);
            page_json_field(payload, sizeof(payload), &used, "created_at", date, 0);
            page_json_field(payload, sizeof(payload), &used, "cannot_infer",
                            "A directed affinity edge. Private until mutual; a pass only excludes discovery.", 0);
            page_json_field(payload, sizeof(payload), &used, "status", "published", 1);
            payload[used++] = '}';
            payload[used] = 0;
            wrote = ph_cms("create", "person_affinity", NULL, payload, "root", NULL, NULL, NULL, NULL);
        }
        if (db) sqlite3_close(db);
        if (!wrote) {
            ph_error(503, "The affinity was not recorded. Reload companions before retrying.\n");
            return 1;
        }
        if (wants_json(accept) && !wants_html(accept)) {
            protocol_reply(201, "application/json; charset=utf-8", "{\"recorded\":true}\n");
            return 1;
        }
        page_redirect("/companions");
        return 1;
    }
    if (theme) {
        /* Account design: the viewing account's own default for surfaces
         * whose owners name nothing. Latest published row wins. */
        char template[65] = "";
        PhField fields[] = {{"template", template, sizeof(template), 0}};
        if (strcmp(method, "POST")) {
            ph_error(405, "Choose a design with POST.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (!signed_in) {
            ph_error(401, "Sign in before choosing a design.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (!ph_origin(raw, host) || !ph_fields(body, fields, 1, 2048) ||
            !bf_route_presentation_safe(template)) {
            ph_error(400, "Choose one of the listed designs.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        {
            sqlite3_stmt *st = NULL;
            int known = 0, wrote = 0;
            char hex[33], spid[24], date[40], payload[4096];
            size_t used = 1;
            if (sqlite3_prepare_v2(db, "SELECT 1 FROM presentation_template WHERE template=?1 AND status='published' LIMIT 1",
                                   -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, template, -1, SQLITE_STATIC);
                known = sqlite3_step(st) == SQLITE_ROW;
            }
            sqlite3_finalize(st);
            if (!known) {
                ph_error(400, "Choose one of the listed designs.\n");
                if (db) sqlite3_close(db);
                return 1;
            }
            ph_nonce(hex);
            snprintf(spid, sizeof(spid), "sp-%.16s", hex);
            iso_utc_now(date, sizeof(date));
            payload[0] = '{';
            page_json_field(payload, sizeof(payload), &used, "scope_presentation", spid, 0);
            page_json_field(payload, sizeof(payload), &used, "scope", "account", 0);
            page_json_field(payload, sizeof(payload), &used, "ref", actor, 0);
            page_json_field(payload, sizeof(payload), &used, "template", template, 0);
            page_json_field(payload, sizeof(payload), &used, "created_at", date, 0);
            page_json_field(payload, sizeof(payload), &used, "cannot_infer", "A chosen account design. It changes presentation only.", 0);
            page_json_field(payload, sizeof(payload), &used, "status", "published", 1);
            payload[used++] = '}';
            payload[used] = 0;
            wrote = ph_cms("create", "scope_presentation", NULL, payload, "root", NULL, NULL, NULL, NULL);
            if (db) sqlite3_close(db);
            if (!wrote) {
                ph_error(503, "The design was not recorded. Reload companions before retrying.\n");
                return 1;
            }
        }
        page_redirect("/companions");
        return 1;
    }
    if (report && !strcmp(method, "POST")) {
        char target_param[192] = "", reason[24] = "", details[501] = "";
        PhField rfields[] = {{"target", target_param, sizeof(target_param), 0},
                             {"reason", reason, sizeof(reason), 0},
                             {"details", details, sizeof(details), 1}};
        if (!signed_in) {
            ph_error(401, "Sign in before reporting.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (!ph_origin(raw, host) || !ph_fields(body, rfields, 3, 4096) ||
            (strcmp(reason, "spam") && strcmp(reason, "harassment") && strcmp(reason, "fake") &&
             strcmp(reason, "minor-safety") && strcmp(reason, "other")) ||
            !cp_member_subject(db, target_param) || !strcmp(actor, target_param)) {
            ph_error(400, "Name a member and a reason: spam, harassment, fake, minor-safety or other.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (bf_report_quota_ok(db, actor) != 1) {
            if (db) sqlite3_close(db);
            ph_error(429, "Report limit reached for today; try again tomorrow.\n");
            return 1;
        }
        {
            char payload[4096], nonce[33], rid[40], date[40];
            size_t used = 1;
            ph_nonce(nonce);
            snprintf(rid, sizeof(rid), "rep-%.16s", nonce);
            iso_utc_now(date, sizeof(date));
            payload[0] = '{';
            page_json_field(payload, sizeof(payload), &used, "report", rid, 0);
            page_json_field(payload, sizeof(payload), &used, "actor_subject", actor, 0);
            page_json_field(payload, sizeof(payload), &used, "target_subject", target_param, 0);
            page_json_field(payload, sizeof(payload), &used, "reason", reason, 0);
            page_json_field(payload, sizeof(payload), &used, "details", details, 0);
            page_json_field(payload, sizeof(payload), &used, "created_at", date, 0);
            page_json_field(payload, sizeof(payload), &used, "cannot_infer",
                            "An observation held for human review, not a verdict and not proof.", 0);
            page_json_field(payload, sizeof(payload), &used, "status", "published", 1);
            payload[used++] = '}';
            payload[used] = 0;
            if (!ph_cms("create", "person_report", NULL, payload, "root", NULL, NULL, NULL, NULL)) {
                if (db) sqlite3_close(db);
                ph_error(503, "The report was not kept. Reload before retrying.\n");
                return 1;
            }
        }
        if (db) sqlite3_close(db);
        page_redirect("/companions");
        return 1;
    }
    if (join) {
        char club[65] = "";
        PhField jfields[] = {{"club", club, sizeof(club), 0}};
        sqlite3_stmt *st = NULL;
        int exists = 0, member = 0;
        if (strcmp(method, "POST")) {
            ph_error(405, "Join clubs with POST.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (!signed_in) {
            ph_error(401, "Sign in before joining.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (!ph_origin(raw, host) || !ph_fields(body, jfields, 1, 2048) || !*club ||
            strlen(club) > 64 ||
            strspn(club, "abcdefghijklmnopqrstuvwxyz0123456789-") != strlen(club)) {
            ph_error(400, "Choose a listed club.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (db && sqlite3_prepare_v2(db, "SELECT 1 FROM community_body WHERE namespace='root'"
                                     " AND community=?1 AND status='published' LIMIT 1",
                                     -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, club, -1, SQLITE_STATIC);
            exists = sqlite3_step(st) == SQLITE_ROW;
        }
        sqlite3_finalize(st);
        st = NULL;
        if (!exists) {
            if (db) sqlite3_close(db);
            ph_error(404, "That club is not open.\n");
            return 1;
        }
        if (db && sqlite3_prepare_v2(db, "SELECT 1 FROM community_membership WHERE namespace='root'"
                                     " AND community=?1 AND member_subject=?2 AND state='active'"
                                     " AND status='published' LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, club, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, actor, -1, SQLITE_STATIC);
            member = sqlite3_step(st) == SQLITE_ROW;
        }
        sqlite3_finalize(st);
        if (!member) {
            char payload[2048], nonce[33], mid[40], date[40];
            size_t used = 1;
            ph_nonce(nonce);
            snprintf(mid, sizeof(mid), "mem-%.16s", nonce);
            iso_utc_now(date, sizeof(date));
            payload[0] = '{';
            page_json_field(payload, sizeof(payload), &used, "membership", mid, 0);
            page_json_field(payload, sizeof(payload), &used, "community", club, 0);
            page_json_field(payload, sizeof(payload), &used, "member_subject", actor, 0);
            page_json_field(payload, sizeof(payload), &used, "role", "member", 0);
            page_json_field(payload, sizeof(payload), &used, "state", "active", 0);
            page_json_field(payload, sizeof(payload), &used, "joined_at", date, 0);
            page_json_field(payload, sizeof(payload), &used, "cannot_infer",
                            "Open club membership. Joining never implies attendance, availability or consent.", 0);
            page_json_field(payload, sizeof(payload), &used, "status", "published", 1);
            payload[used++] = '}';
            payload[used] = 0;
            if (!ph_cms("create", "community_membership", NULL, payload, "root",
                        NULL, NULL, NULL, NULL)) {
                if (db) sqlite3_close(db);
                ph_error(503, "The join was not kept. Reload before retrying.\n");
                return 1;
            }
        }
        if (db) sqlite3_close(db);
        page_redirect("/companions/clubs");
        return 1;
    }
    if (rsvp) {
        char post[128] = "";
        PhField vfields[] = {{"post", post, sizeof(post), 0}};
        sqlite3_stmt *st = NULL;
        long long rowid = 0;
        int active = 0;
        if (strcmp(method, "POST")) {
            ph_error(405, "RSVP with POST.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (!signed_in) {
            ph_error(401, "Sign in before RSVPing.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (!ph_origin(raw, host) || !ph_fields(body, vfields, 1, 2048) || !*post ||
            strlen(post) > 120) {
            ph_error(400, "Choose a listed event.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        {
            int upcoming = 0;
            if (db && sqlite3_prepare_v2(db, "SELECT 1 FROM community_post WHERE post=?1"
                                         " AND status='published' AND visibility='public'"
                                         " AND moderation_state='accepted' AND event_at IS NOT NULL"
                                         " AND datetime(event_at)>=datetime('now') LIMIT 1",
                                         -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, post, -1, SQLITE_STATIC);
                upcoming = sqlite3_step(st) == SQLITE_ROW;
            }
            sqlite3_finalize(st);
            st = NULL;
            if (!upcoming) {
                ph_error(404, "That event is over or unavailable.\n");
                if (db) sqlite3_close(db);
                return 1;
            }
        }
        if (db && sqlite3_prepare_v2(db, "SELECT id, state FROM community_reaction"
                                     " WHERE post=?1 AND actor_subject=?2 AND kind='rsvp'"
                                     " AND status='published' ORDER BY id DESC LIMIT 1",
                                     -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, post, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, actor, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                const char *state = (const char *)sqlite3_column_text(st, 1);
                rowid = sqlite3_column_int64(st, 0);
                active = state && !strcmp(state, "active");
            }
        }
        sqlite3_finalize(st);
        if (rowid && active) {
            char record[32];
            snprintf(record, sizeof(record), "%lld", rowid);
            ph_cms("update", "community_reaction", record, "{\"state\":\"withdrawn\"}", "root",
                   "state", "active", NULL, NULL);
            if (db) sqlite3_close(db);
            page_redirect("/companions/events");
            return 1;
        }
        if (rowid) {
            char record[32];
            snprintf(record, sizeof(record), "%lld", rowid);
            ph_cms("update", "community_reaction", record, "{\"state\":\"active\"}", "root",
                   NULL, NULL, NULL, NULL);
            if (db) sqlite3_close(db);
            page_redirect("/companions/events");
            return 1;
        }
        {
            char payload[2048], nonce[33], rid[40], date[40];
            size_t used = 1;
            ph_nonce(nonce);
            snprintf(rid, sizeof(rid), "rsvp-%.16s", nonce);
            iso_utc_now(date, sizeof(date));
            payload[0] = '{';
            page_json_field(payload, sizeof(payload), &used, "reaction", rid, 0);
            page_json_field(payload, sizeof(payload), &used, "post", post, 0);
            page_json_field(payload, sizeof(payload), &used, "actor_subject", actor, 0);
            page_json_field(payload, sizeof(payload), &used, "kind", "rsvp", 0);
            page_json_field(payload, sizeof(payload), &used, "state", "active", 0);
            page_json_field(payload, sizeof(payload), &used, "created_at", date, 0);
            page_json_field(payload, sizeof(payload), &used, "cannot_infer",
                            "Stated attendance intent, not proof of presence.", 0);
            page_json_field(payload, sizeof(payload), &used, "status", "published", 1);
            payload[used++] = '}';
            payload[used] = 0;
            if (!ph_cms("create", "community_reaction", NULL, payload, "root",
                        NULL, NULL, NULL, NULL)) {
                if (db) sqlite3_close(db);
                ph_error(503, "The RSVP was not kept. Reload before retrying.\n");
                return 1;
            }
        }
        if (db) sqlite3_close(db);
        page_redirect("/companions/events");
        return 1;
    }
    if (report) {
        char target_param[192] = "";
        PhField rfields[] = {{"target", target_param, sizeof(target_param), 0}};
        if (strcmp(method, "GET")) {
            ph_error(405, "Open the report form with GET.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (!signed_in) {
            ph_error(401, "Sign in before reporting.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (!ph_fields(query, rfields, 1, 2048) || !cp_member_subject(db, target_param) ||
            !strcmp(actor, target_param)) {
            ph_error(400, "Report from a listed companion.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        cp_head("Report");
        cp_masthead(db, actor, "Companions", "Report", "Reporting hides them from your discovery at once.");
        {
            static const char *const values[] = {"spam", "harassment", "fake", "minor-safety", "other"};
            static const char *const labels[] = {"Spam", "Harassment", "Fake profile", "Minor safety", "Other"};
            BfAuthoringTree *tree = calloc(1, sizeof(*tree));
            FtField fields[1];
            FtOption options[5];
            int ok = 0;
            size_t i;
            if (!tree) {
                if (db) sqlite3_close(db);
                return 1;
            }
            for (i = 0; i < 5; i++) {
                options[i].value = values[i];
                options[i].label = labels[i];
                options[i].checked = i == 0;
            }
            fields[0].name = "target";
            fields[0].value = target_param;
            ok = ft_badge(tree, "rp-note", NULL, 0, "A report is held for human review; it decides nothing by itself.") &&
                 ft_action(tree, "rp-form", NULL, 1, "Send report", "post", "/companions/report", fields, 1) &&
                 ft_choice(tree, "rp-reason", "rp-form", 0, "Reason", "reason", options, 5) &&
                 ft_field(tree, "rp-details", "rp-form", 1, "Details (optional)", "text", "details", "", "", 1, "3", "500", 0, "");
            if (ok) ft_render(tree, stdout);
            else {
                bf_authoring_tree_clear(tree);
                free(tree);
            }
        }
        cp_end();
        if (db) sqlite3_close(db);
        return 1;
    }
    if (notices) {
        if (strcmp(method, "GET")) {
            ph_error(405, "Open notices with GET.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (!signed_in) {
            ph_error(401, "Sign in before reading notices.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (wants_json(accept) && !wants_html(accept)) {
            char *doc = NULL;
            size_t len = 0;
            FILE *mem = open_memstream(&doc, &len);
            int n = mem ? bf_notices_json(mem, db, actor) : -1;
            if (mem) fclose(mem);
            if (n < 0 || !doc) {
                free(doc);
                if (db) sqlite3_close(db);
                ph_error(503, "Notices are unavailable.\n");
                return 1;
            }
            printf("HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\n"
                   "Cache-Control: private, no-store\r\nConnection: close\r\n\r\n%s", doc);
            free(doc);
            if (db) sqlite3_close(db);
            return 1;
        }
        if (!wants_html(accept) && accept && *accept && !strstr(accept, "*/*")) {
            if (db) sqlite3_close(db);
            return 0;
        }
        cp_head("Notices");
        cp_masthead(db, actor, "Companions", "Notices", "Matches, superlikes and RSVPs to your events. Nothing here sends anything anywhere.");
        if (db && bf_notices_page(stdout, db, actor) < 0) {
            sqlite3_close(db);
            return 1;
        }
        cp_end();
        if (db) sqlite3_close(db);
        return 1;
    }
    if (clubs && !strcmp(method, "GET")) {
        if (!wants_html(accept) && accept && *accept && !strstr(accept, "*/*")) {
            if (db) sqlite3_close(db);
            return 0;
        }
        cp_head("Clubs");
        cp_masthead(db, actor, "Companions", "Clubs", "Interest groups with open doors. Joining never implies anything beyond membership.");
        if (db) bf_clubs_page(stdout, db, signed_in ? actor : NULL);
        cp_end();
        if (db) sqlite3_close(db);
        return 1;
    }
    if (events && !strcmp(method, "GET")) {
        char club[65] = "", erepr[16] = "";
        if (!wants_html(accept) && accept && *accept && !strstr(accept, "*/*")) {
            if (db) sqlite3_close(db);
            return 0;
        }
        PhField efields[] = {{"club", club, sizeof(club), 0}, {"repr", erepr, sizeof(erepr), 0}};
        (void)erepr;
        if (!ph_fields(query, efields, 2, 2048) ||
            (*club && (strlen(club) > 64 ||
                       strspn(club, "abcdefghijklmnopqrstuvwxyz0123456789-") != strlen(club)))) {
            ph_error(400, "Choose a listed club or none.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        cp_head("Events");
        cp_masthead(db, actor, "Companions", "Events", "Upcoming gatherings. An RSVP states intent, never proof.");
        if (db) bf_events_page(stdout, db, signed_in ? actor : NULL, club);
        cp_end();
        if (db) sqlite3_close(db);
        return 1;
    }
    if (accept_plan) {
        char id[64] = "";
        PhField afields[] = {{"id", id, sizeof(id), 0}};
        sqlite3_stmt *st = NULL;
        long long rowid = 0;
        if (strcmp(method, "POST")) {
            ph_error(405, "Accept plans with POST.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (!signed_in) {
            ph_error(401, "Sign in before accepting plans.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (!ph_origin(raw, host) || !ph_fields(body, afields, 1, 2048) || !*id) {
            ph_error(400, "Choose one of your plans.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (db && sqlite3_prepare_v2(db, "SELECT id FROM date_plan WHERE namespace='root'"
                                     " AND plan=?1 AND actor_subject=?2 AND status='published'"
                                     " LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, id, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, actor, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) rowid = sqlite3_column_int64(st, 0);
        }
        sqlite3_finalize(st);
        if (!rowid) {
            if (db) sqlite3_close(db);
            ph_error(404, "That plan is not yours.\n");
            return 1;
        }
        {
            char record[32];
            snprintf(record, sizeof(record), "%lld", rowid);
            ph_cms("update", "date_plan", record, "{\"accepted\":\"true\"}", "root",
                   "accepted", "false", NULL, NULL);
        }
        if (db) sqlite3_close(db);
        page_redirect("/companions/plans");
        return 1;
    }
    if (plans && !strcmp(method, "POST")) {
        char location[121] = "", intent[121] = "", interests[501] = "";
        char bmin[8] = "", bmax[8] = "", companion[192] = "";
        PhField pfields[] = {{"location", location, sizeof(location), 0},
                             {"intent", intent, sizeof(intent), 0},
                             {"budget_min", bmin, sizeof(bmin), 0},
                             {"budget_max", bmax, sizeof(bmax), 0},
                             {"interests", interests, sizeof(interests), 0},
                             {"companion", companion, sizeof(companion), 0}};
        long lo = 0, hi = 0;
        BfDatePlanResult plan;
        int rc;
        if (!signed_in) {
            ph_error(401, "Sign in before planning.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (!ph_origin(raw, host) || !ph_fields(body, pfields, 6, 4096) ||
            !cp_budget(bmin, &lo) || !cp_budget(bmax, &hi) || lo > hi) {
            ph_error(400, "Describe a location, intent and budget within bounds.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (*companion &&
            (strncmp(companion, "person:", 7) || !cp_member_subject(db, companion) ||
             bf_affinity_mutual(db, actor, companion) != 1)) {
            ph_error(403, "Plans name a mutual companion or no one.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        {
            int left = 0;
            if (bf_quota_plans(db, actor, &left) != 0 || left == 0) {
                if (db) sqlite3_close(db);
                ph_error(429, "This week's plans are spent; Gold removes the limit.\n");
                return 1;
            }
        }
        memset(&plan, 0, sizeof(plan));
        rc = bf_dateplan_generate(db, location, intent, lo, hi, interests, &plan);
        if (rc == -2) {
            if (db) sqlite3_close(db);
            ph_error(503, plan.unavail_reason[0] ? plan.unavail_reason
                                                 : "No generation model is available.\n");
            return 1;
        }
        if (rc != 1) {
            if (db) sqlite3_close(db);
            ph_error(502, "No plan survived verification; narrow the brief and retry.\n");
            return 1;
        }
        {
            char payload[28000], nonce[33], planid[40], xid[40], costs[48], date[40];
            size_t used = 1;
            ph_nonce(nonce);
            snprintf(planid, sizeof(planid), "plan-%.16s", nonce);
            snprintf(xid, sizeof(xid), "dp-%.16s", nonce);
            snprintf(costs, sizeof(costs), "%ld-%ld", plan.admitted_min, plan.admitted_max);
            iso_utc_now(date, sizeof(date));
            payload[0] = '{';
            page_json_field(payload, sizeof(payload), &used, "plan", planid, 0);
            page_json_field(payload, sizeof(payload), &used, "actor_subject", actor, 0);
            if (*companion) page_json_field(payload, sizeof(payload), &used, "companion_subject", companion, 0);
            page_json_field(payload, sizeof(payload), &used, "location", location, 0);
            page_json_field(payload, sizeof(payload), &used, "intent", intent, 0);
            {
                char lo_s[24], hi_s[24];
                snprintf(lo_s, sizeof(lo_s), "%ld", lo);
                snprintf(hi_s, sizeof(hi_s), "%ld", hi);
                page_json_field(payload, sizeof(payload), &used, "budget_min", lo_s, 0);
                page_json_field(payload, sizeof(payload), &used, "budget_max", hi_s, 0);
            }
            page_json_field(payload, sizeof(payload), &used, "interests", interests, 0);
            page_json_field(payload, sizeof(payload), &used, "plan_json", plan.admitted, 0);
            page_json_field(payload, sizeof(payload), &used, "costs_admitted", costs, 0);
            page_json_field(payload, sizeof(payload), &used, "accepted", "false", 0);
            page_json_field(payload, sizeof(payload), &used, "generation_model", plan.model, 0);
            page_json_field(payload, sizeof(payload), &used, "prompt_sha256", plan.prompt_sha256, 0);
            page_json_field(payload, sizeof(payload), &used, "output_sha256", plan.output_sha256, 0);
            page_json_field(payload, sizeof(payload), &used, "external_id", xid, 0);
            page_json_field(payload, sizeof(payload), &used, "created_at", date, 0);
            page_json_field(payload, sizeof(payload), &used, "cannot_infer",
                            "Admitted machine output. Costs are admitted numbers, not promises; a plan never books, pays or messages anyone.", 0);
            page_json_field(payload, sizeof(payload), &used, "status", "published", 1);
            payload[used++] = '}';
            payload[used] = 0;
            cp_meter(actor, plan.model, plan.output_bytes, plan.duration_ms,
                     plan.output_sha256, xid);
            if (!ph_cms("create", "date_plan", NULL, payload, "root", NULL, NULL, NULL, NULL)) {
                if (db) sqlite3_close(db);
                ph_error(503, "The plan was not kept. Generate once more before retrying.\n");
                return 1;
            }
        }
        if (db) sqlite3_close(db);
        page_redirect("/companions/plans");
        return 1;
    }
    if (plans) {
        double in_rate = 0, out_rate = 0;
        char observed[16] = "";
        int priced = 0;
        if (strcmp(method, "GET")) {
            ph_error(405, "Open plans with GET.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (!signed_in) {
            ph_error(401, "Sign in before planning.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        {
            const char *pin = getenv("BONFYRE_DATEPLAN_MODEL");
            if (pin && *pin)
                priced = bf_dateplan_price(pin, &in_rate, &out_rate, observed, sizeof(observed));
        }
        cp_head("Date plans");
        cp_masthead(db, actor, "Companions", "Plans", "Machine-drafted outings from a location, intent and budget. Every plan carries its safety note and admitted cost; nothing books or pays.");
        if (priced)
            printf("<div class=\"bf-native-slot\"><p class=\"cp-quota\">Priced model: $%.6f in / $%.6f out per million tokens, observed %s.</p></div>",
                   in_rate * 1e6, out_rate * 1e6, observed);
        else
            fputs("<div class=\"bf-native-slot\"><p class=\"cp-quota\">No admitted rate for the pinned model; spend is recorded by bytes and time.</p></div>", stdout);
        {
            BfAuthoringTree *tree = calloc(1, sizeof(*tree));
            int ok = 0;
            if (!tree) {
                if (db) sqlite3_close(db);
                return 1;
            }
            ok = ft_action(tree, "dp-form", NULL, 0, "Generate plans", "post", "/companions/plans", NULL, 0) &&
                 ft_field(tree, "dp-location", "dp-form", 0, "Location", "text", "location", "", "Dallas, TX", 0, "", "120", 1, "") &&
                 ft_field(tree, "dp-intent", "dp-form", 1, "Intent", "text", "intent", "", "first date", 0, "", "120", 1, "") &&
                 ft_field(tree, "dp-min", "dp-form", 2, "Budget min", "text", "budget_min", "", "0", 0, "", "", 1, "numeric") &&
                 ft_field(tree, "dp-max", "dp-form", 3, "Budget max", "text", "budget_max", "", "60", 0, "", "", 1, "numeric") &&
                 ft_field(tree, "dp-interests", "dp-form", 4, "Interests", "text", "interests", "", "hiking, coffee", 0, "", "500", 0, "") &&
                 ft_field(tree, "dp-companion", "dp-form", 5, "Companion (a mutual, optional)", "text", "companion", "", "person:", 0, "", "191", 0, "");
            if (ok) ft_render(tree, stdout);
            else {
                bf_authoring_tree_clear(tree);
                free(tree);
            }
        }
        if (db) bf_dateplan_history(stdout, db, actor, 24);
        cp_end();
        if (db) sqlite3_close(db);
        return 1;
    }
    if (strcmp(method, "GET")) {
        ph_error(405, "Open companions with GET.\n");
        if (db) sqlite3_close(db);
        return 1;
    }
    {
        char territory[8] = "", needle[65] = "", cursor[8] = "", repr[16] = "", sort[8] = "";
        char view[10] = "", club[65] = "";
        /* The wire layer appends repr=API to machine requests; accept and ignore it. */
        PhField params[] = {{"territory", territory, sizeof(territory), 0},
                            {"q", needle, sizeof(needle), 0},
                            {"cursor", cursor, sizeof(cursor), 0},
                            {"sort", sort, sizeof(sort), 0},
                            {"view", view, sizeof(view), 0},
                            {"club", club, sizeof(club), 0},
                            {"repr", repr, sizeof(repr), 0}};
        (void)repr;
        if (!ph_fields(query, params, 7, 2048) || (*cursor && !ph_digits(cursor)) ||
            (*sort && strcmp(sort, "match")) ||
            (*view && strcmp(view, "matches") && strcmp(view, "attention")) ||
            (*club && (strlen(club) > 64 ||
                       strspn(club, "abcdefghijklmnopqrstuvwxyz0123456789-") != strlen(club)))) {
            ph_error(400, "Choose a territory, search or page on this exact directory.\n");
            if (db) sqlite3_close(db);
            return 1;
        }
        if (!db) {
            ph_error(503, "The companion directory is unavailable.\n");
            return 1;
        }
        if (wants_json(accept) && !wants_html(accept)) {
            char *doc = NULL;
            size_t len = 0;
            FILE *mem = open_memstream(&doc, &len);
            int n = mem ? bf_companions_json(mem, db, signed_in ? actor : NULL,
                                             territory, needle,
                                             *cursor ? (unsigned)strtoul(cursor, NULL, 10) : 0, 24,
                                             sort, view, club) : -1;
            if (mem) fclose(mem);
            if (n < 0 || !doc) {
                free(doc);
                sqlite3_close(db);
                ph_error(400, "Choose a territory, search or page on this exact directory.\n");
                return 1;
            }
            printf("HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\n"
                   "Cache-Control: private, no-store\r\nConnection: close\r\n\r\n%s", doc);
            free(doc);
            sqlite3_close(db);
            return 1;
        }
        if (!wants_html(accept) && accept && *accept && !strstr(accept, "*/*")) {
            sqlite3_close(db);
            return 0;
        }
        cp_head(!strcmp(view, "matches") ? "Matches" : !strcmp(view, "attention") ? "Attention" : "Companions");
        cp_masthead(db, actor, "Companions",
                    !strcmp(view, "matches") ? "Matches" : !strcmp(view, "attention") ? "Attention" : "People",
                    "People with their own address here. Likes stay private until mutual.");
        if (signed_in) {
            int gold = bf_gold_active(db, actor);
            int sl = 0, pl = 0;
            if (gold > 0) {
                fputs("<div class=\"bf-native-slot\"><p class=\"cp-quota\" data-gold>Gold \u00B7 unlimited superlikes and plans.</p></div>", stdout);
            } else if (bf_quota_superlike(db, actor, &sl) == 0 &&
                       bf_quota_plans(db, actor, &pl) == 0) {
                printf("<div class=\"bf-native-slot\"><p class=\"cp-quota\" data-quota>Superlikes today: %d of 1 \u00B7 Plans this week: %d of 3 \u00B7 Gold removes the limits.</p></div>",
                       sl, pl);
            }
        }
        if (signed_in && !strcmp(view, "")) cp_theme_edit(db, actor);
        if (!*view && !*needle && !*territory && !*club && !*cursor) {
            fputs("<section class=\"cp-section cp-events\"><header><p class=\"cp-kicker\">Going somewhere alone</p><h2>Gatherings you can join</h2>"
                  "<p>Open gatherings from the clubs here. An RSVP states that you mean to come, nothing more.</p></header>", stdout);
            bf_events_page(stdout, db, signed_in ? actor : NULL, "");
            fputs("</section>", stdout);
        }
        fputs("<section class=\"cp-section cp-people\"><header><p class=\"cp-kicker\">Kinship and common values</p><h2>", stdout);
        fputs(!strcmp(view, "matches") ? "People who liked you back" : !strcmp(view, "attention") ? "People who noticed you" : "People with an address here", stdout);
        fputs("</h2><p>No endless swiping. Each person keeps their own page, and a like stays private until it is returned.</p></header>", stdout);
        {
            static const char *const values[] = {"", "work", "life", "live", "shop"};
            static const char *const labels[] = {"All territories", "work", "life", "live", "shop"};
            BfAuthoringTree *tree = calloc(1, sizeof(*tree));
            FtOption topts[5], sopts[2];
            size_t i;
            int ok = 0, pos = 0;
            if (!tree) {
                sqlite3_close(db);
                return 1;
            }
            for (i = 0; i < 5; i++) {
                topts[i].value = values[i];
                topts[i].label = labels[i];
                topts[i].checked = !strcmp(territory, values[i]);
            }
            sopts[0].value = "";
            sopts[0].label = "People A to Z";
            sopts[0].checked = !*sort;
            sopts[1].value = "match";
            sopts[1].label = "Best match";
            sopts[1].checked = !strcmp(sort, "match");
            ok = ft_action(tree, "cp-find", NULL, 0, "Search", "get", "/companions", NULL, 0) &&
                 ft_field(tree, "cp-q", "cp-find", pos++, "Search companions", "search", "q",
                          needle, "Search people", 0, "", "", 0, "") &&
                 ft_choice(tree, "cp-territory", "cp-find", pos++, "Territory", "territory", topts, 5);
            if (ok && signed_in)
                ok = ft_choice(tree, "cp-sort", "cp-find", pos++, "Order", "sort", sopts, 2);
            if (ok) ft_render(tree, stdout);
            else {
                bf_authoring_tree_clear(tree);
                free(tree);
            }
        }
        if (bf_companions_page(stdout, db, signed_in ? actor : NULL, territory, needle,
                               *cursor ? (unsigned)strtoul(cursor, NULL, 10) : 0, 24,
                               sort, view, club) < 0) {
            sqlite3_close(db);
            return 1;
        }
        fputs("</section>", stdout);
        if (!*view && !*needle && !*territory && !*club && !*cursor) {
            fputs("<section class=\"cp-section cp-clubs\"><header><p class=\"cp-kicker\">Discovery matrix</p><h2>Clubs with open doors</h2>"
                  "<p>Small groups around a shared practice. Joining means membership and nothing beyond it.</p></header>", stdout);
            bf_clubs_page(stdout, db, signed_in ? actor : NULL);
            fputs("</section><section class=\"cp-cta\"><div><p class=\"cp-kicker\">Host an open seat</p><h2>Going out anyway? Make room for one more.</h2>"
                  "<p>Draft an outing from a place, an intent and a budget. Every plan carries its safety note and cost. Nothing books or pays for you.</p></div>"
                  "<nav><a class=\"cp-cta-main\" href=\"/companions/plans\">Plan an outing</a><a href=\"/companions/events\">See every gathering</a></nav></section>", stdout);
        }
        cp_end();
        sqlite3_close(db);
        return 1;
    }
}
#endif
