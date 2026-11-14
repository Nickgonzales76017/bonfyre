#ifndef BONFYRE_FIELD_DISPATCH_WEB_H
#define BONFYRE_FIELD_DISPATCH_WEB_H
#include "account_continuity_web.h"
/* Field visits: a signed-in person asks for someone to come to a place at a
 * time for a stated need (field_dispatch, keyed by requester user:N). The
 * requester offers it to one named member address; the account behind that
 * address (its person: subject) sees it under "Asked of you" and accepts or
 * declines with its own submit. Nobody is notified, nothing is booked or
 * paid, and only the requester may offer or withdraw, only the named account
 * may answer. States: requested -> offered -> accepted | declined;
 * declined may be offered again; any open state may be withdrawn. */

static void fd_state_tag(const char *state) {
    fputs("<span class=\"ak-tag", stdout);
    if (!strcmp(state, "accepted") || !strcmp(state, "offered")) fputs(" is-on", stdout);
    fputs("\">", stdout);
    fputs(!strcmp(state, "requested") ? "Not yet offered" : !strcmp(state, "offered") ? "Awaiting answer" :
          !strcmp(state, "accepted") ? "Accepted" : !strcmp(state, "declined") ? "Declined" : "Withdrawn", stdout);
    fputs("</span>", stdout);
}
static void fd_page(const AcnViewer *v, const char *said) {
    sqlite3 *db = cms_open_ro();
    int mine = acn_count(db, "SELECT count(*) FROM field_dispatch WHERE namespace='root' AND requester_subject=?1 AND status='published'", v->subject, NULL);
    int asked = v->person[0] ? acn_count(db, "SELECT count(*) FROM field_dispatch WHERE namespace='root' AND assignee_subject=?1 AND state='offered' AND status='published'", v->person, NULL) : 0;
    int accepted = acn_count(db, "SELECT count(*) FROM field_dispatch WHERE namespace='root' AND state='accepted' AND status='published' AND (requester_subject=?1 OR assignee_subject=?2)", v->subject, v->person[0] ? v->person : "-");
    ak_app_http(200, 1);
    ak_app_open("Field visits", "workbench", "route-field-dispatch");
    acn_head("Field tools · Local dispatch", "Field visits and local dispatch",
            "Ask for someone to come to a place, at a time, for something that needs hands. Offer the request to a member by their address; they accept or decline it themselves.");
    printf("<dl class=\"ak-stats\"><div><dt>Your requests</dt><dd class=\"is-on\">%d</dd><small>open or answered</small></div>"
           "<div><dt>Asked of you</dt><dd>%d</dd><small>waiting for your answer</small></div>"
           "<div><dt>Accepted</dt><dd>%d</dd><small>by the person asked</small></div>"
           "<div><dt>Messages sent</dt><dd>None</dd><small>tell them yourself</small></div></dl></header>", mine, asked, accepted);
    if (said) { fputs("<p class=\"cj-notice\" role=\"status\">", stdout); html(said); fputs("</p>", stdout); }
    fputs("<div class=\"ak-grid is-2\" style=\"align-items:start\">", stdout);
    /* Left: the queue of the viewer's own requests. */
    fputs("<section><header style=\"margin-bottom:14px\"><span class=\"ak-kicker\">Your dispatch queue</span><h2 style=\"font:600 1.5rem/1.2 var(--ak-sans)\">Requests you made</h2></header>", stdout);
    sqlite3_stmt *st = NULL; int shown = 0;
    if (db && sqlite3_prepare_v2(db, "SELECT id,place,needed_at,need,COALESCE(assignee_address,''),state,requested_at,COALESCE(answered_at,'') FROM field_dispatch "
                                     "WHERE namespace='root' AND requester_subject=?1 AND status='published' ORDER BY id DESC LIMIT 40", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, v->subject, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            char id[24]; snprintf(id, sizeof(id), "%lld", (long long)sqlite3_column_int64(st, 0));
            const char *state = acn_col(st, 5);
            fputs("<article class=\"ak-panel\" style=\"margin-bottom:12px\" data-dispatch><header><span class=\"ak-kicker\">#", stdout); fputs(id, stdout);
            fputs("</span>", stdout); fd_state_tag(state); fputs("</header><h3>", stdout); html(acn_col(st, 1));
            fputs(" · ", stdout); html(acn_col(st, 2)); fputs("</h3><p style=\"margin:6px 0 10px\">", stdout); html(acn_col(st, 3)); fputs("</p><dl class=\"ak-spec\"><dt>Offered to</dt><dd>", stdout);
            if (*acn_col(st, 4)) html(acn_col(st, 4)); else fputs("nobody yet", stdout);
            fputs("</dd><dt>Requested</dt><dd>", stdout); acn_when(acn_col(st, 6));
            if (*acn_col(st, 7)) { fputs("</dd><dt>Answered</dt><dd>", stdout); acn_when(acn_col(st, 7)); }
            fputs("</dd></dl><div class=\"ak-actions\" style=\"margin-top:12px\">", stdout);
            if (!strcmp(state, "requested") || !strcmp(state, "declined")) {
                BfAuthoringTree *t = calloc(1, sizeof(*t));
                FtField f[] = {{"operation", "offer"}, {"dispatch", id}};
                char aid[40], fid[40]; snprintf(aid, sizeof(aid), "fd-offer-%s", id); snprintf(fid, sizeof(fid), "fd-offer-%s-to", id);
                acn_tree_done(t, t && ft_action(t, aid, NULL, 0, "Offer to this member", "post", "/dispatch", f, 2) &&
                                    ft_field(t, fid, aid, 0, "Member address", "text", "address", "", "name.aurekai.work", 0, NULL, "200", 1, NULL));
            }
            if (strcmp(state, "withdrawn")) { FtField f[] = {{"operation", "withdraw"}, {"dispatch", id}}; acn_button("fd-withdraw", "Withdraw request", "/dispatch", f, 2); }
            fputs("</div></article>", stdout);
            shown++;
        }
    }
    sqlite3_finalize(st); st = NULL;
    if (!shown) fputs("<div class=\"ak-panel\"><p class=\"ak-empty\">No field requests yet. Write the first one with the form.</p></div>", stdout);
    fputs("</section>", stdout);
    /* Right: the intake form. */
    fputs("<section class=\"ak-ask\"><p class=\"ak-kicker\">New field request</p><h2 style=\"margin:6px 0 14px\">Ask for a visit</h2>", stdout);
    {
        BfAuthoringTree *t = calloc(1, sizeof(*t));
        FtField f[] = {{"operation", "request"}};
        acn_tree_done(t, t && ft_action(t, "fd-request", NULL, 0, "Record this request", "post", "/dispatch", f, 1) &&
                            ft_field(t, "fd-place", "fd-request", 0, "Place", "text", "place", "", "Street address or a place people know", 0, NULL, "200", 1, NULL) &&
                            ft_field(t, "fd-when", "fd-request", 1, "When", "text", "needed_at", "", "Saturday morning, or 2026-11-20 09:00", 0, NULL, "120", 1, NULL) &&
                            ft_field(t, "fd-need", "fd-request", 2, "What is needed", "text", "need", "", "What has to be done, and anything to bring", 1, "4", "2000", 1, NULL));
    }
    fputs("<p class=\"ak-empty\">Recording a request tells nobody. Offer it to a member by address, then reach them the way you usually do. Nothing here is a booking, a contract or a payment.</p></section></div>", stdout);
    /* Asked of you */
    fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">Asked of you</span><h2>Requests offered to you</h2></div>"
          "<p>Each was offered to your member address by the person who wrote it. Accepting is your own word; it books nothing.</p></header>", stdout);
    shown = 0;
    if (v->person[0] && db && sqlite3_prepare_v2(db, "SELECT id,place,needed_at,need,state,requested_at FROM field_dispatch WHERE namespace='root' AND assignee_subject=?1 "
                                                     "AND state IN ('offered','accepted') AND status='published' ORDER BY id DESC LIMIT 40", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, v->person, -1, SQLITE_TRANSIENT);
        fputs("<div class=\"ak-grid is-2\">", stdout);
        while (sqlite3_step(st) == SQLITE_ROW) {
            char id[24]; snprintf(id, sizeof(id), "%lld", (long long)sqlite3_column_int64(st, 0));
            fputs("<article class=\"ak-panel\" data-asked><header><span class=\"ak-kicker\">#", stdout); fputs(id, stdout); fputs("</span>", stdout); fd_state_tag(acn_col(st, 4));
            fputs("</header><h3>", stdout); html(acn_col(st, 1)); fputs(" · ", stdout); html(acn_col(st, 2));
            fputs("</h3><p style=\"margin:6px 0 12px\">", stdout); html(acn_col(st, 3)); fputs("</p>", stdout);
            if (!strcmp(acn_col(st, 4), "offered")) {
                fputs("<div class=\"ak-actions\">", stdout);
                FtField a[] = {{"operation", "accept"}, {"dispatch", id}}, d[] = {{"operation", "decline"}, {"dispatch", id}};
                acn_button("fd-accept", "Accept", "/dispatch", a, 2);
                acn_button("fd-decline", "Decline", "/dispatch", d, 2);
                fputs("</div>", stdout);
            }
            fputs("</article>", stdout);
            shown++;
        }
        fputs("</div>", stdout);
    }
    sqlite3_finalize(st);
    if (!shown) fputs(v->person[0] ? "<p class=\"ak-empty\">Nobody has offered you a field request.</p>"
                                   : "<p class=\"ak-empty\">Requests are offered to a member address. This account has none yet, so nothing can be offered to it.</p>", stdout);
    fputs("</section>", stdout);
    if (db) sqlite3_close(db);
    ak_app_close();
}
static int fd_post(const AcnViewer *v, const char *body) {
    char op[16], now[40], id[24] = "";
    acn_iso(time(NULL), now, sizeof(now));
    if (!acn_field(body, "operation", op, sizeof(op))) { acn_notice(400, "Choose an action", "Use the controls on the page.", "/dispatch", "Field visits"); return 1; }
    if (!strcmp(op, "request")) {
        char place[256], when[160], need[2100], did[25];
        if (!acn_field(body, "place", place, sizeof(place)) || !acn_line(place, 2, 200) ||
            !acn_field(body, "needed_at", when, sizeof(when)) || !acn_line(when, 2, 120) ||
            !acn_field(body, "need", need, sizeof(need)) || !acn_text(need, 4, 2000)) {
            acn_notice(400, "Check the request", "Give a place, a time and what is needed.", "/dispatch", "Field visits");
            return 1;
        }
        if (!acn_hex_random(did, 24)) { acn_notice(503, "Nothing was recorded", "Try again.", "/dispatch", "Field visits"); return 1; }
        bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
        acn_kv(&w, "dispatch_id", did); acn_kv(&w, "requester_subject", v->subject); acn_kv(&w, "place", place); acn_kv(&w, "needed_at", when);
        acn_kv(&w, "need", need); acn_kv(&w, "state", "requested"); acn_kv(&w, "requested_at", now);
        acn_kv(&w, "cannot_infer", "A person's stated request for a visit. It is not a booking, contract, payment, safety check, or a message to anyone.");
        acn_kv(&w, "status", "published"); bf_jsonw_obj_end(&w);
        acn_cms_create("field_dispatch", &w);
        sqlite3 *db = cms_open_ro();
        int ok = acn_count(db, "SELECT count(*) FROM field_dispatch WHERE namespace='root' AND dispatch_id=?1 AND requester_subject=?2", did, v->subject);
        if (db) sqlite3_close(db);
        if (!ok) { acn_notice(503, "Request not confirmed", "Reload before trying again.", "/dispatch", "Field visits"); return 1; }
        acn_redirect("/dispatch?recorded=1");
        return 1;
    }
    if (!acn_field(body, "dispatch", id, sizeof(id)) || !*id || strspn(id, "0123456789") != strlen(id)) {
        acn_notice(400, "Choose a request", "Use the controls beside a request.", "/dispatch", "Field visits"); return 1;
    }
    sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL;
    char requester[64] = "", assignee[192] = "", state[24] = "";
    if (db && sqlite3_prepare_v2(db, "SELECT requester_subject,COALESCE(assignee_subject,''),state FROM field_dispatch WHERE namespace='root' AND id=CAST(?1 AS INTEGER) AND status='published'", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) { snprintf(requester, sizeof(requester), "%s", acn_col(st, 0)); snprintf(assignee, sizeof(assignee), "%s", acn_col(st, 1)); snprintf(state, sizeof(state), "%s", acn_col(st, 2)); }
    }
    sqlite3_finalize(st);
    int is_requester = *requester && !strcmp(requester, v->subject);
    int is_assignee = *assignee && v->person[0] && !strcmp(assignee, v->person);
    bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
    const char *need_state = NULL, *message = NULL;
    int allowed = 0;
    if (!strcmp(op, "offer") && is_requester && (!strcmp(state, "requested") || !strcmp(state, "declined"))) {
        char address[256]; PhSite site;
        if (!acn_field(body, "address", address, sizeof(address)) || !acn_line(address, 3, 200)) { if (db) sqlite3_close(db); bf_jsonw_free(&w); acn_notice(400, "Name a member address", "Type the address of the member, such as name.aurekai.work.", "/dispatch", "Field visits"); return 1; }
        for (char *p = address; *p; p++) if (*p >= 'A' && *p <= 'Z') *p = (char)(*p - 'A' + 'a');
        if (!ph_site(db, address, NULL, &site) || !*site.actor) { if (db) sqlite3_close(db); bf_jsonw_free(&w); acn_notice(404, "No member at that address", "Offer the request to a published member address.", "/dispatch", "Field visits"); return 1; }
        if (v->person[0] && !strcmp(site.actor, v->person)) { if (db) sqlite3_close(db); bf_jsonw_free(&w); acn_notice(400, "That address is yours", "Offer the request to someone else.", "/dispatch", "Field visits"); return 1; }
        acn_kv(&w, "assignee_subject", site.actor); acn_kv(&w, "assignee_address", site.host); acn_kv(&w, "state", "offered"); acn_kv(&w, "answered_at", "");
        need_state = state; message = "Offered. Nobody was told; let them know where to find it."; allowed = 1;
    } else if ((!strcmp(op, "accept") || !strcmp(op, "decline")) && is_assignee && !strcmp(state, "offered")) {
        acn_kv(&w, "state", !strcmp(op, "accept") ? "accepted" : "declined"); acn_kv(&w, "answered_at", now);
        need_state = "offered"; message = !strcmp(op, "accept") ? "You accepted. This is your word, not a booking." : "You declined."; allowed = 1;
    } else if (!strcmp(op, "withdraw") && is_requester && strcmp(state, "withdrawn")) {
        acn_kv(&w, "state", "withdrawn"); acn_kv(&w, "status", "archived");
        need_state = state; message = "Request withdrawn."; allowed = 1;
    }
    bf_jsonw_obj_end(&w);
    if (db) sqlite3_close(db);
    if (!allowed) { bf_jsonw_free(&w); acn_notice(403, "Not yours to change", "Only the person who wrote a request may offer or withdraw it, and only the member it was offered to may answer it.", "/dispatch", "Field visits"); return 1; }
    char want[24]; snprintf(want, sizeof(want), "%s", need_state);
    int ok = !w.oom && w.buf && ph_cms("update", "field_dispatch", id, w.buf, "root", "state", want, NULL, NULL);
    bf_jsonw_free(&w);
    db = cms_open_ro();
    ok = ok && !acn_count(db, "SELECT count(*) FROM field_dispatch WHERE id=CAST(?1 AS INTEGER) AND state=?2", id, want);
    if (db) sqlite3_close(db);
    if (!ok) { acn_notice(409, "Change not confirmed", "The request changed while you were looking. Reload it first.", "/dispatch", "Field visits"); return 1; }
    (void)message;
    acn_redirect(!strcmp(op, "accept") ? "/dispatch?answered=accepted" : !strcmp(op, "decline") ? "/dispatch?answered=declined" : "/dispatch?changed=1");
    return 1;
}
static int serve_field_dispatch(const char *method, const char *target, const char *query, const char *body,
                                const char *raw, const char *host, const char *bearer, const char *auth_db) {
    if (strcmp(target, "/dispatch") && strcmp(target, "/dispatch/")) return 0;
    int post = !strcmp(method, "POST");
    if (!post && strcmp(method, "GET")) return 0;
    AcnViewer v;
    if (!acn_viewer(bearer, auth_db, &v)) {
        if (post) { acn_notice(401, "Sign in first", "Field requests belong to a signed-in account. Nothing was recorded.", "/login", "Sign in"); return 1; }
        acn_signin("/dispatch");
        return 1;
    }
    if (post && !pj_same_origin(raw, host)) { acn_notice(403, "Change refused", "Submit the form from this exact secure page. Nothing was recorded.", "/dispatch", "Field visits"); return 1; }
    if (post) return fd_post(&v, body);
    char flag[16] = "";
    const char *said = (query && bf_lex_form_query(query, "recorded", flag, sizeof(flag)) == 1) ? "Request recorded. Nobody has been told yet." :
                       (query && bf_lex_form_query(query, "answered", flag, sizeof(flag)) == 1) ? (!strcmp(flag, "accepted") ? "You accepted. This is your word, not a booking." : "You declined.") :
                       (query && bf_lex_form_query(query, "changed", flag, sizeof(flag)) == 1) ? "Request updated. Nobody was notified." : NULL;
    fd_page(&v, said);
    return 1;
}
#endif
