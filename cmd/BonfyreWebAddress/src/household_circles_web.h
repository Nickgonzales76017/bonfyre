#ifndef BONFYRE_HOUSEHOLD_CIRCLES_WEB_H
#define BONFYRE_HOUSEHOLD_CIRCLES_WEB_H
/* Private circles: a care circle is a community_body with governance_scope
 * care-circle kept in its own CMS namespace ("care"), so no public community
 * read (all of which select namespace 'root') can list it. Its members are
 * community_membership rows in the same namespace; only an active member reads
 * or writes the circle. A household pantry calendar is pantry_entry rows bound
 * to the CMS household and its authority (household_access.h): read with any
 * current household role, write with write or admin. Nothing is sent to
 * anyone: an invitation is a link the steward hands over themselves. */

#define HC_NS "care"
typedef struct { char op[24], request[33], target[97], title[161], text[4001], date[11], kind[16], place[121], value[81], household[64]; } HcForm;

static int hc_parse(const char *body, HcForm *p) {
    memset(p, 0, sizeof(*p));
    PhField f[] = {
        {"op", p->op, sizeof(p->op), 0}, {"request", p->request, sizeof(p->request), 0},
        {"target", p->target, sizeof(p->target), 0}, {"title", p->title, sizeof(p->title), 0},
        {"text", p->text, sizeof(p->text), 1}, {"date", p->date, sizeof(p->date), 0},
        {"kind", p->kind, sizeof(p->kind), 0}, {"place", p->place, sizeof(p->place), 0},
        {"value", p->value, sizeof(p->value), 0}, {"household", p->household, sizeof(p->household), 0},
    };
    if (!body || !ph_fields(body, f, sizeof(f) / sizeof(f[0]), 12000)) return 0;
    return *p->op && strlen(p->request) == 32 && strspn(p->request, "0123456789abcdef") == 32;
}
/* The circle and the viewer's role in it; 0 when not an active member. */
static int hc_circle(sqlite3 *db, const char *key, const char *actor, char title[161], char desc[2001], char role[32]) {
    sqlite3_stmt *s = NULL; int ok = 0;
    *title = *desc = *role = 0;
    if (!cjr_key(key) || !cjr_actor(actor)) return 0;
    if (sqlite3_prepare_v2(db, "SELECT c.title,COALESCE(c.description,''),m.role FROM community_body c JOIN community_membership m ON m.namespace='" HC_NS "' AND m.community=c.community AND m.member_subject=?2 AND m.state='active' AND m.status='published' WHERE c.namespace='" HC_NS "' AND c.community=?1 AND c.governance_scope='care-circle' AND c.status='published' LIMIT 1", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, key, -1, SQLITE_TRANSIENT); sqlite3_bind_text(s, 2, actor, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(s) == SQLITE_ROW) {
            snprintf(title, 161, "%s", (const char *)sqlite3_column_text(s, 0));
            snprintf(desc, 2001, "%s", (const char *)sqlite3_column_text(s, 1));
            snprintf(role, 32, "%s", (const char *)sqlite3_column_text(s, 2));
            ok = 1;
        }
    }
    sqlite3_finalize(s); return ok;
}
static void hc_strip(const char *here) {
    fputs("<nav class=\"ak-wb-strip\" aria-label=\"Household\"><span class=\"ak-kicker\">Private · members only</span><a href=\"/care\"", stdout);
    if (!strcmp(here, "care")) fputs(" aria-current=\"page\"", stdout);
    fputs(">Care circles</a><a href=\"/pantry\"", stdout);
    if (!strcmp(here, "pantry")) fputs(" aria-current=\"page\"", stdout);
    fputs(">Pantry calendar</a><span class=\"ak-live\">Not shared outside its members</span></nav>", stdout);
}

static void hc_care_index(sqlite3 *db, const char *actor, const char *invite_circle, const char *token) {
    char nonce[33];
    ak_app_http(200, 1);
    ak_app_open("Care circles", "workbench", "route-care");
    hc_strip("care");
    fputs("<header class=\"ak-wb-hero is-solo\"><div><span class=\"ak-kicker\">Care circle</span><h1>Care circles</h1><p class=\"ak-lede\">A private circle for the people looking after someone: shared tasks and notes, seen only by the people invited. Nothing here is a medical record, and nothing is sent to anyone.</p></div></header>", stdout);
    if (*token) {
        char circle_title[161] = "";
        gr_row(db, "SELECT c.id,c.title FROM community_body c JOIN community_membership m ON m.namespace='" HC_NS "' AND m.community=c.community AND m.member_subject='invite:'||?2 AND m.state='invited' WHERE c.namespace='" HC_NS "' AND c.community=?1", invite_circle, token, circle_title, sizeof(circle_title), NULL, 0, NULL, 0);
        fputs("<section class=\"ak-ask\"><span class=\"ak-kicker\">Invitation</span>", stdout);
        if (*circle_title) {
            fputs("<h2>Join ", stdout); html(circle_title); fputs("</h2><p>You were handed this link by the circle's steward. Joining lets you and the other members see each other's tasks and notes.</p>", stdout);
            ph_nonce(nonce);
            FtField f[3]; gr_hidden_base(f, "accept", nonce); f[2].name = "target"; f[2].value = token;
            char act[160]; snprintf(act, sizeof(act), "/care/%s/act", invite_circle);
            gr_button("hc-accept", "Join this care circle", act, f, 3);
        } else fputs("<p class=\"ak-empty\">This invitation has already been used or withdrawn.</p>", stdout);
        fputs("</section>", stdout);
    }
    fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">Yours</span><h2>Circles you belong to</h2></div></header><ul class=\"ak-rows\">", stdout);
    sqlite3_stmt *s = NULL; int shown = 0;
    if (sqlite3_prepare_v2(db, "SELECT c.community,c.title,m.role FROM community_body c JOIN community_membership m ON m.namespace='" HC_NS "' AND m.community=c.community AND m.member_subject=?1 AND m.state='active' AND m.status='published' WHERE c.namespace='" HC_NS "' AND c.governance_scope='care-circle' AND c.status='published' ORDER BY c.id DESC LIMIT 50", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, actor, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            shown++;
            fputs("<a href=\"/care/", stdout); html((const char *)sqlite3_column_text(s, 0)); fputs("\"><strong>", stdout);
            html((const char *)sqlite3_column_text(s, 1)); fputs("</strong><small>", stdout);
            fputs(!strcmp((const char *)sqlite3_column_text(s, 2), "steward") ? "You are its steward" : "Member", stdout);
            fputs("</small><span class=\"ak-go\">Open</span></a>", stdout);
        }
    }
    sqlite3_finalize(s);
    fputs("</ul>", stdout);
    if (!shown) fputs("<p class=\"ak-empty\">You are not in a care circle yet.</p>", stdout);
    fputs("</section>", stdout);
    ph_nonce(nonce);
    FtField f[2]; gr_hidden_base(f, "create", nonce);
    GrIn in[] = {
        {"Circle name", "text", "title", "", "", 0, "160", 1, ""},
        {"Who it is for and what it covers", "text", "text", "", "", 1, "2000", 1, ""},
    };
    fputs("<section class=\"ak-sec\"><div class=\"ak-ask\"><span class=\"ak-kicker\">Start one</span><h2>Start a care circle</h2><p>You become its steward. Nobody else can see it until you hand them an invitation link.</p>", stdout);
    gr_form("hc-create", "Start circle", "/care/act", f, 2, in, 2, NULL, NULL, NULL, 0);
    fputs("</div></section>", stdout);
    ak_app_close();
}

static void hc_care(sqlite3 *db, const char *key, const char *actor, const char *title, const char *desc, const char *role) {
    char act[160], nonce[33]; snprintf(act, sizeof(act), "/care/%s/act", key);
    int steward = !strcmp(role, "steward");
    ak_app_http(200, 1);
    ak_app_open(title, "workbench", "route-care");
    hc_strip("care");
    fputs("<header class=\"ak-wb-hero\"><div><span class=\"ak-kicker\">Private care circle</span><h1>", stdout); html(title);
    fputs("</h1><p class=\"ak-lede\">", stdout); html(desc); fputs("</p></div><aside class=\"ak-panel\"><span class=\"ak-kicker\">Who sees this</span><p>Only the members listed below. It is not a medical record and nothing here is sent to a clinic, an insurer or anyone else.</p>", stdout);
    if (steward) {
        ph_nonce(nonce);
        FtField f[2]; gr_hidden_base(f, "invite", nonce);
        gr_button("hc-invite", "Make an invitation link", act, f, 2);
    }
    fputs("</aside></header>", stdout);

    fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">01 / Roster</span><h2>Circle members</h2></div></header><div class=\"ak-grid is-3\">", stdout);
    sqlite3_stmt *s = NULL;
    if (sqlite3_prepare_v2(db, "SELECT membership,member_subject,role,COALESCE(joined_at,'') FROM community_membership WHERE namespace='" HC_NS "' AND community=?1 AND state='active' AND status='published' ORDER BY role='steward' DESC,id LIMIT 40", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, key, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            const char *who = (const char *)sqlite3_column_text(s, 1), *r = (const char *)sqlite3_column_text(s, 2);
            fputs("<article class=\"ak-panel\"><span class=\"ak-tag", stdout); fputs(!strcmp(r, "steward") ? " is-on\">Circle steward" : "\">Member", stdout);
            fputs("</span><h3>", stdout); gr_person(who, actor, NULL); fputs("</h3><p><small>Joined ", stdout);
            html((const char *)sqlite3_column_text(s, 3)); fputs("</small></p>", stdout);
            if (steward && strcmp(who, actor)) {
                ph_nonce(nonce);
                FtField f[3]; gr_hidden_base(f, "remove", nonce); f[2].name = "target"; f[2].value = (const char *)sqlite3_column_text(s, 0);
                char id[120]; snprintf(id, sizeof(id), "hc-rm-%s", (const char *)sqlite3_column_text(s, 0));
                gr_button(id, "Remove from circle", act, f, 3);
            }
            fputs("</article>", stdout);
        }
    }
    sqlite3_finalize(s);
    fputs("</div>", stdout);
    if (steward) {
        int pending = 0;
        if (sqlite3_prepare_v2(db, "SELECT membership,substr(member_subject,8) FROM community_membership WHERE namespace='" HC_NS "' AND community=?1 AND state='invited' AND status='published' AND member_subject LIKE 'invite:%' ORDER BY id DESC LIMIT 10", -1, &s, NULL) == SQLITE_OK) {
            sqlite3_bind_text(s, 1, key, -1, SQLITE_TRANSIENT);
            while (sqlite3_step(s) == SQLITE_ROW) {
                if (!pending++) fputs("<div class=\"ak-panel is-raised\" style=\"margin-top:16px\"><h3>Unused invitation links</h3><p>Hand one to the person yourself. Anyone holding a link can join, so withdraw any you no longer need.</p><ul class=\"ak-rows\">", stdout);
                fputs("<li><strong><code>/care/", stdout); html(key); fputs("?invite=", stdout); html((const char *)sqlite3_column_text(s, 1));
                fputs("</code></strong><span class=\"ak-go\">", stdout);
                ph_nonce(nonce);
                FtField f[3]; gr_hidden_base(f, "remove", nonce); f[2].name = "target"; f[2].value = (const char *)sqlite3_column_text(s, 0);
                char id[120]; snprintf(id, sizeof(id), "hc-wd-%d", pending);
                gr_button(id, "Withdraw", act, f, 3);
                fputs("</span></li>", stdout);
            }
        }
        sqlite3_finalize(s);
        if (pending) fputs("</ul></div>", stdout);
    }
    fputs("</section>", stdout);

    fputs("<div class=\"ak-grid is-main\" style=\"grid-template-columns:minmax(0,1.6fr) minmax(0,1fr);margin-top:32px\"><section class=\"ak-panel\"><header><h2>Shared tasks</h2><span class=\"ak-kicker\">02 / Daily rhythm</span></header><ul class=\"ak-rows\">", stdout);
    int shown = 0;
    if (sqlite3_prepare_v2(db, "SELECT entry,title,COALESCE(body,''),COALESCE(due_on,''),state,author_subject,COALESCE(done_by,'') FROM care_entry WHERE namespace='" HC_NS "' AND circle=?1 AND kind='task' AND status='published' ORDER BY state='open' DESC,COALESCE(due_on,'9999'),id DESC LIMIT 40", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, key, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            const char *state = (const char *)sqlite3_column_text(s, 4);
            shown++;
            fputs("<li><strong>", stdout); html((const char *)sqlite3_column_text(s, 1)); fputs("</strong><small>", stdout);
            if (*(const char *)sqlite3_column_text(s, 3)) { fputs("Due ", stdout); html((const char *)sqlite3_column_text(s, 3)); fputs(" · ", stdout); }
            html((const char *)sqlite3_column_text(s, 2)); fputs("</small><span class=\"ak-go\">", stdout);
            if (!strcmp(state, "open")) {
                ph_nonce(nonce);
                FtField f[3]; gr_hidden_base(f, "done", nonce); f[2].name = "target"; f[2].value = (const char *)sqlite3_column_text(s, 0);
                char id[60]; snprintf(id, sizeof(id), "hc-done-%d", shown);
                gr_button(id, "Mark done", act, f, 3);
            } else { fputs("Done by ", stdout); gr_person((const char *)sqlite3_column_text(s, 6), actor, NULL); }
            fputs("</span></li>", stdout);
        }
    }
    sqlite3_finalize(s);
    fputs("</ul>", stdout);
    if (!shown) fputs("<p class=\"ak-empty\">No shared tasks yet.</p>", stdout);
    {
        ph_nonce(nonce);
        FtField f[3]; gr_hidden_base(f, "add", nonce); f[2].name = "kind"; f[2].value = "task";
        GrIn in[] = {
            {"Task", "text", "title", "", "", 0, "160", 1, ""},
            {"Details", "text", "text", "", "", 0, "2000", 0, ""},
            {"Due", "date", "date", "", "", 0, "10", 0, ""},
        };
        gr_form("hc-task", "Add task", act, f, 3, in, 3, NULL, NULL, NULL, 0);
    }
    fputs("</section><aside class=\"ak-panel is-raised\"><header><h2>Latest note</h2><span class=\"ak-kicker\">03 / Journal</span></header>", stdout);
    if (sqlite3_prepare_v2(db, "SELECT title,COALESCE(body,''),author_subject,created_at FROM care_entry WHERE namespace='" HC_NS "' AND circle=?1 AND kind='note' AND status='published' ORDER BY id DESC LIMIT 1", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, key, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(s) == SQLITE_ROW) {
            fputs("<p class=\"ak-lede\">“", stdout); html((const char *)sqlite3_column_text(s, 1)); fputs("”</p><p><small>", stdout);
            html((const char *)sqlite3_column_text(s, 0)); fputs(" — ", stdout); gr_person((const char *)sqlite3_column_text(s, 2), actor, NULL);
            fputs(", ", stdout); html((const char *)sqlite3_column_text(s, 3)); fputs("</small></p>", stdout);
        } else fputs("<p class=\"ak-empty\">No notes yet.</p>", stdout);
    }
    sqlite3_finalize(s);
    fputs("</aside></div>", stdout);

    fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">04 / Journal</span><h2>Circle notes</h2></div></header><ul class=\"ak-rows\">", stdout);
    shown = 0;
    if (sqlite3_prepare_v2(db, "SELECT title,COALESCE(body,''),author_subject,created_at FROM care_entry WHERE namespace='" HC_NS "' AND circle=?1 AND kind='note' AND status='published' ORDER BY id DESC LIMIT 30 OFFSET 1", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, key, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            shown++;
            fputs("<li><strong>", stdout); html((const char *)sqlite3_column_text(s, 0)); fputs("</strong><small>", stdout);
            html((const char *)sqlite3_column_text(s, 1)); fputs("</small><span class=\"ak-go\">", stdout);
            gr_person((const char *)sqlite3_column_text(s, 2), actor, NULL); fputs("</span></li>", stdout);
        }
    }
    sqlite3_finalize(s);
    fputs("</ul></section>", stdout);
    {
        ph_nonce(nonce);
        FtField f[3]; gr_hidden_base(f, "add", nonce); f[2].name = "kind"; f[2].value = "note";
        GrIn in[] = {
            {"Heading", "text", "title", "", "", 0, "160", 1, ""},
            {"What you noticed", "text", "text", "", "", 1, "4000", 1, ""},
        };
        fputs("<section class=\"ak-sec\"><div class=\"ak-ask\"><span class=\"ak-kicker\">Caregiver journal</span><h2>Record a note for the circle</h2><p>Only this circle's members read it.</p>", stdout);
        gr_form("hc-note", "Save note", act, f, 3, in, 2, NULL, NULL, NULL, 0);
        fputs("</div></section>", stdout);
    }
    fputs("<section class=\"ak-band\"><div><span class=\"ak-kicker\">Nourishment</span><h2>Meals and stores</h2><p>A household's pantry and preservation calendar is kept on its own page, for that household's members.</p></div><div class=\"ak-actions\"><a class=\"ak-btn\" href=\"/pantry\">Open the pantry calendar</a></div></section>", stdout);
    ak_app_close();
}

static int hc_care_commit(sqlite3 *db, const char *key, const char *actor, const HcForm *p, char back[256]) {
    char now[32], rowkey[97], title[161], desc[2001], role[32], cols[2][200]; long long id;
    gr_now(now);
    snprintf(back, 256, "/care/%s", key);
    if (!strcmp(p->op, "create")) {
        if (!gr_words(p->title) || !gr_words(p->text) || strlen(p->text) > 2000) return 400;
        snprintf(rowkey, sizeof(rowkey), "care-%s", p->request);
        char member[97]; snprintf(member, sizeof(member), "care-steward-%s", p->request);
        char subject[128]; snprintf(subject, sizeof(subject), "community_body:%s", rowkey);
        const char *body[] = {"community", rowkey, "subject_address", subject, "title", p->title, "description", p->text,
                              "visibility", "members", "governance_scope", "care-circle", "moderation_policy", "members-only",
                              "created_by_subject", actor,
                              "cannot_infer", "A private care circle; not a medical record, a clinical relationship or a channel to anyone outside it.", "status", "published"};
        const char *mem[] = {"membership", member, "community", rowkey, "member_subject", actor, "role", "steward", "state", "active", "joined_at", now,
                             "cannot_infer", "Membership of a private care circle only; no health, legal or financial authority.", "status", "published"};
        snprintf(back, 256, "/care/%s", rowkey);
        return gr_create("community_body", HC_NS, body, 10) && gr_create("community_membership", HC_NS, mem, 8) ? 303 : 503;
    }
    if (!strcmp(p->op, "accept")) {
        if (strlen(p->target) != 32 || strspn(p->target, "0123456789abcdef") != 32) return 400;
        if (hc_circle(db, key, actor, title, desc, role)) return 303;
        char invite[64]; snprintf(invite, sizeof(invite), "invite:%s", p->target);
        id = gr_row(db, "SELECT id FROM community_membership WHERE namespace='" HC_NS "' AND community=?1 AND member_subject=?2 AND state='invited'", key, invite, NULL, 0, NULL, 0, NULL, 0);
        if (!id) return 404;
        const char *kv[] = {"member_subject", actor, "state", "active", "joined_at", now};
        return gr_update("community_membership", HC_NS, id, "state", "invited", kv, 3) ? 303 : 409;
    }
    if (!hc_circle(db, key, actor, title, desc, role)) return 403;
    int steward = !strcmp(role, "steward");
    if (!strcmp(p->op, "invite")) {
        if (!steward) return 403;
        char token[33], member[97], invite[64]; ph_nonce(token);
        snprintf(member, sizeof(member), "care-invite-%s", p->request);
        snprintf(invite, sizeof(invite), "invite:%s", token);
        const char *kv[] = {"membership", member, "community", key, "member_subject", invite, "role", "member", "state", "invited", "authority_ref", actor,
                            "cannot_infer", "An unused invitation link; it grants nothing until a signed-in person accepts it.", "status", "published"};
        return gr_create("community_membership", HC_NS, kv, 8) ? 303 : 503;
    }
    if (!strcmp(p->op, "remove")) {
        if (!steward || !cjr_key(p->target)) return 403;
        id = gr_row(db, "SELECT id,member_subject,state FROM community_membership WHERE namespace='" HC_NS "' AND community=?1 AND membership=?2", key, p->target, cols[0], 200, cols[1], 200, NULL, 0);
        if (!id) return 404;
        if (!strcmp(cols[0], actor)) return 400;
        const char *kv[] = {"state", "removed", "expires_at", now};
        return gr_update("community_membership", HC_NS, id, "state", cols[1], kv, 2) ? 303 : 409;
    }
    if (!strcmp(p->op, "add")) {
        int task = !strcmp(p->kind, "task");
        if ((!task && strcmp(p->kind, "note")) || !gr_words(p->title) || (!task && !gr_words(p->text)) || (*p->date && !gr_date_ok(p->date))) return 400;
        snprintf(rowkey, sizeof(rowkey), "care-entry-%s", p->request);
        const char *kv[] = {"entry", rowkey, "circle", key, "kind", p->kind, "title", p->title, "body", p->text, "due_on", task ? p->date : "",
                            "state", task ? "open" : "note", "author_subject", actor, "created_at", now,
                            "cannot_infer", "Shared inside a private care circle only; not clinical advice or a record kept for anyone else.", "status", "published"};
        return gr_create("care_entry", HC_NS, kv, 11) ? 303 : 503;
    }
    if (!strcmp(p->op, "done")) {
        if (!cjr_key(p->target)) return 400;
        id = gr_row(db, "SELECT id FROM care_entry WHERE namespace='" HC_NS "' AND circle=?1 AND entry=?2 AND kind='task'", key, p->target, NULL, 0, NULL, 0, NULL, 0);
        if (!id) return 404;
        const char *kv[] = {"state", "done", "done_by", actor};
        return gr_update("care_entry", HC_NS, id, "state", "open", kv, 2) ? 303 : 409;
    }
    return 400;
}

/* ---------------------------------------------------------------- pantry */

static void hc_pantry(sqlite3 *db, const char *actor, const char *chosen) {
    char nonce[33], household[64] = "", label[161] = "";
    BfHouseholdAccess access;
    ak_app_http(200, 1);
    ak_app_open("Pantry calendar", "workbench", "route-pantry");
    hc_strip("pantry");
    sqlite3_stmt *s = NULL; int count = 0;
    const char *list = gr_table(db, "_household_membership") ?
        "SELECT h.household_id,h.label FROM household h WHERE h.namespace='root' AND h.status='published' AND (h.steward=?1 OR EXISTS(SELECT 1 FROM _household_membership m JOIN _household_authority a ON a.household_row=m.household_row AND a.epoch=m.authority_epoch WHERE m.household_row=h.id AND m.member=?1 AND m.role IN ('read','write','admin') AND (m.expires_at=0 OR m.expires_at>CAST(strftime('%s','now') AS INTEGER)))) ORDER BY h.label,h.household_id LIMIT 20" :
        "SELECT household_id,label FROM household WHERE namespace='root' AND steward=?1 AND status='published' ORDER BY label,household_id LIMIT 20";
    fputs("<header class=\"ak-wb-hero\"><div><span class=\"ak-kicker\">Household pantry</span><h1>Pantry and preservation calendar</h1><p class=\"ak-lede\">What your household foraged, put up and has in store, by date. Only your household's members can read it.</p></div><aside class=\"ak-panel\"><span class=\"ak-kicker\">Household</span>", stdout);
    if (gr_table(db, "household") && sqlite3_prepare_v2(db, list, -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, actor, -1, SQLITE_TRANSIENT);
        fputs("<nav class=\"ak-actions\">", stdout);
        while (sqlite3_step(s) == SQLITE_ROW) {
            const char *id = (const char *)sqlite3_column_text(s, 0), *l = (const char *)sqlite3_column_text(s, 1);
            int on = (*chosen && !strcmp(chosen, id)) || (!*chosen && !count);
            if (on) { snprintf(household, sizeof(household), "%s", id); snprintf(label, sizeof(label), "%s", l); }
            fputs("<a class=\"ak-chip\" href=\"/pantry?household=", stdout); url_query_value(id); fputc('"', stdout);
            if (on) fputs(" aria-current=\"page\"", stdout);
            fputs(">", stdout); html(l); fputs("</a>", stdout); count++;
        }
        fputs("</nav>", stdout);
    }
    sqlite3_finalize(s);
    if (!count) fputs("<p class=\"ak-empty\">You are not in a household yet.</p>", stdout);
    fputs("</aside></header>", stdout);
    if (!*household || !bhh_context(db, household, actor, &access) || bhh_rank(access.role) < 1) {
        if (*chosen) fputs("<p class=\"ak-empty\">That household is not one you belong to.</p>", stdout);
        ph_nonce(nonce);
        FtField f[2]; gr_hidden_base(f, "household-open", nonce);
        GrIn in[] = {{"Household name", "text", "title", "", "", 0, "160", 1, ""}};
        fputs("<section class=\"ak-sec\"><div class=\"ak-ask\"><span class=\"ak-kicker\">Start</span><h2>Open a household</h2><p>You become its steward and can give other people access from household access.</p>", stdout);
        gr_form("hc-household", "Open household", "/pantry/act", f, 2, in, 1, NULL, NULL, NULL, 0);
        fputs("</div></section>", stdout);
        ak_app_close(); return;
    }
    int write = bhh_rank(access.role) >= 2;
    printf("<dl class=\"ak-stats is-row\"><div><dt>Entries this season</dt><dd class=\"is-on\">%.0f</dd><small>last 120 days</small></div><div><dt>Foraged</dt><dd>%.0f</dd></div><div><dt>Batches put up</dt><dd>%.0f</dd></div><div><dt>Stores counted</dt><dd>%.0f</dd></div></dl>",
           gr_number(db, "SELECT count(*) FROM pantry_entry WHERE namespace='root' AND household_id=?1 AND status='published' AND julianday(on_date)>=julianday('now')-120", household, NULL),
           gr_number(db, "SELECT count(*) FROM pantry_entry WHERE namespace='root' AND household_id=?1 AND status='published' AND kind='forage'", household, NULL),
           gr_number(db, "SELECT count(*) FROM pantry_entry WHERE namespace='root' AND household_id=?1 AND status='published' AND kind='batch'", household, NULL),
           gr_number(db, "SELECT count(*) FROM pantry_entry WHERE namespace='root' AND household_id=?1 AND status='published' AND kind='store'", household, NULL));

    fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">Harvest horizon</span><h2>By month</h2></div></header>", stdout);
    int shown = 0;
    if (sqlite3_prepare_v2(db, "SELECT substr(on_date,1,7),count(*),group_concat(title,' · ') FROM (SELECT on_date,title FROM pantry_entry WHERE namespace='root' AND household_id=?1 AND status='published' ORDER BY on_date DESC LIMIT 200) GROUP BY 1 ORDER BY 1 DESC LIMIT 4", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, household, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            if (!shown++) fputs("<div class=\"ak-grid\">", stdout);
            fputs("<article class=\"ak-panel\"><span class=\"ak-kicker\">", stdout); html((const char *)sqlite3_column_text(s, 0));
            printf("</span><h3>%d entries</h3><p>", sqlite3_column_int(s, 1)); html((const char *)sqlite3_column_text(s, 2));
            fputs("</p></article>", stdout);
        }
    }
    sqlite3_finalize(s);
    if (shown) fputs("</div>", stdout); else fputs("<p class=\"ak-empty\">Nothing on the calendar yet.</p>", stdout);
    fputs("</section>", stdout);

    fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">Cellar and stores</span><h2>Pantry entries</h2></div></header><div class=\"ak-scroll\"><table class=\"ak-table\"><thead><tr><th>Date</th><th>Entry</th><th>Kind</th><th>Quantity</th><th>Where</th><th>Recorded by</th></tr></thead><tbody>", stdout);
    shown = 0;
    if (sqlite3_prepare_v2(db, "SELECT on_date,title,kind,COALESCE(quantity,''),COALESCE(location,''),author_subject,COALESCE(notes,'') FROM pantry_entry WHERE namespace='root' AND household_id=?1 AND status='published' ORDER BY on_date DESC,id DESC LIMIT 80", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, household, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            const char *kind = (const char *)sqlite3_column_text(s, 2);
            shown++;
            fputs("<tr><td><code>", stdout); html((const char *)sqlite3_column_text(s, 0)); fputs("</code></td><td><strong>", stdout);
            html((const char *)sqlite3_column_text(s, 1)); fputs("</strong>", stdout);
            if (*(const char *)sqlite3_column_text(s, 6)) { fputs("<br><small>", stdout); html((const char *)sqlite3_column_text(s, 6)); fputs("</small>", stdout); }
            fputs("</td><td><span class=\"ak-tag\">", stdout);
            fputs(!strcmp(kind, "forage") ? "Foraged" : !strcmp(kind, "batch") ? "Put up" : "In store", stdout);
            fputs("</span></td><td>", stdout); html((const char *)sqlite3_column_text(s, 3)); fputs("</td><td>", stdout);
            html((const char *)sqlite3_column_text(s, 4)); fputs("</td><td>", stdout);
            gr_person((const char *)sqlite3_column_text(s, 5), actor, NULL); fputs("</td></tr>", stdout);
        }
    }
    sqlite3_finalize(s);
    fputs("</tbody></table></div>", stdout);
    if (!shown) fputs("<p class=\"ak-empty\">No pantry entries yet.</p>", stdout);
    fputs("</section>", stdout);
    if (write) {
        char today[11]; gr_today(today); ph_nonce(nonce);
        FtField f[3]; gr_hidden_base(f, "entry", nonce); f[2].name = "household"; f[2].value = household;
        FtOption o[3] = {{"forage", "Foraged", 1}, {"batch", "Put up a batch", 0}, {"store", "Counted in store", 0}};
        GrIn in[] = {
            {"What", "text", "title", "", "", 0, "160", 1, ""},
            {"Date", "date", "date", today, "", 0, "10", 1, ""},
            {"Quantity", "text", "value", "", "", 0, "80", 0, ""},
            {"Where it is kept or was found", "text", "place", "", "", 0, "120", 0, ""},
            {"Notes", "text", "text", "", "", 1, "2000", 0, ""},
        };
        fputs("<section class=\"ak-sec\"><div class=\"ak-ask\"><span class=\"ak-kicker\">", stdout); html(label);
        fputs("</span><h2>Add to the calendar</h2>", stdout);
        gr_form("hc-pantry", "Add entry", "/pantry/act", f, 3, in, 5, "Kind", "kind", o, 3);
        fputs("</div></section>", stdout);
    } else fputs("<p class=\"ak-empty\">You can read this household's calendar. Its steward can give you write access.</p>", stdout);
    ak_app_close();
}
static int hc_pantry_commit(sqlite3 *db, const char *actor, const HcForm *p, char back[256]) {
    char now[32], key[97]; BfHouseholdAccess access;
    gr_now(now);
    snprintf(back, 256, "/pantry");
    if (!strcmp(p->op, "household-open")) {
        if (!gr_words(p->title)) return 400;
        if (gr_number(db, "SELECT count(*) FROM household WHERE namespace='root' AND steward=?1", actor, NULL) >= 10) return 409;
        char id[64]; snprintf(id, sizeof(id), "hh-%.16s", p->request);
        const char *kv[] = {"household_id", id, "label", p->title, "steward", actor, "opened_at", now,
                            "cannot_infer", "A household its steward opened; read only by its own members.", "status", "published"};
        snprintf(back, 256, "/pantry?household=%s", id);
        return gr_create("household", "root", kv, 6) ? 303 : 503;
    }
    if (!strcmp(p->op, "entry")) {
        if (!bhh_context(db, p->household, actor, &access) || bhh_rank(access.role) < 2) return 403;
        if ((strcmp(p->kind, "forage") && strcmp(p->kind, "batch") && strcmp(p->kind, "store")) || !gr_words(p->title) || !gr_date_ok(p->date)) return 400;
        snprintf(key, sizeof(key), "pantry-%s", p->request);
        snprintf(back, 256, "/pantry?household=%s", p->household);
        const char *kv[] = {"entry", key, "household_id", p->household, "kind", p->kind, "title", p->title, "on_date", p->date,
                            "quantity", p->value, "location", p->place, "notes", p->text, "author_subject", actor, "created_at", now,
                            "cannot_infer", "A household's own pantry note; not a food-safety certification.", "status", "published"};
        return gr_create("pantry_entry", "root", kv, 12) ? 303 : 503;
    }
    return 400;
}
static int hc_status(int code, const char *back) {
    switch (code) {
    case 303: page_redirect(back); break;
    case 403: ph_error(403, "Only this circle's or household's own members can do that.\n"); break;
    case 404: ph_error(404, "That record is not here.\n"); break;
    case 409: ph_error(409, "That record changed. Reopen the page.\n"); break;
    case 503: ph_error(503, "The record owner did not accept the change. Nothing was recorded.\n"); break;
    default: ph_error(400, "Complete the form as written.\n"); break;
    }
    return 1;
}

static int serve_household_circles(const char *method, const char *path, const char *query, const char *body,
                                   const char *raw, const char *host, const char *bearer, const char *auth_db,
                                   const char *accept) {
    if (!path) return 0;
    int care = !strcmp(path, "/care") || !strcmp(path, "/care/") || !strncmp(path, "/care/", 6);
    int pantry = !strcmp(path, "/pantry") || !strcmp(path, "/pantry/") || !strcmp(path, "/pantry/act");
    if (!care && !pantry) return 0;
    int post = !strcmp(method, "POST");
    if (!post && !wants_html(accept)) return 0;
    StudioViewer who = {0}; char actor[64] = "";
    if (session_identity(bearer, auth_db, &who) && who.id > 0) snprintf(actor, sizeof(actor), "user:%ld", who.id);
    if (!*actor) {
        if (post) { ph_error(401, "Sign in first.\n"); return 1; }
        char next[300]; snprintf(next, sizeof(next), "%s%s%s", path, query && *query ? "?" : "", query ? query : "");
        cjw_signin(next); return 1;
    }
    sqlite3 *db = cms_open_ro();
    if (!db) { ph_error(503, "The household records are unavailable.\n"); return 1; }
    if (!gr_table(db, "care_entry") || !gr_table(db, "pantry_entry")) { sqlite3_close(db); ph_error(503, "This store has not adopted care and pantry records yet.\n"); return 1; }
    char back[256];
    if (pantry) {
        if (post) {
            HcForm p;
            if (strcmp(path, "/pantry/act") || !ph_origin(raw, host) || !hc_parse(body, &p)) { sqlite3_close(db); ph_error(403, "Submit one form from the pantry page.\n"); return 1; }
            int code = hc_pantry_commit(db, actor, &p, back); sqlite3_close(db); return hc_status(code, back);
        }
        if (!strcmp(path, "/pantry/act")) { sqlite3_close(db); ph_error(405, "Use the pantry form.\n"); return 1; }
        char chosen[64] = ""; PhField f[] = {{"household", chosen, sizeof(chosen), 0}};
        if (!ph_fields(query, f, 1, 200) || (*chosen && !bhh_slug(chosen))) { sqlite3_close(db); ph_error(400, "Choose one household.\n"); return 1; }
        hc_pantry(db, actor, chosen); sqlite3_close(db); return 1;
    }
    char key[97] = "", rest[32] = "";
    if (strlen(path) > 6) {
        const char *k = path + 6, *slash = strchr(k, '/');
        size_t n = slash ? (size_t)(slash - k) : strlen(k);
        if (n >= sizeof(key)) { sqlite3_close(db); return 0; }
        memcpy(key, k, n); key[n] = 0;
        if (slash) snprintf(rest, sizeof(rest), "%s", slash + 1);
    }
    if (post) {
        HcForm p;
        int is_act = (!strcmp(key, "act") && !*rest) || (cjr_key(key) && !strcmp(rest, "act"));
        if (!is_act || !ph_origin(raw, host) || !hc_parse(body, &p)) { sqlite3_close(db); ph_error(403, "Submit one form from this care circle.\n"); return 1; }
        if (!strcmp(key, "act") && strcmp(p.op, "create")) { sqlite3_close(db); ph_error(400, "Start a circle from the care page.\n"); return 1; }
        int code = hc_care_commit(db, key, actor, &p, back); sqlite3_close(db); return hc_status(code, back);
    }
    char invite[33] = "", circle_q[97] = "";
    PhField f[] = {{"invite", invite, sizeof(invite), 0}, {"circle", circle_q, sizeof(circle_q), 0}};
    if (!ph_fields(query, f, 2, 300) || (*invite && (strlen(invite) != 32 || strspn(invite, "0123456789abcdef") != 32))) { sqlite3_close(db); ph_error(400, "That invitation link is not valid.\n"); return 1; }
    if (!*key) { hc_care_index(db, actor, "", ""); sqlite3_close(db); return 1; }
    if (!cjr_key(key) || *rest) { sqlite3_close(db); return 0; }
    char title[161], desc[2001], role[32];
    if (!hc_circle(db, key, actor, title, desc, role)) {
        if (*invite) { hc_care_index(db, actor, key, invite); sqlite3_close(db); return 1; }
        sqlite3_close(db); ph_error(404, "There is no care circle here that you belong to.\n"); return 1;
    }
    hc_care(db, key, actor, title, desc, role);
    sqlite3_close(db);
    return 1;
}
#endif
