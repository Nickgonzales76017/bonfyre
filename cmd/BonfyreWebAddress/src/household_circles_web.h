#ifndef BONFYRE_HOUSEHOLD_CIRCLES_WEB_H
#define BONFYRE_HOUSEHOLD_CIRCLES_WEB_H
#include "ui_tree.h"
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
    ui_start(stdout,"nav");ui_attr(stdout,"class","ak-wb-strip");ui_attr(stdout,"aria-label","Household");ui_gt(stdout);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Private · members only");ui_close(stdout,"span");ui_start(stdout,"a");ui_attr(stdout,"href","/care");
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
    ui_open(stdout,"header","ak-wb-hero is-solo");ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Care circle");ui_close(stdout,"span");ui_open(stdout,"h1",NULL);ui_copy(stdout,"Care circles");ui_close(stdout,"h1");ui_open(stdout,"p","ak-lede");ui_copy(stdout,"A private circle for the people looking after someone: shared tasks and notes, seen only by the people invited. Nothing here is a medical record, and nothing is sent to anyone.");ui_close(stdout,"p");ui_close(stdout,"div");ui_close(stdout,"header");
    if (*token) {
        char circle_title[161] = "";
        gr_row(db, "SELECT c.id,c.title FROM community_body c JOIN community_membership m ON m.namespace='" HC_NS "' AND m.community=c.community AND m.member_subject='invite:'||?2 AND m.state='invited' WHERE c.namespace='" HC_NS "' AND c.community=?1", invite_circle, token, circle_title, sizeof(circle_title), NULL, 0, NULL, 0);
        ui_open(stdout,"section","ak-ask");ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Invitation");ui_close(stdout,"span");
        if (*circle_title) {
            ui_open(stdout,"h2",NULL);ui_copy(stdout,"Join "); html(circle_title); ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"You were handed this link by the circle's steward. Joining lets you and the other members see each other's tasks and notes.");ui_close(stdout,"p");
            ph_nonce(nonce);
            FtField f[3]; gr_hidden_base(f, "accept", nonce); f[2].name = "target"; f[2].value = token;
            char act[160]; snprintf(act, sizeof(act), "/care/%s/act", invite_circle);
            gr_button("hc-accept", "Join this care circle", act, f, 3);
        } else ui_text(stdout, "p", "ak-empty", "This invitation has already been used or withdrawn.");
        ui_close(stdout,"section");
    }
    ui_open(stdout,"section","ak-sec");ui_open(stdout,"header",NULL);ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Yours");ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Circles you belong to");ui_close(stdout,"h2");ui_close(stdout,"div");ui_close(stdout,"header");ui_open(stdout,"ul","ak-rows");
    sqlite3_stmt *s = NULL; int shown = 0;
    if (sqlite3_prepare_v2(db, "SELECT c.community,c.title,m.role FROM community_body c JOIN community_membership m ON m.namespace='" HC_NS "' AND m.community=c.community AND m.member_subject=?1 AND m.state='active' AND m.status='published' WHERE c.namespace='" HC_NS "' AND c.governance_scope='care-circle' AND c.status='published' ORDER BY c.id DESC LIMIT 50", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, actor, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            shown++;
            ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/care/"); html((const char *)sqlite3_column_text(s, 0)); fputs("\"><strong>", stdout);
            html((const char *)sqlite3_column_text(s, 1)); ui_close(stdout,"strong");ui_open(stdout,"small",NULL);
            fputs(!strcmp((const char *)sqlite3_column_text(s, 2), "steward") ? "You are its steward" : "Member", stdout);
            ui_close(stdout,"small");ui_open(stdout,"span","ak-go");ui_copy(stdout,"Open");ui_close(stdout,"span");ui_close(stdout,"a");
        }
    }
    sqlite3_finalize(s);
    ui_close(stdout,"ul");
    if (!shown) ui_text(stdout, "p", "ak-empty", "You are not in a care circle yet.");
    ui_close(stdout,"section");
    ph_nonce(nonce);
    FtField f[2]; gr_hidden_base(f, "create", nonce);
    GrIn in[] = {
        {"Circle name", "text", "title", "", "", 0, "160", 1, ""},
        {"Who it is for and what it covers", "text", "text", "", "", 1, "2000", 1, ""},
    };
    ui_open(stdout,"section","ak-sec");ui_open(stdout,"div","ak-ask");ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Start one");ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Start a care circle");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"You become its steward. Nobody else can see it until you hand them an invitation link.");ui_close(stdout,"p");
    gr_form("hc-create", "Start circle", "/care/act", f, 2, in, 2, NULL, NULL, NULL, 0);
    ui_close(stdout,"div");ui_close(stdout,"section");
    ak_app_close();
}

static void hc_care(sqlite3 *db, const char *key, const char *actor, const char *title, const char *desc, const char *role) {
    char act[160], nonce[33]; snprintf(act, sizeof(act), "/care/%s/act", key);
    int steward = !strcmp(role, "steward");
    ak_app_http(200, 1);
    ak_app_open(title, "workbench", "route-care");
    hc_strip("care");
    ui_open(stdout,"header","ak-wb-hero");ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Private care circle");ui_close(stdout,"span");ui_open(stdout,"h1",NULL); html(title);
    ui_close(stdout,"h1");ui_open(stdout,"p","ak-lede"); html(desc); ui_close(stdout,"p");ui_close(stdout,"div");ui_open(stdout,"aside","ak-panel");ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Who sees this");ui_close(stdout,"span");ui_open(stdout,"p",NULL);ui_copy(stdout,"Only the members listed below. It is not a medical record and nothing here is sent to a clinic, an insurer or anyone else.");ui_close(stdout,"p");
    if (steward) {
        ph_nonce(nonce);
        FtField f[2]; gr_hidden_base(f, "invite", nonce);
        gr_button("hc-invite", "Make an invitation link", act, f, 2);
    }
    ui_close(stdout,"aside");ui_close(stdout,"header");

    ui_open(stdout,"section","ak-sec");ui_open(stdout,"header",NULL);ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"01 / Roster");ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Circle members");ui_close(stdout,"h2");ui_close(stdout,"div");ui_close(stdout,"header");ui_open(stdout,"div","ak-grid is-3");
    sqlite3_stmt *s = NULL;
    if (sqlite3_prepare_v2(db, "SELECT membership,member_subject,role,COALESCE(joined_at,'') FROM community_membership WHERE namespace='" HC_NS "' AND community=?1 AND state='active' AND status='published' ORDER BY role='steward' DESC,id LIMIT 40", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, key, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            const char *who = (const char *)sqlite3_column_text(s, 1), *r = (const char *)sqlite3_column_text(s, 2);
            ui_open(stdout,"article","ak-panel");ui_start(stdout,"span");ui_attr_open(stdout,"class");ui_copy(stdout,"ak-tag"); fputs(!strcmp(r, "steward") ? " is-on\">Circle steward" : "\">Member", stdout);
            ui_close(stdout,"span");ui_open(stdout,"h3",NULL); gr_person(who, actor, NULL); ui_close(stdout,"h3");ui_open(stdout,"p",NULL);ui_open(stdout,"small",NULL);ui_copy(stdout,"Joined ");
            html((const char *)sqlite3_column_text(s, 3)); ui_close(stdout,"small");ui_close(stdout,"p");
            if (steward && strcmp(who, actor)) {
                ph_nonce(nonce);
                FtField f[3]; gr_hidden_base(f, "remove", nonce); f[2].name = "target"; f[2].value = (const char *)sqlite3_column_text(s, 0);
                char id[120]; snprintf(id, sizeof(id), "hc-rm-%s", (const char *)sqlite3_column_text(s, 0));
                gr_button(id, "Remove from circle", act, f, 3);
            }
            ui_close(stdout,"article");
        }
    }
    sqlite3_finalize(s);
    ui_close(stdout,"div");
    if (steward) {
        int pending = 0;
        if (sqlite3_prepare_v2(db, "SELECT membership,substr(member_subject,8) FROM community_membership WHERE namespace='" HC_NS "' AND community=?1 AND state='invited' AND status='published' AND member_subject LIKE 'invite:%' ORDER BY id DESC LIMIT 10", -1, &s, NULL) == SQLITE_OK) {
            sqlite3_bind_text(s, 1, key, -1, SQLITE_TRANSIENT);
            while (sqlite3_step(s) == SQLITE_ROW) {
                if (!pending++) {ui_start(stdout,"div");ui_attr(stdout,"class","ak-panel is-raised");ui_attr(stdout,"style","margin-top:16px");ui_gt(stdout);ui_open(stdout,"h3",NULL);ui_copy(stdout,"Unused invitation links");ui_close(stdout,"h3");ui_open(stdout,"p",NULL);ui_copy(stdout,"Hand one to the person yourself. Anyone holding a link can join, so withdraw any you no longer need.");ui_close(stdout,"p");ui_open(stdout,"ul","ak-rows");}
                ui_open(stdout,"li",NULL);ui_open(stdout,"strong",NULL);ui_open(stdout,"code",NULL);ui_copy(stdout,"/care/"); html(key); fputs("?invite=", stdout); html((const char *)sqlite3_column_text(s, 1));
                ui_close(stdout,"code");ui_close(stdout,"strong");ui_open(stdout,"span","ak-go");
                ph_nonce(nonce);
                FtField f[3]; gr_hidden_base(f, "remove", nonce); f[2].name = "target"; f[2].value = (const char *)sqlite3_column_text(s, 0);
                char id[120]; snprintf(id, sizeof(id), "hc-wd-%d", pending);
                gr_button(id, "Withdraw", act, f, 3);
                ui_close(stdout,"span");ui_close(stdout,"li");
            }
        }
        sqlite3_finalize(s);
        if (pending) {ui_close(stdout,"ul");ui_close(stdout,"div");}
    }
    ui_close(stdout,"section");

    ui_start(stdout,"div");ui_attr(stdout,"class","ak-grid is-main");ui_attr(stdout,"style","grid-template-columns:minmax(0,1.6fr) minmax(0,1fr);margin-top:32px");ui_gt(stdout);ui_open(stdout,"section","ak-panel");ui_open(stdout,"header",NULL);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Shared tasks");ui_close(stdout,"h2");ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"02 / Daily rhythm");ui_close(stdout,"span");ui_close(stdout,"header");ui_open(stdout,"ul","ak-rows");
    int shown = 0;
    if (sqlite3_prepare_v2(db, "SELECT entry,title,COALESCE(body,''),COALESCE(due_on,''),state,author_subject,COALESCE(done_by,'') FROM care_entry WHERE namespace='" HC_NS "' AND circle=?1 AND kind='task' AND status='published' ORDER BY state='open' DESC,COALESCE(due_on,'9999'),id DESC LIMIT 40", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, key, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            const char *state = (const char *)sqlite3_column_text(s, 4);
            shown++;
            ui_open(stdout,"li",NULL);ui_open(stdout,"strong",NULL); html((const char *)sqlite3_column_text(s, 1)); ui_close(stdout,"strong");ui_open(stdout,"small",NULL);
            if (*(const char *)sqlite3_column_text(s, 3)) { fputs("Due ", stdout); html((const char *)sqlite3_column_text(s, 3)); fputs(" · ", stdout); }
            html((const char *)sqlite3_column_text(s, 2)); ui_close(stdout,"small");ui_open(stdout,"span","ak-go");
            if (!strcmp(state, "open")) {
                ph_nonce(nonce);
                FtField f[3]; gr_hidden_base(f, "done", nonce); f[2].name = "target"; f[2].value = (const char *)sqlite3_column_text(s, 0);
                char id[60]; snprintf(id, sizeof(id), "hc-done-%d", shown);
                gr_button(id, "Mark done", act, f, 3);
            } else { fputs("Done by ", stdout); gr_person((const char *)sqlite3_column_text(s, 6), actor, NULL); }
            ui_close(stdout,"span");ui_close(stdout,"li");
        }
    }
    sqlite3_finalize(s);
    ui_close(stdout,"ul");
    if (!shown) ui_text(stdout, "p", "ak-empty", "No shared tasks yet.");
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
    ui_close(stdout,"section");ui_open(stdout,"aside","ak-panel is-raised");ui_open(stdout,"header",NULL);ui_open(stdout,"h2",NULL);ui_copy(stdout,"Latest note");ui_close(stdout,"h2");ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"03 / Journal");ui_close(stdout,"span");ui_close(stdout,"header");
    if (sqlite3_prepare_v2(db, "SELECT title,COALESCE(body,''),author_subject,created_at FROM care_entry WHERE namespace='" HC_NS "' AND circle=?1 AND kind='note' AND status='published' ORDER BY id DESC LIMIT 1", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, key, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(s) == SQLITE_ROW) {
            ui_open(stdout,"p","ak-lede");ui_copy(stdout,"“"); html((const char *)sqlite3_column_text(s, 1)); fputs("”</p><p><small>", stdout);
            html((const char *)sqlite3_column_text(s, 0)); fputs(" — ", stdout); gr_person((const char *)sqlite3_column_text(s, 2), actor, NULL);
            fputs(", ", stdout); html((const char *)sqlite3_column_text(s, 3)); ui_close(stdout,"small");ui_close(stdout,"p");
        } else ui_text(stdout, "p", "ak-empty", "No notes yet.");
    }
    sqlite3_finalize(s);
    ui_close(stdout,"aside");ui_close(stdout,"div");

    ui_open(stdout,"section","ak-sec");ui_open(stdout,"header",NULL);ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"04 / Journal");ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Circle notes");ui_close(stdout,"h2");ui_close(stdout,"div");ui_close(stdout,"header");ui_open(stdout,"ul","ak-rows");
    shown = 0;
    if (sqlite3_prepare_v2(db, "SELECT title,COALESCE(body,''),author_subject,created_at FROM care_entry WHERE namespace='" HC_NS "' AND circle=?1 AND kind='note' AND status='published' ORDER BY id DESC LIMIT 30 OFFSET 1", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, key, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            shown++;
            ui_open(stdout,"li",NULL);ui_open(stdout,"strong",NULL); html((const char *)sqlite3_column_text(s, 0)); ui_close(stdout,"strong");ui_open(stdout,"small",NULL);
            html((const char *)sqlite3_column_text(s, 1)); ui_close(stdout,"small");ui_open(stdout,"span","ak-go");
            gr_person((const char *)sqlite3_column_text(s, 2), actor, NULL); ui_close(stdout,"span");ui_close(stdout,"li");
        }
    }
    sqlite3_finalize(s);
    ui_close(stdout,"ul");ui_close(stdout,"section");
    {
        ph_nonce(nonce);
        FtField f[3]; gr_hidden_base(f, "add", nonce); f[2].name = "kind"; f[2].value = "note";
        GrIn in[] = {
            {"Heading", "text", "title", "", "", 0, "160", 1, ""},
            {"What you noticed", "text", "text", "", "", 1, "4000", 1, ""},
        };
        ui_open(stdout,"section","ak-sec");ui_open(stdout,"div","ak-ask");ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Caregiver journal");ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Record a note for the circle");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Only this circle's members read it.");ui_close(stdout,"p");
        gr_form("hc-note", "Save note", act, f, 3, in, 2, NULL, NULL, NULL, 0);
        ui_close(stdout,"div");ui_close(stdout,"section");
    }
    ui_open(stdout,"section","ak-band");ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Nourishment");ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Meals and stores");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"A household's pantry and preservation calendar is kept on its own page, for that household's members.");ui_close(stdout,"p");ui_close(stdout,"div");ui_open(stdout,"div","ak-actions");ui_start(stdout,"a");ui_attr(stdout,"class","ak-btn");ui_attr(stdout,"href","/pantry");ui_gt(stdout);ui_copy(stdout,"Open the pantry calendar");ui_close(stdout,"a");ui_close(stdout,"div");ui_close(stdout,"section");
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
    ui_open(stdout,"header","ak-wb-hero");ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Household pantry");ui_close(stdout,"span");ui_open(stdout,"h1",NULL);ui_copy(stdout,"Pantry and preservation calendar");ui_close(stdout,"h1");ui_open(stdout,"p","ak-lede");ui_copy(stdout,"What your household foraged, put up and has in store, by date. Only your household's members can read it.");ui_close(stdout,"p");ui_close(stdout,"div");ui_open(stdout,"aside","ak-panel");ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Household");ui_close(stdout,"span");
    if (gr_table(db, "household") && sqlite3_prepare_v2(db, list, -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, actor, -1, SQLITE_TRANSIENT);
        ui_open(stdout,"nav","ak-actions");
        while (sqlite3_step(s) == SQLITE_ROW) {
            const char *id = (const char *)sqlite3_column_text(s, 0), *l = (const char *)sqlite3_column_text(s, 1);
            int on = (*chosen && !strcmp(chosen, id)) || (!*chosen && !count);
            if (on) { snprintf(household, sizeof(household), "%s", id); snprintf(label, sizeof(label), "%s", l); }
            ui_start(stdout,"a");ui_attr(stdout,"class","ak-chip");ui_attr_open(stdout,"href");ui_copy(stdout,"/pantry?household="); url_query_value(id); fputc('"', stdout);
            if (on) fputs(" aria-current=\"page\"", stdout);
            fputs(">", stdout); html(l); ui_close(stdout,"a"); count++;
        }
        ui_close(stdout,"nav");
    }
    sqlite3_finalize(s);
    if (!count) ui_text(stdout, "p", "ak-empty", "You are not in a household yet.");
    ui_close(stdout,"aside");ui_close(stdout,"header");
    if (!*household || !bhh_context(db, household, actor, &access) || bhh_rank(access.role) < 1) {
        if (*chosen) ui_text(stdout, "p", "ak-empty", "That household is not one you belong to.");
        ph_nonce(nonce);
        FtField f[2]; gr_hidden_base(f, "household-open", nonce);
        GrIn in[] = {{"Household name", "text", "title", "", "", 0, "160", 1, ""}};
        ui_open(stdout,"section","ak-sec");ui_open(stdout,"div","ak-ask");ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Start");ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Open a household");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"You become its steward and can give other people access from household access.");ui_close(stdout,"p");
        gr_form("hc-household", "Open household", "/pantry/act", f, 2, in, 1, NULL, NULL, NULL, 0);
        ui_close(stdout,"div");ui_close(stdout,"section");
        ak_app_close(); return;
    }
    int write = bhh_rank(access.role) >= 2;
    ui_open(stdout,"dl","ak-stats is-row");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Entries this season");ui_close(stdout,"dt");ui_open(stdout,"dd","is-on");fprintf(stdout,"%.0f",gr_number(db, "SELECT count(*) FROM pantry_entry WHERE namespace='root' AND household_id=?1 AND status='published' AND julianday(on_date)>=julianday('now')-120", household, NULL));ui_close(stdout,"dd");ui_open(stdout,"small",NULL);ui_copy(stdout,"last 120 days");ui_close(stdout,"small");ui_close(stdout,"div");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Foraged");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);fprintf(stdout,"%.0f",gr_number(db, "SELECT count(*) FROM pantry_entry WHERE namespace='root' AND household_id=?1 AND status='published' AND kind='forage'", household, NULL));ui_close(stdout,"dd");ui_close(stdout,"div");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Batches put up");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);fprintf(stdout,"%.0f",gr_number(db, "SELECT count(*) FROM pantry_entry WHERE namespace='root' AND household_id=?1 AND status='published' AND kind='batch'", household, NULL));ui_close(stdout,"dd");ui_close(stdout,"div");ui_open(stdout,"div",NULL);ui_open(stdout,"dt",NULL);ui_copy(stdout,"Stores counted");ui_close(stdout,"dt");ui_open(stdout,"dd",NULL);fprintf(stdout,"%.0f",gr_number(db, "SELECT count(*) FROM pantry_entry WHERE namespace='root' AND household_id=?1 AND status='published' AND kind='store'", household, NULL));ui_close(stdout,"dd");ui_close(stdout,"div");ui_close(stdout,"dl");

    ui_open(stdout,"section","ak-sec");ui_open(stdout,"header",NULL);ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Harvest horizon");ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"By month");ui_close(stdout,"h2");ui_close(stdout,"div");ui_close(stdout,"header");
    int shown = 0;
    if (sqlite3_prepare_v2(db, "SELECT substr(on_date,1,7),count(*),group_concat(title,' · ') FROM (SELECT on_date,title FROM pantry_entry WHERE namespace='root' AND household_id=?1 AND status='published' ORDER BY on_date DESC LIMIT 200) GROUP BY 1 ORDER BY 1 DESC LIMIT 4", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, household, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            if (!shown++) ui_open(stdout,"div","ak-grid");
            ui_open(stdout,"article","ak-panel");ui_open(stdout,"span","ak-kicker"); html((const char *)sqlite3_column_text(s, 0));
            ui_close(stdout,"span");ui_open(stdout,"h3",NULL);fprintf(stdout,"%d",sqlite3_column_int(s, 1));ui_copy(stdout," entries");ui_close(stdout,"h3");ui_open(stdout,"p",NULL); html((const char *)sqlite3_column_text(s, 2));
            ui_close(stdout,"p");ui_close(stdout,"article");
        }
    }
    sqlite3_finalize(s);
    if (shown) ui_close(stdout,"div"); else ui_text(stdout, "p", "ak-empty", "Nothing on the calendar yet.");
    ui_close(stdout,"section");

    ui_open(stdout,"section","ak-sec");ui_open(stdout,"header",NULL);ui_open(stdout,"div",NULL);ui_open(stdout,"span","ak-kicker");ui_copy(stdout,"Cellar and stores");ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Pantry entries");ui_close(stdout,"h2");ui_close(stdout,"div");ui_close(stdout,"header");ui_open(stdout,"div","ak-scroll");ui_open(stdout,"table","ak-table");ui_open(stdout,"thead",NULL);ui_open(stdout,"tr",NULL);ui_open(stdout,"th",NULL);ui_copy(stdout,"Date");ui_close(stdout,"th");ui_open(stdout,"th",NULL);ui_copy(stdout,"Entry");ui_close(stdout,"th");ui_open(stdout,"th",NULL);ui_copy(stdout,"Kind");ui_close(stdout,"th");ui_open(stdout,"th",NULL);ui_copy(stdout,"Quantity");ui_close(stdout,"th");ui_open(stdout,"th",NULL);ui_copy(stdout,"Where");ui_close(stdout,"th");ui_open(stdout,"th",NULL);ui_copy(stdout,"Recorded by");ui_close(stdout,"th");ui_close(stdout,"tr");ui_close(stdout,"thead");ui_open(stdout,"tbody",NULL);
    shown = 0;
    if (sqlite3_prepare_v2(db, "SELECT on_date,title,kind,COALESCE(quantity,''),COALESCE(location,''),author_subject,COALESCE(notes,'') FROM pantry_entry WHERE namespace='root' AND household_id=?1 AND status='published' ORDER BY on_date DESC,id DESC LIMIT 80", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, household, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            const char *kind = (const char *)sqlite3_column_text(s, 2);
            shown++;
            ui_open(stdout,"tr",NULL);ui_open(stdout,"td",NULL);ui_open(stdout,"code",NULL); html((const char *)sqlite3_column_text(s, 0)); ui_close(stdout,"code");ui_close(stdout,"td");ui_open(stdout,"td",NULL);ui_open(stdout,"strong",NULL);
            html((const char *)sqlite3_column_text(s, 1)); ui_close(stdout,"strong");
            if (*(const char *)sqlite3_column_text(s, 6)) { ui_open(stdout,"br",NULL);ui_open(stdout,"small",NULL); html((const char *)sqlite3_column_text(s, 6)); ui_close(stdout,"small"); }
            ui_close(stdout,"td");ui_open(stdout,"td",NULL);ui_open(stdout,"span","ak-tag");
            fputs(!strcmp(kind, "forage") ? "Foraged" : !strcmp(kind, "batch") ? "Put up" : "In store", stdout);
            ui_close(stdout,"span");ui_close(stdout,"td");ui_open(stdout,"td",NULL); html((const char *)sqlite3_column_text(s, 3)); ui_close(stdout,"td");ui_open(stdout,"td",NULL);
            html((const char *)sqlite3_column_text(s, 4)); ui_close(stdout,"td");ui_open(stdout,"td",NULL);
            gr_person((const char *)sqlite3_column_text(s, 5), actor, NULL); ui_close(stdout,"td");ui_close(stdout,"tr");
        }
    }
    sqlite3_finalize(s);
    ui_close(stdout,"tbody");ui_close(stdout,"table");ui_close(stdout,"div");
    if (!shown) ui_text(stdout, "p", "ak-empty", "No pantry entries yet.");
    ui_close(stdout,"section");
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
        ui_open(stdout,"section","ak-sec");ui_open(stdout,"div","ak-ask");ui_open(stdout,"span","ak-kicker"); html(label);
        ui_close(stdout,"span");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Add to the calendar");ui_close(stdout,"h2");
        gr_form("hc-pantry", "Add entry", "/pantry/act", f, 3, in, 5, "Kind", "kind", o, 3);
        ui_close(stdout,"div");ui_close(stdout,"section");
    } else {ui_open(stdout,"p","ak-empty");ui_copy(stdout,"You can read this household's calendar. Its steward can give you write access.");ui_close(stdout,"p");}
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

static int serve_household_circles(const BfRequest *r) {
    const char *method=r->method,*path=r->target,*query=r->query,*body=r->body,*raw=r->raw,*host=r->host,*bearer=r->bearer,*auth_db=r->auth_db,*accept=r->accept;
    (void)method;(void)query;(void)body;(void)raw;(void)host;(void)bearer;(void)auth_db;(void)accept;
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
