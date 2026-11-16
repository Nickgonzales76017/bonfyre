#ifndef BONFYRE_DATING_REVIEW_WEB_H
#define BONFYRE_DATING_REVIEW_WEB_H
#include "ui_tree.h"
#include "form_tree.h"
/* /moderation/dating: an institution administrator reviews requests to join
 * introductions. Approving records the age the reviewer confirmed and enrolls
 * the person through the Participation owner; declining closes the request.
 * The reviewer comes from an Auth-verified administrator session, never a form,
 * and the owner refuses anyone reviewing their own request. */

static void dr_page(const char *note, int ok) {
    BfDatingReviewRow rows[100]; size_t count = 0;
    sqlite3 *date = ph_dating_open(0);
    int listed = date && bf_dating_review_pending(date, rows, 100, &count);
    if (date) sqlite3_close(date);
    ak_app_http(200, 1);
    ak_app_open("Review introduction requests", "workbench", "route-dating-review");
    ui_open(stdout,"section","ak-pattern ak-hero");ui_open(stdout,"p","ak-eyebrow");ui_copy(stdout,"Moderation · Introductions");ui_close(stdout,"p");ui_open(stdout,"h1",NULL);ui_copy(stdout,"Requests to join introductions");ui_close(stdout,"h1");ui_open(stdout,"p",NULL);ui_copy(stdout,"Each person asked to take part. Confirm they are an adult and that their profile is fit to be shown, then approve. Approving lets them see and be seen by other approved members; declining closes the request. Nothing is sent to anyone either way.");ui_close(stdout,"p");ui_close(stdout,"section");
    if (note && *note) {
        fputs(ok ? "<p class=\"ak-pattern ak-alert\" role=\"status\">" : "<p class=\"ak-pattern ak-alert ak-tone-danger\" role=\"alert\">", stdout);
        html(note); ui_close(stdout,"p");
    }
    if (!listed) {
        ui_start(stdout,"p");ui_attr(stdout,"class","ak-pattern ak-alert ak-tone-danger");ui_attr(stdout,"role","alert");ui_gt(stdout);ui_copy(stdout,"The private introductions store is not available, so no requests can be shown.");ui_close(stdout,"p");
    } else if (!count) {
        ui_open(stdout,"section","ak-pattern ak-card");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Nothing waiting");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"New requests appear here as people ask to join.");ui_close(stdout,"p");ui_close(stdout,"section");
    } else {
        ui_open(stdout,"section","ak-pattern ak-section");ui_open(stdout,"div","ak-table-wrap");ui_open(stdout,"table","ak-table");ui_open(stdout,"thead",NULL);ui_open(stdout,"tr",NULL);ui_open(stdout,"th",NULL);ui_copy(stdout,"Person");ui_close(stdout,"th");ui_open(stdout,"th",NULL);ui_copy(stdout,"Asked");ui_close(stdout,"th");ui_open(stdout,"th",NULL);ui_copy(stdout,"Confirmed age");ui_close(stdout,"th");ui_open(stdout,"th",NULL);ui_copy(stdout,"Decision");ui_close(stdout,"th");ui_close(stdout,"tr");ui_close(stdout,"thead");ui_open(stdout,"tbody",NULL);
        for (size_t i = 0; i < count; i++) {
            char when[32] = ""; time_t t = (time_t)rows[i].requested_at; struct tm tm;
            if (gmtime_r(&t, &tm)) strftime(when, sizeof(when), "%Y-%m-%d %H:%M UTC", &tm);
            ui_open(stdout,"tr",NULL);ui_open(stdout,"td",NULL); html(rows[i].subject); ui_close(stdout,"td");ui_open(stdout,"td",NULL); html(when);
            ui_close(stdout,"td");ui_start(stdout,"td");ui_attr(stdout,"colspan","2");ui_gt(stdout);
            {
                char id[32];
                const FtField hidden[] = {{"subject", rows[i].subject}};
                static const FtInput age[] = {{.label = "Age", .name = "age", .inputmode = "numeric",
                    .pattern = "[0-9]{2,3}", .min = "18", .max = "115", .placeholder = "18 or over", .required = 1}};
                static const FtSubmit decide[] = {{.label = "Approve", .name = "decision", .value = "approve"},
                    {.label = "Decline", .name = "decision", .value = "decline", .tone = "danger", .novalidate = 1}};
                snprintf(id, sizeof(id), "dr-%zu", i);
                const FtForm form = {.id = id, .action = "/moderation/dating", .hidden = hidden, .nhidden = 1,
                                     .inputs = age, .ninputs = 1, .submits = decide, .nsubmits = 2};
                ft_form(stdout, &form);
            }
            ui_close(stdout,"td");ui_close(stdout,"tr");
        }
        ui_close(stdout,"tbody");ui_close(stdout,"table");ui_close(stdout,"div");ui_close(stdout,"section");
    }
    ak_app_close();
}

static int serve_dating_review(const BfRequest *r) {
    const char *method=r->method,*query=r->query,*body=r->body,*raw=r->raw,*host=r->host,*bearer=r->bearer,*auth_db=r->auth_db,*accept=r->accept;
    (void)method;(void)query;(void)body;(void)raw;(void)host;(void)bearer;(void)auth_db;(void)accept;
    char actor[160], subject_ctx[160]; const char *scope = NULL;
    if (psetup_context(bearer, auth_db, "institution:bonfyre", 1, actor, sizeof(actor),
                       subject_ctx, sizeof(subject_ctx), &scope) != 1 || !scope ||
        strcmp(scope, "institution") || strcmp(subject_ctx, "institution:bonfyre")) {
        ak_app_http(403, 1);
        ak_app_open("Moderators only", "workbench", "route-dating-review");
        ui_open(stdout,"section","ak-pattern ak-card");ui_open(stdout,"h1",NULL);ui_copy(stdout,"Moderators only");ui_close(stdout,"h1");ui_open(stdout,"p",NULL);ui_copy(stdout,"Reviewing introduction requests is for an Aurekai administrator. Sign in with that account.");ui_close(stdout,"p");ui_open(stdout,"p",NULL);ui_start(stdout,"a");ui_attr(stdout,"class","ak-button");ui_attr(stdout,"href","/login?next=/moderation/dating");ui_gt(stdout);ui_copy(stdout,"Sign in");ui_close(stdout,"a");ui_close(stdout,"p");ui_close(stdout,"section");
        ak_app_close();
        return 1;
    }
    if (!strcmp(method, "GET")) { dr_page(NULL, 1); return 1; }
    if (strcmp(method, "POST")) { aw_error(405, "Use the review form.\n"); return 1; }
    if (!pj_same_origin(raw, host)) { aw_error(403, "Submit the review form from this site.\n"); return 1; }
    char subject[192] = "", decision[16] = "", age_s[8] = "";
    query_value(body ? body : "", "subject", subject, sizeof(subject));
    query_value(body ? body : "", "decision", decision, sizeof(decision));
    query_value(body ? body : "", "age", age_s, sizeof(age_s));
    sqlite3 *date = ph_dating_open(1);
    if (!date) { dr_page("The private introductions store is not available. Nothing changed.", 0); return 1; }
    int ok = 0; const char *note;
    if (!strcmp(decision, "approve")) {
        char *end = NULL; long age = strtol(age_s, &end, 10);
        if (!*age_s || !end || *end || age < 18 || age > 115) note = "Enter the person's confirmed age, 18 or over, to approve.";
        else {
            ok = bf_dating_review_approve(date, actor, subject, (unsigned)age);
            note = ok ? "Approved. They can now see and be seen by other approved members."
                      : "Not approved: the request is no longer waiting, or it is your own.";
        }
    } else if (!strcmp(decision, "decline")) {
        ok = bf_dating_review_decline(date, actor, subject);
        note = ok ? "Declined. The request is closed." : "Not declined: the request is no longer waiting, or it is your own.";
    } else note = "Choose Approve or Decline.";
    sqlite3_close(date);
    dr_page(note, ok);
    return 1;
}
#endif
