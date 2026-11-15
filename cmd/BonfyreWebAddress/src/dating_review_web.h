#ifndef BONFYRE_DATING_REVIEW_WEB_H
#define BONFYRE_DATING_REVIEW_WEB_H
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
    fputs("<section class=\"ak-pattern ak-hero\"><p class=\"ak-eyebrow\">Moderation · Introductions</p>"
          "<h1>Requests to join introductions</h1>"
          "<p>Each person asked to take part. Confirm they are an adult and that their profile is fit to be shown, "
          "then approve. Approving lets them see and be seen by other approved members; declining closes the request. "
          "Nothing is sent to anyone either way.</p></section>", stdout);
    if (note && *note) {
        fputs(ok ? "<p class=\"ak-pattern ak-alert\" role=\"status\">" : "<p class=\"ak-pattern ak-alert ak-tone-danger\" role=\"alert\">", stdout);
        html(note); fputs("</p>", stdout);
    }
    if (!listed) {
        fputs("<p class=\"ak-pattern ak-alert ak-tone-danger\" role=\"alert\">The private introductions store is not available, so no requests can be shown.</p>", stdout);
    } else if (!count) {
        fputs("<section class=\"ak-pattern ak-card\"><h2>Nothing waiting</h2><p>New requests appear here as people ask to join.</p></section>", stdout);
    } else {
        fputs("<section class=\"ak-pattern ak-section\"><div class=\"ak-table-wrap\"><table class=\"ak-table\"><thead><tr>"
              "<th>Person</th><th>Asked</th><th>Confirmed age</th><th>Decision</th></tr></thead><tbody>", stdout);
        for (size_t i = 0; i < count; i++) {
            char when[32] = ""; time_t t = (time_t)rows[i].requested_at; struct tm tm;
            if (gmtime_r(&t, &tm)) strftime(when, sizeof(when), "%Y-%m-%d %H:%M UTC", &tm);
            fputs("<tr><td>", stdout); html(rows[i].subject); fputs("</td><td>", stdout); html(when);
            fputs("</td><td colspan=\"2\"><form class=\"ak-pattern ak-action dr-row\" method=\"post\" action=\"/moderation/dating\">"
                  "<input type=\"hidden\" name=\"subject\" value=\"", stdout); html(rows[i].subject);
            fputs("\"><label class=\"ak-field\"><span>Age</span><input name=\"age\" inputmode=\"numeric\" pattern=\"[0-9]{2,3}\" min=\"18\" max=\"115\" required placeholder=\"18 or over\"></label>"
                  "<div class=\"ak-actions\"><button type=\"submit\" name=\"decision\" value=\"approve\">Approve</button>"
                  "<button type=\"submit\" name=\"decision\" value=\"decline\" formnovalidate class=\"ak-tone-danger\">Decline</button></div>"
                  "</form></td></tr>", stdout);
        }
        fputs("</tbody></table></div></section>", stdout);
    }
    ak_app_close();
}

static int serve_dating_review(const BfRequest *r) {
    const char *method=r->method,*target=r->target,*query=r->query,*body=r->body,*raw=r->raw,*host=r->host,*bearer=r->bearer,*auth_db=r->auth_db,*accept=r->accept;
    (void)method;(void)query;(void)body;(void)raw;(void)host;(void)bearer;(void)auth_db;(void)accept;
    if (strcmp(target, "/moderation/dating") && strcmp(target, "/moderation/dating/")) return 0;
    char actor[160], subject_ctx[160]; const char *scope = NULL;
    if (psetup_context(bearer, auth_db, "institution:bonfyre", 1, actor, sizeof(actor),
                       subject_ctx, sizeof(subject_ctx), &scope) != 1 || !scope ||
        strcmp(scope, "institution") || strcmp(subject_ctx, "institution:bonfyre")) {
        ak_app_http(403, 1);
        ak_app_open("Moderators only", "workbench", "route-dating-review");
        fputs("<section class=\"ak-pattern ak-card\"><h1>Moderators only</h1><p>Reviewing introduction requests is for an "
              "Aurekai administrator. Sign in with that account.</p><p><a class=\"ak-button\" href=\"/login?next=/moderation/dating\">Sign in</a></p></section>", stdout);
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
