#ifndef BONFYRE_ACCOUNT_CONTINUITY_WEB_H
#define BONFYRE_ACCOUNT_CONTINUITY_WEB_H
#include "form_tree.h"
/* Account continuity: the devices a person names, moving their own site
 * export to a new device, the declared Bluesky links they ask to have read,
 * and which agents may read each of their saved feeds. Every record is a
 * CMS row keyed by the signed-in account subject (user:N) and written only
 * through the CMS owner after an explicit, same-origin submit. auth.db is
 * never written here.
 *
 *   /account/devices          device_key: label + fingerprint as typed;
 *                             attestation is always "not verified".
 *   /account/move             device_pairing: a one-use code, ten minutes,
 *   /account/move/receive     only its digest kept; the same account on
 *                             another device enters it, then downloads its
 *                             own /cms/compose/export. Two submits, no push.
 *   /connections/bluesky/sync carrier_sync_request: requested, not run;
 *                             there is no AT Protocol reader to run it.
 *   /account/agents           feed_agent_policy: per saved feed and named
 *                             agent. No MCP or /connect read path serves
 *                             saved feeds yet, so the rule is on record and
 *                             the page says it is not enforced. */

typedef struct { StudioViewer who; char subject[64], person[192]; } AcnViewer;

static int acn_viewer(const char *bearer, const char *auth_db, AcnViewer *v) {
    memset(v, 0, sizeof(*v));
    if (!bearer || !*bearer || !auth_db || !session_identity(bearer, auth_db, &v->who) || v->who.id < 1) return 0;
    snprintf(v->subject, sizeof(v->subject), "user:%ld", v->who.id);
    if (!ph_actor(v->who.id, auth_db, v->person)) v->person[0] = 0;
    return 1;
}
static void acn_iso(time_t t, char *out, size_t cap) {
    struct tm tm;
    if (!gmtime_r(&t, &tm) || !strftime(out, cap, "%Y-%m-%dT%H:%M:%SZ", &tm)) snprintf(out, cap, "%s", "");
}
static void acn_when(const char *iso) {
    /* 2026-11-13T18:04:00Z -> 2026-11-13 18:04 UTC */
    if (!iso || strlen(iso) < 16) { fputs("&mdash;", stdout); return; }
    printf("%.10s %.5s UTC", iso, iso + 11);
}
static int acn_hex_random(char *out, size_t hexlen) {
    unsigned char b[32]; size_t n = (hexlen + 1) / 2; FILE *f;
    static const char hex[] = "0123456789abcdef";
    if (n > sizeof(b) || !(f = fopen("/dev/urandom", "rb"))) return 0;
    n = fread(b, 1, n, f); fclose(f);
    if (n != (hexlen + 1) / 2) return 0;
    for (size_t i = 0; i < hexlen; i++) out[i] = hex[(i & 1) ? (b[i / 2] & 15) : (b[i / 2] >> 4)];
    out[hexlen] = 0;
    memset(b, 0, sizeof(b));
    return 1;
}
static void acn_digest(const char *a, const char *b, char out[65]) {
    char seed[512];
    snprintf(seed, sizeof(seed), "%s\n%s", a ? a : "", b ? b : "");
    bf_sha256_hex((const uint8_t *)seed, strlen(seed), out);
    memset(seed, 0, sizeof(seed));
}
static int acn_field(const char *body, const char *key, char *out, size_t cap) {
    out[0] = 0;
    return body && bf_lex_form_query(body, key, out, cap) == 1;
}
/* One printable line: no control bytes, bounded. */
static int acn_line(const char *s, size_t min, size_t max) {
    size_t n = s ? strlen(s) : 0;
    if (n < min || n > max) return 0;
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) if (*p < 32 || *p == 127) return 0;
    return 1;
}
static int acn_text(const char *s, size_t min, size_t max) {
    size_t n = s ? strlen(s) : 0;
    if (n < min || n > max) return 0;
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) if ((*p < 32 && *p != '\n' && *p != '\r' && *p != '\t') || *p == 127) return 0;
    return 1;
}
static void acn_redirect(const char *location) {
    printf("HTTP/1.1 303 See Other\r\nLocation: %s\r\nCache-Control: no-store\r\nReferrer-Policy: same-origin\r\n"
           "Content-Length: 0\r\nConnection: close\r\n\r\n", location);
}
static void acn_signin(const char *next) {
    fputs("HTTP/1.1 303 See Other\r\nLocation: /login?next=", stdout);
    url_query_value(next);
    fputs("\r\nCache-Control: no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", stdout);
}
static void acn_notice(int code, const char *title, const char *message, const char *back, const char *back_label) {
    ak_app_http(code, 1);
    ak_app_open(title, "workbench", "route-account-continuity");
    fputs("<header class=\"ak-wb-hero is-solo\"><div><span class=\"ak-kicker\">Nothing was changed</span><h1>", stdout);
    html(title);
    fputs("</h1><p class=\"ak-lede\" role=\"alert\">", stdout);
    html(message);
    fputs("</p><div class=\"ak-actions\"><a class=\"ak-btn is-primary\" href=\"", stdout);
    html(back);
    fputs("\">", stdout);
    html(back_label);
    fputs("</a></div></div></header>", stdout);
    ak_app_close();
}
/* Writes go through the one CMS owner; namespace is root, every row carries
 * the account subject. Callers read the row back before claiming success. */
static int acn_cms_create(const char *type, bf_jsonw *w) {
    int ok = !w->oom && w->buf && ph_cms("create", type, NULL, w->buf, "root", NULL, NULL, NULL, NULL);
    bf_jsonw_free(w);
    return ok;
}
static void acn_kv(bf_jsonw *w, const char *k, const char *v) { bf_jsonw_key(w, k); bf_jsonw_str(w, v ? v : ""); }
static int acn_count(sqlite3 *db, const char *sql, const char *a, const char *b) {
    sqlite3_stmt *st = NULL; int n = 0;
    if (db && sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
        if (a) sqlite3_bind_text(st, 1, a, -1, SQLITE_TRANSIENT);
        if (b) sqlite3_bind_text(st, 2, b, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) n = sqlite3_column_int(st, 0);
    }
    sqlite3_finalize(st);
    return n;
}
static const char *acn_col(sqlite3_stmt *st, int i) { const char *s = (const char *)sqlite3_column_text(st, i); return s ? s : ""; }
static void acn_tree_done(BfAuthoringTree *t, int ok) {
    if (!t) return;
    if (ok) ft_render(t, stdout);
    else { bf_authoring_tree_clear(t); free(t); }
}
/* A one-button form carrying hidden fields. */
static void acn_button(const char *id, const char *label, const char *action, const FtField *fields, unsigned n) {
    BfAuthoringTree *t = calloc(1, sizeof(*t));
    acn_tree_done(t, t && ft_action(t, id, NULL, 0, label, "post", action, fields, n));
}
static void acn_head(const char *kicker, const char *title, const char *lede) {
    fputs("<header class=\"ak-wb-hero\"><div><span class=\"ak-kicker\">", stdout); html(kicker);
    fputs("</span><h1>", stdout); html(title);
    fputs("</h1><p class=\"ak-lede\">", stdout); html(lede); fputs("</p></div>", stdout);
}
static void acn_nav(const char *current) {
    static const struct { const char *href, *label; } tabs[] = {
        {"/account/devices", "Devices"}, {"/account/move", "Move to a new device"},
        {"/account/move/receive", "Receive on this device"}, {"/connections/bluesky/sync", "Bluesky sync"},
        {"/account/agents", "Agents and feeds"}};
    fputs("<nav class=\"ak-chips\" aria-label=\"Account devices and identities\">", stdout);
    for (size_t i = 0; i < sizeof(tabs) / sizeof(tabs[0]); i++) {
        fputs("<a class=\"ak-chip\" href=\"", stdout); fputs(tabs[i].href, stdout); fputs("\"", stdout);
        if (!strcmp(current, tabs[i].href)) fputs(" aria-current=\"page\"", stdout);
        fputs(">", stdout); fputs(tabs[i].label, stdout); fputs("</a>", stdout);
    }
    fputs("</nav>", stdout);
}

/* ------------------------------------------------------------ devices */
static int acn_fingerprint_ok(const char *s) {
    size_t n = s ? strlen(s) : 0;
    return n >= 16 && n <= 200 &&
           strspn(s, "0123456789abcdefABCDEFghijklmnopqrstuvwxyzGHIJKLMNOPQRSTUVWXYZ:+/=-_") == n;
}
static void acn_devices_page(const AcnViewer *v, const char *added) {
    sqlite3 *db = cms_open_ro();
    int active = acn_count(db, "SELECT count(*) FROM device_key WHERE namespace='root' AND account_subject=?1 AND status='published'", v->subject, NULL);
    int revoked = acn_count(db, "SELECT count(*) FROM device_key WHERE namespace='root' AND account_subject=?1 AND status='archived'", v->subject, NULL);
    ak_app_http(200, 1);
    ak_app_open("Your devices", "workbench", "route-account-devices");
    acn_head("Account · Devices", "Enroll a device key",
            "Name a device you use and the public key fingerprint it shows you. Aurekai keeps exactly what you enter so you can tell your devices apart and revoke one later.");
    printf("<dl class=\"ak-stats\"><div><dt>Enrolled</dt><dd class=\"is-on\">%d</dd><small>devices on record</small></div>"
           "<div><dt>Revoked</dt><dd>%d</dd><small>kept for your history</small></div>"
           "<div><dt>Attestation</dt><dd>None</dd><small>no hardware check is run</small></div>"
           "<div><dt>Signs you in</dt><dd>No</dd><small>sign-in still uses your password</small></div></dl></header>", active, revoked);
    acn_nav("/account/devices");
    if (added && *added) {
        fputs("<p class=\"cj-notice\" role=\"status\">", stdout); html(added); fputs("</p>", stdout);
    }
    fputs("<div class=\"ak-grid is-2\">", stdout);
    /* Left: the enrollment form. */
    fputs("<section class=\"ak-ask\"><p class=\"ak-kicker\">Enrolling device</p><h2 style=\"margin:6px 0 14px\">Name this device and its key</h2>", stdout);
    {
        static const FtOption algs[] = {{"ed25519", "Ed25519", 1}, {"p-256", "P-256", 0}, {"rsa", "RSA", 0}, {"other", "Other", 0}};
        BfAuthoringTree *t = calloc(1, sizeof(*t));
        int ok = t && ft_action(t, "dev-enroll", NULL, 0, "Enroll this device key", "post", "/account/devices", NULL, 0) &&
                 ft_field(t, "dev-label", "dev-enroll", 0, "Device name", "text", "device_label", "", "My phone", 0, NULL, "80", 1, NULL) &&
                 ft_field(t, "dev-fp", "dev-enroll", 1, "Public key fingerprint", "text", "key_fingerprint", "",
                          "SHA256:… or hex as your device shows it", 0, NULL, "200", 1, NULL) &&
                 ft_choice(t, "dev-alg", "dev-enroll", 2, "Key type", "key_algorithm", algs, 4);
        acn_tree_done(t, ok);
    }
    fputs("<p class=\"ak-empty\">Type the fingerprint yourself; never paste a private key. Aurekai cannot see inside your device, so this records your word, not a hardware attestation.</p></section>", stdout);
    /* Right: what is and is not checked. */
    fputs("<section class=\"ak-panel\"><header><h2>What this records</h2><span class=\"ak-tag\">Your word</span></header><ul class=\"ak-rows\">"
          "<li><strong>Device name</strong><small>As you typed it</small><span class=\"ak-go\">Kept</span></li>"
          "<li><strong>Public key fingerprint</strong><small>As you typed it; no key is generated here</small><span class=\"ak-go\">Kept</span></li>"
          "<li><strong>Hardware attestation</strong><small>Aurekai has no attestation verifier</small><span class=\"ak-go\">Not verified</span></li>"
          "<li><strong>Sign-in</strong><small>An enrolled key does not sign you in or replace your password</small><span class=\"ak-go\">Unchanged</span></li>"
          "</ul></section></div>", stdout);
    /* The list. */
    fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">On record</span><h2>Your devices</h2></div>"
          "<p>Revoking marks the device as no longer yours. It stays in this list so you can see when it changed.</p></header><div class=\"ak-panel\">", stdout);
    sqlite3_stmt *st = NULL; int shown = 0;
    if (db && sqlite3_prepare_v2(db, "SELECT id,device_label,key_fingerprint,key_algorithm,enrolled_at,COALESCE(revoked_at,''),status "
                                     "FROM device_key WHERE namespace='root' AND account_subject=?1 ORDER BY status='archived',id DESC LIMIT 60",
                                 -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, v->subject, -1, SQLITE_TRANSIENT);
        fputs("<ul class=\"ak-rows\">", stdout);
        while (sqlite3_step(st) == SQLITE_ROW) {
            char id[24]; snprintf(id, sizeof(id), "%lld", (long long)sqlite3_column_int64(st, 0));
            int live = !strcmp(acn_col(st, 6), "published");
            fputs("<li data-device-key><div><strong>", stdout); html(acn_col(st, 1));
            fputs("</strong> <span class=\"ak-tag", stdout); if (live) fputs(" is-on", stdout);
            fputs("\">", stdout); fputs(live ? "Enrolled" : "Revoked", stdout);
            fputs("</span><br><small>", stdout); html(acn_col(st, 3)); fputs(" · ", stdout); html(acn_col(st, 2));
            fputs("</small><br><small>Enrolled ", stdout); acn_when(acn_col(st, 4));
            if (!live) { fputs(" · revoked ", stdout); acn_when(acn_col(st, 5)); }
            fputs(" · attestation not verified</small></div>", stdout);
            if (live) { FtField f[] = {{"device", id}}; acn_button("dev-revoke", "Revoke", "/account/devices/revoke", f, 1); }
            fputs("</li>", stdout);
            shown++;
        }
        fputs("</ul>", stdout);
    }
    sqlite3_finalize(st);
    if (!shown) fputs("<p class=\"ak-empty\">No devices yet. Enroll the first one above.</p>", stdout);
    fputs("</div></section>", stdout);
    if (db) sqlite3_close(db);
    ak_app_close();
}
static int acn_devices_post(const AcnViewer *v, const char *target, const char *body) {
    char now[40]; acn_iso(time(NULL), now, sizeof(now));
    if (!strcmp(target, "/account/devices/revoke")) {
        char id[24]; sqlite3 *db = cms_open_ro(); int mine = 0;
        if (acn_field(body, "device", id, sizeof(id)) && strspn(id, "0123456789") == strlen(id) && *id)
            mine = acn_count(db, "SELECT count(*) FROM device_key WHERE namespace='root' AND id=CAST(?1 AS INTEGER) AND account_subject=?2 AND status='published'", id, v->subject);
        if (db) sqlite3_close(db);
        if (!mine) { acn_notice(404, "That device is not yours to revoke", "Choose a device enrolled on this account.", "/account/devices", "Your devices"); return 1; }
        bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w); acn_kv(&w, "revoked_at", now); acn_kv(&w, "status", "archived"); bf_jsonw_obj_end(&w);
        int ok = !w.oom && w.buf && ph_cms("update", "device_key", id, w.buf, "root", "account_subject", v->subject, "status", "published");
        bf_jsonw_free(&w);
        db = cms_open_ro();
        ok = ok && acn_count(db, "SELECT count(*) FROM device_key WHERE id=CAST(?1 AS INTEGER) AND status='archived'", id, NULL);
        if (db) sqlite3_close(db);
        if (!ok) { acn_notice(503, "Revocation not confirmed", "Reload your devices before trying again.", "/account/devices", "Your devices"); return 1; }
        acn_redirect("/account/devices?revoked=1");
        return 1;
    }
    char label[128], fp[256], alg[24], digest[65], device_id[40];
    if (!acn_field(body, "device_label", label, sizeof(label)) || !acn_line(label, 1, 80) ||
        !acn_field(body, "key_fingerprint", fp, sizeof(fp)) || !acn_fingerprint_ok(fp) ||
        !acn_field(body, "key_algorithm", alg, sizeof(alg)) ||
        (strcmp(alg, "ed25519") && strcmp(alg, "p-256") && strcmp(alg, "rsa") && strcmp(alg, "other"))) {
        acn_notice(400, "Check the device details", "Give the device a name and a fingerprint of 16 to 200 letters, digits or :+/=-_ characters, and choose its key type.", "/account/devices", "Your devices");
        return 1;
    }
    acn_digest(v->subject, fp, digest);
    snprintf(device_id, sizeof(device_id), "%.32s", digest);
    sqlite3 *db = cms_open_ro();
    int dup = acn_count(db, "SELECT count(*) FROM device_key WHERE namespace='root' AND device_id=?1 AND status='published'", device_id, NULL);
    if (db) sqlite3_close(db);
    if (dup) { acn_notice(409, "That key is already enrolled", "This account already has a device with that fingerprint.", "/account/devices", "Your devices"); return 1; }
    {
        /* A revoked key re-enrolled gets a fresh record; the old one stays. */
        char suffix[9];
        db = cms_open_ro();
        if (acn_count(db, "SELECT count(*) FROM device_key WHERE namespace='root' AND device_id=?1", device_id, NULL) && acn_hex_random(suffix, 8))
            snprintf(device_id, sizeof(device_id), "%.23s-%s", digest, suffix);
        if (db) sqlite3_close(db);
    }
    bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
    acn_kv(&w, "device_id", device_id); acn_kv(&w, "account_subject", v->subject); acn_kv(&w, "device_label", label);
    acn_kv(&w, "key_fingerprint", fp); acn_kv(&w, "key_algorithm", alg); acn_kv(&w, "attestation", "not verified");
    acn_kv(&w, "enrolled_at", now);
    acn_kv(&w, "cannot_infer", "The person typed this name and fingerprint. Aurekai did not see the key, verify any hardware, or bind it to sign-in.");
    acn_kv(&w, "status", "published"); bf_jsonw_obj_end(&w);
    acn_cms_create("device_key", &w);
    db = cms_open_ro();
    int ok = acn_count(db, "SELECT count(*) FROM device_key WHERE namespace='root' AND device_id=?1 AND account_subject=?2 AND status='published'", device_id, v->subject);
    if (db) sqlite3_close(db);
    if (!ok) { acn_notice(503, "Enrollment not confirmed", "The device was not recorded. Reload before trying again.", "/account/devices", "Your devices"); return 1; }
    acn_redirect("/account/devices?enrolled=1");
    return 1;
}

/* ------------------------------------------------------- move a device */
#define AC_PAIR_SECONDS 600
static void acn_export_manifest(sqlite3 *db, const AcnViewer *v, int downloadable) {
    PhSite *sites = calloc(16, sizeof(*sites));
    int n = (sites && v->person[0] && db) ? ph_sites(db, v->person, sites) : 0;
    fputs("<section class=\"ak-panel\"><header><h2>What the new device takes</h2><span class=\"ak-tag\">", stdout);
    printf("%d %s</span></header>", n, n == 1 ? "address" : "addresses");
    if (!n) fputs("<p class=\"ak-empty\">This account has no site address yet, so there is no export to take. Your account itself is reached by signing in; nothing about it needs copying.</p>", stdout);
    else {
        fputs("<ul class=\"ak-rows\">", stdout);
        for (int i = 0; i < n; i++) {
            int pages = acn_count(db, "SELECT count(*) FROM site_page WHERE namespace=?1", sites[i].id, NULL);
            int pub = acn_count(db, "SELECT count(*) FROM site_page WHERE namespace=?1 AND status='published'", sites[i].id, NULL);
            fputs("<li><div><strong>", stdout); html(sites[i].host);
            printf("</strong><br><small>%d pages · %d published · one JSON file with a digest per page</small></div>", pages, pub);
            if (downloadable) {
                fputs("<a class=\"ak-btn is-primary\" download href=\"/cms/compose/export?site=", stdout);
                url_query_value(sites[i].id); fputs("\">Download to this device</a>", stdout);
            } else fputs("<span class=\"ak-go\">Ready</span>", stdout);
            fputs("</li>", stdout);
        }
        fputs("</ul>", stdout);
    }
    fputs("</section>", stdout);
    free(sites);
}
static void acn_move_page(const AcnViewer *v, const char *fresh_code, const char *fresh_expiry) {
    sqlite3 *db = cms_open_ro();
    char now[40]; acn_iso(time(NULL), now, sizeof(now));
    int open = acn_count(db, "SELECT count(*) FROM device_pairing WHERE namespace='root' AND account_subject=?1 AND state='offered' AND status='published' AND expires_at>?2", v->subject, now);
    int received = acn_count(db, "SELECT count(*) FROM device_pairing WHERE namespace='root' AND account_subject=?1 AND state='received'", v->subject, NULL);
    ak_app_http(200, 1);
    ak_app_open("Move to a new device", "workbench", "route-account-move");
    acn_head("Account · Move · Sender", "Move your work to a new device",
            "Offer a one-use code here, then sign in to this same account on the new device and enter it there. The new device downloads your own site export. Nothing moves until both of you choose it.");
    printf("<dl class=\"ak-stats\"><div><dt>Open codes</dt><dd class=\"is-on\">%d</dd><small>unused, unexpired</small></div>"
           "<div><dt>Received</dt><dd>%d</dd><small>codes used by a device</small></div>"
           "<div><dt>Code lifetime</dt><dd>10 min</dd><small>then it stops working</small></div>"
           "<div><dt>Credentials copied</dt><dd>None</dd><small>sign in on the new device yourself</small></div></dl></header>", open, received);
    acn_nav("/account/move");
    fputs("<div class=\"ak-grid is-2\"><section class=\"ak-ask ak-pair\"><p class=\"ak-kicker\">One-use pairing code</p>", stdout);
    if (fresh_code && *fresh_code) {
        fputs("<p class=\"ak-pair-code\" data-pairing-code>", stdout);
        printf("%.3s<span>&ndash;</span>%.3s", fresh_code, fresh_code + 3);
        fputs("</p><p>Enter this on the new device at <b>/account/move/receive</b>, signed in to this account. It works once and stops at ", stdout);
        acn_when(fresh_expiry);
        fputs(". It is shown only now; Aurekai keeps a digest, not the code.</p>", stdout);
    } else {
        fputs("<p class=\"ak-pair-code is-idle\">&middot;&middot;&middot;<span>&ndash;</span>&middot;&middot;&middot;</p>"
              "<p>No code is showing. Offer one when the new device is in front of you.</p>", stdout);
    }
    {
        BfAuthoringTree *t = calloc(1, sizeof(*t));
        FtField f[] = {{"operation", "offer"}};
        acn_tree_done(t, t && ft_action(t, "move-offer", NULL, 0, fresh_code && *fresh_code ? "Offer a new code" : "Offer a pairing code", "post", "/account/move", f, 1));
    }
    fputs("</section>", stdout);
    acn_export_manifest(db, v, 0);
    fputs("</div>", stdout);
    fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">Ledger</span><h2>Codes you offered</h2></div>"
          "<p>Each line is a code this account offered, when, and whether a device used it.</p></header><div class=\"ak-console\">", stdout);
    sqlite3_stmt *st = NULL; int shown = 0;
    if (db && sqlite3_prepare_v2(db, "SELECT id,offered_at,expires_at,state,COALESCE(received_at,'') FROM device_pairing WHERE namespace='root' AND account_subject=?1 ORDER BY id DESC LIMIT 20",
                                 -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, v->subject, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *state = acn_col(st, 3), *exp = acn_col(st, 2);
            int expired = !strcmp(state, "offered") && strcmp(exp, now) <= 0;
            char id[24]; snprintf(id, sizeof(id), "%lld", (long long)sqlite3_column_int64(st, 0));
            fputs("<div data-pairing>[", stdout); acn_when(acn_col(st, 1)); fputs("] <b>", stdout);
            fputs(expired ? "EXPIRED" : !strcmp(state, "offered") ? "OFFERED" : !strcmp(state, "received") ? "RECEIVED" : "WITHDRAWN", stdout);
            fputs("</b> ", stdout);
            if (!strcmp(state, "received")) { fputs("used by a device signed in to this account at ", stdout); acn_when(acn_col(st, 4)); }
            else if (expired) fputs("never used", stdout);
            else if (!strcmp(state, "offered")) { fputs("open until ", stdout); acn_when(exp); }
            else fputs("withdrawn before use", stdout);
            fputs("</div>", stdout);
            if (!strcmp(state, "offered") && !expired) { FtField f[] = {{"operation", "withdraw"}, {"pairing", id}}; acn_button("move-withdraw", "Withdraw this code", "/account/move", f, 2); }
            shown++;
        }
    }
    sqlite3_finalize(st);
    if (!shown) fputs("<div>No codes offered yet.</div>", stdout);
    fputs("</div></section><p class=\"ak-empty\">Aurekai does not send anything to the other device. The new device asks for its own copy after you enter the code there, and only this account can use the code.</p>", stdout);
    if (db) sqlite3_close(db);
    ak_app_close();
}
static int acn_move_post(const AcnViewer *v, const char *body, const char *bearer) {
    char op[16], now[40];
    acn_iso(time(NULL), now, sizeof(now));
    if (!acn_field(body, "operation", op, sizeof(op))) { acn_notice(400, "Choose an action", "Offer or withdraw a code from the page.", "/account/move", "Move to a new device"); return 1; }
    if (!strcmp(op, "withdraw")) {
        char id[24]; sqlite3 *db = cms_open_ro(); int mine = 0;
        if (acn_field(body, "pairing", id, sizeof(id)) && *id && strspn(id, "0123456789") == strlen(id))
            mine = acn_count(db, "SELECT count(*) FROM device_pairing WHERE namespace='root' AND id=CAST(?1 AS INTEGER) AND account_subject=?2 AND state='offered'", id, v->subject);
        if (db) sqlite3_close(db);
        if (!mine) { acn_notice(404, "That code is not open", "It may have been used, withdrawn, or belong to another account.", "/account/move", "Move to a new device"); return 1; }
        ph_cms("update", "device_pairing", id, "{\"state\":\"withdrawn\",\"status\":\"archived\"}", "root", "account_subject", v->subject, "state", "offered");
        acn_redirect("/account/move");
        return 1;
    }
    if (strcmp(op, "offer")) { acn_notice(400, "Choose an action", "Offer or withdraw a code from the page.", "/account/move", "Move to a new device"); return 1; }
    char raw[9], code[7], digest[65], session[65], expiry[40];
    if (!acn_hex_random(raw, 8)) { acn_notice(503, "No code was made", "A random code could not be prepared. Try again.", "/account/move", "Move to a new device"); return 1; }
    snprintf(code, sizeof(code), "%06lu", strtoul(raw, NULL, 16) % 1000000UL);
    acn_digest(v->subject, code, digest);
    acn_digest("session", bearer, session);
    acn_iso(time(NULL) + AC_PAIR_SECONDS, expiry, sizeof(expiry));
    /* Older open codes for this account close when a new one is offered. */
    {
        sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL; char ids[16][24]; int n = 0;
        if (db && sqlite3_prepare_v2(db, "SELECT id FROM device_pairing WHERE namespace='root' AND account_subject=?1 AND state='offered' LIMIT 16", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, v->subject, -1, SQLITE_TRANSIENT);
            while (n < 16 && sqlite3_step(st) == SQLITE_ROW) snprintf(ids[n++], 24, "%lld", (long long)sqlite3_column_int64(st, 0));
        }
        sqlite3_finalize(st); if (db) sqlite3_close(db);
        for (int i = 0; i < n; i++) ph_cms("update", "device_pairing", ids[i], "{\"state\":\"withdrawn\",\"status\":\"archived\"}", "root", "account_subject", v->subject, "state", "offered");
    }
    bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
    char pid[40]; snprintf(pid, sizeof(pid), "%.32s", digest);
    acn_kv(&w, "pairing_id", pid); acn_kv(&w, "account_subject", v->subject); acn_kv(&w, "sender_session", session);
    acn_kv(&w, "offered_at", now); acn_kv(&w, "expires_at", expiry); acn_kv(&w, "state", "offered");
    acn_kv(&w, "cannot_infer", "A code offered by this account. Receiving it lets the same account download its own export on another device; it copies no credential and trusts no device.");
    acn_kv(&w, "status", "published"); bf_jsonw_obj_end(&w);
    acn_cms_create("device_pairing", &w);
    sqlite3 *db = cms_open_ro();
    int ok = acn_count(db, "SELECT count(*) FROM device_pairing WHERE namespace='root' AND pairing_id=?1 AND state='offered'", pid, NULL);
    if (db) sqlite3_close(db);
    memset(digest, 0, sizeof(digest));
    if (!ok) { acn_notice(503, "No code was recorded", "Offer a new code; the last one may have matched an earlier one.", "/account/move", "Move to a new device"); return 1; }
    acn_move_page(v, code, expiry);
    memset(code, 0, sizeof(code));
    return 1;
}
static void acn_receive_page(const AcnViewer *v, int accepted, const char *problem) {
    sqlite3 *db = cms_open_ro();
    ak_app_http(problem ? 400 : 200, 1);
    ak_app_open("Receive on this device", "workbench", "route-account-move-receive");
    acn_head("Account · Move · Receiver", accepted ? "Code accepted on this device" : "Receive your work on this device",
            accepted ? "The code matched an open offer from this account. Download each address below to keep a copy here. Nothing else was copied."
                     : "Enter the code your other device is showing. You must be signed in to the same account here.");
    fputs("<dl class=\"ak-stats\"><div><dt>Signed in as</dt><dd style=\"font-size:1rem\">", stdout); html(v->who.email);
    fputs("</dd><small>the account the code must belong to</small></div><div><dt>Copy</dt><dd", stdout);
    fputs(accepted ? " class=\"is-on\">Ready" : ">Waiting", stdout);
    fputs("</dd><small>", stdout); fputs(accepted ? "your choice to download" : "for a code", stdout); fputs("</small></div></dl></header>", stdout);
    acn_nav("/account/move/receive");
    if (problem) { fputs("<p class=\"cj-notice\" role=\"alert\">", stdout); html(problem); fputs("</p>", stdout); }
    if (accepted) acn_export_manifest(db, v, 1);
    else {
        fputs("<section class=\"ak-ask ak-pair\"><p class=\"ak-kicker\">Pairing code</p>", stdout);
        BfAuthoringTree *t = calloc(1, sizeof(*t));
        acn_tree_done(t, t && ft_action(t, "move-receive", NULL, 0, "Check the code", "post", "/account/move/receive", NULL, 0) &&
                            ft_field(t, "move-code", "move-receive", 0, "Six-digit code", "text", "code", "", "123456", 0, NULL, "7", 1, "numeric"));
        fputs("<p class=\"ak-empty\">A code works once, for ten minutes, for the account that offered it. Checking it does not download anything.</p></section>", stdout);
    }
    if (db) sqlite3_close(db);
    ak_app_close();
}
static int acn_receive_post(const AcnViewer *v, const char *body) {
    char raw[16], code[7], digest[65], pid[40], now[40];
    size_t n = 0;
    acn_iso(time(NULL), now, sizeof(now));
    if (!acn_field(body, "code", raw, sizeof(raw))) { acn_receive_page(v, 0, "Enter the six digits shown on your other device."); return 1; }
    for (const char *p = raw; *p && n < 6; p++) if (*p >= '0' && *p <= '9') code[n++] = *p; else if (*p != '-' && *p != ' ') { n = 0; break; }
    code[n] = 0;
    if (n != 6) { acn_receive_page(v, 0, "Enter the six digits shown on your other device."); return 1; }
    acn_digest(v->subject, code, digest);
    snprintf(pid, sizeof(pid), "%.32s", digest);
    sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL; char id[24] = "";
    if (db && sqlite3_prepare_v2(db, "SELECT id FROM device_pairing WHERE namespace='root' AND pairing_id=?1 AND account_subject=?2 AND state='offered' AND status='published' AND expires_at>?3", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, pid, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, v->subject, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 3, now, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) snprintf(id, sizeof(id), "%lld", (long long)sqlite3_column_int64(st, 0));
    }
    sqlite3_finalize(st); if (db) sqlite3_close(db);
    if (!*id) { acn_receive_page(v, 0, "That code is not open for this account. It may be mistyped, used, withdrawn, expired, or offered by another account."); return 1; }
    bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w); acn_kv(&w, "state", "received"); acn_kv(&w, "received_at", now); bf_jsonw_obj_end(&w);
    int ok = !w.oom && w.buf && ph_cms("update", "device_pairing", id, w.buf, "root", "pairing_id", pid, "state", "offered");
    bf_jsonw_free(&w);
    db = cms_open_ro();
    ok = ok && acn_count(db, "SELECT count(*) FROM device_pairing WHERE id=CAST(?1 AS INTEGER) AND state='received' AND account_subject=?2", id, v->subject);
    if (db) sqlite3_close(db);
    if (!ok) { acn_receive_page(v, 0, "The code could not be marked as used. Offer a new one."); return 1; }
    acn_receive_page(v, 1, NULL);
    return 1;
}

/* ------------------------------------------------------- bluesky sync */
static void acn_bluesky_page(const AcnViewer *v, const char *bearer, const char *auth_db) {
    SdLink links[24]; int nl = sd_links(bearer, auth_db, links, 24), bsky = 0;
    if (nl < 0) nl = 0;
    for (int i = 0; i < nl; i++) if (!strcmp(links[i].provider, "bluesky")) bsky++;
    sqlite3 *db = cms_open_ro();
    int requests = acn_count(db, "SELECT count(*) FROM carrier_sync_request WHERE namespace='root' AND account_subject=?1 AND carrier='atproto'", v->subject, NULL);
    ak_app_http(200, 1);
    ak_app_open("Bluesky repository sync", "workbench", "route-bluesky-sync");
    acn_head("Connections · Carrier · AT Protocol", "Bluesky repository sync",
            "Ask Aurekai to read the public repository behind a Bluesky handle you declared. The request is kept on record with its state. Aurekai has no AT Protocol reader installed yet, so every request stays requested and not run.");
    printf("<dl class=\"ak-stats\"><div><dt>Declared handles</dt><dd class=\"is-on\">%d</dd><small>from your connections</small></div>"
           "<div><dt>Sync requests</dt><dd>%d</dd><small>on record</small></div>"
           "<div><dt>Repositories read</dt><dd>0</dd><small>no reader has run</small></div>"
           "<div><dt>Sync state</dt><dd>Not run</dd><small>no AT Protocol reader</small></div></dl></header>", bsky, requests);
    acn_nav("/connections/bluesky/sync");
    fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">1 · Repositories</span><h2>Your declared Bluesky handles</h2></div>"
          "<p>A handle you declared is your statement, not a verified ownership claim. Add or remove handles on your connections page.</p></header><div class=\"ak-grid is-2\">", stdout);
    for (int i = 0; i < nl; i++) {
        if (strcmp(links[i].provider, "bluesky")) continue;
        char last_at[40] = "", last_state[24] = ""; int count = 0;
        sqlite3_stmt *st = NULL;
        if (db && sqlite3_prepare_v2(db, "SELECT requested_at,run_state,(SELECT count(*) FROM carrier_sync_request WHERE namespace='root' AND account_subject=?1 AND link_id=?2) FROM carrier_sync_request "
                                         "WHERE namespace='root' AND account_subject=?1 AND link_id=?2 ORDER BY id DESC LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, v->subject, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, links[i].id, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) == SQLITE_ROW) { snprintf(last_at, sizeof(last_at), "%s", acn_col(st, 0)); snprintf(last_state, sizeof(last_state), "%s", acn_col(st, 1)); count = sqlite3_column_int(st, 2); }
        }
        sqlite3_finalize(st);
        fputs("<article class=\"ak-panel\" data-bluesky-link><header><h3>", stdout); html(links[i].account);
        fputs("</h3><span class=\"ak-tag\">", stdout); html(links[i].state[0] ? links[i].state : "declared"); fputs("</span></header><dl class=\"ak-spec\">", stdout);
        fputs("<dt>Profile</dt><dd>", stdout); if (public_http_url(links[i].url)) html(links[i].url); else fputs("&mdash;", stdout);
        printf("</dd><dt>Requests</dt><dd>%d</dd><dt>Last requested</dt><dd>", count); acn_when(last_at[0] ? last_at : NULL);
        fputs("</dd><dt>State</dt><dd class=\"is-on\" data-sync-state>", stdout);
        fputs(last_state[0] ? "requested · not run" : "never requested", stdout);
        fputs("</dd></dl><div class=\"ak-actions\" style=\"margin-top:14px\">", stdout);
        FtField f[] = {{"link", links[i].id}};
        acn_button("bsky-request", "Request a repository sync", "/connections/bluesky/sync", f, 1);
        fputs("</div></article>", stdout);
    }
    if (!bsky) fputs("<p class=\"ak-empty\">You have not declared a Bluesky handle. <a href=\"/connections?provider=bluesky\">Link a handle</a> first; this page then lets you ask for its repository to be read.</p>", stdout);
    fputs("</div></section>", stdout);
    fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">2 · Requests</span><h2>Sync request ledger</h2></div>"
          "<p>Every request you made, newest first. A request is not a fetch: no repository bytes were downloaded.</p></header><div class=\"ak-console\">", stdout);
    sqlite3_stmt *st = NULL; int shown = 0;
    if (db && sqlite3_prepare_v2(db, "SELECT requested_at,remote_account,run_state FROM carrier_sync_request WHERE namespace='root' AND account_subject=?1 ORDER BY id DESC LIMIT 30", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, v->subject, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            fputs("<div data-sync-request>[", stdout); acn_when(acn_col(st, 0)); fputs("] <b>#request</b> ", stdout); html(acn_col(st, 1));
            fputs(" &rarr; ", stdout); html(acn_col(st, 2)); fputs(" · not run</div>", stdout); shown++;
        }
    }
    sqlite3_finalize(st);
    if (!shown) fputs("<div>No sync requests yet.</div>", stdout);
    fputs("</div></section>", stdout);
    if (db) sqlite3_close(db);
    ak_app_close();
}
static int acn_bluesky_post(const AcnViewer *v, const char *body, const char *bearer, const char *auth_db) {
    char link[24], now[40], rid[33];
    SdLink links[24]; int nl = sd_links(bearer, auth_db, links, 24); const SdLink *chosen = NULL;
    if (acn_field(body, "link", link, sizeof(link)))
        for (int i = 0; i < nl; i++) if (!strcmp(links[i].id, link) && !strcmp(links[i].provider, "bluesky")) chosen = &links[i];
    if (!chosen) { acn_notice(404, "That handle is not yours", "Choose one of the Bluesky handles declared on this account.", "/connections/bluesky/sync", "Bluesky sync"); return 1; }
    if (!acn_hex_random(rid, 32)) { acn_notice(503, "No request was made", "Try again.", "/connections/bluesky/sync", "Bluesky sync"); return 1; }
    acn_iso(time(NULL), now, sizeof(now));
    bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
    acn_kv(&w, "request_id", rid); acn_kv(&w, "account_subject", v->subject); acn_kv(&w, "carrier", "atproto");
    acn_kv(&w, "link_id", chosen->id); acn_kv(&w, "remote_account", chosen->account); acn_kv(&w, "requested_at", now);
    acn_kv(&w, "run_state", "requested");
    acn_kv(&w, "cannot_infer", "A request to read a declared handle's public repository. No AT Protocol reader exists, so nothing was fetched, verified or copied.");
    acn_kv(&w, "status", "published"); bf_jsonw_obj_end(&w);
    acn_cms_create("carrier_sync_request", &w);
    sqlite3 *db = cms_open_ro();
    int ok = acn_count(db, "SELECT count(*) FROM carrier_sync_request WHERE namespace='root' AND request_id=?1 AND account_subject=?2", rid, v->subject);
    if (db) sqlite3_close(db);
    if (!ok) { acn_notice(503, "Request not recorded", "Reload before trying again.", "/connections/bluesky/sync", "Bluesky sync"); return 1; }
    acn_redirect("/connections/bluesky/sync");
    return 1;
}

/* ------------------------------------------------ agents and feeds */
static const char *acn_access_label(const char *a) {
    return !strcmp(a, "read") ? "Full read" : !strcmp(a, "summary") ? "Summaries only" : !strcmp(a, "none") ? "No access" : "Unknown";
}
static void acn_agents_page(const AcnViewer *v) {
    sqlite3 *db = cms_open_ro();
    int feeds = acn_count(db, "SELECT count(DISTINCT collection) FROM saved_item WHERE actor=?1 AND status='published'", v->subject, NULL);
    int rules = acn_count(db, "SELECT count(*) FROM feed_agent_policy WHERE namespace='root' AND account_subject=?1 AND status='published'", v->subject, NULL);
    int agents = acn_count(db, "SELECT count(DISTINCT agent) FROM feed_agent_policy WHERE namespace='root' AND account_subject=?1 AND status='published'", v->subject, NULL);
    ak_app_http(200, 1);
    ak_app_open("Agents and feeds", "workbench", "route-account-agents");
    acn_head("Account · Agents · Feed access", "Agent permissions for your feeds",
            "For each feed you keep, say which agents and MCP clients may read it: everything, summaries only, or nothing. Your rules are kept as your decision on record.");
    printf("<dl class=\"ak-stats\"><div><dt>Saved feeds</dt><dd class=\"is-on\">%d</dd><small>collections you keep</small></div>"
           "<div><dt>Named agents</dt><dd>%d</dd><small>with a rule</small></div>"
           "<div><dt>Rules</dt><dd>%d</dd><small>on record</small></div>"
           "<div><dt>Enforced</dt><dd>Not yet</dd><small>no agent reads saved feeds</small></div></dl></header>", feeds, agents, rules);
    acn_nav("/account/agents");
    fputs("<div class=\"ak-grid is-main\">", stdout);
    /* 01 Agents named in your rules */
    fputs("<section class=\"ak-panel\"><header><h2>Agents</h2><span class=\"ak-tag\">named by you</span></header><ul class=\"ak-rows\">", stdout);
    sqlite3_stmt *st = NULL; int shown = 0;
    if (db && sqlite3_prepare_v2(db, "SELECT agent,count(*),sum(access='none') FROM feed_agent_policy WHERE namespace='root' AND account_subject=?1 AND status='published' GROUP BY agent ORDER BY agent LIMIT 40", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, v->subject, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            fputs("<li><strong>", stdout); html(acn_col(st, 0)); printf("</strong><small>%d feed rules · %d blocked</small></li>", sqlite3_column_int(st, 1), sqlite3_column_int(st, 2)); shown++;
        }
    }
    sqlite3_finalize(st); st = NULL;
    fputs("</ul>", stdout);
    if (!shown) fputs("<p class=\"ak-empty\">No agent named yet. Name one beside a feed.</p>", stdout);
    fputs("<p class=\"ak-empty\">A name is how you refer to a client. It does not identify who runs it.</p></section>", stdout);
    /* 02 Feed access matrix */
    fputs("<section><p class=\"ak-kicker\" style=\"margin-bottom:12px\">Feed access</p>", stdout);
    shown = 0;
    if (db && sqlite3_prepare_v2(db, "SELECT collection,count(*) FROM saved_item WHERE actor=?1 AND status='published' GROUP BY collection ORDER BY collection LIMIT 40", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, v->subject, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *feed = acn_col(st, 0); char fid[24]; snprintf(fid, sizeof(fid), "feed-%d", shown);
            fputs("<article class=\"ak-panel\" style=\"margin-bottom:14px\" data-feed><header><h3>", stdout); html(feed);
            printf("</h3><span class=\"ak-tag\">%d saved items</span></header>", sqlite3_column_int(st, 1));
            sqlite3_stmt *pr = NULL; int rules_here = 0;
            if (sqlite3_prepare_v2(db, "SELECT agent,access,decided_at FROM feed_agent_policy WHERE namespace='root' AND account_subject=?1 AND feed=?2 AND status='published' ORDER BY agent", -1, &pr, NULL) == SQLITE_OK) {
                sqlite3_bind_text(pr, 1, v->subject, -1, SQLITE_TRANSIENT); sqlite3_bind_text(pr, 2, feed, -1, SQLITE_TRANSIENT);
                fputs("<ul class=\"ak-rows\">", stdout);
                while (sqlite3_step(pr) == SQLITE_ROW) {
                    fputs("<li data-feed-rule><strong>", stdout); html(acn_col(pr, 0)); fputs("</strong><small>decided ", stdout); acn_when(acn_col(pr, 2));
                    fputs("</small><span class=\"ak-go\">", stdout); fputs(acn_access_label(acn_col(pr, 1)), stdout); fputs("</span></li>", stdout); rules_here++;
                }
                fputs("</ul>", stdout);
            }
            sqlite3_finalize(pr);
            if (!rules_here) fputs("<p class=\"ak-empty\">No agent has a rule for this feed.</p>", stdout);
            static const FtOption acc[] = {{"read", "Full read", 0}, {"summary", "Summaries only", 1}, {"none", "No access", 0}};
            BfAuthoringTree *t = calloc(1, sizeof(*t));
            FtField hidden[] = {{"feed", feed}};
            char aid[32], cid[32], fld[32]; snprintf(aid, sizeof(aid), "%s-act", fid); snprintf(cid, sizeof(cid), "%s-acc", fid); snprintf(fld, sizeof(fld), "%s-agent", fid);
            acn_tree_done(t, t && ft_action(t, aid, NULL, 0, "Save rule", "post", "/account/agents", hidden, 1) &&
                                ft_field(t, fld, aid, 0, "Agent or MCP client", "text", "agent", "", "Claude Desktop", 0, NULL, "80", 1, NULL) &&
                                ft_choice(t, cid, aid, 1, "Access", "access", acc, 3));
            fputs("</article>", stdout);
            shown++;
        }
    }
    sqlite3_finalize(st); st = NULL;
    if (!shown) fputs("<div class=\"ak-panel\"><p class=\"ak-empty\">You have no saved feeds yet. Save things from <a href=\"/discover\">Discover</a> and each collection appears here with its own rules.</p></div>", stdout);
    fputs("</section>", stdout);
    /* 03 Audit */
    fputs("<section class=\"ak-panel\"><header><h2>Audit</h2><span class=\"ak-tag\">honest state</span></header>"
          "<dl class=\"ak-stats\"><div><dt>Agent reads</dt><dd>0</dd><small>none served</small></div><div><dt>Refused</dt><dd>0</dd><small>none asked</small></div></dl>"
          "<div class=\"ak-console\" style=\"margin-top:14px\"><div><b>enforcement</b> No MCP tool or /connect path serves saved feeds today, so no agent can read them and none has.</div>"
          "<div><b>when one does</b> it must consult these rules first; a missing rule means no access.</div></div>"
          "<p class=\"ak-empty\">Your public site posts are a different feed: they stay public to every reader and agent.</p></section></div>", stdout);
    if (db) sqlite3_close(db);
    ak_app_close();
}
static int acn_agents_post(const AcnViewer *v, const char *body) {
    char feed[200], agent[128], access[16], now[40], digest[65], pid[40];
    if (!acn_field(body, "feed", feed, sizeof(feed)) || !acn_line(feed, 1, 120) ||
        !acn_field(body, "agent", agent, sizeof(agent)) || !acn_line(agent, 1, 80) ||
        !acn_field(body, "access", access, sizeof(access)) || (strcmp(access, "read") && strcmp(access, "summary") && strcmp(access, "none"))) {
        acn_notice(400, "Check the rule", "Name an agent in up to 80 characters and choose its access.", "/account/agents", "Agents and feeds");
        return 1;
    }
    sqlite3 *db = cms_open_ro();
    int owned = acn_count(db, "SELECT count(*) FROM saved_item WHERE actor=?1 AND collection=?2 AND status='published'", v->subject, feed);
    char key[400]; snprintf(key, sizeof(key), "%s\n%s", feed, agent);
    acn_digest(v->subject, key, digest); snprintf(pid, sizeof(pid), "%.32s", digest);
    sqlite3_stmt *st = NULL; char id[24] = "";
    if (db && sqlite3_prepare_v2(db, "SELECT id FROM feed_agent_policy WHERE namespace='root' AND policy_id=?1 AND account_subject=?2", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, pid, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, v->subject, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) snprintf(id, sizeof(id), "%lld", (long long)sqlite3_column_int64(st, 0));
    }
    sqlite3_finalize(st); if (db) sqlite3_close(db);
    if (!owned) { acn_notice(404, "That feed is not yours", "Choose one of the feeds you keep.", "/account/agents", "Agents and feeds"); return 1; }
    acn_iso(time(NULL), now, sizeof(now));
    bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
    if (!*id) { acn_kv(&w, "policy_id", pid); acn_kv(&w, "account_subject", v->subject); acn_kv(&w, "feed", feed); acn_kv(&w, "agent", agent); }
    acn_kv(&w, "access", access); acn_kv(&w, "decided_at", now);
    acn_kv(&w, "enforcement", "Not enforced: no MCP or /connect read path serves saved feeds. A future read path must consult this rule; no rule means no access.");
    acn_kv(&w, "cannot_infer", "The person's stated rule for one feed and one named client. It does not identify the client's operator or grant access to anything the person does not share.");
    acn_kv(&w, "status", "published"); bf_jsonw_obj_end(&w);
    if (*id) { if (!w.oom && w.buf) ph_cms("update", "feed_agent_policy", id, w.buf, "root", "account_subject", v->subject, "policy_id", pid); bf_jsonw_free(&w); }
    else acn_cms_create("feed_agent_policy", &w);
    db = cms_open_ro();
    sqlite3_stmt *chk = NULL; int ok = 0;
    if (db && sqlite3_prepare_v2(db, "SELECT 1 FROM feed_agent_policy WHERE namespace='root' AND policy_id=?1 AND account_subject=?2 AND access=?3 AND decided_at=?4", -1, &chk, NULL) == SQLITE_OK) {
        sqlite3_bind_text(chk, 1, pid, -1, SQLITE_TRANSIENT); sqlite3_bind_text(chk, 2, v->subject, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(chk, 3, access, -1, SQLITE_TRANSIENT); sqlite3_bind_text(chk, 4, now, -1, SQLITE_TRANSIENT);
        ok = sqlite3_step(chk) == SQLITE_ROW;
    }
    sqlite3_finalize(chk); if (db) sqlite3_close(db);
    if (!ok) { acn_notice(503, "Rule not confirmed", "Reload before trying again.", "/account/agents", "Agents and feeds"); return 1; }
    acn_redirect("/account/agents");
    return 1;
}

static int serve_account_continuity(const char *method, const char *target, const char *query, const char *body,
                                    const char *raw, const char *host, const char *bearer, const char *auth_db) {
    int devices = !strcmp(target, "/account/devices") || !strcmp(target, "/account/devices/revoke");
    int move = !strcmp(target, "/account/move"), receive = !strcmp(target, "/account/move/receive");
    int bluesky = !strcmp(target, "/connections/bluesky/sync"), agents = !strcmp(target, "/account/agents");
    if (!devices && !move && !receive && !bluesky && !agents) return 0;
    int post = !strcmp(method, "POST");
    if (!post && strcmp(method, "GET")) return 0;
    AcnViewer v;
    if (!acn_viewer(bearer, auth_db, &v)) {
        if (post) { acn_notice(401, "Sign in first", "These records belong to a signed-in account. Nothing was recorded.", "/login", "Sign in"); return 1; }
        acn_signin(target);
        return 1;
    }
    if (post && !pj_same_origin(raw, host)) {
        acn_notice(403, "Change refused", "Submit the form from this exact secure page. Nothing was recorded.", target, "Go back");
        return 1;
    }
    if (devices) {
        if (post) return acn_devices_post(&v, target, body);
        if (!strcmp(target, "/account/devices/revoke")) { acn_redirect("/account/devices"); return 1; }
        char flag[8] = "";
        const char *said = (query && bf_lex_form_query(query, "enrolled", flag, sizeof(flag)) == 1) ? "Device enrolled. Its attestation is recorded as not verified." :
                           (query && bf_lex_form_query(query, "revoked", flag, sizeof(flag)) == 1) ? "Device revoked." : NULL;
        acn_devices_page(&v, said);
        return 1;
    }
    if (move) { if (post) return acn_move_post(&v, body, bearer); acn_move_page(&v, NULL, NULL); return 1; }
    if (receive) { if (post) return acn_receive_post(&v, body); acn_receive_page(&v, 0, NULL); return 1; }
    if (bluesky) { if (post) return acn_bluesky_post(&v, body, bearer, auth_db); acn_bluesky_page(&v, bearer, auth_db); return 1; }
    if (post) return acn_agents_post(&v, body);
    acn_agents_page(&v);
    return 1;
}
#endif
