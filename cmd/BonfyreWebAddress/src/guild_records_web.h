#ifndef BONFYRE_GUILD_RECORDS_WEB_H
#define BONFYRE_GUILD_RECORDS_WEB_H
/* A guild's own records, bound to its community_body and community_membership
 * rows (community_journey.h owns both). The guild steward is the body's
 * creator or an active member whose role is steward; membership decides who
 * reads and writes. Every change is one person's explicit form submit,
 * written through the CMS entry owner. Nothing here holds funds, pays, signs
 * outside the guild or transmits a message: hours, sign-offs, attestations
 * and offers are the guild's written word, and the pages say so. */

typedef struct {
    char op[24], request[33], module[97], target[97], member[24], title[161], text[4001],
         hours[8], date[11], url[401], level[8], value[81], kind[16], place[121], closes[11];
} GrForm;
typedef struct {
    const char *label, *type, *name, *value, *placeholder;
    int multiline;
    const char *max;
    int required;
    const char *mode;
} GrIn;

static void gr_now(char out[32]) {
    time_t t = time(NULL); struct tm tm; gmtime_r(&t, &tm);
    strftime(out, 32, "%Y-%m-%dT%H:%M:%SZ", &tm);
}
static void gr_today(char out[11]) {
    time_t t = time(NULL); struct tm tm; gmtime_r(&t, &tm);
    strftime(out, 11, "%Y-%m-%d", &tm);
}
static int gr_date_ok(const char *s) {
    return s && strlen(s) == 10 && s[4] == '-' && s[7] == '-' &&
           strspn(s, "0123456789-") == 10;
}
static int gr_hours_ok(const char *s) {
    if (!s || !*s || strspn(s, "0123456789.") != strlen(s)) return 0;
    double h = strtod(s, NULL);
    return h > 0 && h <= 24;
}
static int gr_url_ok(const char *s) {
    if (!s || (strncmp(s, "https://", 8) && strncmp(s, "http://", 7)) || strlen(s) > 400) return 0;
    for (const unsigned char *p = (const unsigned char *)s; *p; p++)
        if (*p <= 32 || *p == '"' || *p == '<' || *p == '>' || *p == '\\' || *p == '`') return 0;
    return 1;
}
static int gr_words(const char *s) {
    if (!s) return 0;
    while (*s == ' ' || *s == '\n' || *s == '\r' || *s == '\t') s++;
    return *s != 0;
}
/* Steward: the body's creator, or an active member whose role is steward. */
static int gr_steward(sqlite3 *db, const CjrCommunity *c, const char *actor) {
    CjrMember m;
    if (!actor || !*actor) return 0;
    if (!strcmp(c->owner, actor)) return 1;
    return cjr_member(db, c->key, actor, &m) == 1 && m.active && !strcmp(m.role, "steward");
}
static int gr_active(sqlite3 *db, const CjrCommunity *c, const char *actor) {
    CjrMember m;
    if (!actor || !*actor) return 0;
    if (!strcmp(c->owner, actor)) return 1;
    return cjr_member(db, c->key, actor, &m) == 1 && m.active;
}
static void gr_person(const char *actor, const char *viewer, const char *owner) {
    if (actor && cjr_actor(actor)) cjw_name(actor, viewer, owner);
    else fputs("Someone", stdout);
}
/* One JSON object from key/value pairs, through bf_jsonw. */
static char *gr_json(const char *const *kv, size_t pairs) {
    bf_jsonw w; char *out = NULL;
    bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
    for (size_t i = 0; i < pairs; i++) {
        if (!kv[i * 2 + 1]) continue;
        bf_jsonw_key(&w, kv[i * 2]); bf_jsonw_str(&w, kv[i * 2 + 1]);
    }
    bf_jsonw_obj_end(&w);
    if (!w.oom && w.buf) out = strdup(w.buf);
    bf_jsonw_free(&w);
    return out;
}
static int gr_create(const char *type, const char *ns, const char *const *kv, size_t pairs) {
    char *json = gr_json(kv, pairs);
    int ok = json && ph_cms("create", type, NULL, json, ns, NULL, NULL, NULL, NULL);
    free(json); return ok;
}
static int gr_update(const char *type, const char *ns, long long id, const char *guard_field,
                     const char *guard_value, const char *const *kv, size_t pairs) {
    char idtext[24], *json = gr_json(kv, pairs);
    snprintf(idtext, sizeof(idtext), "%lld", id);
    int ok = json && ph_cms("update", type, idtext, json, ns, guard_field, guard_value, NULL, NULL);
    free(json); return ok;
}
/* Row id and two columns of a keyed record inside one community. */
static long long gr_row(sqlite3 *db, const char *sql, const char *a, const char *b,
                        char *c1, size_t n1, char *c2, size_t n2, char *c3, size_t n3) {
    sqlite3_stmt *s = NULL; long long id = 0;
    if (c1) *c1 = 0; if (c2) *c2 = 0; if (c3) *c3 = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &s, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(s, 1, a, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(s, 2, b, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(s) == SQLITE_ROW) {
        id = sqlite3_column_int64(s, 0);
        const char *v;
        if (c1 && (v = (const char *)sqlite3_column_text(s, 1))) snprintf(c1, n1, "%s", v);
        if (c2 && (v = (const char *)sqlite3_column_text(s, 2))) snprintf(c2, n2, "%s", v);
        if (c3 && (v = (const char *)sqlite3_column_text(s, 3))) snprintf(c3, n3, "%s", v);
    }
    sqlite3_finalize(s); return id;
}
static double gr_number(sqlite3 *db, const char *sql, const char *a, const char *b) {
    sqlite3_stmt *s = NULL; double v = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &s, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(s, 1, a ? a : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(s, 2, b ? b : "", -1, SQLITE_TRANSIENT);
    if (sqlite3_step(s) == SQLITE_ROW) v = sqlite3_column_double(s, 0);
    sqlite3_finalize(s); return v;
}
static int gr_table(sqlite3 *db, const char *name) {
    sqlite3_stmt *s = NULL; int found = 0;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, name, -1, SQLITE_STATIC);
        found = sqlite3_step(s) == SQLITE_ROW;
    }
    sqlite3_finalize(s); return found;
}
/* One form through the shared ui:action/ui:field/ui:choice renderer. */
static void gr_form(const char *id, const char *button, const char *action,
                    const FtField *hidden, unsigned nh, const GrIn *in, unsigned ni,
                    const char *choice_label, const char *choice_name,
                    const FtOption *options, unsigned nopt) {
    BfAuthoringTree *tree = calloc(1, sizeof(*tree));
    char child[64]; int ok, pos = 0;
    if (!tree) return;
    ok = ft_action(tree, id, NULL, 0, button, "post", action, hidden, nh);
    if (ok && choice_name && nopt) {
        snprintf(child, sizeof(child), "%s-choice", id);
        ok = ft_choice(tree, child, id, pos++, choice_label, choice_name, options, nopt);
    }
    for (unsigned i = 0; ok && i < ni; i++) {
        snprintf(child, sizeof(child), "%s-%u", id, i);
        ok = ft_field(tree, child, id, pos++, in[i].label, in[i].type, in[i].name, in[i].value,
                      in[i].placeholder, in[i].multiline, in[i].multiline ? "4" : "",
                      in[i].max, in[i].required, in[i].mode);
    }
    if (ok) ft_render(tree, stdout);
    else { bf_authoring_tree_clear(tree); free(tree); }
}
static void gr_button(const char *id, const char *label, const char *action,
                      const FtField *hidden, unsigned nh) {
    gr_form(id, label, action, hidden, nh, NULL, 0, NULL, NULL, NULL, 0);
}
static void gr_open(const char *title, const CjrCommunity *c, const char *here, int personal) {
    ak_app_http(200, personal);
    ak_app_open(title, "workbench", "route-guild");
    fputs("<nav class=\"ak-wb-strip\" aria-label=\"Guild\"><span class=\"ak-kicker\">Guild</span><b>", stdout);
    html(c->title);
    fputs("</b>", stdout);
    static const char *const keys[] = {"", "bench", "attest", "offers", "registry"};
    static const char *const names[] = {"Curriculum", "My bench", "Attestations", "Offers", "Field registry"};
    for (int i = 0; i < 5; i++) {
        fputs("<a href=\"/guild/", stdout); html(c->key); fputc('/', stdout); fputs(keys[i], stdout);
        fputc('"', stdout);
        if (!strcmp(here, keys[i])) fputs(" aria-current=\"page\"", stdout);
        fputs(">", stdout); fputs(names[i], stdout); fputs("</a>", stdout);
    }
    fputs("<a href=\"/charters?community=", stdout); url_query_value(c->key);
    fputs("\">Charters</a><a href=\"/community?community=", stdout); url_query_value(c->key);
    fputs("\">Conversations</a></nav>", stdout);
}
static void gr_module_options(sqlite3 *db, const char *community, FtOption *opt, char (*vals)[97],
                              char (*labels)[200], unsigned *n, unsigned cap) {
    sqlite3_stmt *s = NULL; *n = 0;
    if (sqlite3_prepare_v2(db, "SELECT module,level,title FROM guild_module WHERE namespace='root' AND community=?1 AND status='published' ORDER BY CAST(level AS INTEGER),id LIMIT 24", -1, &s, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(s, 1, community, -1, SQLITE_TRANSIENT);
    while (*n < cap && sqlite3_step(s) == SQLITE_ROW) {
        snprintf(vals[*n], 97, "%s", (const char *)sqlite3_column_text(s, 0));
        snprintf(labels[*n], 200, "Level %s · %s", (const char *)sqlite3_column_text(s, 1),
                 (const char *)sqlite3_column_text(s, 2));
        opt[*n].value = vals[*n]; opt[*n].label = labels[*n]; opt[*n].checked = *n == 0;
        (*n)++;
    }
    sqlite3_finalize(s);
}
static void gr_hidden_base(FtField *f, const char *op, const char *nonce) {
    f[0].name = "op"; f[0].value = op; f[1].name = "request"; f[1].value = nonce;
}
static void gr_act_path(const CjrCommunity *c, char out[160]) {
    snprintf(out, 160, "/guild/%s/act", c->key);
}

/* ---------------------------------------------------------------- pages */

static void gr_curriculum(sqlite3 *db, const CjrCommunity *c, const char *actor) {
    int steward = gr_steward(db, c, actor), member = gr_active(db, c, actor);
    char act[160], nonce[33]; gr_act_path(c, act);
    gr_open(c->title, c, "", *actor != 0);
    fputs("<header class=\"ak-wb-hero\"><div><span class=\"ak-kicker\">Guild curriculum · shop hours · bench proofs</span><h1>", stdout);
    html(c->title); fputs("</h1><p class=\"ak-lede\">", stdout); html(c->description);
    fputs("</p><div class=\"ak-actions\">", stdout);
    if (member) { fputs("<a class=\"ak-btn is-primary\" href=\"/guild/", stdout); html(c->key); fputs("/bench\">Open my bench</a>", stdout); }
    else if (*actor) { fputs("<a class=\"ak-btn is-primary\" href=\"/community?community=", stdout); url_query_value(c->key); fputs("&amp;tab=join\">Ask to join this guild</a>", stdout); }
    else { fputs("<a class=\"ak-btn is-primary\" href=\"/login?next=%2Fguild%2F", stdout); url_query_value(c->key); fputs("%2F\">Sign in to take part</a>", stdout); }
    if (steward) { fputs("<a class=\"ak-btn\" href=\"/guild/", stdout); html(c->key); fputs("/attest\">Attest a skill</a>", stdout); }
    fputs("</div></div><aside class=\"ak-panel\"><span class=\"ak-kicker\">Guild steward</span><h3>", stdout);
    gr_person(c->owner, actor, NULL);
    fputs("</h3><p>Apprentices log their own hours and link their bench work. A named steward confirms the hours and writes each sign-off. Nothing here pays, holds money or certifies anyone outside this guild.</p></aside></header>", stdout);

    double apprentices = gr_number(db, "SELECT count(DISTINCT apprentice_subject) FROM guild_hours WHERE namespace='root' AND community=?1 AND status='published'", c->key, NULL);
    double confirmed = gr_number(db, "SELECT COALESCE(sum(CAST(hours AS REAL)),0) FROM guild_hours WHERE namespace='root' AND community=?1 AND state='confirmed' AND status='published'", c->key, NULL);
    double waiting = gr_number(db, "SELECT (SELECT count(*) FROM guild_hours WHERE namespace='root' AND community=?1 AND state='submitted' AND status='published')+(SELECT count(*) FROM guild_bench_proof WHERE namespace='root' AND community=?1 AND state='submitted' AND status='published')", c->key, NULL);
    double attested = gr_number(db, "SELECT count(*) FROM guild_attestation WHERE namespace='root' AND community=?1 AND state='attested' AND status='published'", c->key, NULL);
    printf("<dl class=\"ak-stats is-row\"><div><dt>Apprentices logging</dt><dd>%.0f</dd><small>people with shop hours here</small></div>"
           "<div><dt>Confirmed shop hours</dt><dd class=\"is-on\">%.1f</dd><small>confirmed by a steward</small></div>"
           "<div><dt>Awaiting sign-off</dt><dd>%.0f</dd><small>hours and bench proofs</small></div>"
           "<div><dt>Skills attested</dt><dd>%.0f</dd><small>current, revocable</small></div></dl>",
           apprentices, confirmed, waiting, attested);

    double required = gr_number(db, "SELECT COALESCE(sum(CAST(required_hours AS REAL)),0) FROM guild_module WHERE namespace='root' AND community=?1 AND status='published'", c->key, NULL);
    fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">Guild cohort</span><h2>Apprentices and their hours</h2></div></header>", stdout);
    sqlite3_stmt *s = NULL; int shown = 0;
    if (sqlite3_prepare_v2(db,
        "SELECT h.apprentice_subject,COALESCE(sum(CASE WHEN h.state='confirmed' THEN CAST(h.hours AS REAL) END),0),"
        " (SELECT m.title FROM guild_hours x JOIN guild_module m ON m.namespace='root' AND m.community=x.community AND m.module=x.module WHERE x.namespace='root' AND x.community=?1 AND x.apprentice_subject=h.apprentice_subject ORDER BY x.id DESC LIMIT 1),"
        " (SELECT p.title||' · '||p.state FROM guild_bench_proof p WHERE p.namespace='root' AND p.community=?1 AND p.apprentice_subject=h.apprentice_subject ORDER BY p.id DESC LIMIT 1)"
        " FROM guild_hours h WHERE h.namespace='root' AND h.community=?1 AND h.status='published' GROUP BY h.apprentice_subject ORDER BY 2 DESC LIMIT 12", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, c->key, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            const char *who = (const char *)sqlite3_column_text(s, 0), *mod = (const char *)sqlite3_column_text(s, 2),
                       *proof = (const char *)sqlite3_column_text(s, 3);
            double h = sqlite3_column_double(s, 1);
            if (!shown++) fputs("<div class=\"ak-grid is-3\">", stdout);
            fputs("<article class=\"ak-panel\"><span class=\"ak-tag is-on\">Apprentice</span><h3>", stdout);
            gr_person(who, actor, c->owner);
            fputs("</h3>", stdout);
            if (required > 0) printf("<p>%.1f / %.0f confirmed hours</p><div class=\"ak-meter\"><i style=\"width:%.0f%%\"></i></div>", h, required, h >= required ? 100.0 : h * 100.0 / required);
            else printf("<p>%.1f confirmed hours</p>", h);
            if (mod) { fputs("<p><span class=\"ak-kicker\">Working on</span><br>", stdout); html(mod); fputs("</p>", stdout); }
            if (proof) { fputs("<p><span class=\"ak-kicker\">Latest bench proof</span><br>", stdout); html(proof); fputs("</p>", stdout); }
            fputs("</article>", stdout);
        }
    }
    sqlite3_finalize(s);
    if (shown) fputs("</div>", stdout);
    else fputs("<p class=\"ak-empty\">No apprentice has logged shop hours in this guild yet.</p>", stdout);
    fputs("</section>", stdout);

    fputs("<section class=\"ak-sec\" id=\"queue\"><header><div><span class=\"ak-kicker\">Steward's desk</span><h2>Shop hour and bench sign-off queue</h2></div><p>Each item waits for a steward who is not the apprentice. Confirming is a written statement inside this guild only.</p></header><ul class=\"ak-rows\">", stdout);
    shown = 0;
    if (sqlite3_prepare_v2(db, "SELECT h.entry,h.apprentice_subject,h.hours,h.activity,h.worked_on,COALESCE(m.title,'') FROM guild_hours h LEFT JOIN guild_module m ON m.namespace='root' AND m.community=h.community AND m.module=h.module WHERE h.namespace='root' AND h.community=?1 AND h.state='submitted' AND h.status='published' ORDER BY h.id LIMIT 30", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, c->key, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            const char *key = (const char *)sqlite3_column_text(s, 0), *who = (const char *)sqlite3_column_text(s, 1);
            shown++;
            fputs("<li><strong>", stdout); gr_person(who, actor, c->owner); fputs(" · ", stdout);
            html((const char *)sqlite3_column_text(s, 2)); fputs(" shop hours · ", stdout);
            html((const char *)sqlite3_column_text(s, 3)); fputs("</strong><small>", stdout);
            html((const char *)sqlite3_column_text(s, 5)); fputs(" · worked ", stdout);
            html((const char *)sqlite3_column_text(s, 4)); fputs("</small>", stdout);
            if (steward && strcmp(who, actor)) {
                ph_nonce(nonce);
                FtField f[3] = {{0}}; gr_hidden_base(f, "hours-confirm", nonce); f[2].name = "target"; f[2].value = key;
                char id[64]; snprintf(id, sizeof(id), "gr-hc-%d", shown);
                fputs("<span class=\"ak-go\">", stdout); gr_button(id, "Confirm hours", act, f, 3);
                f[0].value = "hours-decline"; snprintf(id, sizeof(id), "gr-hd-%d", shown);
                gr_button(id, "Decline", act, f, 3); fputs("</span>", stdout);
            } else fputs("<span class=\"ak-go\">Awaiting a steward</span>", stdout);
            fputs("</li>", stdout);
        }
    }
    sqlite3_finalize(s);
    if (sqlite3_prepare_v2(db, "SELECT proof,apprentice_subject,title,submitted_at FROM guild_bench_proof WHERE namespace='root' AND community=?1 AND state='submitted' AND status='published' ORDER BY id LIMIT 30", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, c->key, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            shown++;
            fputs("<li><strong>", stdout); gr_person((const char *)sqlite3_column_text(s, 1), actor, c->owner);
            fputs(" · bench proof · ", stdout); html((const char *)sqlite3_column_text(s, 2));
            fputs("</strong><small>Submitted ", stdout); html((const char *)sqlite3_column_text(s, 3));
            fputs("</small><a class=\"ak-go\" href=\"/guild/", stdout); html(c->key); fputs("/proof/", stdout);
            html((const char *)sqlite3_column_text(s, 0)); fputs("\">Review bench proof</a></li>", stdout);
        }
    }
    sqlite3_finalize(s);
    fputs("</ul>", stdout);
    if (!shown) fputs("<p class=\"ak-empty\">Nothing is waiting for a steward.</p>", stdout);
    fputs("</section>", stdout);

    fputs("<section class=\"ak-sec\" id=\"syllabus\"><header><div><span class=\"ak-kicker\">Curriculum</span><h2>Living syllabus and guild benchmarks</h2></div><p>The stages this guild's steward has written. Each names the shop hours the guild asks for before its bench proof.</p></header>", stdout);
    shown = 0;
    if (sqlite3_prepare_v2(db, "SELECT module,level,title,description,COALESCE(required_hours,''),"
        " (SELECT COALESCE(sum(CAST(hours AS REAL)),0) FROM guild_hours h WHERE h.namespace='root' AND h.community=m.community AND h.module=m.module AND h.apprentice_subject=?2 AND h.state='confirmed'),"
        " (SELECT count(*) FROM guild_bench_proof p WHERE p.namespace='root' AND p.community=m.community AND p.module=m.module AND p.apprentice_subject=?2 AND p.state='signed')"
        " FROM guild_module m WHERE namespace='root' AND community=?1 AND status='published' ORDER BY CAST(level AS INTEGER),id LIMIT 24", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, c->key, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(s, 2, actor, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            const char *req = (const char *)sqlite3_column_text(s, 4);
            double mine = sqlite3_column_double(s, 5), need = req && *req ? strtod(req, NULL) : 0;
            int signed_off = sqlite3_column_int(s, 6);
            if (!shown++) fputs("<div class=\"ak-grid\">", stdout);
            fputs("<article class=\"ak-panel\"><header><span class=\"ak-kicker\">Level ", stdout);
            html((const char *)sqlite3_column_text(s, 1)); fputs("</span>", stdout);
            if (signed_off) fputs("<span class=\"ak-tag is-on\">Signed off</span>", stdout);
            fputs("</header><h3>", stdout); html((const char *)sqlite3_column_text(s, 2));
            fputs("</h3><p>", stdout); html((const char *)sqlite3_column_text(s, 3)); fputs("</p>", stdout);
            if (need > 0) printf("<p><small>Minimum %.0f shop hours</small></p>", need);
            if (member && need > 0) printf("<div class=\"ak-meter\"><i style=\"width:%.0f%%\"></i></div><p><small>You: %.1f confirmed</small></p>", mine >= need ? 100.0 : mine * 100.0 / need, mine);
            fputs("</article>", stdout);
        }
    }
    sqlite3_finalize(s);
    if (shown) fputs("</div>", stdout);
    else fputs("<p class=\"ak-empty\">The steward has not written a curriculum module yet.</p>", stdout);
    fputs("</section>", stdout);

    if (steward) {
        ph_nonce(nonce);
        FtField f[2]; gr_hidden_base(f, "module-add", nonce);
        GrIn in[] = {
            {"Level", "text", "level", "", "1", 0, "4", 1, "numeric"},
            {"Module title", "text", "title", "", "", 0, "160", 1, ""},
            {"What is practised", "text", "text", "", "", 1, "4000", 1, ""},
            {"Minimum shop hours", "text", "hours", "", "", 0, "6", 0, "decimal"},
        };
        fputs("<section class=\"ak-sec\"><div class=\"ak-ask\"><span class=\"ak-kicker\">Guild registrar</span><h2>Write a curriculum module</h2><p>Only this guild's steward can add a module. It appears to everyone who opens the guild.</p>", stdout);
        gr_form("gr-module", "Add module", act, f, 2, in, 4, NULL, NULL, NULL, 0);
        fputs("</div></section>", stdout);
    } else {
        fputs("<section class=\"ak-band\"><div><span class=\"ak-kicker\">Guild registrar</span><h2>Take up a bench in this guild</h2><p>Members log their own shop hours and link bench work for a steward's written sign-off.</p></div><div class=\"ak-actions\">", stdout);
        if (member) { fputs("<a class=\"ak-btn\" href=\"/guild/", stdout); html(c->key); fputs("/bench\">Open my bench</a>", stdout); }
        else { fputs("<a class=\"ak-btn\" href=\"/community?community=", stdout); url_query_value(c->key); fputs("&amp;tab=join\">Ask to join</a>", stdout); }
        fputs("<a class=\"ak-btn is-quiet\" href=\"/guild/", stdout); html(c->key); fputs("/offers\">See this guild's offers</a></div></section>", stdout);
    }
    ak_app_close();
}

static void gr_bench(sqlite3 *db, const CjrCommunity *c, const char *actor) {
    char act[160], nonce[33]; gr_act_path(c, act);
    gr_open("My bench", c, "bench", 1);
    double required = gr_number(db, "SELECT COALESCE(sum(CAST(required_hours AS REAL)),0) FROM guild_module WHERE namespace='root' AND community=?1 AND status='published'", c->key, NULL);
    double confirmed = gr_number(db, "SELECT COALESCE(sum(CAST(hours AS REAL)),0) FROM guild_hours WHERE namespace='root' AND community=?1 AND apprentice_subject=?2 AND state='confirmed' AND status='published'", c->key, actor);
    double pending = gr_number(db, "SELECT COALESCE(sum(CAST(hours AS REAL)),0) FROM guild_hours WHERE namespace='root' AND community=?1 AND apprentice_subject=?2 AND state='submitted' AND status='published'", c->key, actor);
    double proofs = gr_number(db, "SELECT count(*) FROM guild_bench_proof WHERE namespace='root' AND community=?1 AND apprentice_subject=?2 AND state='submitted' AND status='published'", c->key, actor);
    double skills = gr_number(db, "SELECT count(*) FROM guild_attestation WHERE namespace='root' AND community=?1 AND member_subject=?2 AND state='attested' AND status='published'", c->key, actor);
    fputs("<header class=\"ak-wb-hero\"><div><span class=\"ak-kicker\">Apprentice bench</span><h1>Your bench in ", stdout); html(c->title);
    fputs("</h1><p class=\"ak-lede\">Log the hours you worked and link the bench work you want reviewed. A steward who is not you confirms each entry in writing.</p></div><aside class=\"ak-panel\"><span class=\"ak-kicker\">Supervising steward</span><h3>", stdout);
    gr_person(c->owner, actor, NULL);
    fputs("</h3><p>No allowance, escrow or payment is held or released on this page. Recording hours or a sign-off moves no money.</p></aside></header>", stdout);
    printf("<dl class=\"ak-stats is-row\"><div><dt>Confirmed shop hours</dt><dd class=\"is-on\">%.1f</dd>", confirmed);
    if (required > 0) printf("<small>of %.0f in the curriculum</small><div class=\"ak-meter\"><i style=\"width:%.0f%%\"></i></div>", required, confirmed >= required ? 100.0 : confirmed * 100.0 / required);
    printf("</div><div><dt>Hours awaiting a steward</dt><dd>%.1f</dd></div><div><dt>Bench proofs awaiting sign-off</dt><dd>%.0f</dd></div><div><dt>Skills attested to you</dt><dd>%.0f</dd></div></dl>", pending, proofs, skills);

    FtOption opt[24]; char vals[24][97], labels[24][200]; unsigned nmod = 0;
    gr_module_options(db, c->key, opt, vals, labels, &nmod, 24);

    fputs("<div class=\"ak-grid is-main\" style=\"grid-template-columns:minmax(0,1.6fr) minmax(0,1fr)\">", stdout);
    fputs("<section class=\"ak-panel\" id=\"proofs\"><header><h2>Bench proofs</h2><span class=\"ak-kicker\">Linked work · steward sign-off</span></header><ul class=\"ak-rows\">", stdout);
    sqlite3_stmt *s = NULL; int shown = 0; char note[4001] = "", note_by[64] = "", note_at[32] = "";
    if (sqlite3_prepare_v2(db, "SELECT p.proof,p.title,p.state,p.submitted_at,COALESCE(m.title,''),COALESCE(p.sign_off,''),COALESCE(p.steward_subject,''),COALESCE(p.decided_at,'') FROM guild_bench_proof p LEFT JOIN guild_module m ON m.namespace='root' AND m.community=p.community AND m.module=p.module WHERE p.namespace='root' AND p.community=?1 AND p.apprentice_subject=?2 AND p.status='published' ORDER BY p.id DESC LIMIT 20", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, c->key, -1, SQLITE_TRANSIENT); sqlite3_bind_text(s, 2, actor, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            const char *state = (const char *)sqlite3_column_text(s, 2), *sign = (const char *)sqlite3_column_text(s, 5);
            shown++;
            fputs("<a href=\"/guild/", stdout); html(c->key); fputs("/proof/", stdout); html((const char *)sqlite3_column_text(s, 0));
            fputs("\"><strong>", stdout); html((const char *)sqlite3_column_text(s, 1)); fputs("</strong><small>", stdout);
            html((const char *)sqlite3_column_text(s, 4)); fputs(" · submitted ", stdout); html((const char *)sqlite3_column_text(s, 3));
            fputs("</small><span class=\"ak-go\">", stdout);
            fputs(!strcmp(state, "signed") ? "Signed off" : !strcmp(state, "returned") ? "Returned" : "Awaiting sign-off", stdout);
            fputs("</span></a>", stdout);
            if (!*note && sign && *sign) {
                snprintf(note, sizeof(note), "%s", sign);
                snprintf(note_by, sizeof(note_by), "%s", (const char *)sqlite3_column_text(s, 6));
                snprintf(note_at, sizeof(note_at), "%s", (const char *)sqlite3_column_text(s, 7));
            }
        }
    }
    sqlite3_finalize(s);
    fputs("</ul>", stdout);
    if (!shown) fputs("<p class=\"ak-empty\">You have not linked any bench work yet.</p>", stdout);
    if (nmod) {
        ph_nonce(nonce);
        FtField f[2]; gr_hidden_base(f, "proof-submit", nonce);
        GrIn in[] = {
            {"What you made", "text", "title", "", "", 0, "160", 1, ""},
            {"Link to the work (photos, video, measurements)", "url", "url", "", "https://", 0, "400", 1, "url"},
            {"Note for the steward", "text", "text", "", "", 1, "4000", 1, ""},
        };
        gr_form("gr-proof", "Submit bench proof", act, f, 2, in, 3, "Module", "module", opt, nmod);
    } else fputs("<p class=\"ak-empty\">Bench proofs open once the steward writes a curriculum module.</p>", stdout);
    fputs("</section>", stdout);

    fputs("<aside class=\"ak-panel is-raised\"><span class=\"ak-kicker\">Steward's latest note</span>", stdout);
    if (*note) {
        fputs("<p class=\"ak-lede\">“", stdout); html(note); fputs("”</p><p><small>— ", stdout);
        gr_person(note_by, actor, c->owner); fputs(", ", stdout); html(note_at); fputs("</small></p>", stdout);
    } else fputs("<p class=\"ak-empty\">When a steward signs off or returns your work, their words appear here.</p>", stdout);
    fputs("</aside></div>", stdout);

    fputs("<section class=\"ak-sec\" id=\"hours\"><header><div><span class=\"ak-kicker\">Hours of practice</span><h2>My shop hours</h2></div></header><div class=\"ak-scroll\"><table class=\"ak-table\"><thead><tr><th>Day</th><th>Activity</th><th>Module</th><th>Hours</th><th>State</th></tr></thead><tbody>", stdout);
    shown = 0;
    if (sqlite3_prepare_v2(db, "SELECT h.worked_on,h.activity,COALESCE(m.title,''),h.hours,h.state,COALESCE(h.confirmed_by,'') FROM guild_hours h LEFT JOIN guild_module m ON m.namespace='root' AND m.community=h.community AND m.module=h.module WHERE h.namespace='root' AND h.community=?1 AND h.apprentice_subject=?2 AND h.status='published' ORDER BY h.worked_on DESC,h.id DESC LIMIT 40", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, c->key, -1, SQLITE_TRANSIENT); sqlite3_bind_text(s, 2, actor, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            const char *state = (const char *)sqlite3_column_text(s, 4), *by = (const char *)sqlite3_column_text(s, 5);
            shown++;
            fputs("<tr><td><code>", stdout); html((const char *)sqlite3_column_text(s, 0)); fputs("</code></td><td>", stdout);
            html((const char *)sqlite3_column_text(s, 1)); fputs("</td><td>", stdout); html((const char *)sqlite3_column_text(s, 2));
            fputs("</td><td>", stdout); html((const char *)sqlite3_column_text(s, 3)); fputs("</td><td>", stdout);
            if (!strcmp(state, "confirmed")) { fputs("<span class=\"ak-tag is-on\">Confirmed</span> ", stdout); gr_person(by, actor, c->owner); }
            else if (!strcmp(state, "declined")) fputs("<span class=\"ak-tag\">Declined</span>", stdout);
            else fputs("<span class=\"ak-tag\">Awaiting steward</span>", stdout);
            fputs("</td></tr>", stdout);
        }
    }
    sqlite3_finalize(s);
    fputs("</tbody></table></div>", stdout);
    if (!shown) fputs("<p class=\"ak-empty\">No hours logged yet.</p>", stdout);
    if (nmod) {
        char today[11]; gr_today(today); ph_nonce(nonce);
        FtField f[2]; gr_hidden_base(f, "hours-log", nonce);
        GrIn in[] = {
            {"Day worked", "date", "date", today, "", 0, "10", 1, ""},
            {"Hours", "text", "hours", "", "", 0, "5", 1, "decimal"},
            {"What you did", "text", "title", "", "", 0, "160", 1, ""},
        };
        fputs("<div class=\"ak-ask\" style=\"margin-top:18px\">", stdout);
        gr_form("gr-hours", "Log shop hours", act, f, 2, in, 3, "Module", "module", opt, nmod);
        fputs("</div>", stdout);
    }
    fputs("</section>", stdout);

    fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">Curriculum</span><h2>Progression map</h2></div></header>", stdout);
    shown = 0;
    if (sqlite3_prepare_v2(db, "SELECT level,title,COALESCE(required_hours,''),"
        " (SELECT COALESCE(sum(CAST(hours AS REAL)),0) FROM guild_hours h WHERE h.namespace='root' AND h.community=m.community AND h.module=m.module AND h.apprentice_subject=?2 AND h.state='confirmed'),"
        " (SELECT count(*) FROM guild_bench_proof p WHERE p.namespace='root' AND p.community=m.community AND p.module=m.module AND p.apprentice_subject=?2 AND p.state='signed')"
        " FROM guild_module m WHERE namespace='root' AND community=?1 AND status='published' ORDER BY CAST(level AS INTEGER),id LIMIT 24", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, c->key, -1, SQLITE_TRANSIENT); sqlite3_bind_text(s, 2, actor, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            const char *req = (const char *)sqlite3_column_text(s, 2);
            double mine = sqlite3_column_double(s, 3), need = req && *req ? strtod(req, NULL) : 0;
            if (!shown++) fputs("<div class=\"ak-grid\">", stdout);
            fputs("<article class=\"ak-panel\"><header><span class=\"ak-kicker\">Level ", stdout); html((const char *)sqlite3_column_text(s, 0));
            fputs("</span>", stdout);
            fputs(sqlite3_column_int(s, 4) ? "<span class=\"ak-tag is-on\">Signed off</span>" : mine > 0 ? "<span class=\"ak-tag\">In practice</span>" : "", stdout);
            fputs("</header><h3>", stdout); html((const char *)sqlite3_column_text(s, 1)); fputs("</h3>", stdout);
            if (need > 0) printf("<p><small>%.1f / %.0f hours</small></p><div class=\"ak-meter\"><i style=\"width:%.0f%%\"></i></div>", mine, need, mine >= need ? 100.0 : mine * 100.0 / need);
            else printf("<p><small>%.1f confirmed hours</small></p>", mine);
            fputs("</article>", stdout);
        }
    }
    sqlite3_finalize(s);
    if (shown) fputs("</div>", stdout); else fputs("<p class=\"ak-empty\">No modules yet.</p>", stdout);
    fputs("</section>", stdout);
    ak_app_close();
}

static void gr_proof(sqlite3 *db, const CjrCommunity *c, const char *actor, const char *key) {
    sqlite3_stmt *s = NULL;
    if (sqlite3_prepare_v2(db, "SELECT p.title,p.evidence_url,p.note,p.state,p.apprentice_subject,p.submitted_at,COALESCE(p.sign_off,''),COALESCE(p.steward_subject,''),COALESCE(p.decided_at,''),COALESCE(m.level,''),COALESCE(m.title,''),p.module FROM guild_bench_proof p LEFT JOIN guild_module m ON m.namespace='root' AND m.community=p.community AND m.module=p.module WHERE p.namespace='root' AND p.community=?1 AND p.proof=?2 AND p.status='published' LIMIT 1", -1, &s, NULL) != SQLITE_OK || (sqlite3_bind_text(s, 1, c->key, -1, SQLITE_TRANSIENT), sqlite3_bind_text(s, 2, key, -1, SQLITE_TRANSIENT), sqlite3_step(s)) != SQLITE_ROW) {
        sqlite3_finalize(s); ph_error(404, "This bench proof is not in this guild.\n"); return;
    }
#define GRC(i) ((const char *)sqlite3_column_text(s, i))
    const char *apprentice = GRC(4), *state = GRC(3);
    int steward = gr_steward(db, c, actor);
    char act[160], nonce[33]; gr_act_path(c, act);
    gr_open(GRC(0), c, "", 1);
    fputs("<header class=\"ak-wb-hero\"><div><p class=\"ak-actions\"><span class=\"ak-tag is-on\">Level ", stdout); html(GRC(9)); fputs(" · ", stdout); html(GRC(10));
    fputs("</span><span class=\"ak-tag\">Submitted ", stdout); html(GRC(5)); fputs("</span><span class=\"ak-tag\">", stdout);
    fputs(!strcmp(state, "signed") ? "Signed off" : !strcmp(state, "returned") ? "Returned for more work" : "Awaiting sign-off", stdout);
    fputs("</span></p><h1>", stdout); html(GRC(0)); fputs("</h1><p class=\"ak-lede\">", stdout); html(GRC(2));
    fputs("</p></div><aside class=\"ak-panel\"><span class=\"ak-kicker\">Apprentice</span><h3>", stdout); gr_person(apprentice, actor, c->owner);
    fputs("</h3><span class=\"ak-kicker\">Signatory steward</span><h3>", stdout);
    gr_person(*GRC(7) ? GRC(7) : c->owner, actor, *GRC(7) ? c->owner : NULL);
    fputs("</h3></aside></header>", stdout);
    double hours = gr_number(db, "SELECT COALESCE(sum(CAST(hours AS REAL)),0) FROM guild_hours WHERE namespace='root' AND community=?1 AND state='confirmed' AND status='published' AND apprentice_subject=?2", c->key, apprentice);
    fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">Section 01</span><h2>The work submitted</h2></div></header><div class=\"ak-grid is-2\"><article class=\"ak-panel\"><h3>Linked evidence</h3><p>The apprentice links where the work can be seen. Review it there; this page does not copy or verify it.</p><p><a class=\"ak-btn\" rel=\"nofollow noopener noreferrer\" target=\"_blank\" href=\"", stdout);
    html(GRC(1)); fputs("\">Open the linked work</a></p><p><small>", stdout); html(GRC(1));
    printf("</small></p></article><article class=\"ak-panel\"><h3>Confirmed shop hours</h3><dl class=\"ak-spec\"><dt>All modules</dt><dd class=\"is-on\">%.1f</dd>", hours);
    fputs("</dl><p>Hours count only after a steward confirms them.</p></article></div></section>", stdout);
    fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">Section 02</span><h2>Steward's sign-off</h2></div></header>", stdout);
    if (*GRC(6)) {
        fputs("<blockquote class=\"ak-panel is-raised\"><p class=\"ak-lede\">“", stdout); html(GRC(6)); fputs("”</p><p><small>— ", stdout);
        gr_person(GRC(7), actor, c->owner); fputs(", ", stdout); html(GRC(8)); fputs("</small></p></blockquote>", stdout);
    }
    if (steward && !strcmp(state, "submitted") && strcmp(apprentice, actor)) {
        ph_nonce(nonce);
        FtField f[3]; gr_hidden_base(f, "proof-decide", nonce); f[2].name = "target"; f[2].value = key;
        FtOption o[2] = {{"sign", "Sign off this bench proof", 1}, {"return", "Return it for more work", 0}};
        GrIn in[] = {{"Your written sign-off or reasons", "text", "text", "", "", 1, "4000", 1, ""}};
        fputs("<div class=\"ak-ask\"><p>Your words are recorded with your name. Signing off releases no allowance, escrow or payment and is not a certificate outside this guild.</p>", stdout);
        gr_form("gr-decide", "Record my decision", act, f, 3, in, 1, "Decision", "kind", o, 2);
        fputs("</div>", stdout);
    } else if (!strcmp(state, "submitted")) {
        fputs("<p class=\"ak-empty\">Waiting for a steward who is not the apprentice to write a sign-off.</p>", stdout);
    }
    fputs("</section>", stdout);
#undef GRC
    sqlite3_finalize(s);
    ak_app_close();
}

static void gr_attest(sqlite3 *db, const CjrCommunity *c, const char *actor) {
    int steward = gr_steward(db, c, actor);
    char act[160], nonce[33]; gr_act_path(c, act);
    gr_open("Skill attestations", c, "attest", *actor != 0);
    fputs("<header class=\"ak-wb-hero is-solo\"><div><span class=\"ak-kicker\">Steward attestation</span><h1>Skills this guild attests</h1><p class=\"ak-lede\">A steward states, in writing and by name, that a current member holds a skill. The steward can revoke it; the history stays.</p></div></header>", stdout);
    if (steward) {
        FtOption opt[40]; char vals[40][24], labels[40][40]; unsigned n = 0;
        sqlite3_stmt *s = NULL;
        if (sqlite3_prepare_v2(db, "SELECT member_subject FROM community_membership WHERE namespace='root' AND community=?1 AND status='published' AND state='active' AND member_subject<>?2 AND (COALESCE(expires_at,'')='' OR julianday(expires_at)>julianday('now')) ORDER BY id LIMIT 40", -1, &s, NULL) == SQLITE_OK) {
            sqlite3_bind_text(s, 1, c->key, -1, SQLITE_TRANSIENT); sqlite3_bind_text(s, 2, actor, -1, SQLITE_TRANSIENT);
            while (n < 40 && sqlite3_step(s) == SQLITE_ROW) {
                const char *m = (const char *)sqlite3_column_text(s, 0);
                if (!cjr_actor(m)) continue;
                snprintf(vals[n], 24, "%s", m); snprintf(labels[n], 40, "Member %s", m + 5);
                opt[n].value = vals[n]; opt[n].label = labels[n]; opt[n].checked = n == 0; n++;
            }
        }
        sqlite3_finalize(s);
        fputs("<section class=\"ak-ask\"><span class=\"ak-kicker\">Attestation gateway</span><h2>Attest a member's skill</h2>", stdout);
        if (n) {
            ph_nonce(nonce);
            FtField f[2]; gr_hidden_base(f, "attest", nonce);
            GrIn in[] = {
                {"Skill", "text", "title", "", "", 0, "160", 1, ""},
                {"What you saw them do", "text", "text", "", "", 1, "4000", 1, ""},
            };
            gr_form("gr-attest", "Record attestation", act, f, 2, in, 2, "Member", "member", opt, n);
        } else fputs("<p class=\"ak-empty\">This guild has no other active members to attest yet.</p>", stdout);
        fputs("</section>", stdout);
    }
    fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">Register</span><h2>Attested skills</h2></div></header><ul class=\"ak-rows\">", stdout);
    sqlite3_stmt *s = NULL; int shown = 0;
    if (sqlite3_prepare_v2(db, "SELECT attestation,member_subject,skill,statement,steward_subject,state,attested_at,COALESCE(revoked_at,''),COALESCE(revoke_reason,'') FROM guild_attestation WHERE namespace='root' AND community=?1 AND status='published' ORDER BY id DESC LIMIT 60", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, c->key, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            const char *state = (const char *)sqlite3_column_text(s, 5);
            shown++;
            fputs("<li><strong>", stdout); gr_person((const char *)sqlite3_column_text(s, 1), actor, c->owner); fputs(" · ", stdout);
            html((const char *)sqlite3_column_text(s, 2)); fputs("</strong><small>", stdout); html((const char *)sqlite3_column_text(s, 3));
            fputs(" — attested by ", stdout); gr_person((const char *)sqlite3_column_text(s, 4), actor, c->owner); fputs(", ", stdout);
            html((const char *)sqlite3_column_text(s, 6));
            if (!strcmp(state, "revoked")) { fputs(" · revoked ", stdout); html((const char *)sqlite3_column_text(s, 7)); fputs(": ", stdout); html((const char *)sqlite3_column_text(s, 8)); }
            fputs("</small><span class=\"ak-go\">", stdout);
            if (!strcmp(state, "attested") && steward) {
                ph_nonce(nonce);
                FtField f[3]; gr_hidden_base(f, "attest-revoke", nonce); f[2].name = "target"; f[2].value = (const char *)sqlite3_column_text(s, 0);
                GrIn in[] = {{"Reason", "text", "text", "", "", 0, "400", 1, ""}};
                char id[40]; snprintf(id, sizeof(id), "gr-rv-%d", shown);
                gr_form(id, "Revoke", act, f, 3, in, 1, NULL, NULL, NULL, 0);
            } else fputs(!strcmp(state, "attested") ? "Current" : "Revoked", stdout);
            fputs("</span></li>", stdout);
        }
    }
    sqlite3_finalize(s);
    fputs("</ul>", stdout);
    if (!shown) fputs("<p class=\"ak-empty\">No skill has been attested in this guild yet.</p>", stdout);
    fputs("</section>", stdout);
    ak_app_close();
}

static const char *gr_kind_label(const char *k) {
    return !strcmp(k, "apprenticeship") ? "Apprenticeship opening" : !strcmp(k, "patronage") ? "Patronage" : "Grant";
}
static void gr_offer_panel(sqlite3 *db, sqlite3_stmt *s, const char *actor, int steward,
                           const char *guild_key, const char *guild_title, const char *owner, int show_guild) {
    const char *key = (const char *)sqlite3_column_text(s, 0), *kind = (const char *)sqlite3_column_text(s, 1),
               *state = (const char *)sqlite3_column_text(s, 7);
    char act[160], nonce[33]; snprintf(act, sizeof(act), "/guild/%s/act", guild_key);
    fputs("<article class=\"ak-panel\" id=\"offer-", stdout); html(key); fputs("\"><header><span class=\"ak-tag is-on\">", stdout);
    fputs(gr_kind_label(kind), stdout); fputs("</span><span class=\"ak-tag\">", stdout);
    fputs(!strcmp(state, "open") ? "Open" : "Closed", stdout); fputs("</span></header>", stdout);
    if (show_guild) { fputs("<span class=\"ak-kicker\">", stdout); html(guild_title); fputs("</span>", stdout); }
    fputs("<h3>", stdout); html((const char *)sqlite3_column_text(s, 2)); fputs("</h3><p>", stdout);
    html((const char *)sqlite3_column_text(s, 3)); fputs("</p><dl class=\"ak-spec\">", stdout);
    if (*(const char *)sqlite3_column_text(s, 4)) { fputs("<dt>Stated terms</dt><dd class=\"is-on\">", stdout); html((const char *)sqlite3_column_text(s, 4)); fputs("</dd>", stdout); }
    if (*(const char *)sqlite3_column_text(s, 5)) { fputs("<dt>Where</dt><dd>", stdout); html((const char *)sqlite3_column_text(s, 5)); fputs("</dd>", stdout); }
    if (*(const char *)sqlite3_column_text(s, 6)) { fputs("<dt>Closes</dt><dd>", stdout); html((const char *)sqlite3_column_text(s, 6)); fputs("</dd>", stdout); }
    printf("<dt>Applications</dt><dd>%.0f</dd></dl>", gr_number(db, "SELECT count(*) FROM guild_offer_application WHERE namespace='root' AND community=?1 AND offer=?2 AND status='published' AND state<>'withdrawn'", guild_key, key));
    if (show_guild) { fputs("<p><a class=\"ak-btn\" href=\"/guild/", stdout); html(guild_key); fputs("/offers#offer-", stdout); html(key); fputs("\">Open the offer</a></p></article>", stdout); return; }
    if (!strcmp(state, "open") && *actor && !steward) {
        char existing[24] = "";
        gr_row(db, "SELECT id,state FROM guild_offer_application WHERE namespace='root' AND offer=?1 AND applicant_subject=?2 ORDER BY id DESC LIMIT 1", key, actor, existing, sizeof(existing), NULL, 0, NULL, 0);
        if (*existing && strcmp(existing, "withdrawn")) {
            fputs("<p><span class=\"ak-tag is-on\">Your application: ", stdout); html(existing); fputs("</span></p>", stdout);
        } else {
            ph_nonce(nonce);
            FtField f[3]; gr_hidden_base(f, "apply", nonce); f[2].name = "target"; f[2].value = key;
            GrIn in[] = {{"Why you, in your words", "text", "text", "", "", 1, "4000", 1, ""}};
            char id[120]; snprintf(id, sizeof(id), "gr-apply-%s", key);
            gr_form(id, "Apply", act, f, 3, in, 1, NULL, NULL, NULL, 0);
        }
    } else if (!*actor && !strcmp(state, "open")) {
        fputs("<p><a class=\"ak-btn\" href=\"/login?next=%2Fguild%2F", stdout); url_query_value(guild_key); fputs("%2Foffers\">Sign in to apply</a></p>", stdout);
    }
    if (steward) {
        sqlite3_stmt *a = NULL; int n = 0;
        fputs("<ul class=\"ak-rows\">", stdout);
        if (sqlite3_prepare_v2(db, "SELECT application,applicant_subject,statement,state,COALESCE(decision_note,'') FROM guild_offer_application WHERE namespace='root' AND community=?1 AND offer=?2 AND status='published' ORDER BY id LIMIT 40", -1, &a, NULL) == SQLITE_OK) {
            sqlite3_bind_text(a, 1, guild_key, -1, SQLITE_TRANSIENT); sqlite3_bind_text(a, 2, key, -1, SQLITE_TRANSIENT);
            while (sqlite3_step(a) == SQLITE_ROW) {
                const char *st = (const char *)sqlite3_column_text(a, 3);
                n++;
                fputs("<li><strong>", stdout); gr_person((const char *)sqlite3_column_text(a, 1), actor, owner); fputs("</strong><small>", stdout);
                html((const char *)sqlite3_column_text(a, 2)); fputs("</small><span class=\"ak-go\">", stdout);
                if (!strcmp(st, "submitted")) {
                    ph_nonce(nonce);
                    FtField f[3]; gr_hidden_base(f, "apply-decide", nonce); f[2].name = "target"; f[2].value = (const char *)sqlite3_column_text(a, 0);
                    FtOption o[2] = {{"accept", "Accept", 1}, {"decline", "Decline", 0}};
                    GrIn in[] = {{"Decision note", "text", "text", "", "", 0, "1000", 1, ""}};
                    char id[60]; snprintf(id, sizeof(id), "gr-ad-%d", n);
                    gr_form(id, "Record decision", act, f, 3, in, 1, "Decision", "kind", o, 2);
                } else { html(st); }
                fputs("</span></li>", stdout);
            }
        }
        sqlite3_finalize(a);
        fputs("</ul>", stdout);
        if (!n) fputs("<p class=\"ak-empty\">No applications yet.</p>", stdout);
        if (!strcmp(state, "open")) {
            ph_nonce(nonce);
            FtField f[3]; gr_hidden_base(f, "offer-close", nonce); f[2].name = "target"; f[2].value = key;
            char id[120]; snprintf(id, sizeof(id), "gr-close-%s", key);
            gr_button(id, "Close this offer", act, f, 3);
        }
    }
    fputs("</article>", stdout);
}
#define GR_OFFER_COLUMNS "offer,kind,title,terms,COALESCE(stated_value,''),COALESCE(place,''),COALESCE(closes_on,''),state"
static void gr_offers(sqlite3 *db, const CjrCommunity *c, const char *actor) {
    int steward = gr_steward(db, c, actor);
    char act[160], nonce[33]; gr_act_path(c, act);
    gr_open("Grants, patronage and openings", c, "offers", *actor != 0);
    fputs("<header class=\"ak-wb-hero is-solo\"><div><span class=\"ak-kicker\">Guild offers</span><h1>Grants, patronage and apprenticeship openings</h1><p class=\"ak-lede\">What this guild offers, with its stated terms. Anyone signed in can apply; the steward records each decision. No money moves here and nothing is signed for you.</p></div></header>", stdout);
    printf("<dl class=\"ak-stats is-row\"><div><dt>Open offers</dt><dd class=\"is-on\">%.0f</dd></div><div><dt>Applications</dt><dd>%.0f</dd></div><div><dt>Accepted</dt><dd>%.0f</dd></div></dl>",
           gr_number(db, "SELECT count(*) FROM guild_offer WHERE namespace='root' AND community=?1 AND state='open' AND status='published'", c->key, NULL),
           gr_number(db, "SELECT count(*) FROM guild_offer_application WHERE namespace='root' AND community=?1 AND state<>'withdrawn' AND status='published'", c->key, NULL),
           gr_number(db, "SELECT count(*) FROM guild_offer_application WHERE namespace='root' AND community=?1 AND state='accepted' AND status='published'", c->key, NULL));
    static const char *const kinds[] = {"grant", "patronage", "apprenticeship"};
    static const char *const titles[] = {"Grants", "Patronage", "Apprenticeship openings"};
    int any = 0;
    for (int k = 0; k < 3; k++) {
        sqlite3_stmt *s = NULL; int shown = 0;
        if (sqlite3_prepare_v2(db, "SELECT " GR_OFFER_COLUMNS " FROM guild_offer WHERE namespace='root' AND community=?1 AND kind=?2 AND status='published' ORDER BY state='open' DESC,id DESC LIMIT 30", -1, &s, NULL) != SQLITE_OK) continue;
        sqlite3_bind_text(s, 1, c->key, -1, SQLITE_TRANSIENT); sqlite3_bind_text(s, 2, kinds[k], -1, SQLITE_STATIC);
        while (sqlite3_step(s) == SQLITE_ROW) {
            if (!shown++) { printf("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">Section %d</span><h2>%s</h2></div></header><div class=\"ak-grid\">", k + 1, titles[k]); }
            gr_offer_panel(db, s, actor, steward, c->key, c->title, c->owner, 0);
        }
        if (shown) { fputs("</div></section>", stdout); any = 1; }
        sqlite3_finalize(s);
    }
    if (!any) fputs("<p class=\"ak-empty\">This guild has not posted an offer yet.</p>", stdout);
    if (steward) {
        ph_nonce(nonce);
        FtField f[2]; gr_hidden_base(f, "offer-post", nonce);
        FtOption o[3] = {{"grant", "Grant", 1}, {"patronage", "Patronage", 0}, {"apprenticeship", "Apprenticeship opening", 0}};
        GrIn in[] = {
            {"Title", "text", "title", "", "", 0, "160", 1, ""},
            {"Terms, in plain words", "text", "text", "", "", 1, "4000", 1, ""},
            {"Stated value (as the guild states it)", "text", "value", "", "", 0, "80", 0, ""},
            {"Where", "text", "place", "", "", 0, "120", 0, ""},
            {"Closes on", "date", "closes", "", "", 0, "10", 0, ""},
        };
        fputs("<section class=\"ak-sec\"><div class=\"ak-ask\"><span class=\"ak-kicker\">Guild treasury notice</span><h2>Post an offer</h2><p>The terms are what the guild says it intends. Posting holds no funds and makes no payment.</p>", stdout);
        gr_form("gr-offer", "Post offer", act, f, 2, in, 5, "Kind", "kind", o, 3);
        fputs("</div></section>", stdout);
    }
    ak_app_close();
}
static void gr_openings(sqlite3 *db, const char *actor) {
    ak_app_http(200, *actor != 0);
    ak_app_open("Apprenticeship openings", "workbench", "route-guild");
    fputs("<nav class=\"ak-wb-strip\" aria-label=\"Guilds\"><span class=\"ak-kicker\">Guilds</span><a href=\"/guilds\">All guilds</a><a href=\"/guild/openings\" aria-current=\"page\">Openings</a><a href=\"/charters\">Charters</a></nav>", stdout);
    fputs("<header class=\"ak-wb-hero is-solo\"><div><span class=\"ak-kicker\">Labour commons</span><h1>Apprenticeships and guild openings</h1><p class=\"ak-lede\">Benches and openings posted by guild stewards, with their terms in their own words. You apply to the guild; its steward decides. Nobody filters you by machine.</p><div class=\"ak-actions\"><a class=\"ak-btn is-primary\" href=\"/guild/new\">Start a guild</a><a class=\"ak-btn\" href=\"/guilds\">Browse guilds</a></div></div></header>", stdout);
    printf("<dl class=\"ak-stats is-row\"><div><dt>Open openings</dt><dd class=\"is-on\">%.0f</dd></div><div><dt>Guilds offering</dt><dd>%.0f</dd></div><div><dt>Placements accepted</dt><dd>%.0f</dd></div></dl>",
           gr_number(db, "SELECT count(*) FROM guild_offer WHERE namespace='root' AND kind='apprenticeship' AND state='open' AND status='published'", NULL, NULL),
           gr_number(db, "SELECT count(DISTINCT community) FROM guild_offer WHERE namespace='root' AND state='open' AND status='published'", NULL, NULL),
           gr_number(db, "SELECT count(*) FROM guild_offer_application a JOIN guild_offer o ON o.namespace='root' AND o.offer=a.offer WHERE a.namespace='root' AND o.kind='apprenticeship' AND a.state='accepted'", NULL, NULL));
    sqlite3_stmt *s = NULL; int shown = 0;
    fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">Open now</span><h2>Available openings</h2></div></header>", stdout);
    if (sqlite3_prepare_v2(db, "SELECT o.offer,o.kind,o.title,o.terms,COALESCE(o.stated_value,''),COALESCE(o.place,''),COALESCE(o.closes_on,''),o.state,c.community,c.title,c.created_by_subject FROM guild_offer o JOIN community_body c ON c.namespace='root' AND c.community=o.community AND c.governance_scope='local-guild' AND c.status='published' AND c.visibility='public' WHERE o.namespace='root' AND o.state='open' AND o.status='published' ORDER BY o.kind='apprenticeship' DESC,o.id DESC LIMIT 40", -1, &s, NULL) == SQLITE_OK) {
        while (sqlite3_step(s) == SQLITE_ROW) {
            if (!shown++) fputs("<div class=\"ak-grid\">", stdout);
            gr_offer_panel(db, s, actor, 0, (const char *)sqlite3_column_text(s, 8), (const char *)sqlite3_column_text(s, 9), (const char *)sqlite3_column_text(s, 10), 1);
        }
    }
    sqlite3_finalize(s);
    if (shown) fputs("</div>", stdout);
    else fputs("<p class=\"ak-empty\">No guild has an open apprenticeship or grant right now.</p>", stdout);
    fputs("</section><section class=\"ak-band\"><div><span class=\"ak-kicker\">Open a bench</span><h2>Offer an apprenticeship</h2><p>Start a guild, write its curriculum, and post an opening with your terms.</p></div><div class=\"ak-actions\"><a class=\"ak-btn\" href=\"/guild/new\">Start a guild</a></div></section>", stdout);
    ak_app_close();
}

static void gr_registry(sqlite3 *db, const CjrCommunity *c, const char *actor) {
    int member = gr_active(db, c, actor);
    char act[160], nonce[33]; gr_act_path(c, act);
    gr_open("Field registry", c, "registry", *actor != 0);
    fputs("<header class=\"ak-wb-hero is-solo\"><div><span class=\"ak-kicker\">Field registry · provenance</span><h1>What members have seen, and where to check it</h1><p class=\"ak-lede\">Each observation carries the link a member cites. When the estate already reads that source, its name is shown. A recorded observation is not a verified one.</p></div></header>", stdout);
    printf("<dl class=\"ak-stats is-row\"><div><dt>Observations</dt><dd class=\"is-on\">%.0f</dd></div><div><dt>Observers</dt><dd>%.0f</dd></div><div><dt>Known sources cited</dt><dd>%.0f</dd></div></dl>",
           gr_number(db, "SELECT count(*) FROM field_observation WHERE namespace='root' AND community=?1 AND status='published'", c->key, NULL),
           gr_number(db, "SELECT count(DISTINCT observer_subject) FROM field_observation WHERE namespace='root' AND community=?1 AND status='published'", c->key, NULL),
           gr_number(db, "SELECT count(*) FROM field_observation WHERE namespace='root' AND community=?1 AND status='published' AND COALESCE(internet_source,'')<>''", c->key, NULL));
    sqlite3_stmt *s = NULL; int shown = 0;
    fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">Registry</span><h2>Observations</h2></div></header>", stdout);
    if (sqlite3_prepare_v2(db, "SELECT f.title,f.observation,f.source_url,f.observed_at,COALESCE(f.place,''),f.observer_subject,COALESCE(i.label,'') FROM field_observation f LEFT JOIN internet_source i ON i.namespace='root' AND i.source=f.internet_source AND COALESCE(f.internet_source,'')<>'' WHERE f.namespace='root' AND f.community=?1 AND f.status='published' GROUP BY f.id ORDER BY f.observed_at DESC,f.id DESC LIMIT 60", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, c->key, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(s) == SQLITE_ROW) {
            if (!shown++) fputs("<div class=\"ak-grid\">", stdout);
            fputs("<article class=\"ak-panel\"><header><span class=\"ak-kicker\">", stdout); html((const char *)sqlite3_column_text(s, 3));
            fputs("</span>", stdout);
            if (*(const char *)sqlite3_column_text(s, 6)) { fputs("<span class=\"ak-tag is-on\">", stdout); html((const char *)sqlite3_column_text(s, 6)); fputs("</span>", stdout); }
            fputs("</header><h3>", stdout); html((const char *)sqlite3_column_text(s, 0)); fputs("</h3><p>", stdout);
            html((const char *)sqlite3_column_text(s, 1)); fputs("</p><dl class=\"ak-spec\">", stdout);
            if (*(const char *)sqlite3_column_text(s, 4)) { fputs("<dt>Where</dt><dd>", stdout); html((const char *)sqlite3_column_text(s, 4)); fputs("</dd>", stdout); }
            fputs("<dt>Observer</dt><dd>", stdout); gr_person((const char *)sqlite3_column_text(s, 5), actor, c->owner);
            fputs("</dd></dl><p><a rel=\"nofollow noopener noreferrer\" target=\"_blank\" href=\"", stdout); html((const char *)sqlite3_column_text(s, 2));
            fputs("\">Check the cited source</a></p></article>", stdout);
        }
    }
    sqlite3_finalize(s);
    if (shown) fputs("</div>", stdout); else fputs("<p class=\"ak-empty\">No observation has been recorded here yet.</p>", stdout);
    fputs("</section>", stdout);
    if (member) {
        char today[11]; gr_today(today); ph_nonce(nonce);
        FtField f[2]; gr_hidden_base(f, "observe", nonce);
        GrIn in[] = {
            {"What you observed", "text", "title", "", "", 0, "160", 1, ""},
            {"Details", "text", "text", "", "", 1, "4000", 1, ""},
            {"Where it can be checked", "url", "url", "", "https://", 0, "400", 1, "url"},
            {"Observed on", "date", "date", today, "", 0, "10", 1, ""},
            {"Place", "text", "place", "", "", 0, "120", 0, ""},
        };
        fputs("<section class=\"ak-band\" style=\"display:block\"><span class=\"ak-kicker\">Lodge an observation</span><h2>Add to the registry</h2><p>Members of this community can record what they saw with a source anyone can open.</p></section><div class=\"ak-ask\" style=\"margin-top:16px\">", stdout);
        gr_form("gr-observe", "Record observation", act, f, 2, in, 5, NULL, NULL, NULL, 0);
        fputs("</div>", stdout);
    }
    ak_app_close();
}

/* ---------------------------------------------------------------- writes */

static void gr_key(const char *prefix, const char *request, char out[97]) {
    snprintf(out, 97, "%s-%s", prefix, request);
}
static int gr_commit(sqlite3 *db, const CjrCommunity *c, const char *actor, const GrForm *p, char back[256]) {
    char now[32], key[97], cols[3][4001]; long long id;
    int steward = gr_steward(db, c, actor), member = gr_active(db, c, actor);
    gr_now(now);
    snprintf(back, 256, "/guild/%s/", c->key);
    if (!strcmp(p->op, "module-add")) {
        if (!steward) return 403;
        if (!ph_digits(p->level) || !gr_words(p->title) || !gr_words(p->text) || (*p->hours && !gr_hours_ok(p->hours) && !ph_digits(p->hours))) return 400;
        gr_key("mod", p->request, key);
        const char *kv[] = {"module", key, "community", c->key, "level", p->level, "title", p->title, "description", p->text,
                            "required_hours", p->hours, "steward_subject", actor,
                            "cannot_infer", "A guild's own syllabus stage; not an accreditation or licence outside the guild.", "status", "published"};
        snprintf(back, 256, "/guild/%s/#syllabus", c->key);
        return gr_create("guild_module", "root", kv, 9) ? 303 : 503;
    }
    if (!strcmp(p->op, "hours-log") || !strcmp(p->op, "proof-submit")) {
        if (!member) return 403;
        if (!cjr_key(p->module) || !gr_row(db, "SELECT id FROM guild_module WHERE namespace='root' AND community=?1 AND module=?2 AND status='published'", c->key, p->module, NULL, 0, NULL, 0, NULL, 0)) return 400;
        snprintf(back, 256, "/guild/%s/bench", c->key);
        if (!strcmp(p->op, "hours-log")) {
            if (!gr_date_ok(p->date) || !gr_hours_ok(p->hours) || !gr_words(p->title)) return 400;
            gr_key("hrs", p->request, key);
            const char *kv[] = {"entry", key, "community", c->key, "module", p->module, "apprentice_subject", actor, "worked_on", p->date,
                                "hours", p->hours, "activity", p->title, "state", "submitted", "submitted_at", now,
                                "cannot_infer", "Hours the apprentice states they worked; confirmed only by a steward's written word, and never a payment.", "status", "published"};
            return gr_create("guild_hours", "root", kv, 11) ? 303 : 503;
        }
        if (!gr_words(p->title) || !gr_url_ok(p->url) || !gr_words(p->text)) return 400;
        gr_key("proof", p->request, key);
        const char *kv[] = {"proof", key, "community", c->key, "module", p->module, "apprentice_subject", actor, "title", p->title,
                            "evidence_url", p->url, "note", p->text, "state", "submitted", "submitted_at", now,
                            "cannot_infer", "Linked work awaiting review; the link is not verified and a sign-off releases nothing.", "status", "published"};
        return gr_create("guild_bench_proof", "root", kv, 11) ? 303 : 503;
    }
    if (!strcmp(p->op, "hours-confirm") || !strcmp(p->op, "hours-decline")) {
        if (!steward || !cjr_key(p->target)) return 403;
        id = gr_row(db, "SELECT id,apprentice_subject,state FROM guild_hours WHERE namespace='root' AND community=?1 AND entry=?2", c->key, p->target, cols[0], 64, cols[1], 32, NULL, 0);
        if (!id) return 404;
        if (!strcmp(cols[0], actor)) return 403;
        if (strcmp(cols[1], "submitted")) return 409;
        snprintf(back, 256, "/guild/%s/#queue", c->key);
        const char *kv[] = {"state", !strcmp(p->op, "hours-confirm") ? "confirmed" : "declined", "confirmed_by", actor, "confirmed_at", now};
        return gr_update("guild_hours", "root", id, "state", "submitted", kv, 3) ? 303 : 409;
    }
    if (!strcmp(p->op, "proof-decide")) {
        if (!steward || !cjr_key(p->target)) return 403;
        if ((strcmp(p->kind, "sign") && strcmp(p->kind, "return")) || !gr_words(p->text)) return 400;
        id = gr_row(db, "SELECT id,apprentice_subject,state FROM guild_bench_proof WHERE namespace='root' AND community=?1 AND proof=?2", c->key, p->target, cols[0], 64, cols[1], 32, NULL, 0);
        if (!id) return 404;
        if (!strcmp(cols[0], actor)) return 403;
        if (strcmp(cols[1], "submitted")) return 409;
        snprintf(back, 256, "/guild/%s/proof/%s", c->key, p->target);
        const char *kv[] = {"state", !strcmp(p->kind, "sign") ? "signed" : "returned", "steward_subject", actor, "sign_off", p->text, "decided_at", now};
        return gr_update("guild_bench_proof", "root", id, "state", "submitted", kv, 4) ? 303 : 409;
    }
    if (!strcmp(p->op, "attest")) {
        CjrMember m;
        if (!steward) return 403;
        if (!cjr_actor(p->member) || !strcmp(p->member, actor) || cjr_member(db, c->key, p->member, &m) != 1 || !m.active) return 400;
        if (!gr_words(p->title) || !gr_words(p->text)) return 400;
        gr_key("att", p->request, key);
        snprintf(back, 256, "/guild/%s/attest", c->key);
        const char *kv[] = {"attestation", key, "community", c->key, "member_subject", p->member, "skill", p->title, "statement", p->text,
                            "steward_subject", actor, "state", "attested", "attested_at", now,
                            "cannot_infer", "One steward's revocable statement inside this guild; not a licence, reference or credential elsewhere.", "status", "published"};
        return gr_create("guild_attestation", "root", kv, 10) ? 303 : 503;
    }
    if (!strcmp(p->op, "attest-revoke")) {
        if (!steward || !cjr_key(p->target) || !gr_words(p->text)) return steward ? 400 : 403;
        id = gr_row(db, "SELECT id,state FROM guild_attestation WHERE namespace='root' AND community=?1 AND attestation=?2", c->key, p->target, cols[0], 32, NULL, 0, NULL, 0);
        if (!id) return 404;
        if (strcmp(cols[0], "attested")) return 409;
        snprintf(back, 256, "/guild/%s/attest", c->key);
        const char *kv[] = {"state", "revoked", "revoked_at", now, "revoke_reason", p->text};
        return gr_update("guild_attestation", "root", id, "state", "attested", kv, 3) ? 303 : 409;
    }
    if (!strcmp(p->op, "offer-post")) {
        if (!steward) return 403;
        if ((strcmp(p->kind, "grant") && strcmp(p->kind, "patronage") && strcmp(p->kind, "apprenticeship")) || !gr_words(p->title) || !gr_words(p->text) || (*p->closes && !gr_date_ok(p->closes))) return 400;
        gr_key("offer", p->request, key);
        snprintf(back, 256, "/guild/%s/offers", c->key);
        const char *kv[] = {"offer", key, "community", c->key, "kind", p->kind, "title", p->title, "terms", p->text, "stated_value", p->value,
                            "place", p->place, "closes_on", p->closes, "state", "open", "steward_subject", actor, "posted_at", now,
                            "cannot_infer", "Terms the guild states it intends; no funds are held, promised or moved by this record.", "status", "published"};
        return gr_create("guild_offer", "root", kv, 13) ? 303 : 503;
    }
    if (!strcmp(p->op, "offer-close")) {
        if (!steward || !cjr_key(p->target)) return 403;
        id = gr_row(db, "SELECT id FROM guild_offer WHERE namespace='root' AND community=?1 AND offer=?2", c->key, p->target, NULL, 0, NULL, 0, NULL, 0);
        if (!id) return 404;
        snprintf(back, 256, "/guild/%s/offers", c->key);
        const char *kv[] = {"state", "closed"};
        return gr_update("guild_offer", "root", id, "state", "open", kv, 1) ? 303 : 409;
    }
    if (!strcmp(p->op, "apply")) {
        if (!cjr_key(p->target) || !gr_words(p->text)) return 400;
        if (steward) return 403;
        id = gr_row(db, "SELECT id,state FROM guild_offer WHERE namespace='root' AND community=?1 AND offer=?2 AND status='published'", c->key, p->target, cols[0], 32, NULL, 0, NULL, 0);
        if (!id) return 404;
        if (strcmp(cols[0], "open")) return 409;
        if (gr_row(db, "SELECT id FROM guild_offer_application WHERE namespace='root' AND offer=?1 AND applicant_subject=?2 AND state IN ('submitted','accepted')", p->target, actor, NULL, 0, NULL, 0, NULL, 0)) return 409;
        gr_key("app", p->request, key);
        snprintf(back, 256, "/guild/%s/offers#offer-%s", c->key, p->target);
        const char *kv[] = {"application", key, "community", c->key, "offer", p->target, "applicant_subject", actor, "statement", p->text,
                            "state", "submitted", "submitted_at", now,
                            "cannot_infer", "An application and, later, a steward's written decision; nothing is paid, signed or sent.", "status", "published"};
        return gr_create("guild_offer_application", "root", kv, 9) ? 303 : 503;
    }
    if (!strcmp(p->op, "apply-decide")) {
        if (!steward || !cjr_key(p->target)) return 403;
        if ((strcmp(p->kind, "accept") && strcmp(p->kind, "decline")) || !gr_words(p->text)) return 400;
        id = gr_row(db, "SELECT id,offer,state FROM guild_offer_application WHERE namespace='root' AND community=?1 AND application=?2", c->key, p->target, cols[0], 97, cols[1], 32, NULL, 0);
        if (!id) return 404;
        if (strcmp(cols[1], "submitted")) return 409;
        snprintf(back, 256, "/guild/%s/offers#offer-%s", c->key, cols[0]);
        const char *kv[] = {"state", !strcmp(p->kind, "accept") ? "accepted" : "declined", "decided_by", actor, "decision_note", p->text, "decided_at", now};
        return gr_update("guild_offer_application", "root", id, "state", "submitted", kv, 4) ? 303 : 409;
    }
    if (!strcmp(p->op, "observe")) {
        if (!member) return 403;
        if (!gr_words(p->title) || !gr_words(p->text) || !gr_url_ok(p->url) || !gr_date_ok(p->date)) return 400;
        char source[200] = "";
        gr_row(db, "SELECT id,source FROM internet_source WHERE namespace='root' AND url=?1 AND ?2='' LIMIT 1", p->url, "", source, sizeof(source), NULL, 0, NULL, 0);
        gr_key("obs", p->request, key);
        snprintf(back, 256, "/guild/%s/registry", c->key);
        const char *kv[] = {"observation_id", key, "community", c->key, "title", p->title, "observation", p->text, "source_url", p->url,
                            "internet_source", source, "observed_at", p->date, "place", p->place, "observer_subject", actor,
                            "cannot_infer", "A member's cited observation; recording it does not verify it.", "status", "published"};
        return gr_create("field_observation", "root", kv, 11) ? 303 : 503;
    }
    return 400;
}
static int gr_status(int code, const char *back) {
    switch (code) {
    case 303: page_redirect(back); break;
    case 403: ph_error(403, "Only a current member or this guild's steward can make that change.\n"); break;
    case 404: ph_error(404, "That record is not in this guild.\n"); break;
    case 409: ph_error(409, "That record changed or is already decided. Reopen the page.\n"); break;
    case 503: ph_error(503, "The record owner did not accept the change. Nothing was recorded.\n"); break;
    default: ph_error(400, "Complete the form as written.\n"); break;
    }
    return 1;
}
static int gr_parse(const char *body, GrForm *p) {
    memset(p, 0, sizeof(*p));
    PhField f[] = {
        {"op", p->op, sizeof(p->op), 0}, {"request", p->request, sizeof(p->request), 0},
        {"module", p->module, sizeof(p->module), 0}, {"target", p->target, sizeof(p->target), 0},
        {"member", p->member, sizeof(p->member), 0}, {"title", p->title, sizeof(p->title), 0},
        {"text", p->text, sizeof(p->text), 1}, {"hours", p->hours, sizeof(p->hours), 0},
        {"date", p->date, sizeof(p->date), 0}, {"url", p->url, sizeof(p->url), 0},
        {"level", p->level, sizeof(p->level), 0}, {"value", p->value, sizeof(p->value), 0},
        {"kind", p->kind, sizeof(p->kind), 0}, {"place", p->place, sizeof(p->place), 0},
        {"closes", p->closes, sizeof(p->closes), 0},
    };
    if (!body || !ph_fields(body, f, sizeof(f) / sizeof(f[0]), 12000)) return 0;
    return *p->op && strlen(p->request) == 32 && strspn(p->request, "0123456789abcdef") == 32;
}

static int serve_guild_records(const char *method, const char *path, const char *query, const char *body,
                               const char *raw, const char *host, const char *bearer, const char *auth_db,
                               const char *accept) {
    (void)query;
    if (!path || strncmp(path, "/guild/", 7) || !strcmp(path, "/guild/new")) return 0;
    int post = !strcmp(method, "POST");
    if (!post && !wants_html(accept)) return 0;
    StudioViewer who = {0}; char actor[64] = "";
    if (session_identity(bearer, auth_db, &who) && who.id > 0) snprintf(actor, sizeof(actor), "user:%ld", who.id);
    sqlite3 *db = cms_open_ro();
    if (!db) { ph_error(503, "The guild records are unavailable.\n"); return 1; }
    if (!gr_table(db, "guild_module")) { sqlite3_close(db); ph_error(503, "This store has not adopted guild records yet.\n"); return 1; }
    if (!strcmp(path, "/guild/openings") || !strcmp(path, "/guild/openings/")) {
        if (post) { sqlite3_close(db); ph_error(405, "Apply from the guild's own offers page.\n"); return 1; }
        gr_openings(db, actor); sqlite3_close(db); return 1;
    }
    char key[97] = "", rest[160] = "";
    const char *k = path + 7, *slash = strchr(k, '/');
    size_t n = slash ? (size_t)(slash - k) : strlen(k);
    if (!n || n >= sizeof(key)) { sqlite3_close(db); return 0; }
    memcpy(key, k, n); key[n] = 0;
    if (slash) snprintf(rest, sizeof(rest), "%s", slash + 1);
    size_t rl = strlen(rest); if (rl && rest[rl - 1] == '/') rest[rl - 1] = 0;
    CjrCommunity c;
    if (!cjr_key(key) || cjr_community(db, key, &c) != 1 || !cjr_public(&c)) { sqlite3_close(db); return 0; }
    int guild = cjw_is_guild(db, key);
    if (!guild && strcmp(rest, "registry") && strcmp(rest, "act")) { sqlite3_close(db); return 0; }
    if (!strcmp(rest, "act")) {
        if (!post) { sqlite3_close(db); ph_error(405, "Use the guild's forms.\n"); return 1; }
        if (!*actor) { sqlite3_close(db); ph_error(401, "Sign in before changing guild records.\n"); return 1; }
        GrForm p; char back[256];
        if (!ph_origin(raw, host) || !gr_parse(body, &p)) { sqlite3_close(db); ph_error(403, "Submit one form from this guild's pages.\n"); return 1; }
        if (!guild && strcmp(p.op, "observe")) { sqlite3_close(db); ph_error(404, "That is a guild record.\n"); return 1; }
        int code = gr_commit(db, &c, actor, &p, back);
        sqlite3_close(db); return gr_status(code, back);
    }
    if (post) { sqlite3_close(db); ph_error(405, "Open guild pages with GET.\n"); return 1; }
    if (!*rest) gr_curriculum(db, &c, actor);
    else if (!strcmp(rest, "bench")) {
        if (!*actor) { sqlite3_close(db); char next[200]; snprintf(next, sizeof(next), "/guild/%s/bench", key); cjw_signin(next); return 1; }
        if (!gr_active(db, &c, actor)) { sqlite3_close(db); ph_error(403, "Your bench opens once this guild's steward accepts you as a member.\n"); return 1; }
        gr_bench(db, &c, actor);
    } else if (!strncmp(rest, "proof/", 6) && cjr_key(rest + 6)) {
        if (!*actor) { sqlite3_close(db); char next[300]; snprintf(next, sizeof(next), "/guild/%s/%s", key, rest); cjw_signin(next); return 1; }
        if (!gr_active(db, &c, actor)) { sqlite3_close(db); ph_error(403, "Bench proofs are read by this guild's members.\n"); return 1; }
        gr_proof(db, &c, actor, rest + 6);
    } else if (!strcmp(rest, "attest")) gr_attest(db, &c, actor);
    else if (!strcmp(rest, "offers")) gr_offers(db, &c, actor);
    else if (!strcmp(rest, "registry")) gr_registry(db, &c, actor);
    else { sqlite3_close(db); return 0; }
    sqlite3_close(db);
    return 1;
}
#endif
