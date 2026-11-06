#ifndef BONFYRE_SUBJECT_AND_LENS_H
#define BONFYRE_SUBJECT_AND_LENS_H
/* Extracted verbatim from main.c lines 23600-27582 (mechanical split, no logic changes). */
static size_t subject_conditions(const char *ident, const char *kind,
                                 char *out, size_t cap) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    size_t w = 0;
    int domains = 0;

    if (!ident || !*ident || !out || !cap) return 0;
    out[0] = '\0';
    if (!(db = cms_open_ro())) return 0;
    if (!cms_type_registered(db, "closure_domain")) { sqlite3_close(db); return 0; }

    if (sqlite3_prepare_v2(db,
            "SELECT domain, requirement_type, scope_field, state_field, "
            "       COALESCE(satisfied_state,'SATISFIED') "
            "FROM closure_domain WHERE subject_kind = ? AND status = 'published' "
            "ORDER BY domain", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    sqlite3_bind_text(st, 1, kind, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *domain = (const char *)sqlite3_column_text(st, 0);
        const char *rtype  = (const char *)sqlite3_column_text(st, 1);
        const char *scope  = (const char *)sqlite3_column_text(st, 2);
        const char *sfield = (const char *)sqlite3_column_text(st, 3);
        const char *sat    = (const char *)sqlite3_column_text(st, 4);
        sqlite3_stmt *q = NULL;
        char sql[512], e_domain[128];
        int total = 0, met = 0, unknown = 0;

        if (!domain || !rtype || !scope || !sfield) continue;
        /* Every one of these names a table or a column, so each has to be an
         * identifier the schema could have declared. A declaration is not a
         * place to accept arbitrary text. */
        if (!studio_safe_ident(rtype) || !studio_safe_ident(scope) ||
            !studio_safe_ident(sfield)) continue;

        snprintf(sql, sizeof(sql),
                 "SELECT count(*), "
                 "  sum(CASE WHEN %s = ? THEN 1 ELSE 0 END), "
                 "  sum(CASE WHEN %s = 'UNKNOWN' THEN 1 ELSE 0 END) "
                 "FROM %s WHERE %s = ? AND status = 'published'",
                 sfield, sfield, rtype, scope);
        if (sqlite3_prepare_v2(db, sql, -1, &q, NULL) != SQLITE_OK) continue;
        sqlite3_bind_text(q, 1, sat && *sat ? sat : "SATISFIED", -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(q, 2, ident, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(q) == SQLITE_ROW) {
            total   = sqlite3_column_int(q, 0);
            met     = sqlite3_column_int(q, 1);
            unknown = sqlite3_column_int(q, 2);
        }
        sqlite3_finalize(q);
        if (!total) continue;

        html_escape(domain, e_domain, sizeof(e_domain));
        size_t before = w;
        if (!domains && !bf_bounded_appendf(out, cap, &w, sizeof("</ul>") - 1,
                "<p class=\"subj-note\">Waiting on</p><ul class=\"subj-addrs\">")) break;
        /* Unknown is reported beside unmet and never folded into it: nobody
         * asked is a different answer from blocked, and collapsing the two
         * reports a wall where there is only silence. */
        if (!bf_bounded_appendf(out, cap, &w, sizeof("</ul>") - 1,
             "<li><span class=\"mono\">%s</span> &mdash; %d of %d closed, "
             "%d blocked, %d nobody has asked about "
             "<a href=\"/closure?domain=%s&amp;subject=%.90s\">what would unlock it</a></li>",
             e_domain, met, total, total - met - unknown, unknown,
             e_domain, ident)) {
            if (!domains) { w = before; out[w] = '\0'; }
            break;
        }
        domains++;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    if (domains) (void)bf_bounded_appendf(out, cap, &w, 0, "</ul>");
    return w;
}

/* Every subject the matrix holds, addressed.
 *
 * One indexed row and three counts -- the same read /subjects already does per
 * row -- plus the shortest address as a label, because a subject id is a
 * digest for most kinds and "budgetrequest:01135de0" is not something a person
 * can read. Then the ways in and the relations, capped, because a door is a
 * door and not the whole building.
 *
 * Deliberately a shell. The lenses that fill a subject out -- its world, its
 * discussion, its map, its offers -- are separate reads at their own
 * addresses. A canonical URL that reconstructs everything before it can say
 * its own name is the URL nobody links to.
 *
 * Cannot infer: an addressed subject -> a permitted one. This says the estate
 * holds it and where it is projected, never that this caller may act on it.
 */
static int subject_generic(const char *kind, const char *ident,
                           int signed_in, int may_write, const char *bearer,
                           const char *auth_db) {
    static char body[16384];
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char facts[4096], label[256] = {0}, real_kind[96] = {0};
    char e_id[512], e_kind[192], e_label[512];
    Action acts[8];
    int n = 0, naddr = 0, npres = 0, nrel = 0, found = 0, withheld_ops = 0;
    size_t w = 0;

    {
        const char *mp = getenv("BONFYRE_MATRIX_DB");
        if (!mp || !*mp) mp = "/var/lib/bonfyre/matrix.db";
        if (sqlite3_open_v2(mp, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
            if (db) sqlite3_close(db);
            return 0;
        }
    }
    if (sqlite3_prepare_v2(db,
            "SELECT s.subject_kind, "
            "  (SELECT count(*) FROM matrix_addresses a WHERE a.subject_id = s.subject_id), "
            "  (SELECT count(*) FROM matrix_presences p WHERE p.subject_id = s.subject_id "
            "     AND p.active != 0), "
            "  (SELECT count(*) FROM matrix_subject_relations r "
            "     WHERE r.from_subject = s.subject_id OR r.to_subject = s.subject_id), "
            "  (SELECT a.address FROM matrix_addresses a WHERE a.subject_id = s.subject_id "
            "     ORDER BY length(a.address), a.address LIMIT 1) "
            "FROM matrix_subjects s WHERE s.subject_id = ? LIMIT 1",
            -1, &st, NULL) != SQLITE_OK) { sqlite3_close(db); return 0; }
    sqlite3_bind_text(st, 1, ident, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *k = (const char *)sqlite3_column_text(st, 0);
        const char *a = (const char *)sqlite3_column_text(st, 4);
        if (k) snprintf(real_kind, sizeof(real_kind), "%s", k);
        naddr = sqlite3_column_int(st, 1);
        npres = sqlite3_column_int(st, 2);
        nrel  = sqlite3_column_int(st, 3);
        if (a) snprintf(label, sizeof(label), "%s", a);
        found = 1;
    }
    sqlite3_finalize(st); st = NULL;
    if (!found) { sqlite3_close(db); return 0; }

    /* The kind in the address has to be the kind the matrix holds. Answering
     * for the wrong one would make one subject reachable at many addresses,
     * and the canonical address stops being canonical. */
    if (kind && *kind && strcmp(kind, real_kind)) { sqlite3_close(db); return 0; }

    /* Resolve what can be done before writing what it is.
     *
     * The withheld count belongs in the fixed part of the facts: it was
     * appended after the address list, and a subject with fifty-three ways in
     * filled the buffer first -- so the one subject that most needed to say
     * "operations exist here and you are not being offered them" was the one
     * that silently did not. A refusal must never be the thing that overflows. */
    n = aperture_offer_authored(ident, acts, n, 8, bearer, auth_db, &withheld_ops);

    html_escape(ident, e_id, sizeof(e_id));
    html_escape(real_kind, e_kind, sizeof(e_kind));
    html_escape(label[0] ? label : ident, e_label, sizeof(e_label));

    facts[0] = '\0';
    if (!bf_bounded_appendf(facts, sizeof(facts), &w, 0,
        "<dl class=\"flow-facts\">"
        "<div><dt>Kind</dt><dd>%s</dd></div>"
        "<div><dt>Identifier</dt><dd><span class=\"mono\">%s</span></dd></div>"
        "<div><dt>Ways in</dt><dd>%d</dd></div>"
        "<div><dt>Projected into</dt><dd>%d</dd></div>"
        "<div><dt>Related subjects</dt><dd>%d</dd></div>%s</dl>",
        e_kind, e_id, naddr, npres, nrel,
        withheld_ops ? "<div><dt>Withheld</dt><dd>operations this session is "
                       "not offered</dd></div>" : "")) {
        sqlite3_close(db);
        return 0;
    }

    if (withheld_ops && !bf_bounded_appendf(facts, sizeof(facts), &w, 0,
                 "<p class=\"surface-denied\">%d further operation(s) exist on this "
                 "subject and are not offered to this session.</p>", withheld_ops)) {
        sqlite3_close(db);
        return 0;
    }

    /* The ways in, which is the thing a canonical address is for. */
    if (naddr && sqlite3_prepare_v2(db,
            "SELECT address FROM matrix_addresses WHERE subject_id = ? "
            "ORDER BY length(address), address LIMIT 12", -1, &st, NULL) == SQLITE_OK) {
        int first = 1;
        sqlite3_bind_text(st, 1, ident, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *a = (const char *)sqlite3_column_text(st, 0);
            char ea[400];
            size_t before = w;
            if (!a) continue;
            if (first && !bf_bounded_appendf(facts, sizeof(facts), &w,
                    sizeof("</ul>") - 1,
                    "<p class=\"subj-note\">Addressed at</p><ul class=\"subj-addrs\">")) break;
            html_escape(a, ea, sizeof(ea));
            if (!bf_bounded_appendf(facts, sizeof(facts), &w,
                    sizeof("</ul>") - 1,
                    "<li><span class=\"mono\">%s</span></li>", ea)) {
                if (first) { w = before; facts[w] = '\0'; }
                break;
            }
            first = 0;
        }
        if (!first) (void)bf_bounded_appendf(facts, sizeof(facts), &w, 0, "</ul>");
        sqlite3_finalize(st); st = NULL;
    }
    sqlite3_close(db);

    /* What it is waiting on, if any domain declares conditions for its kind.
     * A subject that may be acted on and a subject that is blocked are
     * different states, and the address should say which. */
    w += subject_conditions(ident, real_kind, facts + w, sizeof(facts) - w);

    /* Offered, never committed: every one of these is a read. */
    {
        /* Sized to the field it is copied into: the host compiler is right
         * that a 320-byte href cannot fit an Action's 256. */
        char href[200];
        snprintf(href, sizeof(href), "/subjects?id=%.180s", ident);
        n = aperture_push(acts, n, 8, "See it as a machine does",
                          "the same subject, as the index answers it", 3, 0, href);
    }
    if (npres)
        n = aperture_push(acts, n, 8, "Where it is projected",
                          "one subject, several territories", 2, 0, "/console");
    if (!signed_in)
        n = aperture_push(acts, n, 8, "Sign in to see your side of it",
                          "what is public is public; the rest needs a session",
                          1, 0, "/login");

    aperture_render(body, sizeof(body), "Subject &middot; one thing, addressed",
                    ident, e_label, facts, acts, n, signed_in, may_write, bearer);
    ambient_render(body, sizeof(body), strlen(body), "subject", ident);
    aperture_close(body, sizeof(body));
    flow_reply(body);
    return 1;
}

/* Percent-encode a value for a query string; defined below. */
static void url_escape_into(const char *in, char *out, size_t cap);
/* Does this fieldname name somebody in a household?
 *
 * Only as a whole underscore-separated segment, and only for a name declared in
 * schemas/control/operators.cue. The honest result across the nine compiled
 * applications is that almost nothing does, which is the contract holding
 * rather than a gap. */
static int field_reaches_household(const char *fieldname) {
    const char *seg = fieldname;
    while (seg && *seg) {
        const char *end = strchr(seg, '_');
        size_t len = end ? (size_t)(end - seg) : strlen(seg);
        size_t i;
        for (i = 0; i < BF_HOUSEHOLD_REACH_COUNT; i++) {
            const char *name = bf_household_reach[i].name;
            if (strlen(name) == len && !strncmp(seg, name, len)) return 1;
        }
        seg = end ? end + 1 : NULL;
    }
    return 0;
}


/* One compiled metric answered from the records this estate keeps of its
 * kind. The compiler owns the read: it opens the store read only, binds every
 * value, and refuses a field the table does not hold or a column the store
 * keeps for its own use. This only says the answer. Returns 1 when a number
 * was said, 0 when the reader refused or could not be run. */
static int metric_answer(const char *cms, const char *table, const char *definition,
                         const char *company, char *out, size_t cap) {
    static char reply[65536];
    const char *bin = getenv("BONFYRE_FRAPPE_COMPILER");
    char param[320];
    char *argv[12];
    int n = 0, said = 0;
    bf_json_doc_t *doc;
    const bf_json_node_t *root, *node;
    char status[16] = "";
    size_t w = 0;
    out[0] = '\0';
    argv[n++] = (char *)(bin && *bin ? bin : "bonfyre-frappe-compiler");
    argv[n++] = "--read-metric"; argv[n++] = "-";
    argv[n++] = "--store"; argv[n++] = (char *)cms;
    argv[n++] = "--table"; argv[n++] = (char *)table;
    argv[n++] = "--not-carried"; argv[n++] = "id,namespace,created_at,updated_at,published_at,status";
    if (company && *company) {
        snprintf(param, sizeof(param), "Company=%s", company);
        argv[n++] = "--param"; argv[n++] = param;
    }
    argv[n] = NULL;
    /* A refusal is a reply too, and the reader exits non-zero for one; what
     * distinguishes it from a reader that could not run is that it answered. */
    if (auth_call(argv, definition, reply, sizeof(reply)) < 0 || reply[0] != '{') return 0;
    if (!(doc = bf_json_parse_str(reply, NULL, 0))) return 0;
    root = bf_json_root(doc);
    if ((node = bf_json_obj_get(doc, root, "status"))) bf_json_get_str_copy(node, status, sizeof(status));
    if (!strcmp(status, "read")) {
        const bf_json_node_t *list;
        if ((node = bf_json_obj_get(doc, root, "value"))) {
            snprintf(out, cap, "%.10g", bf_json_get_double(node));
            said = 1;
        } else if ((list = bf_json_obj_get(doc, root, "rows"))) {
            /* Largest first, as the reader returns them; the first few. */
            for (int i = 0; i < 6 && (node = bf_json_arr_get(doc, list, i)) && w + 200 < cap; ++i) {
                char label[120] = "", elabel[160];
                const bf_json_node_t *l = bf_json_obj_get(doc, node, "label"), *v = bf_json_obj_get(doc, node, "value");
                if (l && l->type == BF_JSON_STRING) bf_json_get_str_copy(l, label, sizeof(label));
                html_escape(label[0] ? label : "not said", elabel, sizeof(elabel));
                w += (size_t)snprintf(out + w, cap - w, "%s%s %.10g", i ? ", " : "", elabel, v ? bf_json_get_double(v) : 0.0);
            }
            if (!w) snprintf(out, cap, "none");
            said = 1;
        } else if ((list = bf_json_obj_get(doc, root, "periods"))) {
            /* The latest period the reader counted, named by its last day. */
            const bf_json_node_t *last = NULL;
            for (int i = 0; (node = bf_json_arr_get(doc, list, i)); ++i) last = node;
            if (last) {
                char until[16] = "";
                const bf_json_node_t *u = bf_json_obj_get(doc, last, "until"), *v = bf_json_obj_get(doc, last, "value");
                if (u) bf_json_get_str_copy(u, until, sizeof(until));
                snprintf(out, cap, "%.10g in the period ending %s", v ? bf_json_get_double(v) : 0.0, until);
                said = 1;
            }
        }
    } else if ((node = bf_json_obj_get(doc, root, "not_carried"))) {
        /* Refused for a field: name it, so the reason is the reader's own. */
        char field[96] = "", efield[128];
        bf_json_get_str_copy(node, field, sizeof(field));
        for (char *c = field; *c; ++c) if (*c == '_') *c = ' ';
        html_escape(field, efield, sizeof(efield));
        if (efield[0]) snprintf(out, cap, "%s", efield);
    }
    bf_json_free(doc);
    return said;
}

/* Readable price words for the What things cost room. */
static const char *price_kind_label(const char *kind) {
    static const struct { const char *kind, *label; } labels[] = {
        {"medicine","Medicines"},{"catalogued","Listed in Wikidata"},{"consumer","Shop prices"},{"award","Government awards"},
        {"compute","Cloud computing"},{"inference","AI models"},{"commodity","Commodities"}};
    for (size_t i = 0; kind && i < sizeof(labels) / sizeof(labels[0]); i++) if (!strcmp(kind, labels[i].kind)) return labels[i].label;
    return kind && *kind ? kind : "Other";
}
/* Wikidata names a currency by its item. Q199 is the number one: a figure with
 * no currency at all, which is not a price anyone can compare. */
static const char *price_currency(const char *code) {
    static const struct { const char *q, *iso; } map[] = {
        {"Q4917","USD"},{"Q4916","EUR"},{"Q8146","JPY"},{"Q25224","GBP"},{"Q80524","INR"},{"Q25417","DKK"},{"Q41588","BRL"},
        {"Q1104069","CAD"},{"Q122922","SEK"},{"Q132643","NOK"},{"Q25344","CHF"},{"Q259502","AUD"},{"Q41044","RUB"},{"Q39099","CNY"}};
    if (!code || !*code) return NULL;
    if (code[0] != 'Q') return strlen(code) == 3 ? code : NULL;
    for (size_t i = 0; i < sizeof(map) / sizeof(map[0]); i++) if (!strcmp(code, map[i].q)) return map[i].iso;
    return NULL;
}
static const char *price_unit_words(const char *unit) {
    if (!unit || !*unit || !strcmp(unit, "stated") || !strcmp(unit, "item") || !strcmp(unit, "award")) return "";
    if (!strcmp(unit, "unit")) return "each";
    if (!strcmp(unit, "token in")) return "per million tokens read";
    if (!strcmp(unit, "token out")) return "per million tokens written";
    if (!strcmp(unit, "1 Hour")) return "an hour";
    if (!strcmp(unit, "1/Month")) return "a month";
    if (!strcmp(unit, "1/Day")) return "a day";
    if (!strcmp(unit, "1 GB/Month")) return "per GB a month";
    if (!strcmp(unit, "1 GB")) return "per GB";
    return unit;
}
/* Enough digits to tell two small prices apart, and no false precision on big ones. */
static void price_amount(double v, char *out, size_t cap) {
    double a = v < 0 ? -v : v;
    if (a >= 1000) {
        char raw[48], *dot; size_t len, i, w = 0; int lead;
        snprintf(raw, sizeof(raw), "%.0f", v); dot = raw + strlen(raw); len = (size_t)(dot - raw);
        lead = raw[0] == '-';
        for (i = 0; i < len && w + 2 < cap; i++) {
            out[w++] = raw[i];
            if (i >= (size_t)lead && i + 1 < len && (len - i - 1) % 3 == 0 && raw[i] != '-') out[w++] = ',';
        }
        out[w] = 0;
    } else if (a >= 1) snprintf(out, cap, "%.2f", v);
    else if (a >= 0.01) snprintf(out, cap, "%.3f", v);
    else if (a == 0) snprintf(out, cap, "0");
    else snprintf(out, cap, "%.3g", v);
}

static int serve_subject(const char *method, const char *target, const char *query,
                         const char *bearer, const char *auth_db)
{
    (void)query;   /* a subject addresses itself in the path, not a parameter */
    static char body[262144], rows[131072];
    const char *bindir = getenv("BONFYRE_BIN_DIR");
    char kind[32], ident[160], raw[192];
    Action acts[8];
    int n = 0, signed_in, may_write;

    if (strncmp(target, "/s/", 3)) return 0;
    if (strcmp(method, "GET")) return 0;
    if (!bindir || !*bindir) bindir = "/usr/local/bin";
    signed_in = session_valid(bearer, auth_db);
    may_write = signed_in && session_authorized(bearer, auth_db, "write");

    /* "/s/<kind>/<identifier>" -- the subject addresses itself. */
    {
        const char *p = target + 3, *slash = strchr(p, '/');
        if (!slash) { subject_miss("No subject named.</p>"); return 1; }
        if ((size_t)(slash - p) >= sizeof(kind)) { subject_miss("Unknown subject.</p>"); return 1; }
        memcpy(kind, p, (size_t)(slash - p)); kind[slash - p] = '\0';
        snprintf(raw, sizeof(raw), "%s", slash + 1);
        if (!subject_ident(raw, ident, sizeof(ident))) {
            subject_miss("Unknown subject.</p>"); return 1;
        }
    }
/* Retired: this was a host/name heuristic, not a Foundry-derived relation.
 * It remains below only as review context until the typed ExperienceMap / owner
 * projection path replaces it; it must not compile or serve. */
#if 0
    /* The public learning aperture is deliberately a reader over the whole
     * admitted frontier, not an application that asks a district to populate
     * an empty account.  It finds the institutions this estate can name from
     * their public host/name and offers each institution's exact public
     * material room.  Host/name classification is useful discovery evidence,
     * never a claim that an institution offers a particular class, accepts a
     * child, has a vacancy, or has agreed to work with this estate. */
    if (!strcmp(kind, "learning") && !strcmp(ident, "opportunities")) {
        const char *fpath = getenv("BONFYRE_SOURCE_FRONTIER_DB");
        sqlite3 *fr = NULL;
        sqlite3_stmt *st = NULL;
        char uri[PATH_MAX], facts[131072];
        size_t fw = 0;
        long roots = 0, addresses = 0;
        int categories = 0;
        static const char *education_where =
          "(lower(r.host) LIKE '%school%' OR lower(r.name) LIKE '%school%'"
          " OR lower(r.host) LIKE '%.edu' OR lower(r.host) LIKE '%library%'"
          " OR lower(r.name) LIKE '%library%' OR lower(r.host) LIKE '%museum%'"
          " OR lower(r.name) LIKE '%museum%' OR lower(r.host) LIKE '%park%'"
          " OR lower(r.name) LIKE '%park%' OR lower(r.host) LIKE '%college%'"
          " OR lower(r.name) LIKE '%college%' OR lower(r.host) LIKE '%university%'"
          " OR lower(r.name) LIKE '%university%')";

        if (!fpath || !*fpath ||
            bf_sqlite_readonly_uri(uri, sizeof(uri), fpath) >= (int)sizeof(uri) ||
            sqlite3_open_v2(uri, &fr, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) != SQLITE_OK) {
            if (fr) sqlite3_close(fr);
            subject_miss("No admitted public source frontier is readable here.");
            return 1;
        }
        {
            char sql[2048];
            snprintf(sql, sizeof(sql),
              "SELECT COUNT(DISTINCT r.root_id), COUNT(c.channel_id) FROM root r"
              " JOIN channel c ON c.root_id=r.root_id"
              " WHERE c.state IN ('admitted','active') AND %s", education_where);
            if (sqlite3_prepare_v2(fr, sql, -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) {
                roots = (long)sqlite3_column_int64(st, 0);
                addresses = (long)sqlite3_column_int64(st, 1);
            }
            if (st) { sqlite3_finalize(st); st = NULL; }
        }
        if (!roots) {
            sqlite3_close(fr);
            subject_miss("The frontier currently holds no admitted public learning-facing institution.");
            return 1;
        }
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<dl class=\"flow-facts\"><div><dt>Institutions named</dt><dd>%ld</dd></div>"
          "<div><dt>Public addresses held</dt><dd>%ld</dd></div></dl>"
          "<h3 class=\"room-h\">Learning material already public</h3>"
          "<div data-bf-list data-bf-row-height=\"48\">", roots, addresses);
        {
            char sql[4096];
            snprintf(sql, sizeof(sql),
              "SELECT r.host, COALESCE(NULLIF(r.name,''),r.host),"
              " CASE WHEN lower(r.host) LIKE '%%school%%' OR lower(r.name) LIKE '%%school%%'"
              " OR lower(r.host) LIKE '%%.edu' THEN 'school or learning institution'"
              " WHEN lower(r.host) LIKE '%%library%%' OR lower(r.name) LIKE '%%library%%' THEN 'library'"
              " WHEN lower(r.host) LIKE '%%museum%%' OR lower(r.name) LIKE '%%museum%%' THEN 'museum'"
              " WHEN lower(r.host) LIKE '%%park%%' OR lower(r.name) LIKE '%%park%%' THEN 'park or nature place'"
              " ELSE 'college or university' END, COUNT(c.channel_id)"
              " FROM root r JOIN channel c ON c.root_id=r.root_id"
              " WHERE c.state IN ('admitted','active') AND %s"
              " GROUP BY r.root_id ORDER BY 3, 2, r.host", education_where);
            if (sqlite3_prepare_v2(fr, sql, -1, &st, NULL) == SQLITE_OK) {
                while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 2048) {
                    const char *host = (const char *)sqlite3_column_text(st, 0);
                    const char *name = (const char *)sqlite3_column_text(st, 1);
                    const char *category = (const char *)sqlite3_column_text(st, 2);
                    char eh[512], en[768], ec[128];
                    if (!host || !*host) continue;
                    html_escape(host, eh, sizeof(eh));
                    html_escape(name ? name : host, en, sizeof(en));
                    html_escape(category ? category : "public institution", ec, sizeof(ec));
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<a class=\"desk-row\" href=\"/s/institution/%s\">"
                      "<span class=\"desk-l\">%s</span><span class=\"desk-a\">%s</span>"
                      "<span class=\"desk-s\">%lld admitted public address%s</span></a>",
                      eh, en, ec, (long long)sqlite3_column_int64(st, 3),
                      sqlite3_column_int64(st, 3) == 1 ? "" : "es");
                    categories++;
                }
            }
            if (st) { sqlite3_finalize(st); st = NULL; }
        }
        sqlite3_close(fr);
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "</div>");
        /* Public material can become the context for real, already-published
         * collective work without pretending that an observed institution has
         * joined it.  Projects retain their own support, team and governance
         * owners; this is a downstream reading edge only. */
        {
            sqlite3 *cdb = cms_open_ro();
            sqlite3_stmt *pst = NULL;
            int projects = 0;
            if (cdb && sqlite3_prepare_v2(cdb,
                  "SELECT project_id,title,pitch,stage FROM project WHERE status='published'"
                  " AND (lower(title) LIKE '%library%' OR lower(title) LIKE '%apprentice%'"
                  " OR lower(title) LIKE '%education%' OR lower(pitch) LIKE '%education%'"
                  " OR lower(project_id) LIKE '%library%' OR lower(project_id) LIKE '%apprentice%')"
                  " ORDER BY title", -1, &pst, NULL) == SQLITE_OK) {
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                  "<h3 class=\"room-h\">Learning-related work already asking for people</h3>");
                while (sqlite3_step(pst) == SQLITE_ROW && fw < sizeof(facts) - 2048) {
                    const char *pid = (const char *)sqlite3_column_text(pst, 0);
                    const char *title = (const char *)sqlite3_column_text(pst, 1);
                    const char *pitch = (const char *)sqlite3_column_text(pst, 2);
                    const char *stage = (const char *)sqlite3_column_text(pst, 3);
                    char ei[256], et[512], ep[1024], es[80];
                    if (!pid || !*pid) continue;
                    html_escape(pid, ei, sizeof(ei)); html_escape(title ? title : pid, et, sizeof(et));
                    html_escape(pitch ? pitch : "", ep, sizeof(ep)); html_escape(stage ? stage : "", es, sizeof(es));
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<a class=\"desk-row\" href=\"/s/project/%s\"><span class=\"desk-l\">%s</span>"
                      "<span class=\"desk-a\">%s</span><span class=\"desk-s\">%s</span></a>", ei, et, es, ep);
                    projects++;
                }
            }
            if (pst) sqlite3_finalize(pst);
            if (cdb) sqlite3_close(cdb);
            if (!projects) fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<p class=\"subj-note\">No published project currently names learning, library or apprenticeship work. "
              "A source being useful for learning does not create a community or a project by itself.</p>");
        }
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<p class=\"subj-note\">This is the public lower bound: %d admitted institution%s "
          "whose published names or hosts place them near learning. Opening one reads its held public "
          "calendars, feeds, documents, data and pages. It does not infer a program, availability, "
          "eligibility, a learner relationship or institutional consent from the name.</p>",
          categories, categories == 1 ? "" : "s");
        n = aperture_push(acts, n, 8, "Read the whole public source frontier",
                          "every admitted publisher, beyond learning", 1, 0, "/sources");
        if (signed_in)
            n = aperture_push(acts, n, 8, "Connect a household relationship",
                              "only after reading one institution's public world", 2, 0, "/account");
        else
            n = aperture_push(acts, n, 8, "Keep a private continuity",
                              "sign in only when saving or establishing a relationship is useful", 1, 0, "/login?next=/learn");
        aperture_render(body, sizeof(body), "Learning &middot; public material before adoption",
                        ident, "A distributed public campus already held by the estate", facts,
                        acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "learning", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

#endif
    /* A parcel: a thing in the world, with a market and work around it. */
    if (!strcmp(kind, "parcel")) {
        char sql[2048], addr[256]={0}, owner[256]={0}, city[64]={0}, tv[64]={0}, lv[64]={0};
        char facts[2048], e_addr[512], e_owner[512], href[256];
        snprintf(sql, sizeof(sql),
          "SELECT json_extract_string(observed,'$.Situs_Addr') AS address, "
          "json_extract_string(observed,'$.Owner_Name') AS owner, "
          "json_extract_string(observed,'$.City') AS city, "
          "TRY_CAST(json_extract_string(observed,'$.Land_Value') AS DOUBLE) AS land_value, "
          "TRY_CAST(json_extract_string(observed,'$.Total_Valu') AS DOUBLE) AS total_value "
          "FROM observation WHERE src_id='src:tad-parcel-view' "
          "AND trim(json_extract_string(observed,'$.Account_Nu'))='%s' LIMIT 1", ident);
        if (fleming_query(bindir, sql, rows, sizeof(rows)) != 0 || rows[0] != '[' || strlen(rows) < 4) {
            subject_miss("No parcel in the observed roll carries that account.");
            return 1;
        }
        studio_json_field(rows, "address", addr, sizeof(addr));
        studio_json_field(rows, "owner", owner, sizeof(owner));
        studio_json_field(rows, "city", city, sizeof(city));
        studio_json_field(rows, "land_value", lv, sizeof(lv)); num_fix(lv, sizeof(lv));
        studio_json_field(rows, "total_value", tv, sizeof(tv)); num_fix(tv, sizeof(tv));
        html_escape(addr, e_addr, sizeof(e_addr));
        html_escape(owner, e_owner, sizeof(e_owner));
        snprintf(facts, sizeof(facts),
          "<dl class=\"flow-facts\">"
          "<div><dt>Account</dt><dd class=\"mono\">%s</dd></div>"
          "<div><dt>Taxpayer of record</dt><dd>%s</dd></div>"
          "<div><dt>Land value</dt><dd>$%s</dd></div>"
          "<div><dt>Total assessed</dt><dd>$%s</dd></div></dl>",
          ident, e_owner[0]?e_owner:"&mdash;", lv[0]?lv:"&mdash;", tv[0]?tv:"&mdash;");

        /* The aperture. Strength is closure, not decoration. */
        snprintf(href, sizeof(href), "/property/market?city=%s&amp;account=%s", city, ident);
        n = aperture_push(acts, n, 8, "See this market", "115k+ parcels observed around it", 2, 0, href);
        if (may_write) {
            snprintf(href, sizeof(href), "/property?watch=%s", ident);
            n = aperture_push(acts, n, 8, "Watch this parcel",
                              "opens a claimable mission in the work graph", 3, 1, href);
            n = aperture_push(acts, n, 8, "Open an opportunity",
                              "records intent against this account", 2, 1, href);
        } else if (signed_in) {
            n = aperture_push(acts, n, 8, "Ask for write scope",
                              "your account reads this estate but cannot open work", 1, 0, "/account");
        } else {
            n = aperture_push(acts, n, 8, "Sign in to act",
                              "reading is open; opening work is not", 2, 0, "/login");
        }
        n = aperture_push(acts, n, 8, "Read it as RESO", "the same parcel in the real-estate grammar",
                          1, 0, "/reso/Property?limit=25");
        aperture_render(body, sizeof(body), "Parcel &middot; observed public record",
                        ident, e_addr[0]?e_addr:"(no situs address)", facts,
                        acts, n, signed_in, may_write, bearer);
        {
            Trail t[8];
            int tn = parcel_trail(bindir, ident, city, owner,
                                  tv[0] ? atof(tv) : 0.0, t, 8);
            size_t tw = trail_render(body, sizeof(body), strlen(body), t, tn);
            ambient_render(body, sizeof(body), tw, "parcel", ident);
        }
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* A capability: a thing that can be bought. */
    if (!strcmp(kind, "capability")) {
        const X402Capability *cap = x402_find(ident);
        double usd = 0.0;
        int priced;
        char facts[1024], href[256];
        if (!cap) { subject_miss("No such capability.</p>"); return 1; }
        priced = x402_price_usd(cap->name, &usd);
        snprintf(facts, sizeof(facts),
          "<dl class=\"flow-facts\">"
          "<div><dt>Status</dt><dd>%s</dd></div>"
          "<div><dt>Price</dt><dd>%s</dd></div>"
          "<div><dt>Settles</dt><dd>%s</dd></div></dl>"
          "<p class=\"subj-desc\">%s</p>",
          priced ? "listed" : "not for sale",
          priced ? "quoted through five grammars" : "&mdash;",
          x402_facilitator()[0] ? "a facilitator is configured" : "nothing can settle yet",
          cap->summary);
        snprintf(href, sizeof(href), "/market/quote?capability=%s", cap->name);
        n = aperture_push(acts, n, 8, "See what an agent pays", "the same number in every grammar",
                          priced ? 3 : 2, 0, href);
        snprintf(href, sizeof(href), "/capability/%s", cap->name);
        n = aperture_push(acts, n, 8, priced ? "Try to buy it" : "See it refuse",
                          priced ? "answers 402 until a payment verifies"
                                 : "answers not_for_sale, which is not free", 2, 0, href);
        if (may_write)
            n = aperture_push(acts, n, 8, priced ? "Change what it costs" : "Put it up for sale",
                              "writes to the service book the quotes read", 3, 1, "/market");
        else if (!signed_in)
            n = aperture_push(acts, n, 8, "Sign in to price it", "pricing needs write scope", 1, 0, "/login");
        aperture_render(body, sizeof(body), "Capability &middot; priced through one book",
                        ident, cap->name, facts, acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "capability", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* A published note: a thing to read, and maybe to edit. */
    if (!strcmp(kind, "note")) {
        sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL;
        char title[512]={0}, status[64]={0}; int guest = 0, id = 0;
        char facts[1024], href[256], e_title[1024];
        char note_namespace[192] = {0};
        (void)cms_namespace_for(bearer, auth_db, note_namespace, sizeof(note_namespace));
        if (db && sqlite3_prepare_v2(db,
              "SELECT id,title,status,allow_guest FROM wiki_page WHERE route=?1 AND (namespace=?2 OR (namespace='root' AND ?3) OR (status='published' AND allow_guest=1)) ORDER BY (namespace=?2) DESC,id DESC LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
            char route[256]; snprintf(route, sizeof(route), "notes/%s", ident);
            sqlite3_bind_text(st, 1, route, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 2, note_namespace, -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 3, signed_in);
            if (sqlite3_step(st) == SQLITE_ROW) {
                id = sqlite3_column_int(st, 0);
                snprintf(title, sizeof(title), "%s", (const char *)sqlite3_column_text(st, 1));
                snprintf(status, sizeof(status), "%s", sqlite3_column_text(st,2) ? (const char*)sqlite3_column_text(st,2) : "draft");
                guest = sqlite3_column_int(st, 3);
            }
        }
        if (st) sqlite3_finalize(st);
        if (db) sqlite3_close(db);
        if (!id) { subject_miss("No note at that route.</p>"); return 1; }
        html_escape(title, e_title, sizeof(e_title));
        snprintf(facts, sizeof(facts),
          "<dl class=\"flow-facts\"><div><dt>Status</dt><dd>%s</dd></div>"
          "<div><dt>Readable by</dt><dd>%s</dd></div></dl>",
          status, guest ? "anyone" : "signed-in accounts only");
        if (!strcmp(status,"published") && guest) {
            snprintf(href, sizeof(href), "/notes/%s", ident);
            n = aperture_push(acts, n, 8, "Read it", "published and open to everyone", 3, 0, href);
        }
        if (may_write) {
            snprintf(href, sizeof(href), "/cms/#%d", id);
            n = aperture_push(acts, n, 8, "Edit it", "changes what readers see", 2, 1, href);
            n = aperture_push(acts, n, 8,
                              !strcmp(status,"published") ? "Unpublish it" : "Publish it",
                              "publishing and readability are separate", 3, 1, "/cms/");
        }
        aperture_render(body, sizeof(body), "Note &middot; addressed content",
                        ident, e_title[0]?e_title:ident, facts, acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "note", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* A retained public event has a stable local occurrence coordinate.  The
     * source remains the organizer's page; this room only exposes what was
     * retained and the person's own local follow. */
    if (!strcmp(kind, "publication_occurrence")) {
        sqlite3 *odb = cms_open_ro(); sqlite3_stmt *ost = NULL;
        char title[768] = {0}, summary[4096] = {0}, href[1024] = {0}, when[96] = {0}, source[256] = {0};
        char etitle[1536], esummary[8192], ewhen[192], esource[512], occurrence_href[512], login_href[512], facts[12288];
        if (odb && cms_type_registered(odb, "publication_occurrence") &&
            sqlite3_prepare_v2(odb,
              "SELECT title,COALESCE(summary,''),COALESCE(canonical_href,''),COALESCE(published_at,''),COALESCE(source_ref,'') "
              "FROM publication_occurrence WHERE occurrence_id=?1 AND status='published' LIMIT 1",
              -1, &ost, NULL) == SQLITE_OK) {
            sqlite3_bind_text(ost, 1, ident, -1, SQLITE_STATIC);
            if (sqlite3_step(ost) == SQLITE_ROW) {
                snprintf(title, sizeof(title), "%s", sqlite3_column_text(ost, 0) ? (const char *)sqlite3_column_text(ost, 0) : "");
                snprintf(summary, sizeof(summary), "%s", sqlite3_column_text(ost, 1) ? (const char *)sqlite3_column_text(ost, 1) : "");
                snprintf(href, sizeof(href), "%s", sqlite3_column_text(ost, 2) ? (const char *)sqlite3_column_text(ost, 2) : "");
                snprintf(when, sizeof(when), "%s", sqlite3_column_text(ost, 3) ? (const char *)sqlite3_column_text(ost, 3) : "");
                snprintf(source, sizeof(source), "%s", sqlite3_column_text(ost, 4) ? (const char *)sqlite3_column_text(ost, 4) : "");
            }
        }
        if (ost) sqlite3_finalize(ost);
        if (odb) sqlite3_close(odb);
        if (!title[0]) { subject_miss("No published occurrence carries that identifier."); return 1; }
        html_escape(title, etitle, sizeof(etitle));
        html_escape(summary, esummary, sizeof(esummary));
        html_escape(when, ewhen, sizeof(ewhen));
        html_escape(source, esource, sizeof(esource));
        snprintf(occurrence_href, sizeof(occurrence_href), "/_aperture/form?subject=publication_occurrence:%s&operation=Occurrence.follow", ident);
        snprintf(login_href, sizeof(login_href), "/login?next=/s/publication_occurrence/%s", ident);
        snprintf(facts, sizeof(facts),
          "<dl class=\"flow-facts\"><div><dt>Observed event time</dt><dd>%s</dd></div>"
          "<div><dt>Retained from</dt><dd>%s</dd></div></dl><p class=\"subj-desc\">%s</p>"
          "<p class=\"subj-note\">This is retained public event material. Following it is private to this estate; it does not RSVP, register, notify, or establish attendance.</p>",
          ewhen[0] ? ewhen : "not stated", esource[0] ? esource : "retained public source", esummary);
        if (public_http_url(href)) n = aperture_push(acts, n, 8, "Open the organizer's public page", "the retained source remains external", 2, 0, href);
        n = aperture_push(acts, n, 8, signed_in ? "Follow this occurrence" : "Sign in to follow this occurrence",
                          "records only your private local reference", 3, 0,
                          signed_in ? occurrence_href : login_href);
        aperture_render(body, sizeof(body), "Occurrence &middot; retained public event material",
                        ident, etitle, facts, acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "publication_occurrence", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    if (!strcmp(kind, "household_opportunity")) {
        sqlite3 *odb = cms_open_ro(); sqlite3_stmt *ost = NULL;
        char title[768] = {0}, description[4096] = {0}, href[1024] = {0}, services[1024] = {0};
        char etitle[1536], edescription[8192], eservices[2048], form_href[512], login_href[512], facts[12288];
        if (odb && cms_type_registered(odb, "household_opportunity") &&
            sqlite3_prepare_v2(odb,
              "SELECT title,COALESCE(description,''),canonical_href,service_kinds FROM household_opportunity "
              "WHERE opportunity_id=?1 AND status='published' LIMIT 1", -1, &ost, NULL) == SQLITE_OK) {
            sqlite3_bind_text(ost, 1, ident, -1, SQLITE_STATIC);
            if (sqlite3_step(ost) == SQLITE_ROW) {
                snprintf(title, sizeof(title), "%s", sqlite3_column_text(ost, 0) ? (const char *)sqlite3_column_text(ost, 0) : "");
                snprintf(description, sizeof(description), "%s", sqlite3_column_text(ost, 1) ? (const char *)sqlite3_column_text(ost, 1) : "");
                snprintf(href, sizeof(href), "%s", sqlite3_column_text(ost, 2) ? (const char *)sqlite3_column_text(ost, 2) : "");
                snprintf(services, sizeof(services), "%s", sqlite3_column_text(ost, 3) ? (const char *)sqlite3_column_text(ost, 3) : "");
            }
        }
        if (ost) sqlite3_finalize(ost);
        if (odb) sqlite3_close(odb);
        if (!title[0]) { subject_miss("No published household opportunity carries that identifier."); return 1; }
        html_escape(title, etitle, sizeof(etitle)); html_escape(description, edescription, sizeof(edescription));
        html_escape(services, eservices, sizeof(eservices));
        snprintf(form_href, sizeof(form_href), "/_aperture/form?subject=household_opportunity:%s&operation=HouseholdOpportunity.save", ident);
        snprintf(login_href, sizeof(login_href), "/login?next=/s/household_opportunity/%s", ident);
        snprintf(facts, sizeof(facts),
          "<dl class=\"flow-facts\"><div><dt>Public service classifications</dt><dd>%s</dd></div></dl>"
          "<p class=\"subj-desc\">%s</p><p class=\"subj-note\">This is a public service opportunity observed from 211. It does not establish eligibility, application, enrollment, referral, notice, or delivery.</p>",
          eservices, edescription);
        if (public_http_url(href)) n = aperture_push(acts, n, 8, "Open the public provider page", "the provider still owns the service path", 2, 0, href);
        n = aperture_push(acts, n, 8, signed_in ? "Save this opportunity" : "Sign in to save this opportunity",
                          "keeps a private local reference only", 3, 0, signed_in ? form_href : login_href);
        aperture_render(body, sizeof(body), "Household opportunity &middot; observed public service",
                        ident, etitle, facts, acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "household_opportunity", ident);
        aperture_close(body, sizeof(body)); flow_reply(body); return 1;
    }

    /* A publisher: an internet thing with a presence, whether or not anyone
     * has claimed it.
     *
     * The estate holds twenty-nine thousand source roots and not one had an
     * address a person could open. They were an ingestion inventory -- rows an
     * operator inspects -- when each is a thing in the world that publishes,
     * that this estate reads, and that it knows something about. A room is
     * composed from what is actually there; a section with nothing behind it
     * is not drawn. */
    if (!strcmp(kind, "publisher")) {
        sqlite3 *fr = NULL, *ar = NULL; sqlite3_stmt *st = NULL;
        char host[192] = {0}, name[256] = {0}, e_name[512];
        /* facts carries the settlement band too, and snprintf truncates in silence. */
        char facts[6144], kinds[768] = {0}, href[512], eh[256], settle[3072] = {0};
        long kept = 0, first_at = 0, last_at = 0;
        const char *fpath = getenv("BONFYRE_SOURCE_FRONTIER_DB");
        char uri[PATH_MAX];

        if (fpath && *fpath && bf_sqlite_readonly_uri(uri, sizeof(uri), fpath) < (int)sizeof(uri) &&
            sqlite3_open_v2(uri, &fr, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) == SQLITE_OK &&
            sqlite3_prepare_v2(fr, "SELECT host, COALESCE(NULLIF(name,''),host) FROM root"
                                   " WHERE host = ?1 LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, ident, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                snprintf(host, sizeof(host), "%s", (const char *)sqlite3_column_text(st, 0));
                snprintf(name, sizeof(name), "%s", (const char *)sqlite3_column_text(st, 1));
            }
            sqlite3_finalize(st); st = NULL;
        }
        if (fr) sqlite3_close(fr);
        if (!host[0]) { subject_miss("No source root this estate has admitted carries that host."); return 1; }

        /* The archive files a thing under the publisher its feed credited --
         * the registrable name -- while a root is keyed by the host that served
         * it, so the tail is what matches. */
        if ((ar = page_archive_open(0))) {
            if (sqlite3_prepare_v2(ar,
                  "SELECT COUNT(*), COALESCE(MIN(NULLIF(at,0)),0), COALESCE(MAX(at),0) FROM item"
                  " WHERE publisher = ?1 OR ?1 LIKE '%.' || publisher", -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, host, -1, SQLITE_STATIC);
                if (sqlite3_step(st) == SQLITE_ROW) {
                    kept = (long)sqlite3_column_int64(st, 0);
                    first_at = (long)sqlite3_column_int64(st, 1);
                    last_at = (long)sqlite3_column_int64(st, 2);
                }
                sqlite3_finalize(st); st = NULL;
            }
            if (sqlite3_prepare_v2(ar,
                  "SELECT kind, COUNT(*) FROM item"
                  " WHERE publisher = ?1 OR ?1 LIKE '%.' || publisher"
                  " GROUP BY kind ORDER BY 2 DESC LIMIT 5", -1, &st, NULL) == SQLITE_OK) {
                size_t kw = 0;
                sqlite3_bind_text(st, 1, host, -1, SQLITE_STATIC);
                while (sqlite3_step(st) == SQLITE_ROW && kw < sizeof(kinds) - 96) {
                    char ek[64]; html_escape((const char *)sqlite3_column_text(st, 0), ek, sizeof(ek));
                    kw += (size_t)snprintf(kinds + kw, sizeof(kinds) - kw,
                        "<div><dt>%s</dt><dd>%lld</dd></div>", ek,
                        (long long)sqlite3_column_int64(st, 1));
                }
                sqlite3_finalize(st); st = NULL;
            }
            sqlite3_close(ar);
        }

        {
            /* matrix_institutions keys a publisher as inst:<host with dashes>. */
            char inst[224]; size_t q;
            snprintf(inst, sizeof(inst), "inst:%s", host);
            for (q = 5; inst[q]; q++) if (inst[q] == '.') inst[q] = '-';
            room_settlement(settle, sizeof(settle), inst);
        }
        {
            char span[96] = "";
            if (first_at && last_at) {
                struct tm tf, tl; char fb[24], lb[24];
                time_t f = (time_t)first_at, l = (time_t)last_at;
                page_local_time(f, &tf); page_local_time(l, &tl);
                strftime(fb, sizeof(fb), "%Y-%m-%d", &tf);
                strftime(lb, sizeof(lb), "%Y-%m-%d", &tl);
                snprintf(span, sizeof(span), "<div><dt>Watched</dt><dd>%s to %s</dd></div>", fb, lb);
            }
            snprintf(facts, sizeof(facts),
              "<dl class=\"flow-facts\"><div><dt>Kept</dt><dd>%ld</dd></div>%s%s</dl>"
              "<p class=\"subj-note\">These are things this estate read and kept from it. The count "
              "is what reached here rather than what the publisher published, and the span is how "
              "long this estate has been watching rather than how long the thing has existed. "
              "Nobody has claimed this presence.</p>%s", kept, kinds, span, settle);
        }

        html_escape(name, e_name, sizeof(e_name));
        url_escape_into(host, eh, sizeof(eh));
        snprintf(href, sizeof(href), "/archive?pub=%s", eh);
        n = aperture_push(acts, n, 8, "Read what was kept from it", "in the reading room", 3, 0, href);
        {
            char href2[512];
            snprintf(href2, sizeof(href2), "/press?of=%s", eh);
            n = aperture_push(acts, n, 8, "Set a page on it", "and see who else carries the same stories", 2, 0, href2);
        }
        n = aperture_push(acts, n, 8, "See how densely it files", "against every other source", 1, 0, "/sources");
        aperture_render(body, sizeof(body), "Publisher &middot; a thing on the internet that publishes",
                        ident, e_name, facts, acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "publisher", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* A program: something a subject is trying to run, and what it still needs.
     *
     * This is the pitch, and the estate's own vocabulary is what keeps it
     * honest. A requirement is an obligation recorded, never one met. A work
     * package is a declared scope kept distinct from other streams so one real
     * effort supported several ways can be told apart from one cost claimed
     * several times -- and never a claim that money was awarded, applied for,
     * billed, paid, booked or authorized. Nothing here can become a pledge by
     * being looked at.
     */
    if (!strcmp(kind, "program")) {
        sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL;
        char arch[128] = {0}, owner[160] = {0}, opened[64] = {0}, site[128] = {0};
        char e_arch[256], e_owner[320], needs[4096] = {0}, money[3072] = {0};
        /* facts carries the needs, the scopes and the settlement band. */
        char facts[12288], href[512], eid[256], settle[3072] = {0};
        int open_needs = 0, met_needs = 0, packages = 0;

        if (db && sqlite3_prepare_v2(db,
              "SELECT archetype, owner_subject, COALESCE(opened_at,''), COALESCE(site_id,'')"
              " FROM program_instance WHERE instance_id = ?1 AND status='published' LIMIT 1",
              -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, ident, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                snprintf(arch, sizeof(arch), "%s", (const char *)sqlite3_column_text(st, 0));
                snprintf(owner, sizeof(owner), "%s", (const char *)sqlite3_column_text(st, 1));
                snprintf(opened, sizeof(opened), "%s", (const char *)sqlite3_column_text(st, 2));
                snprintf(site, sizeof(site), "%s", (const char *)sqlite3_column_text(st, 3));
            }
            sqlite3_finalize(st); st = NULL;
        }
        if (!arch[0]) {
            if (db) sqlite3_close(db);
            subject_miss("No published program instance carries that identifier.");
            return 1;
        }

        /* What it still needs, read through the grammar rather than out of the
         * obligation table. A proposal here was derived from a requirement the
         * program fabric already held: binding it to the verb states no new
         * fact, it names the thing in the words a member reads. The backing is
         * carried so nobody has to guess what would decide it. */
        if (sqlite3_prepare_v2(db,
              "SELECT asks, state, backing FROM governance_proposal"
              " WHERE subject_ref = ?1 AND status='published'"
              " ORDER BY CASE state WHEN 'open' THEN 0 ELSE 1 END, proposal_id LIMIT 14",
              -1, &st, NULL) == SQLITE_OK) {
            size_t nw = 0;
            sqlite3_bind_text(st, 1, ident, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW && nw < sizeof(needs) - 640) {
                const char *asks = (const char *)sqlite3_column_text(st, 0);
                const char *state = (const char *)sqlite3_column_text(st, 1);
                const char *backing = (const char *)sqlite3_column_text(st, 2);
                char es[1024], eb[128];
                int is_open = state && !strcmp(state, "open");
                if (is_open) open_needs++; else met_needs++;
                html_escape(asks ? asks : "", es, sizeof(es));
                html_escape(backing ? backing : "", eb, sizeof(eb));
                nw += (size_t)snprintf(needs + nw, sizeof(needs) - nw,
                    "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
                    "<span class=\"desk-a\">%s</span>"
                    "<span class=\"desk-s\">Proposal &middot; would be decided by %s</span></div>",
                    es, is_open ? "open" : "settled", eb);
            }
            sqlite3_finalize(st); st = NULL;
        }

        /* What could pay for it, kept as distinct scopes. */
        if (sqlite3_prepare_v2(db,
              "SELECT funding_program, COALESCE(scope,''), COALESCE(distinctness_rule,'')"
              " FROM funding_work_package WHERE instance_id = ?1 AND status='published' LIMIT 8",
              -1, &st, NULL) == SQLITE_OK) {
            size_t mw = 0;
            sqlite3_bind_text(st, 1, ident, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW && mw < sizeof(money) - 512) {
                char ep[256], er[768];
                html_escape((const char *)sqlite3_column_text(st, 0), ep, sizeof(ep));
                html_escape((const char *)sqlite3_column_text(st, 2), er, sizeof(er));
                packages++;
                mw += (size_t)snprintf(money + mw, sizeof(money) - mw,
                    "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
                    "<span class=\"desk-a\">a declared scope</span>"
                    "<span class=\"desk-s\">%s</span></div>", ep, er);
            }
            sqlite3_finalize(st); st = NULL;
        }
        sqlite3_close(db);

        {
            /* person:nick stands as inst:person-nick when it stands at all. */
            char inst[224]; size_t q;
            snprintf(inst, sizeof(inst), "inst:%s", owner);
            for (q = 5; inst[q]; q++) if (inst[q] == ':' || inst[q] == '.') inst[q] = '-';
            room_settlement(settle, sizeof(settle), inst);
        }
        html_escape(arch, e_arch, sizeof(e_arch));
        html_escape(owner, e_owner, sizeof(e_owner));
        snprintf(facts, sizeof(facts),
          "<dl class=\"flow-facts\">"
          "<div><dt>Run by</dt><dd>%s</dd></div>"
          "<div><dt>Still needed</dt><dd>%d</dd></div>"
          "<div><dt>Already met</dt><dd>%d</dd></div>"
          "<div><dt>Funding scopes</dt><dd>%d</dd></div>"
          "%s%s%s</dl>"
          "%s%s%s"
          "%s%s%s"
          "<p class=\"subj-note\">This is what a subject is trying to run and what it has not got "
          "yet. An open requirement is an obligation recorded, never one met, and never authority "
          "to meet it. A funding scope is kept distinct from the others so one real effort "
          "supported several ways can be told apart from one cost claimed several times: it is "
          "not a claim that money was awarded, applied for, billed, paid, booked or authorized, "
          "and nothing on this page can become a pledge by being read.</p>%s",
          e_owner, open_needs, met_needs, packages,
          opened[0] ? "<div><dt>Opened</dt><dd>" : "", opened[0] ? opened : "", opened[0] ? "</dd></div>" : "",
          needs[0] ? "<h3 class=\"room-h\">What is being asked</h3>" : "", needs, needs[0] ? "" : "",
          money[0] ? "<h3 class=\"room-h\">What could pay for it</h3>" : "", money, money[0] ? "" : "",
          settle);

        url_escape_into(ident, eid, sizeof(eid));
        if (site[0]) {
            char es[192]; url_escape_into(site, es, sizeof(es));
            snprintf(href, sizeof(href), "/w/%s", es);
            n = aperture_push(acts, n, 8, "See the world it runs in", "where its material is composed", 2, 0, href);
        }
        if (may_write)
            n = aperture_push(acts, n, 8, "Record a requirement as met",
                              "an obligation is closed by evidence, not by intent", 1, 1, "/cms/");
        else
            n = aperture_push(acts, n, 8, "Sign in to record evidence",
                              "closing a requirement needs write scope", 1, 0, "/login");
        aperture_render(body, sizeof(body), "Program &middot; something a subject is trying to run",
                        ident, e_arch, facts, acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "program", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* The governance grammar this estate publishes.
     *
     * Not a dashboard. The fourteen verbs a community sees when it decides
     * something, each printed with the sentence that bounds it, and the
     * backings that may carry any of them -- only three of which can produce
     * an effect outside this estate, and those still gated by the world the
     * institution declares.
     *
     * A community chooses a backing per verb, so a group can take its votes in
     * a database and its treasury through a Safe without either fact reaching
     * the page a member reads. */
    if (!strcmp(kind, "governance") && !strcmp(ident, "grammar")) {
        char facts[16384]; size_t fw = 0; size_t i;
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">What a community sees</h3>");
        for (i = 0; i < BF_GOVERNANCE_VERB_COUNT && fw < sizeof(facts) - 1024; i++) {
            char el[256], ec[1024];
            html_escape(bf_governance_verbs[i].label, el, sizeof(el));
            html_escape(bf_governance_verbs[i].cannot_infer, ec, sizeof(ec));
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
              "<span class=\"desk-a\">%s</span>"
              "<span class=\"desk-s\">%s</span></div>",
              el, bf_governance_verbs[i].name, ec);
        }
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">What can carry it</h3>");
        for (i = 0; i < BF_GOVERNANCE_BACKING_COUNT && fw < sizeof(facts) - 1024; i++) {
            char eb[128], ec[1024];
            html_escape(bf_governance_backings[i].backing, eb, sizeof(eb));
            html_escape(bf_governance_backings[i].cannot_infer, ec, sizeof(ec));
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
              "<span class=\"desk-a\">%s</span>"
              "<span class=\"desk-s\">%s</span></div>",
              eb, bf_governance_backings[i].settles ? "can settle" : "cannot settle", ec);
        }
        {
            /* What is actually bound. A grammar that names nothing is a
             * vocabulary; the count is what makes it a governance layer. */
            sqlite3 *gdb = cms_open_ro(); sqlite3_stmt *gs = NULL;
            long props = 0, opens = 0, decs = 0;
            if (gdb) {
                if (sqlite3_prepare_v2(gdb, "SELECT COUNT(*), SUM(state='open')"
                        " FROM governance_proposal WHERE status='published'", -1, &gs, NULL) == SQLITE_OK) {
                    if (sqlite3_step(gs) == SQLITE_ROW) {
                        props = (long)sqlite3_column_int64(gs, 0);
                        opens = (long)sqlite3_column_int64(gs, 1);
                    }
                    sqlite3_finalize(gs); gs = NULL;
                }
                if (sqlite3_prepare_v2(gdb, "SELECT COUNT(*) FROM governance_decision"
                        " WHERE status='published'", -1, &gs, NULL) == SQLITE_OK) {
                    if (sqlite3_step(gs) == SQLITE_ROW) decs = (long)sqlite3_column_int64(gs, 0);
                    sqlite3_finalize(gs); gs = NULL;
                }
                sqlite3_close(gdb);
            }
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<h3 class=\"room-h\">What is bound to it</h3>"
              "<dl class=\"flow-facts\">"
              "<div><dt>Proposals</dt><dd>%ld</dd></div>"
              "<div><dt>Still open</dt><dd>%ld</dd></div>"
              "<div><dt>Decisions</dt><dd>%ld</dd></div>"
              "<div><dt>Votes cast</dt><dd>0</dd></div></dl>"
              "<p class=\"subj-note\">Every proposal here was derived from an obligation a program "
              "already held, so binding it to the verb states no new fact. No vote has been cast "
              "against any of them and the count says zero rather than leaving the reader to "
              "assume: a decision recorded as met was recorded by the program fabric on evidence, "
              "not by anyone voting.</p>", props, opens, decs);
        }
        snprintf(facts + fw, sizeof(facts) - fw,
          "<p class=\"subj-note\">Most governance in this estate is not blockchain governance. "
          "A parent-association vote, a moderator appointment, a household spending decision and "
          "a community funding proposal need no wallet, and making them carry one would be the "
          "protocol leaking into the vocabulary. Human approval is the first backing because it "
          "is the honest default: most decisions are made by a person who is allowed to make "
          "them.</p>");
        n = aperture_push(acts, n, 8, "See a program that needs decisions",
                          "the pitch these verbs would be used on", 1, 0, "/subjects");
        aperture_render(body, sizeof(body),
                        "Governance &middot; verbs a person already knows",
                        ident, "The grammar this estate publishes", facts,
                        acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "governance", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* A project: everything one effort needs, in one room.
     *
     * Kickstarter before it starts, a supporter relationship while it runs, an
     * open ledger for what it costs, recruiting for the people it lacks,
     * bounties for the discrete work, and the decisions it has to take -- as
     * facets of one thing rather than six products. The target is a stage in a
     * life that continues, not an ending.
     *
     * Every act on this page is a form this estate can actually carry out, and
     * each says what it is not: no money moves here, and a name against a role
     * is willingness rather than engagement. */
    if (!strcmp(kind, "project")) {
        sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL;
        char title[256] = {0}, pitch[2048] = {0}, stage[64] = {0}, subject[192] = {0};
        char e_title[512], e_pitch[4096], facts[24576]; size_t fw = 0;
        long supporters = 0, joiners = 0, votes = 0;
        char eid[288];

        if (db && sqlite3_prepare_v2(db,
              "SELECT title, pitch, stage, subject_ref FROM project"
              " WHERE project_id = ?1 AND status='published' LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, ident, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                snprintf(title, sizeof(title), "%s", (const char *)sqlite3_column_text(st, 0));
                snprintf(pitch, sizeof(pitch), "%s", (const char *)sqlite3_column_text(st, 1));
                snprintf(stage, sizeof(stage), "%s", (const char *)sqlite3_column_text(st, 2));
                snprintf(subject, sizeof(subject), "%s", (const char *)sqlite3_column_text(st, 3));
            }
            sqlite3_finalize(st); st = NULL;
        }
        if (!title[0]) { if (db) sqlite3_close(db); subject_miss("No published project carries that identifier."); return 1; }

        url_escape_into(ident, eid, sizeof(eid));
        html_escape(title, e_title, sizeof(e_title));
        html_escape(pitch, e_pitch, sizeof(e_pitch));

        /* Who has already put something in, counted rather than claimed. */
        if (sqlite3_prepare_v2(db, "SELECT SUM(mode='stated'), SUM(mode='joining')"
              " FROM project_support WHERE project_id=?1 AND status='published'", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, ident, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                supporters = (long)sqlite3_column_int64(st, 0);
                joiners = (long)sqlite3_column_int64(st, 1);
            }
            sqlite3_finalize(st); st = NULL;
        }
        if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM governance_vote v"
              " JOIN governance_proposal p ON p.proposal_id=v.proposal_id"
              " WHERE p.subject_ref=?1 AND v.status='published'", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, subject, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) votes = (long)sqlite3_column_int64(st, 0);
            sqlite3_finalize(st); st = NULL;
        }

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<p class=\"room-read\">%s</p>"
          "<dl class=\"flow-facts\">"
          "<div><dt>Stage</dt><dd>%s</dd></div>"
          "<div><dt>Stated support</dt><dd>%ld</dd></div>"
          "<div><dt>Offered to help</dt><dd>%ld</dd></div>"
          "<div><dt>Votes cast</dt><dd>%ld</dd></div></dl>",
          e_pitch, stage, supporters, joiners, votes);

        /* Say where you stand. The tally is of people, and one person holds
         * one position: changing it replaces the old one. */
        if (sqlite3_prepare_v2(db,
              "SELECT p.proposal_id, p.asks, p.backing,"
              " (SELECT COUNT(*) FROM governance_vote v WHERE v.proposal_id=p.proposal_id AND v.choice='for' AND v.status='published'),"
              " (SELECT COUNT(*) FROM governance_vote v WHERE v.proposal_id=p.proposal_id AND v.choice='against' AND v.status='published'),"
              " (SELECT COUNT(*) FROM governance_vote v WHERE v.proposal_id=p.proposal_id AND v.choice='abstain' AND v.status='published')"
              " FROM governance_proposal p WHERE p.subject_ref=?1 AND p.state='open' AND p.status='published'"
              " ORDER BY p.proposal_id LIMIT 4", -1, &st, NULL) == SQLITE_OK) {
            int shown = 0;
            sqlite3_bind_text(st, 1, subject, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 3072) {
                char epid[288], easks[1024], eb[96];
                long f = (long)sqlite3_column_int64(st, 3), a = (long)sqlite3_column_int64(st, 4),
                     ab = (long)sqlite3_column_int64(st, 5);
                url_escape_into((const char *)sqlite3_column_text(st, 0), epid, sizeof(epid));
                html_escape((const char *)sqlite3_column_text(st, 1), easks, sizeof(easks));
                html_escape((const char *)sqlite3_column_text(st, 2), eb, sizeof(eb));
                if (!shown++) fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                    "<h3 class=\"room-h\">Needs a decision</h3>");
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                  "<div class=\"gv\"><p class=\"gv-asks\">%s</p>"
                  "<p class=\"gv-tally\">%ld for &middot; %ld against &middot; %ld abstaining &middot; "
                  "would be decided by %s</p>"
                  "<form class=\"gv-form\" method=\"post\" action=\"/govern/vote\">"
                  "<input type=\"hidden\" name=\"id\" value=\"%s\">"
                  "<input type=\"hidden\" name=\"return\" value=\"/s/project/%s\">"
                  "<button name=\"choice\" value=\"for\">For</button>"
                  "<button name=\"choice\" value=\"against\">Against</button>"
                  "<button name=\"choice\" value=\"abstain\">Abstain</button>"
                  "</form></div>", easks, f, a, ab, eb, epid, eid);
            }
            sqlite3_finalize(st); st = NULL;
        }

        /* The people it lacks. */
        if (sqlite3_prepare_v2(db,
              "SELECT need_id, role, wanted FROM team_need"
              " WHERE project_id=?1 AND status='published' AND filled='0' LIMIT 6",
              -1, &st, NULL) == SQLITE_OK) {
            int shown = 0;
            sqlite3_bind_text(st, 1, ident, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 2048) {
                char er[256], enid[288];
                html_escape((const char *)sqlite3_column_text(st, 1), er, sizeof(er));
                url_escape_into((const char *)sqlite3_column_text(st, 0), enid, sizeof(enid));
                if (!shown++) fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                    "<h3 class=\"room-h\">Help wanted</h3>");
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                  "<div class=\"gv gv-row\"><span class=\"gv-role\">%s</span>"
                  "<span class=\"gv-want\">%s wanted</span>"
                  "<form class=\"gv-form\" method=\"post\" action=\"/govern/join\">"
                  "<input type=\"hidden\" name=\"id\" value=\"%s\">"
                  "<input type=\"hidden\" name=\"return\" value=\"/s/project/%s\">"
                  "<button>Put my name to it</button></form></div>",
                  er, (const char *)sqlite3_column_text(st, 2), enid, eid);
            }
            sqlite3_finalize(st); st = NULL;
        }

        /* Discrete work, and what the project says it is worth. */
        if (sqlite3_prepare_v2(db,
              "SELECT task, amount_stated, state FROM bounty"
              " WHERE project_id=?1 AND status='published' LIMIT 6", -1, &st, NULL) == SQLITE_OK) {
            int shown = 0;
            sqlite3_bind_text(st, 1, ident, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 1536) {
                char et[1024]; const char *amt = (const char *)sqlite3_column_text(st, 1);
                html_escape((const char *)sqlite3_column_text(st, 0), et, sizeof(et));
                if (!shown++) fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                    "<h3 class=\"room-h\">Bounties</h3>");
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                  "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
                  "<span class=\"desk-a\">%s</span></div>",
                  et, (amt && *amt) ? amt : "unpriced");
            }
            sqlite3_finalize(st); st = NULL;
        }
        sqlite3_close(db);

        /* What has actually been promised, read off the rail that owns
         * promises rather than off the content row that renders them. The
         * state is the graph's, and nothing here discharges anything: delivery
         * needs an explicit event and no click is one. */
        {
            const char *cpath = getenv("BONFYRE_COMMITMENT_DB");
            sqlite3 *cdb = NULL; sqlite3_stmt *cs = NULL; char uri[PATH_MAX];
            int shown = 0;
            if (cpath && *cpath &&
                bf_sqlite_readonly_uri(uri, sizeof(uri), cpath) < (int)sizeof(uri) &&
                sqlite3_open_v2(uri, &cdb, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) == SQLITE_OK &&
                sqlite3_prepare_v2(cdb,
                  "SELECT c.committer, c.resource,"
                  " (SELECT e.state FROM commitment_events e WHERE e.commitment_id=c.commitment_id"
                  "   ORDER BY e.rowid DESC LIMIT 1)"
                  " FROM commitments c WHERE c.beneficiary = ?1 LIMIT 8",
                  -1, &cs, NULL) == SQLITE_OK) {
                sqlite3_bind_text(cs, 1, ident, -1, SQLITE_STATIC);
                while (sqlite3_step(cs) == SQLITE_ROW && fw < sizeof(facts) - 1536) {
                    char ecm[128], ers[512];
                    const char *state = (const char *)sqlite3_column_text(cs, 2);
                    html_escape((const char *)sqlite3_column_text(cs, 0), ecm, sizeof(ecm));
                    html_escape((const char *)sqlite3_column_text(cs, 1), ers, sizeof(ers));
                    if (!shown++) fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                        "<h3 class=\"room-h\">What has been promised</h3>");
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
                      "<span class=\"desk-a\">%s</span>"
                      "<span class=\"desk-s\">%s</span></div>",
                      ers, state ? state : "open", ecm);
                }
                sqlite3_finalize(cs); cs = NULL;
            }
            if (cdb) sqlite3_close(cdb);
            if (shown) fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<p class=\"subj-note\">These are promises on this estate's commitment graph, where a "
              "promise is never a delivery and only an explicit discharge event says otherwise. "
              "Every one of them is open.</p>");
        }

        /* Whether this estate has any basis for pricing what the project is
         * asking for.
         *
         * The observatory holds what publishers say things cost. None of it is
         * about this project, and matching a bounty to a market rate would be
         * a judgement nobody has made. What it can honestly say is whether a
         * basis exists at all: how much observed evidence there is in the
         * currency this project deals in, and that the unpriced work here is
         * unpriced because nobody has priced it, not because it is free. */
        {
            const char *ppath = getenv("BONFYRE_PRICES_DB");
            sqlite3 *pdb = NULL; sqlite3_stmt *ps = NULL; char puri[PATH_MAX];
            long observed = 0, pubs = 0, unpriced = 0;
            sqlite3 *cdb2 = cms_open_ro();
            if (cdb2) {
                if (sqlite3_prepare_v2(cdb2,
                      "SELECT COUNT(*) FROM bounty WHERE project_id=?1 AND status='published'"
                      " AND (amount_stated='' OR amount_stated IS NULL)", -1, &ps, NULL) == SQLITE_OK) {
                    sqlite3_bind_text(ps, 1, ident, -1, SQLITE_STATIC);
                    if (sqlite3_step(ps) == SQLITE_ROW) unpriced = (long)sqlite3_column_int64(ps, 0);
                    sqlite3_finalize(ps); ps = NULL;
                }
                sqlite3_close(cdb2);
            }
            if (ppath && *ppath &&
                bf_sqlite_readonly_uri(puri, sizeof(puri), ppath) < (int)sizeof(puri) &&
                sqlite3_open_v2(puri, &pdb, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) == SQLITE_OK) {
                if (sqlite3_prepare_v2(pdb,
                      "SELECT COUNT(*), COUNT(DISTINCT publisher) FROM observed_price"
                      " WHERE currency='USD'", -1, &ps, NULL) == SQLITE_OK &&
                    sqlite3_step(ps) == SQLITE_ROW) {
                    observed = (long)sqlite3_column_int64(ps, 0);
                    pubs = (long)sqlite3_column_int64(ps, 1);
                }
                if (ps) { sqlite3_finalize(ps); ps = NULL; }
                sqlite3_close(pdb);
            }
            if (observed || unpriced)
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                  "<h3 class=\"room-h\">Whether any of this can be priced</h3>"
                  "<dl class=\"flow-facts\">"
                  "<div><dt>Unpriced work here</dt><dd>%ld</dd></div>"
                  "<div><dt>Prices observed in USD</dt><dd>%ld</dd></div>"
                  "<div><dt>From publishers</dt><dd>%ld</dd></div></dl>"
                  "<p class=\"subj-note\">This estate holds %ld observed USD prices from %ld "
                  "publishers, and not one of them is about this project. Matching a piece of work "
                  "here to a market rate would be a judgement nobody has made, so none is offered. "
                  "The work shown as unpriced is unpriced because nobody has priced it, never "
                  "because it is free.</p>", unpriced, observed, pubs, observed, pubs);
        }

        /* Support, stated before anything moves. */
        snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">Support it</h3>"
          "<form class=\"gv-form gv-fund\" method=\"post\" action=\"/govern/support\">"
          "<input type=\"hidden\" name=\"id\" value=\"%s\">"
          "<input type=\"hidden\" name=\"return\" value=\"/s/project/%s\">"
          "<input name=\"amount\" inputmode=\"decimal\" placeholder=\"amount you would put in\">"
          "<button>State my support</button></form>"
          "<p class=\"subj-note\">Stating support records an intention and a relationship. No money "
          "moves here, this estate cannot collect it, and nothing recorded is evidence that value "
          "moved: money movement belongs to a payment rail and returns a receipt reference, which "
          "is a digest of something presented rather than proof of settlement. A name against a "
          "role is willingness, not an engagement or a contract. A tally is what was said by those "
          "who said it, and one person holds one position at a time.</p>", eid, eid);

        {
            char href[512];
            snprintf(href, sizeof(href), "/s/program/%s", subject);
            n = aperture_push(acts, n, 8, "See the program behind it",
                              "the obligations these proposals came from", 2, 0, href);
        }
        n = aperture_push(acts, n, 8, "See the grammar it is governed in",
                          "the verbs and what can carry them", 1, 0, "/s/governance/grammar");
        aperture_render(body, sizeof(body), "Project &middot; one effort, every facet at once",
                        ident, e_title, facts, acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "project", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* Who this is for, and what one effort is made of.
     *
     * Ten populations of the same product, and six faces of the same project.
     * The archetypes are here because "bring the thing you already know how to
     * operate" is a different offer from "create an account and start
     * posting", and the difference is worth writing down where a person can
     * read it. The facets are here because a campaign, a supporter
     * relationship, an open ledger, a bounty board, a recruiting page and a
     * set of decisions are one object seen from six sides, not six products. */
    if (!strcmp(kind, "operators") && !strcmp(ident, "archetypes")) {
        char facts[24576]; size_t fw = 0; size_t i;
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">Who already knows how to run one</h3>");
        for (i = 0; i < BF_OPERATOR_ARCHETYPE_COUNT && fw < sizeof(facts) - 2048; i++) {
            char el[256], eh[768], eu[768];
            html_escape(bf_operator_archetypes[i].label, el, sizeof(el));
            html_escape(bf_operator_archetypes[i].has, eh, sizeof(eh));
            html_escape(bf_operator_archetypes[i].unlocks, eu, sizeof(eu));
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
              "<span class=\"desk-a\">already has</span>"
              "<span class=\"desk-s\">%s &mdash; unlocks: %s</span></div>", el, eh, eu);
        }
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">What one project is made of</h3>");
        for (i = 0; i < BF_PROJECT_FACET_COUNT && fw < sizeof(facts) - 2048; i++) {
            char el[256], es[768], ec[1024];
            html_escape(bf_project_facets[i].label, el, sizeof(el));
            html_escape(bf_project_facets[i].studied, es, sizeof(es));
            html_escape(bf_project_facets[i].cannot_infer, ec, sizeof(ec));
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
              "<span class=\"desk-a\">%s</span>"
              "<span class=\"desk-s\">studied from %s &middot; %s</span></div>",
              el, bf_project_facets[i].facet, es, ec);
        }
        {
            /* How many of these faces actually have something behind them. */
            sqlite3 *pdb = cms_open_ro(); sqlite3_stmt *ps = NULL;
            long projects = 0, needs = 0, bounties = 0, supports = 0;
            if (pdb) {
                if (sqlite3_prepare_v2(pdb,
                      "SELECT (SELECT COUNT(*) FROM project WHERE status='published'),"
                      " (SELECT COUNT(*) FROM team_need WHERE status='published'),"
                      " (SELECT COUNT(*) FROM bounty WHERE status='published'),"
                      " (SELECT COUNT(*) FROM project_support WHERE status='published')",
                      -1, &ps, NULL) == SQLITE_OK && sqlite3_step(ps) == SQLITE_ROW) {
                    projects = (long)sqlite3_column_int64(ps, 0);
                    needs = (long)sqlite3_column_int64(ps, 1);
                    bounties = (long)sqlite3_column_int64(ps, 2);
                    supports = (long)sqlite3_column_int64(ps, 3);
                }
                if (ps) sqlite3_finalize(ps);
                sqlite3_close(pdb);
            }
            snprintf(facts + fw, sizeof(facts) - fw,
              "<h3 class=\"room-h\">What is standing behind it</h3>"
              "<dl class=\"flow-facts\">"
              "<div><dt>Projects</dt><dd>%ld</dd></div>"
              "<div><dt>Roles wanted</dt><dd>%ld</dd></div>"
              "<div><dt>Bounties</dt><dd>%ld</dd></div>"
              "<div><dt>Stated support</dt><dd>%ld</dd></div></dl>"
              "<p class=\"subj-note\">Every project here was derived from a program this estate "
              "already held, so none of it is a mock-up. Naming a platform an operator came from "
              "is not a claim that platform knows this estate exists, participates, or has been "
              "approached, and where a grammar was studied is provenance only: no donor code runs "
              "and no donor vocabulary reaches a page.</p>", projects, needs, bounties, supports);
        }
        n = aperture_push(acts, n, 8, "Open a project", "every facet of one effort at once", 3, 0, "/subjects");
        n = aperture_push(acts, n, 8, "See the grammar it decides in", "the verbs and what carries them", 1, 0, "/s/governance/grammar");
        aperture_render(body, sizeof(body), "Operators &middot; ten populations, one product",
                        ident, "Bring the thing you already know how to operate", facts,
                        acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "operators", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* What the rest of the frontier would cost, at rates that were evidenced.
     *
     * Two admitted rates are not a price book, but they are enough to price a
     * hundred thousand things, because the missing part was never the rate --
     * it was the count. This estate knows exactly how many addresses it has
     * not yet read, and it knows what one reading costs because the provider's
     * own document said so. The product of those two is a projection.
     *
     * It is deliberately not a bill and not a plan. Nothing here has been
     * ordered, budgeted or authorized, the rate is what a provider stated on
     * the date recorded rather than one that still holds, and a provider with
     * no admitted rate contributes a count and no figure -- counted and left
     * unpriced, never counted as free.
     */
    if (!strcmp(kind, "frontier") && !strcmp(ident, "cost")) {
        const char *fpath = getenv("BONFYRE_SOURCE_FRONTIER_DB");
        const char *mpath = getenv("BONFYRE_METER_DB");
        sqlite3 *fr = NULL, *me = NULL; sqlite3_stmt *st = NULL; char uri[PATH_MAX];
        char facts[16384]; size_t fw = 0;
        long candidates = 0, admitted = 0, roots = 0;
        int rates = 0;

        if (fpath && *fpath && bf_sqlite_readonly_uri(uri, sizeof(uri), fpath) < (int)sizeof(uri) &&
            sqlite3_open_v2(uri, &fr, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) == SQLITE_OK &&
            sqlite3_prepare_v2(fr,
              "SELECT SUM(state='candidate'), SUM(state IN ('admitted','active')),"
              " (SELECT COUNT(*) FROM root) FROM channel", -1, &st, NULL) == SQLITE_OK &&
            sqlite3_step(st) == SQLITE_ROW) {
            candidates = (long)sqlite3_column_int64(st, 0);
            admitted = (long)sqlite3_column_int64(st, 1);
            roots = (long)sqlite3_column_int64(st, 2);
        }
        if (st) { sqlite3_finalize(st); st = NULL; }
        if (fr) sqlite3_close(fr);
        if (!candidates && !admitted) { subject_miss("No source frontier is readable here."); return 1; }

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<dl class=\"flow-facts\">"
          "<div><dt>Addresses held</dt><dd>%ld</dd></div>"
          "<div><dt>Already read</dt><dd>%ld</dd></div>"
          "<div><dt>Not yet read</dt><dd>%ld</dd></div>"
          "<div><dt>Publishers</dt><dd>%ld</dd></div></dl>"
          "<h3 class=\"room-h\">What reading the rest would cost</h3>",
          candidates + admitted, admitted, candidates, roots);

        /* One line per admitted rate: the provider's own figure, multiplied by
         * the work that is actually outstanding, carrying the document. */
        if (mpath && *mpath && bf_sqlite_readonly_uri(uri, sizeof(uri), mpath) < (int)sizeof(uri) &&
            sqlite3_open_v2(uri, &me, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) == SQLITE_OK) {
            if (sqlite3_prepare_v2(me,
                  "SELECT provider, op, unit, amount, currency, COALESCE(evidence_ref,'')"
                  " FROM rate ORDER BY provider", -1, &st, NULL) == SQLITE_OK) {
                while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 2048) {
                    char ep[96], eo[96], ec[64], eev[640];
                    double unit_cost = sqlite3_column_double(st, 3);
                    html_escape((const char *)sqlite3_column_text(st, 0), ep, sizeof(ep));
                    html_escape((const char *)sqlite3_column_text(st, 1), eo, sizeof(eo));
                    html_escape((const char *)sqlite3_column_text(st, 4), ec, sizeof(ec));
                    html_escape((const char *)sqlite3_column_text(st, 5), eev, sizeof(eev));
                    rates++;
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<div class=\"desk-row\"><span class=\"desk-l\">%s &middot; %s</span>"
                      "<span class=\"desk-a\">%.4f %s</span>"
                      "<span class=\"desk-s\">%.6f %s each &times; %ld unread = %.2f %s. "
                      "Admitted on: %s</span></div>",
                      ep, eo, unit_cost * (double)candidates, ec,
                      unit_cost, ec, candidates, unit_cost * (double)candidates, ec, eev);
                }
                sqlite3_finalize(st); st = NULL;
            }
            /* Work this estate does that no provider has priced. It is counted
             * here so the total is honest about what it leaves out. */
            if (sqlite3_prepare_v2(me,
                  "SELECT COALESCE(NULLIF(provider,''),'(none named)'), op, COUNT(*)"
                  " FROM events e WHERE NOT EXISTS"
                  "   (SELECT 1 FROM rate r WHERE r.provider = e.provider AND r.op = e.op)"
                  " GROUP BY 1,2 ORDER BY 3 DESC LIMIT 6", -1, &st, NULL) == SQLITE_OK) {
                int shown = 0;
                while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 1536) {
                    char ep[96], eo[96];
                    html_escape((const char *)sqlite3_column_text(st, 0), ep, sizeof(ep));
                    html_escape((const char *)sqlite3_column_text(st, 1), eo, sizeof(eo));
                    if (!shown++) fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<h3 class=\"room-h\">Counted, and left unpriced</h3>");
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<div class=\"desk-row\"><span class=\"desk-l\">%s &middot; %s</span>"
                      "<span class=\"desk-a\">%lld done</span>"
                      "<span class=\"desk-s\">no admitted rate, so no figure is put against it</span></div>",
                      ep, eo, (long long)sqlite3_column_int64(st, 2));
                }
                sqlite3_finalize(st); st = NULL;
            }
            sqlite3_close(me);
        }

        snprintf(facts + fw, sizeof(facts) - fw,
          "<p class=\"subj-note\">%d admitted rates price %ld unread addresses, which is the whole "
          "of what this estate has not yet read. The rate is what a provider's own document stated "
          "on the date recorded, not one that still holds; the count is what this estate actually "
          "holds. Their product is a projection and nothing else: no work has been ordered, no "
          "budget set, nothing authorized to spend, and no provider has quoted this estate for "
          "this. Work for which no rate has been admitted is counted above and left unpriced, "
          "never counted as free.</p>", rates, candidates);

        n = aperture_push(acts, n, 8, "See how densely each source files",
                          "what the reading would be buying", 1, 0, "/sources");
        aperture_render(body, sizeof(body), "Frontier &middot; what the rest would cost",
                        ident, "Priced at rates a provider stated about itself", facts,
                        acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "frontier", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* What things cost, according to whoever published the price.
     *
     * This is not the meter and must never be read as one. The meter holds
     * rates for usage this estate performed, in units it can actually apply,
     * and admitting a drug's unit cost or a government award there would
     * corrupt what accrual means. This holds what the world says things cost,
     * each entry carrying the endpoint that said it.
     *
     * Every source has the same property: the publisher states its own price
     * openly and in machine-readable form. Nothing here is scraped from a
     * marketing page, averaged across competitors, or carried forward because
     * it looked stable. A publisher that states no price is absent rather than
     * assumed to be free.
     */
    if (!strcmp(kind, "prices") && !strcmp(ident, "observed")) {
        const char *path = getenv("BONFYRE_PRICES_DB");
        sqlite3 *db = NULL, *db2 = NULL; sqlite3_stmt *st = NULL; char uri[PATH_MAX];
        static char facts[98304]; size_t fw = 0;
        long total = 0, publishers = 0, currencies = 0, sources = 0;
        char want_kind[32] = "", want_q[96] = "", want_page[12] = "";
        long page_offset = 0;
        if (query && *query) { query_value(query, "kind", want_kind, sizeof(want_kind)); query_value(query, "q", want_q, sizeof(want_q)); query_value(query, "offset", want_page, sizeof(want_page)); }
        if (want_page[0] && strspn(want_page, "0123456789") == strlen(want_page)) {
            long parsed = strtol(want_page, NULL, 10);
            if (parsed >= 0 && parsed <= 1000000) page_offset = parsed;
        }
        if (want_kind[0] && strspn(want_kind, "abcdefghijklmnopqrstuvwxyz-") != strlen(want_kind)) want_kind[0] = 0;

        if (!path || !*path) path = "";
        if (!*path || bf_sqlite_readonly_uri(uri, sizeof(uri), path) >= (int)sizeof(uri) ||
            sqlite3_open_v2(uri, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) != SQLITE_OK) {
            if (db) sqlite3_close(db);
            subject_miss("No price observatory is readable here.");
            return 1;
        }
        if (sqlite3_prepare_v2(db,
              "SELECT COUNT(*), COUNT(DISTINCT publisher), COUNT(DISTINCT currency),"
              " COUNT(DISTINCT source) FROM observed_price", -1, &st, NULL) == SQLITE_OK &&
            sqlite3_step(st) == SQLITE_ROW) {
            total = (long)sqlite3_column_int64(st, 0);
            publishers = (long)sqlite3_column_int64(st, 1);
            currencies = (long)sqlite3_column_int64(st, 2);
            sources = (long)sqlite3_column_int64(st, 3);
        }
        if (st) { sqlite3_finalize(st); st = NULL; }

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<section class=\"price-lead\"><p class=\"price-eyebrow\">Published prices &middot; attributed to their sources</p>"
          "<h2>Find out what things cost</h2>"
          "<p>Explore what publishers recorded for products, services and medicines. Search for a name or browse a category. These are dated observations, not live checkout quotes.</p></section>"
          "<h3 class=\"room-h\">Browse prices by category</h3>");

        if (sqlite3_prepare_v2(db,
              "SELECT kind, COUNT(*), COUNT(DISTINCT publisher) FROM observed_price"
              " WHERE currency<>'Q199' AND kind NOT IN('inference','compute') GROUP BY kind ORDER BY COUNT(*) DESC", -1, &st, NULL) == SQLITE_OK) {
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "<nav class=\"price-kinds\" aria-label=\"Kinds of price\"><a href=\"/s/prices/observed\"%s>Everything</a>",
                                   want_kind[0] ? "" : " aria-current=\"page\"");
            while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 2048) {
                const char *k = (const char *)sqlite3_column_text(st, 0); char ek[64];
                html_escape(k, ek, sizeof(ek));
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                  "<a href=\"/s/prices/observed?kind=%s\"%s>%s <small>%lld from %lld publisher%s</small></a>",
                  ek, !strcmp(want_kind, k ? k : "") ? " aria-current=\"page\"" : "", price_kind_label(k),
                  (long long)sqlite3_column_int64(st, 1), (long long)sqlite3_column_int64(st, 2),
                  sqlite3_column_int64(st, 2) == 1 ? "" : "s");
            }
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "</nav><p class=\"subj-note\">What AI models and cloud machines cost is kept with the models, in <a href=\"/s/models/all\">Model Commons</a>.</p>");
            sqlite3_finalize(st); st = NULL;
        }

        /* The list itself, searchable and by kind. A price nobody could read --
         * a figure with no currency, or a currency this page cannot name -- is
         * counted above and left out here rather than printed as a bare number.
         * Model prices are per token at the source and are read per million. */
        {
            char ewq[300]; int shown = 0, has_more = 0, models = !strcmp(want_kind, "inference");
            html_escape(want_q, ewq, sizeof(ewq));
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<form class=\"price-find\" role=\"search\" method=\"get\" action=\"/s/prices/observed\">%s%s%s"
              "<input type=\"search\" name=\"q\" value=\"%s\" placeholder=\"Find a price: a drug, a model, a machine, a product\" aria-label=\"Find a price\">"
              "<button type=\"submit\">Find</button></form>",
              want_kind[0] ? "<input type=\"hidden\" name=\"kind\" value=\"" : "", want_kind[0] ? want_kind : "", want_kind[0] ? "\">" : "", ewq);
            if (models) {
                if (sqlite3_prepare_v2(db,
                      "SELECT item, publisher, MAX(CASE WHEN unit='token in' THEN amount END), MAX(CASE WHEN unit='token out' THEN amount END),"
                      " observed_at, evidence_ref FROM observed_price WHERE kind='inference' AND currency='USD' AND (?1='' OR instr(lower(item),lower(?1))>0)"
                      " GROUP BY item,source,publisher,observed_at,evidence_ref HAVING MAX(CASE WHEN unit='token in' THEN amount END) IS NOT NULL AND COUNT(DISTINCT CASE WHEN unit='token in' THEN amount END)=1 AND COUNT(DISTINCT CASE WHEN unit='token out' THEN amount END)<=1"
                      " ORDER BY MAX(CASE WHEN unit='token in' THEN amount END)+MAX(COALESCE(CASE WHEN unit='token out' THEN amount END,0)), item LIMIT 60",
                      -1, &st, NULL) == SQLITE_OK) {
                    sqlite3_bind_text(st, 1, want_q, -1, SQLITE_STATIC);
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<h3 class=\"room-h\">AI model price observations</h3><p class=\"subj-note\">US dollars per million tokens from the same publisher observation. Listed prices can change.</p>"
                      "<table class=\"price-table\"><thead><tr><th scope=\"col\">Model</th><th scope=\"col\">Reading in</th><th scope=\"col\">Writing out</th><th scope=\"col\">Publisher</th><th scope=\"col\">Read</th></tr></thead><tbody>");
                    while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 4096) {
                        char ei[320], ep[200], in[48], outp[48], day[16];
                        html_escape((const char *)sqlite3_column_text(st, 0), ei, sizeof(ei));
                        html_escape((const char *)sqlite3_column_text(st, 1), ep, sizeof(ep));
                        price_amount(sqlite3_column_double(st, 2) * 1e6, in, sizeof(in));
                        if (sqlite3_column_type(st, 3) == SQLITE_NULL) snprintf(outp, sizeof(outp), "&mdash;");
                        else price_amount(sqlite3_column_double(st, 3) * 1e6, outp, sizeof(outp));
                        snprintf(day, sizeof(day), "%.10s", sqlite3_column_text(st, 4) ? (const char *)sqlite3_column_text(st, 4) : "");
                        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                          "<tr><th scope=\"row\">%s</th><td>$%s</td><td>%s%s</td><td>%s</td><td><time>%s</time></td></tr>",
                          ei, in, sqlite3_column_type(st, 3) == SQLITE_NULL ? "" : "$", outp, ep, day);
                        shown++;
                    }
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "</tbody></table>");
                    sqlite3_finalize(st); st = NULL;
                }
            } else if (!want_q[0] && !want_kind[0]) {
                /* With nothing asked, the room answers the market question a
                 * person brings: is this a fair price? The same barcode at
                 * several shops, and the same medicine from several makers. */
                if (sqlite3_prepare_v2(db,
                      "SELECT item, currency, COUNT(DISTINCT publisher), MIN(amount), MAX(amount),"
                      " (SELECT publisher FROM observed_price c WHERE c.kind='consumer' AND c.item=o.item AND c.currency=o.currency ORDER BY amount LIMIT 1),"
                      " (SELECT publisher FROM observed_price c WHERE c.kind='consumer' AND c.item=o.item AND c.currency=o.currency ORDER BY amount DESC LIMIT 1)"
                      " FROM observed_price o WHERE kind='consumer' AND item<>'' AND item NOT GLOB '*[^0-9]*' AND length(item) BETWEEN 8 AND 14 GROUP BY item, currency HAVING COUNT(DISTINCT publisher)>=3 AND MIN(amount)>0"
                      " ORDER BY COUNT(DISTINCT publisher) DESC, MAX(amount)/MIN(amount) DESC LIMIT 12", -1, &st, NULL) == SQLITE_OK) {
                    int any = 0;
                    while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 4096) {
                        const char *item = (const char *)sqlite3_column_text(st, 0), *code = price_currency((const char *)sqlite3_column_text(st, 1));
                        char ei[200], lo[48], hi[48], el[200], eh[200]; double a = sqlite3_column_double(st, 3), b = sqlite3_column_double(st, 4);
                        if (!code || !item) continue;
                        if (!any++) fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                            "<h3 class=\"room-h\">Observed barcode price ranges</h3><p class=\"subj-note\">Retained observations for a barcode across publishers and dates. Prices may have changed; the range is not a current offer or a guaranteed saving.</p><table class=\"price-table\"><thead><tr><th scope=\"col\">Product</th>"
                            "<th scope=\"col\">Cheapest</th><th scope=\"col\">Dearest</th><th scope=\"col\">Gap</th></tr></thead><tbody>");
                        html_escape(item, ei, sizeof(ei)); price_amount(a, lo, sizeof(lo)); price_amount(b, hi, sizeof(hi));
                        html_escape((const char *)sqlite3_column_text(st, 5), el, sizeof(el)); html_escape((const char *)sqlite3_column_text(st, 6), eh, sizeof(eh));
                        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                          "<tr><th scope=\"row\"><a rel=\"nofollow\" href=\"https://world.openfoodfacts.org/product/%s\">Barcode %s</a> <small>%lld shops</small></th>"
                          "<td><strong>%s %s</strong> <small>%s</small></td><td>%s %s <small>%s</small></td><td>%.0f%%</td></tr>",
                          ei, ei, (long long)sqlite3_column_int64(st, 2), lo, code, el, hi, code, eh, (b / a - 1.0) * 100.0);
                        shown++;
                    }
                    if (any) fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "</tbody></table>");
                    sqlite3_finalize(st); st = NULL;
                }
                /* NADAC names carry the package code in parentheses; the name
                 * before it is the medicine, and each code is one maker's pack. */
                if (sqlite3_prepare_v2(db,
                      "SELECT trim(substr(item,1,instr(item,'(')-1)) AS drug, COUNT(*), MIN(amount), MAX(amount) FROM observed_price"
                      " WHERE kind='medicine' AND currency='USD' AND instr(item,'(')>1 AND amount>0 GROUP BY drug HAVING COUNT(*)>=4"
                      " ORDER BY MAX(amount)/MIN(amount) DESC, COUNT(*) DESC LIMIT 12", -1, &st, NULL) == SQLITE_OK) {
                    int any = 0;
                    while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 4096) {
                        char ed[260], lo[48], hi[48], find[300]; double a = sqlite3_column_double(st, 2), b = sqlite3_column_double(st, 3);
                        const char *drug = (const char *)sqlite3_column_text(st, 0);
                        if (!drug || !*drug) continue;
                        if (!any++) fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                            "<h3 class=\"room-h\">Medicine observations by name</h3><p class=\"subj-note\">Retained NADAC unit-price observations grouped by listed name. Records may differ in strength, form, package, maker or date; compare the individual listings before drawing conclusions.</p><table class=\"price-table\"><thead><tr><th scope=\"col\">Medicine</th>"
                            "<th scope=\"col\">Lowest a unit</th><th scope=\"col\">Highest a unit</th><th scope=\"col\">Records</th></tr></thead><tbody>");
                        html_escape(drug, ed, sizeof(ed)); price_amount(a, lo, sizeof(lo)); price_amount(b, hi, sizeof(hi));
                        if (strlen(drug) >= sizeof(want_q) || !url_query_buffer(drug, find, sizeof(find))) continue;
                        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                          "<tr><th scope=\"row\"><a href=\"/s/prices/observed?kind=medicine&amp;q=%s\">%s</a></th><td><strong>$%s</strong></td><td>$%s <small>%.0f&times;</small></td><td>%lld</td></tr>",
                          find, ed, lo, hi, b / a, (long long)sqlite3_column_int64(st, 1));
                        shown++;
                    }
                    if (any) fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "</tbody></table>");
                    sqlite3_finalize(st); st = NULL;
                }
                /* The unfiltered room must remain useful even when no barcode
                 * or medicine group qualifies. These are observations, never
                 * inferred current offers. */
                if (sqlite3_prepare_v2(db,
                      "SELECT kind,item,amount,currency,unit,publisher,observed_at,evidence_ref"
                      " FROM observed_price WHERE item<>'' AND amount>0 AND kind NOT IN('inference','compute')"
                      " ORDER BY observed_at DESC,id DESC LIMIT 240", -1, &st, NULL) == SQLITE_OK) {
                    char seen[16][32] = {{0}}; int per_kind[16] = {0}, kinds = 0, recent = 0;
                    while (sqlite3_step(st) == SQLITE_ROW && recent < 12 && fw < sizeof(facts) - 4096) {
                        const char *kind = (const char *)sqlite3_column_text(st, 0);
                        const char *item = (const char *)sqlite3_column_text(st, 1);
                        const char *code = price_currency((const char *)sqlite3_column_text(st, 3));
                        const char *ref = (const char *)sqlite3_column_text(st, 7);
                        char label[300], maker[180], unit[96], day[32], rawday[16], amount[48], item_url[480], url[2048];
                        int k = 0, has_ref;
                        if (!kind || !item || !*item || !code || strlen(kind) >= sizeof(seen[0]) ||
                            strspn(kind,"abcdefghijklmnopqrstuvwxyz-") != strlen(kind) ||
                            strlen(item) >= 160 || !url_query_buffer(item,item_url,sizeof(item_url))) continue;
                        for (; k < kinds; k++) if (!strcmp(kind,seen[k])) break;
                        if (k == kinds) {
                            if (kinds >= 16) continue;
                            snprintf(seen[kinds],sizeof(seen[0]),"%s",kind); kinds++;
                        }
                        if (per_kind[k] >= 3) continue;
                        html_escape(item,label,sizeof(label));
                        html_escape((const char *)sqlite3_column_text(st,5),maker,sizeof(maker));
                        html_escape(price_unit_words((const char *)sqlite3_column_text(st,4)),unit,sizeof(unit));
                        snprintf(rawday,sizeof(rawday),"%.10s",sqlite3_column_text(st,6) ?
                                 (const char *)sqlite3_column_text(st,6) : "");
                        html_escape(rawday,day,sizeof(day));
                        price_amount(sqlite3_column_double(st,2),amount,sizeof(amount));
                        has_ref = ref && public_http_url(ref) && !strpbrk(ref,"@ \"<>\t\r\n") && strlen(ref)<900;
                        if (has_ref) html_escape(ref,url,sizeof(url));
                        if (!recent) fw += (size_t)snprintf(facts+fw,sizeof(facts)-fw,
                            "<section class=\"price-recent\" aria-labelledby=\"price-recent-title\">"
                            "<h3 id=\"price-recent-title\" class=\"room-h\">Recent publisher price observations</h3>"
                            "<p class=\"subj-note\">Historical statements, not live quotes or available offers. Compare units, dates and publishers.</p><div class=\"price-recent-grid\">");
                        fw += (size_t)snprintf(facts+fw,sizeof(facts)-fw,
                            "<article class=\"price-recent-item\"><p class=\"price-recent-kind\">%s</p>"
                            "<h4><a href=\"/s/prices/observed?kind=%s&amp;q=%s\">%s</a></h4>"
                            "<p><strong>%s %s</strong> %s</p><p>%s &middot; <time>%s</time></p>%s%s%s</article>",
                            price_kind_label(kind),kind,item_url,label,amount,code,unit,maker,day,
                            has_ref ? "<a rel=\"nofollow noopener noreferrer\" href=\"" : "",
                            has_ref ? url : "",has_ref ? "\">View publisher evidence</a>" : "");
                        per_kind[k]++; recent++; shown++;
                    }
                    if (recent) fw += (size_t)snprintf(facts+fw,sizeof(facts)-fw,"</div></section>");
                    sqlite3_finalize(st); st = NULL;
                }
            } else if (sqlite3_prepare_v2(db,
                  "SELECT item, amount, currency, unit, publisher, kind, observed_at, region, evidence_ref FROM observed_price"
                  " WHERE item<>'' AND ((length(currency)=3 AND currency NOT GLOB 'Q*') OR currency IN('Q4917','Q4916','Q8146','Q25224','Q80524','Q25417','Q41588','Q1104069','Q122922','Q132643','Q25344','Q259502','Q41044','Q39099')) AND (?1='' OR kind=?1) AND (?2='' OR instr(lower(item),lower(?2))>0 OR instr(lower(publisher),lower(?2))>0)"
                  " ORDER BY observed_at DESC, item, id LIMIT 61 OFFSET ?3", -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, want_kind, -1, SQLITE_STATIC);
                sqlite3_bind_text(st, 2, want_q, -1, SQLITE_STATIC);
                sqlite3_bind_int64(st, 3, page_offset);
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "<h3 class=\"room-h\">%s</h3>"
                  "<table class=\"price-table\"><thead><tr><th scope=\"col\">What</th><th scope=\"col\">Price</th><th scope=\"col\">Stated by</th><th scope=\"col\">Read</th></tr></thead><tbody>",
                  want_q[0] ? "Prices that match" : want_kind[0] ? price_kind_label(want_kind) : "A spread of what is listed");
                while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 4096) {
                    const char *item = (const char *)sqlite3_column_text(st, 0), *cur = (const char *)sqlite3_column_text(st, 2);
                    const char *unit = (const char *)sqlite3_column_text(st, 3), *kind_row = (const char *)sqlite3_column_text(st, 5);
                    const char *code = price_currency(cur);
                    char ei[360], eu[96], ep[200], er[96], evidence[2048], amount[48], day[16];
                    const char *ref = (const char *)sqlite3_column_text(st, 8);
                    int has_ref = ref && public_http_url(ref) && !strpbrk(ref, "@ \"<>\t\r\n");
                    double value = sqlite3_column_double(st, 1);
                    if (!code || !item || !*item) continue;
                    if (shown >= 60) { has_more = 1; break; }
                    if (unit && !strncmp(unit, "token", 5)) { value *= 1e6; }
                    price_amount(value, amount, sizeof(amount));
                    if (kind_row && !strcmp(kind_row, "consumer") && strspn(item, "0123456789") == strlen(item)) {
                        char named[80]; snprintf(named, sizeof(named), "Product with barcode %s", item); html_escape(named, ei, sizeof(ei));
                    } else html_escape(item, ei, sizeof(ei));
                    html_escape(price_unit_words(unit), eu, sizeof(eu));
                    html_escape((const char *)sqlite3_column_text(st, 4), ep, sizeof(ep));
                    html_escape((const char *)sqlite3_column_text(st, 7), er, sizeof(er));
                    html_escape(has_ref ? ref : "", evidence, sizeof(evidence));
                    snprintf(day, sizeof(day), "%.10s", sqlite3_column_text(st, 6) ? (const char *)sqlite3_column_text(st, 6) : "");
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<tr><th scope=\"row\">%s%s%s%s</th><td><strong>%s %s</strong>%s%s</td><td>%s%s%s%s%s</td><td><time>%s</time></td></tr>",
                      ei, er[0] ? " <small>" : "", er, er[0] ? "</small>" : "", amount, code, eu[0] ? " " : "", eu, has_ref ? "<a rel=\"noopener noreferrer\" href=\"" : "", evidence, has_ref ? "\">" : "", ep, has_ref ? "</a>" : "", day);
                    shown++;
                }
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "</tbody></table>");
                sqlite3_finalize(st); st = NULL;
            }
            if (!shown) fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "<p class=\"subj-note\">Nothing listed matches that. Try a shorter word, or another kind above.</p>");
            else if (!models && !want_q[0] && !want_kind[0])
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "<p class=\"subj-note\">Choose a kind or search the retained observations.</p>");
            if (!models && (want_q[0] || want_kind[0]) && (page_offset || has_more)) {
                char encoded[300];
                if (url_query_buffer(want_q, encoded, sizeof(encoded))) {
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "<nav class=\"price-pages\" aria-label=\"Price result pages\">");
                    if (page_offset) fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "<a href=\"?kind=%s&amp;q=%s&amp;offset=%ld\">Previous prices</a>", want_kind, encoded, page_offset >= 60 ? page_offset - 60 : 0);
                    if (has_more) fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "<a href=\"?kind=%s&amp;q=%s&amp;offset=%ld\">More prices</a>", want_kind, encoded, page_offset + 60);
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "</nav>");
                }
            }
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<style>.price-lead{padding:clamp(20px,3.5vw,44px);margin:8px 0 26px;border:1px solid color-mix(in srgb,currentColor 18%%,transparent);border-radius:22px;background:linear-gradient(135deg,color-mix(in srgb,currentColor 10%%,transparent),color-mix(in srgb,currentColor 2%%,transparent))}"
              ".price-lead h2{margin:10px 0;font-size:clamp(1.7rem,4vw,3.2rem);letter-spacing:-.045em;line-height:1.08;max-width:20ch}.price-lead p{max-width:65ch;line-height:1.6}.price-eyebrow{margin:0;font-size:.72rem;letter-spacing:.11em;text-transform:uppercase;opacity:.72}"
              ".price-method{margin:28px 0 10px;padding:15px 20px;border:1px solid color-mix(in srgb,currentColor 18%%,transparent);border-radius:14px}.price-method summary{cursor:pointer;font-weight:600}.price-method .flow-facts{margin-top:18px}.price-method p{opacity:.78;max-width:74ch}"
              ".price-kinds{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0 22px}.price-kinds a{border:1px solid currentColor;border-radius:999px;padding:7px 12px;text-decoration:none}"
              ".price-kinds a[aria-current]{border-width:2px;font-weight:700;text-decoration:underline;text-underline-offset:4px}.price-kinds small{opacity:.7}"
              ".price-find{display:flex;gap:8px;margin:6px 0 18px;border-bottom:2px solid currentColor}.price-find input{flex:1;min-width:0;background:none;border:0;color:inherit;font:inherit;padding:10px 2px}"
              ".price-find button{font:inherit;background:none;color:inherit;border:0;cursor:pointer;font-weight:600}.price-pages{display:flex;flex-wrap:wrap;gap:10px;margin:20px 0}.price-pages a{display:inline-flex;align-items:center;min-height:44px;padding:8px 16px;border:1px solid currentColor;border-radius:999px;text-decoration:none;font-weight:600}.price-pages a:hover{background:color-mix(in srgb,currentColor 12%%,transparent)}"
              ".price-table{width:100%%;border-collapse:collapse;font-size:.92rem}.price-table th,.price-table td{text-align:left;padding:9px 10px 9px 0;border-bottom:1px solid color-mix(in srgb,currentColor 18%%,transparent);vertical-align:top}"
              ".price-table thead th{font-size:.72rem;text-transform:uppercase;letter-spacing:.08em;opacity:.7}.price-table tbody th{font-weight:500;overflow-wrap:anywhere}.price-table small{opacity:.65}.price-table time{white-space:nowrap;opacity:.7}"
              ".price-recent{margin-top:30px;padding-top:16px;border-top:1px solid color-mix(in srgb,currentColor 20%%,transparent)}.price-recent-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}.price-recent-item{padding:16px;border:1px solid color-mix(in srgb,currentColor 24%%,transparent);border-radius:14px;overflow-wrap:anywhere}.price-recent-item h4{font-size:1rem;margin:8px 0}.price-recent-item p{margin:8px 0;font-size:.85rem}.price-recent-kind{font-size:.72rem!important;letter-spacing:.07em;text-transform:uppercase;opacity:.7}.price-recent-item a{overflow-wrap:anywhere}@media(max-width:760px){.price-recent-grid{grid-template-columns:1fr}}"
              ".price-find{border:1px solid color-mix(in srgb,currentColor 25%%,transparent);border-radius:14px;padding:4px 8px 4px 14px}.price-find button{border-radius:10px;padding:8px 14px;background:color-mix(in srgb,currentColor 12%%,transparent)}.price-kinds a{min-height:44px;display:inline-flex;align-items:center;gap:8px}.price-table tbody tr:hover{background:color-mix(in srgb,currentColor 5%%,transparent)}@media(max-width:640px){.price-table{display:block;overflow-x:auto;overscroll-behavior-inline:contain}.price-table th,.price-table td{min-width:100px}.price-table th:first-child{min-width:210px}.price-find{flex-wrap:wrap}.price-find input{min-width:170px}}</style>");
        }
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<details class=\"price-method\"><summary>About these observations</summary><dl class=\"flow-facts\">"
          "<div><dt>Price observations</dt><dd>%ld</dd></div>"
          "<div><dt>Publishers</dt><dd>%ld</dd></div>"
          "<div><dt>Currencies</dt><dd>%ld</dd></div>"
          "<div><dt>Price lists</dt><dd>%ld</dd></div></dl>"
          "<p>These counts describe retained source observations, not current inventory, quotes or verified savings.</p></details>",
          total, publishers, currencies, sources);
        sqlite3_close(db);

        /* A distribution, drawn by the chart organ from numbers the room has
         * already written into the page. A reader who cannot run the organ
         * still has the counts above; the drawing adds shape, never fact.
         * Prices are bucketed by order of magnitude because a store holding a
         * drug at three-tenths of a cent and a federal award at two million
         * has no linear axis that shows both. */
        if ((db2 = NULL, path && *path &&
             bf_sqlite_readonly_uri(uri, sizeof(uri), path) < (int)sizeof(uri) &&
             sqlite3_open_v2(uri, &db2, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) == SQLITE_OK)) {
            if (sqlite3_prepare_v2(db2,
                  "SELECT CAST(FLOOR(LOG10(amount)) AS INTEGER) AS mag, COUNT(*)"
                  " FROM observed_price WHERE amount > 0"
                  " GROUP BY mag ORDER BY mag", -1, &st, NULL) == SQLITE_OK) {
                char series[2048]; size_t sw = 0; int first = 1;
                sw += (size_t)snprintf(series + sw, sizeof(series) - sw, "[");
                while (sqlite3_step(st) == SQLITE_ROW && sw < sizeof(series) - 64) {
                    sw += (size_t)snprintf(series + sw, sizeof(series) - sw, "%s[%lld,%lld]",
                        first ? "" : ",", (long long)sqlite3_column_int64(st, 0),
                        (long long)sqlite3_column_int64(st, 1));
                    first = 0;
                }
                snprintf(series + sw, sizeof(series) - sw, "]");
                sqlite3_finalize(st); st = NULL;
                if (!first)
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<h3 class=\"room-h\">How the prices are spread</h3>"
                      "<div class=\"plot\" data-bf-series='%s' data-bf-label=\"prices\" "
                      "data-bf-height=\"150\" role=\"img\" "
                      "aria-label=\"prices held, by order of magnitude\"></div>"
                      "<p class=\"subj-note\">One point per order of magnitude: the left is "
                      "fractions of a cent, the right is millions.</p>",
                      series);
            }
            sqlite3_close(db2);
        }

        snprintf(facts + fw, sizeof(facts) - fw,
          "<p class=\"subj-note\">Each of these is a publisher quoting itself: a cloud vendor's own "
          "retail list, a router's own model pricing, a government's own award record, a "
          "pharmacy-cost survey, shop prices contributed by the people who paid them, and the "
          "figures the world has written into a public knowledge base. </p>");

        n = aperture_push(acts, n, 8, "See what the estate itself is charged",
                          "the admitted rates, and what they price", 2, 0, "/s/frontier/cost");
        aperture_render(body, sizeof(body), "What things cost",
                        ident, "Prices as their publishers state them", facts,
                        acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "prices", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* An institution, and what this estate can actually reach of it.
     *
     * A parent should not have to ask whether their district has an API. They
     * name the school; this works out which surfaces exist and says what each
     * one can and cannot tell you. The distinction that matters is not
     * technical: some planes are published to anyone, and some answer only to
     * a relationship the institution itself established. Showing them in one
     * list as though a reader could simply have either is how a calendar feed
     * ends up presented as if it knew something about a child.
     *
     * So the reachable planes are counted from what the frontier actually
     * holds for that host, and the granted ones are named as absent with the
     * reason: not "coming soon", but "this answers to a relationship, and none
     * exists here".
     */
    if (!strcmp(kind, "institution")) {
        const char *fpath = getenv("BONFYRE_SOURCE_FRONTIER_DB");
        sqlite3 *fr = NULL; sqlite3_stmt *st = NULL; char uri[PATH_MAX];
        char name[256] = {0}, e_name[512], facts[16384]; size_t fw = 0;
        long ics = 0, feeds = 0, pages = 0, data = 0, docs = 0, total = 0;
        char mine_planes[512];
        mine_planes[0] = 0;
        size_t i;

        if (!fpath || !*fpath ||
            bf_sqlite_readonly_uri(uri, sizeof(uri), fpath) >= (int)sizeof(uri) ||
            sqlite3_open_v2(uri, &fr, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) != SQLITE_OK) {
            if (fr) sqlite3_close(fr);
            subject_miss("No source frontier is readable here.");
            return 1;
        }
        /* A plane is what the address actually is, not what its protocol
         * column happens to say. The frontier records how a channel was
         * acquired; an agenda published as a PDF arrived over HTTP and was
         * filed as a page, which made every institution look as though it
         * published no documents. The address itself is the better witness:
         * an .ics is a calendar however it was fetched, and a path ending in
         * .pdf under /agenda or /minutes is a document whatever column it sits
         * in. Protocol is still consulted, but it no longer decides alone. */
        if (sqlite3_prepare_v2(fr,
              /* The path decides, not the whole address. Matching anywhere in
               * the URL made coinigy.docs.apiary.io publish 23 documents,
               * because its hostname contains ".docs.". So the scheme and host
               * are cut away and any query string dropped before anything is
               * classified, and a channel lands in exactly one plane rather
               * than being counted under several. */
              "WITH ch AS ("
              " SELECT c.root_id, c.protocol AS proto,"
              "  CASE WHEN instr(p0,'?')>0 THEN substr(p0,1,instr(p0,'?')-1) ELSE p0 END AS path"
              " FROM ("
              "  SELECT root_id, protocol AS protocol,"
              "   CASE WHEN instr(rest,'/')>0 THEN substr(rest,instr(rest,'/')) ELSE '/' END AS p0"
              "  FROM (SELECT root_id, protocol,"
              "         CASE WHEN instr(lower(exact_address),'://')>0"
              "              THEN substr(lower(exact_address),instr(lower(exact_address),'://')+3)"
              "              ELSE lower(exact_address) END AS rest"
              "        FROM channel WHERE state IN ('admitted','active'))) AS c"
              ")"
              "SELECT COALESCE(NULLIF(r.name,''),r.host),"
              " SUM(k='calendar'), SUM(k='feed'), SUM(k='page'), SUM(k='data'),"
              " SUM(k='document'), SUM(k IS NOT NULL)"
              " FROM root r LEFT JOIN ("
              /* What the file is beats what the path is called. lozere.fr
               * publishes /agendas.rss: a feed about agendas, not 23
               * documents. So every extension rule is tried before any
               * keyword rule, and only an address with no telling extension
               * is classified by the words in its path. */
              "  SELECT root_id, CASE"
              "   WHEN path LIKE '%.ics' THEN 'calendar'"
              "   WHEN path LIKE '%.pdf' OR path LIKE '%.doc'"
              "        OR path LIKE '%.docx' THEN 'document'"
              "   WHEN path LIKE '%.csv' OR path LIKE '%.xlsx' THEN 'data'"
              "   WHEN path LIKE '%.rss' OR path LIKE '%.atom'"
              "        OR path LIKE '%.xml' THEN 'feed'"
              "   WHEN proto='ics' THEN 'calendar'"
              "   WHEN proto IN ('rss','atom','rss-xml','json_feed') THEN 'feed'"
              "   WHEN proto IN ('dcat','socrata','ckan') THEN 'data'"
              "   WHEN path LIKE '%/calendar%' THEN 'calendar'"
              "   WHEN path LIKE '%/agenda%' OR path LIKE '%/minutes%' THEN 'document'"
              "   WHEN path LIKE '%/feed%' THEN 'feed'"
              "   ELSE 'page' END AS k FROM ch) c ON c.root_id = r.root_id"
              " WHERE r.host = ?1 GROUP BY r.root_id LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, ident, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                snprintf(name, sizeof(name), "%s", (const char *)sqlite3_column_text(st, 0));
                ics = (long)sqlite3_column_int64(st, 1);
                feeds = (long)sqlite3_column_int64(st, 2);
                pages = (long)sqlite3_column_int64(st, 3);
                data = (long)sqlite3_column_int64(st, 4);
                docs = (long)sqlite3_column_int64(st, 5);
                total = (long)sqlite3_column_int64(st, 6);
            }
            sqlite3_finalize(st); st = NULL;
        }
        sqlite3_close(fr); fr = NULL;
        if (!name[0] || !total) { subject_miss("No institution with admitted public material carries that host."); return 1; }

        html_escape(name, e_name, sizeof(e_name));
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<dl class=\"flow-facts\">"
          "<div><dt>Addresses held</dt><dd>%ld</dd></div>"
          "<div><dt>Calendars</dt><dd>%ld</dd></div>"
          "<div><dt>Feeds</dt><dd>%ld</dd></div>"
          "<div><dt>Documents</dt><dd>%ld</dd></div>"
          "<div><dt>Data</dt><dd>%ld</dd></div></dl>"
          "<h3 class=\"room-h\">What can be reached, and what it knows</h3>",
          total, ics, feeds, docs, data);

        /* Which granted planes this reader's own household holds, and only
         * this reader's. A connection belongs to the household that
         * established it: another household's connection is not visible here,
         * and a signed-out reader sees none at all. */
        {
            char actor[64];
            sqlite3 *cdb = cms_open_ro();
            if (cdb && page_session_actor(bearer, auth_db, actor, sizeof(actor))) {
                if (sqlite3_prepare_v2(cdb,
                      "SELECT DISTINCT c.plane FROM institution_connection c"
                      " JOIN household h ON h.household_id = c.household_id"
                      " WHERE c.institution = ?1 AND c.state = 'established'"
                      "   AND c.status = 'published' AND h.steward = ?2",
                      -1, &st, NULL) == SQLITE_OK) {
                    size_t mw = 0;
                    sqlite3_bind_text(st, 1, ident, -1, SQLITE_STATIC);
                    sqlite3_bind_text(st, 2, actor, -1, SQLITE_STATIC);
                    mine_planes[mw++] = '|';
                    while (sqlite3_step(st) == SQLITE_ROW &&
                           mw < sizeof(mine_planes) - 130) {
                        const char *pn = (const char *)sqlite3_column_text(st, 0);
                        if (pn && *pn)
                            mw += (size_t)snprintf(mine_planes + mw,
                                     sizeof(mine_planes) - mw, "%s|", pn);
                    }
                    mine_planes[mw] = 0;
                    sqlite3_finalize(st); st = NULL;
                }
            }
            if (cdb) sqlite3_close(cdb);
        }

        for (i = 0; i < BF_INSTITUTION_PLANE_COUNT && fw < sizeof(facts) - 2560; i++) {
            const BfInstitutionPlane *pl = &bf_institution_planes[i];
            int is_public = !strcmp(pl->reach, "public");
            int mine = 0;
            char connect_form[420];
            long held = 0;
            char el[192], ecar[768], eci[1024];
            if (!strcmp(pl->plane, "calendar-ics")) held = ics;
            else if (!strcmp(pl->plane, "feed")) held = feeds;
            else if (!strcmp(pl->plane, "web-page")) held = pages;
            else if (!strcmp(pl->plane, "open-data")) held = data;
            else if (!strcmp(pl->plane, "document")) held = docs;
            html_escape(pl->label, el, sizeof(el));
            html_escape(pl->carries, ecar, sizeof(ecar));
            html_escape(pl->cannot_infer, eci, sizeof(eci));
            if (!is_public && mine_planes[0]) {
                char key[80];
                snprintf(key, sizeof(key), "|%s|", pl->plane);
                if (strstr(mine_planes, key)) mine = 1;
            }
            connect_form[0] = 0;
            if (!is_public && signed_in) {
                char epl[64], eid[128];
                html_escape(pl->plane, epl, sizeof(epl));
                html_escape(ident, eid, sizeof(eid));
                /* Said and unsaid are the same control. A relationship a
                 * household cannot end is not one it holds. */
                snprintf(connect_form, sizeof(connect_form),
                  "<form class=\"gv-form\" method=\"post\" action=\"/govern/connect\">"
                  "<input type=\"hidden\" name=\"institution\" value=\"%s\">"
                  "<input type=\"hidden\" name=\"plane\" value=\"%s\">"
                  "<input type=\"hidden\" name=\"return\" value=\"/s/institution/%s\">"
                  "%s<button>%s</button></form>",
                  eid, epl, eid,
                  mine ? "<input type=\"hidden\" name=\"release\" value=\"1\">" : "",
                  mine ? "Release it" : "This household holds it");
            }
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
              "<span class=\"desk-a\">%s</span>"
              "<span class=\"desk-s\">%s%.0s</span>%s</div>",
              el,
              is_public ? (held ? "reachable" : "none held")
                        : (mine ? "yours" : "needs a relationship"),
              ecar, eci,
              connect_form);
        }

        /* A public institution is useful before anybody adopts it.  These are
         * the exact public addresses currently admitted for this host, not a
         * promise that a private plane will become readable.  The room keeps
         * the source's own address so a parent can read a calendar, notice or
         * document directly while the estate retains the boundary around it. */
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">Published material already in this world</h3>"
          "<div data-bf-list data-bf-row-height=\"44\">");
        if (fpath && *fpath &&
            bf_sqlite_readonly_uri(uri, sizeof(uri), fpath) < (int)sizeof(uri) &&
            sqlite3_open_v2(uri, &fr, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) == SQLITE_OK &&
            sqlite3_prepare_v2(fr,
              "SELECT c.exact_address, COALESCE(c.protocol,'unknown')"
              " FROM channel c JOIN root r ON r.root_id=c.root_id"
              " WHERE r.host=?1 AND c.state IN ('admitted','active')"
              " ORDER BY CASE c.protocol WHEN 'ics' THEN 0 WHEN 'rss' THEN 1"
              " WHEN 'atom' THEN 1 WHEN 'json_feed' THEN 1 ELSE 2 END, c.exact_address LIMIT 24",
              -1, &st, NULL) == SQLITE_OK) {
            int shown = 0;
            sqlite3_bind_text(st, 1, ident, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 1400) {
                const char *address = (const char *)sqlite3_column_text(st, 0);
                const char *protocol = (const char *)sqlite3_column_text(st, 1);
                char ea[1536], ep[80];
                if (!address || !public_http_url(address)) continue;
                html_escape(address, ea, sizeof(ea));
                html_escape(protocol ? protocol : "unknown", ep, sizeof(ep));
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                  "<a class=\"desk-row\" href=\"%s\" target=\"_blank\" rel=\"noopener noreferrer\">"
                  "<span class=\"desk-l\">Open published material</span>"
                  "<span class=\"desk-a\">%s</span><span class=\"desk-s\">%s</span></a>",
                  ea, ep, ea);
                shown++;
            }
            sqlite3_finalize(st); st = NULL;
            if (!shown) fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<p class=\"subj-note\">The frontier holds material for this host, but none of its "
              "stored addresses are a public HTTP address that can be opened here.</p>");
        }
        if (st) { sqlite3_finalize(st); st = NULL; }
        if (fr) { sqlite3_close(fr); fr = NULL; }
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "</div>");

        /* What each plane is, and is not, is the atlas's to say; the page lists the planes. */

        {
            /* Its own page here leads: what it published, who it names and
             * every channel, composed.  This room stays the ledger of what can
             * be reached. */
            static char own[600];
            EstateReferenceStores stores = {0}; EstateReference self;
            estate_reference_open(&stores);
            if (estate_reference_resolve(&stores, ident, &self) && self.url[0]) {
                snprintf(own, sizeof(own), "%s", self.url);
                n = aperture_push(acts, n, 8, "Open its page", "what it published, who it names, every channel", 3, 0, own);
            } else n = aperture_push(acts, n, 8, "Read what was kept from it", "in the reading room", 2, 0, "/archive");
            estate_reference_close(&stores);
        }
        n = aperture_push(acts, n, 8, "See it as a publisher", "what it files and how densely", 1, 0, "/sources");
        aperture_render(body, sizeof(body), "Institution &middot; what can be reached of it",
                        ident, e_name, facts, acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "institution", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* What a community body is short of, over the projects the estate holds.
     *
     * Ten projects, sixty roles they say they lack, thirty-one tasks and a
     * hundred and twelve proposals about them were all in the record store with
     * nothing reading them together. None of this is developer work: the projects
     * are housing case work, lead-hazard inspection, road-safety evidence, a
     * local food market. What a work tracker adds is being able to see, for one
     * of them, what it needs and what anybody has actually promised -- and to see
     * that a task carries no figure rather than assuming it carries one.
     */
    if (!strcmp(kind, "work")) {
        sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL;
        char facts[262144]; size_t fw = 0;
        long projects = 0, needs = 0, filled = 0, tasks = 0, priced = 0, supports = 0;
        if (!db) { subject_miss("No record store is readable here."); return 1; }

        if (sqlite3_prepare_v2(db,
              "SELECT (SELECT COUNT(*) FROM project WHERE status='published'),"
              " (SELECT COUNT(*) FROM team_need WHERE status='published'),"
              " (SELECT COUNT(*) FROM team_need WHERE status='published'"
              "    AND filled NOT IN ('','0','false')),"
              " (SELECT COUNT(*) FROM bounty WHERE status='published'),"
              " (SELECT COUNT(*) FROM bounty WHERE status='published'"
              "    AND CAST(amount_stated AS REAL) > 0),"
              " (SELECT COUNT(*) FROM project_support WHERE status='published')",
              -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) {
            projects = (long)sqlite3_column_int64(st, 0);
            needs    = (long)sqlite3_column_int64(st, 1);
            filled   = (long)sqlite3_column_int64(st, 2);
            tasks    = (long)sqlite3_column_int64(st, 3);
            priced   = (long)sqlite3_column_int64(st, 4);
            supports = (long)sqlite3_column_int64(st, 5);
        }
        if (st) { sqlite3_finalize(st); st = NULL; }
        if (!projects) { sqlite3_close(db); subject_miss("No project is published here."); return 1; }

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<dl class=\"flow-facts\">"
          "<div><dt>Projects</dt><dd>%ld</dd></div>"
          "<div><dt>People they want</dt><dd>%ld</dd></div>"
          "<div><dt>Roles taken</dt><dd>%ld</dd></div>"
          "<div><dt>Jobs to do</dt><dd>%ld</dd></div></dl>"
          "<p class=\"subj-note\">%ld of the %ld jobs state what they pay. "
          "%ld people have offered to help.</p>"
          "<h3 class=\"room-h\">Projects</h3><div data-bf-list data-bf-row-height=\"34\">",
          projects, needs, filled, tasks, priced, tasks, supports);

        if (sqlite3_prepare_v2(db,
              "SELECT p.project_id, p.title, p.stage,"
              " (SELECT COUNT(*) FROM team_need t WHERE t.project_id=p.project_id"
              "    AND t.status='published'),"
              " (SELECT COUNT(*) FROM team_need t WHERE t.project_id=p.project_id"
              "    AND t.status='published' AND t.filled NOT IN ('','0','false')),"
              " (SELECT COUNT(*) FROM bounty b WHERE b.project_id=p.project_id"
              "    AND b.status='published'),"
              " (SELECT COUNT(*) FROM governance_proposal g WHERE g.status='published'"
              "    AND p.project_id LIKE '%' || g.subject_ref)"
              " FROM project p WHERE p.status='published' ORDER BY p.title, p.project_id",
              -1, &st, NULL) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 2048) {
                char eid[320], et[320], es[96];
                html_escape((const char *)sqlite3_column_text(st, 0), eid, sizeof(eid));
                html_escape((const char *)sqlite3_column_text(st, 1), et, sizeof(et));
                html_escape((const char *)sqlite3_column_text(st, 2), es, sizeof(es));
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                  "<a class=\"desk-row\" href=\"/s/project/%s\"><span class=\"desk-l\">%s</span>"
                  "<span class=\"desk-a\">%lld of %lld roles taken</span>"
                  "<span class=\"desk-s\">%s &middot; %lld jobs &middot; %lld open "
                  "questions</span></a>",
                  eid, et[0] ? et : eid,
                  (long long)sqlite3_column_int64(st, 4), (long long)sqlite3_column_int64(st, 3),
                  es[0] ? es : "no stage stated",
                  (long long)sqlite3_column_int64(st, 5), (long long)sqlite3_column_int64(st, 6));
            }
            sqlite3_finalize(st); st = NULL;
        }
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "</div>");
        sqlite3_close(db);

        n = aperture_push(acts, n, 8, "See what is coming up", "the dated things still to happen", 2, 0, "/s/whats-on/all");
        n = aperture_push(acts, n, 8, "Read the governance grammar", "what an act has to answer to", 1, 0, "/s/governance/grammar");
        aperture_render(body, sizeof(body), "Needs doing",
                        ident, "Projects looking for people, and the jobs they need done", facts,
                        acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "work", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* One project: what it wants, what it is asking, and what was decided. */
    if (!strcmp(kind, "project")) {
        sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL;
        char facts[262144]; size_t fw = 0;
        char title[320] = {0}, stage[96] = {0}, pitch[4096] = {0}, subject_ref[320] = {0};
        if (!db) { subject_miss("No record store is readable here."); return 1; }
        if (sqlite3_prepare_v2(db,
              "SELECT title, stage, COALESCE(pitch,''), COALESCE(subject_ref,'')"
              " FROM project WHERE project_id=?1 AND status='published'", -1, &st, NULL) != SQLITE_OK ||
            (sqlite3_bind_text(st, 1, ident, -1, SQLITE_STATIC), sqlite3_step(st)) != SQLITE_ROW) {
            if (st) sqlite3_finalize(st);
            sqlite3_close(db);
            subject_miss("No project with that identifier.");
            return 1;
        }
        snprintf(title, sizeof(title), "%s", (const char *)sqlite3_column_text(st, 0));
        snprintf(stage, sizeof(stage), "%s", (const char *)sqlite3_column_text(st, 1));
        snprintf(pitch, sizeof(pitch), "%s", (const char *)sqlite3_column_text(st, 2));
        snprintf(subject_ref, sizeof(subject_ref), "%s", (const char *)sqlite3_column_text(st, 3));
        sqlite3_finalize(st); st = NULL;

        {
            char es[192], ep[8192];
            html_escape(stage, es, sizeof(es));
            html_escape(pitch, ep, sizeof(ep));
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<dl class=\"flow-facts\"><div><dt>Stage</dt><dd>%s</dd></div></dl>%s%s%s",
              es[0] ? es : "not stated",
              ep[0] ? "<p class=\"subj-note\">" : "", ep, ep[0] ? "</p>" : "");
        }

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">People it wants</h3>");
        if (sqlite3_prepare_v2(db,
              "SELECT role, COALESCE(wanted,''), COALESCE(filled,'') FROM team_need"
              " WHERE project_id=?1 AND status='published' ORDER BY role", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, ident, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 2048) {
                const char *fl = (const char *)sqlite3_column_text(st, 2);
                int is_filled = fl[0] && strcmp(fl, "0") && strcmp(fl, "false");
                char er[192], ew[1024];
                html_escape((const char *)sqlite3_column_text(st, 0), er, sizeof(er));
                html_escape((const char *)sqlite3_column_text(st, 1), ew, sizeof(ew));
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                  "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
                  "<span class=\"desk-a\">%s</span><span class=\"desk-s\">%s</span></div>",
                  er, is_filled ? "somebody said yes" : "nobody yet",
                  ew[0] ? ew : "the project has not said what it wants from this role");
            }
            sqlite3_finalize(st); st = NULL;
        }

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">Jobs to do</h3>");
        if (sqlite3_prepare_v2(db,
              "SELECT task, COALESCE(amount_stated,''), COALESCE(currency,''), COALESCE(state,'')"
              " FROM bounty WHERE project_id=?1 AND status='published' ORDER BY task",
              -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, ident, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 2048) {
                double a = atof((const char *)sqlite3_column_text(st, 1));
                char etask[1024], ecur[32], est2[96];
                html_escape((const char *)sqlite3_column_text(st, 0), etask, sizeof(etask));
                html_escape((const char *)sqlite3_column_text(st, 2), ecur, sizeof(ecur));
                html_escape((const char *)sqlite3_column_text(st, 3), est2, sizeof(est2));
                if (a > 0.0)
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
                      "<span class=\"desk-a\">%.2f %s</span>"
                      "<span class=\"desk-s\">%s &middot; a stated figure is not an award and not an "
                      "offer anybody can accept</span></div>",
                      etask, a, ecur[0] ? ecur : "", est2[0] ? est2 : "stated");
                else
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
                      "<span class=\"desk-a\">no figure</span>"
                      "<span class=\"desk-s\">%s &middot; nobody has priced this, and this estate will "
                      "not price it to fill the column</span></div>",
                      etask, est2[0] ? est2 : "stated");
            }
            sqlite3_finalize(st); st = NULL;
        }

        if (subject_ref[0]) {
            int shown = 0;
            if (sqlite3_prepare_v2(db,
                  "SELECT g.verb, g.asks, g.state, g.backing,"
                  " (SELECT COUNT(*) FROM governance_vote v WHERE v.proposal_id=g.proposal_id"
                  "    AND v.status='published')"
                  " FROM governance_proposal g WHERE g.subject_ref=?1 AND g.status='published'"
                  " ORDER BY g.state, g.opened_at DESC LIMIT 40", -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, subject_ref, -1, SQLITE_STATIC);
                while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 2048) {
                    char everb[96], easks[2048], est3[64], eback[96];
                    html_escape((const char *)sqlite3_column_text(st, 0), everb, sizeof(everb));
                    html_escape((const char *)sqlite3_column_text(st, 1), easks, sizeof(easks));
                    html_escape((const char *)sqlite3_column_text(st, 2), est3, sizeof(est3));
                    html_escape((const char *)sqlite3_column_text(st, 3), eback, sizeof(eback));
                    if (!shown) {
                        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                          "<h3 class=\"room-h\">What its members were asked</h3>");
                        shown = 1;
                    }
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
                      "<span class=\"desk-a\">%s, %lld said</span>"
                      "<span class=\"desk-s\">%s &middot; backed by %s. A tally is what was said by "
                      "those who said it, never authority to carry it out</span></div>",
                      everb, est3, (long long)sqlite3_column_int64(st, 4), easks, eback);
                }
                sqlite3_finalize(st); st = NULL;
            }
        }
        sqlite3_close(db);

        n = aperture_push(acts, n, 8, "See every project", "and what each is short of", 2, 0, "/s/work/all");
        n = aperture_push(acts, n, 8, "See what is coming up", "the dated things still to happen", 1, 0, "/s/whats-on/all");
        aperture_render(body, sizeof(body), "Project &middot; what it wants and what it asked",
                        ident, title[0] ? title : ident, facts, acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "project", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* What is actually coming up.
     *
     * The occurrence store holds 55,697 rows and almost none of them is an event.
     * A podcast episode, an NPR segment, a job posting and a Sunday afternoon gig
     * are all one row type with a date on it, so "what's on" read straight off
     * that table would be a listings page mostly made of articles.
     *
     * So this counts the mixture and shows the part that answers the question: the
     * rows whose date is still ahead. It says how small that part is rather than
     * padding it, because a page that presents 55,697 dated publications as
     * things to go to is worse than one that admits 219.
     */
    if (!strcmp(kind, "whats-on")) {
        sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL;
        char facts[262144]; size_t fw = 0;
        long total = 0, ahead = 0, timed = 0, sources = 0;
        if (!db) { subject_miss("No record store is readable here."); return 1; }

        if (sqlite3_prepare_v2(db,
              "SELECT COUNT(*),"
              " SUM(published_at > strftime('%Y-%m-%dT%H:%M:%SZ','now')),"
              " SUM(title LIKE '%AM)%' OR title LIKE '%PM)%'),"
              " COUNT(DISTINCT source_twin)"
              " FROM publication_occurrence WHERE status='published'", -1, &st, NULL) == SQLITE_OK &&
            sqlite3_step(st) == SQLITE_ROW) {
            total = (long)sqlite3_column_int64(st, 0);
            ahead = (long)sqlite3_column_int64(st, 1);
            timed = (long)sqlite3_column_int64(st, 2);
            sources = (long)sqlite3_column_int64(st, 3);
        }
        if (st) { sqlite3_finalize(st); st = NULL; }
        if (!total) { sqlite3_close(db); subject_miss("Nothing dated has been retained here."); return 1; }

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<dl class=\"flow-facts\">"
          "<div><dt>Still ahead</dt><dd>%ld</dd></div>"
          "<div><dt>With a start time</dt><dd>%ld</dd></div>"
          "<div><dt>Dated rows in all</dt><dd>%ld</dd></div>"
          "<div><dt>Sources</dt><dd>%ld</dd></div></dl>"
          "<p class=\"subj-note\">%ld of the %ld dated rows this estate holds are still ahead. The "
          "rest have a date because they were published on one, not because anything is happening: a "
          "podcast episode, a news segment and a Sunday afternoon gig are the same row type here, and "
          "nothing in the record distinguishes them. So this shows the part that answers the question "
          "and says how small it is, rather than presenting fifty-five thousand articles as things to "
          "go to.</p>"
          "<h3 class=\"room-h\">Coming up</h3><div data-bf-list data-bf-row-height=\"34\">",
          ahead, timed, total, sources, ahead, total);

        if (sqlite3_prepare_v2(db,
              "SELECT substr(published_at,1,10), title, COALESCE(canonical_href,''),"
              " COALESCE(source_twin,''), COALESCE(rights_basis,''), COALESCE(assertion_state,'')"
              " FROM publication_occurrence"
              " WHERE status='published' AND published_at > strftime('%Y-%m-%dT%H:%M:%SZ','now')"
              " ORDER BY published_at LIMIT 400", -1, &st, NULL) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 2048) {
                char eday[32], etitle[600], esrc[200], erights[160];
                html_escape((const char *)sqlite3_column_text(st, 0), eday, sizeof(eday));
                html_escape((const char *)sqlite3_column_text(st, 1), etitle, sizeof(etitle));
                html_escape((const char *)sqlite3_column_text(st, 3), esrc, sizeof(esrc));
                html_escape((const char *)sqlite3_column_text(st, 4), erights, sizeof(erights));
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                  "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
                  "<span class=\"desk-a\">%s</span>"
                  "<span class=\"desk-s\">from %s%s%s</span></div>",
                  etitle, eday, esrc,
                  erights[0] ? " &middot; " : "", erights);
            }
            sqlite3_finalize(st); st = NULL;
        }
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "</div>");
        sqlite3_close(db);

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<p class=\"subj-note\">A row here is something a source published with a date on it. It is "
          "not a ticket, not a reservation, not a promise the thing will happen, and not this estate's "
          "endorsement of the source. Where the source stated the basis on which it may be retained, "
          "that is shown beside it; where it stated none, nothing is claimed on its behalf.</p>");

        n = aperture_push(acts, n, 8, "See what these bodies are short of", "roles and jobs", 2, 0, "/s/work/all");
        n = aperture_push(acts, n, 8, "See how densely each source files", "what a listing is drawn from", 1, 0, "/sources");
        aperture_render(body, sizeof(body), "Coming up &middot; the dated things still ahead",
                        ident, "And how much of the pile is not an event at all", facts,
                        acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "whats-on", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* The model estate, as something a person can look at.
     *
     * The catalogue has held this all along: 22 models, 201 layer artifacts, 134
     * capabilities and 724 edges between them, including which model was derived
     * from which, which capability a model powers, and which workflow steps use
     * it. What it has never had is a page. `bonfyre-model list` prints a table of
     * names and sizes, and everything that makes a model page worth reading --
     * its variants, what it came from, what depends on it, what it would cost to
     * run -- was reachable only by querying the graph by hand.
     *
     * Nothing here runs a model. The cost band is the one honest part of a run
     * button: it reads the admitted rates, and where no rate exists it says so
     * rather than estimating.
     */
    if (!strcmp(kind, "models")) {
        char uri[PATH_MAX];
        char facts[262144]; size_t fw = 0;
        int state = bf_model_catalogue_index(getenv("BONFYRE_MODEL_CATALOG"),
                                            facts, sizeof(facts), &fw);
        if (state <= 0) {
            subject_miss(state == 0 ? "The model catalogue holds no model."
                                    : "No model catalogue is readable here.");
            return 1;
        }

        n = aperture_push(acts, n, 8, "See what things cost",
                          "everyday prices, compared across shops and makers", 2, 0, "/s/prices/observed");
        /* What hosted models charge, as each router lists itself: the price
         * side of the commons, read from the same observatory as every other
         * stated price. */
        {
            const char *prices = getenv("BONFYRE_PRICES_DB"); sqlite3 *pdb = NULL; sqlite3_stmt *ps = NULL; int rows = 0;
            if (prices && *prices && bf_sqlite_readonly_uri(uri, sizeof(uri), prices) < (int)sizeof(uri) &&
                sqlite3_open_v2(uri, &pdb, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) == SQLITE_OK &&
                sqlite3_prepare_v2(pdb,
                  "SELECT item, MAX(CASE WHEN unit='token in' THEN amount END), MAX(CASE WHEN unit='token out' THEN amount END), publisher, observed_at"
                  " FROM observed_price WHERE kind='inference' AND currency='USD' GROUP BY item,source,publisher,observed_at,evidence_ref"
                  " HAVING MAX(CASE WHEN unit='token in' THEN amount END)>0 AND COUNT(DISTINCT CASE WHEN unit='token in' THEN amount END)=1 AND COUNT(DISTINCT CASE WHEN unit='token out' THEN amount END)<=1"
                  " ORDER BY MAX(CASE WHEN unit='token in' THEN amount END)+COALESCE(MAX(CASE WHEN unit='token out' THEN amount END),0), item LIMIT 40",
                  -1, &ps, NULL) == SQLITE_OK) {
                while (sqlite3_step(ps) == SQLITE_ROW && fw < sizeof(facts) - 4096) {
                    char ei[320], ep[200], day[16], eday[96], in[48], outp[48];
                    if (!rows++) fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                        "<h3 class=\"room-h\" id=\"model-prices-title\">Hosted model price observations</h3><p class=\"subj-note\" id=\"model-prices-note\">US dollars per million tokens from the same publisher observation. Listed prices can change and do not rank quality.</p><style>.model-price-scroll{overflow-x:auto;overscroll-behavior-inline:contain}.model-price-scroll:focus-visible{outline:2px solid currentColor;outline-offset:3px}.model-price-scroll table{width:100%%;min-width:600px;border-collapse:collapse}.model-price-scroll th,.model-price-scroll td{text-align:left;padding:9px 12px 9px 0;border-bottom:1px solid color-mix(in srgb,currentColor 18%%,transparent);vertical-align:top}.model-price-scroll tbody th{font-weight:500;overflow-wrap:anywhere}.model-price-scroll time{white-space:nowrap}</style><div class=\"model-price-scroll\" role=\"region\" tabindex=\"0\" aria-labelledby=\"model-prices-title\" aria-describedby=\"model-prices-note\"><table class=\"price-table\"><thead><tr><th scope=\"col\">Model</th>"
                        "<th scope=\"col\" style=\"text-align:left\">Reading in</th><th scope=\"col\" style=\"text-align:left\">Writing out</th><th scope=\"col\" style=\"text-align:left\">Publisher</th><th scope=\"col\" style=\"text-align:left\">Read</th></tr></thead><tbody>");
                    html_escape((const char *)sqlite3_column_text(ps, 0), ei, sizeof(ei));
                    html_escape((const char *)sqlite3_column_text(ps, 3), ep, sizeof(ep));
                    snprintf(day, sizeof(day), "%.10s", sqlite3_column_text(ps, 4) ? (const char *)sqlite3_column_text(ps, 4) : "");
                    html_escape(day, eday, sizeof(eday));
                    price_amount(sqlite3_column_double(ps, 1) * 1e6, in, sizeof(in));
                    if (sqlite3_column_type(ps, 2) == SQLITE_NULL) snprintf(outp, sizeof(outp), "&mdash;");
                    else { char v[40]; price_amount(sqlite3_column_double(ps, 2) * 1e6, v, sizeof(v)); snprintf(outp, sizeof(outp), "$%s", v); }
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "<tr><th scope=\"row\" style=\"text-align:left;font-weight:500\">%s</th><td>$%s</td><td>%s</td><td>%s</td><td><time>%s</time></td></tr>", ei, in, outp, ep, eday);
                }
                if (rows) fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "</tbody></table></div>");
            }
            sqlite3_finalize(ps); if (pdb) sqlite3_close(pdb);
        }
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<link rel=\"stylesheet\" href=\"/apps/aurekai-model.css\">"
          "<section class=\"ak-model-workbench\" data-aurekai-model=\"\" aria-label=\"Model execution\">"
          "<h2>Run a model</h2><p>Checking models available for your account&hellip;</p>"
          "<noscript>Enable JavaScript to review current execution capabilities.</noscript></section>"
          "<script type=\"module\" src=\"/apps/aurekai-model.mjs\"></script>");
        aperture_render(body, sizeof(body), "Models &middot; what this estate holds and what depends on it",
                        ident, "Registered models, their variants and their dependents", facts,
                        acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "models", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* One model: its variants, what it came from, what depends on it, and what
     * running it would cost where a rate has been admitted.
     *
     * The cost band is deliberately the least impressive part. A run button that
     * quoted a number the estate had invented would be worse than no button, so
     * this reads only rates a provider stated about itself and says plainly when
     * there is none -- which, for a model this estate holds locally, is the usual
     * answer and the honest one.
     */
    if (!strcmp(kind, "model")) {
        char name[320] = {0}, facts[262144], uri[PATH_MAX];
        size_t fw = 0;
        int state = bf_model_catalogue_detail(getenv("BONFYRE_MODEL_CATALOG"),
                ident, name, sizeof(name), facts, sizeof(facts), &fw);
        if (state <= 0) {
            subject_miss(state == 0 ? "No model with that identifier is registered."
                                    : "No model catalogue is readable here.");
            return 1;
        }

        /* What running it would cost, from admitted rates only. */
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">Provider price observations</h3>");
        {
            const char *ppath = getenv("BONFYRE_PRICES_DB");
            sqlite3 *pdb = NULL; sqlite3_stmt *pst = NULL;
            int priced = 0;
            if (ppath && *ppath &&
                bf_sqlite_readonly_uri(uri, sizeof(uri), ppath) < (int)sizeof(uri) &&
                sqlite3_open_v2(uri, &pdb, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) == SQLITE_OK &&
                sqlite3_prepare_v2(pdb,
                  "SELECT publisher, item, amount, currency, unit, observed_at FROM observed_price"
                  " WHERE kind='inference' AND (lower(item)=lower(?1) OR lower(item)=lower(?2)) ORDER BY observed_at DESC LIMIT 6",
                  -1, &pst, NULL) == SQLITE_OK) {
                /* Matched on the model's own name, because a price this estate
                 * holds is a price some provider published about a named model.
                 * No match is the common case and is stated as such. */
                sqlite3_bind_text(pst, 1, ident, -1, SQLITE_STATIC);
                sqlite3_bind_text(pst, 2, name, -1, SQLITE_STATIC);
                while (sqlite3_step(pst) == SQLITE_ROW && fw < sizeof(facts) - 2048) {
                    char epub[200], eitem[300], ecur[32], eunit[64], eat[64];
                    html_escape((const char *)sqlite3_column_text(pst, 0), epub, sizeof(epub));
                    html_escape((const char *)sqlite3_column_text(pst, 1), eitem, sizeof(eitem));
                    html_escape((const char *)sqlite3_column_text(pst, 3), ecur, sizeof(ecur));
                    html_escape((const char *)sqlite3_column_text(pst, 4), eunit, sizeof(eunit));
                    html_escape((const char *)sqlite3_column_text(pst, 5), eat, sizeof(eat));
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
                      "<span class=\"desk-a\">%.6f %s per %s</span>"
                      "<span class=\"desk-s\">%s, as %s stated it on %s</span></div>",
                      eitem, sqlite3_column_double(pst, 2), ecur, eunit[0] ? eunit : "call",
                      epub, epub, eat);
                    priced = 1;
                }
            }
            if (pst) sqlite3_finalize(pst);
            if (pdb) sqlite3_close(pdb);
            if (!priced)
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                  "<p class=\"subj-note\">No retained provider price observation matches this catalogue "
                  "entry. A missing price does not establish free execution.</p>");
        }

        {
            char model_id[2048], model_name[2048];
            html_escape(ident, model_id, sizeof(model_id));
            html_escape(name, model_name, sizeof(model_name));
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<link rel=\"stylesheet\" href=\"/apps/aurekai-model.css\">"
              "<section class=\"ak-model-workbench\" data-aurekai-model=\"%s\" data-model-name=\"%s\" aria-label=\"Model execution\">"
              "<h2>Run this model</h2><p>Checking execution for your account&hellip;</p>"
              "<noscript>Enable JavaScript to review current execution capabilities.</noscript></section>"
              "<script type=\"module\" src=\"/apps/aurekai-model.mjs\"></script>", model_id, model_name);
        }
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<p class=\"subj-note\">Catalogue records describe observed relationships. Execution "
          "requires a currently available model, account access and a reviewed usage limit. "
          "Registration alone does not establish availability or permission to run.</p>");

        n = aperture_push(acts, n, 8, "See every model", "and what depends on each", 2, 0, "/s/models/all");
        n = aperture_push(acts, n, 8, "See what the estate pays", "prices it holds evidence for", 1, 0, "/s/prices/observed");
        aperture_render(body, sizeof(body), "Model &middot; what it is and what depends on it",
                        ident, name, facts, acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "model", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* A working set, as something a person operates rather than a list.
     *
     * The estate's older collections name their members in an array somebody
     * typed: nothing can say why a publisher is in one, and nothing can propose
     * a new one. This reads the other shape -- criteria that ran, what they
     * proposed, and who accepted or rejected each candidate -- so the set is
     * legible as work in progress instead of a frozen answer.
     *
     * The distinction the page has to keep is the one the contract keeps: the
     * criteria propose, and accepting is somebody's act. A pending candidate is
     * not a member of anything yet.
     */
    if (!strcmp(kind, "sets")) {
        sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL;
        char facts[131072]; size_t fw = 0;
        long sets = 0, cands = 0;
        if (!db) { subject_miss("No record store is readable here."); return 1; }

        if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM discovery_set WHERE status='published'",
                               -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW)
            sets = (long)sqlite3_column_int64(st, 0);
        if (st) { sqlite3_finalize(st); st = NULL; }
        if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM discovery_candidate WHERE status='published'",
                               -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW)
            cands = (long)sqlite3_column_int64(st, 0);
        if (st) { sqlite3_finalize(st); st = NULL; }

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<dl class=\"flow-facts\">"
          "<div><dt>Working sets</dt><dd>%ld</dd></div>"
          "<div><dt>Candidates</dt><dd>%ld</dd></div>"
          "<div><dt>Criterion kinds</dt><dd>%d</dd></div></dl>"
          "<p class=\"subj-note\">A set is criteria that run against every publisher in the ledger, "
          "not a list of members. Each criterion may only read a column declared for it, so a set "
          "cannot express a judgement this estate holds no data for.</p>"
          "<h3 class=\"room-h\">The sets</h3>",
          sets, cands, (int)BF_CRITERION_KIND_COUNT);

        if (sqlite3_prepare_v2(db,
              "SELECT s.set_id, s.label, s.criteria, COALESCE(s.last_run_at,''),"
              " (SELECT COUNT(*) FROM discovery_candidate c WHERE c.set_id=s.set_id AND c.verdict='pending'),"
              " (SELECT COUNT(*) FROM discovery_candidate c WHERE c.set_id=s.set_id AND c.verdict='accepted'),"
              " (SELECT COUNT(*) FROM discovery_candidate c WHERE c.set_id=s.set_id AND c.verdict='rejected')"
              " FROM discovery_set s WHERE s.status='published' ORDER BY s.label",
              -1, &st, NULL) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 2048) {
                char eid[160], el[320], ec[640];
                html_escape((const char *)sqlite3_column_text(st, 0), eid, sizeof(eid));
                html_escape((const char *)sqlite3_column_text(st, 1), el, sizeof(el));
                html_escape((const char *)sqlite3_column_text(st, 2), ec, sizeof(ec));
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                  "<a class=\"desk-row\" href=\"/s/set/%s\"><span class=\"desk-l\">%s</span>"
                  "<span class=\"desk-a\">%lld accepted, %lld waiting</span>"
                  "<span class=\"desk-s\">%s%s</span></a>",
                  eid, el,
                  (long long)sqlite3_column_int64(st, 5),
                  (long long)sqlite3_column_int64(st, 4), ec,
                  sqlite3_column_text(st, 3)[0] ? "" : " &middot; never run");
            }
            sqlite3_finalize(st); st = NULL;
        }
        sqlite3_close(db);

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">What a criterion may ask</h3>");
        {
            size_t ci;
            for (ci = 0; ci < BF_CRITERION_KIND_COUNT && fw < sizeof(facts) - 2048; ci++) {
                const BfCriterionKind *k = &bf_criterion_kinds[ci];
                char el[160], em[800], eci[800];
                html_escape(k->label, el, sizeof(el));
                html_escape(k->means, em, sizeof(em));
                html_escape(k->cannot_infer, eci, sizeof(eci));
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                  "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
                  "<span class=\"desk-a\">%s.%s</span>"
                  "<span class=\"desk-s\">%s%.0s</span></div>",
                  el, k->over, k->column, em, eci);
            }
        }

        n = aperture_push(acts, n, 8, "See how densely each source files",
                          "what a set would be drawing from", 2, 0, "/sources");
        aperture_render(body, sizeof(body), "Working sets &middot; criteria that run",
                        ident, "Publishers the criteria propose, and who decided", facts,
                        acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "sets", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* One set: its criteria, its standing, and every candidate with its verdict. */
    if (!strcmp(kind, "set")) {
        sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL;
        char facts[262144]; size_t fw = 0;
        char label[320] = {0}, criteria[1024] = {0}, last_run[64] = {0}, opened[96] = {0};
        long pending = 0, accepted = 0, rejected = 0;
        int human_only = 0;
        {
            /* Counted from the contract rather than written into the sentence:
             * the number of verdicts a machine may not set is the whole reason
             * this page distinguishes proposing from deciding. */
            size_t vi;
            for (vi = 0; vi < BF_CANDIDATE_VERDICT_COUNT; vi++)
                if (!bf_verdict_machine_may_set(bf_candidate_verdicts[vi].verdict)) human_only++;
        }
        if (!db) { subject_miss("No record store is readable here."); return 1; }
        if (sqlite3_prepare_v2(db,
              "SELECT label, criteria, COALESCE(last_run_at,''), opened_by FROM discovery_set"
              " WHERE set_id=?1 AND status='published'", -1, &st, NULL) != SQLITE_OK ||
            (sqlite3_bind_text(st, 1, ident, -1, SQLITE_STATIC), sqlite3_step(st)) != SQLITE_ROW) {
            if (st) sqlite3_finalize(st);
            sqlite3_close(db);
            subject_miss("No working set with that identifier.");
            return 1;
        }
        snprintf(label, sizeof(label), "%s", (const char *)sqlite3_column_text(st, 0));
        snprintf(criteria, sizeof(criteria), "%s", (const char *)sqlite3_column_text(st, 1));
        snprintf(last_run, sizeof(last_run), "%s", (const char *)sqlite3_column_text(st, 2));
        snprintf(opened, sizeof(opened), "%s", (const char *)sqlite3_column_text(st, 3));
        sqlite3_finalize(st); st = NULL;

        if (sqlite3_prepare_v2(db,
              "SELECT verdict, COUNT(*) FROM discovery_candidate WHERE set_id=?1"
              " AND status='published' GROUP BY verdict", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, ident, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW) {
                const char *v = (const char *)sqlite3_column_text(st, 0);
                long nrows = (long)sqlite3_column_int64(st, 1);
                if (!strcmp(v, "pending")) pending = nrows;
                else if (!strcmp(v, "accepted")) accepted = nrows;
                else if (!strcmp(v, "rejected")) rejected = nrows;
            }
            sqlite3_finalize(st); st = NULL;
        }

        {
            char eo[160];
            html_escape(opened, eo, sizeof(eo));
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<dl class=\"flow-facts\">"
              "<div><dt>Accepted</dt><dd>%ld</dd></div>"
              "<div><dt>Waiting on a person</dt><dd>%ld</dd></div>"
              "<div><dt>Rejected</dt><dd>%ld</dd></div>"
              "<div><dt>Opened by</dt><dd>%s</dd></div>"
              "<div><dt>Last run</dt><dd>%s</dd></div></dl>",
              accepted, pending, rejected, eo, last_run[0] ? last_run : "never");
        }

        /* The criteria, each read back through the compiled table so the page
         * shows the column it actually queries rather than the text it was
         * given. */
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">What it asks</h3>");
        {
            char work[1024];
            char *save = NULL, *tok;
            snprintf(work, sizeof(work), "%s", criteria);
            for (tok = strtok_r(work, ";", &save); tok; tok = strtok_r(NULL, ";", &save)) {
                char *eq = strchr(tok, '=');
                const BfCriterionKind *k;
                char el[160], ev[320], em[800], eci[800];
                if (!eq) continue;
                *eq = '\0';
                k = bf_criterion_kind(tok);
                html_escape(eq + 1, ev, sizeof(ev));
                if (!k) {
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
                      "<span class=\"desk-a\">not declared</span>"
                      "<span class=\"desk-s\">this set names a criterion kind the contract does not "
                      "declare, so a run of it would be refused rather than guessed at</span></div>",
                      ev);
                    continue;
                }
                html_escape(k->label, el, sizeof(el));
                html_escape(k->means, em, sizeof(em));
                html_escape(k->cannot_infer, eci, sizeof(eci));
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                  "<div class=\"desk-row\"><span class=\"desk-l\">%s %s</span>"
                  "<span class=\"desk-a\">%s.%s</span>"
                  "<span class=\"desk-s\">%s%.0s</span></div>",
                  el, ev, k->over, k->column, em, eci);
            }
        }

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">What the criteria proposed</h3>"
          "<div data-bf-list data-bf-row-height=\"34\">");
        if (sqlite3_prepare_v2(db,
              "SELECT root_host, COALESCE(display_name,''), verdict, COALESCE(decided_by,''),"
              " COALESCE(reason,''),updated_at,evaluated_at FROM discovery_candidate WHERE set_id=?1 AND status='published'"
              " ORDER BY CASE verdict WHEN 'accepted' THEN 0 WHEN 'pending' THEN 1 ELSE 2 END,"
              " root_host", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, ident, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 4096) {
                const char *verdict = (const char *)sqlite3_column_text(st, 2);
                const char *by = (const char *)sqlite3_column_text(st, 3);
                char eh[300], en[400], ew[600], eu[140], ee[140], eset[260];
                size_t vi;
                const char *vlabel = verdict;
                for (vi = 0; vi < BF_CANDIDATE_VERDICT_COUNT; vi++)
                    if (!strcmp(bf_candidate_verdicts[vi].verdict, verdict))
                        vlabel = bf_candidate_verdicts[vi].label;
                html_escape((const char *)sqlite3_column_text(st, 0), eh, sizeof(eh));
                html_escape((const char *)sqlite3_column_text(st, 1), en, sizeof(en));
                html_escape((const char *)sqlite3_column_text(st, 4), ew, sizeof(ew));
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                  "<a class=\"desk-row\" href=\"/s/institution/%s\">"
                  "<span class=\"desk-l\">%s</span><span class=\"desk-a\">%s</span>"
                  "<span class=\"desk-s\">%s%s%s%s</span></a>",
                  eh, en[0] ? en : eh, vlabel, eh,
                  by[0] ? " &middot; " : "", by[0] ? by : "",
                  ew[0] ? "" : "");
                if (may_write && verdict && !strcmp(verdict,"pending") && fw < sizeof(facts)-4096) {
                    const char *u=(const char *)sqlite3_column_text(st,5);
                    const char *e=(const char *)sqlite3_column_text(st,6);
                    html_escape(ident,eset,sizeof(eset));
                    html_escape(u?u:"",eu,sizeof(eu));
                    html_escape(e?e:"",ee,sizeof(ee));
                    fw += (size_t)snprintf(facts+fw,sizeof(facts)-fw,
                      "<details class=\"source-review\" style=\"padding:10px 14px;border-bottom:1px solid #bcc9bc\">"
                      "<summary style=\"cursor:pointer\">Accept or reject this proposed publisher</summary>"
                      "<form method=\"post\" action=\"/discovery/decide\" style=\"display:grid;gap:8px;padding:12px 0\">"
                      "<input type=\"hidden\" name=\"set\" value=\"%s\">"
                      "<input type=\"hidden\" name=\"publisher\" value=\"%s\">"
                      "<input type=\"hidden\" name=\"updated\" value=\"%s\">"
                      "<input type=\"hidden\" name=\"evaluated\" value=\"%s\">"
                      "<label>Why does this publisher belong, or not belong, in this working set?"
                      "<textarea name=\"reason\" minlength=\"8\" maxlength=\"500\" required "
                      "placeholder=\"Cite the source and reasoning for this decision\" "
                      "style=\"display:block;width:100%%;min-height:76px\"></textarea></label>"
                      "<div style=\"display:flex;gap:8px;flex-wrap:wrap\">"
                      "<button type=\"submit\" name=\"verdict\" value=\"accepted\">Accept for further consideration</button>"
                      "<button type=\"submit\" name=\"verdict\" value=\"rejected\">Reject this publisher</button>"
                      "</div><small>Defer by leaving pending. A choice does not read, licence, or verify source material.</small>"
                      "</form></details>",
                      eset,eh,eu,ee);
                }
            }
            sqlite3_finalize(st); st = NULL;
        }
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "</div>");
        sqlite3_close(db);

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<p class=\"subj-note\">%ld publishers are waiting on a person. The criteria proposed them "
          "and that is all a run may do: %d of the declared verdicts cannot be set by a machine, so "
          "accepting or rejecting carries the name of whoever did it. A proposed publisher has not "
          "been read, asked for, or endorsed, and matching every criterion is a filter over facts "
          "this estate recorded rather than a judgement about the publisher.</p>", pending, human_only);

        /* The same reviewed sources can be used in several legitimate brief
         * contexts. No separate frontend source store, no imported claims. */
        if (may_write && fw < sizeof(facts)-2400) {
            char es[260];
            html_escape(ident,es,sizeof(es));
            fw += (size_t)snprintf(facts+fw,sizeof(facts)-fw,
               "<section class=\"desk-row\" style=\"display:block;padding:20px\">"
               "<h3>Continue accepted sources into a review brief</h3>"
               "<p>Only owner-accepted publishers with admitted SourceFrontier channels "
               "and retained ItemArchive material can contribute. Rejected sources are excluded.</p>"
               "<nav style=\"display:flex;gap:12px;flex-wrap:wrap\">"
               "<a href=\"/discovery/brief?set=%s&amp;domain=cross-domain\">Cross-domain source review</a>"
               "<a href=\"/discovery/brief?set=%s&amp;domain=itad\">ITAD source review</a>"
               "<a href=\"/discovery/brief?set=%s&amp;domain=property\">Property source review</a>"
               "<a href=\"/discovery/brief?set=%s&amp;domain=upper90\">Upper90 source review</a>"
               "<a href=\"/discovery/brief?set=%s&amp;domain=civic\">Civic source review</a>"
               "</nav><p>A recipient brief is a read-only proposal, never an effect or approval.</p></section>",
               es,es,es,es,es);
        }
        n = aperture_push(acts, n, 8, "See every working set", "and what a criterion may ask", 2, 0, "/s/sets/all");
        aperture_render(body, sizeof(body), "Working set &middot; proposed, then decided",
                        ident, label, facts, acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "set", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* The thirty grammars, and what is actually under them.
     *
     * A list of thirty product names is a wish list. What makes it work is the
     * layer below: thirty vocabularies expressed as the primitives they need,
     * each primitive bound to the organ that already owns it, so a proposal from
     * a crowdfunding pitch, a proposal in a DAO and a proposal at a parent
     * association are one cell under three names rather than three systems that
     * can never be counted together.
     *
     * Everything on this page is computed from that binding. How many of the
     * thirty share a primitive is counted, not asserted. A primitive no organ
     * owns says so, and every grammar depending on one inherits the gap rather
     * than presenting as ready.
     */
    if (!strcmp(kind, "grammars")) {
        char facts[262144]; size_t fw = 0;
        size_t i, j;
        int owned = 0, unowned = 0, clear = 0, blocked = 0;
        long shared_total = 0;

        for (i = 0; i < BF_INTERACTION_PRIMITIVE_COUNT; i++) {
            if (bf_interaction_primitives[i].owner[0]) owned++; else unowned++;
            shared_total += bf_interaction_primitives[i].shared_by;
        }
        for (i = 0; i < BF_CLONE_GRAMMAR_COUNT; i++) {
            if (bf_clone_grammars[i].unowned_needs) blocked++; else clear++;
        }

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<dl class=\"flow-facts\">"
          "<div><dt>Grammars</dt><dd>%d</dd></div>"
          "<div><dt>Primitives</dt><dd>%d</dd></div>"
          "<div><dt>With an organ</dt><dd>%d</dd></div>"
          "<div><dt>Nothing owns</dt><dd>%d</dd></div>"
          "<div><dt>Rest on solid ground</dt><dd>%d</dd></div></dl>"
          "<p class=\"subj-note\">Thirty vocabularies needing %ld primitive bindings between them, "
          "which is what makes them cheaper than three applications: %d cells carry all of it. "
          "Naming a grammar is provenance and nothing else -- no donor code runs here, and it is "
          "not a claim its authors know this estate exists or that this estate reproduces their "
          "product.</p>",
          (int)BF_CLONE_GRAMMAR_COUNT, (int)BF_INTERACTION_PRIMITIVE_COUNT,
          owned, unowned, clear, shared_total, (int)BF_INTERACTION_PRIMITIVE_COUNT);

        /* The primitives, most shared first, since that is the density claim. */
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">What they share</h3><div data-bf-list data-bf-row-height=\"34\">");
        {
            /* A selection sort over thirty-odd rows, because sorting a generated
             * table in place would mean copying it. */
            unsigned short done[BF_INTERACTION_PRIMITIVE_COUNT];
            memset(done, 0, sizeof(done));
            for (j = 0; j < BF_INTERACTION_PRIMITIVE_COUNT && fw < sizeof(facts) - 4096; j++) {
                size_t best = BF_INTERACTION_PRIMITIVE_COUNT;
                for (i = 0; i < BF_INTERACTION_PRIMITIVE_COUNT; i++) {
                    if (done[i]) continue;
                    if (best == BF_INTERACTION_PRIMITIVE_COUNT ||
                        bf_interaction_primitives[i].shared_by >
                          bf_interaction_primitives[best].shared_by) best = i;
                }
                if (best == BF_INTERACTION_PRIMITIVE_COUNT) break;
                done[best] = 1;
                {
                    const BfInteractionPrimitive *pr = &bf_interaction_primitives[best];
                    char el[128], eo[128], em[1024], eg[1024];
                    html_escape(pr->label, el, sizeof(el));
                    html_escape(pr->owner, eo, sizeof(eo));
                    html_escape(pr->means, em, sizeof(em));
                    html_escape(pr->unowned_because, eg, sizeof(eg));
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<a class=\"desk-row\" href=\"/s/primitive/%s\">"
                      "<span class=\"desk-l\">%s</span>"
                      "<span class=\"desk-a\">%u of %d</span>"
                      "<span class=\"desk-s\">%s%s%s</span></a>",
                      pr->primitive, el, (unsigned)pr->shared_by, (int)BF_CLONE_GRAMMAR_COUNT,
                      eo[0] ? "held by " : "nothing holds this: ",
                      eo[0] ? eo : eg,
                      eo[0] ? "" : "");
                }
            }
        }
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "</div>");

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">The thirty</h3><div data-bf-list data-bf-row-height=\"34\">");
        for (i = 0; i < BF_CLONE_GRAMMAR_COUNT && fw < sizeof(facts) - 4096; i++) {
            const BfCloneGrammar *g = &bf_clone_grammars[i];
            char es[192], eo[128], ex[1024];
            html_escape(g->studied, es, sizeof(es));
            html_escape(g->over, eo, sizeof(eo));
            html_escape(g->explodes, ex, sizeof(ex));
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<a class=\"desk-row\" href=\"/s/grammar/%s\">"
              "<span class=\"desk-l\">%s</span>"
              "<span class=\"desk-a\">%u cell%s%s</span>"
              "<span class=\"desk-s\">over %s &middot; %s</span></a>",
              g->grammar, es, (unsigned)g->need_count, g->need_count == 1 ? "" : "s",
              g->unowned_needs ? ", one missing" : "", eo, ex);
        }
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "</div>");

        if (blocked)
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<p class=\"subj-note\">%d of the thirty need a cell no organ owns yet, and they are "
              "marked rather than presented as ready. That is the honest shape of the work: the "
              "grammars are cheap because the cells are shared, and the ones that are not cheap are "
              "the ones waiting on something that does not exist.</p>", blocked);

        n = aperture_push(acts, n, 8, "See the governance grammar",
                          "the cell most of these share", 2, 0, "/s/governance/grammar");
        n = aperture_push(acts, n, 8, "See what the estate pays",
                          "the priced cell, with evidence", 1, 0, "/s/prices/observed");
        aperture_render(body, sizeof(body), "Grammars &middot; thirty vocabularies, one set of cells",
                        ident, "What each one would make legible, and what already holds it", facts,
                        acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "grammars", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* One grammar: the cells it needs, who holds each, and who else needs them.
     *
     * The last band is the whole argument. A grammar that shares nine of its ten
     * cells with other grammars is nine-tenths already built, and the page says
     * which grammars those are, so the claim is checkable rather than rhetorical.
     */
    if (!strcmp(kind, "grammar")) {
        const BfCloneGrammar *g = bf_clone_grammar(ident);
        char facts[262144]; size_t fw = 0;
        size_t i, j;
        if (!g) { subject_miss("No grammar of that name is declared."); return 1; }

        {
            char es[256], eo[128], ex[2048], eci[2048];
            html_escape(g->studied, es, sizeof(es));
            html_escape(g->over, eo, sizeof(eo));
            html_escape(g->explodes, ex, sizeof(ex));
            html_escape(g->cannot_infer, eci, sizeof(eci));
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<dl class=\"flow-facts\">"
              "<div><dt>Studied</dt><dd>%s</dd></div>"
              "<div><dt>Over</dt><dd>%s</dd></div>"
              "<div><dt>Cells</dt><dd>%u</dd></div>"
              "<div><dt>Waiting on</dt><dd>%u</dd></div>"
              "<div><dt>Wave</dt><dd>%u</dd></div></dl>"
              "<p class=\"subj-note\">%s</p>"
              "%.0s",
              es, eo, (unsigned)g->need_count, (unsigned)g->unowned_needs, (unsigned)g->wave,
              ex, eci);
        }

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">What it needs, and who holds it</h3>");
        for (i = 0; i < BF_INTERACTION_PRIMITIVE_COUNT && fw < sizeof(facts) - 6144; i++) {
            const BfInteractionPrimitive *pr = &bf_interaction_primitives[i];
            char el[128], eo[128], em[1024], eg[1024], eci[1024];
            if (!bf_grammar_needs(g, pr->primitive)) continue;
            html_escape(pr->label, el, sizeof(el));
            html_escape(pr->owner, eo, sizeof(eo));
            html_escape(pr->means, em, sizeof(em));
            html_escape(pr->unowned_because, eg, sizeof(eg));
            html_escape(pr->cannot_infer, eci, sizeof(eci));
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<a class=\"desk-row\" href=\"/s/primitive/%s\"><span class=\"desk-l\">%s</span>"
              "<span class=\"desk-a\">%s</span>"
              "<span class=\"desk-s\">%s%.0s</span></a>",
              pr->primitive, el,
              eo[0] ? eo : "nothing holds this",
              eo[0] ? em : eg, eci);
        }

        /* Who else needs the same cells. */
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">Who else needs the same cells</h3>");
        {
            int any = 0;
            for (j = 0; j < BF_CLONE_GRAMMAR_COUNT && fw < sizeof(facts) - 4096; j++) {
                const BfCloneGrammar *other = &bf_clone_grammars[j];
                int overlap = 0;
                char shared_names[512]; size_t sw = 0;
                if (other == g) continue;
                for (i = 0; i < BF_INTERACTION_PRIMITIVE_COUNT; i++) {
                    const char *name = bf_interaction_primitives[i].primitive;
                    if (!bf_grammar_needs(g, name) || !bf_grammar_needs(other, name)) continue;
                    overlap++;
                    if (sw < sizeof(shared_names) - 40)
                        sw += (size_t)snprintf(shared_names + sw, sizeof(shared_names) - sw,
                                               "%s%s", sw ? ", " : "", name);
                }
                if (overlap < 2) continue;
                {
                    char es[256], esh[600];
                    html_escape(other->studied, es, sizeof(es));
                    html_escape(shared_names, esh, sizeof(esh));
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<a class=\"desk-row\" href=\"/s/grammar/%s\"><span class=\"desk-l\">%s</span>"
                      "<span class=\"desk-a\">%d shared</span>"
                      "<span class=\"desk-s\">%s</span></a>",
                      other->grammar, es, overlap, esh);
                    any = 1;
                }
            }
            if (!any)
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                  "<p class=\"subj-note\">Nothing else needs two or more of the same cells, which "
                  "makes this one genuinely its own build rather than a vocabulary over shared "
                  "ground.</p>");
        }

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<p class=\"subj-note\">%s</p>",
          g->unowned_needs
            ? "One or more cells this needs is held by nothing, so this grammar is not ready however "
              "complete the rest of it looks. The missing cell is named above with what is absent, "
              "because a surface built on it would be describing a capability rather than carrying "
              "one."
            : "Every cell this needs is already held by an organ of this estate. That does not make "
              "the surface built, and it does mean nothing new has to be invented underneath it.");

        n = aperture_push(acts, n, 8, "See all thirty", "and the cells they share", 2, 0, "/s/grammars/all");
        {
            char eover[160];
            html_escape(g->studied, eover, sizeof(eover));
            aperture_render(body, sizeof(body), "Grammar &middot; a vocabulary over existing organs",
                            ident, eover, facts, acts, n, signed_in, may_write, bearer);
        }
        ambient_render(body, sizeof(body), strlen(body), "grammar", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* One cell: what it means, who holds it, and every grammar that needs it. */
    if (!strcmp(kind, "primitive")) {
        const BfInteractionPrimitive *pr = bf_interaction_primitive(ident);
        char facts[131072]; size_t fw = 0;
        size_t i;
        if (!pr) { subject_miss("No interaction primitive of that name is declared."); return 1; }
        {
            char eo[128], em[2048], eg[2048], eci[2048];
            html_escape(pr->owner, eo, sizeof(eo));
            html_escape(pr->means, em, sizeof(em));
            html_escape(pr->unowned_because, eg, sizeof(eg));
            html_escape(pr->cannot_infer, eci, sizeof(eci));
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<dl class=\"flow-facts\">"
              "<div><dt>Held by</dt><dd>%s</dd></div>"
              "<div><dt>Needed by</dt><dd>%u of %d</dd></div></dl>"
              "<p class=\"subj-note\">%s</p>"
              "%.0s",
              eo[0] ? eo : "nothing yet",
              (unsigned)pr->shared_by, (int)BF_CLONE_GRAMMAR_COUNT,
              eo[0] ? em : eg, eci);
        }
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">Every grammar that needs it</h3>");
        for (i = 0; i < BF_CLONE_GRAMMAR_COUNT && fw < sizeof(facts) - 3072; i++) {
            const BfCloneGrammar *g = &bf_clone_grammars[i];
            char es[256], ex[1024];
            if (!bf_grammar_needs(g, pr->primitive)) continue;
            html_escape(g->studied, es, sizeof(es));
            html_escape(g->explodes, ex, sizeof(ex));
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<a class=\"desk-row\" href=\"/s/grammar/%s\"><span class=\"desk-l\">%s</span>"
              "<span class=\"desk-a\">wave %u</span><span class=\"desk-s\">%s</span></a>",
              g->grammar, es, (unsigned)g->wave, ex);
        }
        n = aperture_push(acts, n, 8, "See all thirty", "and the cells they share", 2, 0, "/s/grammars/all");
        aperture_render(body, sizeof(body), "Cell &middot; one thing several vocabularies ask for",
                        ident, pr->label, facts, acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "primitive", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* A compiled application, as the estate can actually show it.
     *
     * The compiler has always parsed these apps in full and then published a
     * list of names: "surface:crm/CRM Notification", forty-four times. Every
     * part a reader would need in order to see the thing was dropped on the
     * floor -- the tab and section and column breaks that are the whole layout,
     * each field's kind, which fields are mandatory, what a link points at, and
     * which fields must never be shown at all. A renderer handed a name has to
     * invent all of it, and inventing it is how a compiled application turns
     * back into a hand-written page.
     *
     * So the compiler now writes the layout into a store, and this room reads
     * it. Nothing here is a Frappe runtime, a controller or a permission, and
     * no row is evidence that a record of any of these DocTypes exists in this
     * estate. It is the shape a reader would be shown if one did.
     */
    if (!strcmp(kind, "apps")) {
        const char *path = getenv("BONFYRE_FRAPPE_SURFACE_DB");
        sqlite3 *db = NULL; sqlite3_stmt *st = NULL; char uri[PATH_MAX];
        char facts[262144]; size_t fw = 0;
        long programs = 0, fields = 0, rendered = 0, withheld = 0, laid = 0;
        long lists = 0, spaces = 0, apertures = 0;

        if (!path || !*path ||
            bf_sqlite_readonly_uri(uri, sizeof(uri), path) >= (int)sizeof(uri) ||
            sqlite3_open_v2(uri, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) != SQLITE_OK) {
            if (db) sqlite3_close(db);
            subject_miss("No compiled application surfaces are readable here.");
            return 1;
        }

        if (sqlite3_prepare_v2(db,
              "SELECT COUNT(*), SUM(field_count), SUM(rendered), SUM(withheld), SUM(layout),"
              " SUM(organs LIKE '%list%'), SUM(organs LIKE '%space%'), SUM(organs LIKE '%aperture%')"
              " FROM frappe_program WHERE ?1='' OR app=?1", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, strcmp(ident, "all") ? ident : "", -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                programs = (long)sqlite3_column_int64(st, 0);
                fields   = (long)sqlite3_column_int64(st, 1);
                rendered = (long)sqlite3_column_int64(st, 2);
                withheld = (long)sqlite3_column_int64(st, 3);
                laid     = (long)sqlite3_column_int64(st, 4);
                lists    = (long)sqlite3_column_int64(st, 5);
                spaces   = (long)sqlite3_column_int64(st, 6);
                apertures= (long)sqlite3_column_int64(st, 7);
            }
            sqlite3_finalize(st); st = NULL;
        }
        if (!programs) {
            sqlite3_close(db);
            subject_miss("No application of that name has been compiled here.");
            return 1;
        }

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<dl class=\"flow-facts\">"
          "<div><dt>Record kinds</dt><dd>%ld</dd></div>"
          "<div><dt>Fields</dt><dd>%ld</dd></div>"
          "<div><dt>Renderable</dt><dd>%ld</dd></div>"
          "<div><dt>Withheld</dt><dd>%ld</dd></div>"
          "<div><dt>Layout breaks</dt><dd>%ld</dd></div></dl>",
          programs, fields, rendered, withheld, laid);
        if (strcmp(ident, "all")) {   /* Its record shapes that what this estate reads would fill. */
            char *more = NULL; size_t more_len = 0; FILE *m = open_memstream(&more, &more_len);
            if (m) {
                BfDetailFocus focus = {BF_DETAIL_APP, ident, NULL, 1};
                bf_detail_continue(m, &focus);
                fclose(m);
                if (more && more_len && more_len + fw + 4096 < sizeof(facts)) { memcpy(facts + fw, more, more_len); fw += more_len; facts[fw] = 0; }
                free(more);
            }
        }

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">What the browser is asked to do</h3>"
          "<div class=\"desk-row\"><span class=\"desk-l\">Virtualised list</span>"
          "<span class=\"desk-a\">%ld record kinds</span>"
          "<span class=\"desk-s\">a child table can hold more rows than a page can lay out at "
          "once; the rows are sent as HTML and positioned as you reach them</span></div>"
          "<div class=\"desk-row\"><span class=\"desk-l\">Spatial index</span>"
          "<span class=\"desk-a\">%ld record kinds</span>"
          "<span class=\"desk-s\">a geolocation field asks what is in view, which is answered "
          "over material already delivered and never by reaching for more</span></div>"
          "<div class=\"desk-row\"><span class=\"desk-l\">Progressive disclosure</span>"
          "<span class=\"desk-a\">%ld record kinds</span>"
          "<span class=\"desk-s\">a collapsible section is the DocType's own statement that its "
          "contents are secondary, honoured rather than flattened</span></div>"
          "<p class=\"subj-note\">The other %ld record kinds ask for no script at all, and saying "
          "so is the point: an organ named where none is needed is how a page acquires a script it "
          "never uses. Every field below is server-rendered; none of these organs is required in "
          "order to read it.</p>",
          lists, spaces, apertures, programs - lists);

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">%s</h3><div data-bf-list data-bf-row-height=\"34\">",
          !strcmp(ident, "all") ? "Every compiled record kind" : "Its record kinds");
        if (sqlite3_prepare_v2(db,
              "SELECT slug, app, doctype, module, field_count, rendered, withheld, organs,"
              " submittable, child"
              " FROM frappe_program WHERE ?1='' OR app=?1"
              " ORDER BY app, field_count DESC, doctype", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, strcmp(ident, "all") ? ident : "", -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 1024) {
                char edt[160], emod[96], eorg[96], eid[400];
                html_escape((const char *)sqlite3_column_text(st, 2), edt, sizeof(edt));
                html_escape((const char *)sqlite3_column_text(st, 3), emod, sizeof(emod));
                html_escape((const char *)sqlite3_column_text(st, 7), eorg, sizeof(eorg));
                html_escape((const char *)sqlite3_column_text(st, 0), eid, sizeof(eid));
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                  "<a class=\"desk-row\" href=\"/s/record-kind/%s\">"
                  "<span class=\"desk-l\">%s</span>"
                  "<span class=\"desk-a\">%lld field%s</span>"
                  "<span class=\"desk-s\">%s%s%s%s%s%s</span></a>",
                  eid, edt, (long long)sqlite3_column_int64(st, 4),
                  sqlite3_column_int64(st, 4) == 1 ? "" : "s",
                  emod[0] ? emod : "no module",
                  sqlite3_column_int(st, 8) ? " &middot; submittable" : "",
                  sqlite3_column_int(st, 9) ? " &middot; a part of another record" : "",
                  sqlite3_column_int64(st, 6) ? " &middot; some fields withheld" : "",
                  eorg[0] ? " &middot; needs " : "", eorg);
            }
            sqlite3_finalize(st); st = NULL;
        }
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "</div>");

        sqlite3_close(db);
        n = aperture_push(acts, n, 8, "Read the governance grammar",
                          "what a record kind would have to answer to", 2, 0, "/s/governance/grammar");
        n = aperture_push(acts, n, 8, "See what the estate pays",
                          "prices it holds evidence for", 1, 0, "/s/prices/observed");
        aperture_render(body, sizeof(body), "Application &middot; compiled record kinds",
                        ident, "Layout, field kinds and what is withheld", facts,
                        acts, n, signed_in, may_write, bearer);
        ambient_render(body, sizeof(body), strlen(body), "apps", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* One compiled record kind, laid out as it declares itself.
     *
     * Tabs, sections and columns are the DocType's own statement about what
     * belongs with what, and every one of them survived the compile and was
     * then thrown away at the last step. They are honoured here. A field that
     * this estate refuses to render is named with the reason rather than
     * omitted, because a missing field and a refused one are different facts.
     *
     * The cross-links at the end are derived from the fields, not decoration:
     * money fields mean the thing carries a price, so the room points at the
     * prices this estate actually has evidence for; a submittable record means
     * somebody's act commits somebody, so it points at the governance grammar
     * and the commitment rail; a field naming a student, parent or guardian
     * means the record reaches into a household, so it points at the planes
     * that answer only to a relationship.
     */
    if (!strcmp(kind, "record-kind")) {
        const char *path = getenv("BONFYRE_FRAPPE_SURFACE_DB");
        sqlite3 *db = NULL; sqlite3_stmt *st = NULL; char uri[PATH_MAX];
        char facts[262144]; size_t fw = 0;
        char app[96] = {0}, doctype[200] = {0}, module[96] = {0}, title_field[96] = {0};
        char program_id[768] = {0};
        char descr[1024] = {0}, organs[96] = {0};
        long fields = 0, rendered = 0, withheld = 0, laid = 0;
        int submittable = 0, child = 0, single = 0, tree = 0, read_only = 0, guest = 0;
        long money = 0, household_touch = 0, composition = 0, places = 0;

        if (!path || !*path ||
            bf_sqlite_readonly_uri(uri, sizeof(uri), path) >= (int)sizeof(uri) ||
            sqlite3_open_v2(uri, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) != SQLITE_OK) {
            if (db) sqlite3_close(db);
            subject_miss("No compiled application surfaces are readable here.");
            return 1;
        }
        if (sqlite3_prepare_v2(db,
              "SELECT app,doctype,module,title_field,description,organs,field_count,rendered,"
              " withheld,layout,submittable,child,single,tree,read_only,guest_readable,program_id"
              " FROM frappe_program WHERE slug=?1", -1, &st, NULL) != SQLITE_OK ||
            (sqlite3_bind_text(st, 1, ident, -1, SQLITE_STATIC), sqlite3_step(st)) != SQLITE_ROW) {
            if (st) sqlite3_finalize(st);
            sqlite3_close(db);
            subject_miss("No compiled record kind with that identifier.");
            return 1;
        }
        snprintf(app, sizeof(app), "%s", (const char *)sqlite3_column_text(st, 0));
        snprintf(doctype, sizeof(doctype), "%s", (const char *)sqlite3_column_text(st, 1));
        snprintf(module, sizeof(module), "%s", (const char *)sqlite3_column_text(st, 2));
        snprintf(title_field, sizeof(title_field), "%s", (const char *)sqlite3_column_text(st, 3));
        snprintf(descr, sizeof(descr), "%s", (const char *)sqlite3_column_text(st, 4));
        snprintf(organs, sizeof(organs), "%s", (const char *)sqlite3_column_text(st, 5));
        fields = (long)sqlite3_column_int64(st, 6);
        rendered = (long)sqlite3_column_int64(st, 7);
        withheld = (long)sqlite3_column_int64(st, 8);
        laid = (long)sqlite3_column_int64(st, 9);
        submittable = sqlite3_column_int(st, 10);
        child = sqlite3_column_int(st, 11);
        single = sqlite3_column_int(st, 12);
        tree = sqlite3_column_int(st, 13);
        read_only = sqlite3_column_int(st, 14);
        guest = sqlite3_column_int(st, 15);
        snprintf(program_id, sizeof(program_id), "%s", (const char *)sqlite3_column_text(st, 16));
        sqlite3_finalize(st); st = NULL;

        {
            char eapp[128], emod[128], edes[2048];
            html_escape(app, eapp, sizeof(eapp));
            html_escape(module, emod, sizeof(emod));
            html_escape(descr, edes, sizeof(edes));
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<dl class=\"flow-facts\">"
              "<div><dt>Application</dt><dd><a href=\"/s/apps/%s\">%s</a></dd></div>"
              "<div><dt>Module</dt><dd>%s</dd></div>"
              "<div><dt>Fields</dt><dd>%ld</dd></div>"
              "<div><dt>Renderable</dt><dd>%ld</dd></div>"
              "<div><dt>Withheld</dt><dd>%ld</dd></div>"
              "<div><dt>Layout breaks</dt><dd>%ld</dd></div></dl>%s%s%s",
              eapp, eapp, emod[0] ? emod : "&mdash;", fields, rendered, withheld, laid,
              edes[0] ? "<p class=\"subj-note\">" : "", edes, edes[0] ? "</p>" : "");
        }

        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">What kind of record it is</h3>"
          "<div class=\"desk-row\"><span class=\"desk-l\">Submittable</span>"
          "<span class=\"desk-a\">%s</span><span class=\"desk-s\">%s</span></div>"
          "<div class=\"desk-row\"><span class=\"desk-l\">Standing alone</span>"
          "<span class=\"desk-a\">%s</span><span class=\"desk-s\">%s</span></div>"
          "<div class=\"desk-row\"><span class=\"desk-l\">Readable without signing in</span>"
          "<span class=\"desk-a\">%s</span><span class=\"desk-s\">the DocType's own setting, "
          "carried here unchanged; this estate holds no records of it either way</span></div>",
          submittable ? "yes" : "no",
          submittable ? "submitting is an act somebody performs and it binds somebody: it answers "
                        "to the governance grammar, not to a page"
                      : "there is no submit step, so nothing about it is a commitment",
          child ? "no" : (single ? "one only" : "yes"),
          child ? "it exists only as a part of another record and is never addressed by itself"
                : (single ? "there is one of it, not a collection"
                          : (tree ? "records of it form a hierarchy" : "records of it stand independently")),
          guest ? "yes" : "no");

        /* The layout, as the DocType states it. */
        if (sqlite3_prepare_v2(db,
              "SELECT tab,tab_ord,tab_empty,section,section_ord,collapsible,column_ord,"
              " field,label,presentation,organ,required,read_only,in_list_view,reference,"
              " depends_on,withheld FROM frappe_field WHERE program_id=?1 ORDER BY ord",
              -1, &st, NULL) == SQLITE_OK) {
            int last_tab = -2, last_section = -2, last_column = -1;
            int open_section = 0;
            /* What a field depends on is said from its compiled plan. A store
             * written before the plans existed has no such table, and then the
             * DocType's own text is shown as it always was. */
            sqlite3_stmt *cond = NULL;
            if (sqlite3_prepare_v2(db,
                  "SELECT property,kind,said FROM frappe_condition"
                  " WHERE program_id=?1 AND field=?2 ORDER BY property DESC",
                  -1, &cond, NULL) != SQLITE_OK) cond = NULL;
            sqlite3_bind_text(st, 1, program_id, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 2048) {
                int tab_ord = sqlite3_column_int(st, 1), section_ord = sqlite3_column_int(st, 4);
                int column_ord = sqlite3_column_int(st, 6);
                const char *why = (const char *)sqlite3_column_text(st, 16);
                char etab[160], esec[160], efield[160], elabel[160], eref[200], edep[400], ewhy[400];
                html_escape((const char *)sqlite3_column_text(st, 0), etab, sizeof(etab));
                html_escape((const char *)sqlite3_column_text(st, 3), esec, sizeof(esec));
                html_escape((const char *)sqlite3_column_text(st, 7), efield, sizeof(efield));
                html_escape((const char *)sqlite3_column_text(st, 8), elabel, sizeof(elabel));
                html_escape((const char *)sqlite3_column_text(st, 14), eref, sizeof(eref));
                html_escape((const char *)sqlite3_column_text(st, 15), edep, sizeof(edep));
                html_escape(why ? why : "", ewhy, sizeof(ewhy));

                char econd[1200];
                size_t cw = 0;
                econd[0] = '\0';
                if (cond) {
                    sqlite3_reset(cond);
                    sqlite3_bind_text(cond, 1, program_id, -1, SQLITE_STATIC);
                    sqlite3_bind_text(cond, 2, (const char *)sqlite3_column_text(st, 7), -1, SQLITE_TRANSIENT);
                    while (sqlite3_step(cond) == SQLITE_ROW && cw < sizeof(econd) - 520) {
                        const char *prop = (const char *)sqlite3_column_text(cond, 0);
                        const char *kind = (const char *)sqlite3_column_text(cond, 1);
                        const char *lead = "", *tail = "";
                        char esaid[400];
                        int says = 0;
                        if (!prop || !kind) continue;
                        html_escape((const char *)sqlite3_column_text(cond, 2), esaid, sizeof(esaid));
                        if (!strcmp(prop, "visible")) {
                            if (!strcmp(kind, "words")) { lead = "only when "; says = 1; }
                            else if (!strcmp(kind, "depends")) { lead = "shown depending on "; says = 1; }
                            else if (!strcmp(kind, "never")) tail = "never shown: its rule names no field of this record";
                            else if (!strcmp(kind, "unevaluated")) tail = "shown by a rule this estate does not evaluate";
                        } else if (!strcmp(prop, "mandatory")) {
                            if (!strcmp(kind, "words")) { lead = "required when "; says = 1; }
                            else if (!strcmp(kind, "depends")) { lead = "required depending on "; says = 1; }
                            else if (!strcmp(kind, "always")) tail = "always required";
                            else if (!strcmp(kind, "unevaluated")) tail = "sometimes required, by a rule this estate does not evaluate";
                        } else if (!strcmp(prop, "read_only")) {
                            if (!strcmp(kind, "words")) { lead = "not editable when "; says = 1; }
                            else if (!strcmp(kind, "depends")) { lead = "editable depending on "; says = 1; }
                            else if (!strcmp(kind, "always")) tail = "never editable";
                            else if (!strcmp(kind, "unevaluated")) tail = "sometimes not editable, by a rule this estate does not evaluate";
                        }
                        if (!*lead && !*tail) continue;
                        cw += (size_t)snprintf(econd + cw, sizeof(econd) - cw, " &middot; %s%s%s",
                                               lead, says ? esaid : "", tail);
                    }
                } else if (edep[0]) {
                    snprintf(econd, sizeof(econd), " &middot; only when %s", edep);
                }

                if (tab_ord != last_tab) {
                    if (open_section) { fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "</div>"); open_section = 0; }
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<h3 class=\"room-h\">%s</h3>", etab[0] ? etab : "The record");
                    last_tab = tab_ord; last_section = -2; last_column = -1;
                }
                if (section_ord != last_section) {
                    if (open_section) fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "</div>");
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<div class=\"room-band\">%s%s%s",
                      esec[0] ? "<h4 class=\"room-sub\">" : "", esec,
                      esec[0] ? (sqlite3_column_int(st, 5) ? " <small>(secondary, shown on request)</small></h4>" : "</h4>") : "");
                    open_section = 1; last_section = section_ord; last_column = -1;
                }
                if (column_ord != last_column) last_column = column_ord;

                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                  "<div class=\"desk-row\"><span class=\"desk-l\">%s%s</span>"
                  "<span class=\"desk-a\">%s</span><span class=\"desk-s\">%s%s%s%s%s%s%s</span></div>",
                  elabel[0] ? elabel : (efield[0] ? efield : "Nothing here"),
                  sqlite3_column_int(st, 11) ? " <em>required</em>" : "",
                  *ewhy ? "withheld" : (const char *)sqlite3_column_text(st, 9),
                  *ewhy ? ewhy : "",
                  (!*ewhy && eref[0]) ? "points at " : "",
                  (!*ewhy) ? eref : "",
                  (!*ewhy && sqlite3_column_int(st, 12)) ? " &middot; not editable" : "",
                  (!*ewhy && sqlite3_column_int(st, 13)) ? " &middot; shown in a list" : "",
                  (!*ewhy) ? econd : "", "");

                if (!*ewhy) {
                    const char *pres = (const char *)sqlite3_column_text(st, 9);
                    const char *fname = (const char *)sqlite3_column_text(st, 7);
                    if (!strcmp(pres, "money")) money++;
                    if (!strcmp(pres, "composition")) composition++;
                    if (!strcmp(pres, "place")) places++;
                    /* A whole segment of the fieldname, and only a declared
                     * name. Asking whether the name merely contained one made
                     * no_of_employees -- a dropdown for how big a company is --
                     * into a household, and would have made every Frappe child
                     * row's parent into somebody's parent. */
                    if (fname && field_reaches_household(fname)) household_touch++;
                }
            }
            if (open_section) fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw, "</div>");
            if (cond) sqlite3_finalize(cond);
            sqlite3_finalize(st); st = NULL;
        }

        /* What the application counts from this kind of record, said from
         * the compiled read of each of its number cards and charts. A store
         * written before those reads existed has no such table and the page
         * says nothing about counting. */
        if (sqlite3_prepare_v2(db,
              "SELECT metric,status,said,needs,missing_operation,definition FROM frappe_metric"
              " WHERE doctype=?1 ORDER BY status, metric", -1, &st, NULL) == SQLITE_OK) {
            int counted = 0, kept = 0, by_company = 0;
            const char *cms_path = getenv("BONFYRE_CMS_DB");
            sqlite3 *cms = NULL;
            char cms_uri[PATH_MAX], table[128], companies[8][160];
            int company_count = 0;
            /* The records of a compiled kind live in the content type the
             * compiler names for it. Whether any are kept is read, not assumed. */
            {
                size_t pos = 0;
                const char *parts[] = { "frappe_", app, "_", doctype, NULL };
                for (size_t part = 0; parts[part] && pos + 1 < sizeof(table); ++part)
                    for (const unsigned char *c = (const unsigned char *)parts[part]; *c && pos + 1 < sizeof(table); ++c)
                        table[pos++] = isalnum(*c) ? (char)tolower(*c) : '_';
                while (pos > 1 && table[pos - 1] == '_') --pos;
                table[pos] = '\0';
            }
            if (cms_path && *cms_path &&
                bf_sqlite_readonly_uri(cms_uri, sizeof(cms_uri), cms_path) < (int)sizeof(cms_uri) &&
                sqlite3_open_v2(cms_uri, &cms, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) == SQLITE_OK) {
                sqlite3_stmt *probe = NULL;
                char sql[320];
                if (sqlite3_prepare_v2(cms, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1", -1, &probe, NULL) == SQLITE_OK) {
                    sqlite3_bind_text(probe, 1, table, -1, SQLITE_STATIC);
                    kept = sqlite3_step(probe) == SQLITE_ROW;
                }
                if (probe) sqlite3_finalize(probe);
                probe = NULL;
                /* The donor would ask the reader's own company. Nobody is asked
                 * here; each company these records name gets its own number,
                 * and no number is ever taken across all of them. */
                snprintf(sql, sizeof(sql), "SELECT DISTINCT \"company\" FROM \"%s\" WHERE \"company\" IS NOT NULL AND \"company\" != '' ORDER BY 1 LIMIT 8", table);
                if (kept && sqlite3_prepare_v2(cms, sql, -1, &probe, NULL) == SQLITE_OK) {
                    by_company = 1;
                    while (company_count < 8 && sqlite3_step(probe) == SQLITE_ROW)
                        snprintf(companies[company_count++], sizeof(companies[0]), "%s", (const char *)sqlite3_column_text(probe, 0));
                }
                if (probe) sqlite3_finalize(probe);
            }
            if (cms) sqlite3_close(cms);
            sqlite3_bind_text(st, 1, doctype, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW && fw < sizeof(facts) - 4096) {
                const char *status = (const char *)sqlite3_column_text(st, 1);
                const char *needs = (const char *)sqlite3_column_text(st, 3);
                const char *missing = (const char *)sqlite3_column_text(st, 4);
                const char *definition = (const char *)sqlite3_column_text(st, 5);
                char ename[400], esaid[800], eneeds[200], answer[1600];
                html_escape((const char *)sqlite3_column_text(st, 0), ename, sizeof(ename));
                html_escape((const char *)sqlite3_column_text(st, 2), esaid, sizeof(esaid));
                html_escape(needs ? needs : "", eneeds, sizeof(eneeds));
                for (char *c = eneeds; *c; ++c) *c = (char)tolower((unsigned char)*c);
                if (!counted++) {
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<h3 class=\"room-h\">What is counted from it</h3>");
                    if (!kept)
                        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                          "<p class=\"flow-sub\">No records of this kind are kept here yet, so these say "
                          "what would be counted and give no number.</p>");
                    else if (!signed_in)
                        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                          "<p class=\"flow-sub\">Records of this kind are kept here. Their numbers are "
                          "said to somebody signed in, not to a stranger.</p>");
                }
                if (status && !strcmp(status, "compiled")) {
                    int answered = 0;
                    size_t aw = 0;
                    answer[0] = '\0';
                    /* A number is drawn from records, and so is a company's
                     * name. Neither is said to somebody not signed in. */
                    if (kept && signed_in && definition && *definition) {
                        if (needs && !strcmp(needs, "Company")) {
                            for (int c = 0; by_company && c < company_count && aw + 600 < sizeof(answer); ++c) {
                                char one[480], ecompany[200];
                                if (!metric_answer(cms_path, table, definition, companies[c], one, sizeof(one))) continue;
                                html_escape(companies[c], ecompany, sizeof(ecompany));
                                aw += (size_t)snprintf(answer + aw, sizeof(answer) - aw, "%sat %s: %s", answered ? "; " : "", ecompany, one);
                                answered = 1;
                            }
                        } else if (!needs || !*needs) {
                            answered = metric_answer(cms_path, table, definition, NULL, answer, sizeof(answer));
                        }
                    }
                    if (answered)
                        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                          "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
                          "<span class=\"desk-a\">%s</span>"
                          "<span class=\"desk-s\">%s, from the records kept here</span></div>",
                          ename, answer, esaid);
                    else if (kept && signed_in && answer[0])
                        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                          "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
                          "<span class=\"desk-s\">%s; no number, because the records kept here do not carry %s</span></div>",
                          ename, esaid, answer);
                    else
                        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                          "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
                          "<span class=\"desk-s\">%s%s%s</span></div>",
                          ename, esaid, eneeds[0] ? ", once it is said which " : "", eneeds);
                } else
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<div class=\"desk-row\"><span class=\"desk-l\">%s</span>"
                      "<span class=\"desk-s\">not counted here: %s</span></div>", ename,
                      missing && !strncmp(missing, "report:", 7) ? "the application works it out with a report of its own" :
                      missing && !strncmp(missing, "method:", 7) ? "the application works it out with code of its own" :
                      missing && !strncmp(missing, "source:", 7) ? "the application draws it from a source of its own" :
                      missing && !strcmp(missing, "child_table_metric") ? "it counts rows of a table kept inside each record" :
                      "it needs something this estate does not do");
            }
            sqlite3_finalize(st); st = NULL;
        }
        sqlite3_close(db);

        /* What this record kind reaches into, read off its own fields. */
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<h3 class=\"room-h\">What it reaches into</h3>");
        if (money)
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<a class=\"desk-row\" href=\"/s/prices/observed\">"
              "<span class=\"desk-l\">Money</span><span class=\"desk-a\">%ld field%s</span>"
              "<span class=\"desk-s\">it carries amounts, so it carries a price somebody set. This "
              "estate quotes only what it can evidence being charged, never a price of its "
              "own invention</span></a>", money, money == 1 ? "" : "s");
        if (submittable)
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<a class=\"desk-row\" href=\"/s/governance/grammar\">"
              "<span class=\"desk-l\">An act that binds</span><span class=\"desk-a\">submittable</span>"
              "<span class=\"desk-s\">submitting one of these is somebody promising something to "
              "somebody. A promise is never a delivery, and nothing on a page discharges one</span></a>");
        if (household_touch)
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<div class=\"desk-row\"><span class=\"desk-l\">A household</span>"
              "<span class=\"desk-a\">%ld field%s</span>"
              "<span class=\"desk-s\">it names people who belong to one. Reaching them is a plane "
              "that answers only to a relationship the household says it holds, never to this "
              "estate</span></div>", household_touch, household_touch == 1 ? "" : "s");
        if (composition)
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<div class=\"desk-row\"><span class=\"desk-l\">Other records</span>"
              "<span class=\"desk-a\">%ld tables</span>"
              "<span class=\"desk-s\">it is composed of rows of other record kinds, which is why "
              "this page asks the browser to lay out a list rather than a form</span></div>",
              composition);
        if (places)
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<div class=\"desk-row\"><span class=\"desk-l\">Somewhere</span>"
              "<span class=\"desk-a\">%ld field%s</span>"
              "<span class=\"desk-s\">it names a place, indexed in the reader's own tab over "
              "material already sent</span></div>", places, places == 1 ? "" : "s");
        if (!money && !submittable && !household_touch && !composition && !places)
            fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
              "<p class=\"subj-note\">Nothing: it holds its own fields and reaches nowhere else "
              "this estate can name.</p>");

        /* Who here would recognise the thing.
         *
         * The operator grammar and the compiled record kinds were two published
         * vocabularies with nothing between them: a PTA officer existed as an
         * archetype, 1,152 layouts existed, and nothing said which of them a
         * PTA officer has any business operating. The pairing is declared in
         * CUE and bound to a property measured off this DocType's own fields,
         * so it is neither a resemblance read out of prose nor a permission. */
        {
            size_t ai; int named = 0;
            for (ai = 0; ai < BF_RECORD_AFFINITY_COUNT && fw < sizeof(facts) - 2048; ai++) {
                const BfRecordAffinity *af = &bf_record_affinities[ai];
                const BfOperatorArchetype *op = NULL;
                size_t oi;
                long have = 0;
                if (!strcmp(af->needs, "money")) have = money;
                else if (!strcmp(af->needs, "composition")) have = composition;
                else if (!strcmp(af->needs, "place")) have = places;
                else if (!strcmp(af->needs, "household")) have = household_touch;
                else if (!strcmp(af->needs, "submittable")) have = submittable;
                if (!have) continue;
                for (oi = 0; oi < BF_OPERATOR_ARCHETYPE_COUNT; oi++)
                    if (!strcmp(bf_operator_archetypes[oi].archetype, af->archetype)) {
                        op = &bf_operator_archetypes[oi];
                        break;
                    }
                if (!op) continue;
                if (!named) {
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<h3 class=\"room-h\">Who here would recognise it</h3>");
                    named = 1;
                }
                {
                    char el[192], ebe[640], eci[640];
                    html_escape(op->label, el, sizeof(el));
                    html_escape(af->because, ebe, sizeof(ebe));
                    html_escape(af->cannot_infer, eci, sizeof(eci));
                    fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                      "<a class=\"desk-row\" href=\"/s/operators/archetypes\">"
                      "<span class=\"desk-l\">%s</span><span class=\"desk-a\">%s</span>"
                      "<span class=\"desk-s\">%s%.0s</span></a>",
                      el, af->needs, ebe, eci);
                }
            }
            if (named)
                fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
                  "<p class=\"subj-note\">Naming somebody beside a record kind says who would "
                  "already know what it is for. It is not permission to act on one, not a role "
                  "anybody holds here, and not a claim this estate holds a record of this kind "
                  "for anyone.</p>");
        }

        {   /* What this estate has read that would be kept in this shape. */
            char *more = NULL; size_t more_len = 0; FILE *m = open_memstream(&more, &more_len);
            if (m) {
                BfDetailFocus focus = {BF_DETAIL_RECORD, ident, NULL, 1};
                bf_detail_continue(m, &focus);
                fclose(m);
                if (more && more_len && more_len + fw + 4096 < sizeof(facts)) { memcpy(facts + fw, more, more_len); fw += more_len; facts[fw] = 0; }
                free(more);
            }
        }
        fw += (size_t)snprintf(facts + fw, sizeof(facts) - fw,
          "<p class=\"subj-note\">This is the shape of a record, not a record. No document of this "
          "kind exists in this estate, nothing here runs that application's code, and none of it "
          "is a permission, a controller or an effect. %s%s%s%s</p>",
          read_only ? "The DocType declares itself read-only, and that is carried across unchanged. " : "",
          title_field[0] ? "A record of it would be known by its " : "",
          title_field[0] ? title_field : "",
          title_field[0] ? "." : "");

        n = aperture_push(acts, n, 8, "See the whole application", "its other record kinds", 2, 0, "/s/apps/all");
        n = aperture_push(acts, n, 8, "Read the governance grammar", "what an act has to answer to", 1, 0, "/s/governance/grammar");
        {
            char etitle[320];
            html_escape(doctype, etitle, sizeof(etitle));
            aperture_render(body, sizeof(body), "Record kind &middot; the shape, not a record",
                            ident, etitle, facts, acts, n, signed_in, may_write, bearer);
        }
        ambient_render(body, sizeof(body), strlen(body), "record-kind", ident);
        aperture_close(body, sizeof(body));
        flow_reply(body);
        return 1;
    }

    /* Everything else the matrix actually holds. Three hand-written kinds and
     * a refusal for the other four hundred subjects was the estate declining
     * to answer at its own canonical address. */
    if (subject_generic(kind, ident, signed_in, may_write, bearer, auth_db)) return 1;

    subject_miss("No subject with that identifier.</p>");
    return 1;
}

/* ── LENS: selection and representation, separately ──────────────────────
 *
 * Source E's point is that a lens alters two things independently: what enters
 * the stream, and how those objects should appear. So the two are separate
 * fields here rather than one "feed type" -- a lens over the same selection can
 * be read as a list, a ledger of values, or a set of things to act on, and a
 * different selection can reuse a representation.
 *
 * A lens can be entered, forked with a different parameter, and combined with
 * another. None of that is a stored preference: the lens is in the address, so
 * sharing the address shares the lens.
 */
typedef struct {
    const char *slug;
    const char *title;
    const char *selection;      /* what enters, in words */
    const char *representation; /* how it should appear */
    const char *sql;            /* the selection, as a query */
    const char *cols[4];        /* what representation shows */
    const char *unit;
} Lens;

static const Lens LENSES[] = {
  {"vacant-land","Vacant land carrying value",
   "parcels with land value and no improvement",
   "value first, because the number is the reason to look",
   "SELECT trim(json_extract_string(observed,'$.Situs_Addr')) AS a, "
   "trim(json_extract_string(observed,'$.Account_Nu')) AS b, "
   "TRY_CAST(json_extract_string(observed,'$.Land_Value') AS DOUBLE) AS c "
   "FROM observation WHERE src_id='src:tad-parcel-view' "
   "AND TRY_CAST(json_extract_string(observed,'$.Improvemen') AS DOUBLE)=0 "
   "AND TRY_CAST(json_extract_string(observed,'$.Land_Value') AS DOUBLE)>0 "
   "ORDER BY c DESC LIMIT 25",
   {"address","account","land value",NULL},"parcel"},

  {"concentrated-owners","Names holding the most",
   "taxpayer names of record above a disclosure floor",
   "name first, count as weight",
   "SELECT trim(json_extract_string(observed,'$.Owner_Name')) AS a, "
   "'' AS b, count(*) AS c FROM observation WHERE src_id='src:tad-parcel-view' "
   "AND trim(json_extract_string(observed,'$.Owner_Name'))<>'' "
   "GROUP BY 1 HAVING c >= 60 ORDER BY c DESC LIMIT 25",
   {"taxpayer of record","","parcels",NULL},"name"},

  {"newest-building","Recently built",
   "parcels built in the last decade the roll records",
   "year first, because recency is the ordering",
   "SELECT trim(json_extract_string(observed,'$.Situs_Addr')) AS a, "
   "trim(json_extract_string(observed,'$.Account_Nu')) AS b, "
   "TRY_CAST(json_extract_string(observed,'$.Year_Built') AS BIGINT) AS c "
   "FROM observation WHERE src_id='src:tad-parcel-view' "
   "AND TRY_CAST(json_extract_string(observed,'$.Year_Built') AS BIGINT) >= 2015 "
   "ORDER BY c DESC LIMIT 25",
   {"address","account","year built",NULL},"parcel"},
};
#define LENS_N ((int)(sizeof(LENSES)/sizeof(LENSES[0])))

static const Lens *lens_find(const char *slug) {
    for (int i = 0; i < LENS_N; i++) if (!strcmp(slug, LENSES[i].slug)) return &LENSES[i];
    return NULL;
}

static int serve_lens(const char *method, const char *target, const char *query,
                      const char *bearer, const char *auth_db)
{
    static char body[262144], rows[131072];
    const char *bindir = getenv("BONFYRE_BIN_DIR");
    size_t w = 0;
    /* A lens is named in the path so it can be shared; nothing is read from a
     * parameter, and it needs no session to look through. */
    (void)bearer; (void)auth_db; (void)query;

    if (strncmp(target, "/lens", 5)) return 0;
    if (strcmp(method, "GET")) return 0;
    if (!bindir || !*bindir) bindir = "/usr/local/bin";

    /* One lens, applied. */
    if (!strncmp(target, "/lens/", 6)) {
        char slug[64];
        const Lens *L;
        if (!studio_path_tail(target, "/lens/", slug, sizeof(slug)) || !studio_safe_ident(slug) ||
            !(L = lens_find(slug))) {
            subject_miss("No lens by that name.");
            return 1;
        }
        w = (size_t)snprintf(body, sizeof(body), SUBJ_HEAD, L->title);
        w += (size_t)snprintf(body + w, sizeof(body) - w,
          "<article class=\"subj\">"
          "<p class=\"eyebrow\"><i></i>Lens &middot; one cut of the same Matrix</p>"
          "<h2 class=\"subj-name\">%s</h2>"
          "<dl class=\"flow-facts\">"
          "<div><dt>Selection</dt><dd class=\"lens-field\">%s</dd></div>"
          "<div><dt>Representation</dt><dd class=\"lens-field\">%s</dd></div></dl>",
          L->title, L->selection, L->representation);

        if (fleming_query(bindir, L->sql, rows, sizeof(rows)) != 0 || rows[0] != '[') {
            w += (size_t)snprintf(body + w, sizeof(body) - w,
              "<p class=\"flow-empty\">This lens cannot be applied right now.</p>");
        } else {
            const char *p = rows; int count = 0;
            w += (size_t)snprintf(body + w, sizeof(body) - w, "<div class=\"lens-stream\">");
            while ((p = strstr(p, "{\"a\"")) && w < sizeof(body) - 2048) {
                char a[512]={0}, b[128]={0}, c[64]={0}, ea[1024], rec[2048];
                const char *end = strchr(p, '}');
                size_t n = end ? (size_t)(end - p + 1) : 0;
                if (!n || n >= sizeof(rec)) break;
                memcpy(rec, p, n); rec[n] = '\0';
                studio_json_field(rec, "a", a, sizeof(a));
                studio_json_field(rec, "b", b, sizeof(b));
                studio_json_field(rec, "c", c, sizeof(c));
                html_escape(a, ea, sizeof(ea));
                /* Representation: the third column leads, because the lens says so. */
                if (b[0])
                    w += (size_t)snprintf(body + w, sizeof(body) - w,
                      "<a class=\"lens-item\" href=\"/s/parcel/%s\">"
                      "<span class=\"li-weight\">%s</span>"
                      "<span class=\"li-name\">%s</span>"
                      "<span class=\"li-meta\">%s %s</span></a>",
                      b, c[0]?c:"&mdash;", ea[0]?ea:"(no address)", L->cols[2], "");
                else
                    w += (size_t)snprintf(body + w, sizeof(body) - w,
                      "<span class=\"lens-item\">"
                      "<span class=\"li-weight\">%s</span>"
                      "<span class=\"li-name\">%s</span>"
                      "<span class=\"li-meta\">%s</span></span>",
                      c[0]?c:"&mdash;", ea[0]?ea:"&mdash;", L->cols[2]);
                count++; p = end + 1;
            }
            w += (size_t)snprintf(body + w, sizeof(body) - w,
              "</div><p class=\"subj-note\">%d %s%s in this cut. Selection and representation are separate: "
              "another lens can reuse this selection and show it differently, or show this representation "
              "over something else entirely.</p>", count, L->unit, count == 1 ? "" : "s");
        }
        /* Fork and combine: the lens is in the address, so both are links. */
        w += (size_t)snprintf(body + w, sizeof(body) - w, "<div class=\"trail\"><p class=\"ap-label\">Other cuts</p>");
        for (int i = 0; i < LENS_N; i++) {
            if (!strcmp(LENSES[i].slug, L->slug)) continue;
            w += (size_t)snprintf(body + w, sizeof(body) - w,
              "<a class=\"tr-dir\" href=\"/lens/%s\"><span class=\"tr-label\">%s</span>"
              "<span class=\"tr-why\">%s</span></a>", LENSES[i].slug, LENSES[i].title, LENSES[i].selection);
        }
        snprintf(body + w, sizeof(body) - w,
          "<p class=\"subj-note\">A lens is entered, left and shared by its address. Nothing here is a stored "
          "preference, so sending someone this link sends them this exact cut.</p></div></article>%s", SUBJ_FOOT);
        flow_reply(body);
        return 1;
    }

    /* The set of lenses. */
    if (!strcmp(target, "/lens") || !strcmp(target, "/lens/")) {
        w = (size_t)snprintf(body, sizeof(body), SUBJ_HEAD, "Lenses");
        w += (size_t)snprintf(body + w, sizeof(body) - w,
          "<article class=\"subj\">"
          "<p class=\"eyebrow\"><i></i>Lenses &middot; same Matrix, different cut</p>"
          "<h2 class=\"subj-name\">Not one feed.</h2>"
          "<p class=\"subj-desc\">A lens alters two things independently: what enters the stream, and how "
          "those objects should appear. They are not tabs, and none of them is the default.</p>"
          "<div class=\"trail\">");
        for (int i = 0; i < LENS_N; i++)
            w += (size_t)snprintf(body + w, sizeof(body) - w,
              "<a class=\"tr-dir\" href=\"/lens/%s\"><span class=\"tr-label\">%s</span>"
              "<span class=\"tr-why\">%s</span></a>",
              LENSES[i].slug, LENSES[i].title, LENSES[i].representation);
        snprintf(body + w, sizeof(body) - w, "</div></article>%s", SUBJ_FOOT);
        flow_reply(body);
        return 1;
    }
    return 0;
}

/* ── AMBIENT: one subject, every carrier it already appears through ──────
 *
 * The AddressPlane made perceptible. A parcel is not "a page with an API"; it
 * is one thing that shows up as a RESO record, an OGC feature, a MISMO subject
 * property, rows in the estate, and -- once someone acts on it -- a mission and
 * a CloudEvent. Listing those coordinates on the subject itself is what stops
 * each carrier from feeling like a separate product.
 */
#endif /* BONFYRE_SUBJECT_AND_LENS_H */
