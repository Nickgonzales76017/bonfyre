#ifndef BONFYRE_GOVERNANCE_SELF_AND_DISCOVERY_H
#define BONFYRE_GOVERNANCE_SELF_AND_DISCOVERY_H
/* Extracted verbatim from main.c lines 19094-21183 (mechanical split, no logic changes). */
static void govern_ref(char *out, size_t cap, const char *a, const char *b, const char *c) {
    char key[768]; char hex[65];
    snprintf(key, sizeof(key), "%s|%s|%s", a ? a : "", b ? b : "", c ? c : "");
    bf_sha256_hex((const unsigned char *)key, strlen(key), hex);
    snprintf(out, cap, "%.24s", hex);
}


/* A promise, put on the rail that already owns promises.
 *
 * Stating support and putting a name to a role are both promises: somebody
 * said they would do something for somebody else. The estate has a
 * CommitmentGraph whose whole discipline is that a promise is never a
 * delivery, delegation never releases the original obligor, and only the state
 * machine may append what happened next. Writing these there rather than
 * leaving them as rows in a content table is the difference between a record
 * of an intention and an obligation the estate can be asked about later.
 *
 * It never discharges anything. Discharge requires an explicit event, and
 * nothing a member clicks on a page is a delivery.
 */
static void govern_commit(const char *id, const char *committer, const char *beneficiary,
                          const char *resource, const char *conditions, const char *now) {
    const char *path = getenv("BONFYRE_COMMITMENT_DB");
    BfCommitment *graph = NULL;
    BfCommitmentSpec spec;
    if (!path || !*path) return;   /* no rail configured here: say nothing, invent nothing */
    if (bf_commitment_open(path, &graph, NULL) != 0 || !graph) return;
    memset(&spec, 0, sizeof(spec));
    spec.commitment_id = id;
    spec.committer = committer;
    spec.beneficiary = beneficiary;
    spec.resource = resource;
    spec.conditions = conditions;
    spec.authority = "stated by the person named, on a page of this estate";
    spec.origin = "aurekai:govern";
    spec.at = now;
    /* An identical promise recorded twice is one promise; EXISTS is not an
     * error a reader needs to hear about. */
    (void)bf_commitment_record(graph, &spec);
    bf_commitment_close(graph);
}

/* What an external provider charges, and what that leaves open.
 *
 * The first version of this had the estate quoting its own capability out of
 * its own book -- a number it had written down about itself, presented as a
 * price. That is a synthetic quote, and it is the wrong direction. This estate
 * does not need to invent a price for what it sells; it needs to say what it
 * *pays*, because that is the cost a project actually carries and the only
 * figure here with evidence behind it.
 *
 * So a quote is read from the admitted rates in the meter, where a price may
 * only enter with the document that admits it: exa's rate exists because exa's
 * own /search answered HTTP 402 stating the amount, firecrawl's because its
 * credit-usage endpoint reported the balance. A provider with no admitted rate
 * is refused rather than guessed at -- usage with no admitted rate is counted
 * and left unpriced, never counted as free.
 *
 * Nothing here settles. The chain reports the evidenced cost and where it
 * stops, and a promise is discharged only on a verified payment.
 */
static int govern_settle(const char *body, const char *actor, const char *now,
                         char *why, size_t why_cap) {
    char provider[96] = {0}, institution[160] = {0};
    char op[96] = {0}, unit[64] = {0}, currency[32] = {0}, evidence[512] = {0}, effective[64] = {0};
    const char *mpath = getenv("BONFYRE_METER_DB");
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL; char uri[PATH_MAX];
    double amount = 0.0; int found = 0;
    const char *fac;

    if (!query_value(body, "provider", provider, sizeof(provider)) || !provider[0]) {
        snprintf(why, why_cap,
                 "name the external provider whose work this would pay for. This estate quotes "
                 "what it pays others, not what it might charge for itself.");
        return 0;
    }
    if (!mpath || !*mpath ||
        bf_sqlite_readonly_uri(uri, sizeof(uri), mpath) >= (int)sizeof(uri) ||
        sqlite3_open_v2(uri, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        snprintf(why, why_cap, "no metered store is configured here, so no rate can be read");
        return 0;
    }
    /* Newest admitted rate for that provider, with the document that admitted it. */
    if (sqlite3_prepare_v2(db,
          "SELECT op, unit, amount, currency, COALESCE(evidence_ref,''), COALESCE(effective_at,'')"
          " FROM rate WHERE provider = ?1 ORDER BY effective_at DESC LIMIT 1",
          -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, provider, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) {
            snprintf(op, sizeof(op), "%s", (const char *)sqlite3_column_text(st, 0));
            snprintf(unit, sizeof(unit), "%s", (const char *)sqlite3_column_text(st, 1));
            amount = sqlite3_column_double(st, 2);
            snprintf(currency, sizeof(currency), "%s", (const char *)sqlite3_column_text(st, 3));
            snprintf(evidence, sizeof(evidence), "%s", (const char *)sqlite3_column_text(st, 4));
            snprintf(effective, sizeof(effective), "%s", (const char *)sqlite3_column_text(st, 5));
            found = 1;
        }
        sqlite3_finalize(st); st = NULL;
    }
    sqlite3_close(db);

    if (!found) {
        snprintf(why, why_cap,
                 "this estate has admitted no rate for %s. A price may only enter the book with the "
                 "document that admits it, so usage of %s would be counted and left unpriced, never "
                 "counted as free.", provider, provider);
        return 0;
    }

    query_value(body, "institution", institution, sizeof(institution));
    fac = x402_facilitator_for(institution[0] ? institution : NULL);
    if (!fac || !*fac) {
        snprintf(why, why_cap,
                 "%s charges %.6f %s per %s for %s, admitted on: %s. Nothing settled: no "
                 "facilitator stands behind %s. A DEMO institution has none however the "
                 "environment is configured, and an institution that declares no world is treated "
                 "as LIVE. The promise stays open.",
                 provider, amount, currency, unit, op, evidence[0] ? evidence : "no document recorded",
                 institution[0] ? institution : "this estate");
        return 0;
    }
    snprintf(why, why_cap,
             "%s charges %.6f %s per %s for %s, admitted on: %s (effective %s). A payment presented "
             "against that through %s is retained as a digest of what was shown; it is not proof "
             "that value moved, and the promise is discharged only on a verified payment.",
             provider, amount, currency, unit, op,
             evidence[0] ? evidence : "no document recorded",
             effective[0] ? effective : "no date recorded", fac);
    (void)actor; (void)now;
    return 1;
}

/* A household connection is meaningful only against an institution that this
 * deployment already has in its admitted public frontier.  The public room
 * uses the richer query below to describe the institution; this small guard
 * is deliberately narrower because mutation needs only the admission fact.
 * In particular, a syntactically convincing hostname must never manufacture
 * an institution row in the CMS. */
static int institution_frontier_admitted(const char *host) {
    const char *path = getenv("BONFYRE_SOURCE_FRONTIER_DB");
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char uri[PATH_MAX];
    int admitted = 0;

    if (!host || !*host || !path || !*path ||
        bf_sqlite_readonly_uri(uri, sizeof(uri), path) >= (int)sizeof(uri) ||
        sqlite3_open_v2(uri, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return 0;
    }
    if (sqlite3_prepare_v2(db,
          "SELECT 1 FROM root r JOIN channel c ON c.root_id=r.root_id"
          " WHERE r.host=?1 AND c.state IN ('admitted','active') LIMIT 1",
          -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, host, -1, SQLITE_STATIC);
        admitted = sqlite3_step(st) == SQLITE_ROW;
    }
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);
    return admitted;
}

/* A behavior contract can address a typed owner only when that owner material
 * exists. The old handler accepted an arbitrary project/proposal string after
 * sign-in, which made an exact ObjectContract look like a relationship to a
 * thing that had never been published. Keep this check in the native owner:
 * the compiler supplies an id, but cannot certify a row at render time. */
static int govern_published_id(sqlite3 *db, const char *table, const char *field,
                               const char *id) {
    sqlite3_stmt *st = NULL;
    char sql[320];
    int found = 0;
    if (!db || !table || !field || !id || !*id || !cms_type_registered(db, table) ||
        snprintf(sql, sizeof(sql), "SELECT 1 FROM %s WHERE %s=?1 AND status='published' LIMIT 1",
                 table, field) >= (int)sizeof(sql) ||
        sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, id, -1, SQLITE_STATIC);
    found = sqlite3_step(st) == SQLITE_ROW;
    sqlite3_finalize(st);
    return found;
}

static int govern_act(const char *method, const char *target, const char *body,
                      const char *raw, const char *host, const char *bearer,
                      const char *auth_db) {
    char actor[64], back[400] = {0}, id[200] = {0}, choice[32] = {0}, amount[32] = {0};
    char now[32], ref[40], row_id[288];
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL;
    int is_vote, is_join, is_opportunity_save, is_support, is_settle, is_connect;
    time_t t;

    if (!target) return 0;
    if (pj_legacy(method,target,body,raw,host,bearer,auth_db)) return 1;
    is_vote = !strcmp(target, "/govern/vote");
    is_join = !strcmp(target, "/govern/join");
    is_opportunity_save = !strcmp(target, "/govern/opportunity-save");
    is_support = !strcmp(target, "/govern/support");
    is_settle = !strcmp(target, "/govern/settle");
    is_connect = !strcmp(target, "/govern/connect");
    if (!is_vote && !is_join && !is_opportunity_save && !is_support && !is_settle && !is_connect) return 0;
    if (strcmp(method, "POST")) {
        protocol_reply(405, "text/plain; charset=utf-8", "Taking part is a form submission.\n");
        return 1;
    }

    /* Only ever asked for from this site. */
    {
        const char *origin = raw ? strstr(raw, "\nOrigin:") : NULL;
        if (!origin && raw) origin = strstr(raw, "\norigin:");
        if (origin) {
            char value[300] = {0}, https[300], http[300];
            sscanf(origin + 8, " %299[^\r\n]", value);
            snprintf(https, sizeof(https), "https://%s", host ? host : "");
            snprintf(http, sizeof(http), "http://%s", host ? host : "");
            if (strcmp(value, https) && strcmp(value, http) && strcmp(value, "null")) {
                protocol_reply(403, "text/plain; charset=utf-8",
                               "Taking part is only accepted from this site.\n");
                return 1;
            }
        }
    }

    /* Nobody acts anonymously. A position with no name on it is not a position
     * a community can weigh, and an unsigned intention is not one either. */
    if (!page_session_actor(bearer, auth_db, actor, sizeof(actor))) {
        protocol_reply(401, "text/plain; charset=utf-8",
                       "Sign in first: an act recorded here carries the name of whoever performed it.\n");
        return 1;
    }
    if (!body || !query_value(body, is_connect ? "institution" : "id", id, sizeof(id)) || !id[0] ||
        strspn(id, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:") != strlen(id)) {
        protocol_reply(400, "text/plain; charset=utf-8", "That is not something that can be acted on.\n");
        return 1;
    }
    if (!query_value(body, "return", back, sizeof(back)) || back[0] != '/' || back[1] == '/' ||
        strpbrk(back, "\r\n\\"))
        snprintf(back, sizeof(back), "/subjects");

    t = time(NULL);
    strftime(now, sizeof(now), "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
    if (!(db = cms_open_rw())) {
        protocol_reply(503, "text/plain; charset=utf-8", "The record is not writable here.\n");
        return 1;
    }

    if (is_vote && !govern_published_id(db, "governance_proposal", "proposal_id", id)) {
        sqlite3_close(db);
        protocol_reply(404, "text/plain; charset=utf-8",
                       "That governance proposal is not a published subject here.\n");
        return 1;
    }
    if ((is_join || is_support) && !govern_published_id(db, "project", "project_id", id)) {
        sqlite3_close(db);
        protocol_reply(404, "text/plain; charset=utf-8",
                       "That project is not a published subject here.\n");
        return 1;
    }
    if (is_opportunity_save && !govern_published_id(db, "household_opportunity", "opportunity_id", id)) {
        sqlite3_close(db);
        protocol_reply(404, "text/plain; charset=utf-8",
                       "That household opportunity is not a published subject here.\n");
        return 1;
    }

    if (is_connect) {
        /* A household saying what it already holds.
         *
         * The granted planes on an institution's page were, until now, a shape
         * with nothing behind it: the room could name a roster or a gradebook
         * and say it answers only to a relationship, and no relationship
         * existed anywhere in this estate, so the sentence described a
         * possibility rather than a record. This is the record.
         *
         * What it is not: it is not the institution agreeing, and writing it
         * here cannot make the institution agree. The estate has no channel to
         * that institution's roster and acquires none by being told a
         * connection exists. So the row states who said it and when, and the
         * plane it opens is opened for that household's own reading only --
         * never for this estate to hold, compare or publish, and never for
         * another household. Released by the same household whenever it likes,
         * because a relationship it cannot end is not one it holds. */
        char plane[80] = {0}, release[8] = {0}, hh[64], hh_label[96];
        int known = 0, granted = 0; size_t pi;
        if (!query_value(body, "plane", plane, sizeof(plane)) || !plane[0]) {
            sqlite3_close(db);
            protocol_reply(400, "text/plain; charset=utf-8",
                           "A connection opens one named plane, not a relationship in general.\n");
            return 1;
        }
        for (pi = 0; pi < BF_INSTITUTION_PLANE_COUNT; pi++)
            if (!strcmp(bf_institution_planes[pi].plane, plane)) {
                known = 1;
                granted = strcmp(bf_institution_planes[pi].reach, "public") != 0;
                break;
            }
        if (!known || !granted) {
            sqlite3_close(db);
            protocol_reply(400, "text/plain; charset=utf-8",
                           known ? "That plane is already public: a relationship adds nothing to it.\n"
                                 : "No such plane.\n");
            return 1;
        }
        if (!institution_frontier_admitted(id)) {
            sqlite3_close(db);
            protocol_reply(404, "text/plain; charset=utf-8",
                           "This household connection needs an institution already admitted to the public frontier.\n");
            return 1;
        }
        query_value(body, "release", release, sizeof(release));

        /* One household per steward, opened the first time they claim
         * anything. Nothing about it is published: it exists so a connection
         * has an owner that is not this estate. */
        govern_ref(ref, sizeof(ref), "household", actor, "");
        snprintf(hh, sizeof(hh), "hh-%s", ref);
        snprintf(hh_label, sizeof(hh_label), "%s's household", actor);
        if (sqlite3_prepare_v2(db,
              "INSERT OR IGNORE INTO household(namespace,created_at,updated_at,published_at,status,"
              " household_id,label,steward,opened_at,cannot_infer)"
              " VALUES('root',?1,?1,?1,'published',?2,?3,?4,?1,?5)",
              -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, now, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, hh, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 3, hh_label, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 4, actor, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 5,
              "a household is the people in it; this row is not a roster of them, not an address, "
              "and not a claim that anyone else in it agreed to anything recorded under it",
              -1, SQLITE_STATIC);
            sqlite3_step(st); sqlite3_finalize(st); st = NULL;
        }

        govern_ref(ref, sizeof(ref), id, plane, hh);
        snprintf(row_id, sizeof(row_id), "conn-%s", ref);
        if (sqlite3_prepare_v2(db,
              "INSERT OR REPLACE INTO institution_connection(namespace,created_at,updated_at,"
              " published_at,status,connection_id,household_id,institution,plane,established_by,"
              " established_at,state,cannot_infer)"
              " VALUES('root',?1,?1,?1,'published',?2,?3,?4,?5,?6,?1,?7,?8)",
              -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, now, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, row_id, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 3, hh, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 4, id, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 5, plane, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 6, actor, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 7, release[0] ? "released" : "established", -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 8,
              "stated by the household, not confirmed by the institution: this estate has no "
              "channel to that plane and gains none here, holds nothing fetched through it, and "
              "cannot show it to anyone but the household that said so",
              -1, SQLITE_STATIC);
            sqlite3_step(st); sqlite3_finalize(st); st = NULL;
        }
        sqlite3_close(db);
        printf("HTTP/1.1 303 See Other\r\nLocation: %s\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", back);
        fflush(stdout);
        return 1;
    }
    if (is_settle) {
        /* The chain runs as far as the evidence goes and reports where it
         * stopped. Nothing is discharged on an unverified reference. */
        char why[640];
        int can_present = govern_settle(body, actor, now, why, sizeof(why));
        sqlite3_close(db);
        protocol_reply(can_present ? 200 : 409, "text/plain; charset=utf-8", why);
        return 1;
    }
    if (is_vote) {
        if (!query_value(body, "choice", choice, sizeof(choice)) ||
            (strcmp(choice, "for") && strcmp(choice, "against") && strcmp(choice, "abstain"))) {
            sqlite3_close(db);
            protocol_reply(400, "text/plain; charset=utf-8", "A vote is for, against, or abstain.\n");
            return 1;
        }
        /* One member holds one position at a time. Changing it replaces the
         * old one: a tally that grew every time somebody reconsidered would
         * not be a count of people. */
        govern_ref(ref, sizeof(ref), "vote", id, actor);
        snprintf(row_id, sizeof(row_id), "vote-%s", ref);
        if (sqlite3_prepare_v2(db, "DELETE FROM governance_vote WHERE proposal_id=?1 AND voter=?2",
                               -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, id, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, actor, -1, SQLITE_STATIC);
            sqlite3_step(st); sqlite3_finalize(st); st = NULL;
        }
        if (sqlite3_prepare_v2(db,
              "INSERT INTO governance_vote(namespace,created_at,updated_at,published_at,status,"
              " vote_id,proposal_id,voter,choice,cast_at,backing,cannot_infer)"
              " VALUES('root',?1,?1,?1,'published',?2,?3,?4,?5,?1,'human_approval',?6)",
              -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, now, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, row_id, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 3, id, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 4, actor, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 5, choice, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 6,
              "a vote is what was said by the person who said it: never the membership's view, "
              "never a decision, and never authority to carry anything out", -1, SQLITE_STATIC);
            sqlite3_step(st); sqlite3_finalize(st); st = NULL;
        }
    } else if (is_opportunity_save) {
        govern_ref(ref, sizeof(ref), "opportunity-save", id, actor);
        snprintf(row_id, sizeof(row_id), "save-%s", ref);
        if (sqlite3_prepare_v2(db,
              "INSERT OR REPLACE INTO household_opportunity_save(namespace,created_at,updated_at,published_at,"
              " status,save_id,opportunity_id,saver,saved_at,cannot_infer)"
              " VALUES('root',?1,?1,?1,'published',?2,?3,?4,?1,?5)",
              -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, now, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, row_id, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 3, id, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 4, actor, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 5,
              "saving a public household opportunity is a private local reference; it is not an application, "
              "referral, eligibility finding, enrollment, provider notification, service delivery, or authority",
              -1, SQLITE_STATIC);
            sqlite3_step(st); sqlite3_finalize(st); st = NULL;
        }
    } else if (is_join) {
        govern_ref(ref, sizeof(ref), "join", id, actor);
        snprintf(row_id, sizeof(row_id), "sup-%s", ref);
        if (sqlite3_prepare_v2(db,
              "INSERT OR REPLACE INTO project_support(namespace,created_at,updated_at,published_at,"
              " status,support_id,project_id,supporter,mode,amount_stated,currency,stated_at,cannot_infer)"
              " VALUES('root',?1,?1,?1,'published',?2,?3,?4,'joining','', '', ?1, ?5)",
              -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, now, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, row_id, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 3, id, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 4, actor, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 5,
              "putting a name to a role is stating willingness; it is not an engagement, not a "
              "contract, and not a claim that anyone accepted", -1, SQLITE_STATIC);
            sqlite3_step(st); sqlite3_finalize(st); st = NULL;
        }
        govern_commit(row_id, actor, id, "a role this project says it lacks",
                      "willingness stated on a page; not an engagement, a contract, or acceptance by anyone",
                      now);
    } else {
        query_value(body, "amount", amount, sizeof(amount));
        if (amount[0] && strspn(amount, "0123456789.") != strlen(amount)) amount[0] = '\0';
        govern_ref(ref, sizeof(ref), "support", id, actor);
        snprintf(row_id, sizeof(row_id), "sup-%s", ref);
        if (sqlite3_prepare_v2(db,
              "INSERT OR REPLACE INTO project_support(namespace,created_at,updated_at,published_at,"
              " status,support_id,project_id,supporter,mode,amount_stated,currency,stated_at,cannot_infer)"
              " VALUES('root',?1,?1,?1,'published',?2,?3,?4,'stated',?5,'USD',?1,?6)",
              -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, now, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, row_id, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 3, id, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 4, actor, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 5, amount, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 6,
              "support recorded here is an intention and a relationship: no money has moved, this "
              "estate cannot collect it, and nothing about it is evidence that value moved",
              -1, SQLITE_STATIC);
            sqlite3_step(st); sqlite3_finalize(st); st = NULL;
        }
        {
            char res[128];
            snprintf(res, sizeof(res), "support%s%s%s", amount[0] ? " of " : "",
                     amount[0] ? amount : "", amount[0] ? " USD, stated" : ", unquantified");
            govern_commit(row_id, actor, id, res,
                          "stated before any payment and independent of whether one happens; "
                          "this estate cannot collect it and recording it is not delivery", now);
        }
    }
    sqlite3_close(db);
    printf("HTTP/1.1 303 See Other\r\nLocation: %s\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", back);
    fflush(stdout);
    return 1;
}

/* An operating role is a personal, scoped posture over the shared actor and
 * subject graph.  Assuming it is intentionally weaker than receiving an
 * authority grant: the assignment may choose a workbench but every offered
 * effect still performs its own authority check. */
static int serve_role_assume(const char *method, const char *target, const char *body,
                             const char *raw, const char *host, const char *bearer,
                             const char *auth_db) {
    char actor[64], role[96] = {0}, scope[192] = {0}, back[400] = {0};
    char now[32], ref[40], assignment[288], coordinate[320];
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL;
    time_t t; int written = 0;
    if (!target || strcmp(target, "/roles/assume")) return 0;
    if (strcmp(method, "POST")) {
        protocol_reply(405, "text/plain; charset=utf-8", "Assuming a role is a form submission.\n");
        return 1;
    }
    {
        const char *origin = raw ? strstr(raw, "\nOrigin:") : NULL;
        if (!origin && raw) origin = strstr(raw, "\norigin:");
        if (origin) {
            char value[300] = {0}, https[300], http[300];
            sscanf(origin + 8, " %299[^\r\n]", value);
            snprintf(https, sizeof(https), "https://%s", host ? host : "");
            snprintf(http, sizeof(http), "http://%s", host ? host : "");
            if (strcmp(value, https) && strcmp(value, http) && strcmp(value, "null")) {
                protocol_reply(403, "text/plain; charset=utf-8", "Assuming a role is only accepted from this site.\n");
                return 1;
            }
        }
    }
    if (!page_session_actor(bearer, auth_db, actor, sizeof(actor))) {
        protocol_reply(401, "text/plain; charset=utf-8", "Sign in first: a role posture belongs to an actor.\n");
        return 1;
    }
    if (!body || !query_value(body, "role", role, sizeof(role)) || !role[0] ||
        strspn(role, "abcdefghijklmnopqrstuvwxyz0123456789-") != strlen(role)) {
        protocol_reply(400, "text/plain; charset=utf-8", "That is not a published role program.\n");
        return 1;
    }
    if (!query_value(body, "scope", scope, sizeof(scope)) || !scope[0]) snprintf(scope, sizeof(scope), "global");
    if (strspn(scope, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_:." ) != strlen(scope)) {
        protocol_reply(400, "text/plain; charset=utf-8", "That role scope is not usable.\n");
        return 1;
    }
    if (!query_value(body, "return", back, sizeof(back)) || back[0] != '/' || back[1] == '/' || strpbrk(back, "\r\n\\"))
        snprintf(back, sizeof(back), "/subjects");
    if (!(db = cms_open_rw()) || !govern_published_id(db, "role_program", "role_id", role)) {
        if (db) sqlite3_close(db);
        protocol_reply(404, "text/plain; charset=utf-8", "That role program is not published here.\n");
        return 1;
    }
    t = time(NULL);
    strftime(now, sizeof(now), "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
    snprintf(coordinate, sizeof(coordinate), "%s|%s", role, scope);
    govern_ref(ref, sizeof(ref), "role", actor, coordinate);
    snprintf(assignment, sizeof(assignment), "role-%s", ref);
    if (sqlite3_prepare_v2(db,
          "INSERT OR REPLACE INTO role_assignment(namespace,created_at,updated_at,published_at,status,"
          " assignment_id,actor_subject,role_program,context_scope,state,assumed_at,cannot_infer)"
          " VALUES('root',?1,?1,?1,'published',?2,?3,?4,?5,'assumed',?1,?6)",
          -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, now, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, assignment, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 3, actor, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 4, role, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 5, scope, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 6,
          "assuming a role program is a local operating posture, not an Actor subtype, authority grant, "
          "provider access, institutional role, employment relationship, or permission to perform an effect",
          -1, SQLITE_STATIC);
        written = sqlite3_step(st) == SQLITE_DONE;
        sqlite3_finalize(st);
    }
    sqlite3_close(db);
    if (!written) {
        protocol_reply(503, "text/plain; charset=utf-8", "The role assignment was not recorded.\n");
        return 1;
    }
    printf("HTTP/1.1 303 See Other\r\nLocation: %s\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", back);
    fflush(stdout);
    return 1;
}

/* The passport reads the compiled posture for its assigning actor only.  It
 * deliberately returns authority requirements as requirements, not grants:
 * this workspace can guide a person globally but cannot authorize an effect. */
static int serve_role_workspace(const char *method, const char *target, const char *query,
                                const char *bearer, const char *auth_db) {
    char actor[64], assignment[288] = {0}, label[512], scope[384], body[24576];
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL;
    int found = 0;
    if (!target || strcmp(target, "/roles/workspace")) return 0;
    if (strcmp(method, "GET")) { protocol_reply(405, "application/json", "{\"error\":\"get_required\"}\n"); return 1; }
    if (!page_session_actor(bearer, auth_db, actor, sizeof(actor))) {
        protocol_reply(401, "application/json", "{\"error\":\"sign_in_required\"}\n"); return 1;
    }
    if (!query_value(query, "assignment", assignment, sizeof(assignment)) || !assignment[0] ||
        strspn(assignment, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_:." ) != strlen(assignment)) {
        protocol_reply(400, "application/json", "{\"error\":\"assignment_required\"}\n"); return 1;
    }
    if (!(db = cms_open_ro()) || !cms_type_registered(db, "role_assignment") || !cms_type_registered(db, "role_program") ||
        sqlite3_prepare_v2(db,
          "SELECT a.role_program,a.context_scope,p.label,p.search_scopes,p.lenses,p.work_queues,p.capability_programs,"
          "p.exact_operations,p.unresolved_operations,p.authority_requirements,p.metric_views,p.handoff_contracts,p.surface_scene "
          "FROM role_assignment a JOIN role_program p ON p.role_id=a.role_program AND p.namespace=a.namespace "
          "WHERE a.assignment_id=?1 AND a.actor_subject=?2 AND a.state='assumed' AND a.status='published' AND p.status='published' LIMIT 1",
          -1, &st, NULL) != SQLITE_OK) {
        if (st) sqlite3_finalize(st); if (db) sqlite3_close(db);
        protocol_reply(503, "application/json", "{\"error\":\"role_workspace_unavailable\"}\n"); return 1;
    }
    sqlite3_bind_text(st, 1, assignment, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, actor, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *role = (const char *)sqlite3_column_text(st, 0);
        const char *raw_scope = (const char *)sqlite3_column_text(st, 1);
        const char *raw_label = (const char *)sqlite3_column_text(st, 2);
        char erole[256], eactor[128];
        json_escape_into(role ? role : "", erole, sizeof(erole));
        json_escape_into(raw_label ? raw_label : "", label, sizeof(label));
        json_escape_into(raw_scope ? raw_scope : "", scope, sizeof(scope));
        json_escape_into(actor, eactor, sizeof(eactor));
        snprintf(body, sizeof(body),
          "{\"assignment\":\"%s\",\"actor\":\"%s\",\"role_program\":\"%s\",\"label\":\"%s\",\"context_scope\":\"%s\","
          "\"public_search_scopes\":%s,\"lenses\":%s,\"work_queues\":%s,\"capability_programs\":%s,"
          "\"exact_operations\":%s,\"unresolved_operations\":%s,\"required_authority\":%s,\"metric_views\":%s,\"handoff_contracts\":%s,\"surface_scene\":%s,"
          "\"forbidden_inference\":\"a role workspace is a private projection of one assignment; role requirements are not authority grants and each effect remains locally checked by its native owner\"}\n",
          assignment, eactor, erole, label, scope,
          sqlite3_column_text(st, 3) ? (const char *)sqlite3_column_text(st, 3) : "[]",
          sqlite3_column_text(st, 4) ? (const char *)sqlite3_column_text(st, 4) : "[]",
          sqlite3_column_text(st, 5) ? (const char *)sqlite3_column_text(st, 5) : "[]",
          sqlite3_column_text(st, 6) ? (const char *)sqlite3_column_text(st, 6) : "[]",
          sqlite3_column_text(st, 7) ? (const char *)sqlite3_column_text(st, 7) : "[]",
          sqlite3_column_text(st, 8) ? (const char *)sqlite3_column_text(st, 8) : "[]",
          sqlite3_column_text(st, 9) ? (const char *)sqlite3_column_text(st, 9) : "[]",
          sqlite3_column_text(st, 10) ? (const char *)sqlite3_column_text(st, 10) : "[]",
          sqlite3_column_text(st, 11) ? (const char *)sqlite3_column_text(st, 11) : "[]",
          sqlite3_column_text(st, 12) ? (const char *)sqlite3_column_text(st, 12) : "{}");
        found = 1;
    }
    sqlite3_finalize(st); sqlite3_close(db);
    if (!found) protocol_reply(404, "application/json", "{\"error\":\"role_assignment_not_found\"}\n");
    else protocol_reply(200, "application/json", body);
    return 1;
}

/* Set only by the already-admitted private-host dispatcher for the duration
 * of one single-threaded request.  It is a server-side closure coordinate,
 * never a query parameter or browser-selected namespace. */
static char private_cms_namespace[192] = {0};
/* A private ForeignTwin observation is not merely hidden at rendering time.
 * Its retained body and receipt live below a closure-derived private root, so
 * the public foreign carrier cannot accidentally enumerate its existence. */
static char private_foreign_root[PATH_MAX] = {0};

/* Personal ForeignTwin intake has the same non-public storage discipline as a
 * delegated estate, but its namespace derives solely from the authenticated
 * actor.  The browser cannot select a different actor or a public root. */
static int self_foreign_root_for(const char *bearer, const char *auth_db,
                                 char *out, size_t out_sz) {
    const char *base = getenv("BONFYRE_FOREIGN_ROOT");
    char user[32];
    if (!base || !*base) base = "/var/lib/bonfyre/foreign";
    if (!out || !out_sz || !studio_auth_user(bearer, auth_db, user, sizeof(user))) return 0;
    return snprintf(out, out_sz, "%s/private/self/actor-%s", base, user) > 0 &&
           strlen(out) < out_sz;
}

/* A personal ForeignTwin is useful only when the person can return to it.
 * These helpers deliberately resolve every receipt and body below the current
 * actor root; neither a browser-supplied path nor a public ForeignTwin route
 * can be used to inspect it. */
static int self_foreign_receipt_name(const char *name) {
    size_t n;
    if (!name || !(n = strlen(name)) || n > 192 || n < 6 || strcmp(name + n - 5, ".json")) return 0;
    for (size_t i = 0; i < n; i++)
        if (!(isalnum((unsigned char)name[i]) || name[i] == '-' || name[i] == '_' || name[i] == '.')) return 0;
    return 1;
}

static int self_foreign_read_receipt(const char *root, const char *name,
                                     char *out, size_t cap) {
    char path[PATH_MAX];
    FILE *f;
    size_t got;
    if (!root || !self_foreign_receipt_name(name) || !out || cap < 2 ||
        snprintf(path, sizeof(path), "%s/receipts/%s", root, name) >= (int)sizeof(path)) return 0;
    if (!(f = bf_receipt_fopen(path, "rb"))) return 0;
    got = fread(out, 1, cap - 1, f);
    if (ferror(f)) { fclose(f); return 0; }
    fclose(f); out[got] = '\0';
    return got > 0;
}

static int self_foreign_body_path(const char *root, const char *receipt,
                                  char *out, size_t cap) {
    char recorded[PATH_MAX];
    const char *base;
    if (!root || !receipt || !out || !cap ||
        !studio_json_field(receipt, "body_path", recorded, sizeof(recorded)) ||
        strstr(recorded, "..")) return 0;
    base = strrchr(recorded, '/'); base = base ? base + 1 : recorded;
    if (!foreign_safe_file(base) ||
        snprintf(out, cap, "%s/bodies/%s", root, base) >= (int)cap) return 0;
    return 1;
}

static int self_foreign_digest_file(const char *path, char out[65], long long *bytes) {
    BfSha256 sha;
    unsigned char buffer[32768], digest[32];
    FILE *f;
    size_t got;
    long long total = 0;
    if (bytes) *bytes = 0;
    if (!path || !(f = fopen(path, "rb"))) return 0;
    bf_sha256_init(&sha);
    while ((got = fread(buffer, 1, sizeof(buffer), f)) > 0) {
        bf_sha256_update(&sha, buffer, got);
        total += (long long)got;
    }
    if (ferror(f)) { fclose(f); return 0; }
    fclose(f);
    bf_sha256_final(&sha, digest);
    bf_sha256_digest_hex(digest, out);
    if (bytes) *bytes = total;
    return 1;
}

static char *self_foreign_case_find(char *haystack, const char *needle) {
    size_t n = needle ? strlen(needle) : 0;
    if (!haystack || !n) return NULL;
    for (; *haystack; haystack++)
        if (!strncasecmp(haystack, needle, n)) return haystack;
    return NULL;
}

static int self_foreign_name_cmp(const void *a, const void *b) {
    const char *const *left = a, *const *right = b;
    return strcmp(*right, *left); /* receipt names end in an ISO-like stamp */
}

static void self_foreign_body_facts(const char *path, char *title, size_t title_cap,
                                    int *links, int *images, int *structured) {
    FILE *f; char body[131073]; size_t n, i;
    if (title && title_cap) title[0] = '\0';
    if (links) *links = 0; if (images) *images = 0; if (structured) *structured = 0;
    if (!path || !(f = fopen(path, "rb"))) return;
    n = fread(body, 1, sizeof(body) - 1, f); fclose(f); body[n] = '\0';
    for (i = 0; i + 2 < n; i++) {
        if (links && !strncasecmp(body + i, "<a", 2) && (isspace((unsigned char)body[i + 2]) || body[i + 2] == '>')) (*links)++;
        if (images && !strncasecmp(body + i, "<img", 4)) (*images)++;
        if (structured && (!strncasecmp(body + i, "application/ld+json", 19) || !strncasecmp(body + i, "<rss", 4) || !strncasecmp(body + i, "<feed", 5))) *structured = 1;
    }
    if (title && title_cap) {
        char *start = self_foreign_case_find(body, "<title");
        if (start && (start = strchr(start, '>'))) {
            char *end = self_foreign_case_find(++start, "</title>");
            size_t len = end ? (size_t)(end - start) : 0;
            if (len && len < title_cap) { memcpy(title, start, len); title[len] = '\0'; }
        }
    }
}

static void self_foreign_html_reply(int code, const char *html_body) {
    const char *reason = code == 200 ? "OK" : code == 403 ? "Forbidden" :
                         code == 404 ? "Not Found" : code == 405 ? "Method Not Allowed" : "Error";
    const char *body = html_body ? html_body : "";
    printf("HTTP/1.1 %d %s\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: %zu\r\n"
           "Cache-Control: private, no-store\r\nX-Robots-Tag: noindex, nofollow, noarchive\r\nConnection: close\r\n\r\n%s",
           code, reason, strlen(body), body);
}

static int serve_self_foreign_workspace(const char *method, const char *target,
                                        const char *body, const char *bearer,
                                        const char *auth_db, const char *host) {
    char host_norm[256], root[PATH_MAX], html_body[98304];
    int inspect = !strcmp(target, "/studio/v1/self/foreign/inspect");
    int workspace = !strcmp(target, "/studio/v1/self/foreign");
    if (!workspace && !inspect) return 0;
    if ((workspace && strcmp(method, "GET")) || (inspect && strcmp(method, "POST"))) {
        studio_reply(405, "application/json", "{\"error\":\"method_not_allowed\"}\n"); return 1;
    }
    if (!session_self_authoring(bearer, auth_db) || !lower_host(host, host_norm) ||
        strcmp(host_norm, "aurekai.life") || !self_foreign_root_for(bearer, auth_db, root, sizeof(root))) {
        studio_reply(403, "application/json", "{\"error\":\"self_private_foreign_requires_life_self_scope\"}\n"); return 1;
    }
    if (inspect) {
        char name[256] = {0}, receipt[65536], body_path[PATH_MAX], expected[80] = {0}, actual[65] = {0};
        char title[512], e_title[1100], e_url[3000], e_name[512], e_preview[131072];
        char *preview = NULL; long long bytes = 0; int links = 0, images = 0, structured = 0, matches = 0;
        FILE *f; size_t n = 0;
        if (!query_value(body, "receipt", name, sizeof(name)) || !self_foreign_read_receipt(root, name, receipt, sizeof(receipt))) {
            self_foreign_html_reply(404, "<section class=\"studio-result is-error\"><h3>Private receipt not found</h3></section>"); return 1;
        }
        (void)studio_json_field(receipt, "content_sha256", expected, sizeof(expected));
        if (!self_foreign_body_path(root, receipt, body_path, sizeof(body_path)) ||
            !self_foreign_digest_file(body_path, actual, &bytes)) {
            self_foreign_html_reply(200, "<section class=\"studio-result is-error\"><h3>Receipt retained; body unavailable</h3><p>This observation has no readable body in this private root.</p></section>"); return 1;
        }
        matches = !expected[0] || !strcmp(expected, actual);
        self_foreign_body_facts(body_path, title, sizeof(title), &links, &images, &structured);
        html_escape(title[0] ? title : "Untitled source body", e_title, sizeof(e_title));
        { char url[1400] = {0}; (void)studio_json_field(receipt, "url", url, sizeof(url)); html_escape(url, e_url, sizeof(e_url)); }
        html_escape(name, e_name, sizeof(e_name));
        if ((f = fopen(body_path, "rb"))) { preview = malloc(32769); if (preview) { n = fread(preview, 1, 32768, f); preview[n] = '\0'; } fclose(f); }
        html_escape(preview ? preview : "", e_preview, sizeof(e_preview)); free(preview);
        snprintf(html_body, sizeof(html_body),
            "<section class=\"studio-result %s\" aria-live=\"polite\"><span class=\"result-kicker\">Private source inspection</span>"
            "<h3>%s</h3><p><a href=\"%s\" rel=\"noreferrer noopener\" target=\"_blank\">Open source</a></p>"
            "<dl class=\"result-facts\"><div><dt>Receipt</dt><dd>%s</dd></div><div><dt>Bytes held</dt><dd>%lld</dd></div>"
            "<div><dt>Integrity</dt><dd>%s</dd></div><div><dt>Links in first 128 KiB</dt><dd>%d</dd></div>"
            "<div><dt>Images in first 128 KiB</dt><dd>%d</dd></div><div><dt>Structured markup</dt><dd>%s</dd></div></dl>"
            "<details><summary>Read retained source excerpt</summary><pre>%s</pre></details></section>",
            matches ? "is-ok" : "is-error", e_title, e_url, e_name, bytes,
            matches ? "matches receipt" : "digest mismatch — not usable", links, images,
            structured ? "detected" : "not detected", e_preview);
        self_foreign_html_reply(200, html_body); return 1;
    }
    {
        char dir_path[PATH_MAX], names[96][193], *ordered[96];
        BfReceiptDir *dir; struct dirent *entry; int count = 0, held = 0, unreadable = 0; size_t w;
        if (snprintf(dir_path, sizeof(dir_path), "%s/receipts", root) >= (int)sizeof(dir_path) || !(dir = bf_receipt_opendir(dir_path))) {
            self_foreign_html_reply(200, "<section class=\"studio-result\"><span class=\"result-kicker\">Private sources</span><h3>No sources yet</h3><p>Observe a public URL above. It will remain in this account’s private evidence space.</p></section>"); return 1;
        }
        while (count < 96 && (entry = bf_receipt_readdir(dir))) if (self_foreign_receipt_name(entry->d_name)) {
            snprintf(names[count], sizeof(names[count]), "%s", entry->d_name); ordered[count] = names[count]; count++;
        }
        bf_receipt_closedir(dir);
        if (count > 1) qsort(ordered, (size_t)count, sizeof(ordered[0]), self_foreign_name_cmp);
        w = (size_t)snprintf(html_body, sizeof(html_body),
            "<section class=\"studio-result\"><span class=\"result-kicker\">Your private source desk</span>"
            "<h3>%d observation%s</h3><p>Inspect retained material, check its receipt hash, and re-observe the exact source when it changes.</p><div class=\"source-workspace\">",
            count, count == 1 ? "" : "s");
        for (int i = 0; i < count && i < 18 && w < sizeof(html_body) - 8192; i++) {
            char receipt[65536], twin[128] = {0}, url[1400] = {0}, body_path[PATH_MAX], e_twin[300], e_url[3000], e_name[512];
            char reobserve[4600] = {0};
            long long bytes = 0; int readable = 0;
            if (!self_foreign_read_receipt(root, ordered[i], receipt, sizeof(receipt))) continue;
            (void)studio_json_field(receipt, "twin_id", twin, sizeof(twin));
            (void)studio_json_field(receipt, "url", url, sizeof(url));
            readable = self_foreign_body_path(root, receipt, body_path, sizeof(body_path)) && self_foreign_digest_file(body_path, (char[65]){0}, &bytes);
            if (readable) held++; else unreadable++;
            html_escape(twin[0] ? twin : ordered[i], e_twin, sizeof(e_twin)); html_escape(url, e_url, sizeof(e_url)); html_escape(ordered[i], e_name, sizeof(e_name));
            if (twin[0] && url[0]) snprintf(reobserve, sizeof(reobserve),
                "<form class=\"actions\" hx-post=\"/studio/v1/self/foreign/observe\" hx-target=\"#self-foreign-result\" hx-swap=\"innerHTML\">"
                "<input type=\"hidden\" name=\"twin_id\" value=\"%s\"><input type=\"hidden\" name=\"url\" value=\"%s\">"
                "<button class=\"button quiet\" type=\"submit\">Observe again</button></form>", e_twin, e_url);
            w += (size_t)snprintf(html_body + w, sizeof(html_body) - w,
                "<article class=\"source-row\"><h4>%s</h4><p>%s</p><dl><div><dt>Body</dt><dd>%s%s</dd></div><div><dt>Receipt</dt><dd>%s</dd></div></dl>"
                "<form class=\"actions\" hx-post=\"/studio/v1/self/foreign/inspect\" hx-target=\"#self-foreign-inspection\" hx-swap=\"innerHTML\"><input type=\"hidden\" name=\"receipt\" value=\"%s\"><button class=\"button quiet\" type=\"submit\">Inspect retained source</button></form>"
                "%s</article>", e_twin, e_url, readable ? "held" : "unavailable", "", e_name, e_name, reobserve);
        }
        w += (size_t)snprintf(html_body + w, sizeof(html_body) - w,
            "</div><p class=\"hint\">%d body%s readable; %d receipt%s currently lack a readable retained body.</p><div id=\"self-foreign-inspection\"></div></section>",
            held, held == 1 ? " is" : "ies are", unreadable, unreadable == 1 ? "" : "s");
        self_foreign_html_reply(200, html_body); return 1;
    }
}

/* The owner registry, not a web-server enum, decides whether a type can be
 * rendered.  The old hard-coded five-name list made a freshly compiled Frappe
 * type visible in navigation and then refused it at the first useful click. */
static int cms_type_registered(sqlite3 *db, const char *type) {
    sqlite3_stmt *st = NULL;
    int found = 0;
    if (!db || !type || !studio_safe_ident(type)) return 0;
    if (sqlite3_prepare_v2(db,
            "SELECT 1 FROM _content_types WHERE name=? LIMIT 1", -1, &st, NULL) != SQLITE_OK)
        return 0;
    sqlite3_bind_text(st, 1, type, -1, SQLITE_TRANSIENT);
    found = sqlite3_step(st) == SQLITE_ROW;
    sqlite3_finalize(st);
    return found;
}

static int cms_declared_json_field(sqlite3 *db, const char *type, const char *field) {
    sqlite3_stmt *st = NULL;
    const char *schema;
    const char *name;
    const char *end;
    const char *kind;
    char needle[192];
    int json_field = 0;
    if (!db || !type || !field ||
        snprintf(needle, sizeof(needle), "\"%s\"", field) >= (int)sizeof(needle) ||
        sqlite3_prepare_v2(db, "SELECT schema_json FROM _content_types WHERE name=?1", -1, &st, NULL) != SQLITE_OK)
        return 0;
    sqlite3_bind_text(st, 1, type, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && (schema = (const char *)sqlite3_column_text(st, 0)) &&
        (name = strstr(schema, needle)) && (end = strchr(name, '}')) &&
        (kind = strstr(name, "\"type\"")) && kind < end) {
        kind += strlen("\"type\"");
        while (kind < end && (*kind == ':' || isspace((unsigned char)*kind))) kind++;
        json_field = kind + 6 <= end && !memcmp(kind, "\"json\"", 6);
    }
    sqlite3_finalize(st);
    return json_field;
}

static int cms_json_value_valid(sqlite3 *db, const char *value) {
    sqlite3_stmt *st = NULL;
    int valid = 0;
    if (!db || !value || sqlite3_prepare_v2(db, "SELECT json_valid(?1)", -1, &st, NULL) != SQLITE_OK)
        return 0;
    sqlite3_bind_text(st, 1, value, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) valid = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    return valid;
}

/* Render the authored navigation, once per process.
 *
 * Read through the organ's own CMS reader rather than a second query path.
 * Cached because the nav is on every page and a page render is not the place to
 * re-read a store; a nav change lands on the next restart, which is the same
 * cost as the deploy it replaces and a great deal cheaper than the rebuild. */
/* The estate mark and the kit that draws it.
 *
 * Every compiled page already emits the authored nav, so that is where the
 * lockup goes: one mount point, one script, and the same Aurekai x Bonfyre
 * mark the clone surfaces carry. The pages keep everything else they do -- this
 * adds a mark and the address-plane popover and takes nothing away.
 *
 * The script is deferred and failure is silent by design: a page whose
 * navigation depends on an asset finishing is a page that can be taken down by
 * a cache miss. */
#define BF_MARK_MOUNT \
"<span id=\"bf-mk\" class=\"bf-estate-mark\"></span>"
#define BF_SHAPE_MOUNT \
"<span id=\"bf-shape\"></span>"
/* The nav's semantic role changes rather than the nav disappearing.
 * Route, pattern, renderer and data source used to be one thing, so every
 * link could only ask where do you want to go. With the renderer separated
 * the same links ask how do you want to view this world, and the frame --
 * subject, context, territory, audience -- travels with you. */
#define BF_VIEW_MOUNT \
"<span id=\"bf-view\"></span>"
#define BF_MARK_BOOT \
"<script src=\"/assets/bonfyre-kit.js\" defer></script>" \
"<script defer>addEventListener('DOMContentLoaded',function(){" \
"if(!window.K||!K.mountMark)return;" \
/* The compiled pages already carry a wordmark before the nav. The lockup \
 * replaces it rather than joining it: two wordmarks in a row is not a \
 * brand, it is a merge artefact. Where there is no wordmark the mark stays \
 * where the nav put it. */ \
"var w=document.querySelector('.wordmark'),s=document.getElementById('bf-mk');" \
"if(w&&s){w.parentNode.insertBefore(s,w);w.remove();}" \
"K.mountMark('bf-mk',{small:true});" \
/* The density dial belongs to every page that composes /_surface/ fragments, \
 * not only to the console. These pages load their content from the same \
 * paths, so ?x= narrows the whole estate page and the organ -- not this \
 * script -- caps the records and withholds the effect class. shapeBar draws \
 * nothing on a page with no fragment to narrow. */ \
"if(K.shapeBar)K.shapeBar('bf-shape');" \
"if(K.viewControl)K.viewControl('bf-view');});</script>" \
"<style>.bf-estate-mark{display:inline-flex;vertical-align:middle}" \
".topbar .bf-mk{margin-right:1.4rem}" \
"#bf-shape{margin-left:auto;padding-left:1rem}" \
"#bf-view{margin-right:1rem}</style>"

/* The bare authored links, without the chrome the compiled pages wrap them in.
 *
 * A surface that is not a compiled page still has to reach the estate -- a
 * live surface nobody links to is orphaned rather than shipped -- but it does
 * not want the mark mount, the view mount, the density mount and the boot
 * script, because it mounts those where its own design puts them. */
static char bf_nav_links[2048];

static const char *site_nav_links(void) {
    /* The links are a by-product of rendering the nav, and a surface may be the
     * first thing this process serves. Ask for the nav rather than returning an
     * empty list because nobody happened to have needed it yet. */
    if (!bf_nav_links[0]) (void)site_nav_html();
    return bf_nav_links;
}

static const char *site_nav_html(void) {
    static char rendered[4096];
    static int authored = 0;
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    size_t n = 0;

    /* Cache the authored answer, never the degraded one.
     *
     * The first version latched on first call, so a restart while the store was
     * briefly unavailable pinned the compiled fallback for the whole life of
     * the process -- the site kept serving, quietly, with the wrong navigation
     * and no way to notice. A fallback is what you show while you cannot read;
     * it is not an answer worth remembering. */
    if (authored) return rendered;
    snprintf(rendered, sizeof(rendered), "%s%s%s",
             BF_MARK_MOUNT BF_VIEW_MOUNT, BF_NAV_FALLBACK, BF_SHAPE_MOUNT BF_MARK_BOOT);

    db = cms_open_ro();
    if (!db) return rendered;
    if (!cms_type_registered(db, "site_nav")) { sqlite3_close(db); return rendered; }
    if (sqlite3_prepare_v2(db,
            "SELECT label, href FROM site_nav ORDER BY position, id", -1, &st, NULL) == SQLITE_OK) {
        char built[2048];
        size_t used = 0;
        int rows = 0;
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *label = (const char *)sqlite3_column_text(st, 0);
            const char *href = (const char *)sqlite3_column_text(st, 1);
            int w;
            if (!label || !href) continue;
            w = snprintf(built + used, sizeof(built) - used,
                         "<a href=\"%s\">%s</a>", href, label);
            if (w < 0 || (size_t)w >= sizeof(built) - used) { rows = 0; break; }
            used += (size_t)w;
            rows++;
        }
        /* An empty or overflowing nav is worse than the compiled one. The
         * mark wraps whichever list wins: authoring the navigation changes the
         * links, never whether the estate has a mark. */
        if (rows > 0) {
            int w = snprintf(rendered, sizeof(rendered), "%s%.*s%s",
                             BF_MARK_MOUNT BF_VIEW_MOUNT, (int)used, built,
                             BF_SHAPE_MOUNT BF_MARK_BOOT);
            if (w > 0 && (size_t)w < sizeof(rendered)) {
                authored = 1;
                snprintf(bf_nav_links, sizeof(bf_nav_links), "%.*s", (int)used, built);
            }
            else snprintf(rendered, sizeof(rendered), "%s%s%s",
                          BF_MARK_MOUNT BF_VIEW_MOUNT, BF_NAV_FALLBACK, BF_SHAPE_MOUNT BF_MARK_BOOT);
        }
        n = used;
    }
    (void)n;
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);
    return rendered;
}


/* XVII: speculation rules, generated from what this institution actually
 * addresses rather than from a guess that a link might be clicked.
 *
 * The candidate set is the authored navigation -- the paths this institution
 * says are its own -- so a page never speculates on a foreign origin or on a
 * route the address plane does not resolve. Eagerness stays `moderate`, which
 * fires on hover or pointerdown rather than at load: the estate has real
 * paths worth prefetching, and prefetching all of them on every page view
 * would spend a visitor's bandwidth to make its own numbers look better.
 *
 * Cannot infer: a speculated path is not a visited one, and prefetching is not
 * permission -- these are GETs of public authored surfaces, which is the only
 * kind of route that may appear here. */
static const char *site_speculation_html(void) {
    static char rendered[2048];
    static int built = 0;
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    size_t n = 0;
    int rows = 0;

    if (built) return rendered;
    rendered[0] = '\0';
    db = cms_open_ro();
    if (!db) return rendered;
    if (!cms_type_registered(db, "site_nav")) { sqlite3_close(db); return rendered; }
    n = (size_t)snprintf(rendered, sizeof(rendered),
        "<script type=\"speculationrules\">{\"prefetch\":[{\"where\":{\"href_matches\":[");
    if (sqlite3_prepare_v2(db, "SELECT href FROM site_nav WHERE status = 'published' "
                               "ORDER BY position, id", -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW && n < sizeof(rendered) - 256) {
            const char *href = (const char *)sqlite3_column_text(st, 0);
            /* Same-origin absolute paths only. A relative or foreign href in
             * the nav must not become a speculative fetch of someone else's
             * server on this institution's behalf. */
            if (!href || href[0] != '/' || strchr(href, '"') || !strncmp(href, "//", 2)) continue;
            n += (size_t)snprintf(rendered + n, sizeof(rendered) - n,
                                  "%s\"%s\"", rows ? "," : "", href);
            rows++;
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(db);
    if (!rows) { rendered[0] = '\0'; return rendered; }
    snprintf(rendered + n, sizeof(rendered) - n, "]},\"eagerness\":\"moderate\"}]}</script>");
    built = 1;
    return rendered;
}

#include "../../BonfyreCMS/src/entry_snapshot.h"
static int cms_namespace_for(const char *bearer, const char *auth_db,
                             char *out, size_t out_sz) {
    char user[32];
    if (private_cms_namespace[0])
        return snprintf(out, out_sz, "%s", private_cms_namespace) > 0;
    if (!studio_auth_user(bearer, auth_db, user, sizeof(user))) return 0;
    return snprintf(out, out_sz, "actor-%s", user) > 0;
}

/* A free account is intentionally not shown a catalogue full of grammars it
 * cannot lawfully use.  Its `self` scope owns one narrow durable artifact:
 * a private PhysicalCarrier recipe.  Private client CMS is different: its
 * namespace was already compiled from an Upper90 institution closure before
 * this function ran, so it keeps the declared grammar of that admitted estate.
 *
 * This is surface compilation, not a second write policy.  serve_cms_api()
 * still rechecks every mutation at the native boundary. */
static int cms_type_visible_in_closure(const char *bearer, const char *auth_db,
                                       const char *type) {
    if (!type || !*type) return 0;
    if (private_cms_namespace[0]) return 1;
    if (session_authorized(bearer, auth_db, "write")) return 1;
    return !strcmp(type, "physical_carrier") &&
           session_authorized(bearer, auth_db, "self");
}

static void partial_reply(const char *html_body) {
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
           "Content-Length: %zu\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n%s",
           strlen(html_body), html_body);
    fflush(stdout);
}

#include "cms_page_composer.h"
#include "production_publication.h"
#include "source_publication_web.h"
#include "source_web_work.h"
#include "account_activity_web.h"

/* One decoded field bound governs admission and complete form rendering.
 * HTML can expand each admitted byte sixfold; the page reserves room for
 * the entire bounded JSON input plus native form controls. */
enum { CMS_FORM_TEXT_BYTES = 64 * 1024, CMS_FORM_JSON_BYTES = 128 * 1024,
       CMS_FORM_HTML_BYTES = 8 * CMS_FORM_JSON_BYTES };
static int serve_cms_partials(const char *method, const char *target, const char *query,
                              const char *bearer, const char *auth_db) {
    static char body[CMS_FORM_HTML_BYTES];
    char type[128] = {0}, namespace_name[192] = {0};
    int w = 0;

    if (strncmp(target, "/cms/parts/", 11)) return 0;
    if (strcmp(method, "GET")) return 0;
    if (!session_valid(bearer, auth_db)) {
        partial_reply("<div class=\"cms-empty\">Session required. "
                      "<a class=\"text-link\" href=\"/login\">Sign in <span>&rarr;</span></a></div>");
        return 1;
    }
    if (!cms_namespace_for(bearer, auth_db, namespace_name, sizeof(namespace_name))) {
        partial_reply("<div class=\"cms-empty\">Session identity is unavailable.</div>");
        return 1;
    }

    /* The CMS navigation is compiled from the owner registry, not duplicated
     * in browser JavaScript. A newly admitted content type becomes reachable
     * here as soon as the CMS has its table and declaration. */
    if (!strcmp(target, "/cms/parts/nav")) {
        sqlite3 *db = cms_open_ro(); sqlite3_stmt *st = NULL;
        if (!db) { partial_reply("<div class=\"cms-empty\">Content store unavailable.</div>"); return 1; }
        w = snprintf(body, sizeof(body), "<nav class=\"cms-types\" aria-label=\"Content types\">");
        if (sqlite3_prepare_v2(db,
                "SELECT name, COALESCE(json_extract(schema_json,'$.displayName'),name) FROM _content_types ORDER BY "
                "CASE name WHEN 'wiki_page' THEN 0 WHEN 'course_lesson' THEN 1 WHEN 'frappe_app' THEN 2 ELSE 3 END, name",
                -1, &st, NULL) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW && (size_t)w < sizeof(body) - 768) {
                const char *name = (const char *)sqlite3_column_text(st, 0);
                char ename[192], elabel[384];
                if (!name || !studio_safe_ident(name) ||
                    !cms_type_visible_in_closure(bearer, auth_db, name)) continue;
                html_escape(name, ename, sizeof(ename)); html_escape((const char *)sqlite3_column_text(st,1), elabel, sizeof(elabel));
                w += snprintf(body + w, sizeof(body) - (size_t)w,
                  "<button type=\"button\" hx-get=\"/cms/parts/list?type=%s\" hx-target=\"#cms-pane\" hx-swap=\"innerHTML\">"
                  "<span>%s</span><span class=\"n\">Open</span></button>", ename, elabel);
            }
        }
        if (st) sqlite3_finalize(st);
        sqlite3_close(db);
        snprintf(body + w, sizeof(body) - (size_t)w, "</nav>%s%s", BF_CMS_WORKBENCH_STYLE, BF_CMS_WORKBENCH_SCRIPT);
        partial_reply(body); return 1;
    }

    /* Content Home is intentionally a live desk, not a schema catalogue.
     * It reads the same tables the editor writes and gives a person a real
     * starting point before exposing lower-level native building blocks. */
    if (!strcmp(target, "/cms/parts/home")) {
        sqlite3 *db = cms_open_ro();
        sqlite3_stmt *types = NULL;
        int cards = 0;
        if (!db) { partial_reply("<div class=\"cms-empty\">Content store unavailable.</div>"); return 1; }
        w = snprintf(body, sizeof(body),
            "<section class=\"cms-home\"><div class=\"cms-home-intro\">"
            "<p class=\"eyebrow\"><i></i>Authoring / sources, behavior and appearance</p><h3>Give the work a shape.</h3>"
            "<p>Compose working editors, bind your own source material and prepare the next step. Publish pages through the page composer; keep private working productions in their native destination.</p><p><a data-production-studio href=\"/cms/productions?context=self\" style=\"display:inline-flex;padding:13px 21px;background:#d6edb8;color:#153825;border-radius:999px;text-decoration:none;font-weight:700\">Open your Production studio ↗</a></p>"
            "<p><a data-public-production-catalogue href=\"/productions\" style=\"display:inline-flex;padding:12px 20px;border:1px solid #bbd8aa;color:#e4f3d7;border-radius:999px;text-decoration:none;font-weight:700\">Explore published production designs ↗</a></p>"
            "</div>");
        w += snprintf(body+w,sizeof(body)-(size_t)w,"<style id=\"cms-journeys-v1\">.cms-journeys{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;margin:25px 0}.cms-journey{position:relative;display:flex;flex-direction:column;gap:12px;text-align:left;border:1px solid #d2e0cd;border-radius:20px;padding:25px;min-height:165px;background:linear-gradient(135deg,#fff,#eff6eb);cursor:pointer;transition:transform .25s,box-shadow .25s,border-color .25s}.cms-journey:hover,.cms-journey:focus-visible{transform:translateY(-4px);box-shadow:0 15px 35px #14391b1e;border-color:#8fb17c}.cms-journey>span:first-child{color:#71966c;font:700 10px monospace;letter-spacing:.16em}.cms-journey strong{color:#1c3929;font:400 clamp(25px,3vw,40px)/1.04 Georgia,serif;letter-spacing:-.04em}.cms-journey small{color:#537157;font-size:12px}.cms-journey .cms-start-arrow{position:absolute;right:25px;top:22px;color:#2a5a39}.cms-continuations{display:flex;gap:12px;flex-wrap:wrap;margin:20px 0 40px}.cms-continuations a{padding:12px 17px;border-radius:999px;border:1px solid #b8cbb1;color:#24482c;text-decoration:none;font-weight:600;background:#f9fff3}.cms-continuations a:hover{background:#e9f4e4}.cms-schema-library{margin-top:30px;border-top:1px solid #d1ddc9;padding-top:25px}.cms-schema-library summary{cursor:pointer;font:600 16px sans-serif;color:#24482c;margin-bottom:16px}.cms-schema-library .cms-home-grid{margin-top:18px}.cms-schema-library:not([open])>.cms-home-grid{display:none}@media(max-width:780px){.cms-journeys{grid-template-columns:1fr}.cms-journey{min-height:135px;padding:20px}}@media(prefers-reduced-motion:reduce){.cms-journey{transition:none}.cms-journey:hover{transform:none}}</style>");
        /* Common authoring journeys stay prominent; the full schema inventory remains available below. */
        static const struct {const char *type,*title;} starts[] = {
            {"wiki_page","Create a page"},{"cms_article","Develop an article"},
            {"media_program","Produce media"},{"course_lesson","Build a lesson"},
            {"product_identity","Work on a product"},{"site_page","Author a site"},
            {"source_collection","Curate sources"},{"program_template","Configure a program"}
        };
        w += snprintf(body+w,sizeof(body)-(size_t)w,"<div class=\"cms-journeys\">");
        for(size_t i=0;i<sizeof(starts)/sizeof(starts[0]);i++){
            if(!cms_type_visible_in_closure(bearer,auth_db,starts[i].type))continue;
            sqlite3_stmt *look=NULL;int registered=0;
            if(sqlite3_prepare_v2(db,"SELECT 1 FROM _content_types WHERE name=?1",-1,&look,NULL)==SQLITE_OK){
                sqlite3_bind_text(look,1,starts[i].type,-1,SQLITE_STATIC);registered=sqlite3_step(look)==SQLITE_ROW;
            }if(look)sqlite3_finalize(look);if(!registered)continue;
            if(!strcmp(starts[i].type,"site_page")) {
                w += snprintf(body+w,sizeof(body)-(size_t)w,"<a class=\"cms-journey\" href=\"/cms/compose\"><span>PAGE COMPOSITION</span><strong>Compose a page</strong><small>Edit and arrange its sections together ↗</small></a>");
                continue;
            }
            w += snprintf(body+w,sizeof(body)-(size_t)w,
                "<button class=\"cms-journey\" type=\"button\" hx-get=\"/cms/parts/list?type=%s\" hx-target=\"#cms-pane\" hx-swap=\"innerHTML\"><span>%02zu / START</span><strong>%s</strong><small>Continue native records ↗</small></button>",
                starts[i].type,i+1,starts[i].title);
        }
        w += snprintf(body+w,sizeof(body)-(size_t)w,"</div><div class=\"cms-continuations\"><a href=\"/apps?context=self\">Installed tools ↗</a><a href=\"/account#presence\">Your profile ↗</a><a href=\"/packs/setup?context=self\">More capabilities ↗</a></div><section class=\"cms-installed-releases\" aria-labelledby=\"cms-installed-title\" style=\"margin:32px 0\"><h3 id=\"cms-installed-title\" style=\"font:400 clamp(29px,4vw,48px)/1.05 Georgia,serif;margin:0 0 12px\">Continue with your installed instruments</h3><p style=\"max-width:65ch;color:#526254\">Your content can become working material in a specialized instrument. These editors are read from this destination’s native installations, not copied into the CMS.</p><div hx-get=\"/apps/_cards?context=self\" hx-trigger=\"load\" hx-swap=\"innerHTML\"><p class=\"cms-empty\">Checking the editors installed for this destination…</p></div></section><details class=\"cms-schema-library\"><summary>Explore all native content types <span aria-hidden=\"true\">⌄</span></summary><div class=\"cms-home-grid\">");
        /* Every grammar this estate registered, not the first six by name.
         * The desk showed six of forty-odd types, so the census, the packs and
         * the renderers -- all authored here, all registered here -- were
         * unreachable from the surface meant to author them, and every one of
         * them got written by going around it. */
        if (sqlite3_prepare_v2(db, "SELECT name FROM _content_types ORDER BY name", -1, &types, NULL) == SQLITE_OK) {
            while (sqlite3_step(types) == SQLITE_ROW && w > 0 && (size_t)w < sizeof(body) - 768) {
                const char *name = (const char *)sqlite3_column_text(types, 0);
                sqlite3_stmt *count = NULL;
                char sql[192], ename[256];
                int n = 0;
                if (!name || !studio_safe_ident(name) ||
                    !cms_type_visible_in_closure(bearer, auth_db, name)) continue;
                snprintf(sql, sizeof(sql), "SELECT count(*) FROM %s WHERE namespace=?", name);
                if (sqlite3_prepare_v2(db, sql, -1, &count, NULL) == SQLITE_OK) {
                    sqlite3_bind_text(count, 1, namespace_name, -1, SQLITE_TRANSIENT);
                    if (sqlite3_step(count) == SQLITE_ROW) n = sqlite3_column_int(count, 0);
                }
                if (count) sqlite3_finalize(count);
                html_escape(name, ename, sizeof(ename));
                w += snprintf(body + w, sizeof(body) - (size_t)w,
                    "<button class=\"cms-home-card\" type=\"button\" hx-get=\"/cms/parts/list?type=%s\" "
                    "hx-target=\"#cms-pane\" hx-swap=\"innerHTML\"><strong>%d</strong><span>%s</span></button>",
                    ename, n, ename);
                cards++;
            }
        }
        if (types) sqlite3_finalize(types);
        sqlite3_close(db);
        if (!cards)
            w += snprintf(body + w, sizeof(body) - (size_t)w,
                "<div class=\"cms-empty\">No native content grammar is registered yet. Compile or apply one through an authorized owner first.</div>");
        if (w < 0 || (size_t)w >= sizeof(body) - 512) { partial_reply("<div class=\"cms-empty\">Content home is too large to render safely.</div>"); return 1; }
        snprintf(body + w, sizeof(body) - (size_t)w,
            "</div></details></section>");
        partial_reply(body); return 1;
    }

    /* Columns every content type carries, which are not editable by hand. */
    static const char *CMS_SYSTEM_COLS[] = {
        "id","namespace","created_at","updated_at","published_at",NULL
    };
    /* A content type is addressed by name, so it is checked against the native
     * owner registry before becoming an SQL identifier. */
    int type_known = 0;
    if (query_value(query, "type", type, sizeof(type)) && studio_safe_ident(type)) {
        sqlite3 *registry = cms_open_ro();
        if (registry) {
            type_known = cms_type_registered(registry, type);
            sqlite3_close(registry);
        }
    }

    if (type_known && !cms_type_visible_in_closure(bearer, auth_db, type)) {
        partial_reply("<div class=\"cms-empty\">This closure can prepare its own physical carrier recipes. "
                      "Institutional and operational content needs a separately granted write closure.</div>");
        return 1;
    }

    /* Pick the column that best names a row for a person. */
    #define CMS_MAX_COLS 48
    struct { char name[64]; char kind[24]; } cols[CMS_MAX_COLS];
    int ncols = 0, has_status = 0, has_route = 0;
    char display_col[64] = {0};
    if (type_known) {
        sqlite3 *db = cms_open_ro();
        if (db) {
            sqlite3_stmt *st = NULL;
            char pragma[192];
            snprintf(pragma, sizeof(pragma), "PRAGMA table_info(%s)", type);
            if (sqlite3_prepare_v2(db, pragma, -1, &st, NULL) == SQLITE_OK) {
                while (sqlite3_step(st) == SQLITE_ROW && ncols < CMS_MAX_COLS) {
                    const char *cn = (const char *)sqlite3_column_text(st, 1);
                    const char *ct = (const char *)sqlite3_column_text(st, 2);
                    if (!cn) continue;
                    snprintf(cols[ncols].name, sizeof(cols[ncols].name), "%s", cn);
                    snprintf(cols[ncols].kind, sizeof(cols[ncols].kind), "%s", ct ? ct : "TEXT");
                    if (!strcmp(cn, "status")) has_status = 1;
                    if (!strcmp(cn, "route"))  has_route = 1;
                    ncols++;
                }
            }
            if (st) sqlite3_finalize(st);
            sqlite3_close(db);
        }
        /* Prefer fields that actually name the human record.  Compiled Frappe
         * grammars often carry opaque Link columns such as `organization`
         * before their readable `organization_name`; choosing the first
         * column made a successfully saved record look blank in the list. */
        static const char *display_candidates[] = {
            "title", "organization_name", "lead_name", "app_name", "route", "name", NULL
        };
        for (int candidate = 0; display_candidates[candidate] && !display_col[0]; candidate++)
            for (int i = 0; i < ncols && i < CMS_MAX_COLS; i++)
                if (!strcmp(cols[i].name, display_candidates[candidate])) {
                    snprintf(display_col, sizeof(display_col), "%.63s", cols[i].name);
                    break;
                }
        if (!display_col[0])
            for (int i = 0; i < ncols && i < CMS_MAX_COLS && !display_col[0]; i++) {
                int sys = 0;
                for (int k = 0; CMS_SYSTEM_COLS[k]; k++) if (!strcmp(cols[i].name, CMS_SYSTEM_COLS[k])) sys = 1;
                if (!sys && strcmp(cols[i].name,"status")) snprintf(display_col, sizeof(display_col), "%.63s", cols[i].name);
            }
    }

    if (!strncmp(target, "/cms/parts/list", 15)) {
        sqlite3 *db; sqlite3_stmt *st = NULL; char sql[768];
        if (!type_known) { partial_reply("<div class=\"cms-empty\">Choose a content type.</div>"); return 1; }
        db = cms_open_ro();
        if (!db) { partial_reply("<div class=\"cms-empty\">Content store unavailable.</div>"); return 1; }
        w = snprintf(body, sizeof(body),
          "<div class=\"cms-toolbar\"><button class=\"button primary\" "
          "hx-get=\"/cms/parts/edit?type=%s&amp;id=new\" hx-target=\"#cms-pane\" hx-swap=\"innerHTML\">"
          "New entry <span>+</span></button></div>"
          "<table class=\"cms-table\"><thead><tr><th>ID</th><th>%s</th>%s%s<th></th></tr></thead><tbody>",
          type, display_col[0] ? display_col : "Entry",
          has_status ? "<th>Status</th>" : "", has_route ? "<th>Route</th>" : "");
        snprintf(sql, sizeof(sql), "SELECT id, %s%s%s FROM %s WHERE namespace=? ORDER BY id DESC LIMIT 200",
                 display_col[0] ? display_col : "id",
                 has_status ? ", status" : ", ''",
                 has_route ? ", route" : ", ''",
                 type);
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, namespace_name, -1, SQLITE_TRANSIENT);
            int any = 0;
            while (sqlite3_step(st) == SQLITE_ROW && (size_t)w < sizeof(body) - 2048) {
                char et[512], es[64], er[512];
                int id = sqlite3_column_int(st, 0);
                html_escape(sqlite3_column_text(st,1) ? (const char*)sqlite3_column_text(st,1) : "", et, sizeof(et));
                html_escape(sqlite3_column_text(st,2) ? (const char*)sqlite3_column_text(st,2) : "", es, sizeof(es));
                html_escape(sqlite3_column_text(st,3) ? (const char*)sqlite3_column_text(st,3) : "", er, sizeof(er));
                w += snprintf(body + w, sizeof(body) - (size_t)w,
                  "<tr><td class=\"mono\">%d</td><td class=\"cms-title\">%s</td>",
                  id, et[0] ? et : "&mdash;");
                if (has_status)
                    w += snprintf(body + w, sizeof(body) - (size_t)w,
                      "<td><span class=\"pill %s\">%s</span></td>",
                      es[0] ? es : "draft", es[0] ? es : "draft");
                if (has_route)
                    w += snprintf(body + w, sizeof(body) - (size_t)w,
                      "<td class=\"mono\">%s</td>", er[0] ? er : "&mdash;");
                w += snprintf(body + w, sizeof(body) - (size_t)w,
                  "<td><button class=\"button quiet\" hx-get=\"/cms/parts/edit?type=%s&amp;id=%d\" "
                  "hx-target=\"#cms-pane\" hx-swap=\"innerHTML\">Edit <span>&rarr;</span></button></td></tr>",
                  type, id);
                any = 1;
            }
            if (!any) w += snprintf(body + w, sizeof(body) - (size_t)w,
              "<tr><td colspan=\"%d\" class=\"cms-empty\">No entries yet. Use <em>New entry</em> to create the first one.</td></tr>", 3 + has_status + has_route);
        }
        if (st) sqlite3_finalize(st);
        sqlite3_close(db);
        snprintf(body + w, sizeof(body) - (size_t)w, "</tbody></table>");
        partial_reply(body);
        return 1;
    }

    if (!strncmp(target, "/cms/parts/edit", 15)) {
        char idbuf[32] = {0};
        int id = 0, creating;
        sqlite3 *db = NULL; sqlite3_stmt *st = NULL;
        if (!type_known) { partial_reply("<div class=\"cms-empty\">Choose a content type.</div>"); return 1; }
        query_value(query, "id", idbuf, sizeof(idbuf));
        creating = (!idbuf[0] || !strcmp(idbuf, "new"));
        if (!creating) { id = atoi(idbuf); if (id <= 0) creating = 1; }

        w = snprintf(body, sizeof(body),
          "<form class=\"cms-form\" %s=\"/cms/v1/api/%s%s%s\" hx-target=\"#cms-save\" hx-swap=\"innerHTML\">"
          "<div class=\"cms-form-head\"><h3>%s %s</h3><div class=\"actions\">"
          "<button type=\"button\" class=\"button quiet\" hx-get=\"/cms/parts/list?type=%s\" "
          "hx-target=\"#cms-pane\" hx-swap=\"innerHTML\">Back</button>"
          "<button type=\"submit\" class=\"button primary\">%s <span>&rarr;</span></button>"
          "</div></div>",
          creating ? "hx-post" : "hx-put", type, creating ? "" : "/", creating ? "" : idbuf,
          creating ? "New" : "Edit", type,
          type,
          creating ? "Create" : "Save");

        db = cms_open_ro();
        if (!db) { partial_reply("<div class=\"cms-empty\">Content store unavailable. Your record was not changed.</div>"); return 1; }
        if (!creating) {
            if (db) {
                char sel[256];
                snprintf(sel, sizeof(sel), "SELECT * FROM %s WHERE id=? AND namespace=? LIMIT 1", type);
                if (sqlite3_prepare_v2(db, sel, -1, &st, NULL) == SQLITE_OK) {
                    sqlite3_bind_int(st, 1, id);
                    sqlite3_bind_text(st, 2, namespace_name, -1, SQLITE_TRANSIENT);
                    if (sqlite3_step(st) != SQLITE_ROW) { sqlite3_finalize(st); st = NULL; }
                }
            }
        }
        if (!creating && !st) { sqlite3_close(db); partial_reply("<div class=\"cms-empty\">This record is not available in your current destination.</div>"); return 1; }
        if (st) {
            char snapshot[65];
            if (!cms_snapshot_statement(st,snapshot)) { sqlite3_finalize(st); sqlite3_close(db); partial_reply("<div class=\"cms-empty\">A complete editing baseline could not be read.</div>"); return 1; }
            w += snprintf(body+w,sizeof(body)-(size_t)w,"<input type=\"hidden\" name=\"_reviewed_digest\" value=\"%s\">",snapshot);
        }
        for (int i = 0; i < ncols && i < CMS_MAX_COLS; i++) {
            int sys = 0, idx = -1;
            static char val[CMS_FORM_TEXT_BYTES], esc[6 * CMS_FORM_TEXT_BYTES];
            val[0] = 0;
            for (int k = 0; CMS_SYSTEM_COLS[k]; k++) if (!strcmp(cols[i].name, CMS_SYSTEM_COLS[k])) sys = 1;
            if (sys) continue;
            if (st) {
                for (int c = 0; c < sqlite3_column_count(st); c++)
                    if (!strcmp(sqlite3_column_name(st, c), cols[i].name)) { idx = c; break; }
                if (idx >= 0 && sqlite3_column_text(st, idx)) {
                    int bytes = sqlite3_column_bytes(st, idx);
                    if (bytes < 0 || (size_t)bytes >= sizeof(val) ||
                        memchr(sqlite3_column_text(st, idx), 0, (size_t)bytes)) {
                        sqlite3_finalize(st); sqlite3_close(db);
                        partial_reply("<div class=\"cms-empty\">This record exceeds the complete-text limit of this form. Nothing was shortened or changed.</div>");
                        return 1;
                    }
                    snprintf(val, sizeof(val), "%s", (const char *)sqlite3_column_text(st, idx));
                }
            }
            if (creating && !strcmp(cols[i].name,"status")) snprintf(val,sizeof(val),"draft");
            html_escape(val, esc, sizeof(esc));
            if (w < 0 || (size_t)w + strlen(esc) + 4096 >= sizeof(body)) {
                if (st) sqlite3_finalize(st);
                if (db) sqlite3_close(db);
                partial_reply("<div class=\"cms-empty\">The complete form is too large. No partial form was emitted and no record changed.</div>");
                return 1;
            }
            {
                char readable[128],schema_kind[48];int required=0;cms_human_field(cols[i].name,readable,sizeof(readable));int typed=cms_field_metadata(db,type,cols[i].name,schema_kind,sizeof(schema_kind),&required);(void)required;
                int longform = (strstr(cols[i].name,"content") || strstr(cols[i].name,"body") ||
                                strstr(cols[i].name,"notes")   || strstr(cols[i].name,"description"));
                int boolish  = ((typed && !strcmp(schema_kind,"boolean")) || !strcmp(cols[i].name,"published") || !strcmp(cols[i].name,"allow_guest"));
                if (boolish) {
                    w += snprintf(body + w, sizeof(body) - (size_t)w,
                      "<label class=\"check\"><input type=\"checkbox\" value=\"1\" name=\"%s\" %s> %s</label>",
                      cols[i].name, (val[0] && val[0] != '0') ? "checked" : "", readable);
                } else if (longform) {
                    w += snprintf(body + w, sizeof(body) - (size_t)w,
                      "<label><span class=\"flabel\">%s <em>%s</em></span>"
                      "<textarea name=\"%s\" %s>%s</textarea></label>",
                      readable, required?"Required":schema_kind, cols[i].name, required?"required":"", esc);
                } else {
                    w += snprintf(body + w, sizeof(body) - (size_t)w,
                      "<label><span class=\"flabel\">%s <em>%s</em></span>"
                      "<input type=\"text\" name=\"%s\" value=\"%s\" %s></label>",
                      readable, required?"Required":schema_kind, cols[i].name, esc, required?"required":"");
                }
            }
            if ((size_t)w >= sizeof(body) - 4096) break;
        }
        if (st) sqlite3_finalize(st);
        if (db) sqlite3_close(db);
        snprintf(body + w, sizeof(body) - (size_t)w,
          "<p class=\"cms-note\">%s</p>"
          "<div id=\"cms-save\" class=\"cms-status\" aria-live=\"polite\"></div></form>",
          (!strcmp(type, "physical_carrier") && !private_cms_namespace[0] &&
           !session_authorized(bearer, auth_db, "write"))
            ? "This account may retain this private carrier recipe only. It does not publish a QR, authenticate anyone, or authorize payment or device effects."
            : "Fields come from the content type itself. Writing requires the closure that admitted this grammar; a read-only account can open this form and will be refused on save.");
        partial_reply(body);
        return 1;
    }

    return 0;
}

/* Build the request passed to the native CMS from declared table columns only.
 * The edge does not invent a second schema and a browser cannot smuggle an
 * arbitrary column into a native write merely by naming it in a form body. */
static int cms_form_json(sqlite3 *db, const char *type, const char *form,
                         char *out, size_t out_sz) {
    sqlite3_stmt *st = NULL;
    char pragma[192];
    size_t used = 0;
    int emitted = 0;
    if (!db || !type || !form || !out || out_sz < 3) return 0;
    if (snprintf(pragma, sizeof(pragma), "PRAGMA table_info(%s)", type) >= (int)sizeof(pragma)) return 0;
    if (sqlite3_prepare_v2(db, pragma, -1, &st, NULL) != SQLITE_OK) return 0;
    out[used++] = '{'; out[used] = '\0';
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *column = (const char *)sqlite3_column_text(st, 1);
        char value[CMS_FORM_TEXT_BYTES]; /* decoded declared field; still bounded below the CMS JSON carrier */
        int system = 0;
        int json_field;
        if (!column || !studio_safe_ident(column)) continue;
        system = !strcmp(column, "id") || !strcmp(column, "namespace") ||
                 !strcmp(column, "created_at") || !strcmp(column, "updated_at") ||
                 !strcmp(column, "published_at");
        if (system) continue;
        int presence = bf_lex_form_query(form, column, value, sizeof(value));
        if (presence < 0) { sqlite3_finalize(st); return 0; }
        if (!presence) {
            /* HTML omits an unchecked checkbox.  These two CMS disclosure
             * fields must still be able to transition back to false. */
            if (!strcmp(column, "published") || !strcmp(column, "allow_guest"))
                snprintf(value, sizeof(value), "0");
            else
                continue;
        }
        json_field = cms_declared_json_field(db, type, column);
        if (json_field && !cms_json_value_valid(db, value)) { sqlite3_finalize(st); return 0; }
        if (emitted++ && used + 1 < out_sz) out[used++] = ',';
        if (!studio_json_append(out, out_sz, &used, column) || used + 1 >= out_sz) { sqlite3_finalize(st); return 0; }
        out[used++] = ':'; out[used] = '\0';
        if (!(json_field ? studio_json_append_raw(out, out_sz, &used, value)
                         : studio_json_append(out, out_sz, &used, value))) { sqlite3_finalize(st); return 0; }
    }
    sqlite3_finalize(st);
    if (used + 2 >= out_sz) return 0;
    out[used++] = '}'; out[used] = '\0';
    return emitted > 0;
}

static int physical_carrier_value_in(const char *value, const char *const *allowed) {
    for (; value && allowed && *allowed; allowed++) if (!strcmp(value, *allowed)) return 1;
    return 0;
}

/* Mirrors the portable CUE envelope at schemas/control/physical-carrier.cue at
 * the only current native write boundary.  This is intentionally a narrow
 * policy check: it validates a projection recipe, while Matrix/Authority/
 * ProtocolMachine still own address resolution, authorization and effects. */
static int physical_carrier_form_valid(const char *body) {
    static const char *const carriers[] = {"qr","datamatrix","nfc","ble","uwb","barcode",NULL};
    static const char *const payloads[] = {"web-uri","gs1-digital-link","app-clip","emv-qr","matter-onboarding","fido-cross-device","wallet","signed-bonfyre-invocation",NULL};
    static const char *const purposes[] = {"identify","resolve","open","claim","pair","authenticate","pay","admit","redeem","check-in","configure",NULL};
    static const char *const temporal[] = {"permanent","bounded","single-use","rotating",NULL};
    static const char *const audiences[] = {"self","household","group","community","public","market","institution",NULL};
    static const char *const authority[] = {"none","propose","authenticate","requires-confirmation","bounded-effect",NULL};
    static const char *const proximity[] = {"none","scanned-present","nfc","ble-confirmed","uwb-confirmed",NULL};
    char address[1024], carrier[64], payload[96], purpose[64], time_policy[64], audience[64], effect[64], proximity_policy[64], security[2048];
    if (!query_value(body, "subject_address", address, sizeof(address)) || !public_http_url(address) ||
        !query_value(body, "carrier", carrier, sizeof(carrier)) || !physical_carrier_value_in(carrier, carriers) ||
        !query_value(body, "payload_profile", payload, sizeof(payload)) || !physical_carrier_value_in(payload, payloads) ||
        !query_value(body, "purpose", purpose, sizeof(purpose)) || !physical_carrier_value_in(purpose, purposes) ||
        !query_value(body, "temporal_policy", time_policy, sizeof(time_policy)) || !physical_carrier_value_in(time_policy, temporal) ||
        !query_value(body, "audience", audience, sizeof(audience)) || !physical_carrier_value_in(audience, audiences) ||
        !query_value(body, "authority_boundary", effect, sizeof(effect)) || !physical_carrier_value_in(effect, authority) ||
        !query_value(body, "proximity_policy", proximity_policy, sizeof(proximity_policy)) || !physical_carrier_value_in(proximity_policy, proximity) ||
        !query_value(body, "security_policy", security, sizeof(security))) return 0;
    if (!strcmp(time_policy, "permanent") && strcmp(effect, "none") && strcmp(effect, "propose")) return 0;
    if (!strcmp(effect, "bounded-effect") &&
        (!strcmp(time_policy, "permanent") || !strstr(security, "nonce") || !strstr(security, "expiry") ||
         !strstr(security, "signature") || (!strstr(security, "single-use") && !strstr(security, "rotate")))) return 0;
    return 1;
}

/* A carrier recipe names an AddressPlane coordinate, not an arbitrary string
 * that happens to look like a URL.  This parser deliberately accepts only one
 * controlled HTTPS host/path with no caller-selected query, fragment, port or
 * foreign origin; Matrix then resolves the exact active presence at that
 * coordinate.  The carrier still owns neither that subject nor the resulting
 * authority—it records a private projection recipe over the resolved cut. */
static int physical_carrier_resolve_address(BfMatrix *matrix, const char *address,
                                            char *subject_out, size_t subject_out_sz) {
    const char *p, *slash;
    char host[256], path[1024];
    BfMatrixAddressResolution resolution;
    size_t host_len;
    if (!matrix || !address || strncmp(address, "https://", 8) ||
        strlen(address) >= 1400 || strchr(address, '?') || strchr(address, '#')) return 0;
    p = address + 8;
    slash = strchr(p, '/');
    if (!slash) return 0;
    host_len = (size_t)(slash - p);
    if (!host_len || host_len >= sizeof(host) ||
        (strchr(p, ':') && strchr(p, ':') < slash) || strlen(slash) >= sizeof(path)) return 0;
    memcpy(host, p, host_len); host[host_len] = '\0';
    snprintf(path, sizeof(path), "%s", slash);
    if (!matrix_path_resolution(matrix, host, path, &resolution)) return 0;
    return subject_out && subject_out_sz &&
           snprintf(subject_out, subject_out_sz, "%s", resolution.subject_id) < (int)subject_out_sz;
}

/* The CMS write surface answers in whichever grammar the caller asked for.
 *
 * It replied in HTML partials because the only caller was the Content Studio's
 * HTMX desk. Anything that was not a browser -- an agent, a script, this
 * estate's own tooling -- received a fragment of markup where an answer should
 * have been, so authoring routed around the surface entirely and went at the
 * store through ssh and a binary. A write path with no machine projection is a
 * write path that will be bypassed, and then the session check, the schema
 * check and the receipt are bypassed with it. */
static int cms_api_json_wanted = 0;

static void cms_api_status(const char *kind, const char *message, const char *type,
                           const char *id, const char *html) {
    if (!cms_api_json_wanted) { partial_reply(html); return; }
    {
        char em[2048], et[192], ek[64];
        json_escape_into(message ? message : "", em, sizeof(em));
        json_escape_into(type ? type : "", et, sizeof(et));
        json_escape_into(kind ? kind : "", ek, sizeof(ek));
        printf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
               "Cache-Control: no-store\r\nConnection: close\r\n\r\n"
               "{\"schema\":\"bonfyre-cms-write.v1\",\"outcome\":\"%s\","
               "\"type\":\"%s\",\"id\":%s,\"detail\":\"%s\","
               "\"cannot_infer\":\"an authored record is content and never authority; "
               "it says what this institution wrote, not that it is true\"}\n",
               ek, et, (id && *id) ? id : "null", em);
        fflush(stdout);
    }
}

/* The discovery-set room performs a genuine named human transition. Neither
 * the browser nor a form owns the verdict. It only carries a reviewed candidate
 * identity to the same native CMS owner that records discovery decisions.
 * Stronger HLC/multi-field CAS is not provided by this existing CMS contract:
 * an evaluated_at reheat concurrent with this POST remains a known boundary. */
static int serve_discovery_review(const char *method, const char *target,
                                  const char *body, const char *bearer,
                                  const char *auth_db, const char *origin,
                                  const char *host) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char set[120], publisher[256], verdict[24], why[550];
    char updated[70], evaluated[70], actor[32], stamp[64];
    char expected_origin[360], json_body[1300], output[4096], diagnostic[4096];
    char binary[PATH_MAX], *cms_bin, *cms_db, *schemas;
    long id = 0;
    int matched = 0, rc, verified = 0;
    size_t used;
    if (strcmp(target, "/discovery/decide")) return 0;
    if (strcmp(method, "POST")) {
        protocol_reply(405, "text/plain", "This decision requires a POST.\n");
        return 1;
    }
    if (!host || snprintf(expected_origin, sizeof(expected_origin), "https://%s", host) >= (int)sizeof(expected_origin) ||
        !origin || strcmp(origin, expected_origin)) {
        protocol_reply(403, "text/plain", "Cross-origin or originless review refused.\n");
        return 1;
    }
    if (!session_valid(bearer, auth_db) || !session_authorized(bearer, auth_db, "write") ||
        !studio_auth_user(bearer, auth_db, actor, sizeof(actor))) {
        protocol_reply(403, "text/plain", "Reviewer identity with CMS write authority required.\n");
        return 1;
    }
    if (!query_value(body, "set", set, sizeof(set)) ||
        !query_value(body, "publisher", publisher, sizeof(publisher)) ||
        !query_value(body, "verdict", verdict, sizeof(verdict)) ||
        !query_value(body, "reason", why, sizeof(why)) ||
        !query_value(body, "updated", updated, sizeof(updated)) ||
        !query_value(body, "evaluated", evaluated, sizeof(evaluated)) ||
        !studio_safe_ident(set) || (strcmp(verdict, "accepted") && strcmp(verdict, "rejected")) ||
        strlen(why) < 8 || strlen(why) > 500 || !*updated || !*evaluated) {
        protocol_reply(400, "text/plain", "Review must identify a candidate, current generation, declared verdict and reason.\n");
        return 1;
    }
    /* Host identifiers here are exact frontier publisher keys, not URLs. */
    for (const unsigned char *c=(const unsigned char *)publisher; *c; c++)
        if (!isalnum(*c) && *c!='-' && *c!='.') {
            protocol_reply(400, "text/plain", "Invalid publisher identity.\n");
            return 1;
        }
    if (!*publisher || !strchr(publisher,'.')) {
        protocol_reply(400, "text/plain", "Publisher identity required.\n");
        return 1;
    }
    db = cms_open_ro();
    if (!db || sqlite3_prepare_v2(db,
        "SELECT id,verdict,updated_at,evaluated_at FROM discovery_candidate "
        "WHERE set_id=?1 AND root_host=?2 AND namespace='root' AND status='published'",
        -1, &st, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        protocol_reply(503, "text/plain", "The native discovery owner is unavailable.\n");
        return 1;
    }
    sqlite3_bind_text(st,1,set,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,2,publisher,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(st)==SQLITE_ROW) {
        const char *v=(const char *)sqlite3_column_text(st,1);
        const char *u=(const char *)sqlite3_column_text(st,2);
        const char *e=(const char *)sqlite3_column_text(st,3);
        if (v && u && e && !strcmp(v,"pending") && !strcmp(u,updated) && !strcmp(e,evaluated)) {
            id=(long)sqlite3_column_int64(st,0);
            matched=1;
        }
    }
    sqlite3_finalize(st); sqlite3_close(db); st=NULL; db=NULL;
    if (!matched || id<=0) {
        protocol_reply(409, "text/plain", "Candidate changed or was already reviewed. Refresh its source set.\n");
        return 1;
    }
    iso_utc_now(stamp,sizeof(stamp));
    used=(size_t)snprintf(json_body,sizeof(json_body),
        "{\"verdict\":\"%s\",\"decided_by\":\"%s\",\"decided_at\":\"%s\",\"reason\":",
        verdict,actor,stamp);
    if (used >= sizeof(json_body) || !studio_json_append(json_body,sizeof(json_body),&used,why) ||
        used + 2 >= sizeof(json_body)) {
        protocol_reply(400,"text/plain","Review reason exceeded its permitted bounds.\n");
        return 1;
    }
    json_body[used++]='}'; json_body[used]='\0';
    cms_bin=(char*)getenv("BONFYRE_CMS_BINARY");
    cms_db=(char*)getenv("BONFYRE_CMS_DB");
    schemas=(char*)getenv("BONFYRE_CMS_SCHEMAS");
    if (!cms_db || !*cms_db) cms_db="/var/lib/bonfyre/cms/bonfyre_cms.db";
    if (!cms_bin || !*cms_bin) {
        const char *bindir=getenv("BONFYRE_BIN_DIR");
        if (!bindir || !*bindir) bindir="/usr/local/bin";
        if (snprintf(binary,sizeof(binary),"%s/bonfyre-cms",bindir)>=(int)sizeof(binary)) {
            protocol_reply(503,"text/plain","CMS binary coordinate is invalid.\n");return 1;
        }
        cms_bin=binary;
    }
    char rowid[32]; snprintf(rowid,sizeof(rowid),"%ld",id);
    char *argv[] = {cms_bin,(char*)"entry",(char*)"update",(char*)"discovery_candidate",rowid,
       json_body,(char*)"--ns",(char*)"root",(char*)"--db",cms_db,
       (char*)"--if-field",(char*)"verdict",(char*)"--if-value",(char*)"pending",
       (char*)"--if-field2",(char*)"evaluated_at",(char*)"--if-value2",evaluated,
       schemas&&*schemas?(char*)"--schemas":NULL,schemas&&*schemas?schemas:NULL,NULL};
    rc=auth_call_split(argv,NULL,output,sizeof(output),diagnostic,sizeof(diagnostic));
    if (rc != 0) {
        protocol_reply(409,"text/plain","Native CMS refused the reviewed change. Refresh this source set.\n");
        return 1;
    }
    db=cms_open_ro();
    if (db && sqlite3_prepare_v2(db,
        "SELECT verdict,decided_by,reason FROM discovery_candidate "
        "WHERE id=?1 AND namespace='root' AND set_id=?2 AND root_host=?3",
        -1,&st,NULL)==SQLITE_OK) {
        sqlite3_bind_int64(st,1,(sqlite3_int64)id);
        sqlite3_bind_text(st,2,set,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,3,publisher,-1,SQLITE_TRANSIENT);
        if (sqlite3_step(st)==SQLITE_ROW) {
            const char *v=(const char*)sqlite3_column_text(st,0);
            const char *a=(const char*)sqlite3_column_text(st,1);
            const char *r=(const char*)sqlite3_column_text(st,2);
            verified=v&&a&&r&&!strcmp(v,verdict)&&!strcmp(a,actor)&&!strcmp(r,why);
        }
    }
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    if (!verified) {
        protocol_reply(503,"text/plain","Native reviewer state did not match the requested transition.\n");
        return 1;
    }
    printf("HTTP/1.1 303 See Other\r\nLocation: /s/set/%s\r\nCache-Control: no-store\r\n"
           "Content-Type: text/plain\r\nConnection: close\r\n\r\nNative CMS review recorded and read back.\n",set);
    fflush(stdout);
    return 1;
}
/* A native, authorized, read-only continuation of reviewed publisher membership
 * into the existing SourceFrontier+ItemArchive handoff compiler. There is no
 * browser-owned acceptance list, no client-supplied source URL, and no external
 * effect. This route cannot create a SourceCollection or publish a World. */
static int serve_discovery_brief(const char *method, const char *target,
                                 const char *query, const char *bearer,
                                 const char *auth_db) {
    static char output[1024*1024], diagnostic[8192];
    char set[120], domain[36]="cross-domain", format[12]="html";
    char recipient[140]="Research reviewer";
    const char *cms=getenv("BONFYRE_CMS_DB");
    const char *frontier=getenv("BONFYRE_SOURCE_FRONTIER_DB");
    const char *archive=getenv("BONFYRE_ITEM_ARCHIVE_DB");
    const char *script=getenv("BONFYRE_REVIEW_COMPILER");
    const char *bun=getenv("BONFYRE_BUN_BINARY");
    int status;
    if (strcmp(target,"/discovery/brief")) return 0;
    if (strcmp(method,"GET")) {
        protocol_reply(405,"text/plain","The source brief is read-only and requires GET.\n");
        return 1;
    }
    if (!session_valid(bearer,auth_db) || !session_authorized(bearer,auth_db,"write")) {
        protocol_reply(403,"text/plain","A review-authorized session is required to inspect this source cut.\n");
        return 1;
    }
    if (!query_value(query,"set",set,sizeof(set)) || !studio_safe_ident(set)) {
        protocol_reply(400,"text/plain","Choose a named, published native discovery set.\n");
        return 1;
    }
    if (query && *query) {
        char value[140];
        if (query_value(query,"recipient",value,sizeof(value)) && *value)
            snprintf(recipient,sizeof(recipient),"%s",value);
        if (query_value(query,"domain",value,sizeof(value)) && *value)
            snprintf(domain,sizeof(domain),"%s",value);
        if (query_value(query,"format",value,sizeof(value)) && *value)
            snprintf(format,sizeof(format),"%s",value);
    }
    if (!*recipient || strlen(recipient) > 130 ||
        (strcmp(domain,"cross-domain")&&strcmp(domain,"itad")&&strcmp(domain,"property")&&
         strcmp(domain,"upper90")&&strcmp(domain,"civic")&&strcmp(domain,"media")&&strcmp(domain,"supplier")) ||
        (strcmp(format,"html")&&strcmp(format,"md")&&strcmp(format,"yaff"))) {
        protocol_reply(400,"text/plain","Recipient, operating domain or document type is unsupported.\n");
        return 1;
    }
    if (!cms || !*cms || !frontier || !*frontier || !archive || !*archive ||
        !script || !*script || access(script,R_OK)!=0) {
        protocol_reply(503,"text/plain",
          "The source owner, frontier, archive or managed review compiler is unavailable; no brief was fabricated.\n");
        return 1;
    }
    if (!bun || !*bun) bun="bun";
    char *argv[]={ (char*)bun,(char*)"--no-env-file",(char*)"--no-install",
      (char*)script,(char*)"brief",(char*)"--cms",(char*)cms,(char*)"--set",set,
      (char*)"--frontier",(char*)frontier,(char*)"--archive",(char*)archive,
      (char*)"--recipient",recipient,(char*)"--domain",domain,
      (char*)"--format",format,NULL };
    output[0]='\0'; diagnostic[0]='\0';
    status=auth_call_split(argv,NULL,output,sizeof(output),diagnostic,sizeof(diagnostic));
    if (status || !*output) {
        protocol_reply(503,"text/plain",
          "Native source material was unavailable or the reviewed generation was refused.\n");
        return 1;
    }
    const char *type=!strcmp(format,"html")?"text/html":
      !strcmp(format,"md")?"text/markdown":"text/plain";
    printf("HTTP/1.1 200 OK\r\nContent-Type: %s; charset=utf-8\r\n"
      "Cache-Control: no-store\r\nX-Robots-Tag: noindex, nofollow\r\n"
      "Connection: close\r\n",type);
    if (!strcmp(format,"md"))
        fputs("Content-Disposition: attachment; filename=\"Source review.md\"\r\n",stdout);
    else if (!strcmp(format,"yaff"))
        fputs("Content-Disposition: attachment; filename=\"Source review.yaff\"\r\n",stdout);
    printf("\r\n%s",output);
    fflush(stdout);
    return 1;
}
/* Source-reviewed material -> exact native BonfyreEconomy verification case.
 * The existing human source-review owner and the Economy owner perform all
 * writes/readback. No new acceptance database or in-browser approval state.
 */
static int serve_discovery_verification(const char *method,const char *target,
                                         const char *body,const char *bearer,
                                         const char *auth_db,const char *origin,
                                         const char *host) {
    static char output[1024*1024], diagnostic[8192];
    char set[120],source[120],domain[36],recipient[140],generation[80],budget[32];
    char actor[32],expected_origin[360];
    const char *cms=getenv("BONFYRE_CMS_DB");
    const char *frontier=getenv("BONFYRE_SOURCE_FRONTIER_DB");
    const char *archive=getenv("BONFYRE_ITEM_ARCHIVE_DB");
    const char *script=getenv("BONFYRE_REVIEW_COMPILER");
    const char *economy=getenv("BONFYRE_ECONOMY_BINARY");
    const char *economy_db=getenv("BONFYRE_ECONOMY_DB");
    const char *bun=getenv("BONFYRE_BUN_BINARY");
    size_t len;
    if(strcmp(target,"/discovery/verification"))return 0;
    if(strcmp(method,"POST")){
        protocol_reply(405,"text/plain","Native verification intake requires POST.\n");
        return 1;
    }
    if(!host||!origin||snprintf(expected_origin,sizeof(expected_origin),"https://%s",host)>=(int)sizeof(expected_origin)||
       strcmp(origin,expected_origin)){
        protocol_reply(403,"text/plain","Cross-origin or originless verification refused.\n");
        return 1;
    }
    if(!session_valid(bearer,auth_db)||!session_authorized(bearer,auth_db,"write")||
       !studio_auth_user(bearer,auth_db,actor,sizeof(actor))){
        protocol_reply(403,"text/plain","Named reviewer with write authority required.\n");
        return 1;
    }
    if(!query_value(body,"set",set,sizeof(set))||
       !query_value(body,"source",source,sizeof(source))||
       !query_value(body,"domain",domain,sizeof(domain))||
       !query_value(body,"recipient",recipient,sizeof(recipient))||
       !query_value(body,"generation",generation,sizeof(generation))||
       !query_value(body,"budget-cents",budget,sizeof(budget))||
       !studio_safe_ident(set)||
       !*source||strlen(source)>100||
       !*recipient||strlen(recipient)<2||strlen(recipient)>130||
       (strcmp(domain,"cross-domain")&&strcmp(domain,"itad")&&strcmp(domain,"property")&&
        strcmp(domain,"upper90")&&strcmp(domain,"civic")&&strcmp(domain,"media")&&strcmp(domain,"supplier"))){
        protocol_reply(400,"text/plain","Bounded native set, source, domain and recipient required.\n");
        return 1;
    }
    for(const unsigned char *p=(const unsigned char*)source;*p;p++)
        if(!isalnum(*p)&&*p!=':'&&*p!='.'&&*p!='_'&&*p!='-'){
            protocol_reply(400,"text/plain","Source identity must be an exact native source ID.\n");
            return 1;
        }
    if(strlen(generation)!=64||strspn(generation,"0123456789abcdef")!=64||
       !(len=strlen(budget))||len>10||strspn(budget,"0123456789")!=len||
       strtoull(budget,NULL,10)>1000000000ULL){
        protocol_reply(400,"text/plain","A current source digest and bounded cents are required.\n");
        return 1;
    }
    if(!cms||!*cms||!frontier||!*frontier||!archive||!*archive||
       !script||!*script||access(script,R_OK)!=0||
       !economy||!*economy||access(economy,X_OK)!=0||
       !economy_db||!*economy_db){
        protocol_reply(503,"text/plain","A native source or Economy owner is not available; no requirements were invented.\n");
        return 1;
    }
    if(!bun||!*bun)bun="bun";
    char *argv[]={(char*)bun,(char*)"--no-env-file",(char*)"--no-install",
      (char*)script,(char*)"verification",(char*)"--cms",(char*)cms,(char*)"--set",set,
      (char*)"--frontier",(char*)frontier,(char*)"--archive",(char*)archive,
      (char*)"--source",source,(char*)"--domain",domain,
      (char*)"--recipient",recipient,(char*)"--budget-cents",budget,
      (char*)"--expect-generation",generation,(char*)"--economy-bin",(char*)economy,
      (char*)"--economy-db",(char*)economy_db,(char*)"--format",(char*)"html",NULL};
    output[0]='\0';diagnostic[0]='\0';
    if(auth_call_split(argv,NULL,output,sizeof(output),diagnostic,sizeof(diagnostic))!=0||!*output){
        protocol_reply(409,"text/plain","Native source, provenance, domain or verification case changed or was refused. Refresh and inspect the owner state.\n");
        return 1;
    }
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
       "Cache-Control: no-store\r\nX-Robots-Tag: noindex, nofollow\r\n"
       "Connection: close\r\n\r\n%s",output);
    fflush(stdout);
    return 1;
}
static int serve_cms_api(BfMatrix *matrix, const char *method, const char *target, const char *body,
                         const char *bearer, const char *auth_db, const char *accept) {
    const char *prefix = "/cms/v1/api/";
    const char *tail;
    const char *cms_db = getenv("BONFYRE_CMS_DB");
    const char *cms_bin = getenv("BONFYRE_CMS_BINARY");
    const char *schemas = getenv("BONFYRE_CMS_SCHEMAS");
    char type[128] = {0}, id_text[32] = {0}, namespace_name[192] = {0};
    char carrier_subject[256] = {0};
    char json_body[CMS_FORM_JSON_BYTES], output[16384], diagnostic[4096], result_id[32] = {0};
    sqlite3 *db = NULL;
    int updating = 0, rc;

    if (strncmp(target, prefix, strlen(prefix))) return 0;
    cms_api_json_wanted = wants_json(accept);

    if (strcmp(method, "POST") && strcmp(method, "PUT")) {
        cms_api_status("refused", "this surface takes a create or a save", type, id_text,
                       "<div class=\"cms-status err\">This CMS endpoint accepts a create or save form.</div>");
        return 1;
    }
    tail = target + strlen(prefix);
    {
        const char *slash = strchr(tail, '/');
        size_t length = slash ? (size_t)(slash - tail) : strlen(tail);
        if (!length || length >= sizeof(type)) {
            cms_api_status("refused", "the content type is not an identifier", type, id_text,
                       "<div class=\"cms-status err\">Invalid content type.</div>"); return 1;
        }
        memcpy(type, tail, length); type[length] = '\0';
        if (slash) {
            if (strlen(slash + 1) >= sizeof(id_text)) {
                cms_api_status("refused", "the entry identifier is not a number", type, id_text,
                       "<div class=\"cms-status err\">Invalid entry identifier.</div>"); return 1;
            }
            snprintf(id_text, sizeof(id_text), "%s", slash + 1);
            for (char *p = id_text; *p; ++p) if (!isdigit((unsigned char)*p)) {
                partial_reply("<div class=\"cms-status err\">Invalid entry identifier.</div>"); return 1;
            }
            updating = 1;
        }
    }
    if (!studio_safe_ident(type) || (updating != (!strcmp(method, "PUT")))) {
        cms_api_status("refused", "POST creates and PUT saves; the method and the path disagree", type, id_text,
                       "<div class=\"cms-status err\">The request does not match a CMS record transition.</div>"); return 1;
    }
    /* Self scope may retain only a private PhysicalCarrier recipe in the
     * authenticated actor namespace. All other CMS types stay behind generic
     * write scope, including every institutional/Frappe-derived grammar. */
    if (!session_valid(bearer, auth_db) ||
        (!session_authorized(bearer, auth_db, "write") &&
         (strcmp(type, "physical_carrier") || !session_authorized(bearer, auth_db, "self")))) {
        cms_api_status("refused", "this session does not carry write on this record", type, id_text,
                       "<div class=\"cms-status err\">This account is not authorized to write this CMS record.</div>");
        return 1;
    }
    if (!cms_namespace_for(bearer, auth_db, namespace_name, sizeof(namespace_name))) {
        cms_api_status("refused", "no session identity", type, id_text,
                       "<div class=\"cms-status err\">Session identity is unavailable.</div>");
        return 1;
    }
    if (!cms_db || !*cms_db) cms_db = "/var/lib/bonfyre/cms/bonfyre_cms.db";
    db = cms_open_ro();
    char reviewed[65]={0};int has_reviewed=bf_lex_form_query(body?body:"","_reviewed_digest",reviewed,sizeof(reviewed));
    if (has_reviewed<0 || (has_reviewed && (strlen(reviewed)!=64 || strspn(reviewed,"0123456789abcdef")!=64))) {
        if(db)sqlite3_close(db);
        cms_api_status("refused","invalid editing baseline",type,id_text,"<div class=\"cms-status err\">The editing baseline is invalid. Reopen this record.</div>");return 1;
    }
    if (!db || !cms_type_registered(db, type) || !cms_form_json(db, type, body ? body : "", json_body, sizeof(json_body))) {
        if (db) sqlite3_close(db);
        cms_api_status("refused", "the type or the submitted fields did not validate", type, id_text,
                       "<div class=\"cms-status err\">The content type or submitted fields could not be validated.</div>");
        return 1;
    }
    if (updating && has_reviewed) { char actual[65];
        if(!cms_snapshot_row(db,type,atoi(id_text),namespace_name,actual)||strcmp(actual,reviewed)) {
            sqlite3_close(db);cms_api_status("conflict","record changed since opening",type,id_text,"<div class=\"cms-status err\">Someone changed this record after you opened it. Your edits are still here; reopen the saved version before trying again.</div>");return 1;
        }
    }
    sqlite3_close(db);
    if (!strcmp(type, "physical_carrier") && !physical_carrier_form_valid(body ? body : "")) {
        cms_api_status("refused", "the carrier recipe does not satisfy the physical-carrier policy; nothing was published or invoked", type, id_text,
                       "<div class=\"cms-status err\">This carrier recipe does not satisfy the physical-carrier policy: permanent carriers may only resolve or propose; bounded-effect handoffs require nonce, expiry, signature, and single-use or rotation policy. No carrier was published or invoked.</div>");
        return 1;
    }
    if (!strcmp(type, "physical_carrier")) {
        char address[1400], user[32], expected_prefix[64];
        int generic_write = session_authorized(bearer, auth_db, "write");
        if (!query_value(body ? body : "", "subject_address", address, sizeof(address)) ||
            !physical_carrier_resolve_address(matrix, address, carrier_subject, sizeof(carrier_subject))) {
            cms_api_status("refused", "the carrier does not resolve to one controlled address", type, id_text,
                       "<div class=\"cms-status err\">This carrier must resolve to one current controlled Matrix address. It was not saved.</div>");
            return 1;
        }
        if (!generic_write &&
            (!studio_auth_user(bearer, auth_db, user, sizeof(user)) ||
             snprintf(expected_prefix, sizeof(expected_prefix), "subject:actor-%s:", user) >= (int)sizeof(expected_prefix) ||
             strncmp(carrier_subject, expected_prefix, strlen(expected_prefix)))) {
            cms_api_status("refused", "a self carrier may only reference this account's own subject", type, id_text,
                       "<div class=\"cms-status err\">A self carrier may only reference this account's own Life subject. It was not saved.</div>");
            return 1;
        }
    }
    if (!cms_bin || !*cms_bin) {
        const char *bindir = getenv("BONFYRE_BIN_DIR");
        static char default_bin[PATH_MAX];
        if (bindir && *bindir && snprintf(default_bin, sizeof(default_bin), "%s/bonfyre-cms", bindir) < (int)sizeof(default_bin))
            cms_bin = default_bin;
        else cms_bin = "bonfyre-cms";
    }
    if (updating) {
        char *argv[] = { (char *)cms_bin, (char *)"entry", (char *)"update", type, id_text,
                         json_body, (char *)"--ns", namespace_name, (char *)"--db", (char *)cms_db,
                         NULL,NULL,NULL,NULL,NULL };
        int argn=10;if(has_reviewed){argv[argn++]=(char*)"--if-digest";argv[argn++]=reviewed;}
        if(schemas&&*schemas){argv[argn++]=(char*)"--schemas";argv[argn++]=(char*)schemas;}argv[argn]=NULL;
        rc = auth_call_split(argv, NULL, output, sizeof(output), diagnostic, sizeof(diagnostic));
    } else {
        char *argv[] = { (char *)cms_bin, (char *)"entry", (char *)"create", type,
                         (char *)"--ns", namespace_name, json_body, (char *)"--db", (char *)cms_db,
                         schemas && *schemas ? (char *)"--schemas" : NULL,
                         schemas && *schemas ? (char *)schemas : NULL, NULL };
        rc = auth_call_split(argv, NULL, output, sizeof(output), diagnostic, sizeof(diagnostic));
    }
    if (rc != 0) {
        cms_api_status("refused", "the store's own schema check declined this record; nothing was published or sent anywhere", type, id_text,
                       "<div class=\"cms-status err\">The native CMS refused this save. Nothing was published or sent outside Bonfÿre.</div>");
        return 1;
    }
    studio_json_field(output, "id", result_id, sizeof(result_id));
    {
        char escaped_type[256], escaped_id[64], response[3072];
        html_escape(type, escaped_type, sizeof(escaped_type));
        html_escape(result_id[0] ? result_id : id_text, escaped_id, sizeof(escaped_id));
        snprintf(response, sizeof(response),
            "<div class=\"cms-status ok\">%s in the native CMS. "
            "<button type=\"button\" class=\"text-link\" hx-get=\"/cms/parts/edit?type=%s&amp;id=%s\" "
            "hx-target=\"#cms-pane\" hx-swap=\"innerHTML\">Continue editing</button> "
            "<button type=\"button\" class=\"text-link\" hx-get=\"/cms/parts/list?type=%s\" "
            "hx-target=\"#cms-pane\" hx-swap=\"innerHTML\">Back to entries</button>. "
            "%s</div>",
            updating ? "Saved" : "Created", escaped_type, escaped_id, escaped_type,
            !strcmp(type, "physical_carrier")
              ? "The recipe now names one resolved Matrix subject; it remains private, and publication, QR rendering, identity, payment, device control, and effects remain separate decisions."
              : "Publication and public readability remain separate decisions.");
        /* The desk gets its fragment; a machine gets the record's id and the
         * same sentence about what an authored record is not. */
        cms_api_status("saved", updating ? "record saved" : "record created",
                       type, result_id, response);
    }
    return 1;
}

/* ── assets and themes are ForeignTwins ──────────────────────────────────
 *
 * An image, a font, a stylesheet: each is an externally-owned resource we hold
 * a cached projection of. That is exactly a ForeignTwin, so an asset is
 * admitted through the same law as any other foreign observation -- a declared
 * public observe right, a robots preflight, a receipted body, and a content
 * digest -- rather than through an upload path with no provenance.
 *
 * Serving one re-hashes the retained body first. The admission law says a
 * retained body is parsed only while it still hashes to its receipt, and that
 * applies to bytes we are about to hand a browser as much as to bytes we parse.
 * A twin whose body no longer matches its digest is refused, not served. */
static const char *asset_media_type(const unsigned char *b, size_t n) {
    if (n >= 8 && !memcmp(b, "\x89PNG\r\n\x1a\n", 8)) return "image/png";
    if (n >= 3 && b[0] == 0xFF && b[1] == 0xD8 && b[2] == 0xFF) return "image/jpeg";
    if (n >= 6 && (!memcmp(b, "GIF87a", 6) || !memcmp(b, "GIF89a", 6))) return "image/gif";
    if (n >= 12 && !memcmp(b, "RIFF", 4) && !memcmp(b + 8, "WEBP", 4)) return "image/webp";
    if (n >= 4 && !memcmp(b, "wOF2", 4)) return "font/woff2";
    if (n >= 4 && !memcmp(b, "wOFF", 4)) return "font/woff";
    /* Audio is a first-class retained asset too.  Do not label it from a
     * filename: the twin is addressed by a digest and an extension is merely
     * a carrier convenience for static object stores. */
    if (n >= 4 && !memcmp(b, "OggS", 4)) return "audio/ogg";
    if (n >= 4 && !memcmp(b, "fLaC", 4)) return "audio/flac";
    if (n >= 12 && !memcmp(b, "RIFF", 4) && !memcmp(b + 8, "WAVE", 4)) return "audio/wav";
    if (n >= 3 && !memcmp(b, "ID3", 3)) return "audio/mpeg";
    if (n >= 2 && b[0] == 0xff && (b[1] & 0xe0) == 0xe0) return "audio/mpeg";
    if (n >= 5 && !memcmp(b, "<?xml", 5)) return "image/svg+xml";
    if (n >= 4 && !memcmp(b, "<svg", 4)) return "image/svg+xml";
    if (n >= 1 && (b[0] == '{' || b[0] == '[')) return "application/json";
    /* A stylesheet has no magic number; only claim CSS when it reads like one. */
    if (n >= 1) {
        for (size_t i = 0; i + 1 < n && i < 512; i++)
            if (b[i] == '{' && memchr(b, ':', i)) return "text/css; charset=utf-8";
    }
    return "application/octet-stream";
}

/* The content digest is the semantic identity of an asset twin.  A suffix is
 * allowed only as a static-carrier MIME hint (R2 and other object stores use
 * it); it never participates in lookup or changes what bytes are verified.
 * This lets a MediaProgram name /assets/twin/<digest>.ogg while local native
 * serving and static publication address the exact same twin. */
#endif /* BONFYRE_GOVERNANCE_SELF_AND_DISCOVERY_H */
