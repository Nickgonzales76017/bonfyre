#ifndef BONFYRE_WORLD_CACHE_AND_CMS_ROUTE_H
#define BONFYRE_WORLD_CACHE_AND_CMS_ROUTE_H
/* Extracted verbatim from main.c lines 13519-14173 (mechanical split, no logic changes). */
#include "record_factor.h"
static size_t world_cache_dependencies(const char *world, char storage[][640],
                                       const char **out, size_t cap) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL, *source_rows = NULL, *topic = NULL, *publications = NULL;
    size_t count = 0;
    const char *sql =
        "SELECT 'cms:world_fragment:'||fragment_id FROM world_fragment "
        "WHERE world=?1 AND status='published'";
    const char *base[] = {
        "surface:world", "cms:published-material", "foreign:retained-observation", "address:world",
        "cms:community_post", "cms:media_program", "cms:music_recording"
    };
    if (!world || !*world || !out || !cap) return 0;
    snprintf(storage[count], sizeof(storage[count]), "cms:world:%s", world);
    out[count] = storage[count]; count++;
    for (size_t i = 0; i < sizeof(base) / sizeof(base[0]) && count < cap; i++) {
        snprintf(storage[count], sizeof(storage[count]), "%s", base[i]);
        out[count] = storage[count]; count++;
    }
    if (!(db = cms_open_ro()) || !cms_type_registered(db, "world_fragment") ||
        sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) goto out;
    sqlite3_bind_text(st, 1, world, -1, SQLITE_TRANSIENT);
    while (count < cap && sqlite3_step(st) == SQLITE_ROW) {
        const char *dependency = (const char *)sqlite3_column_text(st, 0);
        if (!dependency || !*dependency || strlen(dependency) >= sizeof(storage[count])) continue;
        snprintf(storage[count], sizeof(storage[count]), "%s", dependency);
        out[count] = storage[count]; count++;
    }
    sqlite3_finalize(st); st = NULL;
    /* World source membership is a declared CMS edge.  Join it to the source's
     * exact ForeignTwin topic here so an observed source change retires just
     * this derived public closure. This is forwarding only: no cache row
     * asserts the source is evidence, current truth, or effect authority. */
    if (!cms_type_registered(db, "world") || !cms_type_registered(db, "internet_source") ||
        sqlite3_prepare_v2(db, "SELECT COALESCE(sources,'') FROM world WHERE world=?1 AND status='published'", -1, &source_rows, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, "SELECT url FROM internet_source WHERE source=?1 AND status='published' ORDER BY id DESC LIMIT 1", -1, &topic, NULL) != SQLITE_OK ||
        (cms_type_registered(db, "publication_occurrence") &&
         sqlite3_prepare_v2(db,
           "SELECT occurrence_id FROM publication_occurrence WHERE source_ref=?1 "
           "AND status='published' AND visibility='public' AND assertion_state='OBSERVED' "
           "ORDER BY published_at DESC,id DESC LIMIT 18", -1, &publications, NULL) != SQLITE_OK)) goto out;
    sqlite3_bind_text(source_rows, 1, world, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(source_rows) == SQLITE_ROW && sqlite3_column_text(source_rows, 0)) {
        char list[2048], *part;
        snprintf(list, sizeof(list), "%s", (const char *)sqlite3_column_text(source_rows, 0));
        for (part = strtok(list, ","); part && count < cap; part = strtok(NULL, ",")) {
            char *end = part + strlen(part);
            while (*part && isspace((unsigned char)*part)) part++;
            while (end > part && isspace((unsigned char)end[-1])) *--end = '\0';
            if (!*part || strlen(part) + sizeof("cms:internet_source:") >= sizeof(storage[count])) continue;
            snprintf(storage[count], sizeof(storage[count]), "cms:internet_source:%s", part);
            out[count] = storage[count]; count++;
            if (count >= cap) break;
            if (strlen(part) + sizeof("cms:publication_source:") < sizeof(storage[count])) {
                snprintf(storage[count], sizeof(storage[count]), "cms:publication_source:%s", part);
                out[count] = storage[count]; count++;
            }
            if (count >= cap) break;
            sqlite3_reset(topic); sqlite3_clear_bindings(topic);
            sqlite3_bind_text(topic, 1, part, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(topic) == SQLITE_ROW && sqlite3_column_text(topic, 0)) {
                const char *url = (const char *)sqlite3_column_text(topic, 0);
                if (strlen(url) + sizeof("ForeignTwin:") < sizeof(storage[count])) {
                    snprintf(storage[count], sizeof(storage[count]), "ForeignTwin:%s", url);
                    out[count] = storage[count]; count++;
                }
            }
            /* A World renders admitted PublicationOccurrence bodies, not a
             * generic source feed. Keep their exact owner coordinates in the
             * derived closure so one published/retracted occurrence retires
             * just the World bodies that consumed it. This is still only a
             * cache dependency: it neither admits a foreign assertion nor
             * turns publication material into evidence or a World fact. */
            if (!publications || count >= cap) continue;
            if (strlen("cms:publication_occurrence") < sizeof(storage[count])) {
                snprintf(storage[count], sizeof(storage[count]), "cms:publication_occurrence");
                out[count] = storage[count]; count++;
            }
            sqlite3_reset(publications); sqlite3_clear_bindings(publications);
            sqlite3_bind_text(publications, 1, part, -1, SQLITE_TRANSIENT);
            while (count < cap && sqlite3_step(publications) == SQLITE_ROW) {
                const char *occurrence = (const char *)sqlite3_column_text(publications, 0);
                if (!occurrence || !*occurrence ||
                    strlen(occurrence) + sizeof("cms:publication_occurrence:") >= sizeof(storage[count])) continue;
                snprintf(storage[count], sizeof(storage[count]), "cms:publication_occurrence:%s", occurrence);
                out[count] = storage[count]; count++;
            }
        }
    }
out:
    if (st) sqlite3_finalize(st);
    if (source_rows) sqlite3_finalize(source_rows);
    if (topic) sqlite3_finalize(topic);
    if (publications) sqlite3_finalize(publications);
    if (db) sqlite3_close(db);
    return count;
}
static int emit_native_world_page_cached(const char *path, const char *target, const char *query) {
    BfWorldProjectionEmit emit = {path, target, query};
    char key[512], fingerprint[65], world[128] = {0};
    char dependency_storage[64][640];
    const char *dependencies[64];
    const char *slug = target && !strncmp(target, "/w/", 3) ? target + 3 : NULL;
    size_t n = slug ? strcspn(slug, "/") : 0;
    if ((query && *query) || !public_projection_cache_db() || !slug || !n || n >= sizeof(world))
        return emit_cached_world_body(&emit);
    memcpy(world, slug, n); world[n] = '\0';
    if (!studio_safe_slug_arg(world) ||
        snprintf(key, sizeof(key), "public-world-reading-v2|%s", target) >= (int)sizeof(key))
        return emit_cached_world_body(&emit);
    public_projection_fingerprint(fingerprint, path, target, world);
    size_t dependency_count = world_cache_dependencies(world, dependency_storage, dependencies,
                                                       sizeof(dependencies) / sizeof(dependencies[0]));
    return public_projection_emit(emit_cached_world_body, &emit, key, fingerprint,
                                  dependencies, dependency_count);
}

typedef struct { const char *path; } BfWorldShelfProjectionEmit;
static int emit_cached_world_shelf_body(void *opaque) {
    BfWorldShelfProjectionEmit *shelf = opaque;
    return emit_native_world_shelf(shelf->path);
}
static int emit_native_world_shelf_cached(const char *path) {
    BfWorldShelfProjectionEmit emit = {path};
    char fingerprint[65];
    static const char *const dependencies[] = {
        "surface:world-shelf", "cms:published-world", "address:world-shelf"
    };
    if (!public_projection_cache_db()) return emit_cached_world_shelf_body(&emit);
    public_projection_fingerprint(fingerprint, path, "/w", NULL);
    return public_projection_emit(emit_cached_world_shelf_body, &emit,
                                  "public-world-shelf|/w", fingerprint,
                                  dependencies, sizeof(dependencies) / sizeof(dependencies[0]));
}

/* Listen composes the canonical music identities at read time. It does not
 * collapse Work, Recording, Release, credit, right or playback into one row. */
static int listen_safe_asset(const char *asset) {
    return asset && asset[0] == '/' && asset[1] != '/' && !strstr(asset, "..") && !strchr(asset, '\\');
}

static int serve_listen_program(const char *method, const char *target, const char *query,
                                const char *accept) {
    char recording[192], subject[256], work_ref[256] = {0}, title[512] = {0}, artist[512] = {0};
    char asset[1024] = {0}, transcript[1024] = {0};
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL; int as_html;
    if (strcmp(target, "/_surface/listen-program")) return 0;
    if (strcmp(method, "GET") || !query_value(query, "recording", recording, sizeof(recording)) || !studio_safe_slug_arg(recording)) {
        protocol_reply(400, "application/json", "{\"error\":\"recording_required\"}\n"); return 1;
    }
    if (!(db = cms_open_ro()) || !cms_type_registered(db, "music_recording") || !cms_type_registered(db, "music_work")) {
        if (db) sqlite3_close(db);
        protocol_reply(503, "application/json", "{\"error\":\"music_store_unavailable\"}\n");
        return 1;
    }
    if (sqlite3_prepare_v2(db, "SELECT work_ref,title,primary_artist_ref FROM music_recording WHERE recording_id=?1 AND status='published' ORDER BY id DESC LIMIT 1", -1, &st, NULL) != SQLITE_OK) goto unavailable;
    sqlite3_bind_text(st, 1, recording, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW) { sqlite3_finalize(st); sqlite3_close(db); protocol_reply(404, "application/json", "{\"error\":\"recording_not_found\"}\n"); return 1; }
    snprintf(work_ref, sizeof(work_ref), "%s", sqlite3_column_text(st, 0) ? (const char *)sqlite3_column_text(st, 0) : "");
    snprintf(title, sizeof(title), "%s", sqlite3_column_text(st, 1) ? (const char *)sqlite3_column_text(st, 1) : "");
    snprintf(artist, sizeof(artist), "%s", sqlite3_column_text(st, 2) ? (const char *)sqlite3_column_text(st, 2) : "");
    sqlite3_finalize(st); st = NULL;
    snprintf(subject, sizeof(subject), "recording:%s", recording);
    if (cms_type_registered(db, "media_program") && sqlite3_prepare_v2(db, "SELECT asset_ref,COALESCE(transcript_ref,'') FROM media_program WHERE subject_ref=?1 AND media_kind='audio' AND publication_state='published' AND status='published' ORDER BY id DESC LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, subject, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) { snprintf(asset, sizeof(asset), "%s", sqlite3_column_text(st,0)?(const char *)sqlite3_column_text(st,0):""); snprintf(transcript, sizeof(transcript), "%s", sqlite3_column_text(st,1)?(const char *)sqlite3_column_text(st,1):""); }
        sqlite3_finalize(st); st = NULL;
    }
    as_html = accept && strstr(accept, "text/html");
    printf("HTTP/1.1 200 OK\r\nContent-Type: %s\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n", as_html ? "text/html; charset=utf-8" : "application/json");
    if (!as_html) { fputs("{\"schema_version\":\"bonfyre.listen.v1\",\"recording\":",stdout); json(recording); fputs(",\"recording_title\":",stdout);json(title);fputs(",\"work_ref\":",stdout);json(work_ref);fputs(",\"primary_artist_ref\":",stdout);json(artist);fputs(",\"audio_asset_ref\":",stdout);json(listen_safe_asset(asset)?asset:"");fputs(",\"transcript_ref\":",stdout);json(listen_safe_asset(transcript)?transcript:"");fputs(",\"cannot_infer\":\"a playable representation -> a Work, Recording, Release, credit, right, listener identity, playback state, authority or a licence to redistribute\"}\n",stdout); }
    else {
        /* One shell; the recording plays in the one player, never its own <audio>. */
        ak_app_open(title, "workbench", "route-listen-program");
        fputs("<header class=\"ak-wb-hero is-solo listen-program\"><div><p class=\"ak-kicker\">Listen <span>/</span> Recording</p><h1>",stdout);html(title);
        /* an artist reference that is still an identifier is not a name to show */
        const char *by = strchr(artist, ':') ? "" : artist;
        fputs("</h1>",stdout);if(*by){fputs("<p class=\"ak-lede\">",stdout);html(by);fputs("</p>",stdout);}fputs("<div class=\"ak-actions\">",stdout);
        if(listen_safe_asset(asset)){fputs("<button type=\"button\" class=\"ak-btn is-primary\" data-ak-track data-ak-default data-src=\"",stdout);html(asset);fputs("\" data-title=\"",stdout);html(title);fputs("\" data-by=\"",stdout);html(*by?by:"Listen");fputs("\">Play</button>",stdout);}
        if(listen_safe_asset(transcript)){fputs("<a class=\"ak-btn\" href=\"",stdout);html(transcript);fputs("\">Transcript</a>",stdout);}
        fputs("</div>",stdout);
        if(!listen_safe_asset(asset))fputs("<p class=\"ak-empty\">This recording is listed here, but no public audio for it has been published.</p>",stdout);
        fputs("<p class=\"ak-empty\">The work, its release, credits and rights are kept as separate records.</p></div></header>",stdout);
        ak_app_close();
    }
    sqlite3_close(db); return 1;
unavailable:
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    protocol_reply(503,"application/json","{\"error\":\"music_store_unavailable\"}\n");
    return 1;
}

/* /community is the human community body, not the federation graph.  The
 * latter remains a distinct projection under its own carrier. */
static int serve_human_community_page(const char *method, const char *target,
                                      const char *query, const char *accept) {
    char community[128] = "aurekai-commons";
    sqlite3 *db = NULL; sqlite3_stmt *body = NULL, *posts = NULL;
    const char *title, *description, *governance, *policy, *limit;
    int members = 0, present = 0;
    if (strcmp(target, "/community")) return 0;
    if (strcmp(method, "GET")) { protocol_reply(405, "application/json", "{\"error\":\"GET required\"}\n"); return 1; }
    if (!accept || !strstr(accept, "text/html")) return 0; /* existing machine descriptor */
    if (query && *query && !query_value(query, "community", community, sizeof(community))) {
        protocol_reply(400, "application/json", "{\"error\":\"community_required\"}\n"); return 1;
    }
    if (!studio_safe_slug_arg(community) || !(db = cms_open_ro()) ||
        !cms_type_registered(db, "community_body") || !cms_type_registered(db, "community_post")) {
        if (db) sqlite3_close(db);
        protocol_reply(503, "application/json", "{\"error\":\"community_store_unavailable\"}\n");
        return 1;
    }
    if (sqlite3_prepare_v2(db, "SELECT title,COALESCE(description,''),COALESCE(governance_scope,''),COALESCE(moderation_policy,''),cannot_infer FROM community_body WHERE community=?1 AND status='published' AND visibility='public' ORDER BY id DESC LIMIT 1", -1, &body, NULL) != SQLITE_OK) goto unavailable;
    sqlite3_bind_text(body, 1, community, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(body) != SQLITE_ROW) { sqlite3_finalize(body); sqlite3_close(db); protocol_reply(404, "application/json", "{\"error\":\"community_not_found\"}\n"); return 1; }
    title=(const char *)sqlite3_column_text(body,0); description=(const char *)sqlite3_column_text(body,1);
    governance=(const char *)sqlite3_column_text(body,2); policy=(const char *)sqlite3_column_text(body,3); limit=(const char *)sqlite3_column_text(body,4);
    members = community_active_member_count(db, community);
    present = community_public_presence_count(db, community);
    printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n");
    ak_app_open(title, "workbench", "route-community-body");
    fputs("<header class=\"ak-wb-hero\"><div><p class=\"ak-kicker\">Community <span>/</span> ", stdout); html(governance); fputs("</p><h1>",stdout); html(title); fputs("</h1><p class=\"ak-lede\">",stdout); html(description);
    fputs("</p></div>",stdout);
    printf("<dl class=\"ak-stats\"><div><dt>Members</dt><dd>%d</dd></div><div><dt>Here now</dt><dd class=\"is-on\">%d</dd></div></dl></header>", members, present);
    if (policy && *policy) { fputs("<p class=\"ak-empty\">",stdout); html(policy); fputs("</p>",stdout); }
    char limit_text[1024]; snprintf(limit_text, sizeof(limit_text), "%s", limit ? limit : "");
    sqlite3_finalize(body); body = NULL;
    surface_emit_participation_nav();
    if (sqlite3_prepare_v2(db, "SELECT author_subject,COALESCE(title,''),body,published_at,COALESCE(reply_to,''),post FROM community_post WHERE community=?1 AND status='published' AND visibility='public' AND moderation_state IN ('accepted','visible','published') ORDER BY published_at DESC,id DESC LIMIT 100", -1, &posts, NULL) != SQLITE_OK) goto unavailable;
    sqlite3_bind_text(posts, 1, community, -1, SQLITE_TRANSIENT);
    fputs("<section class=\"ak-sec\" aria-labelledby=\"cb-posts-h\"><header><h2 id=\"cb-posts-h\">Conversation</h2></header><ol class=\"ak-rows cb-posts\">",stdout); int any_post=0;
    while (sqlite3_step(posts) == SQLITE_ROW) { const char *author=(const char *)sqlite3_column_text(posts,0), *ptitle=(const char *)sqlite3_column_text(posts,1), *text=(const char *)sqlite3_column_text(posts,2), *at=(const char *)sqlite3_column_text(posts,3), *reply=(const char *)sqlite3_column_text(posts,4), *post=(const char *)sqlite3_column_text(posts,5); int reactions=community_post_reaction_count(db,post); any_post=1; fputs("<li class=\"post\"><small>",stdout); html(author); fputs(" · ",stdout); html(at); if(reply&&*reply){fputs(" · reply",stdout);} fputs("</small>",stdout); if(ptitle&&*ptitle){fputs("<strong>",stdout);html(ptitle);fputs("</strong>",stdout);} fputs("<p>",stdout);html(text);fputs("</p>",stdout); if(reactions) printf("<small>%d accepted reactions</small>", reactions); fputs("</li>",stdout); }
    if (!any_post) fputs("<li class=\"ak-empty\">Nobody has posted here yet.</li>",stdout);
    fputs("</ol></section>",stdout);
    sqlite3_finalize(posts); sqlite3_close(db); fputs("<p class=\"ak-empty cb-limit\">",stdout); html(limit_text); fputs(" Federation is projected elsewhere.</p>",stdout); ak_app_close(); return 1;
unavailable:
    if (body) sqlite3_finalize(body);
    if (posts) sqlite3_finalize(posts);
    if (db) sqlite3_close(db);
    protocol_reply(503,"application/json","{\"error\":\"community_store_unavailable\"}\n"); return 1;
}

/* Read the trailing segment of a path after a fixed prefix. */
static int studio_path_tail(const char *target, const char *prefix, char *out, size_t size) {
    size_t plen = strlen(prefix);
    if (strncmp(target, prefix, plen)) return 0;
    if (snprintf(out, size, "%s", target + plen) >= (int)size) return 0;
    return out[0] != '\0';
}

static int entity_query(const char *bindir, const char *sql, char *out, size_t out_sz) {
    const char *db = getenv("BONFYRE_ENTITY_DB");
    char qbin[PATH_MAX];
    static char composed[8192];
    char diag[1024];
    if (!db || !*db) db = "/var/lib/bonfyre/state/entity.db";
    if (snprintf(qbin, sizeof(qbin), "%s/bonfyre-query", bindir) >= (int)sizeof(qbin)) return -1;
    if (access(qbin, X_OK) != 0 || access(db, R_OK) != 0) return -1;
    if (snprintf(composed, sizeof(composed),
            "SET home_directory='%s'; INSTALL sqlite; LOAD sqlite; "
            "ATTACH '%s' AS e (TYPE sqlite, READ_ONLY); %s",
            duckdb_home(), db, sql) >= (int)sizeof(composed)) return -1;
    {
        char *argv[] = { qbin, "sql", ":memory:", composed, NULL };
        int rc = auth_call_split(argv, NULL, out, out_sz, diag, sizeof(diag));
        if (rc != 0) fprintf(stderr, "entity_query: %s\n", diag);
        return rc;
    }
}


/* Hex token as issued by the WorkGraph claim (64 lowercase hex). Validated as a
 * character class because it reaches an argv slot. */
static int studio_safe_hex(const char *text, size_t want) {
    size_t n = 0;
    if (!text) return 0;
    for (; text[n]; n++) if (!isxdigit((unsigned char)text[n])) return 0;
    return n == want;
}

/* Mission and node identifiers as the WorkGraph prints them. */
static int studio_safe_ident(const char *text) {
    if (!text || !*text || strlen(text) > 96) return 0;
    for (; *text; text++) if (!(isalnum((unsigned char)*text) || *text == '-' || *text == '_' || *text == '.')) return 0;
    return 1;
}

static int studio_safe_usd_amount(const char *text, char out[32]) {
    char *end = NULL; double v;
    if (!text || !*text || strlen(text) > 24) return 0;
    errno = 0; v = strtod(text, &end);
    if (errno || !end || *end || v < 0.0 || v > 1000000.0) return 0;
    snprintf(out, 32, "%.6f", v);
    return 1;
}

/* The lineage name selects from installed Frappe source trees; it is never a
 * user-controlled filesystem path. Phase one extracts metadata only and does
 * not execute controllers, hooks, migrations, or external effects. */
static const char *studio_frappe_app_dir(const char *lineage) {
    static const struct { const char *lineage; const char *dir; } apps[] = {
        {"frappe-core", "frappe"}, {"crm", "crm"}, {"helpdesk", "helpdesk"},
        {"erpnext", "erpnext"}, {"hrms", "hrms"}, {"lms", "lms"},
        {"wiki", "wiki"}, {"insights", "insights"}, {"drive", "drive"},
    };
    for (size_t i = 0; i < sizeof(apps) / sizeof(apps[0]); i++)
        if (!strcmp(lineage, apps[i].lineage)) return apps[i].dir;
    return NULL;
}


/* Self-invocation for the Matrix binding verbs.
 *
 * The address organ already owns subject/address/alias/presence/standing as CLI
 * verbs against the Matrix DB. Exposing them as typed HTTP mutations by calling
 * that same entry point keeps one implementation: a surface that re-implemented
 * the binding rules would be a second place for them to drift. */
/* Stream a file as the response body.
 *
 * studio_reply() takes a NUL-terminated string, so any projection larger than
 * the caller's buffer was silently cut off -- the ERPNext schema IR is 960 KB
 * and arrived as exactly buffer-minus-one bytes of invalid JSON under a
 * Content-Type promising JSON. Length comes from stat(), and the body is copied
 * straight through, so the response is either complete or it is an error. */
static int studio_reply_file(const char *type, const char *path) {
    struct stat st;
    FILE *f;
    char buf[65536];
    size_t n;
    if (stat(path, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size <= 0) return 0;
    f = fopen(path, "rb");
    if (!f) return 0;
    printf("HTTP/1.1 200 OK\r\nContent-Type: %s\r\nContent-Length: %lld\r\n"
           "Cache-Control: no-store\r\nConnection: close\r\n\r\n",
           type, (long long)st.st_size);
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0) {
        if (fwrite(buf, 1, n, stdout) != n) { fclose(f); return 0; }
    }
    fclose(f);
    fflush(stdout);
    return 1;
}

static int matrix_cli(const char *bindir, char *const extra[], int nextra,
                      char *out, size_t out_sz) {
    const char *db = getenv("BONFYRE_MATRIX_DB");
    char self[PATH_MAX], diag[1024];
    char *argv[24];
    int n = 0;
    if (!db || !*db) return -1;
    if (snprintf(self, sizeof(self), "%s/bonfyre-web-address", bindir) >= (int)sizeof(self)) return -1;
    if (access(self, X_OK) != 0) return -1;
    argv[n++] = self;
    for (int i = 0; i < nextra && n < 20; i++) argv[n++] = extra[i];
    argv[n++] = (char *)"--db";
    argv[n++] = (char *)db;
    argv[n] = NULL;
    {
        int rc = auth_call_split(argv, NULL, out, out_sz, diag, sizeof(diag));
        if (rc != 0) fprintf(stderr, "matrix_cli: %s\n", diag);
        return rc;
    }
}

/* A free-text label that reaches an argv slot: letters, digits, and the
 * punctuation a real name or address needs. Validated, never shell-escaped. */
static int studio_safe_label(const char *text, size_t max) {
    if (!text || !*text || strlen(text) > max) return 0;
    for (; *text; text++)
        if (!(isalnum((unsigned char)*text) || *text == '-' || *text == '_' || *text == '.' ||
              *text == ':' || *text == '/' || *text == ' ' || *text == '@')) return 0;
    return 1;
}

/* Serve a published CMS page at its own route.
 *
 * Content could be created and published, and then still 404 for every reader:
 * the CMS stores a route but nothing resolved one. A page is only served when
 * it is published AND marked guest-readable; a published page without
 * allow_guest requires a session, so publishing is not by itself a disclosure
 * decision. The CMS store is opened read-only -- this surface renders content,
 * it never writes it. */
/* A number out of the roll arrives as a DuckDB double: "232174.0", "1734.0".
 * Printed raw it reads as machine output rather than money or area, so it is
 * grouped in thousands and the empty decimal dropped. Cents survive when they
 * are real. Anything that is not a plain number is passed through untouched,
 * because guessing at a malformed value would be worse than showing it. */
static const char *num_group(const char *raw, char *buf, size_t cap) {
    const char *dot; size_t ip, w = 0, k;
    char frac[8] = {0};
    if (!raw || !*raw) return raw;
    for (const char *z = raw; *z; z++)
        if (!isdigit((unsigned char)*z) && *z != '.' && *z != '-') return raw;
    dot = strchr(raw, '.');
    ip = dot ? (size_t)(dot - raw) : strlen(raw);
    if (dot) {
        snprintf(frac, sizeof(frac), "%s", dot + 1);
        for (char *z = frac + strlen(frac); z > frac && z[-1] == '0'; ) *--z = '\0';
    }
    if (ip && raw[0] == '-') { if (w + 1 < cap) buf[w++] = '-'; }
    for (k = (raw[0] == '-' ? 1 : 0); k < ip && w + 2 < cap; k++) {
        size_t left = ip - k;
        buf[w++] = raw[k];
        if (left > 1 && (left - 1) % 3 == 0) buf[w++] = ',';
    }
    if (frac[0] && w + strlen(frac) + 2 < cap) w += (size_t)snprintf(buf + w, cap - w, ".%s", frac);
    buf[w] = '\0';
    return buf[0] ? buf : raw;
}

/* Group a numeric field in place, right where it is read, so every surface
 * that shows the value shows it the same way. */
static void num_fix(char *v, size_t cap) {
    char buf[64];
    const char *g = num_group(v, buf, sizeof(buf));
    if (g != v) snprintf(v, cap, "%s", g);
}

static void html_escape(const char *in, char *out, size_t out_sz) {
    size_t w = 0;
    for (const unsigned char *p = (const unsigned char *)in; *p && w + 7 < out_sz; p++) {
        const char *rep = NULL;
        switch (*p) {
            case '<': rep = "&lt;"; break;
            case '>': rep = "&gt;"; break;
            case '&': rep = "&amp;"; break;
            case '"': rep = "&quot;"; break;
            default: break;
        }
        if (rep) { size_t n = strlen(rep); memcpy(out + w, rep, n); w += n; }
        else out[w++] = (char)*p;
    }
    out[w] = '\0';
}

static int serve_cms_route(const char *method, const char *target,
                           const char *bearer, const char *auth_db) {
    const char *cms_db = getenv("BONFYRE_CMS_DB");
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char route[512];
    int served = 0;

    if (strncmp(target, "/notes", 6)) return 0;
    if (strcmp(method, "GET")) return 0;

    /* Index: only pages that are published AND guest-readable are listed, so
     * the index can never advertise something a reader cannot open. */
    if (!strcmp(target, "/notes") || !strcmp(target, "/notes/")) {
        sqlite3 *idb = NULL; sqlite3_stmt *ist = NULL;
        if (!cms_db || !*cms_db) cms_db = "/var/lib/bonfyre/cms/bonfyre_cms.db";
        if (sqlite3_open_v2(cms_db, &idb, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { if (idb) sqlite3_close(idb); return 0; }
        printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n");
        ak_app_open("Notes", "workbench", "route-notes");
        fputs("<header class=\"ak-wb-hero is-solo\"><div><p class=\"ak-kicker\">Aurekai <span>/</span> Notes</p><h1>Notes</h1></div></header><ul class=\"ak-rows notes-list\">", stdout);
        {
            int any = 0;
            if (sqlite3_prepare_v2(idb,
                /* status and published_at are what the CMS maintains; the separate
                 * `published` boolean is caller-supplied and can be absent, which
                 * silently 404d a legitimately published page. allow_guest is
                 * unchanged -- it is still the disclosure gate. */
                "SELECT route, title, meta_description FROM wiki_page "
                "WHERE status='published' AND allow_guest=1 "
                "ORDER BY published_at DESC LIMIT 200", -1, &ist, NULL) == SQLITE_OK) {
                while (sqlite3_step(ist) == SQLITE_ROW) {
                    const char *r = (const char *)sqlite3_column_text(ist, 0), *t = (const char *)sqlite3_column_text(ist, 1),
                               *d = (const char *)sqlite3_column_text(ist, 2);
                    fputs("<li><a href=\"/", stdout); html(r ? r : ""); fputs("\"><strong>", stdout); html(t ? t : "");
                    fputs("</strong>", stdout); if (d && *d) { fputs("<small>", stdout); html(d); fputs("</small>", stdout); }
                    fputs("</a></li>", stdout); any = 1;
                }
            }
            if (!any) fputs("<li class=\"ak-empty\">No published notes yet.</li>", stdout);
        }
        if (ist) sqlite3_finalize(ist);
        sqlite3_close(idb);
        fputs("</ul>", stdout);
        ak_app_close();
        fflush(stdout);
        return 1;
    }
    if (strncmp(target, "/notes/", 7)) return 0;
    if (!cms_db || !*cms_db) cms_db = "/var/lib/bonfyre/cms/bonfyre_cms.db";
    if (snprintf(route, sizeof(route), "notes/%s", target + 7) >= (int)sizeof(route)) return 0;
    /* Strip a query string: the route is the path, not the request. */
    { char *q = strchr(route, '?'); if (q) *q = '\0'; }
    if (sqlite3_open_v2(cms_db, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { if (db) sqlite3_close(db); return 0; }
    if (sqlite3_prepare_v2(db,
            "SELECT title, content, published, allow_guest FROM wiki_page "
            "WHERE route = ? AND status = 'published' LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, route, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const char *title = (const char *)sqlite3_column_text(st, 0);
            const char *content = (const char *)sqlite3_column_text(st, 1);
            /* The row was already filtered on status='published'; the separate
             * boolean is caller-supplied and may be absent, so it is not the
             * gate. allow_guest still decides whether a stranger may read. */
            int published = 1;
            int guest = sqlite3_column_int(st, 3);
            (void)sqlite3_column_int(st, 2);
            if (published && (guest || session_valid(bearer, auth_db))) {
                /* The one shell; the note reads in the Stitch reading measure
                 * and can be heard in the one player. */
                printf("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n");
                ak_app_open(title ? title : "Note", "workbench", "route-notes");
                fputs("<article class=\"notes-read\"><header class=\"ak-wb-hero is-solo\"><div><p class=\"ak-kicker\"><a href=\"/notes/\">Notes</a></p><h1>", stdout);
                html(title ? title : ""); fputs("</h1><div class=\"ak-actions\"><button type=\"button\" class=\"ak-btn\" data-ak-track data-narrate=\"#note-text\" data-title=\"", stdout);
                html(title ? title : "Note"); fputs("\" data-by=\"Aurekai notes\">Listen</button></div></div></header><div class=\"notes-text\" id=\"note-text\">", stdout);
                {   /* blank lines separate paragraphs */
                    const char *p = content ? content : "";
                    while (*p) {
                        const char *e = strstr(p, "\n\n"); size_t n = e ? (size_t)(e - p) : strlen(p);
                        if (n) { char *para = strndup(p, n); if (para) { fputs("<p>", stdout); html(para); fputs("</p>", stdout); free(para); } }
                        if (!e) break; p = e + 2; while (*p == '\n') p++;
                    }
                }
                fputs("</div><p class=\"ak-actions\"><a class=\"ak-btn is-quiet\" href=\"/notes/\">All notes</a></p></article>", stdout);
                ak_app_close();
                fflush(stdout);
                served = 1;
            } else if (published && !guest) {
                const char *b = "{\"error\":\"session_required\",\"reason\":\"this page is published but not guest-readable\"}\n";
                printf("HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\n"
                       "Content-Length: %zu\r\nConnection: close\r\n\r\n%s", strlen(b), b);
                fflush(stdout);
                served = 1;
            }
        }
    }
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);
    return served;
}

/* ── x402: the machine-facing edge of the same page ───────────────────────
 *
 * A capability is advertised to agents at /capability, and each one answers
 * HTTP 402 with x402-v1 payment requirements until a payment is presented and
 * verified. Prices are read from the finance service book, so the price an
 * agent is quoted is the same number the financial membrane reports -- there is
 * no second price list to drift.
 *
 * Settlement is deliberately not simulated. Verification requires a configured
 * facilitator; with none, the surface answers 402 and says so rather than
 * treating an unverified X-PAYMENT as settled. A payment rail that pretends is
 * worse than one that refuses. */
typedef struct {
    const char *name;      /* capability id, also the service name in the book */
    const char *binary;    /* organ that produces the result */
    const char *verb;
    const char *summary;
} X402Capability;

static const X402Capability X402_CATALOG[] = {
    {"parcel-underwrite", "bonfyre-query", NULL,
     "Per-city land and exit anchors computed from the receipted DFW parcel estate"},
    {"owner-concentration", "bonfyre-query", NULL,
     "Parcel counts by taxpayer name of record, above a disclosure floor"},
    {"estate-coverage", "bonfyre-query", NULL,
     "Row and page coverage per observed public source, with page digests"},
    {"frappe-schema", "bonfyre-frappe-compiler", NULL,
     "Compiled schema IR for one installed Frappe lineage"},
    /* The routing line. The estate swept 3,316 sources, proved it can read
     * 2,344 of them under a published permission, priced the metered ones from
     * their own published documents, and then served that only to itself
     * through a shell script. This is the same capability offered where an
     * agent can actually reach it: the card advertises it, the price comes from
     * the same finance book every other capability is quoted from, and the
     * answer carries the route that justified it. */
    {"route-frontier", "bonfyre-external-estate", NULL,
     "Cheapest source this estate has proven it can read for a named need, with "
     "the frontier it was chosen from, what it avoided, and a freshness verdict"},
    /* The rest of this catalogue is the same institution's own native command
     * surface, named here the same honest way: a real binary, a one-line
     * statement of what it actually does, and no claim it executes until a
     * payment settles through a configured facilitator. Discovery costs
     * nothing; these are not stubs invented for the marketplace, they are the
     * organs that already ship, listed where an agent can find them. */
    {"entity-resolve", "bonfyre-entity", NULL,
     "Resolves a name or identifier to one canonical actor across records, or "
     "states plainly that it does not resolve"},
    {"artifact-lineage", "bonfyre-graph", NULL,
     "Lineage and dependency layers for a named build artifact, read from the "
     "Merkle-DAG artifact graph"},
    {"content-hash", "bonfyre-hash", NULL,
     "Content-addressed digest and lookup for a named artifact"},
    {"artifact-search", "bonfyre-index", NULL,
     "Searches the artifact family index by name, family or kind"},
    {"compress-family", "bonfyre-compress", NULL,
     "Family-aware compression ratio and plan for a named artifact set"},
    {"text-embed", "bonfyre-embed", NULL,
     "Vector embedding for submitted text, onnx or hash backend"},
    {"value-ledger", "bonfyre-ledger", NULL,
     "Posted value-accounting entries for a named account or period"},
    {"usage-meter", "bonfyre-meter", NULL,
     "Metered usage and billable events for a named provider rate"},
    {"budget-route", "bonfyre-economy", NULL,
     "Cheapest route under a stated budget, with what it enforced to hold it"},
    {"brief-proof", "bonfyre-proof", NULL,
     "Scores a written brief against the source it names; finds invented or "
     "broken statements, not missing ones"},
    {"multi-reason", "bonfyre-reason", NULL,
     "Several answers to one prompt from a model that was actually run, diffable "
     "by mode"},
    {"recipe-gen", "bonfyre-gen", NULL,
     "Compiles a natural-language request to a recipe YAML; does not execute it"},
    {"capability-match", "bonfyre-capability", NULL,
     "Matches a stated need against this institution's own declared capability "
     "catalogue"},
    {"clip-discover", "bonfyre-clips", NULL,
     "Finds candidate clips in retained media matching a stated criterion"},
    {"object-detect", "bonfyre-detect-objects", NULL,
     "What a detector finds in a submitted image, with confidence"},
    {"code-canon", "bonfyre-canon", NULL,
     "Structural canonicalization of submitted source text via Tree-sitter, for "
     "stable comparison"},
    {"frame-extract", "bonfyre-frame-extract", NULL,
     "Extracts named frames from a submitted video for downstream inspection"},
    {"compute-admission", "bonfyre-admission", NULL,
     "Whether this institution's memory or disk floor can currently admit a "
     "stated heavy claim"},
};
#define X402_CATALOG_N ((int)(sizeof(X402_CATALOG) / sizeof(X402_CATALOG[0])))

static const X402Capability *x402_find(const char *name) {
    for (int i = 0; i < X402_CATALOG_N; i++)
        if (!strcmp(name, X402_CATALOG[i].name)) return &X402_CATALOG[i];
    return NULL;
}

/* The skills the A2A card advertises.
 *
 * This table exists because the card and the /a2a endpoint were two places that
 * each spelled the skill list out by hand, which is the same drift the price
 * book was built to prevent one layer down. A skill named here is named
 * identically in both, or in neither.
 *
 * `capability` is the X402_CATALOG entry that executes the skill, so the price
 * an agent is quoted over A2A is read from the same finance service book the
 * x402 surface quotes. NULL means the skill is real but is not a priced query,
 * and `note` has to say what it is instead -- an unpriced skill is not free, it
 * is admitted somewhere else. */
typedef struct {
    const char *id;
    const char *name;
    const char *description;
    const char *tags;        /* JSON array literal, emitted verbatim */
    const char *capability;  /* X402_CATALOG name, or NULL */
    const char *note;        /* required when capability and execute are both NULL */
    /* Set only for a skill that is real, free and answerable without any
     * settlement question at all -- not a stub waiting on a facilitator, and
     * not work admitted elsewhere. Writes a real JSON value into `out` and
     * returns 1, or writes a stated reason and returns 0. Called in-process,
     * same request, no fork. */
    int (*execute)(const char *body_in, char *out, size_t out_sz);
} A2ASkill;

/* Two free, in-process capabilities over a corpus that already answers people
 * at /recipes: 159,329 factored records with real FTS5 search and an exact-
 * fraction quantity planner, linked into this binary already (recipe_material.c,
 * record_factor.c). Nothing here is paid or gated -- the same data already
 * answers any GET request with no facilitator and no price -- so an agent
 * asking over A2A gets the identical real result a browser gets, not a quote
 * it can never settle. This is the corpus's machine door; /recipes is its
 * human one. */
typedef struct { char *buf; size_t cap; size_t n; int count; } A2ARecipeHits;

static int a2a_recipe_hit(void *ctx, sqlite3_int64 seq, const char *key, const char *title, const char *kind) {
    A2ARecipeHits *h = ctx;
    char ek[256], et[512];
    (void)seq; (void)kind;
    json_escape_into(key ? key : "", ek, sizeof(ek));
    json_escape_into(title ? title : "", et, sizeof(et));
    int w = snprintf(h->buf + h->n, h->cap - h->n, "%s{\"key\":\"%s\",\"title\":\"%s\"}",
                      h->count ? "," : "", ek, et);
    if (w < 0 || (size_t)w >= h->cap - h->n) return 1; /* genuine overflow: stop the visit */
    h->n += (size_t)w; h->count++;
    return 0;
}

static int a2a_exec_recipe_search(const char *body_in, char *out, size_t out_sz) {
    char query[512] = {0}, equery[1024];
    static char hits[8192];
    A2ARecipeHits ctx = { hits, sizeof(hits), 0, 0 };
    sqlite3 *db = NULL;
    int n;
    if (!studio_json_string(body_in, "query", query, sizeof(query)) || !query[0]) {
        snprintf(out, out_sz, "{\"error\":\"missing_param\",\"param\":\"query\","
                 "\"usage\":\"params must include a 'query' string\"}");
        return 0;
    }
    if (bf_record_factor_open(bf_recipe_material_path(), &db)) {
        snprintf(out, out_sz, "{\"error\":\"no_recipe_material\"}");
        return 0;
    }
    n = bf_record_factor_visit(db, query, 20, 0, a2a_recipe_hit, &ctx);
    sqlite3_close(db);
    if (n < 0) {
        snprintf(out, out_sz, "{\"error\":\"search_failed\"}");
        return 0;
    }
    json_escape_into(query, equery, sizeof(equery));
    snprintf(out, out_sz,
        "{\"query\":\"%s\",\"count\":%d,\"hits\":[%.*s],"
        "\"note\":\"factored corpus search; open a hit with the recipe-brief skill\"}",
        equery, ctx.count, (int)ctx.n, hits);
    return 1;
}

static int a2a_exec_recipe_brief(const char *body_in, char *out, size_t out_sz) {
    char key[256] = {0}, ekey[512], copies_text[32] = {0}, *end;
    static char escaped[32768];
    char *text = NULL; size_t length = 0;
    long copies = 1;
    FILE *stream;
    int failed;
    if (!studio_json_string(body_in, "recipe_key", key, sizeof(key)) || !key[0]) {
        snprintf(out, out_sz, "{\"error\":\"missing_param\",\"param\":\"recipe_key\","
                 "\"usage\":\"params must include 'recipe_key' from recipe-search\"}");
        return 0;
    }
    if (studio_json_string(body_in, "copies", copies_text, sizeof(copies_text)) && copies_text[0]) {
        errno = 0; copies = strtol(copies_text, &end, 10);
        if (errno || *end || copies < 1 || copies > 100) copies = 1;
    }
    json_escape_into(key, ekey, sizeof(ekey));
    stream = open_memstream(&text, &length);
    if (!stream) { snprintf(out, out_sz, "{\"error\":\"stream_failed\"}"); return 0; }
    failed = bf_recipe_material_brief(bf_recipe_material_path(), key, (int)copies, stream);
    fclose(stream);
    if (failed) {
        free(text);
        snprintf(out, out_sz, "{\"error\":\"no_verified_brief\",\"recipe_key\":\"%s\"}", ekey);
        return 0;
    }
    json_escape_into(text, escaped, sizeof(escaped));
    free(text);
    snprintf(out, out_sz, "{\"recipe_key\":\"%s\",\"copies\":%ld,\"brief\":\"%s\"}", ekey, copies, escaped);
    return 1;
}

#include "a2a_retained_material.h"

static const A2ASkill A2A_SKILLS[] = {
    {"parcel-underwrite", "Parcel underwriting anchors",
     "Per-city land and exit anchors from the receipted DFW parcel roll",
     "[\"real-estate\",\"valuation\"]", "parcel-underwrite", NULL, NULL},
    {"owner-concentration", "Owner concentration",
     "Parcel counts by taxpayer name of record",
     "[\"real-estate\",\"ownership\"]", "owner-concentration", NULL, NULL},
    {"work-mission", "Durable work mission",
     "Enqueue, claim and complete a leased WorkGraph mission",
     "[\"work\",\"tasks\"]", NULL,
     "a WorkGraph mission is leased work admitted through the work surface, "
     "not a query sold by the row", NULL},
    {"route-frontier", "Routed source for a need",
     "Name a coordinate and get the cheapest source this estate has proven it can "
     "read, the frontier it was chosen from, the metered alternatives it avoided, "
     "and whether the content still matches what was last observed",
     "[\"routing\",\"discovery\",\"provenance\"]", "route-frontier", NULL, NULL},
    {"estate-coverage", "Source coverage",
     "Row and page coverage per observed public source, with page digests",
     "[\"estate\",\"provenance\"]", "estate-coverage", NULL, NULL},
    {"frappe-schema", "Compiled Frappe schema",
     "Compiled schema IR for one installed Frappe lineage",
     "[\"schema\",\"compiler\"]", "frappe-schema", NULL, NULL},
    {"entity-resolve", "Entity resolution",
     "Resolves a name or identifier to one canonical actor across records",
     "[\"identity\",\"resolution\"]", "entity-resolve", NULL, NULL},
    {"artifact-lineage", "Artifact lineage",
     "Lineage and dependency layers for a named build artifact",
     "[\"provenance\",\"build\"]", "artifact-lineage", NULL, NULL},
    {"content-hash", "Content addressing",
     "Content-addressed digest and lookup for a named artifact",
     "[\"provenance\",\"integrity\"]", "content-hash", NULL, NULL},
    {"artifact-search", "Artifact search",
     "Searches the artifact family index by name, family or kind",
     "[\"search\",\"artifacts\"]", "artifact-search", NULL, NULL},
    {"compress-family", "Family-aware compression",
     "Compression ratio and plan for a named artifact set",
     "[\"storage\",\"compression\"]", "compress-family", NULL, NULL},
    {"text-embed", "Text embedding",
     "Vector embedding for submitted text, onnx or hash backend",
     "[\"embedding\",\"search\"]", "text-embed", NULL, NULL},
    {"value-ledger", "Value ledger read",
     "Posted value-accounting entries for a named account or period",
     "[\"finance\",\"ledger\"]", "value-ledger", NULL, NULL},
    {"usage-meter", "Usage metering read",
     "Metered usage and billable events for a named provider rate",
     "[\"finance\",\"metering\"]", "usage-meter", NULL, NULL},
    {"budget-route", "Budget-bounded routing",
     "Cheapest route under a stated budget, with what it enforced to hold it",
     "[\"routing\",\"cost\"]", "budget-route", NULL, NULL},
    {"brief-proof", "Brief-against-source proof",
     "Scores a written brief against the source it names; finds invented or "
     "broken statements, not missing ones",
     "[\"evidence\",\"review\"]", "brief-proof", NULL, NULL},
    {"multi-reason", "Multi-mode reasoning",
     "Several answers to one prompt from a model that was actually run",
     "[\"reasoning\",\"inference\"]", "multi-reason", NULL, NULL},
    {"recipe-gen", "Recipe generation",
     "Compiles a natural-language request to a recipe YAML; does not execute it",
     "[\"authoring\",\"pipeline\"]", "recipe-gen", NULL, NULL},
    {"capability-match", "Capability matching",
     "Matches a stated need against this institution's own declared capability "
     "catalogue",
     "[\"discovery\",\"meta\"]", "capability-match", NULL, NULL},
    {"clip-discover", "Clip discovery",
     "Finds candidate clips in retained media matching a stated criterion",
     "[\"media\",\"discovery\"]", "clip-discover", NULL, NULL},
    {"object-detect", "Object detection",
     "What a detector finds in a submitted image, with confidence",
     "[\"media\",\"vision\"]", "object-detect", NULL, NULL},
    {"code-canon", "Structural code canonicalization",
     "Canonicalizes submitted source text via Tree-sitter, for stable comparison",
     "[\"code\",\"diffing\"]", "code-canon", NULL, NULL},
    {"frame-extract", "Video frame extraction",
     "Extracts named frames from a submitted video for downstream inspection",
     "[\"media\",\"video\"]", "frame-extract", NULL, NULL},
    {"compute-admission", "Compute admission check",
     "Whether this institution's memory or disk floor can currently admit a "
     "stated heavy claim",
     "[\"infra\",\"capacity\"]", "compute-admission", NULL, NULL},
    /* Free, not paid, not a stub: real FTS5 search over 159,329 factored
     * records, executed in process, same request. params: {"query":"..."}. */
    {"recipe-search", "Recipe search",
     "Full-text search over the factored recipe corpus; returns matching "
     "titles and keys to open with recipe-brief",
     "[\"food\",\"search\"]", NULL, NULL, a2a_exec_recipe_search},
    /* Same corpus, one record: the exact-fraction quantity plan and shopping
     * brief already served to people at /recipes?format=brief, executed here
     * for an agent instead. params: {"recipe_key":"...","copies":N}. */
    {"recipe-brief", "Recipe brief",
     "Exact-quantity preparation brief for one recipe or preparation family, "
     "scaled to a stated number of sets",
     "[\"food\",\"planning\"]", NULL, NULL, a2a_exec_recipe_brief},
    {"retained-sources", "Released source discovery",
     "List released public feed sources from the canonical CMS declaration",
     "[\"feeds\",\"provenance\",\"discovery\"]", NULL, NULL, a2a_exec_retained_sources},
    {"retained-latest", "Recent retained source items",
     "Browse a published source by retained archive time and exact item key",
     "[\"feeds\",\"archive\"]", NULL, NULL, a2a_exec_retained_latest},
    {"retained-item", "Exact retained source excerpt",
     "Read one archived public excerpt and its original credited link, never an inferred full text",
     "[\"feeds\",\"provenance\"]", NULL, NULL, a2a_exec_retained_item},
};
#define A2A_SKILLS_N ((int)(sizeof(A2A_SKILLS) / sizeof(A2A_SKILLS[0])))

static const A2ASkill *a2a_find(const char *id) {
    for (int i = 0; i < A2A_SKILLS_N; i++)
        if (!strcmp(id, A2A_SKILLS[i].id)) return &A2A_SKILLS[i];
    return NULL;
}

/* Sell price for a capability, read from the finance service book. Returns 0
 * when the capability is not priced -- an unpriced capability is not for sale,
 * which is different from being free. */
#endif /* BONFYRE_WORLD_CACHE_AND_CMS_ROUTE_H */
