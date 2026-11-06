/* BonfyreSourceFrontier -- the accounting plane between everything this estate
 * has listed and the small number of channels a person actually sees.
 *
 * The estate holds several inventories that all say "source" while meaning
 * different cardinalities: the V4 ExternalEstate occurrences, AddressPlane
 * candidate advertisements, the public dataset registry, the census rows the
 * pages read, retained-body link discoveries, the public commons passports.
 * Added together they double-count; kept apart they cannot be reasoned about.
 *
 * This organ holds one ledger with four levels and keeps each inventory's
 * provenance:
 *
 *   SourceRoot      one canonical publisher origin (a host, minus www)
 *   ChannelFamily   one machine protocol at that root (rss, ics, arcgis, ...)
 *   Channel         one exact addressed endpoint in that family
 *   SourceGap       one thing the estate does not know about that root
 *
 * Counting law, enforced here rather than described: a channel is counted at
 * exactly one state, a root is counted once however many inventories listed
 * it, a link discovered inside a retained body starts as SHADOW rather than a
 * source, a virtual channel (a catalogue's declared members) is reported in
 * its own column, and a refused channel stays counted as refused instead of
 * disappearing.  So the ledger may grow to hundreds of thousands of generated
 * channels without ever claiming hundreds of thousands of active sources.
 *
 * Cannot infer: a listed channel -> a lawful, callable, credentialed or
 * available source; an admitted channel -> one a person ever saw; an
 * attention level -> permission to fetch, buy, contact or enrol; a gap's
 * resolution class -> a decision to take it.
 */
#define _POSIX_C_SOURCE 200809L
#include <bonfyre.h>
#include <bf_json.h>
#include <bf_sqlite_path.h>
#include <bf_command_lookup.h>
#include <ctype.h>
#include <limits.h>
#include <strings.h>
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define SF_ADDRESS_MAX 8192
#define SF_ID_LEN 24

static const char *arg(int argc, char **argv, const char *name) {
    int i;
    for (i = 2; i + 1 < argc; i++) if (!strcmp(argv[i], name)) return argv[i + 1];
    return NULL;
}

static int flag(int argc, char **argv, const char *name) {
    int i;
    for (i = 2; i < argc; i++) if (!strcmp(argv[i], name)) return 1;
    return 0;
}

static void json_fstr(FILE *out, const char *s) {
    fputc('"', out);
    for (; s && *s; s++) {
        unsigned char c = (unsigned char)*s;
        if (c == '"' || c == '\\') { fputc('\\', out); fputc(c, out); }
        else if (c == '\n') fputs("\\n", out);
        else if (c < 0x20) fprintf(out, "\\u%04x", c);
        else fputc(c, out);
    }
    fputc('"', out);
}

static void json_str(const char *s) {
    json_fstr(stdout, s);
}

static int exec_sql(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) { fprintf(stderr, "sqlite: %s\n", err ? err : "error"); sqlite3_free(err); return 0; }
    return 1;
}

static const char *jstr(const bf_json_doc_t *doc, const bf_json_node_t *obj, const char *key, char *buf, size_t cap) {
    const bf_json_node_t *n = bf_json_obj_get(doc, obj, key);
    buf[0] = '\0';
    if (n && n->type == BF_JSON_STRING) bf_json_get_str_copy(n, buf, cap);
    return buf[0] ? buf : NULL;
}

static int jbool(const bf_json_doc_t *doc, const bf_json_node_t *obj, const char *key) {
    const bf_json_node_t *n = bf_json_obj_get(doc, obj, key);
    return n && n->type == BF_JSON_BOOL && bf_json_get_bool(n);
}

static void digest_id(const char *text, char out[SF_ID_LEN + 1]) {
    char hex[65];
    bf_sha256_hex((const uint8_t *)text, strlen(text), hex);
    memcpy(out, hex, SF_ID_LEN);
    out[SF_ID_LEN] = '\0';
}

/* ------------------------------------------------------------ addresses
 *
 * Two inventories may list the same endpoint with a different case, a
 * tracking parameter or a trailing slash.  Those are one channel, and the
 * exact address is kept as written for whoever reads it.
 */
static int tracking_param(const char *p, size_t n) {
    static const char *const junk[] = {"utm_", "fbclid", "gclid", "mc_cid", "mc_eid", "igshid", "_ga", "yclid", "msclkid"};
    size_t k;
    for (k = 0; k < sizeof(junk) / sizeof(junk[0]); k++) {
        size_t len = strlen(junk[k]);
        if (junk[k][len - 1] == '_') { if (n > len && !strncasecmp(p, junk[k], len)) return 1; }
        else if (n == len && !strncasecmp(p, junk[k], len)) return 1;
    }
    return 0;
}

static int normalise_address(const char *raw, char *out, size_t cap, char *host_out, size_t host_cap) {
    const char *p = raw, *host, *rest, *query;
    char scheme[16] = "", hostbuf[256] = "", path[SF_ADDRESS_MAX] = "";
    size_t n, used = 0;
    if (!raw || !*raw) return 0;
    while (*p == ' ' || *p == '\t') p++;
    if (!strncasecmp(p, "https://", 8)) { snprintf(scheme, sizeof(scheme), "https"); p += 8; }
    else if (!strncasecmp(p, "http://", 7)) { snprintf(scheme, sizeof(scheme), "http"); p += 7; }
    else return 0;                              /* only what a browser or a fetch can open */
    host = p;
    rest = host + strcspn(host, "/?#");
    n = (size_t)(rest - host);
    if (!n || n >= sizeof(hostbuf)) return 0;
    memcpy(hostbuf, host, n); hostbuf[n] = '\0';
    for (n = 0; hostbuf[n]; n++) hostbuf[n] = (char)tolower((unsigned char)hostbuf[n]);
    { char *at = strchr(hostbuf, '@'); if (at) memmove(hostbuf, at + 1, strlen(at)); }
    { char *port = strrchr(hostbuf, ':');
      if (port && (!strcmp(port, ":80") || !strcmp(port, ":443"))) *port = '\0'; }
    if (!strncmp(hostbuf, "www.", 4)) memmove(hostbuf, hostbuf + 4, strlen(hostbuf) - 3);
    if (!hostbuf[0] || !strchr(hostbuf, '.')) return 0;
    query = strchr(rest, '#') ? NULL : NULL;
    (void)query;
    /* path without fragment */
    n = strcspn(rest, "#");
    if (n >= sizeof(path)) n = sizeof(path) - 1;
    memcpy(path, rest, n); path[n] = '\0';
    used = (size_t)snprintf(out, cap, "%s://%s", scheme, hostbuf);
    if (used >= cap) return 0;
    {
        char *q = strchr(path, '?');
        char kept[SF_ADDRESS_MAX]; size_t k = 0;
        if (q) *q++ = '\0';
        n = strlen(path);
        while (n > 1 && path[n - 1] == '/') path[--n] = '\0';   /* one address, not two */
        used += (size_t)snprintf(out + used, cap - used, "%s", path);
        if (used >= cap) return 0;
        kept[0] = '\0';
        while (q && *q) {
            char *amp = strchr(q, '&');
            size_t len = amp ? (size_t)(amp - q) : strlen(q);
            size_t name = strcspn(q, "=");
            if (name > len) name = len;
            if (len && !tracking_param(q, name)) {
                if (k + len + 2 >= sizeof(kept)) break;
                kept[k] = k ? '&' : '?'; k++;
                memcpy(kept + k, q, len); k += len; kept[k] = '\0';
            }
            if (!amp) break;
            q = amp + 1;
        }
        if (k) {
            kept[0] = '?';
            used += (size_t)snprintf(out + used, cap - used, "%s", kept);
            if (used >= cap) return 0;
        }
    }
    snprintf(host_out, host_cap, "%s", hostbuf);
    return 1;
}

/* ------------------------------------------------------------ schema */

static const char *SCHEMA =
    "PRAGMA journal_mode=DELETE;"
    "CREATE TABLE IF NOT EXISTS inventory(label TEXT PRIMARY KEY, first_seen TEXT, last_seen TEXT, rows INTEGER DEFAULT 0, refused INTEGER DEFAULT 0);"
    "CREATE TABLE IF NOT EXISTS root(root_id TEXT PRIMARY KEY, host TEXT NOT NULL, name TEXT DEFAULT '', identity TEXT DEFAULT '',"
    " kind TEXT DEFAULT '', robots TEXT DEFAULT 'unknown', economic TEXT DEFAULT '', first_seen TEXT, last_seen TEXT);"
    "CREATE TABLE IF NOT EXISTS root_origin(root_id TEXT, label TEXT, listings INTEGER DEFAULT 0, PRIMARY KEY(root_id,label));"
    "CREATE TABLE IF NOT EXISTS family(family_id TEXT PRIMARY KEY, root_id TEXT NOT NULL, protocol TEXT NOT NULL, endpoint TEXT DEFAULT '',"
    " discovered_from TEXT DEFAULT '', first_seen TEXT);"
    "CREATE TABLE IF NOT EXISTS channel(channel_id TEXT PRIMARY KEY, family_id TEXT NOT NULL, root_id TEXT NOT NULL,"
    " address TEXT NOT NULL, exact_address TEXT NOT NULL, title TEXT DEFAULT '', material TEXT DEFAULT '', protocol TEXT DEFAULT 'unknown',"
    " state TEXT NOT NULL DEFAULT 'candidate', truth_level TEXT DEFAULT 'listed', refusal TEXT DEFAULT '', virtual INTEGER DEFAULT 0,"
    " census_source TEXT DEFAULT '', credential TEXT DEFAULT '', rights TEXT DEFAULT '', cadence TEXT DEFAULT '',"
    " discovered_from TEXT DEFAULT '', body_sha256 TEXT DEFAULT '', identity TEXT DEFAULT '',"
    " first_seen TEXT, last_seen TEXT, last_contributed TEXT DEFAULT '', contributions INTEGER DEFAULT 0);"
    "CREATE TABLE IF NOT EXISTS channel_origin(channel_id TEXT, label TEXT, PRIMARY KEY(channel_id,label));"
    /* One endpoint may be read by several census rows -- three Wikipedia feeds
     * read different material from one address -- so readers are many. */
    "CREATE TABLE IF NOT EXISTS channel_census(channel_id TEXT, census_source TEXT, material TEXT DEFAULT '', PRIMARY KEY(channel_id,census_source));"
    "CREATE TABLE IF NOT EXISTS contribution(route TEXT, channel_id TEXT, day TEXT, items INTEGER DEFAULT 0, PRIMARY KEY(route,channel_id,day));"
    "CREATE TABLE IF NOT EXISTS gap(gap_id TEXT PRIMARY KEY, root_id TEXT NOT NULL, channel_id TEXT DEFAULT '', kind TEXT NOT NULL,"
    " state TEXT NOT NULL, resolutions TEXT NOT NULL, dependents INTEGER DEFAULT 0, measured TEXT DEFAULT '', status TEXT DEFAULT 'open',"
    " first_seen TEXT, last_seen TEXT);"
    "CREATE TABLE IF NOT EXISTS attention(root_id TEXT PRIMARY KEY, level TEXT NOT NULL, reason TEXT DEFAULT '', updated TEXT);"
    "CREATE INDEX IF NOT EXISTS channel_root ON channel(root_id);"
    "CREATE INDEX IF NOT EXISTS channel_family ON channel(family_id);"
    "CREATE INDEX IF NOT EXISTS family_root ON family(root_id);"
    "CREATE INDEX IF NOT EXISTS channel_exact_address ON channel(exact_address);"
    "CREATE INDEX IF NOT EXISTS root_host ON root(host);"
    "CREATE INDEX IF NOT EXISTS channel_state ON channel(state);"
    "CREATE INDEX IF NOT EXISTS channel_by_census ON channel(census_source);"
    "CREATE INDEX IF NOT EXISTS contribution_channel ON contribution(channel_id);";

static sqlite3 *open_db(const char *path, int create) {
    sqlite3 *db = NULL;
    if (!path) { fprintf(stderr, "source frontier: --db is required\n"); return NULL; }
    if (sqlite3_open(path, &db) != SQLITE_OK) { fprintf(stderr, "open %s failed\n", path); sqlite3_close(db); return NULL; }
    sqlite3_busy_timeout(db, 5000);
    if (create && !exec_sql(db, SCHEMA)) { sqlite3_close(db); return NULL; }
    /* Older ledgers predate measured carriage. Index only its declared column;
     * no read request or guessed migration invents that evidence. */
    if (create && sqlite3_table_column_metadata(db,NULL,"channel","carries",NULL,NULL,NULL,NULL,NULL)==SQLITE_OK &&
        !exec_sql(db,"CREATE INDEX IF NOT EXISTS channel_public_selection ON channel("
                     "state,material,carries,census_source,root_id,protocol,last_contributed,contributions,first_seen)")) {
        sqlite3_close(db);return NULL;
    }
    return db;
}

static const char *now_iso(char buf[32]) {
    time_t t = time(NULL); struct tm tm;
    gmtime_r(&t, &tm);
    strftime(buf, 32, "%Y-%m-%dT%H:%M:%SZ", &tm);
    return buf;
}

/* A state may rise as the estate learns more about a channel; it never falls
 * silently.  A refusal is written beside the state so a refused channel stays
 * in the count with its reason. */
static int state_rank(const char *state) {
    if (!state || !*state) return 0;
    if (!strcmp(state, "shadow")) return 1;
    if (!strcmp(state, "candidate")) return 2;
    if (!strcmp(state, "eligible")) return 3;
    if (!strcmp(state, "waiting_credential")) return 4;
    if (!strcmp(state, "admitted")) return 5;
    if (!strcmp(state, "active")) return 6;
    return 0;
}

static int valid_state(const char *state) {
    return state_rank(state) > 0 || (state && !strcmp(state, "refused"));
}

/* ------------------------------------------------------------ import */

typedef struct {
    sqlite3 *db;
    sqlite3_stmt *root_ins, *root_touch, *origin_ins, *family_ins, *chan_sel, *chan_ins, *chan_up, *chan_origin;
    const char *label;
    const char *stamp;
    int rows, channels, roots, refused, skipped;
} Import;

static int import_row(Import *im, const bf_json_doc_t *doc, const bf_json_node_t *row) {
    char address[SF_ADDRESS_MAX], exact[SF_ADDRESS_MAX], host[256], key[SF_ADDRESS_MAX + 64];
    char root_id[SF_ID_LEN + 1], family_id[SF_ID_LEN + 1], channel_id[SF_ID_LEN + 1];
    char buf[SF_ADDRESS_MAX], protocol[64] = "unknown", state[32] = "candidate";
    char name[256] = "", identity[64] = "", kind[64] = "", material[64] = "", title[512] = "";
    char census[128] = "", credential[64] = "", rights[128] = "", cadence[64] = "", from[256] = "", body[65] = "";
    char refusal[256] = "", truth[64] = "listed", robots[32] = "";
    int is_virtual = jbool(doc, row, "virtual"), rank_old = 0;
    if (!jstr(doc, row, "address", buf, sizeof(buf))) { im->skipped++; return 0; }
    snprintf(exact, sizeof(exact), "%s", buf);
    if (!normalise_address(exact, address, sizeof(address), host, sizeof(host))) { im->skipped++; return 0; }
    if (jstr(doc, row, "protocol", buf, sizeof(buf))) snprintf(protocol, sizeof(protocol), "%s", buf);
    if (jstr(doc, row, "state", buf, sizeof(buf)) && valid_state(buf)) snprintf(state, sizeof(state), "%s", buf);
    if (jstr(doc, row, "refusal", buf, sizeof(buf))) snprintf(refusal, sizeof(refusal), "%s", buf);
    if (jstr(doc, row, "truth_level", buf, sizeof(buf))) snprintf(truth, sizeof(truth), "%s", buf);
    jstr(doc, row, "name", name, sizeof(name));
    jstr(doc, row, "identity", identity, sizeof(identity));
    jstr(doc, row, "kind", kind, sizeof(kind));
    jstr(doc, row, "material", material, sizeof(material));
    jstr(doc, row, "title", title, sizeof(title));
    jstr(doc, row, "census_source", census, sizeof(census));
    jstr(doc, row, "credential", credential, sizeof(credential));
    jstr(doc, row, "rights", rights, sizeof(rights));
    jstr(doc, row, "cadence", cadence, sizeof(cadence));
    jstr(doc, row, "discovered_from", from, sizeof(from));
    jstr(doc, row, "body_sha256", body, sizeof(body));
    jstr(doc, row, "robots", robots, sizeof(robots));

    digest_id(host, root_id);
    snprintf(key, sizeof(key), "%s|%s", root_id, protocol);
    digest_id(key, family_id);
    digest_id(address, channel_id);

    sqlite3_reset(im->root_ins);
    sqlite3_bind_text(im->root_ins, 1, root_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(im->root_ins, 2, host, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(im->root_ins, 3, im->stamp, -1, SQLITE_STATIC);
    if (sqlite3_step(im->root_ins) == SQLITE_DONE && sqlite3_changes(im->db)) im->roots++;
    sqlite3_reset(im->root_touch);
    sqlite3_bind_text(im->root_touch, 1, name, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(im->root_touch, 2, identity, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(im->root_touch, 3, kind, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(im->root_touch, 4, robots, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(im->root_touch, 5, im->stamp, -1, SQLITE_STATIC);
    sqlite3_bind_text(im->root_touch, 6, root_id, -1, SQLITE_TRANSIENT);
    sqlite3_step(im->root_touch);
    sqlite3_reset(im->origin_ins);
    sqlite3_bind_text(im->origin_ins, 1, root_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(im->origin_ins, 2, im->label, -1, SQLITE_STATIC);
    sqlite3_step(im->origin_ins);

    sqlite3_reset(im->family_ins);
    sqlite3_bind_text(im->family_ins, 1, family_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(im->family_ins, 2, root_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(im->family_ins, 3, protocol, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(im->family_ins, 4, from, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(im->family_ins, 5, im->stamp, -1, SQLITE_STATIC);
    sqlite3_step(im->family_ins);

    sqlite3_reset(im->chan_sel);
    sqlite3_bind_text(im->chan_sel, 1, channel_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(im->chan_sel) == SQLITE_ROW) {
        rank_old = state_rank((const char *)sqlite3_column_text(im->chan_sel, 0));
        if (rank_old > state_rank(state) && strcmp(state, "refused")) snprintf(state, sizeof(state), "%s", (const char *)sqlite3_column_text(im->chan_sel, 0));
        sqlite3_reset(im->chan_up);
        sqlite3_bind_text(im->chan_up, 1, state, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_up, 2, refusal, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_up, 3, title, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_up, 4, material, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_up, 5, census, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_up, 6, credential, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_up, 7, identity, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_up, 8, im->stamp, -1, SQLITE_STATIC);
        sqlite3_bind_text(im->chan_up, 9, channel_id, -1, SQLITE_TRANSIENT);
        sqlite3_step(im->chan_up);
    } else {
        sqlite3_reset(im->chan_ins);
        sqlite3_bind_text(im->chan_ins, 1, channel_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_ins, 2, family_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_ins, 3, root_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_ins, 4, address, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_ins, 5, exact, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_ins, 6, title, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_ins, 7, material, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_ins, 8, protocol, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_ins, 9, state, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_ins, 10, truth, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_ins, 11, refusal, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(im->chan_ins, 12, is_virtual ? 1 : 0);
        sqlite3_bind_text(im->chan_ins, 13, census, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_ins, 14, credential, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_ins, 15, rights, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_ins, 16, cadence, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_ins, 17, from, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_ins, 18, body, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_ins, 19, identity, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(im->chan_ins, 20, im->stamp, -1, SQLITE_STATIC);
        if (sqlite3_step(im->chan_ins) != SQLITE_DONE) { im->skipped++; return 0; }
        im->channels++;
    }
    sqlite3_reset(im->chan_origin);
    sqlite3_bind_text(im->chan_origin, 1, channel_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(im->chan_origin, 2, im->label, -1, SQLITE_STATIC);
    sqlite3_step(im->chan_origin);
    if (refusal[0]) im->refused++;
    im->rows++;
    return 1;
}

static int cmd_import(sqlite3 *db, const char *label, const char *path) {
    Import im = {0};
    FILE *f = strcmp(path, "-") ? fopen(path, "r") : stdin;
    char *line = NULL, stamp[32]; size_t cap = 0; ssize_t len;
    if (!f) { perror(path); return 0; }
    im.db = db; im.label = label; im.stamp = now_iso(stamp);
    sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO root(root_id,host,first_seen,last_seen) VALUES(?1,?2,?3,?3)", -1, &im.root_ins, NULL);
    sqlite3_prepare_v2(db, "UPDATE root SET name=CASE WHEN ?1<>'' THEN ?1 ELSE name END, identity=CASE WHEN ?2<>'' THEN ?2 ELSE identity END,"
                           " kind=CASE WHEN ?3<>'' THEN ?3 ELSE kind END, robots=CASE WHEN ?4<>'' THEN ?4 ELSE robots END, last_seen=?5 WHERE root_id=?6", -1, &im.root_touch, NULL);
    sqlite3_prepare_v2(db, "INSERT INTO root_origin(root_id,label,listings) VALUES(?1,?2,1)"
                           " ON CONFLICT(root_id,label) DO UPDATE SET listings=listings+1", -1, &im.origin_ins, NULL);
    sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO family(family_id,root_id,protocol,discovered_from,first_seen) VALUES(?1,?2,?3,?4,?5)", -1, &im.family_ins, NULL);
    sqlite3_prepare_v2(db, "SELECT state FROM channel WHERE channel_id=?1", -1, &im.chan_sel, NULL);
    sqlite3_prepare_v2(db, "INSERT INTO channel(channel_id,family_id,root_id,address,exact_address,title,material,protocol,state,truth_level,refusal,"
                           "virtual,census_source,credential,rights,cadence,discovered_from,body_sha256,identity,first_seen,last_seen)"
                           " VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?20)", -1, &im.chan_ins, NULL);
    sqlite3_prepare_v2(db, "UPDATE channel SET state=?1, refusal=CASE WHEN ?2<>'' THEN ?2 ELSE refusal END,"
                           " title=CASE WHEN ?3<>'' THEN ?3 ELSE title END, material=CASE WHEN ?4<>'' THEN ?4 ELSE material END,"
                           " census_source=CASE WHEN ?5<>'' THEN ?5 ELSE census_source END, credential=CASE WHEN ?6<>'' THEN ?6 ELSE credential END,"
                           " identity=CASE WHEN ?7<>'' THEN ?7 ELSE identity END, last_seen=?8 WHERE channel_id=?9", -1, &im.chan_up, NULL);
    sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO channel_origin(channel_id,label) VALUES(?1,?2)", -1, &im.chan_origin, NULL);
    exec_sql(db, "BEGIN");
    while ((len = getline(&line, &cap, f)) > 0) {
        char err[160]; bf_json_doc_t *doc;
        if (len < 3) continue;
        doc = bf_json_parse(line, (size_t)len, err, sizeof(err));
        if (doc && bf_json_root(doc)) import_row(&im, doc, bf_json_root(doc)); else im.skipped++;
        if (doc) bf_json_free(doc);
    }
    exec_sql(db, "COMMIT");
    {
        sqlite3_stmt *inv = NULL;
        sqlite3_prepare_v2(db, "INSERT INTO inventory(label,first_seen,last_seen,rows,refused) VALUES(?1,?2,?2,?3,?4)"
                               " ON CONFLICT(label) DO UPDATE SET last_seen=?2, rows=?3, refused=?4", -1, &inv, NULL);
        sqlite3_bind_text(inv, 1, label, -1, SQLITE_STATIC);
        sqlite3_bind_text(inv, 2, stamp, -1, SQLITE_STATIC);
        sqlite3_bind_int(inv, 3, im.rows);
        sqlite3_bind_int(inv, 4, im.refused);
        sqlite3_step(inv); sqlite3_finalize(inv);
    }
    sqlite3_finalize(im.root_ins); sqlite3_finalize(im.root_touch); sqlite3_finalize(im.origin_ins);
    sqlite3_finalize(im.family_ins); sqlite3_finalize(im.chan_sel); sqlite3_finalize(im.chan_ins);
    sqlite3_finalize(im.chan_up); sqlite3_finalize(im.chan_origin);
    free(line);
    if (f != stdin) fclose(f);
    printf("{\"label\":"); json_str(label);
    printf(",\"rows\":%d,\"new_channels\":%d,\"new_roots\":%d,\"refused\":%d,\"unusable\":%d}\n", im.rows, im.channels, im.roots, im.refused, im.skipped);
    return 1;
}

static int count_of(sqlite3 *db, const char *sql);

/* ------------------------------------------------------------ census link
 *
 * The census is what the pages actually read.  A channel whose address is a
 * published census row is admitted; a census row nothing listed becomes its
 * own channel, so the ledger never claims fewer sources than the site reads.
 */
typedef struct {
    int requested;
    int considered;
    int candidates;
    int ambiguous;
    int conflicts;
    int unresolved;
    int by_census_root;
    int by_official_website;
    int by_official_website_parent;
    int names_from_items;
    int name_only;
    int refused_platform;
    int ident_only;
    int host_shaped_names;
} IdentityBackfillStats;

static int valid_qid(const char *s) {
    size_t i;
    if (!s || s[0] != 'Q' || s[1] < '1' || s[1] > '9') return 0;
    for (i = 2; s[i]; i++) if (!isdigit((unsigned char)s[i])) return 0;
    return 1;
}

/* A host that carries many unrelated publishers' material.  Its own identity
 * (the hosting company) is never the identity of what a tenant publishes, so
 * a source read from it is never joined to that identity by host. */
static const char *const SHARED_PUBLISHING_HOSTS[] = {
    "blogspot.com", "wordpress.com", "substack.com", "github.io", "medium.com", "tumblr.com",
    "wixsite.com", "squarespace.com", "typepad.com", "livejournal.com", "feedburner.com",
    "feedblitz.com", "libsyn.com", "anchor.fm", "podbean.com", "buzzsprout.com", "simplecast.com",
    "megaphone.fm", "soundcloud.com", "rss.com", "pdrl.fm", "omny.fm", "spreaker.com",
    "transistor.fm", "acast.com", "art19.com", "youtube.com", "google.com", "apple.com",
    "spotify.com", "podcastics.com", "captivate.fm", "redcircle.com", "audioboom.com",
    "blubrry.com", "fireside.fm", "ghost.io", "beehiiv.com", "mailchimp.com", "eventbrite.com",
    "meetup.com", "archive.org", "wikimedia.org", "wikipedia.org", "reddit.com", "x.com",
    "twitter.com", "facebook.com", "instagram.com", "tiktok.com", "linkedin.com", "amazonaws.com",
    "cloudfront.net", "azurewebsites.net", "herokuapp.com", "netlify.app", "vercel.app", "pages.dev",
    "socrata.com", "arcgis.com", "hub.arcgis.com", "opendatasoft.com", "ckan.org", NULL
};

static int shared_publishing_host(const char *host) {
    size_t hn = strlen(host), i;
    for (i = 0; SHARED_PUBLISHING_HOSTS[i]; i++) {
        const char *p = SHARED_PUBLISHING_HOSTS[i];
        size_t pn = strlen(p);
        if (hn == pn && !strcmp(host, p)) return 1;
        if (hn > pn && host[hn - pn - 1] == '.' && !strcmp(host + hn - pn, p)) return 1;
    }
    return 0;
}

/* A retained item's publisher field often carries only the host it was read
 * from.  A host is an address, not what the publisher is called. */
static int host_shaped_name(const char *name) {
    return name && strchr(name, '.') && !strchr(name, ' ');
}

/* The canonical identity an encyclopedia records for exactly this host as an
 * official website.  Returns 1 with qid set, 0 for none, -1 when the root
 * carries something that is not one canonical identity. */
static int root_identity_for_host(sqlite3_stmt *by_root, const char *host, char *qid, size_t cap) {
    char root_id[SF_ID_LEN + 1];
    int found = 0;
    digest_id(host, root_id);
    sqlite3_reset(by_root);
    sqlite3_clear_bindings(by_root);
    sqlite3_bind_text(by_root, 1, root_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(by_root, 2, host, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(by_root) == SQLITE_ROW) {
        const char *identity = (const char *)sqlite3_column_text(by_root, 0);
        if (valid_qid(identity)) { snprintf(qid, cap, "%s", identity); found = 1; }
    }
    return found;
}

/* SourceFrontier may prove an exact census source -> canonical publisher QID,
 * but CMS remains the only owner allowed to write internet_source. This emits
 * stale-safe CMS update rows only; ambiguous identity remains unresolved.
 *
 * Evidence, strongest first, and nothing past it:
 *   census_root               the census channel's own root or channel identity
 *   official_website          the source's host is a host an encyclopedia
 *                             records as an official website of one QID
 *   official_website_parent   one label below such a host (news.nd.edu under
 *                             nd.edu), never under a shared publishing host
 *   retained_items_publisher  every retained item of the source names one
 *                             publisher; a name only, never a QID
 * A root name that is itself a QID is a placeholder, not a publisher's name. */
static int cmd_identity_backfill(sqlite3 *db, const char *cms_path, const char *items_path,
                                 const char *out_path, IdentityBackfillStats *stats) {
    sqlite3 *cms = NULL, *items = NULL;
    sqlite3_stmt *sources = NULL, *identity = NULL, *by_root = NULL, *item_publishers = NULL;
    FILE *out = NULL;
    char tmp_path[PATH_MAX];
    int ok = 0;

    memset(stats, 0, sizeof(*stats));
    stats->requested = 1;
    if (!out_path || !*out_path ||
        snprintf(tmp_path, sizeof(tmp_path), "%s.tmp.%ld", out_path, (long)getpid()) >= (int)sizeof(tmp_path)) {
        fprintf(stderr, "identity backfill: invalid output path\n");
        return 0;
    }
    cms = bf_sqlite_open_readonly_path(cms_path, 1000);
    if (!cms) {
        fprintf(stderr, "identity backfill: cannot open CMS %s\n", cms_path ? cms_path : "(missing)");
        return 0;
    }
    if (items_path && *items_path) {
        items = bf_sqlite_open_readonly_path(items_path, 1000);
        if (!items) {
            fprintf(stderr, "identity backfill: cannot open item archive %s\n", items_path);
            goto cleanup;
        }
        if (sqlite3_prepare_v2(items,
                "SELECT DISTINCT publisher FROM item WHERE source=?1 AND COALESCE(publisher,'')<>'' LIMIT 2",
                -1, &item_publishers, NULL) != SQLITE_OK) {
            fprintf(stderr, "identity backfill item query: %s\n", sqlite3_errmsg(items));
            goto cleanup;
        }
    }
    if (sqlite3_prepare_v2(cms,
            "SELECT id, source, COALESCE(publisher_ident,''), COALESCE(publisher_name,''), namespace, COALESCE(url,'') "
            "FROM internet_source WHERE status='published' "
            "AND (COALESCE(publisher_ident,'')='' OR COALESCE(publisher_name,'')='') ORDER BY id",
            -1, &sources, NULL) != SQLITE_OK) {
        fprintf(stderr, "identity backfill CMS query: %s\n", sqlite3_errmsg(cms));
        goto cleanup;
    }
    if (sqlite3_prepare_v2(db,
            "SELECT DISTINCT COALESCE(NULLIF(c.identity,''),NULLIF(r.identity,''),''), COALESCE(r.name,'') "
            "FROM channel_census cc JOIN channel c ON c.channel_id=cc.channel_id "
            "JOIN root r ON r.root_id=c.root_id WHERE cc.census_source=?1 "
            "AND COALESCE(NULLIF(c.identity,''),NULLIF(r.identity,''),'')<>'' ORDER BY 1,2",
            -1, &identity, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, "SELECT COALESCE(identity,'') FROM root WHERE root_id=?1 AND host=?2",
            -1, &by_root, NULL) != SQLITE_OK) {
        fprintf(stderr, "identity backfill frontier query: %s\n", sqlite3_errmsg(db));
        goto cleanup;
    }
    out = fopen(tmp_path, "w");
    if (!out) {
        fprintf(stderr, "identity backfill: cannot write %s\n", tmp_path);
        goto cleanup;
    }

    while (sqlite3_step(sources) == SQLITE_ROW) {
        int id = sqlite3_column_int(sources, 0), ambiguous = 0, names_conflict = 0;
        const char *source = (const char *)sqlite3_column_text(sources, 1);
        const char *existing_qid = (const char *)sqlite3_column_text(sources, 2);
        const char *existing_name = (const char *)sqlite3_column_text(sources, 3);
        const char *cms_namespace = (const char *)sqlite3_column_text(sources, 4);
        const char *url = (const char *)sqlite3_column_text(sources, 5);
        const char *evidence = "census_root", *name_evidence = "";
        char qid[64] = "", publisher[512] = "";
        stats->considered++;

        sqlite3_reset(identity);
        sqlite3_clear_bindings(identity);
        sqlite3_bind_text(identity, 1, source ? source : "", -1, SQLITE_TRANSIENT);
        while (sqlite3_step(identity) == SQLITE_ROW) {
            const char *candidate_qid = (const char *)sqlite3_column_text(identity, 0);
            const char *candidate_name = (const char *)sqlite3_column_text(identity, 1);
            if (!valid_qid(candidate_qid)) continue;
            if (!qid[0]) snprintf(qid, sizeof(qid), "%s", candidate_qid);
            else if (strcmp(qid, candidate_qid)) { ambiguous = 1; break; }
            if (candidate_name && *candidate_name && !valid_qid(candidate_name)) {
                if (!publisher[0]) snprintf(publisher, sizeof(publisher), "%s", candidate_name);
                else if (strcmp(publisher, candidate_name)) names_conflict = 1;
            }
        }
        if (ambiguous || names_conflict) { stats->ambiguous++; continue; }

        if (!qid[0] && url && *url) {
            char address[SF_ADDRESS_MAX], host[256];
            if (normalise_address(url, address, sizeof(address), host, sizeof(host))) {
                const char *parent = strchr(host, '.');
                if (shared_publishing_host(host)) {
                    stats->refused_platform++;
                } else if (root_identity_for_host(by_root, host, qid, sizeof(qid)) > 0) {
                    evidence = "official_website";
                } else if (parent && strchr(parent + 1, '.') &&
                           root_identity_for_host(by_root, parent + 1, qid, sizeof(qid)) > 0) {
                    evidence = "official_website_parent";
                }
            }
        }

        if (!publisher[0] && !(existing_name && *existing_name) && item_publishers) {
            int distinct = 0;
            sqlite3_reset(item_publishers);
            sqlite3_clear_bindings(item_publishers);
            sqlite3_bind_text(item_publishers, 1, source ? source : "", -1, SQLITE_TRANSIENT);
            while (sqlite3_step(item_publishers) == SQLITE_ROW) {
                const char *name = (const char *)sqlite3_column_text(item_publishers, 0);
                if (++distinct == 1 && name) snprintf(publisher, sizeof(publisher), "%s", name);
            }
            if (distinct == 1 && host_shaped_name(publisher)) { stats->host_shaped_names++; distinct = 0; }
            if (distinct != 1) publisher[0] = '\0';
            else name_evidence = "retained_items_publisher";
        }

        if (!qid[0]) {
            /* No canonical identity: an observed name may still be filled, but
             * the source stays an identity gap until a QID is evidenced. */
            if (publisher[0] && !(existing_name && *existing_name)) {
                fprintf(out, "{\"id\":%d,\"namespace\":", id);
                json_fstr(out, cms_namespace && *cms_namespace ? cms_namespace : "root");
                fputs(",\"attributes\":{\"publisher_name\":", out);
                json_fstr(out, publisher);
                fputs("},\"if_field\":\"publisher_name\",\"if_value\":\"\",\"evidence\":", out);
                json_fstr(out, name_evidence);
                fputs("}\n", out);
                stats->name_only++;
                stats->names_from_items++;
            }
            stats->unresolved++;
            continue;
        }

        if (existing_qid && *existing_qid) {
            if (!valid_qid(existing_qid) || strcmp(existing_qid, qid)) {
                stats->conflicts++;
                continue;
            }
            if (existing_name && *existing_name) continue;
            if (!publisher[0]) { stats->unresolved++; continue; }
            fprintf(out, "{\"id\":%d,\"namespace\":", id);
            json_fstr(out, cms_namespace && *cms_namespace ? cms_namespace : "root");
            fputs(",\"attributes\":{\"publisher_name\":", out);
            json_fstr(out, publisher);
            fputs("},\"if_field\":\"publisher_name\",\"if_value\":\"\",\"evidence\":", out);
            json_fstr(out, *name_evidence ? name_evidence : evidence);
            fputs("}\n", out);
            if (*name_evidence) stats->names_from_items++;
            stats->candidates++;
            continue;
        }

        fprintf(out, "{\"id\":%d,\"namespace\":", id);
        json_fstr(out, cms_namespace && *cms_namespace ? cms_namespace : "root");
        fputs(",\"attributes\":{\"publisher_ident\":", out);
        json_fstr(out, qid);
        if ((existing_name && *existing_name) || !publisher[0]) {
            /* A canonical identity with no evidenced name is still written;
             * the source stays an identity gap until it is also named. */
            if (!(existing_name && *existing_name)) stats->ident_only++;
            fputs("},\"if_field\":\"publisher_ident\",\"if_value\":\"\",\"evidence\":", out);
            json_fstr(out, evidence);
            fputs("}\n", out);
        } else {
            fputs(",\"publisher_name\":", out);
            json_fstr(out, publisher);
            fputs("},\"if_field\":\"publisher_ident\",\"if_value\":\"\","
                  "\"if_field2\":\"publisher_name\",\"if_value2\":\"\",\"evidence\":", out);
            json_fstr(out, evidence);
            fputs("}\n", out);
            if (*name_evidence) stats->names_from_items++;
        }
        if (!strcmp(evidence, "official_website")) stats->by_official_website++;
        else if (!strcmp(evidence, "official_website_parent")) stats->by_official_website_parent++;
        else stats->by_census_root++;
        stats->candidates++;
    }

    if (ferror(out) || fflush(out) != 0 || fclose(out) != 0) {
        out = NULL;
        fprintf(stderr, "identity backfill: failed writing %s\n", tmp_path);
        goto cleanup;
    }
    out = NULL;
    if (rename(tmp_path, out_path) != 0) {
        perror("identity backfill rename");
        goto cleanup;
    }
    ok = 1;

cleanup:
    if (out) fclose(out);
    if (!ok) unlink(tmp_path);
    sqlite3_finalize(item_publishers);
    sqlite3_finalize(by_root);
    sqlite3_finalize(identity);
    sqlite3_finalize(sources);
    if (items) sqlite3_close(items);
    if (cms) sqlite3_close(cms);
    return ok;
}

/* ------------------------------------------------------------ census link
 *
 * The census is what the pages actually read.  A channel whose address is a
 * published census row is admitted; a census row nothing listed becomes its
 * own channel, so the ledger never claims fewer sources than the site reads.
 */
static int cmd_census_link(sqlite3 *db, const char *cms_path, const char *identity_backfill_out,
                           const char *items_path) {
    sqlite3 *cms = NULL; sqlite3_stmt *st = NULL, *up = NULL, *reader = NULL; char stamp[32]; int admitted = 0, added = 0, missing = 0;
    IdentityBackfillStats identity_stats = {0};
    now_iso(stamp);
    cms = bf_sqlite_open_readonly_path(cms_path, 1000);
    if (!cms) { fprintf(stderr, "open %s failed\n", cms_path ? cms_path : "(missing)"); return 0; }
    {
        /* An older census may not carry every column this ledger can use; the
         * link reads what is there rather than refusing the whole estate. */
        sqlite3_stmt *cols = NULL; char sql[512];
        int has_credential = 0, has_publisher = 0;
        if (sqlite3_prepare_v2(cms, "PRAGMA table_info(internet_source)", -1, &cols, NULL) == SQLITE_OK) {
            while (sqlite3_step(cols) == SQLITE_ROW) {
                const char *name = (const char *)sqlite3_column_text(cols, 1);
                if (name && !strcmp(name, "credential")) has_credential = 1;
                if (name && !strcmp(name, "publisher_ident")) has_publisher = 1;
            }
        }
        sqlite3_finalize(cols);
        snprintf(sql, sizeof(sql), "SELECT source, COALESCE(url,''), COALESCE(family,''), COALESCE(label,''), %s, %s, COALESCE(kind,'')"
                 " FROM internet_source WHERE status='published'",
                 has_credential ? "COALESCE(credential,'')" : "''", has_publisher ? "COALESCE(publisher_ident,'')" : "''");
        if (sqlite3_prepare_v2(cms, sql, -1, &st, NULL) != SQLITE_OK) {
            fprintf(stderr, "census: %s\n", sqlite3_errmsg(cms)); sqlite3_close(cms); return 0;
        }
    }
    sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO channel_census(channel_id,census_source,material) VALUES(?1,?2,?3)", -1, &reader, NULL);
    sqlite3_prepare_v2(db, "UPDATE channel SET state=CASE WHEN ?1<>'' THEN 'waiting_credential' ELSE 'admitted' END, census_source=CASE WHEN census_source='' THEN ?2 ELSE census_source END,"
                           " material=CASE WHEN ?3<>'' THEN ?3 ELSE material END, title=CASE WHEN ?4<>'' THEN ?4 ELSE title END,"
                           " identity=CASE WHEN ?5<>'' THEN ?5 ELSE identity END, credential=?1, last_seen=?6 WHERE channel_id=?7", -1, &up, NULL);
    exec_sql(db, "BEGIN");
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *source = (const char *)sqlite3_column_text(st, 0);
        const char *url = (const char *)sqlite3_column_text(st, 1);
        const char *family = (const char *)sqlite3_column_text(st, 2);
        const char *label = (const char *)sqlite3_column_text(st, 3);
        const char *credential = (const char *)sqlite3_column_text(st, 4);
        const char *ident = (const char *)sqlite3_column_text(st, 5);
        const char *kind = (const char *)sqlite3_column_text(st, 6);
        char address[SF_ADDRESS_MAX], host[256], channel_id[SF_ID_LEN + 1];
        sqlite3_stmt *seen = NULL; int exists = 0;
        if (!url || !normalise_address(url, address, sizeof(address), host, sizeof(host))) { missing++; continue; }
        digest_id(address, channel_id);
        sqlite3_prepare_v2(db, "SELECT 1 FROM channel WHERE channel_id=?1", -1, &seen, NULL);
        sqlite3_bind_text(seen, 1, channel_id, -1, SQLITE_TRANSIENT);
        exists = sqlite3_step(seen) == SQLITE_ROW;
        sqlite3_finalize(seen);
        if (!exists) {
            /* A census row the inventories never listed is still a channel. */
            char row[SF_ADDRESS_MAX + 1024];
            char err[160]; bf_json_doc_t *doc;
            Import im = {0};
            snprintf(row, sizeof(row), "{\"address\":\"%s\",\"protocol\":\"%s\",\"material\":\"%s\",\"state\":\"admitted\",\"census_source\":\"%s\"}",
                     address, kind && *kind ? kind : "unknown", family, source);
            doc = bf_json_parse(row, strlen(row), err, sizeof(err));
            if (doc && bf_json_root(doc)) {
                im.db = db; im.label = "CENSUS"; im.stamp = stamp;
                sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO root(root_id,host,first_seen,last_seen) VALUES(?1,?2,?3,?3)", -1, &im.root_ins, NULL);
                sqlite3_prepare_v2(db, "UPDATE root SET name=CASE WHEN ?1<>'' THEN ?1 ELSE name END, identity=CASE WHEN ?2<>'' THEN ?2 ELSE identity END,"
                                       " kind=CASE WHEN ?3<>'' THEN ?3 ELSE kind END, robots=CASE WHEN ?4<>'' THEN ?4 ELSE robots END, last_seen=?5 WHERE root_id=?6", -1, &im.root_touch, NULL);
                sqlite3_prepare_v2(db, "INSERT INTO root_origin(root_id,label,listings) VALUES(?1,?2,1) ON CONFLICT(root_id,label) DO UPDATE SET listings=listings+1", -1, &im.origin_ins, NULL);
                sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO family(family_id,root_id,protocol,discovered_from,first_seen) VALUES(?1,?2,?3,?4,?5)", -1, &im.family_ins, NULL);
                sqlite3_prepare_v2(db, "SELECT state FROM channel WHERE channel_id=?1", -1, &im.chan_sel, NULL);
                sqlite3_prepare_v2(db, "INSERT INTO channel(channel_id,family_id,root_id,address,exact_address,title,material,protocol,state,truth_level,refusal,"
                                       "virtual,census_source,credential,rights,cadence,discovered_from,body_sha256,identity,first_seen,last_seen)"
                                       " VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?20)", -1, &im.chan_ins, NULL);
                sqlite3_prepare_v2(db, "UPDATE channel SET state=?1 WHERE channel_id=?9 AND ?2 IS NOT NULL AND ?3 IS NOT NULL AND ?4 IS NOT NULL"
                                       " AND ?5 IS NOT NULL AND ?6 IS NOT NULL AND ?7 IS NOT NULL AND ?8 IS NOT NULL", -1, &im.chan_up, NULL);
                sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO channel_origin(channel_id,label) VALUES(?1,?2)", -1, &im.chan_origin, NULL);
                import_row(&im, doc, bf_json_root(doc));
                sqlite3_finalize(im.root_ins); sqlite3_finalize(im.root_touch); sqlite3_finalize(im.origin_ins);
                sqlite3_finalize(im.family_ins); sqlite3_finalize(im.chan_sel); sqlite3_finalize(im.chan_ins);
                sqlite3_finalize(im.chan_up); sqlite3_finalize(im.chan_origin);
                added++;
            }
            if (doc) bf_json_free(doc);
        }
        sqlite3_reset(up);
        sqlite3_bind_text(up, 1, credential, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(up, 2, source, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(up, 3, family, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(up, 4, label, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(up, 5, ident, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(up, 6, stamp, -1, SQLITE_STATIC);
        sqlite3_bind_text(up, 7, channel_id, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(up) == SQLITE_DONE) admitted++;
        sqlite3_reset(reader);
        sqlite3_bind_text(reader, 1, channel_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(reader, 2, source, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(reader, 3, family, -1, SQLITE_TRANSIENT);
        sqlite3_step(reader);
    }
    exec_sql(db, "COMMIT");
    sqlite3_finalize(up); sqlite3_finalize(reader); sqlite3_finalize(st); sqlite3_close(cms);
    if (identity_backfill_out && *identity_backfill_out &&
        !cmd_identity_backfill(db, cms_path, items_path, identity_backfill_out, &identity_stats))
        return 0;
    printf("{\"admitted\":%d,\"added_from_census\":%d,\"unusable_urls\":%d,\"readers\":%d,"
           "\"identity_backfill\":{\"requested\":%s,\"considered\":%d,\"candidates\":%d,"
           "\"ambiguous\":%d,\"conflicts\":%d,\"unresolved\":%d,\"refused_platform\":%d,"
           "\"by_evidence\":{\"census_root\":%d,\"official_website\":%d,\"official_website_parent\":%d},"
           "\"names_from_items\":%d,\"name_only\":%d,\"ident_only\":%d,\"host_shaped_names_refused\":%d}}\n",
           admitted, added, missing, count_of(db, "SELECT COUNT(*) FROM channel_census"),
           identity_stats.requested ? "true" : "false", identity_stats.considered, identity_stats.candidates,
           identity_stats.ambiguous, identity_stats.conflicts, identity_stats.unresolved, identity_stats.refused_platform,
           identity_stats.by_census_root, identity_stats.by_official_website, identity_stats.by_official_website_parent,
           identity_stats.names_from_items, identity_stats.name_only,
           identity_stats.ident_only, identity_stats.host_shaped_names);
    return 1;
}

/* ------------------------------------------------------------ contributions
 *
 * What a page actually showed, per day, by census source.  A channel that no
 * page ever showed is the estate's own dogfood failure, and this is the only
 * place that can say so.
 */
static int cmd_contributions(sqlite3 *db, const char *path, int dry_run) {
    FILE *f = strcmp(path, "-") ? fopen(path, "r") : stdin;
    sqlite3_stmt *ins = NULL, *up = NULL, *prior = NULL; char *line = NULL; size_t cap = 0; ssize_t len;
    int rows = 0, unknown = 0, ambiguous = 0, invalid = 0, ok = 1, ended;
    if (!f) { perror(path); return 0; }
    sqlite3_prepare_v2(db, "INSERT INTO contribution(route,channel_id,day,items) VALUES(?1,?2,?3,?4)"
                           " ON CONFLICT(route,channel_id,day) DO UPDATE SET items=MAX(items,?4)", -1, &ins, NULL);
    sqlite3_prepare_v2(db, "SELECT items FROM contribution WHERE route=?1 AND channel_id=?2 AND day=?3", -1, &prior, NULL);
    sqlite3_prepare_v2(db, "UPDATE channel SET contributions=contributions+?1, last_contributed=MAX(last_contributed,?2),"
                           " state=CASE WHEN state IN ('admitted','active') THEN 'active' ELSE state END WHERE channel_id=?3", -1, &up, NULL);
    if (!ins || !prior || !up || !exec_sql(db, "BEGIN")) {
        sqlite3_finalize(ins); sqlite3_finalize(prior); sqlite3_finalize(up);
        if (f != stdin) fclose(f);
        return 0; /* preview must never continue after a failed transaction */
    }
    while ((len = getline(&line, &cap, f)) > 0) {
        char err[160], route[256], source[128], day[16], channel_id[SF_ID_LEN + 1] = "";
        bf_json_doc_t *doc; const bf_json_node_t *root, *items_node;
        int items = 0, previous = 0, delta = 0;
        sqlite3_stmt *find = NULL;
        if (len < 3) continue;
        doc = bf_json_parse(line, (size_t)len, err, sizeof(err));
        root = doc ? bf_json_root(doc) : NULL;
        if (!root || !jstr(doc, root, "route", route, sizeof(route)) || !jstr(doc, root, "source", source, sizeof(source))) { if (doc) bf_json_free(doc); continue; }
        if (!jstr(doc, root, "day", day, sizeof(day))) { char stamp[32]; now_iso(stamp); memcpy(day, stamp, 10); day[10] = '\0'; }
        items_node = bf_json_obj_get(doc, root, "items");
        if (!items_node || items_node->type != BF_JSON_INT ||
            bf_json_get_int(items_node) <= 0 || bf_json_get_int(items_node) > 1000000) {
            invalid++; bf_json_free(doc); continue; /* only positive bounded whole item counts */
        }
        items = (int)bf_json_get_int(items_node);
        bf_json_free(doc);
        /* A census name may appear on several channels. Never choose one
         * arbitrarily or count a candidate that has not been admitted. */
        if (sqlite3_prepare_v2(db,
            "SELECT c.channel_id FROM channel_census cc JOIN channel c ON c.channel_id=cc.channel_id"
            " WHERE cc.census_source=?1 AND c.state IN ('admitted','active')"
            " UNION SELECT channel_id FROM channel WHERE census_source=?1"
            " AND state IN ('admitted','active') LIMIT 2", -1, &find, NULL) != SQLITE_OK) {
            ok = 0; break; /* SQL failure aborts the whole batch */
        }
        sqlite3_bind_text(find, 1, source, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(find) == SQLITE_ROW)
            snprintf(channel_id, sizeof(channel_id), "%s", (const char *)sqlite3_column_text(find, 0));
        if (channel_id[0] && sqlite3_step(find) == SQLITE_ROW) {
            ambiguous++; sqlite3_finalize(find); continue;
        }
        sqlite3_finalize(find);
        if (!channel_id[0]) { unknown++; continue; }
        /* Identical receipts must not manufacture a second contribution. */
        sqlite3_reset(prior);
        sqlite3_clear_bindings(prior);
        sqlite3_bind_text(prior, 1, route, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(prior, 2, channel_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(prior, 3, day, -1, SQLITE_TRANSIENT);
        {
            int prior_state = sqlite3_step(prior);
            if (prior_state == SQLITE_ROW) previous = sqlite3_column_int(prior, 0);
            else if (prior_state != SQLITE_DONE) { ok = 0; break; }
        }
        sqlite3_reset(prior);
        delta = items > previous ? items - previous : 0;
        sqlite3_reset(ins);
        sqlite3_bind_text(ins, 1, route, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 2, channel_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 3, day, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(ins, 4, items);
        if (sqlite3_step(ins) != SQLITE_DONE) { ok = 0; break; }
        sqlite3_reset(up);
        sqlite3_bind_int(up, 1, delta);
        sqlite3_bind_text(up, 2, day, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(up, 3, channel_id, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(up) != SQLITE_DONE) { ok = 0; break; }
        rows++;
    }
    if (ferror(f)) ok = 0;
    ended = exec_sql(db, (dry_run || !ok) ? "ROLLBACK" : "COMMIT");
    sqlite3_finalize(ins); sqlite3_finalize(up); sqlite3_finalize(prior); free(line);
    if (f != stdin) fclose(f);
    printf("{\"receipts\":%d,\"unknown_sources\":%d,\"ambiguous_sources\":%d,\"invalid_receipts\":%d,\"dry_run\":%s}\n", rows, unknown, ambiguous, invalid, dry_run ? "true" : "false");
    return ok && ended;
}

/* ------------------------------------------------------------ report */

static int count_of(sqlite3 *db, const char *sql) {
    sqlite3_stmt *st = NULL; int n = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) n = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    return n;
}

static void print_group(sqlite3 *db, const char *sql, const char *name) {
    sqlite3_stmt *st = NULL; int first = 1;
    printf(",\"%s\":{", name);
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *key = (const char *)sqlite3_column_text(st, 0);
            printf("%s", first ? "" : ","); first = 0;
            json_str(key && *key ? key : "unknown");
            printf(":%d", sqlite3_column_int(st, 1));
        }
    }
    sqlite3_finalize(st);
    putchar('}');
}

static int cmd_report(sqlite3 *db) {
    sqlite3_stmt *st = NULL; int first = 1;
    printf("{\"schema\":\"bonfyre-source-frontier-report.v1\",\"inventories\":[");
    if (sqlite3_prepare_v2(db, "SELECT i.label, i.rows, i.refused,"
                               " (SELECT COUNT(*) FROM root_origin o WHERE o.label=i.label),"
                               " (SELECT COUNT(*) FROM channel_origin c WHERE c.label=i.label)"
                               " FROM inventory i ORDER BY i.label", -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            printf("%s{\"label\":", first ? "" : ","); first = 0;
            json_str((const char *)sqlite3_column_text(st, 0));
            printf(",\"listed_rows\":%d,\"refused\":%d,\"roots\":%d,\"channels\":%d}",
                   sqlite3_column_int(st, 1), sqlite3_column_int(st, 2), sqlite3_column_int(st, 3), sqlite3_column_int(st, 4));
        }
    }
    sqlite3_finalize(st);
    printf("],\"roots\":%d,\"families\":%d", count_of(db, "SELECT COUNT(*) FROM root"), count_of(db, "SELECT COUNT(*) FROM family"));
    printf(",\"channels\":{\"exact\":%d,\"virtual\":%d,\"shadow\":%d,\"candidate\":%d,\"eligible\":%d,\"waiting_credential\":%d,\"admitted\":%d,\"active\":%d,\"refused\":%d}",
           count_of(db, "SELECT COUNT(*) FROM channel WHERE virtual=0"),
           count_of(db, "SELECT COUNT(*) FROM channel WHERE virtual=1"),
           count_of(db, "SELECT COUNT(*) FROM channel WHERE state='shadow'"),
           count_of(db, "SELECT COUNT(*) FROM channel WHERE state='candidate'"),
           count_of(db, "SELECT COUNT(*) FROM channel WHERE state='eligible'"),
           count_of(db, "SELECT COUNT(*) FROM channel WHERE state='waiting_credential'"),
           count_of(db, "SELECT COUNT(*) FROM channel WHERE state='admitted'"),
           count_of(db, "SELECT COUNT(*) FROM channel WHERE state='active'"),
           count_of(db, "SELECT COUNT(*) FROM channel WHERE refusal<>'' AND state NOT IN ('admitted','active')"));
    printf(",\"roots_listed_by_more_than_one_inventory\":%d",
           count_of(db, "SELECT COUNT(*) FROM (SELECT root_id FROM root_origin GROUP BY root_id HAVING COUNT(DISTINCT label)>1)"));
    printf(",\"channels_listed_by_more_than_one_inventory\":%d",
           count_of(db, "SELECT COUNT(*) FROM (SELECT channel_id FROM channel_origin GROUP BY channel_id HAVING COUNT(DISTINCT label)>1)"));
    print_group(db, "SELECT protocol, COUNT(*) FROM channel GROUP BY protocol ORDER BY COUNT(*) DESC LIMIT 40", "protocols");
    print_group(db, "SELECT material, COUNT(*) FROM channel GROUP BY material ORDER BY COUNT(*) DESC LIMIT 40", "materials");
    print_group(db, "SELECT kind, COUNT(*) FROM root GROUP BY kind ORDER BY COUNT(*) DESC LIMIT 20", "root_kinds");
    printf(",\"contributing_channels\":%d,\"contributing_roots\":%d,\"open_gaps\":%d",
           count_of(db, "SELECT COUNT(DISTINCT channel_id) FROM contribution"),
           count_of(db, "SELECT COUNT(DISTINCT root_id) FROM channel WHERE contributions>0"),
           count_of(db, "SELECT COUNT(*) FROM gap WHERE status='open'"));
    printf(",\"cannot_infer\":\"a listed channel is not a lawful, callable or available source, and an admitted channel is not one a person saw\"}\n");
    return 1;
}

/* ------------------------------------------------------------ gaps
 *
 * A gap is something the estate does not know about a root, with the lawful
 * classes of closure available for it.  A class is a candidate route, never a
 * decision, and never an instruction to contact anyone.
 */
struct GapRule { const char *kind; const char *sql; const char *resolutions; };

static const struct GapRule GAP_RULES[] = {
    {"OBSERVATION",
     "SELECT r.root_id FROM root r WHERE EXISTS(SELECT 1 FROM channel c WHERE c.root_id=r.root_id)"
     " AND NOT EXISTS(SELECT 1 FROM channel c WHERE c.root_id=r.root_id AND c.state IN ('admitted','active'))",
     "DISCOVER,PUBLIC_BACKFILL,OUTREACH"},
    {"STRUCTURED_CHANNEL",
     "SELECT r.root_id FROM root r WHERE EXISTS(SELECT 1 FROM channel c WHERE c.root_id=r.root_id AND c.state IN ('admitted','active'))"
     " AND NOT EXISTS(SELECT 1 FROM channel c WHERE c.root_id=r.root_id AND c.protocol IN ('rss','atom','json_feed','ics','openapi','geojson','socrata','ckan','arcgis','gtfs','gbfs','stac','oai_pmh','sparql','mcp','a2a'))",
     "DISCOVER,OUTREACH,PARTNER"},
    {"RIGHTS",
     "SELECT root_id FROM root WHERE robots NOT IN ('allow','refuse')",
     "HUMAN_VERIFY,DISCOVER"},
    {"IDENTITY",
     "SELECT r.root_id FROM root r WHERE COALESCE(r.identity,'')='' AND EXISTS(SELECT 1 FROM channel c WHERE c.root_id=r.root_id AND c.state IN ('admitted','active'))",
     "PUBLIC_BACKFILL,DISCOVER,HUMAN_VERIFY"},
    {"CREDENTIAL",
     "SELECT DISTINCT root_id FROM channel WHERE state='waiting_credential' OR (credential<>'' AND state NOT IN ('admitted','active'))",
     "CREDENTIAL,PARTNER,PURCHASE"},
    {"ECONOMIC_INTERFACE",
     "SELECT DISTINCT c.root_id FROM channel c JOIN root r ON r.root_id=c.root_id WHERE COALESCE(r.economic,'')=''"
     " AND c.material IN ('event','offer','capacity','tickets','booking') AND c.state IN ('admitted','active')",
     "AFFILIATE,PARTNER,OUTREACH"},
};

static int cmd_gaps(sqlite3 *db, int emit) {
    sqlite3_stmt *ins = NULL; char stamp[32]; size_t k; int written = 0;
    now_iso(stamp);
    sqlite3_prepare_v2(db, "INSERT INTO gap(gap_id,root_id,kind,state,resolutions,dependents,measured,status,first_seen,last_seen)"
                           " VALUES(?1,?2,?3,'UNKNOWN',?4,?5,?6,'open',?7,?7)"
                           " ON CONFLICT(gap_id) DO UPDATE SET dependents=?5, measured=?6, last_seen=?7, status='open'", -1, &ins, NULL);
    exec_sql(db, "BEGIN");
    for (k = 0; k < sizeof(GAP_RULES) / sizeof(GAP_RULES[0]); k++) {
        sqlite3_stmt *sel = NULL;
        if (sqlite3_prepare_v2(db, GAP_RULES[k].sql, -1, &sel, NULL) != SQLITE_OK) { fprintf(stderr, "gap %s: %s\n", GAP_RULES[k].kind, sqlite3_errmsg(db)); continue; }
        while (sqlite3_step(sel) == SQLITE_ROW) {
            const char *root_id = (const char *)sqlite3_column_text(sel, 0);
            char key[128], gap_id[SF_ID_LEN + 1], measured[256];
            int dependents = 0, contributions = 0;
            sqlite3_stmt *dep = NULL;
            snprintf(key, sizeof(key), "%s|%s", root_id, GAP_RULES[k].kind);
            digest_id(key, gap_id);
            if (sqlite3_prepare_v2(db, "SELECT COUNT(*), COALESCE(SUM(contributions),0) FROM channel WHERE root_id=?1", -1, &dep, NULL) == SQLITE_OK) {
                sqlite3_bind_text(dep, 1, root_id, -1, SQLITE_TRANSIENT);
                if (sqlite3_step(dep) == SQLITE_ROW) { dependents = sqlite3_column_int(dep, 0); contributions = sqlite3_column_int(dep, 1); }
            }
            sqlite3_finalize(dep);
            /* Nothing is valued that was not measured: a root no page has shown
             * yet has no scene reuse, and the plan says so rather than guessing. */
            if (contributions > 0)
                snprintf(measured, sizeof(measured), "{\"channels\":%d,\"contributions\":%d}", dependents, contributions);
            else
                snprintf(measured, sizeof(measured), "{\"channels\":%d,\"contributions\":\"UNMEASURED\"}", dependents);
            sqlite3_reset(ins);
            sqlite3_bind_text(ins, 1, gap_id, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 2, root_id, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 3, GAP_RULES[k].kind, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 4, GAP_RULES[k].resolutions, -1, SQLITE_STATIC);
            sqlite3_bind_int(ins, 5, dependents);
            sqlite3_bind_text(ins, 6, measured, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 7, stamp, -1, SQLITE_STATIC);
            if (sqlite3_step(ins) == SQLITE_DONE) written++;
        }
        sqlite3_finalize(sel);
    }
    /* A gap whose condition no longer holds is closed, not deleted. */
    exec_sql(db, "UPDATE gap SET status='closed' WHERE last_seen<(SELECT MAX(last_seen) FROM gap) AND status='open'");
    exec_sql(db, "COMMIT");
    sqlite3_finalize(ins);
    if (emit) {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(db, "SELECT g.kind, r.host, COALESCE(r.name,''), g.resolutions, g.dependents, g.measured FROM gap g"
                                   " JOIN root r ON r.root_id=g.root_id WHERE g.status='open' ORDER BY g.dependents DESC, r.host", -1, &st, NULL) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW) {
                printf("{\"kind\":"); json_str((const char *)sqlite3_column_text(st, 0));
                printf(",\"host\":"); json_str((const char *)sqlite3_column_text(st, 1));
                printf(",\"name\":"); json_str((const char *)sqlite3_column_text(st, 2));
                printf(",\"resolutions\":"); json_str((const char *)sqlite3_column_text(st, 3));
                printf(",\"channels\":%d,\"measured\":%s}\n", sqlite3_column_int(st, 4), sqlite3_column_text(st, 5));
            }
        }
        sqlite3_finalize(st);
    } else {
        printf("{\"gaps\":%d", written);
        print_group(db, "SELECT kind, COUNT(*) FROM gap WHERE status='open' GROUP BY kind ORDER BY COUNT(*) DESC", "by_kind");
        printf(",\"cannot_infer\":\"a resolution class is a lawful route, not a decision to take it or permission to contact anyone\"}\n");
    }
    return 1;
}

/* ------------------------------------------------------------ forward
 *
 * A gap is not worth closing because it is a gap.  It is worth closing when
 * closing it would remove branches this estate is otherwise forced to carry,
 * reach surfaces people actually see, or make an action possible.  Each of
 * those is counted from the ledger, or it is UNMEASURED -- there is no score
 * made out of nothing.
 *
 *   branches_before   how many candidate roots share this root's name, so an
 *                     identity gap is worth exactly the ambiguity it causes
 *   channels          how many channels at that root depend on the answer
 *   contributions     how many items from that root a page has actually shown
 *   surfaces          how many distinct routes those items reached
 *
 * The plan names lawful closure routes.  It does not decide, it does not
 * contact anyone, and it never treats an economic relationship as a reason to
 * prefer one source's facts over another's.
 */
static int cmd_forward(sqlite3 *db, int limit) {
    sqlite3_stmt *st = NULL;
    const char *sql =
        "SELECT g.kind, r.host, COALESCE(NULLIF(r.name,''), r.host), g.resolutions, g.dependents,"
        " (SELECT COALESCE(SUM(c.contributions),0) FROM channel c WHERE c.root_id=g.root_id),"
        " (SELECT COUNT(DISTINCT co.route) FROM contribution co JOIN channel c2 ON c2.channel_id=co.channel_id WHERE c2.root_id=g.root_id),"
        " (SELECT COUNT(*) FROM root r2 WHERE r2.name<>'' AND r2.name=r.name),"
        " COALESCE(r.identity,''), COALESCE(r.economic,'')"
        " FROM gap g JOIN root r ON r.root_id=g.root_id WHERE g.status='open'"
        " ORDER BY (SELECT COALESCE(SUM(c3.contributions),0) FROM channel c3 WHERE c3.root_id=g.root_id) DESC, g.dependents DESC LIMIT ?1";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) { fprintf(stderr, "forward: %s\n", sqlite3_errmsg(db)); return 0; }
    sqlite3_bind_int(st, 1, limit);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *kind = (const char *)sqlite3_column_text(st, 0);
        int channels = sqlite3_column_int(st, 4), contributions = sqlite3_column_int(st, 5);
        int surfaces = sqlite3_column_int(st, 6), sharing_a_name = sqlite3_column_int(st, 7);
        const char *identity = (const char *)sqlite3_column_text(st, 8);
        printf("{\"gap\":"); json_str(kind);
        printf(",\"host\":"); json_str((const char *)sqlite3_column_text(st, 1));
        printf(",\"name\":"); json_str((const char *)sqlite3_column_text(st, 2));
        printf(",\"resolutions\":"); json_str((const char *)sqlite3_column_text(st, 3));
        printf(",\"channels\":%d", channels);
        /* What a page has actually shown from this publisher, or nothing. */
        if (contributions > 0) printf(",\"items_shown\":%d,\"surfaces_reached\":%d", contributions, surfaces);
        else printf(",\"items_shown\":\"UNMEASURED\",\"surfaces_reached\":\"UNMEASURED\"");
        /* The branches an identity gap actually causes: how many roots this
         * estate cannot tell apart by name until someone says which is which. */
        if (!strcmp(kind, "IDENTITY") && sharing_a_name > 1)
            printf(",\"branches_before\":%d,\"branches_after\":1,\"branches_removed\":%d", sharing_a_name, sharing_a_name - 1);
        else if (!strcmp(kind, "IDENTITY"))
            printf(",\"branches_before\":\"UNMEASURED\"");
        if (identity && *identity) { printf(",\"identity\":"); json_str(identity); }
        printf(",\"cannot_infer\":\"a closure route is lawful, not chosen; nothing here contacts anyone or spends\"}\n");
    }
    sqlite3_finalize(st);
    return 1;
}

/* ------------------------------------------------------------ starvation
 *
 * The estate turned against itself: which admitted sources never reach a
 * person, which surfaces are dominated by one publisher, which discoveries
 * were never looked at again.  None of these says a source is bad.
 */
struct Starve { const char *mode; const char *sql; const char *means; };

static const struct Starve STARVE[] = {
    {"ADMITTED_NO_CONTRIBUTION",
     "SELECT c.address, r.host FROM channel c JOIN root r ON r.root_id=c.root_id WHERE c.state IN ('admitted') AND c.contributions=0 ORDER BY c.first_seen LIMIT ?1",
     "admitted and never shown on any page"},
    {"ADMITTED_NO_RECENT_CONTRIBUTION",
     "SELECT c.address, r.host FROM channel c JOIN root r ON r.root_id=c.root_id WHERE c.contributions>0 AND c.last_contributed<?2 ORDER BY c.last_contributed LIMIT ?1",
     "shown once but not inside the window"},
    {"ROOT_DOMINATES_A_SURFACE",
     "SELECT co.route || ' ' || r.host, CAST(SUM(co.items) AS TEXT) FROM contribution co JOIN channel c ON c.channel_id=co.channel_id"
     " JOIN root r ON r.root_id=c.root_id WHERE co.day>=?2 GROUP BY co.route, r.root_id"
     " HAVING SUM(co.items)*100 > 12*(SELECT SUM(items) FROM contribution c2 WHERE c2.route=co.route AND c2.day>=?2) LIMIT ?1",
     "more than an eighth of one surface came from one publisher"},
    {"PROTOCOL_DOMINATES_A_SURFACE",
     "SELECT co.route || ' ' || c.protocol, CAST(SUM(co.items) AS TEXT) FROM contribution co JOIN channel c ON c.channel_id=co.channel_id"
     " WHERE co.day>=?2 GROUP BY co.route, c.protocol"
     " HAVING SUM(co.items)*100 > 30*(SELECT SUM(items) FROM contribution c2 WHERE c2.route=co.route AND c2.day>=?2) LIMIT ?1",
     "more than a third of one surface arrived by one protocol"},
    {"SHADOW_NEVER_EXAMINED",
     "SELECT c.address, r.host FROM channel c JOIN root r ON r.root_id=c.root_id WHERE c.state='shadow' AND c.first_seen<?2 ORDER BY c.first_seen LIMIT ?1",
     "a link found inside a retained body that nobody looked at since"},
    {"ELIGIBLE_NEVER_ADMITTED",
     "SELECT c.address, r.host FROM channel c JOIN root r ON r.root_id=c.root_id WHERE c.state='eligible' ORDER BY c.first_seen LIMIT ?1",
     "lawful and shaped to read, and still not read"},
    {"ADMITTED_WITHOUT_IDENTITY",
     "SELECT c.address, r.host FROM channel c JOIN root r ON r.root_id=c.root_id WHERE c.state IN ('admitted','active')"
     " AND COALESCE(c.identity,'')='' AND COALESCE(r.identity,'')='' LIMIT ?1",
     "read, but joined to no identity, so nothing it says can meet anything else"},
};

static int cmd_starvation(sqlite3 *db, int days, int limit) {
    char since[16]; time_t t = time(NULL) - (time_t)days * 86400; struct tm tm; size_t k;
    gmtime_r(&t, &tm);
    strftime(since, sizeof(since), "%Y-%m-%d", &tm);
    printf("{\"schema\":\"bonfyre-source-starvation.v1\",\"window_days\":%d,\"since\":", days); json_str(since);
    for (k = 0; k < sizeof(STARVE) / sizeof(STARVE[0]); k++) {
        sqlite3_stmt *st = NULL; int n = 0;
        printf(",\"%s\":{\"means\":", STARVE[k].mode); json_str(STARVE[k].means); printf(",\"examples\":[");
        if (sqlite3_prepare_v2(db, STARVE[k].sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_int(st, 1, limit);
            sqlite3_bind_text(st, 2, since, -1, SQLITE_TRANSIENT);
            while (sqlite3_step(st) == SQLITE_ROW) {
                printf("%s[", n ? "," : "");
                json_str((const char *)sqlite3_column_text(st, 0)); putchar(',');
                json_str((const char *)sqlite3_column_text(st, 1)); putchar(']');
                n++;
            }
        } else fprintf(stderr, "starvation %s: %s\n", STARVE[k].mode, sqlite3_errmsg(db));
        sqlite3_finalize(st);
        printf("],\"count\":%d}", n);
    }
    printf(",\"cannot_infer\":\"a starved source is not a bad source; it is a surface, join or aperture this estate has not built yet\"}\n");
    return 1;
}

/* ------------------------------------------------------------ attention
 *
 * Demand lowered into the estate's own HOT/WARM/COOL/COLD language.  This is
 * an internal watch plan: it says what is worth reading again sooner, never
 * that anything may be fetched, bought, joined or answered.
 */
static int cmd_attention(sqlite3 *db, int days) {
    sqlite3_stmt *st = NULL, *ins = NULL; char stamp[32], hot[16], warm[16], cool[16];
    time_t now = time(NULL); struct tm tm; int counts[4] = {0, 0, 0, 0};
    now_iso(stamp);
    { time_t t = now - 1 * 86400; gmtime_r(&t, &tm); strftime(hot, sizeof(hot), "%Y-%m-%d", &tm); }
    { time_t t = now - (time_t)days * 86400; gmtime_r(&t, &tm); strftime(warm, sizeof(warm), "%Y-%m-%d", &tm); }
    { time_t t = now - (time_t)days * 4 * 86400; gmtime_r(&t, &tm); strftime(cool, sizeof(cool), "%Y-%m-%d", &tm); }
    sqlite3_prepare_v2(db, "INSERT INTO attention(root_id,level,reason,updated) VALUES(?1,?2,?3,?4)"
                           " ON CONFLICT(root_id) DO UPDATE SET level=?2, reason=?3, updated=?4", -1, &ins, NULL);
    exec_sql(db, "BEGIN");
    if (sqlite3_prepare_v2(db, "SELECT root_id, MAX(last_contributed), SUM(contributions), MAX(state IN ('admitted','active'))"
                               " FROM channel GROUP BY root_id", -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *root_id = (const char *)sqlite3_column_text(st, 0);
            const char *last = (const char *)sqlite3_column_text(st, 1);
            int admitted = sqlite3_column_int(st, 3);
            const char *level = "COLD", *reason = "nothing here has reached a person";
            if (last && *last && strcmp(last, hot) >= 0) { level = "HOT"; reason = "reached a person within a day"; counts[0]++; }
            else if (last && *last && strcmp(last, warm) >= 0) { level = "WARM"; reason = "reached a person within the window"; counts[1]++; }
            else if (admitted) { level = "COOL"; reason = "admitted, and not shown lately"; counts[2]++; }
            else counts[3]++;
            sqlite3_reset(ins);
            sqlite3_bind_text(ins, 1, root_id, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 2, level, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 3, reason, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 4, stamp, -1, SQLITE_STATIC);
            sqlite3_step(ins);
        }
    }
    sqlite3_finalize(st);
    exec_sql(db, "COMMIT");
    sqlite3_finalize(ins);
    (void)cool;
    printf("{\"HOT\":%d,\"WARM\":%d,\"COOL\":%d,\"COLD\":%d,\"window_days\":%d,"
           "\"cannot_infer\":\"an attention level is a reading plan, never permission to fetch, buy, contact or enrol\"}\n",
           counts[0], counts[1], counts[2], counts[3], days);
    return 1;
}

/* ------------------------------------------------------------ qualification work
 *
 * Project unresolved feed/calendar candidates into the existing WorkGraph
 * queue shape without moving their semantic ownership out of SourceFrontier.
 * The channel row remains authoritative; this output is only an execution
 * envelope.  WorkGraph priority deliberately combines two lessons already
 * learned by this estate: an observed publisher-declared link outranks a raw
 * listing, and sparse roots are worked before roots whose channel grammar is
 * already rich.  That prevents the old first-seen queue from burying newly
 * useful material behind a hundred thousand older candidates.
 *
 * TSV columns are the BonfyreQueue enqueue-batch projection:
 * mission, operator type, input URI, family, priority, retries, backoff_ms.
 */
static int cmd_qualification_work(sqlite3 *db, int limit) {
    sqlite3_stmt *st = NULL;
    const char *sql =
        "WITH root_counts AS (SELECT root_id,COUNT(*) AS n FROM channel GROUP BY root_id) "
        "SELECT c.channel_id,(c.truth_level='observed_link_candidate'),COALESCE(rc.n,0) "
        "FROM channel c LEFT JOIN root_counts rc ON rc.root_id=c.root_id "
        "WHERE c.state IN ('shadow','candidate') AND COALESCE(c.refusal,'')='' "
        "AND c.protocol NOT IN ('mcp','x402','graphql','openapi','soap') "
        "ORDER BY (c.truth_level='observed_link_candidate') DESC,COALESCE(rc.n,0) ASC,c.first_seen ASC "
        "LIMIT ?1";
    int emitted = 0;
    if (limit < 1) limit = 1;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        fprintf(stderr, "qualification-work: %s\n", sqlite3_errmsg(db));
        return 0;
    }
    sqlite3_bind_int(st, 1, limit);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *channel_id = (const char *)sqlite3_column_text(st, 0);
        int observed = sqlite3_column_int(st, 1);
        int root_channels = sqlite3_column_int(st, 2);
        int coverage_bonus = 5000 - (root_channels < 5000 ? root_channels : 5000);
        int priority = 10000 + coverage_bonus + (observed ? 10000 : 0);
        if (!channel_id || !*channel_id) continue;
        printf("source-qualify:%s\tsource.qualify\tbonfyre://source-frontier/channel/%s"
               "\tsource-qualify\t%d\t4\t30000\n",
               channel_id, channel_id, priority);
        emitted++;
    }
    sqlite3_finalize(st);
    fprintf(stderr, "qualification-work: projected %d unresolved channels; SourceFrontier remains owner\n", emitted);
    return 1;
}

/* ------------------------------------------------------------ channels out
 *
 * The allocator inside the page reads this: which admitted channels exist,
 * how long since each was last shown, and which publisher each belongs to.
 */
static int cmd_channels(sqlite3 *db, const char *state, const char *material, int limit) {
    sqlite3_stmt *st = NULL;
    const char *sql = "SELECT COALESCE(cc.census_source, c.census_source), c.address, COALESCE(NULLIF(cc.material,''), c.material), c.protocol, c.state,"
                      " COALESCE(c.last_contributed,''), c.contributions, r.host, COALESCE(NULLIF(c.identity,''), r.identity, '')"
                      " FROM channel c JOIN root r ON r.root_id=c.root_id LEFT JOIN channel_census cc ON cc.channel_id=c.channel_id"
                      " WHERE (?1='' OR c.state=?1) AND (?2='' OR COALESCE(NULLIF(cc.material,''), c.material)=?2)"
                      " ORDER BY c.contributions ASC, c.first_seen ASC LIMIT ?3";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) { fprintf(stderr, "channels: %s\n", sqlite3_errmsg(db)); return 0; }
    sqlite3_bind_text(st, 1, state ? state : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, material ? material : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 3, limit);
    while (sqlite3_step(st) == SQLITE_ROW) {
        printf("{\"source\":"); json_str((const char *)sqlite3_column_text(st, 0));
        printf(",\"address\":"); json_str((const char *)sqlite3_column_text(st, 1));
        printf(",\"material\":"); json_str((const char *)sqlite3_column_text(st, 2));
        printf(",\"protocol\":"); json_str((const char *)sqlite3_column_text(st, 3));
        printf(",\"state\":"); json_str((const char *)sqlite3_column_text(st, 4));
        printf(",\"last_contributed\":"); json_str((const char *)sqlite3_column_text(st, 5));
        printf(",\"contributions\":%d,\"host\":", sqlite3_column_int(st, 6));
        json_str((const char *)sqlite3_column_text(st, 7));
        printf(",\"identity\":"); json_str((const char *)sqlite3_column_text(st, 8));
        printf("}\n");
    }
    sqlite3_finalize(st);
    return 1;
}

/* ------------------------------------------------------------ main */

static void usage(void) {
    fputs("usage: bonfyre-source-frontier init --db DB\n"
          "       bonfyre-source-frontier import --db DB --label INVENTORY --jsonl FILE|-\n"
          "       bonfyre-source-frontier census-link --db DB --cms CMS.db [--identity-backfill-out FILE.jsonl [--items ITEMS.db]]\n"
          "       bonfyre-source-frontier contributions --db DB --jsonl FILE|- [--dry-run]\n"
          "       bonfyre-source-frontier report --db DB\n"
          "       bonfyre-source-frontier gaps --db DB [--emit]\n"
          "       bonfyre-source-frontier forward --db DB [--limit 40]\n"
          "       bonfyre-source-frontier starvation --db DB [--days 7] [--limit 5]\n"
          "       bonfyre-source-frontier attention --db DB [--days 7]\n"
          "       bonfyre-source-frontier qualification-work --db DB [--limit N]\n"
          "       bonfyre-source-frontier channels --db DB [--state S] [--material M] [--limit N]\n", stderr);
    fputs("       bonfyre-source-frontier classified-market refresh|replay|enrich|route [options]\n"
          "       bonfyre-source-frontier discourse-review --receipt FILE [--board | --supplement FILE ... --post ID]\n"
          "       bonfyre-source-frontier traktok preflight|enqueue|collect|run|resume|import|deliver|export|status [options]\n", stderr);
}

#define FRONTIER_COMMANDS(X) \
    X("attention", FC_ATTENTION) \
    X("census-link", FC_CENSUS_LINK) \
    X("channels", FC_CHANNELS) \
    X("classified-market", FC_CLASSIFIED_MARKET) \
    X("contributions", FC_CONTRIBUTIONS) \
    X("discourse-review", FC_DISCOURSE_REVIEW) \
    X("forward", FC_FORWARD) \
    X("gaps", FC_GAPS) \
    X("import", FC_IMPORT) \
    X("init", FC_INIT) \
    X("qualification-work", FC_QUALIFICATION_WORK) \
    X("report", FC_REPORT) \
    X("starvation", FC_STARVATION) \
    X("traktok", FC_TRAKTOK)

enum {
#define FC_ENUM(name, id) id,
    FRONTIER_COMMANDS(FC_ENUM)
#undef FC_ENUM
};
static const BfCommandEntry frontier_commands[] = {
#define FC_ENTRY(name, id) {name, id, 0, NULL},
    FRONTIER_COMMANDS(FC_ENTRY)
#undef FC_ENTRY
};

/* This source-specific protocol is a packaged projection of SourceFrontier,
 * not another service. Paths are operator configuration, never page input. */
static int projection_command(int argc,char **argv,const char *script_env,const char *script_name) {
    const char *home=getenv("HOME"),*script=getenv(script_env),*runtime=getenv("BONFYRE_BUN_BINARY");
    char default_script[4096],default_runtime[4096];
    if(argc<3||argc>1024||!home){usage();return 2;}
    if(!script||!*script){int n=snprintf(default_script,sizeof(default_script),"%s/.bonfyre/lib/source-frontier/%s",home,script_name);
        if(n<0||(size_t)n>=sizeof(default_script))return 2;script=default_script;}
    if(!runtime||!*runtime){int n=snprintf(default_runtime,sizeof(default_runtime),"%s/.bun/bin/bun",home);
        if(n<0||(size_t)n>=sizeof(default_runtime))return 2;runtime=default_runtime;}
    if(access(script,R_OK)||access(runtime,X_OK)){fputs("SourceFrontier projection or runtime is not installed\n",stderr);return 2;}
    char **args=calloc((size_t)argc+5,sizeof(char*));if(!args)return 2;
    args[0]=(char*)runtime;args[1]="--no-env-file";args[2]="--no-install";args[3]=(char*)script;
    for(int i=2;i<argc;i++)args[i+2]=argv[i];
    execv(runtime,args);perror("SourceFrontier projection");free(args);return 2;
}

int main(int argc, char **argv) {
    const BfCommandEntry *command;
    sqlite3 *db = NULL; int ok = 0;
    if (argc < 2 || !(command = bf_command_lookup(frontier_commands,
            sizeof(frontier_commands) / sizeof(frontier_commands[0]), argv[1]))) {
        usage();
        return 2;
    }
    /* Retained-source readers resolve before any ledger is opened. */
    if(command->id == FC_CLASSIFIED_MARKET)
        return projection_command(argc,argv,"BONFYRE_CLASSIFIED_MARKET","classified_market.mjs");
    if(command->id == FC_DISCOURSE_REVIEW)
        return projection_command(argc,argv,"BONFYRE_DISCOURSE_READER","discourse_thread.mjs");
    if(command->id == FC_TRAKTOK)
        return projection_command(argc,argv,"BONFYRE_TRAKTOK_COLLECTOR","traktok_collect.mjs");
    db = open_db(arg(argc, argv, "--db"), 1);
    if (!db) return 2;
    switch (command->id) {
    case FC_INIT: printf("{\"ready\":true}\n"); ok = 1; break;
    case FC_IMPORT:
        if (arg(argc, argv, "--label") && arg(argc, argv, "--jsonl"))
            ok = cmd_import(db, arg(argc, argv, "--label"), arg(argc, argv, "--jsonl"));
        break;
    case FC_CENSUS_LINK:
        if (arg(argc, argv, "--cms")) ok = cmd_census_link(db, arg(argc, argv, "--cms"),
            arg(argc, argv, "--identity-backfill-out"), arg(argc, argv, "--items"));
        break;
    case FC_CONTRIBUTIONS:
        if (arg(argc, argv, "--jsonl")) ok = cmd_contributions(db, arg(argc, argv, "--jsonl"), flag(argc, argv, "--dry-run"));
        break;
    case FC_REPORT: ok = cmd_report(db); break;
    case FC_GAPS: ok = cmd_gaps(db, flag(argc, argv, "--emit")); break;
    case FC_FORWARD:
        ok = cmd_forward(db, arg(argc, argv, "--limit") ? atoi(arg(argc, argv, "--limit")) : 40);
        break;
    case FC_STARVATION:
        ok = cmd_starvation(db, arg(argc, argv, "--days") ? atoi(arg(argc, argv, "--days")) : 7,
            arg(argc, argv, "--limit") ? atoi(arg(argc, argv, "--limit")) : 5);
        break;
    case FC_ATTENTION:
        ok = cmd_attention(db, arg(argc, argv, "--days") ? atoi(arg(argc, argv, "--days")) : 7);
        break;
    case FC_QUALIFICATION_WORK:
        ok = cmd_qualification_work(db, arg(argc, argv, "--limit") ? atoi(arg(argc, argv, "--limit")) : 5000);
        break;
    case FC_CHANNELS:
        ok = cmd_channels(db, arg(argc, argv, "--state"), arg(argc, argv, "--material"),
            arg(argc, argv, "--limit") ? atoi(arg(argc, argv, "--limit")) : 200);
        break;
    default: usage(); break;
    }
    sqlite3_close(db);
    return ok ? 0 : 2;
}
