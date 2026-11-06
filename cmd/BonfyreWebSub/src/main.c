/* BonfyreWebSub: native, receipt-bound WebSub subscription adapter.
 *
 * WebSub carries a change notification, never a foreign fact.  This organ owns
 * only the bounded carrier subscription state; WebAddress verifies callbacks
 * and ForeignTwin performs the later, separately receipted re-observation.
 */
#define _POSIX_C_SOURCE 200809L
#include <curl/curl.h>
#include <sqlite3.h>

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "bf_lex.h"
#include "bf_command_lookup.h"
#define BF_UA "BonfyreWebSub/2.0 (+https://aurekai.life/protocols)"
#define BF_MAX_URL 2048
#define BF_MAX_BODY 200000
#define BF_MAX_RECONCILE 4096

typedef struct {
    char body[BF_MAX_BODY + 1];
    size_t used;
    char link[8192];
    long status;
} HttpResponse;

typedef struct {
    char token[65];
    char topic[BF_MAX_URL];
    char hub[BF_MAX_URL];
    char secret[65];
    char authority[1024];
} SubscriptionRow;

static const char *db_path(void) {
    const char *v = getenv("BONFYRE_WEBSUB_DB");
    return (v && *v) ? v : "/var/lib/bonfyre/state/websub.db";
}

static const char *origin(void) {
    const char *v = getenv("BONFYRE_PUBLIC_ORIGIN");
    return (v && *v) ? v : "https://aurekai.life";
}

/* Delegates to the one implementation. Nineteen copies of this shape existed
 * with sixteen different opinions about escaping.
 */
static void json_string(const char *s) {
    bf_lex_json_emit(stdout, s);
}

/* FactDelta is a change notice, never a ForeignTwin assertion.  Keep a file
 * producer here because this carrier owns the successful, independently
 * receipted re-observation boundary. */
static void json_file_string(FILE *f, const char *s) {
    const unsigned char *p = (const unsigned char *)(s ? s : "");
    fputc('"', f);
    for (; *p; ++p) {
        if (*p == '"' || *p == '\\') { fputc('\\', f); fputc(*p, f); }
        else if (*p == '\n') fputs("\\n", f);
        else if (*p == '\r') fputs("\\r", f);
        else if (*p == '\t') fputs("\\t", f);
        else if (*p < 0x20) fprintf(f, "\\u%04x", *p);
        else fputc(*p, f);
    }
    fputc('"', f);
}

static int is_http_url(const char *url, int https_only) {
    if (!url || !*url || strlen(url) >= BF_MAX_URL) return 0;
    if (!strncmp(url, "https://", 8)) return 1;
    return !https_only && !strncmp(url, "http://", 7);
}

static int permit_insecure_test(void) {
    const char *v = getenv("BONFYRE_WEBSUB_ALLOW_INSECURE_TEST");
    return v && !strcmp(v, "1");
}

static const char *find_ci(const char *haystack, const char *needle) {
    size_t n = strlen(needle);
    if (!n) return haystack;
    for (; *haystack; ++haystack)
        if (!strncasecmp(haystack, needle, n)) return haystack;
    return NULL;
}

static size_t collect_body(void *ptr, size_t size, size_t nmemb, void *userdata) {
    HttpResponse *r = userdata;
    size_t n = size * nmemb;
    if (n > BF_MAX_BODY - r->used) return 0;
    memcpy(r->body + r->used, ptr, n);
    r->used += n;
    r->body[r->used] = '\0';
    return n;
}

static size_t collect_header(char *ptr, size_t size, size_t nmemb, void *userdata) {
    HttpResponse *r = userdata;
    size_t n = size * nmemb;
    if (n >= 6 && !strncasecmp(ptr, "Link:", 5)) {
        size_t have = strlen(r->link);
        size_t take = n < sizeof(r->link) - have - 2 ? n : sizeof(r->link) - have - 2;
        if (take) {
            memcpy(r->link + have, ptr, take);
            r->link[have + take] = ',';
            r->link[have + take + 1] = '\0';
        }
    }
    return n;
}

static int http_get(const char *url, HttpResponse *out) {
    CURL *c = curl_easy_init();
    CURLcode rc;
    if (!c) return 0;
    memset(out, 0, sizeof(*out));
    curl_easy_setopt(c, CURLOPT_URL, url);
    curl_easy_setopt(c, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(c, CURLOPT_MAXREDIRS, 3L);
    curl_easy_setopt(c, CURLOPT_USERAGENT, BF_UA);
    curl_easy_setopt(c, CURLOPT_TIMEOUT, 25L);
    curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT, 10L);
    curl_easy_setopt(c, CURLOPT_PROTOCOLS_STR, "http,https");
    curl_easy_setopt(c, CURLOPT_REDIR_PROTOCOLS_STR, "http,https");
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, collect_body);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, out);
    curl_easy_setopt(c, CURLOPT_HEADERFUNCTION, collect_header);
    curl_easy_setopt(c, CURLOPT_HEADERDATA, out);
    rc = curl_easy_perform(c);
    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &out->status);
    curl_easy_cleanup(c);
    return rc == CURLE_OK && out->status >= 200 && out->status < 400;
}

static int post_form(const char *url, const char *form, long *status) {
    CURL *c = curl_easy_init();
    CURLcode rc;
    struct curl_slist *headers = NULL;
    if (!c) return 0;
    headers = curl_slist_append(headers, "Content-Type: application/x-www-form-urlencoded");
    curl_easy_setopt(c, CURLOPT_URL, url);
    curl_easy_setopt(c, CURLOPT_POST, 1L);
    curl_easy_setopt(c, CURLOPT_POSTFIELDS, form);
    curl_easy_setopt(c, CURLOPT_POSTFIELDSIZE, (long)strlen(form));
    curl_easy_setopt(c, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(c, CURLOPT_USERAGENT, BF_UA);
    curl_easy_setopt(c, CURLOPT_TIMEOUT, 30L);
    curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT, 10L);
    curl_easy_setopt(c, CURLOPT_PROTOCOLS_STR, "http,https");
    rc = curl_easy_perform(c);
    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, status);
    curl_slist_free_all(headers);
    curl_easy_cleanup(c);
    return rc == CURLE_OK;
}

static int attr_value(const char *tag, const char *name, char *out, size_t n) {
    const char *p = tag;
    size_t want = strlen(name);
    while ((p = find_ci(p, name))) {
        const char *v, *e;
        if (p != tag && (isalnum((unsigned char)p[-1]) || p[-1] == '-' || p[-1] == '_')) { p += want; continue; }
        p += want;
        while (isspace((unsigned char)*p)) p++;
        if (*p != '=') continue;
        p++;
        while (isspace((unsigned char)*p)) p++;
        if (*p != '\'' && *p != '"') continue;
        char q = *p++;
        v = p; e = strchr(v, q);
        if (!e) return 0;
        if ((size_t)(e - v) >= n) return 0;
        memcpy(out, v, (size_t)(e - v)); out[e - v] = '\0';
        return 1;
    }
    return 0;
}

/* WebSub feeds almost universally publish absolute hub URLs.  We refuse an
 * ambiguous relative URL rather than invent a wrong remote endpoint. */
static int link_hub(const char *text, char *hub, size_t hub_n, char *self, size_t self_n) {
    const char *p = text;
    while ((p = strchr(p, '<'))) {
        const char *end = strchr(p, '>');
        const char *rel;
        size_t len;
        if (!end) break;
        len = (size_t)(end - p - 1);
        rel = find_ci(end, "rel=");
        if (rel && (!strncasecmp(rel + 4, "\"hub", 4) || !strncasecmp(rel + 4, "hub", 3))) {
            if (len < hub_n) { memcpy(hub, p + 1, len); hub[len] = '\0'; return 1; }
        }
        if (rel && (!strncasecmp(rel + 4, "\"self", 5) || !strncasecmp(rel + 4, "self", 4))) {
            if (len < self_n) { memcpy(self, p + 1, len); self[len] = '\0'; }
        }
        p = end + 1;
    }
    return 0;
}

static int discover(const char *topic, char *hub, size_t hub_n, char *self, size_t self_n) {
    HttpResponse r;
    const char *p;
    if (!is_http_url(topic, 0) || !http_get(topic, &r)) return 0;
    snprintf(self, self_n, "%s", topic);
    if (link_hub(r.link, hub, hub_n, self, self_n)) return 1;
    for (p = r.body; (p = find_ci(p, "<link")); ) {
        const char *end = strchr(p, '>');
        char tag[4096], rel[256] = {0}, href[BF_MAX_URL] = {0};
        size_t n;
        if (!end) break;
        n = (size_t)(end - p + 1);
        if (n >= sizeof(tag)) { p = end + 1; continue; }
        memcpy(tag, p, n); tag[n] = '\0';
        if (attr_value(tag, "rel", rel, sizeof(rel)) && attr_value(tag, "href", href, sizeof(href))) {
            if (find_ci(rel, "hub") && is_http_url(href, 0)) {
                snprintf(hub, hub_n, "%s", href);
                return 1;
            }
            if (find_ci(rel, "self") && is_http_url(href, 0)) snprintf(self, self_n, "%s", href);
        }
        p = end + 1;
    }
    return 0;
}

static int random_hex(char *out, size_t bytes) {
    unsigned char buf[64]; int fd; size_t i;
    if (bytes > sizeof(buf)) return 0;
    fd = open("/dev/urandom", O_RDONLY);
    if (fd < 0 || read(fd, buf, bytes) != (ssize_t)bytes) { if (fd >= 0) close(fd); return 0; }
    close(fd);
    for (i = 0; i < bytes; ++i) sprintf(out + 2 * i, "%02x", buf[i]);
    out[2 * bytes] = '\0';
    return 1;
}

/* A SourceDirty token is a queue key, not an identity or a bearer.  Material
 * demands have no subscription token, so derive a compact, stable queue key
 * from their topic.  We still compare the retained topic below: a hash
 * collision refuses the demand rather than merging two foreign resources. */
static unsigned long long topic_key(const char *s) {
    unsigned long long h = 1469598103934665603ULL;
    for (; s && *s; ++s) { h ^= (unsigned char)*s; h *= 1099511628211ULL; }
    return h;
}

static char *read_text_file(const char *path) {
    FILE *f = NULL; long n; char *out = NULL;
    if (!path || !(f = fopen(path, "rb"))) return NULL;
    if (fseek(f, 0, SEEK_END) || (n = ftell(f)) < 1 || n > 65536 || fseek(f, 0, SEEK_SET)) goto done;
    if (!(out = malloc((size_t)n + 1)) || fread(out, 1, (size_t)n, f) != (size_t)n) { free(out); out = NULL; goto done; }
    out[n] = '\0';
done:
    fclose(f); return out;
}

static sqlite3 *open_db(int writable) {
    sqlite3 *db = NULL;
    int flags = writable ? SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE : SQLITE_OPEN_READONLY;
    if (sqlite3_open_v2(db_path(), &db, flags, NULL) != SQLITE_OK) { if (db) sqlite3_close(db); return NULL; }
    sqlite3_busy_timeout(db, 5000);
    if (writable) {
        if (sqlite3_exec(db, "PRAGMA journal_mode=WAL;"
          "CREATE TABLE IF NOT EXISTS subscriptions("
          "token TEXT PRIMARY KEY,topic TEXT NOT NULL,hub TEXT NOT NULL,secret TEXT NOT NULL,"
          "authority_receipt TEXT NOT NULL DEFAULT '',state TEXT NOT NULL DEFAULT 'pending',"
          "lease_expires INTEGER DEFAULT 0,created_at TEXT,verified_at TEXT,last_delivery_at TEXT,"
          "last_fetch_at INTEGER DEFAULT 0,deliveries INTEGER DEFAULT 0,rejected INTEGER DEFAULT 0);"
          "CREATE TABLE IF NOT EXISTS source_dirty("
          "token TEXT PRIMARY KEY,topic TEXT NOT NULL,frontier TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'pending',"
          "dirty_at INTEGER NOT NULL,claimed_at INTEGER DEFAULT 0,attempts INTEGER DEFAULT 0,last_error TEXT DEFAULT '',"
          "demand_count INTEGER NOT NULL DEFAULT 0,last_demand_identity TEXT NOT NULL DEFAULT '');"
          "CREATE TABLE IF NOT EXISTS source_dirty_demand("
          "topic TEXT NOT NULL,identity TEXT NOT NULL,first_requested_at INTEGER NOT NULL,last_requested_at INTEGER NOT NULL,"
          "PRIMARY KEY(topic,identity));"
          "CREATE TABLE IF NOT EXISTS foreign_delta("
          "token TEXT PRIMARY KEY,topic TEXT NOT NULL,observed_at INTEGER NOT NULL,"
          "attempts INTEGER NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '');",
          NULL, NULL, NULL) != SQLITE_OK) { sqlite3_close(db); return NULL; }
        /* Migration is deliberately additive; old callback readers ignore it. */
        (void)sqlite3_exec(db, "ALTER TABLE subscriptions ADD COLUMN authority_receipt TEXT NOT NULL DEFAULT ''", NULL, NULL, NULL);
        (void)sqlite3_exec(db, "ALTER TABLE subscriptions ADD COLUMN renewal_requested_at TEXT", NULL, NULL, NULL);
        (void)sqlite3_exec(db, "ALTER TABLE source_dirty ADD COLUMN demand_count INTEGER NOT NULL DEFAULT 0", NULL, NULL, NULL);
        (void)sqlite3_exec(db, "ALTER TABLE source_dirty ADD COLUMN last_demand_identity TEXT NOT NULL DEFAULT ''", NULL, NULL, NULL);
    }
    return db;
}

/* MaterialCompiler requests observation; it does not perform it.  This is
 * the one WebSub-owned ingress that translates that request into the durable
 * SourceDirty queue consumed later by cmd_drain.  It has no network path. */
static int enqueue_material_demand(sqlite3 *db, const char *identity, const char *topic) {
    char token[65], current[BF_MAX_URL] = {0}; sqlite3_stmt *st = NULL; time_t now = time(NULL); int ok = 0;
    if (!db || !identity || !*identity || !topic || !is_http_url(topic, !permit_insecure_test())) return 0;
    if (snprintf(token, sizeof(token), "material-%016llx", topic_key(topic)) >= (int)sizeof(token)) return 0;
    if (sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) return 0;
    if (sqlite3_prepare_v2(db, "SELECT topic FROM source_dirty WHERE token=?", -1, &st, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(st, 1, token, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0)) snprintf(current, sizeof(current), "%s", sqlite3_column_text(st, 0));
    sqlite3_finalize(st); st = NULL;
    /* A collision must never cause an observation of the wrong source. */
    if (current[0] && strcmp(current, topic)) goto done;
    if (sqlite3_prepare_v2(db, "INSERT INTO source_dirty_demand(topic,identity,first_requested_at,last_requested_at) VALUES(?,?,?,?) ON CONFLICT(topic,identity) DO UPDATE SET last_requested_at=excluded.last_requested_at", -1, &st, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(st,1,topic,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,2,identity,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int64(st,3,(sqlite3_int64)now); sqlite3_bind_int64(st,4,(sqlite3_int64)now);
    if (sqlite3_step(st) != SQLITE_DONE) goto done;
    sqlite3_finalize(st); st = NULL;
    if (sqlite3_prepare_v2(db, "INSERT INTO source_dirty(token,topic,frontier,state,dirty_at,demand_count,last_demand_identity) VALUES(?,?,?,'pending',?,1,?) ON CONFLICT(token) DO UPDATE SET frontier=excluded.frontier,dirty_at=excluded.dirty_at,demand_count=source_dirty.demand_count+1,last_demand_identity=excluded.last_demand_identity", -1, &st, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(st,1,token,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,2,topic,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,3,"MaterialCompiler:observation-demand",-1,SQLITE_STATIC); sqlite3_bind_int64(st,4,(sqlite3_int64)now);
    sqlite3_bind_text(st,5,identity,-1,SQLITE_TRANSIENT);
    ok = sqlite3_step(st) == SQLITE_DONE;
done:
    if (st) sqlite3_finalize(st);
    if (ok) sqlite3_exec(db, "COMMIT", NULL, NULL, NULL); else sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
    return ok;
}

static int cmd_accept_demands(const char *dir, long limit) {
    DIR *d = NULL; struct dirent *e; int seen = 0, accepted = 0, rejected = 0, topics = 0; sqlite3 *db = NULL;
    if (!dir || limit < 1 || limit > 4096 || !(d = opendir(dir)) || !(db = open_db(1))) { if (d) closedir(d); return 2; }
    while ((e = readdir(d)) && seen < limit) {
        char path[4096], handled[4096], *body = NULL, *identity = NULL, *topic = NULL; sqlite3_stmt *parse = NULL; int new_topic = 0;
        { size_t name_n = strlen(e->d_name);
          /* A handled demand is durable history, not a fresh request.  A
           * substring check would accept `request.json.handled` again and
           * turn one schedule refresh into repeated SourceDirty ingress. */
          if (e->d_name[0] == '.' || name_n < 6 || strcmp(e->d_name + name_n - 5, ".json") || snprintf(path,sizeof(path),"%s/%s",dir,e->d_name) >= (int)sizeof(path)) continue;
        }
        seen++; body = read_text_file(path);
        if (!body || sqlite3_prepare_v2(db, "SELECT json_extract(?1,'$.identity'),json_extract(?1,'$.topic') WHERE json_valid(?1) AND json_extract(?1,'$.schema')='bonfyre-foreign-observation-demand.v1' AND json_extract(?1,'$.truth')='observation_requested_not_performed' AND typeof(json_extract(?1,'$.identity'))='text' AND length(json_extract(?1,'$.identity'))>0 AND typeof(json_extract(?1,'$.topic'))='text'", -1, &parse, NULL) != SQLITE_OK) goto reject;
        sqlite3_bind_text(parse,1,body,-1,SQLITE_TRANSIENT);
        if (sqlite3_step(parse) != SQLITE_ROW || !sqlite3_column_text(parse,0) || !sqlite3_column_text(parse,1)) goto reject;
        identity = strdup((const char *)sqlite3_column_text(parse,0)); topic = strdup((const char *)sqlite3_column_text(parse,1));
        sqlite3_finalize(parse); parse = NULL;
        if (!identity || !topic) goto reject;
        { sqlite3_stmt *q = NULL; if (sqlite3_prepare_v2(db,"SELECT 1 FROM source_dirty_demand WHERE topic=? LIMIT 1",-1,&q,NULL)==SQLITE_OK) { sqlite3_bind_text(q,1,topic,-1,SQLITE_TRANSIENT); new_topic = sqlite3_step(q) != SQLITE_ROW; } if (q) sqlite3_finalize(q); }
        if (!enqueue_material_demand(db, identity, topic) || snprintf(handled,sizeof(handled),"%s.handled",path) >= (int)sizeof(handled) || rename(path,handled)) goto reject;
        accepted++; topics += new_topic; free(body); free(identity); free(topic); continue;
reject:
        if (parse) sqlite3_finalize(parse); free(body); free(identity); free(topic); rejected++;
    }
    closedir(d); sqlite3_close(db);
    printf("{\"accepted_demands\":%d,\"coalesced_topics\":%d,\"rejected_or_deferred\":%d,\"truth\":\"SourceDirty_only_no_foreign_read_or_fact\"}\n", accepted, topics, rejected);
    return rejected ? 1 : 0;
}

static void urlenc(CURL *c, const char *v, char **out) { *out = curl_easy_escape(c, v, 0); }

static int send_subscription(const char *hub, const char *mode, const char *topic,
                             const char *callback, const char *secret, long lease, long *status) {
    CURL *esc = curl_easy_init();
    char *a = NULL, *b = NULL, *d = NULL, *e = NULL, *f = NULL;
    char lease_text[32], *form; int ok;
    if (!esc) return 0;
    urlenc(esc, callback, &a); urlenc(esc, mode, &b); urlenc(esc, topic, &d); urlenc(esc, secret, &e);
    if (!a || !b || !d || !e) { curl_free(a); curl_free(b); curl_free(d); curl_free(e); curl_easy_cleanup(esc); return 0; }
    snprintf(lease_text, sizeof(lease_text), "%ld", lease > 0 ? lease : 0L);
    f = curl_easy_escape(esc, lease_text, 0);
    form = malloc(strlen(a)+strlen(b)+strlen(d)+strlen(e)+strlen(f)+96);
    if (!form) { curl_free(a); curl_free(b); curl_free(d); curl_free(e); curl_free(f); curl_easy_cleanup(esc); return 0; }
    snprintf(form, strlen(a)+strlen(b)+strlen(d)+strlen(e)+strlen(f)+96,
      "hub.callback=%s&hub.mode=%s&hub.topic=%s&hub.secret=%s&hub.lease_seconds=%s", a,b,d,e,f);
    ok = post_form(hub, form, status);
    free(form); curl_free(a); curl_free(b); curl_free(d); curl_free(e); curl_free(f); curl_easy_cleanup(esc);
    return ok;
}

static int subscribe_one(const char *topic_input, const char *hub_override, long lease,
                         const char *authority, int quiet) {
    char hub[BF_MAX_URL] = {0}, self[BF_MAX_URL] = {0}, token[33], secret[65], callback[BF_MAX_URL];
    sqlite3 *db; sqlite3_stmt *st = NULL; long status = 0; int accepted;
    if (!authority || !*authority) { fprintf(stderr, "subscribe requires --authority-receipt; a CLI caller is not effect authority\n"); return 2; }
    if (!discover(topic_input, hub, sizeof(hub), self, sizeof(self))) {
        if (!quiet) printf("{\"topic\":"); json_string(topic_input); puts(",\"pushable\":false,\"state\":\"no_hub\"}");
        return 1;
    }
    if (hub_override && *hub_override) snprintf(hub, sizeof(hub), "%s", hub_override);
    if (!is_http_url(hub, !permit_insecure_test())) { fprintf(stderr, "hub must be HTTPS (or explicitly enabled local test HTTP)\n"); return 2; }
    if (!random_hex(token, 16) || !random_hex(secret, 32)) { perror("random"); return 1; }
    if (snprintf(callback, sizeof(callback), "%s/websub/%s", origin(), token) >= (int)sizeof(callback)) return 1;
    db = open_db(1); if (!db) { fprintf(stderr, "cannot open WebSub store %s\n", db_path()); return 1; }
    if (sqlite3_prepare_v2(db, "INSERT INTO subscriptions(token,topic,hub,secret,authority_receipt,state,created_at) VALUES(?,?,?,?,?,'pending',datetime('now'))", -1, &st, NULL) != SQLITE_OK) { sqlite3_close(db); return 1; }
    sqlite3_bind_text(st,1,token,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,2,self,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,3,hub,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,4,secret,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,5,authority,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_DONE) { sqlite3_finalize(st); sqlite3_close(db); return 1; }
    sqlite3_finalize(st);
    accepted = send_subscription(hub, "subscribe", self, callback, secret, lease, &status) && (status == 200 || status == 202 || status == 204);
    if (!accepted) {
        sqlite3_stmt *u = NULL;
        if (sqlite3_prepare_v2(db, "UPDATE subscriptions SET state='refused' WHERE token=?", -1, &u, NULL) == SQLITE_OK) { sqlite3_bind_text(u,1,token,-1,SQLITE_TRANSIENT); sqlite3_step(u); }
        if (u) sqlite3_finalize(u);
    }
    sqlite3_close(db);
    if (!quiet) { printf("{\"topic\":"); json_string(self); printf(",\"hub\":"); json_string(hub); printf(",\"callback\":"); json_string(callback); printf(",\"hub_response\":%ld,\"state\":\"%s\"}\n", status, accepted ? "pending" : "refused"); }
    return accepted ? 0 : 1;
}

static int cmd_list(void) {
    sqlite3 *db = open_db(0); sqlite3_stmt *st = NULL; int first = 1;
    if (!db) { puts("[]"); return 0; }
    if (sqlite3_prepare_v2(db, "SELECT token,topic,hub,state,deliveries,rejected,last_delivery_at,lease_expires,authority_receipt FROM subscriptions ORDER BY created_at DESC", -1, &st, NULL) != SQLITE_OK) { sqlite3_close(db); return 1; }
    puts("[");
    while (sqlite3_step(st) == SQLITE_ROW) {
        if (!first) puts(","); first = 0;
        printf(" {\"token\":"); json_string((const char*)sqlite3_column_text(st,0));
        printf(",\"topic\":"); json_string((const char*)sqlite3_column_text(st,1));
        printf(",\"hub\":"); json_string((const char*)sqlite3_column_text(st,2));
        printf(",\"state\":"); json_string((const char*)sqlite3_column_text(st,3));
        printf(",\"deliveries\":%d,\"rejected\":%d,\"lease_expires\":%lld,\"authority_receipt\":", sqlite3_column_int(st,4),sqlite3_column_int(st,5),(long long)sqlite3_column_int64(st,7));
        json_string((const char*)sqlite3_column_text(st,8)); printf(" }");
    }
    puts("\n]"); sqlite3_finalize(st); sqlite3_close(db); return 0;
}

static int cmd_unsubscribe(const char *token) {
    sqlite3 *db = open_db(1); sqlite3_stmt *st = NULL; char topic[BF_MAX_URL]={0},hub[BF_MAX_URL]={0},secret[65]={0},callback[BF_MAX_URL]; long status=0; int ok;
    if (!db) return 1;
    if (sqlite3_prepare_v2(db,"SELECT topic,hub,secret FROM subscriptions WHERE token=? AND state IN ('active','pending')",-1,&st,NULL)!=SQLITE_OK) { sqlite3_close(db); return 1; }
    sqlite3_bind_text(st,1,token,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(st)==SQLITE_ROW) { snprintf(topic,sizeof(topic),"%s",sqlite3_column_text(st,0)); snprintf(hub,sizeof(hub),"%s",sqlite3_column_text(st,1)); snprintf(secret,sizeof(secret),"%s",sqlite3_column_text(st,2)); }
    sqlite3_finalize(st);
    if (!topic[0] || !is_http_url(hub, !permit_insecure_test())) { sqlite3_close(db); return 1; }
    snprintf(callback,sizeof(callback),"%s/websub/%s",origin(),token);
    sqlite3_exec(db,"BEGIN IMMEDIATE",NULL,NULL,NULL);
    st=NULL; if (sqlite3_prepare_v2(db,"UPDATE subscriptions SET state='unsubscribe_pending' WHERE token=?",-1,&st,NULL)==SQLITE_OK) { sqlite3_bind_text(st,1,token,-1,SQLITE_TRANSIENT); sqlite3_step(st); } if(st)sqlite3_finalize(st);
    ok=send_subscription(hub,"unsubscribe",topic,callback,secret,0,&status) && (status==200||status==202||status==204);
    if (!ok) { st=NULL; if(sqlite3_prepare_v2(db,"UPDATE subscriptions SET state='active' WHERE token=?",-1,&st,NULL)==SQLITE_OK){sqlite3_bind_text(st,1,token,-1,SQLITE_TRANSIENT);sqlite3_step(st);} if(st)sqlite3_finalize(st); }
    sqlite3_exec(db,"COMMIT",NULL,NULL,NULL); sqlite3_close(db);
    printf("{\"token\":");json_string(token);printf(",\"hub_response\":%ld,\"state\":\"%s\"}\n",status,ok?"unsubscribe_pending":"active"); return ok?0:1;
}

static int cmd_reconcile(long window) {
    sqlite3 *db = open_db(1); sqlite3_stmt *st = NULL;
    SubscriptionRow *rows = NULL; size_t n = 0, capacity = 0;
    int renewed = 0, skipped = 0, failed = 0;
    time_t now = time(NULL);
    if (!db) return 1;
    if (sqlite3_prepare_v2(db, "UPDATE subscriptions SET state='expired' WHERE state='active' AND lease_expires>0 AND lease_expires<=?", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(st, 1, (sqlite3_int64)now); sqlite3_step(st);
    }
    if (st) sqlite3_finalize(st);
    if (sqlite3_prepare_v2(db,
        "SELECT token,topic,hub,secret,authority_receipt FROM subscriptions "
        "WHERE state='active' AND lease_expires>0 AND lease_expires-?<? "
        "AND (renewal_requested_at IS NULL OR renewal_requested_at < datetime('now','-5 minutes'))",
        -1, &st, NULL) != SQLITE_OK) { sqlite3_close(db); return 1; }
    sqlite3_bind_int64(st, 1, (sqlite3_int64)now); sqlite3_bind_int64(st, 2, (sqlite3_int64)window);
    while (n < BF_MAX_RECONCILE && sqlite3_step(st) == SQLITE_ROW) {
        if (n == capacity) {
            size_t next = capacity ? capacity * 2 : 16;
            SubscriptionRow *grown = realloc(rows, next * sizeof(*rows));
            if (!grown) {
                sqlite3_finalize(st); sqlite3_close(db); free(rows);
                return 1;
            }
            rows = grown;
            capacity = next;
        }
        snprintf(rows[n].token, sizeof(rows[n].token), "%s", sqlite3_column_text(st, 0));
        snprintf(rows[n].topic, sizeof(rows[n].topic), "%s", sqlite3_column_text(st, 1));
        snprintf(rows[n].hub, sizeof(rows[n].hub), "%s", sqlite3_column_text(st, 2));
        snprintf(rows[n].secret, sizeof(rows[n].secret), "%s", sqlite3_column_text(st, 3));
        snprintf(rows[n].authority, sizeof(rows[n].authority), "%s", sqlite3_column_text(st, 4));
        n++;
    }
    sqlite3_finalize(st); sqlite3_close(db);
    for (size_t i = 0; i < n; ++i) {
        char callback[BF_MAX_URL]; long status = 0; int accepted;
        if (!rows[i].authority[0]) { skipped++; continue; }
        snprintf(callback, sizeof(callback), "%s/websub/%s", origin(), rows[i].token);
        accepted = send_subscription(rows[i].hub, "subscribe", rows[i].topic, callback, rows[i].secret, 86400, &status)
            && (status == 200 || status == 202 || status == 204);
        db = open_db(1); if (!db) { free(rows); return 1; }
        if (sqlite3_prepare_v2(db, "UPDATE subscriptions SET renewal_requested_at=datetime('now') WHERE token=?", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, rows[i].token, -1, SQLITE_TRANSIENT); sqlite3_step(st);
        }
        if (st) sqlite3_finalize(st); sqlite3_close(db);
        if (accepted) renewed++; else failed++;
    }
    printf("{\"checked_at\":%lld,\"renewed_same_identity\":%d,\"authority_missing\":%d,\"failed\":%d}\n",
           (long long)now, renewed, skipped, failed);
    free(rows);
    return failed ? 1 : 0;
}

/* A child exit status is not a ForeignTwin receipt.  The observer appends a
 * JSON receipt log, so validate its newest record against the topic before a
 * WebSub worker can emit the downstream FactDelta. */
static int receipt_matches_foreign_observation(const char *path, const char *topic) {
    FILE *f = NULL; char line[16384], latest[16384] = {0}; sqlite3 *db = NULL; sqlite3_stmt *st = NULL; int ok = 0;
    if (!path || !topic || !(f = fopen(path, "rb"))) return 0;
    while (fgets(line, sizeof(line), f)) if (line[0] == '{') snprintf(latest, sizeof(latest), "%s", line);
    fclose(f);
    if (!latest[0] || sqlite3_open(":memory:", &db) != SQLITE_OK) goto done;
    if (sqlite3_prepare_v2(db,
        "SELECT json_valid(?1) AND json_extract(?1,'$.schema')='bonfyre-foreign-observation.v1' "
        "AND json_extract(?1,'$.source_url')=?2 AND json_extract(?1,'$.observation_kind') IN "
        "('source_observed','repeat_observed','boundary_response','policy_excluded','policy_indeterminate')",
        -1, &st, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(st,1,latest,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,2,topic,-1,SQLITE_TRANSIENT);
    ok = sqlite3_step(st) == SQLITE_ROW && sqlite3_column_int(st,0) == 1;
done:
    if (st) sqlite3_finalize(st); if (db) sqlite3_close(db); return ok;
}

static int observe_dirty(const char *token, const char *topic) {
    char executable[BF_MAX_URL], root[BF_MAX_URL], receipts[BF_MAX_URL], bodies[BF_MAX_URL], stamp[32], twin[160], receipt[BF_MAX_URL], body[BF_MAX_URL];
    const char *bindir = getenv("BONFYRE_BIN_DIR"); const char *foreign_root = getenv("BONFYRE_FOREIGN_ROOT");
    pid_t child; int status;
    if (!bindir || !*bindir) bindir = "/usr/local/bin";
    if (!foreign_root || !*foreign_root) foreign_root = "/var/lib/bonfyre/foreign";
    if (snprintf(executable, sizeof(executable), "%s/bonfyre-project", bindir) >= (int)sizeof(executable) || access(executable, X_OK)) return 0;
    if (snprintf(root, sizeof(root), "%s", foreign_root) >= (int)sizeof(root) ||
        snprintf(receipts, sizeof(receipts), "%s/receipts", root) >= (int)sizeof(receipts) ||
        snprintf(bodies, sizeof(bodies), "%s/bodies", root) >= (int)sizeof(bodies)) return 0;
    if ((mkdir(root,0750) && errno != EEXIST) || (mkdir(receipts,0750) && errno != EEXIST) || (mkdir(bodies,0750) && errno != EEXIST)) return 0;
    snprintf(stamp, sizeof(stamp), "%lld", (long long)time(NULL));
    snprintf(twin, sizeof(twin), "websub-%.32s-%s", token, stamp);
    snprintf(receipt, sizeof(receipt), "%s/%s.json", receipts, twin);
    snprintf(body, sizeof(body), "%s/%s.body", bodies, twin);
    child = fork();
    if (child < 0) return 0;
    if (child == 0) {
        char *const args[] = { executable, "foreign", "observe", twin, (char *)topic,
          "--receipt", receipt, "--body", body, "--observe-right", "public", NULL };
        execv(executable, args); _exit(127);
    }
    return waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0 &&
           receipt_matches_foreign_observation(receipt, topic);
}

/* ForeignTwin has already written its own receipt when this runs.  This is
 * only the causal notification which lets AddressPlane calculate a dependent
 * cut.  A deterministic delta id makes retry safe: a pre-existing completed
 * file is the same notification, not a second observation. */
static int write_foreign_delta(const char *token, const char *topic, time_t observed_at) {
    const char *root=getenv("BONFYRE_FACT_DELTAS"); char path[4096], tmp[4096], id[160], write[4096]; int fd; FILE *f;
    if(!root||!*root)root="/var/lib/bonfyre/fact-deltas";
    if((mkdir(root,0750)&&errno!=EEXIST))return 0;
    if(snprintf(id,sizeof(id),"foreign-twin:%s:%lld",token,(long long)observed_at)>=(int)sizeof(id)||
       snprintf(write,sizeof(write),"ForeignTwin:%s",topic)>=(int)sizeof(write)||
       snprintf(path,sizeof(path),"%s/foreign-twin-%s-%lld.json",root,token,(long long)observed_at)>=(int)sizeof(path)||
       snprintf(tmp,sizeof(tmp),"%s.tmp.%ld",path,(long)getpid())>=(int)sizeof(tmp))return 0;
    if(access(path,F_OK)==0)return 1;
    fd=open(tmp,O_WRONLY|O_CREAT|O_EXCL,0640);if(fd<0)return 0;
    f=fdopen(fd,"wb");if(!f){close(fd);unlink(tmp);return 0;}
    fputs("{\"schema\":\"bonfyre-fact-delta.v1\",\"delta_id\":",f);json_file_string(f,id);
    fputs(",\"causal_frontier\":\"ForeignTwin:receipt\",\"writes\":[",f);json_file_string(f,write);
    fputs("],\"write_count\":1,\"forbidden_inference\":\"a ForeignTwin change notification is not evidence, proof, authority or an external effect\"}\n",f);
    if(fflush(f)||fsync(fd)||fclose(f)){unlink(tmp);return 0;}
    if(rename(tmp,path)){unlink(tmp);return access(path,F_OK)==0;}
    return 1;
}

static int flush_foreign_deltas(sqlite3 *db, long limit) {
    int forwarded=0;sqlite3_stmt *pick=NULL,*done=NULL;
    if(sqlite3_prepare_v2(db,"SELECT token,topic,observed_at FROM foreign_delta ORDER BY observed_at LIMIT ?",-1,&pick,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_int(pick,1,(int)limit);
    while(sqlite3_step(pick)==SQLITE_ROW){const char *token=(const char*)sqlite3_column_text(pick,0),*topic=(const char*)sqlite3_column_text(pick,1);time_t at=(time_t)sqlite3_column_int64(pick,2);int ok=token&&topic&&write_foreign_delta(token,topic,at);
        if(ok){if(sqlite3_prepare_v2(db,"DELETE FROM foreign_delta WHERE token=?",-1,&done,NULL)!=SQLITE_OK)break;sqlite3_bind_text(done,1,token,-1,SQLITE_TRANSIENT);sqlite3_step(done);sqlite3_finalize(done);done=NULL;forwarded++;}
        else {if(sqlite3_prepare_v2(db,"UPDATE foreign_delta SET attempts=attempts+1,last_error='fact_delta_write_failed' WHERE token=?",-1,&done,NULL)!=SQLITE_OK)break;sqlite3_bind_text(done,1,token,-1,SQLITE_TRANSIENT);sqlite3_step(done);sqlite3_finalize(done);done=NULL;}
    }
    sqlite3_finalize(pick);sqlite3_finalize(done);return forwarded;
}

static int cmd_drain(long limit) {
    int observed = 0, failed = 0, forwarded = 0; time_t now = time(NULL); sqlite3 *db = NULL;
    if (limit < 1 || limit > 256) return 2;
    for (long i = 0; i < limit; ++i) {
        sqlite3 *db = open_db(1); sqlite3_stmt *st = NULL; char token[65]={0}, topic[BF_MAX_URL]={0}; int claimed = 0;
        if (!db) return 1;
        sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL);
        if (sqlite3_prepare_v2(db, "SELECT token,topic FROM source_dirty WHERE state='pending' ORDER BY dirty_at LIMIT 1", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) {
            snprintf(token,sizeof(token),"%s",sqlite3_column_text(st,0)); snprintf(topic,sizeof(topic),"%s",sqlite3_column_text(st,1));
        }
        if (st) sqlite3_finalize(st); st = NULL;
        if (token[0] && sqlite3_prepare_v2(db,"UPDATE source_dirty SET state='running',claimed_at=? WHERE token=? AND state='pending'",-1,&st,NULL)==SQLITE_OK) {
            sqlite3_bind_int64(st,1,(sqlite3_int64)now);sqlite3_bind_text(st,2,token,-1,SQLITE_TRANSIENT);sqlite3_step(st);claimed=sqlite3_changes(db)==1;
        }
        if(st)sqlite3_finalize(st);sqlite3_exec(db,"COMMIT",NULL,NULL,NULL);sqlite3_close(db);
        if (!claimed) break;
        db=open_db(1); if(!db)return 1;
        if (observe_dirty(token, topic)) {
            /* Store a separate durable notification obligation before the
             * dirty source is retired.  If file publication fails, the next
             * bounded drain retries this local handoff without re-observing
             * the foreign world. */
            if(sqlite3_prepare_v2(db,"INSERT OR REPLACE INTO foreign_delta(token,topic,observed_at,attempts,last_error) VALUES(?,?,?,0,'')",-1,&st,NULL)==SQLITE_OK){sqlite3_bind_text(st,1,token,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,topic,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(st,3,(sqlite3_int64)now);sqlite3_step(st);} if(st)sqlite3_finalize(st);st=NULL;
            if(sqlite3_prepare_v2(db,"UPDATE source_dirty SET state='pending' WHERE token=? AND dirty_at>claimed_at",-1,&st,NULL)==SQLITE_OK){sqlite3_bind_text(st,1,token,-1,SQLITE_TRANSIENT);sqlite3_step(st);} if(st)sqlite3_finalize(st); st=NULL;
            if(sqlite3_prepare_v2(db,"DELETE FROM source_dirty WHERE token=? AND dirty_at<=claimed_at",-1,&st,NULL)==SQLITE_OK){sqlite3_bind_text(st,1,token,-1,SQLITE_TRANSIENT);sqlite3_step(st);} observed++;
        } else {
            if(sqlite3_prepare_v2(db,"UPDATE source_dirty SET state='pending',attempts=attempts+1,last_error='foreign_observe_failed' WHERE token=?",-1,&st,NULL)==SQLITE_OK){sqlite3_bind_text(st,1,token,-1,SQLITE_TRANSIENT);sqlite3_step(st);} failed++;
        }
        if(st)sqlite3_finalize(st);sqlite3_close(db);
    }
    db=open_db(1);if(!db)return 1;forwarded=flush_foreign_deltas(db,limit);sqlite3_close(db);
    printf("{\"observed\":%d,\"foreign_twin_fact_deltas\":%d,\"failed\":%d}\n",observed,forwarded,failed); return failed?1:0;
}

static void usage(void) {
    fputs("BonfyreWebSub — receipt-bound WebSub carrier adapter\n\n"
          "  bonfyre-websub discover TOPIC\n"
          "  bonfyre-websub subscribe TOPIC --authority-receipt RECEIPT [--hub URL] [--lease SECONDS]\n"
          "  bonfyre-websub reconcile [--window SECONDS]\n"
          "  bonfyre-websub accept-demands --dir DIRECTORY [--limit 256]\n"
          "  bonfyre-websub drain [--limit 16]\n"
          "  bonfyre-websub unsubscribe TOKEN\n"
          "  bonfyre-websub list\n", stderr);
}

#define WEBSUB_COMMANDS(X) \
    X("accept-demands", WC_ACCEPT_DEMANDS) \
    X("discover", WC_DISCOVER) \
    X("drain", WC_DRAIN) \
    X("list", WC_LIST) \
    X("reconcile", WC_RECONCILE) \
    X("subscribe", WC_SUBSCRIBE) \
    X("unsubscribe", WC_UNSUBSCRIBE)

enum {
#define WC_ENUM(name, id) id,
    WEBSUB_COMMANDS(WC_ENUM)
#undef WC_ENUM
};
static const BfCommandEntry websub_commands[] = {
#define WC_ENTRY(name, id) {name, id, 0, NULL},
    WEBSUB_COMMANDS(WC_ENTRY)
#undef WC_ENTRY
};

int main(int argc, char **argv) {
    const BfCommandEntry *command;
    const char *hub = NULL, *authority = NULL; long lease = 86400, window = 3600;
    if (argc < 2 || !(command = bf_command_lookup(websub_commands,
            sizeof(websub_commands) / sizeof(websub_commands[0]), argv[1]))) {
        usage(); return 2;
    }
    if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) return 1;
    switch (command->id) {
    case WC_DISCOVER:
        if (argc == 3) {
            char h[BF_MAX_URL] = {0}, s[BF_MAX_URL] = {0};
            int ok = discover(argv[2], h, sizeof(h), s, sizeof(s));
            printf("{\"topic\":"); json_string(argv[2]);
            printf(",\"self\":"); json_string(ok ? s : argv[2]);
            printf(",\"hub\":"); if (ok) json_string(h); else fputs("null", stdout);
            printf(",\"pushable\":%s}\n", ok ? "true" : "false");
            return ok ? 0 : 1;
        }
        break;
    case WC_SUBSCRIBE:
        if (argc >= 3) {
            for (int i = 3; i < argc; i++) {
                if (!strcmp(argv[i], "--hub") && i + 1 < argc) hub = argv[++i];
                else if (!strcmp(argv[i], "--lease") && i + 1 < argc) lease = strtol(argv[++i], NULL, 10);
                else if (!strcmp(argv[i], "--authority-receipt") && i + 1 < argc) authority = argv[++i];
                else { usage(); return 2; }
            }
            return subscribe_one(argv[2], hub, lease, authority, 0);
        }
        break;
    case WC_RECONCILE:
        for (int i = 2; i < argc; i++) {
            if (!strcmp(argv[i], "--window") && i + 1 < argc) window = strtol(argv[++i], NULL, 10);
            else { usage(); return 2; }
        }
        return cmd_reconcile(window);
    case WC_ACCEPT_DEMANDS: {
        const char *dir = NULL; long limit = 256;
        for (int i = 2; i < argc; i++) {
            if (!strcmp(argv[i], "--dir") && i + 1 < argc) dir = argv[++i];
            else if (!strcmp(argv[i], "--limit") && i + 1 < argc) limit = strtol(argv[++i], NULL, 10);
            else { usage(); return 2; }
        }
        return cmd_accept_demands(dir, limit);
    }
    case WC_DRAIN: {
        long limit = 16;
        for (int i = 2; i < argc; i++) {
            if (!strcmp(argv[i], "--limit") && i + 1 < argc) limit = strtol(argv[++i], NULL, 10);
            else { usage(); return 2; }
        }
        return cmd_drain(limit);
    }
    case WC_UNSUBSCRIBE: if (argc == 3) return cmd_unsubscribe(argv[2]); break;
    case WC_LIST: if (argc == 2) return cmd_list(); break;
    default: break;
    }
    usage(); return 2;
}
