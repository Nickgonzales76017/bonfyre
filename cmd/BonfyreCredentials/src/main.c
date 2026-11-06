/* BonfyreCredentials -- the dedicated credential boundary.
 *
 * Holds what providers require before the estate can read from them (keys,
 * secrets, account and client identifiers), generates what the estate can
 * make for itself, applies credentials to outbound requests for internal
 * fetchers, verifies them against the provider, and derives the required
 * fields for any service described by an OpenAPI document.
 *
 * Laws:
 *   - every value is sealed with AES-256-GCM under a 32-byte key file that
 *     only the service user can read; the provider and field are the
 *     associated data, so a value cannot be moved to another slot.
 *   - a stored value is never shown again: status answers set / missing /
 *     generated with the last four characters of a sensitive value only.  A
 *     generated secret may be revealed once, at generation, to be handed on.
 *   - no value reaches argv, a receipt, a log or an agent answer; verification
 *     hands headers to curl on its standard input.
 *   - the page answers only on loopback, only to a Host naming loopback, only
 *     for a signed-in Bonfyre admin, and only with a matching CSRF token.
 *   - possession of a credential is never authority: it does not identify an
 *     actor, admit a provider, approve spend or grant an effect.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE
#endif
#include <bonfyre.h>
#include <bf_json.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/sha.h>
#include <signal.h>
#include <sqlite3.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define MAX_VALUE 4096
#define MAX_BODY (1024 * 1024)

static const char *arg(int argc, char **argv, const char *name) {
    int i;
    for (i = 2; i + 1 < argc; i++) if (!strcmp(argv[i], name)) return argv[i + 1];
    return NULL;
}
static int flag(int argc, char **argv, const char *name) { int i; for (i = 2; i < argc; i++) if (!strcmp(argv[i], name)) return 1; return 0; }

static void jstr(FILE *out, const char *s) {
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

static void html(FILE *out, const char *s) {
    for (; s && *s; s++) {
        if (*s == '&') fputs("&amp;", out); else if (*s == '<') fputs("&lt;", out); else if (*s == '>') fputs("&gt;", out);
        else if (*s == '"') fputs("&quot;", out); else if (*s == '\'') fputs("&#39;", out); else fputc(*s, out);
    }
}

static void now_text(char *out, size_t cap) {
    time_t t = time(NULL); struct tm tm;
    if (gmtime_r(&t, &tm)) strftime(out, cap, "%Y-%m-%dT%H:%M:%SZ", &tm); else snprintf(out, cap, "unknown");
}

/* ------------------------------------------------------------ key and seal */

static int load_key(const char *path, unsigned char key[32]) {
    int fd; struct stat st; ssize_t n;
    if (!path || (fd = open(path, O_RDONLY)) < 0) return 0;
    if (fstat(fd, &st) || (st.st_mode & 077)) { close(fd); fprintf(stderr, "credentials: key file must not be readable by group or others\n"); return 0; }
    n = read(fd, key, 32);
    close(fd);
    return n == 32;
}

static int init_key(const char *path) {
    unsigned char key[32]; int fd; ssize_t n;
    if (!path) return 0;
    if ((fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600)) < 0) { fprintf(stderr, "credentials: %s: %s (an existing key is never replaced)\n", path, strerror(errno)); return 0; }
    if (RAND_bytes(key, sizeof(key)) != 1) { close(fd); unlink(path); return 0; }
    n = write(fd, key, sizeof(key));
    OPENSSL_cleanse(key, sizeof(key));
    close(fd);
    if (n != 32) { unlink(path); return 0; }
    printf("{\"key\":\"created\",\"mode\":\"0600\"}\n");
    return 1;
}

/* The account a sealed value belongs to.  Empty is the estate's own account and
 * keeps the original binding, so values sealed before accounts existed still
 * open.  One process works for one account; the loopback page uses the empty
 * account only. */
static const char *g_account = "";

static void aad_for(char *aad, size_t cap, const char *provider, const char *field) {
    if (g_account[0]) snprintf(aad, cap, "%s\n%s\n%s", provider, field, g_account);
    else snprintf(aad, cap, "%s\n%s", provider, field);
}

static int seal(const unsigned char key[32], const char *provider, const char *field, const char *value, unsigned char **blob, int *blob_len) {
    unsigned char nonce[12], tag[16]; char aad[256]; int len = 0, total, vlen = (int)strlen(value);
    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new(); unsigned char *out;
    if (!ctx || RAND_bytes(nonce, 12) != 1 || !(out = malloc((size_t)vlen + 28))) { EVP_CIPHER_CTX_free(ctx); return 0; }
    aad_for(aad, sizeof(aad), provider, field);
    if (EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), NULL, key, nonce) != 1 ||
        EVP_EncryptUpdate(ctx, NULL, &len, (const unsigned char *)aad, (int)strlen(aad)) != 1 ||
        EVP_EncryptUpdate(ctx, out + 28, &len, (const unsigned char *)value, vlen) != 1) goto fail;
    total = len;
    if (EVP_EncryptFinal_ex(ctx, out + 28 + total, &len) != 1) goto fail;
    total += len;
    if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, 16, tag) != 1) goto fail;
    memcpy(out, nonce, 12); memcpy(out + 12, tag, 16);
    *blob = out; *blob_len = 28 + total;
    EVP_CIPHER_CTX_free(ctx);
    return 1;
fail:
    EVP_CIPHER_CTX_free(ctx); free(out);
    return 0;
}

static int unseal(const unsigned char key[32], const char *provider, const char *field, const unsigned char *blob, int blob_len, char *out, size_t cap) {
    char aad[256]; int len = 0, total; EVP_CIPHER_CTX *ctx;
    if (blob_len < 28 || (size_t)(blob_len - 28) >= cap || !(ctx = EVP_CIPHER_CTX_new())) return 0;
    aad_for(aad, sizeof(aad), provider, field);
    if (EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), NULL, key, blob) != 1 ||
        EVP_DecryptUpdate(ctx, NULL, &len, (const unsigned char *)aad, (int)strlen(aad)) != 1 ||
        EVP_DecryptUpdate(ctx, (unsigned char *)out, &len, blob + 28, blob_len - 28) != 1) { EVP_CIPHER_CTX_free(ctx); return 0; }
    total = len;
    if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, 16, (void *)(blob + 12)) != 1 || EVP_DecryptFinal_ex(ctx, (unsigned char *)out + total, &len) != 1) {
        EVP_CIPHER_CTX_free(ctx); OPENSSL_cleanse(out, cap); return 0;
    }
    total += len; out[total] = '\0';
    EVP_CIPHER_CTX_free(ctx);
    return 1;
}

/* ------------------------------------------------------------ store */

/* A rollback journal, not WAL: the page service and the reheat run as
 * different users, and WAL side files left by one would lock out the other. */
static const char *schema_sql =
    "PRAGMA journal_mode=DELETE;"
    "CREATE TABLE IF NOT EXISTS imported_provider(provider TEXT PRIMARY KEY, declaration TEXT NOT NULL, spec_source TEXT NOT NULL, imported_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS sealed_field(provider TEXT NOT NULL, field TEXT NOT NULL, sealed BLOB NOT NULL, last4 TEXT NOT NULL, generated INTEGER NOT NULL,"
    " set_by TEXT NOT NULL, set_at TEXT NOT NULL, account TEXT NOT NULL DEFAULT '', PRIMARY KEY(provider, account, field));"
    "CREATE TABLE IF NOT EXISTS verification(provider TEXT NOT NULL, at TEXT NOT NULL, http_status INTEGER NOT NULL, outcome TEXT NOT NULL, account TEXT NOT NULL DEFAULT '');"
    "CREATE TABLE IF NOT EXISTS token_cache(provider TEXT NOT NULL, sealed BLOB NOT NULL, expires_at INTEGER NOT NULL, account TEXT NOT NULL DEFAULT '', PRIMARY KEY(provider, account));"
    "CREATE TABLE IF NOT EXISTS audit(at TEXT NOT NULL, actor TEXT NOT NULL, provider TEXT NOT NULL, field TEXT NOT NULL, action TEXT NOT NULL, outcome TEXT NOT NULL, account TEXT NOT NULL DEFAULT '');";

/* Stores made before accounts existed keyed a value by provider and field
 * alone, so a second account's key for the same provider would have replaced
 * the first.  Their rows move, unchanged, into the estate's own account. */
static const char *account_migration_sql =
    "BEGIN IMMEDIATE;"
    "CREATE TABLE sealed_field_accounts(provider TEXT NOT NULL, field TEXT NOT NULL, sealed BLOB NOT NULL, last4 TEXT NOT NULL, generated INTEGER NOT NULL,"
    " set_by TEXT NOT NULL, set_at TEXT NOT NULL, account TEXT NOT NULL DEFAULT '', PRIMARY KEY(provider, account, field));"
    "INSERT INTO sealed_field_accounts SELECT provider, field, sealed, last4, generated, set_by, set_at, '' FROM sealed_field;"
    "DROP TABLE sealed_field; ALTER TABLE sealed_field_accounts RENAME TO sealed_field;"
    "CREATE TABLE token_cache_accounts(provider TEXT NOT NULL, sealed BLOB NOT NULL, expires_at INTEGER NOT NULL, account TEXT NOT NULL DEFAULT '', PRIMARY KEY(provider, account));"
    "INSERT INTO token_cache_accounts SELECT provider, sealed, expires_at, '' FROM token_cache;"
    "DROP TABLE token_cache; ALTER TABLE token_cache_accounts RENAME TO token_cache;"
    "ALTER TABLE verification ADD COLUMN account TEXT NOT NULL DEFAULT '';"
    "ALTER TABLE audit ADD COLUMN account TEXT NOT NULL DEFAULT '';"
    "COMMIT;";

static int has_column(sqlite3 *db, const char *table, const char *column) {
    sqlite3_stmt *st = NULL; char sql[96]; int found = 0;
    snprintf(sql, sizeof(sql), "PRAGMA table_info(%s)", table);
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
    while (!found && sqlite3_step(st) == SQLITE_ROW) found = !strcmp((const char *)sqlite3_column_text(st, 1), column);
    sqlite3_finalize(st);
    return found;
}

static sqlite3 *open_store(const char *path) {
    sqlite3 *db = NULL; char *err = NULL;
    if (!path) return NULL;
    { int fd = open(path, O_RDWR | O_CREAT, 0600); if (fd >= 0) close(fd); }
    if (sqlite3_open(path, &db) != SQLITE_OK || sqlite3_exec(db, schema_sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "credentials: %s\n", err ? err : sqlite3_errmsg(db)); sqlite3_free(err); sqlite3_close(db); return NULL;
    }
    if (!has_column(db, "sealed_field", "account") && sqlite3_exec(db, account_migration_sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "credentials: account migration: %s\n", err ? err : sqlite3_errmsg(db));
        sqlite3_free(err); sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL); sqlite3_close(db); return NULL;
    }
    chmod(path, 0600);
    return db;
}

static void audit(sqlite3 *db, const char *actor, const char *provider, const char *field, const char *action, const char *outcome) {
    sqlite3_stmt *st = NULL; char at[32];
    now_text(at, sizeof(at));
    if (sqlite3_prepare_v2(db, "INSERT INTO audit VALUES(?,?,?,?,?,?,?)", -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, at, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, actor ? actor : "unknown", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, provider, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 4, field ? field : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 5, action, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 6, outcome, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 7, g_account, -1, SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);
}

/* ------------------------------------------------------------ declarations */

typedef struct {
    bf_json_doc_t *declared;       /* the compiled CUE providers */
    sqlite3 *db;                   /* imported OpenAPI providers live here */
} Registry;

static char *read_file(const char *path, size_t *size) {
    FILE *f = fopen(path, "rb"); long n; char *buf;
    if (!f || fseek(f, 0, SEEK_END) || (n = ftell(f)) < 0 || fseek(f, 0, SEEK_SET) || !(buf = malloc((size_t)n + 1)) || fread(buf, 1, (size_t)n, f) != (size_t)n) { if (f) fclose(f); return NULL; }
    fclose(f); buf[n] = '\0'; if (size) *size = (size_t)n;
    return buf;
}

static int string_of(const bf_json_doc_t *doc, const bf_json_node_t *obj, const char *key, char *out, size_t cap) {
    const bf_json_node_t *n = obj ? bf_json_obj_get(doc, obj, key) : NULL;
    out[0] = '\0';
    return n && n->type == BF_JSON_STRING && bf_json_get_str_copy(n, out, cap) >= 0 && out[0];
}

/* Finds a provider's declaration: compiled first, then imported.  The caller
 * frees *owned when set. */
static const bf_json_node_t *find_provider(Registry *r, const char *id, const bf_json_doc_t **doc, bf_json_doc_t **owned) {
    const bf_json_node_t *root = r->declared ? bf_json_root(r->declared) : NULL, *p;
    char buf[128]; sqlite3_stmt *st = NULL;
    *owned = NULL;
    for (p = root ? bf_json_child_first(r->declared, root) : NULL; p; p = bf_json_child_next(r->declared, p))
        if (string_of(r->declared, p, "provider", buf, sizeof(buf)) && !strcmp(buf, id)) { *doc = r->declared; return p; }
    if (sqlite3_prepare_v2(r->db, "SELECT declaration FROM imported_provider WHERE provider=?", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            char err[160];
            *owned = bf_json_parse_str((const char *)sqlite3_column_text(st, 0), err, sizeof(err));
        }
    }
    sqlite3_finalize(st);
    if (*owned) { *doc = *owned; return bf_json_root(*owned); }
    return NULL;
}

static const bf_json_node_t *find_field(const bf_json_doc_t *doc, const bf_json_node_t *provider, const char *field) {
    const bf_json_node_t *fields = bf_json_obj_get(doc, provider, "fields"), *f; char buf[64];
    for (f = fields ? bf_json_child_first(doc, fields) : NULL; f; f = bf_json_child_next(doc, f))
        if (string_of(doc, f, "field", buf, sizeof(buf)) && !strcmp(buf, field)) return f;
    return NULL;
}

static int load_value(sqlite3 *db, const unsigned char key[32], const char *provider, const char *field, char *out, size_t cap) {
    sqlite3_stmt *st = NULL; int ok = 0;
    if (sqlite3_prepare_v2(db, "SELECT sealed FROM sealed_field WHERE provider=? AND field=? AND account=?", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, provider, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, field, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, g_account, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) ok = unseal(key, provider, field, sqlite3_column_blob(st, 0), sqlite3_column_bytes(st, 0), out, cap);
    sqlite3_finalize(st);
    return ok;
}

static int valid_id(const char *s, size_t max) {
    size_t n = s ? strlen(s) : 0;
    return n && n <= max && strspn(s, "abcdefghijklmnopqrstuvwxyz0123456789-_") == n;
}

static int store_value(sqlite3 *db, const unsigned char key[32], const char *provider, const char *field, const char *value, int generated, const char *actor, int sensitive) {
    unsigned char *blob = NULL; int blob_len = 0, ok; sqlite3_stmt *st = NULL; char at[32], last4[8] = "";
    size_t len = strlen(value);
    if (!len || len >= MAX_VALUE || !seal(key, provider, field, value, &blob, &blob_len)) return 0;
    if (sensitive) snprintf(last4, sizeof(last4), "%s", len > 8 ? value + len - 4 : "");
    else snprintf(last4, sizeof(last4), "%.7s", value);
    now_text(at, sizeof(at));
    ok = sqlite3_prepare_v2(db, "INSERT INTO sealed_field VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(provider,account,field) DO UPDATE SET "
        "sealed=excluded.sealed,last4=excluded.last4,generated=excluded.generated,set_by=excluded.set_by,set_at=excluded.set_at", -1, &st, NULL) == SQLITE_OK;
    if (ok) {
        sqlite3_bind_text(st, 1, provider, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, field, -1, SQLITE_TRANSIENT);
        sqlite3_bind_blob(st, 3, blob, blob_len, SQLITE_TRANSIENT); sqlite3_bind_text(st, 4, last4, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 5, generated); sqlite3_bind_text(st, 6, actor ? actor : "unknown", -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 7, at, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 8, g_account, -1, SQLITE_STATIC);
        ok = sqlite3_step(st) == SQLITE_DONE;
    }
    sqlite3_finalize(st);
    OPENSSL_cleanse(blob, (size_t)blob_len); free(blob);
    audit(db, actor, provider, field, generated ? "generate" : "set", ok ? "stored" : "failed");
    return ok;
}

static int generate_value(const char *generator, char *out, size_t cap) {
    unsigned char raw[32]; int i;
    if (RAND_bytes(raw, sizeof(raw)) != 1) return 0;
    if (!strcmp(generator, "random-hex-32")) { for (i = 0; i < 32; i++) snprintf(out + 2 * i, cap - 2 * (size_t)i, "%02x", raw[i]); }
    else if (!strcmp(generator, "random-base64url-32")) {
        unsigned char b64[64]; int n = EVP_EncodeBlock(b64, raw, 32), j = 0;
        for (i = 0; i < n && (size_t)j + 1 < cap; i++) { char c = (char)b64[i]; if (c == '=') continue; out[j++] = c == '+' ? '-' : c == '/' ? '_' : c; }
        out[j] = '\0';
    } else if (!strcmp(generator, "uuid")) {
        raw[6] = (unsigned char)((raw[6] & 0x0f) | 0x40); raw[8] = (unsigned char)((raw[8] & 0x3f) | 0x80);
        snprintf(out, cap, "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x", raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7], raw[8], raw[9], raw[10], raw[11], raw[12], raw[13], raw[14], raw[15]);
    } else return 0;
    OPENSSL_cleanse(raw, sizeof(raw));
    return 1;
}

/* ------------------------------------------------------------ templates */

static int registry_value(Registry *r, const unsigned char key[32], const char *provider, const char *field, char *out, size_t cap);

static int render(Registry *r, const unsigned char key[32], const char *provider, const char *tmpl, time_t epoch, char *out, size_t cap) {
    sqlite3 *db = r->db;
    size_t n = 0; const char *t = tmpl;
    while (*t && n + 1 < cap) {
        if (*t != '{') { out[n++] = *t++; continue; }
        {
            const char *close = strchr(t, '}'); char expr[256], value[MAX_VALUE];
            size_t len;
            if (!close || (size_t)(close - t - 1) >= sizeof(expr)) return 0;
            memcpy(expr, t + 1, (size_t)(close - t - 1)); expr[close - t - 1] = '\0';
            value[0] = '\0';
            if (!strcmp(expr, "epoch")) snprintf(value, sizeof(value), "%lld", (long long)epoch);
            else if (!strncmp(expr, "sha1:", 5) || !strncmp(expr, "basic:", 6)) {
                char joined[MAX_VALUE * 2] = "", part[MAX_VALUE], *save = NULL, *name, copy[256]; size_t used = 0; int basic = expr[0] == 'b', count = 0;
                snprintf(copy, sizeof(copy), "%s", expr + (basic ? 6 : 5));
                for (name = strtok_r(copy, "+", &save); name; name = strtok_r(NULL, "+", &save)) {
                    if (!strcmp(name, "epoch")) snprintf(part, sizeof(part), "%lld", (long long)epoch);
                    else if (!registry_value(r, key, provider, name, part, sizeof(part))) { OPENSSL_cleanse(joined, sizeof(joined)); return 0; }
                    used += (size_t)snprintf(joined + used, sizeof(joined) - used, "%s%s", basic && count ? ":" : "", part);
                    OPENSSL_cleanse(part, sizeof(part));
                    count++;
                }
                if (basic) EVP_EncodeBlock((unsigned char *)value, (const unsigned char *)joined, (int)used);
                else { unsigned char digest[SHA_DIGEST_LENGTH]; int i; SHA1((const unsigned char *)joined, used, digest); for (i = 0; i < SHA_DIGEST_LENGTH; i++) snprintf(value + 2 * i, sizeof(value) - 2 * (size_t)i, "%02x", digest[i]); }
                OPENSSL_cleanse(joined, sizeof(joined));
            } else if (!registry_value(r, key, provider, expr, value, sizeof(value))) return 0;
            (void)db;
            len = strlen(value);
            if (n + len >= cap) { OPENSSL_cleanse(value, sizeof(value)); return 0; }
            memcpy(out + n, value, len); n += len;
            OPENSSL_cleanse(value, sizeof(value));
            t = close + 1;
        }
    }
    out[n] = '\0';
    return !*t;
}

static void url_encode(const char *in, char *out, size_t cap) {
    static const char hex[] = "0123456789ABCDEF"; size_t n = 0;
    for (; *in && n + 4 < cap; in++) {
        unsigned char c = (unsigned char)*in;
        if (isalnum(c) || strchr("-._~", c)) out[n++] = (char)c;
        else { out[n++] = '%'; out[n++] = hex[c >> 4]; out[n++] = hex[c & 15]; }
    }
    out[n] = '\0';
}

/* The request a fetcher should send: the address with query credentials and
 * the header lines.  Missing required fields refuse (exit 4). */
/* A field's value: the sealed value, else the public default a provider
 * documents for anyone (api.data.gov's DEMO_KEY), else -- for {oauth_token} --
 * a client-credentials token exchanged with the provider and cached sealed
 * until shortly before it expires. */
static int oauth_token(Registry *r, const unsigned char key[32], const char *provider, char *out, size_t cap);

static int registry_value(Registry *r, const unsigned char key[32], const char *provider, const char *field, char *out, size_t cap) {
    const bf_json_doc_t *doc; bf_json_doc_t *owned; const bf_json_node_t *p, *f; int ok = 0;
    if (load_value(r->db, key, provider, field, out, cap)) return 1;
    if (!strcmp(field, "oauth_token")) return oauth_token(r, key, provider, out, cap);
    if ((p = find_provider(r, provider, &doc, &owned)) && (f = find_field(doc, p, field))) ok = string_of(doc, f, "public_default", out, cap);
    if (owned) bf_json_free(owned);
    return ok;
}

static int oauth_token(Registry *r, const unsigned char key[32], const char *provider, char *out, size_t cap) {
    const bf_json_doc_t *doc; bf_json_doc_t *owned; const bf_json_node_t *p = find_provider(r, provider, &doc, &owned), *oauth;
    char token_url[1024], id_field[64], secret_field[64], scope[256], client_id[MAX_VALUE], client_secret[MAX_VALUE];
    sqlite3_stmt *st = NULL; time_t now = time(NULL); int ok = 0;
    if (!p || !(oauth = bf_json_obj_get(doc, p, "oauth")) || !string_of(doc, oauth, "token_url", token_url, sizeof(token_url)) ||
        !string_of(doc, oauth, "client_id_field", id_field, sizeof(id_field)) || !string_of(doc, oauth, "client_secret_field", secret_field, sizeof(secret_field))) {
        if (owned) bf_json_free(owned);
        return 0;
    }
    string_of(doc, oauth, "scope", scope, sizeof(scope));
    if (owned) bf_json_free(owned);
    if (sqlite3_prepare_v2(r->db, "SELECT sealed FROM token_cache WHERE provider=? AND expires_at>? AND account=?", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, provider, -1, SQLITE_TRANSIENT); sqlite3_bind_int64(st, 2, (sqlite3_int64)now + 60);
        sqlite3_bind_text(st, 3, g_account, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) ok = unseal(key, provider, "oauth_token", sqlite3_column_blob(st, 0), sqlite3_column_bytes(st, 0), out, cap);
    }
    sqlite3_finalize(st);
    if (ok) return 1;
    if (!load_value(r->db, key, provider, id_field, client_id, sizeof(client_id)) || !load_value(r->db, key, provider, secret_field, client_secret, sizeof(client_secret))) return 0;
    {
        int in[2], outp[2]; pid_t pid; char answer[16384]; size_t used = 0; ssize_t n; FILE *cfg;
        if (pipe(in) || pipe(outp)) return 0;
        if ((pid = fork()) == 0) {
            dup2(in[0], 0); dup2(outp[1], 1); close(in[1]); close(outp[0]);
            { int devnull = open("/dev/null", O_WRONLY); if (devnull >= 0) dup2(devnull, 2); }
            execlp("curl", "curl", "--config", "-", NULL); _exit(127);
        }
        close(in[0]); close(outp[1]);
        cfg = fdopen(in[1], "w");
        fprintf(cfg, "silent\nmax-time = 30\nuser-agent = \"BonfyreProject/1.0 (+https://aurekai.work/)\"\nurl = \"%s\"\n", token_url);
        fprintf(cfg, "user = \"%s:%s\"\n", client_id, client_secret);
        fprintf(cfg, "data = \"grant_type=client_credentials%s%s\"\n", scope[0] ? "&scope=" : "", scope);
        fclose(cfg);
        OPENSSL_cleanse(client_id, sizeof(client_id)); OPENSSL_cleanse(client_secret, sizeof(client_secret));
        while (used + 1 < sizeof(answer) && (n = read(outp[0], answer + used, sizeof(answer) - used - 1)) > 0) used += (size_t)n;
        answer[used] = '\0'; close(outp[0]); waitpid(pid, NULL, 0);
        {
            char err[160]; bf_json_doc_t *tok = bf_json_parse_str(answer, err, sizeof(err)); char access[MAX_VALUE];
            const bf_json_node_t *root = tok ? bf_json_root(tok) : NULL, *exp = root ? bf_json_obj_get(tok, root, "expires_in") : NULL;
            if (root && string_of(tok, root, "access_token", access, sizeof(access))) {
                long ttl = exp ? (long)(exp->type == BF_JSON_INT ? exp->val.i : exp->type == BF_JSON_DOUBLE ? exp->val.d : 3600) : 3600;
                unsigned char *blob = NULL; int blob_len = 0;
                snprintf(out, cap, "%s", access);
                if (seal(key, provider, "oauth_token", access, &blob, &blob_len) &&
                    sqlite3_prepare_v2(r->db, "INSERT OR REPLACE INTO token_cache VALUES(?,?,?,?)", -1, &st, NULL) == SQLITE_OK) {
                    sqlite3_bind_text(st, 1, provider, -1, SQLITE_TRANSIENT); sqlite3_bind_blob(st, 2, blob, blob_len, SQLITE_TRANSIENT);
                    sqlite3_bind_int64(st, 3, (sqlite3_int64)now + (ttl > 120 ? ttl - 60 : ttl)); sqlite3_bind_text(st, 4, g_account, -1, SQLITE_STATIC);
                    sqlite3_step(st);
                }
                sqlite3_finalize(st); free(blob);
                OPENSSL_cleanse(access, sizeof(access));
                audit(r->db, "boundary", provider, "oauth_token", "token-exchange", "issued");
                ok = 1;
            } else audit(r->db, "boundary", provider, "oauth_token", "token-exchange", "refused");
            OPENSSL_cleanse(answer, sizeof(answer));
            if (tok) bf_json_free(tok);
        }
    }
    return ok;
}

/* A header or query item whose template names only fields that are optional
 * and unset is left out, rather than refusing the whole request. */
static int template_optional_unset(Registry *r, const unsigned char key[32], const bf_json_doc_t *doc, const bf_json_node_t *p, const char *provider, const char *tmpl) {
    const char *t = tmpl; int optional_missing = 0;
    while ((t = strchr(t, '{'))) {
        const char *close = strchr(t, '}'); char name[64], probe[MAX_VALUE]; const bf_json_node_t *f;
        if (!close || close - t - 1 <= 0 || close - t - 1 >= (long)sizeof(name)) return 0;
        memcpy(name, t + 1, (size_t)(close - t - 1)); name[close - t - 1] = '\0';
        t = close + 1;
        if (!strcmp(name, "epoch") || strchr(name, ':')) continue;
        if ((f = find_field(doc, p, name))) {
            const bf_json_node_t *req = bf_json_obj_get(doc, f, "required");
            if ((!req || !bf_json_get_bool(req)) && !load_value(r->db, key, provider, name, probe, sizeof(probe))) optional_missing = 1;
            OPENSSL_cleanse(probe, sizeof(probe));
        }
    }
    return optional_missing;
}

static int apply_provider(Registry *r, const unsigned char key[32], const char *provider, const char *url, FILE *out, int as_curl_config) {
    const bf_json_doc_t *doc; bf_json_doc_t *owned; const bf_json_node_t *p = find_provider(r, provider, &doc, &owned), *apply, *list, *h;
    char keyed[8192], name[128], tmpl[1024], value[MAX_VALUE * 2]; time_t epoch = time(NULL); size_t used;
    if (!p) { fprintf(stderr, "credentials: unknown provider\n"); return 2; }
    {
        const bf_json_node_t *fields = bf_json_obj_get(doc, p, "fields"), *f;
        for (f = fields ? bf_json_child_first(doc, fields) : NULL; f; f = bf_json_child_next(doc, f)) {
            const bf_json_node_t *req = bf_json_obj_get(doc, f, "required"); char field[64], probe[MAX_VALUE];
            if (req && bf_json_get_bool(req) && string_of(doc, f, "field", field, sizeof(field)) && !registry_value(r, key, provider, field, probe, sizeof(probe))) {
                fprintf(stderr, "credentials: %s needs %s\n", provider, field); if (owned) bf_json_free(owned); return 4;
            }
            OPENSSL_cleanse(probe, sizeof(probe));
        }
    }
    apply = bf_json_obj_get(doc, p, "apply");
    snprintf(keyed, sizeof(keyed), "%s", url);
    used = strlen(keyed);
    list = apply ? bf_json_obj_get(doc, apply, "query") : NULL;
    for (h = list ? bf_json_child_first(doc, list) : NULL; h; h = bf_json_child_next(doc, h)) {
        char encoded[MAX_VALUE * 3];
        if (!string_of(doc, h, "name", name, sizeof(name)) || !string_of(doc, h, "value", tmpl, sizeof(tmpl))) { if (owned) bf_json_free(owned); return 4; }
        if (template_optional_unset(r, key, doc, p, provider, tmpl)) continue;
        if (!render(r, key, provider, tmpl, epoch, value, sizeof(value))) { if (owned) bf_json_free(owned); return 4; }
        url_encode(value, encoded, sizeof(encoded));
        used += (size_t)snprintf(keyed + used, sizeof(keyed) - used, "%s%s=%s", strchr(keyed, '?') ? "&" : "?", name, encoded);
        OPENSSL_cleanse(value, sizeof(value)); OPENSSL_cleanse(encoded, sizeof(encoded));
        if (used >= sizeof(keyed)) { if (owned) bf_json_free(owned); return 4; }
    }
    if (as_curl_config) { fputs("url = \"", out); fputs(keyed, out); fputs("\"\n", out); }
    else { fputs("{\"url\":", out); jstr(out, keyed); fputs(",\"headers\":[", out); }
    list = apply ? bf_json_obj_get(doc, apply, "headers") : NULL;
    {
        int count = 0;
        for (h = list ? bf_json_child_first(doc, list) : NULL; h; h = bf_json_child_next(doc, h)) {
            if (!string_of(doc, h, "name", name, sizeof(name)) || !string_of(doc, h, "value", tmpl, sizeof(tmpl))) { if (owned) bf_json_free(owned); return 4; }
            if (template_optional_unset(r, key, doc, p, provider, tmpl)) continue;
            if (!render(r, key, provider, tmpl, epoch, value, sizeof(value))) { if (owned) bf_json_free(owned); return 4; }
            if (as_curl_config) fprintf(out, "header = \"%s: %s\"\n", name, value);
            else { char line[MAX_VALUE * 2 + 200]; snprintf(line, sizeof(line), "%s: %s", name, value); fputs(count++ ? "," : "", out); jstr(out, line); OPENSSL_cleanse(line, sizeof(line)); }
            OPENSSL_cleanse(value, sizeof(value));
        }
    }
    if (!as_curl_config) fputs("]}\n", out);
    OPENSSL_cleanse(keyed, sizeof(keyed));
    if (owned) bf_json_free(owned);
    return 0;
}

/* Verification sends the provider's own check through curl, with the address
 * and headers on curl's standard input so no value appears in argv. */
static int verify_provider(Registry *r, const unsigned char key[32], const char *provider, const char *actor, int *status_out) {
    const bf_json_doc_t *doc; bf_json_doc_t *owned; const bf_json_node_t *p = find_provider(r, provider, &doc, &owned), *v;
    char url[2048]; int expect = 200, in[2], out_pipe[2], status = 0, rc; pid_t pid; char text[16] = ""; ssize_t n;
    FILE *cfg;
    if (!p || !(v = bf_json_obj_get(doc, p, "verify")) || !string_of(doc, v, "url", url, sizeof(url))) { if (owned) bf_json_free(owned); return 2; }
    if (bf_json_obj_get(doc, v, "expect")) expect = (int)bf_json_get_int(bf_json_obj_get(doc, v, "expect"));
    if (pipe(in) || pipe(out_pipe)) { if (owned) bf_json_free(owned); return 1; }
    if ((pid = fork()) == 0) {
        dup2(in[0], 0); dup2(out_pipe[1], 1); close(in[1]); close(out_pipe[0]);
        { int devnull = open("/dev/null", O_WRONLY); if (devnull >= 0) dup2(devnull, 2); }
        execlp("curl", "curl", "--config", "-", NULL);
        _exit(127);
    }
    close(in[0]); close(out_pipe[1]);
    cfg = fdopen(in[1], "w");
    fputs("silent\nmax-time = 30\noutput = \"/dev/null\"\nwrite-out = \"%{http_code}\"\nuser-agent = \"BonfyreProject/1.0\"\n", cfg);
    rc = apply_provider(r, key, provider, url, cfg, 1);
    fclose(cfg);
    n = read(out_pipe[0], text, sizeof(text) - 1); if (n > 0) text[n] = '\0';
    close(out_pipe[0]);
    waitpid(pid, NULL, 0);
    if (owned) bf_json_free(owned);
    if (rc) { audit(r->db, actor, provider, "", "verify", "missing required field"); return rc; }
    status = atoi(text);
    *status_out = status;
    {
        sqlite3_stmt *st = NULL; char at[32];
        now_text(at, sizeof(at));
        if (sqlite3_prepare_v2(r->db, "INSERT INTO verification VALUES(?,?,?,?,?)", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, provider, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, at, -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 3, status); sqlite3_bind_text(st, 4, status == expect ? "accepted" : "refused", -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 5, g_account, -1, SQLITE_STATIC);
            sqlite3_step(st);
        }
        sqlite3_finalize(st);
    }
    audit(r->db, actor, provider, "", "verify", status == expect ? "accepted" : "refused");
    return status == expect ? 0 : 5;
}

/* ------------------------------------------------------------ OpenAPI */

typedef struct { char *s; size_t n, cap; } Buf;
static void buf_add(Buf *b, const char *fmt, ...) {
    va_list ap; int need;
    va_start(ap, fmt); need = vsnprintf(NULL, 0, fmt, ap); va_end(ap);
    if (need < 0) return;
    if (b->n + (size_t)need + 1 > b->cap) { size_t cap = (b->cap + (size_t)need + 1) * 2; char *s = realloc(b->s, cap); if (!s) return; b->s = s; b->cap = cap; }
    va_start(ap, fmt); vsnprintf(b->s + b->n, b->cap - b->n, fmt, ap); va_end(ap);
    b->n += (size_t)need;
}
static void buf_json(Buf *b, const char *s) {
    buf_add(b, "\"");
    for (; s && *s; s++) {
        unsigned char c = (unsigned char)*s;
        if (c == '"' || c == '\\') buf_add(b, "\\%c", c); else if (c < 0x20) buf_add(b, "\\u%04x", c); else buf_add(b, "%c", c);
    }
    buf_add(b, "\"");
}

/* OpenAPI sometimes models payment-carrier challenges as apiKey security
 * schemes.  They are requirements for economic settlement, not credentials
 * the credential boundary should store or apply.  Keep the full OpenAPI spec
 * as ForeignTwin evidence and let the finance/protocol owners lower these
 * requirements later. */
/* L402 Authorization is not filtered here: it is an authorization token
 * applied to later requests, even when acquired through an economic flow. */
static int payment_security_scheme(const char *key, const char *pname) {
    char k[160] = "", h[160] = ""; size_t i;
    if (key) { snprintf(k, sizeof(k), "%s", key); for (i = 0; k[i]; i++) k[i] = (char)tolower((unsigned char)k[i]); }
    if (pname) { snprintf(h, sizeof(h), "%s", pname); for (i = 0; h[i]; i++) h[i] = (char)tolower((unsigned char)h[i]); }
    if (strstr(k, "x402")) return 1;
    if (!strcmp(h, "payment-signature") || !strcmp(h, "x-payment") ||
        !strcmp(h, "x-payment-response") || !strcmp(h, "payment-required") ||
        !strcmp(h, "x402-payment")) return 1;
    return 0;
}

/* Reads components.securitySchemes (OpenAPI 3) or securityDefinitions
 * (Swagger 2) and declares the fields each scheme needs and how to apply them.
 * OAuth2 and OpenID Connect schemes declare client credentials and record the
 * token or discovery address; exchanging them for a token is not done here. */
static int import_openapi(sqlite3 *db, const char *provider, const char *name, const char *spec_text, const char *source, const char *actor) {
    char err[200], title[256] = "", server[512] = ""; bf_json_doc_t *doc = bf_json_parse_str(spec_text, err, sizeof(err));
    const bf_json_node_t *root, *schemes = NULL, *s, *info, *servers; Buf out = {0}, fields = {0}, headers = {0}, query = {0}, notes = {0};
    int nfields = 0, nheaders = 0, nquery = 0, nnotes = 0, ok, oauth_declared = 0;
    char oauth_block[1400] = "";
    if (!doc || !(root = bf_json_root(doc))) { fprintf(stderr, "credentials: not a JSON OpenAPI document: %s\n", err); return 0; }
    { const bf_json_node_t *components = bf_json_obj_get(doc, root, "components"); if (components) schemes = bf_json_obj_get(doc, components, "securitySchemes"); }
    if (!schemes) schemes = bf_json_obj_get(doc, root, "securityDefinitions");
    if ((info = bf_json_obj_get(doc, root, "info"))) string_of(doc, info, "title", title, sizeof(title));
    if ((servers = bf_json_obj_get(doc, root, "servers")) && servers->type == BF_JSON_ARRAY && bf_json_child_first(doc, servers)) string_of(doc, bf_json_child_first(doc, servers), "url", server, sizeof(server));
    if (!schemes || schemes->type != BF_JSON_OBJECT) { fprintf(stderr, "credentials: the document declares no security schemes\n"); bf_json_free(doc); return 0; }
    for (s = bf_json_child_first(doc, schemes); s; s = bf_json_child_next(doc, s)) {
        char type[32], in[16], pname[128], scheme[32], field[64], url[512], keybuf[64];
        const char *key = keybuf;
        snprintf(keybuf, sizeof(keybuf), "%.*s", s->key && s->key_len > 0 ? (s->key_len < 63 ? s->key_len : 63) : 10, s->key && s->key_len > 0 ? s->key : "credential");
        scheme[0] = '\0';
        string_of(doc, s, "type", type, sizeof(type));
        snprintf(field, sizeof(field), "%s", key);
        for (char *c = field; *c; c++) *c = isalnum((unsigned char)*c) ? (char)tolower((unsigned char)*c) : '_';
        if (!strcmp(type, "apiKey") && string_of(doc, s, "in", in, sizeof(in)) && string_of(doc, s, "name", pname, sizeof(pname))) {
            if (payment_security_scheme(key, pname)) {
                buf_add(&notes, "%s", nnotes++ ? "; " : "");
                buf_add(&notes, "%s (%s) is an economic/payment requirement and is not imported as a credential", key ? key : "payment", pname);
                continue;
            }
            buf_add(&fields, "%s{\"field\":", nfields++ ? "," : ""); buf_json(&fields, field);
            buf_add(&fields, ",\"label\":"); { char label[256]; snprintf(label, sizeof(label), "%.60s (%.15s %.127s)", key ? key : "API key", in, pname); buf_json(&fields, label); }
            buf_add(&fields, ",\"kind\":\"api_key\",\"sensitive\":true,\"required\":true,\"help\":\"Declared by the service's OpenAPI document.\"}");
            if (!strcmp(in, "header")) { buf_add(&headers, "%s{\"name\":", nheaders++ ? "," : ""); buf_json(&headers, pname); buf_add(&headers, ",\"value\":\"{%s}\"}", field); }
            else if (!strcmp(in, "query")) { buf_add(&query, "%s{\"name\":", nquery++ ? "," : ""); buf_json(&query, pname); buf_add(&query, ",\"value\":\"{%s}\"}", field); }
            else { buf_add(&notes, "%s", nnotes++ ? "; " : ""); buf_add(&notes, "%s is sent as a cookie, which is not applied automatically", pname); }
        } else if (!strcmp(type, "http") && string_of(doc, s, "scheme", scheme, sizeof(scheme)) && !strcasecmp(scheme, "bearer")) {
            buf_add(&fields, "%s{\"field\":", nfields++ ? "," : ""); buf_json(&fields, field);
            buf_add(&fields, ",\"label\":\"Bearer token\",\"kind\":\"token\",\"sensitive\":true,\"required\":true,\"help\":\"Declared by the service's OpenAPI document.\"}");
            buf_add(&headers, "%s{\"name\":\"Authorization\",\"value\":\"Bearer {%s}\"}", nheaders++ ? "," : "", field);
        } else if (!strcmp(type, "http") && !strcasecmp(scheme, "basic")) {
            buf_add(&fields, "%s{\"field\":\"%s_username\",\"label\":\"Username\",\"kind\":\"username\",\"sensitive\":false,\"required\":true,\"help\":\"Declared by the service's OpenAPI document.\"}", nfields++ ? "," : "", field);
            buf_add(&fields, ",{\"field\":\"%s_password\",\"label\":\"Password\",\"kind\":\"password\",\"sensitive\":true,\"required\":true,\"help\":\"Declared by the service's OpenAPI document.\"}", field);
            nfields++;
            buf_add(&headers, "%s{\"name\":\"Authorization\",\"value\":\"Basic {basic:%s_username+%s_password}\"}", nheaders++ ? "," : "", field, field);
        } else if (!strcmp(type, "oauth2") || !strcmp(type, "openIdConnect")) {
            buf_add(&fields, "%s{\"field\":\"%s_client_id\",\"label\":\"Client ID\",\"kind\":\"client_id\",\"sensitive\":false,\"required\":true,\"help\":\"Issued when you register an application with the service.\"}", nfields++ ? "," : "", field);
            buf_add(&fields, ",{\"field\":\"%s_client_secret\",\"label\":\"Client secret\",\"kind\":\"client_secret\",\"sensitive\":true,\"required\":true,\"help\":\"Issued with the client ID.\"}", field);
            nfields++;
            url[0] = '\0';
            if (!strcmp(type, "openIdConnect")) string_of(doc, s, "openIdConnectUrl", url, sizeof(url));
            else {
                const bf_json_node_t *flows = bf_json_obj_get(doc, s, "flows"), *flow;
                for (flow = flows ? bf_json_child_first(doc, flows) : NULL; flow && !url[0]; flow = bf_json_child_next(doc, flow)) string_of(doc, flow, "tokenUrl", url, sizeof(url));
                if (!url[0]) string_of(doc, s, "tokenUrl", url, sizeof(url));
            }
            if (!strcmp(type, "oauth2") && url[0] && !strncmp(url, "https://", 8) && !oauth_declared) {
                /* A client-credentials flow is exchanged by the boundary itself. */
                snprintf(oauth_block, sizeof(oauth_block), ",\"oauth\":{\"token_url\":\"%s\",\"client_id_field\":\"%s_client_id\",\"client_secret_field\":\"%s_client_secret\"}", url, field, field);
                buf_add(&headers, "%s{\"name\":\"Authorization\",\"value\":\"Bearer {oauth_token}\"}", nheaders++ ? "," : "");
                oauth_declared = 1;
            } else {
                buf_add(&notes, "%s", nnotes++ ? "; " : "");
                buf_add(&notes, "%s needs a token from %s, which this boundary does not obtain for this scheme", key ? key : "OAuth", url[0] ? url : "the service's discovery address");
            }
        }
    }
    if (!nfields) { fprintf(stderr, "credentials: no scheme this boundary understands\n"); free(fields.s); free(headers.s); free(query.s); free(notes.s); bf_json_free(doc); return 0; }
    buf_add(&out, "{\"provider\":"); buf_json(&out, provider);
    buf_add(&out, ",\"name\":"); buf_json(&out, name && *name ? name : title[0] ? title : provider);
    buf_add(&out, ",\"purpose\":"); { char purpose[1024]; snprintf(purpose, sizeof(purpose), "A service described by its OpenAPI document%s%s.%s%s", server[0] ? " at " : "", server, nnotes ? " " : "", notes.s ? notes.s : ""); buf_json(&out, purpose); }
    buf_add(&out, ",\"get_it\":"); buf_json(&out, server[0] && !strncmp(server, "https://", 8) ? server : "https://example.invalid/");
    buf_add(&out, ",\"fields\":[%s],\"apply\":{\"headers\":[%s],\"query\":[%s]}%s,\"origin\":\"openapi\",\"spec_source\":", fields.s ? fields.s : "", headers.s ? headers.s : "", query.s ? query.s : "", oauth_block);
    buf_json(&out, source);
    buf_add(&out, ",\"cannot_infer\":\"a declared security scheme says what a service asks for, not that it is safe, lawful or worth reading\"}");
    {
        sqlite3_stmt *st = NULL; char at[32];
        now_text(at, sizeof(at));
        ok = sqlite3_prepare_v2(db, "INSERT INTO imported_provider VALUES(?,?,?,?) ON CONFLICT(provider) DO UPDATE SET declaration=excluded.declaration,spec_source=excluded.spec_source,imported_at=excluded.imported_at", -1, &st, NULL) == SQLITE_OK;
        if (ok) {
            sqlite3_bind_text(st, 1, provider, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, out.s, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 3, source, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 4, at, -1, SQLITE_TRANSIENT);
            ok = sqlite3_step(st) == SQLITE_DONE;
        }
        sqlite3_finalize(st);
    }
    audit(db, actor, provider, "", "import-openapi", ok ? "declared" : "failed");
    if (ok) printf("%s\n", out.s);
    free(out.s); free(fields.s); free(headers.s); free(query.s); free(notes.s); bf_json_free(doc);
    return ok;
}

/* ------------------------------------------------------------ status */

static void status_json(Registry *r, FILE *out) {
    const bf_json_node_t *root = r->declared ? bf_json_root(r->declared) : NULL, *p;
    int first = 1; sqlite3_stmt *imported = NULL;
    fputs("{\"account\":", out); jstr(out, g_account); fputs(",\"providers\":[", out);
    for (int pass = 0; pass < 2; pass++) {
        bf_json_doc_t *owned = NULL; const bf_json_doc_t *doc = r->declared;
        if (pass == 1 && sqlite3_prepare_v2(r->db, "SELECT declaration FROM imported_provider ORDER BY provider", -1, &imported, NULL) != SQLITE_OK) break;
        p = pass == 0 && root ? bf_json_child_first(r->declared, root) : NULL;
        for (;;) {
            char id[128], name[256], purpose[1024], get_it[512];
            const bf_json_node_t *fields, *f; int missing = 0;
            if (pass == 1) {
                char err[160];
                if (owned) { bf_json_free(owned); owned = NULL; }
                if (sqlite3_step(imported) != SQLITE_ROW) break;
                owned = bf_json_parse_str((const char *)sqlite3_column_text(imported, 0), err, sizeof(err));
                if (!owned) continue;
                doc = owned; p = bf_json_root(owned);
            } else if (!p) break;
            string_of(doc, p, "provider", id, sizeof(id)); string_of(doc, p, "name", name, sizeof(name));
            string_of(doc, p, "purpose", purpose, sizeof(purpose)); string_of(doc, p, "get_it", get_it, sizeof(get_it));
            fprintf(out, "%s{\"provider\":", first ? "" : ","); jstr(out, id); first = 0;
            fputs(",\"name\":", out); jstr(out, name); fputs(",\"purpose\":", out); jstr(out, purpose); fputs(",\"get_it\":", out); jstr(out, get_it);
            fprintf(out, ",\"origin\":\"%s\",\"fields\":[", pass ? "openapi" : "declared");
            fields = bf_json_obj_get(doc, p, "fields");
            for (f = fields ? bf_json_child_first(doc, fields) : NULL; f; f = bf_json_child_next(doc, f)) {
                char field[64], label[256], kind[32], generator[32]; sqlite3_stmt *st = NULL; int set = 0;
                const bf_json_node_t *req = bf_json_obj_get(doc, f, "required"), *sens = bf_json_obj_get(doc, f, "sensitive");
                string_of(doc, f, "field", field, sizeof(field)); string_of(doc, f, "label", label, sizeof(label));
                string_of(doc, f, "kind", kind, sizeof(kind)); string_of(doc, f, "generator", generator, sizeof(generator));
                fprintf(out, "%s{\"field\":", f == bf_json_child_first(doc, fields) ? "" : ","); jstr(out, field);
                fputs(",\"label\":", out); jstr(out, label); fputs(",\"kind\":", out); jstr(out, kind);
                fprintf(out, ",\"required\":%s,\"sensitive\":%s,\"generator\":", req && bf_json_get_bool(req) ? "true" : "false", sens && bf_json_get_bool(sens) ? "true" : "false");
                jstr(out, generator);
                if (sqlite3_prepare_v2(r->db, "SELECT last4,generated,set_by,set_at FROM sealed_field WHERE provider=? AND field=? AND account=?", -1, &st, NULL) == SQLITE_OK) {
                    sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, field, -1, SQLITE_TRANSIENT);
                    sqlite3_bind_text(st, 3, g_account, -1, SQLITE_STATIC);
                    if (sqlite3_step(st) == SQLITE_ROW) {
                        set = 1;
                        fprintf(out, ",\"state\":\"%s\",\"shows\":", sqlite3_column_int(st, 1) ? "generated" : "set"); jstr(out, (const char *)sqlite3_column_text(st, 0));
                        fputs(",\"set_by\":", out); jstr(out, (const char *)sqlite3_column_text(st, 2)); fputs(",\"set_at\":", out); jstr(out, (const char *)sqlite3_column_text(st, 3));
                    }
                }
                sqlite3_finalize(st);
                if (!set) {
                    char def[64];
                    if (string_of(doc, f, "public_default", def, sizeof(def))) fputs(",\"state\":\"public default\"", out);
                    else { fputs(",\"state\":\"missing\"", out); if (req && bf_json_get_bool(req)) missing++; }
                }
                fputs("}", out);
            }
            fprintf(out, "],\"ready\":%s", missing ? "false" : "true");
            {
                sqlite3_stmt *st = NULL;
                if (sqlite3_prepare_v2(r->db, "SELECT at,http_status,outcome FROM verification WHERE provider=? AND account=? ORDER BY rowid DESC LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
                    sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, g_account, -1, SQLITE_STATIC);
                    if (sqlite3_step(st) == SQLITE_ROW) {
                        fputs(",\"verified\":{\"at\":", out); jstr(out, (const char *)sqlite3_column_text(st, 0));
                        fprintf(out, ",\"http_status\":%d,\"outcome\":", sqlite3_column_int(st, 1)); jstr(out, (const char *)sqlite3_column_text(st, 2)); fputs("}", out);
                    }
                }
                sqlite3_finalize(st);
            }
            fputs("}", out);
            if (pass == 0) p = bf_json_child_next(r->declared, p);
        }
        if (owned) bf_json_free(owned);
        if (imported) { sqlite3_finalize(imported); imported = NULL; }
    }
    fputs("],\"cannot_infer\":\"a stored credential is possession, never authority, admission, spend approval or an effect right\"}\n", out);
}

/* ------------------------------------------------------------ operations */

/* Ready when every required field has a sealed value or a public default. */
static int provider_ready(Registry *r, const unsigned char key[32], const char *provider) {
    const bf_json_doc_t *doc; bf_json_doc_t *owned; const bf_json_node_t *p = find_provider(r, provider, &doc, &owned), *fields, *f; int ready = p != NULL;
    for (fields = p ? bf_json_obj_get(doc, p, "fields") : NULL, f = fields ? bf_json_child_first(doc, fields) : NULL; f && ready; f = bf_json_child_next(doc, f)) {
        const bf_json_node_t *req = bf_json_obj_get(doc, f, "required"); char field[64], probe[MAX_VALUE];
        if (req && bf_json_get_bool(req) && string_of(doc, f, "field", field, sizeof(field))) {
            char def[64];
            if (!load_value(r->db, key, provider, field, probe, sizeof(probe)) && !string_of(doc, f, "public_default", def, sizeof(def))) ready = 0;
            OPENSSL_cleanse(probe, sizeof(probe));
        }
    }
    if (owned) bf_json_free(owned);
    return ready;
}

/* When a provider becomes ready, whatever was waiting on it is told at once:
 * BONFYRE_CREDENTIALS_ON_READY names a program run with the provider id and
 * nothing else (no value ever leaves the boundary this way). */
static void on_ready(Registry *r, const unsigned char key[32], const char *provider) {
    const char *hook = getenv("BONFYRE_CREDENTIALS_ON_READY"); pid_t pid;
    if (!hook || !*hook || g_account[0] || !provider_ready(r, key, provider)) return;   /* consumers bind to the estate account only */
    if ((pid = fork()) == 0) {
        if (fork() == 0) {
            int devnull = open("/dev/null", O_RDWR);
            if (devnull >= 0) { dup2(devnull, 0); dup2(devnull, 1); dup2(devnull, 2); }
            setsid();
            execl(hook, hook, provider, (char *)NULL);
            _exit(127);
        }
        _exit(0);
    }
    if (pid > 0) waitpid(pid, NULL, 0);
    audit(r->db, "boundary", provider, "", "on-ready", "waiting reads told");
}

static int op_set(Registry *r, const unsigned char key[32], const char *provider, const char *field, const char *value, const char *actor) {
    const bf_json_doc_t *doc; bf_json_doc_t *owned; const bf_json_node_t *p = find_provider(r, provider, &doc, &owned), *f;
    int ok = 0, sensitive = 1;
    if (p && (f = find_field(doc, p, field))) {
        const bf_json_node_t *s = bf_json_obj_get(doc, f, "sensitive");
        sensitive = !s || bf_json_get_bool(s);
        ok = store_value(r->db, key, provider, field, value, 0, actor, sensitive);
    } else audit(r->db, actor, provider ? provider : "", field ? field : "", "set", "unknown provider or field");
    if (owned) bf_json_free(owned);
    if (ok) {
        sqlite3_stmt *st = NULL;   /* a changed client credential invalidates its exchanged token */
        if (sqlite3_prepare_v2(r->db, "DELETE FROM token_cache WHERE provider=? AND account=?", -1, &st, NULL) == SQLITE_OK) { sqlite3_bind_text(st, 1, provider, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, g_account, -1, SQLITE_STATIC); sqlite3_step(st); }
        sqlite3_finalize(st);
        on_ready(r, key, provider);
    }
    return ok;
}

static int op_generate(Registry *r, const unsigned char key[32], const char *provider, const char *field, const char *actor, char *revealed, size_t cap) {
    const bf_json_doc_t *doc; bf_json_doc_t *owned; const bf_json_node_t *p = find_provider(r, provider, &doc, &owned), *f;
    char generator[32]; int ok = 0;
    if (p && (f = find_field(doc, p, field)) && string_of(doc, f, "generator", generator, sizeof(generator)) && generate_value(generator, revealed, cap)) {
        const bf_json_node_t *s = bf_json_obj_get(doc, f, "sensitive");
        ok = store_value(r->db, key, provider, field, revealed, 1, actor, !s || bf_json_get_bool(s));
    }
    if (owned) bf_json_free(owned);
    if (!ok) OPENSSL_cleanse(revealed, cap);
    else on_ready(r, key, provider);
    return ok;
}

/* Everything the estate can make for itself is made once, on install. */
static int op_bootstrap(Registry *r, const unsigned char key[32], FILE *out) {
    const bf_json_node_t *root = r->declared ? bf_json_root(r->declared) : NULL, *p; int generated = 0, first = 1;
    fputs("{\"generated\":[", out);
    for (p = root ? bf_json_child_first(r->declared, root) : NULL; p; p = bf_json_child_next(r->declared, p)) {
        const bf_json_node_t *fields = bf_json_obj_get(r->declared, p, "fields"), *f; char provider[128];
        string_of(r->declared, p, "provider", provider, sizeof(provider));
        for (f = fields ? bf_json_child_first(r->declared, fields) : NULL; f; f = bf_json_child_next(r->declared, f)) {
            char field[64], generator[32], probe[MAX_VALUE], value[128];
            if (!string_of(r->declared, f, "generator", generator, sizeof(generator)) || !string_of(r->declared, f, "field", field, sizeof(field))) continue;
            if (load_value(r->db, key, provider, field, probe, sizeof(probe))) { OPENSSL_cleanse(probe, sizeof(probe)); continue; }
            if (op_generate(r, key, provider, field, "bootstrap", value, sizeof(value))) {
                fprintf(out, "%s{\"provider\":", first ? "" : ","); jstr(out, provider); fputs(",\"field\":", out); jstr(out, field); fputs("}", out);
                first = 0; generated++;
            }
            OPENSSL_cleanse(value, sizeof(value));
        }
    }
    fprintf(out, "],\"count\":%d,\"shown\":false}\n", generated);
    return 1;
}

/* Every ready provider with a declared check is checked; the page and MCP
 * status show the outcomes. */
static int op_verify_all(Registry *r, const unsigned char key[32], FILE *out) {
    const bf_json_node_t *root = r->declared ? bf_json_root(r->declared) : NULL, *p; int checked = 0, accepted = 0, first = 1;
    fputs("{\"checks\":[", out);
    for (p = root ? bf_json_child_first(r->declared, root) : NULL; p; p = bf_json_child_next(r->declared, p)) {
        char provider[128]; int status = 0, rc;
        string_of(r->declared, p, "provider", provider, sizeof(provider));
        {
            const bf_json_node_t *v = bf_json_obj_get(r->declared, p, "verify"); char url[16];
            if (!v || !string_of(r->declared, v, "url", url, sizeof(url)) || !provider_ready(r, key, provider)) continue;
        }
        rc = verify_provider(r, key, provider, "verify-all", &status);
        fprintf(out, "%s{\"provider\":", first ? "" : ","); jstr(out, provider);
        fprintf(out, ",\"http_status\":%d,\"outcome\":\"%s\"}", status, rc == 0 ? "accepted" : "refused");
        first = 0; checked++; accepted += rc == 0;
    }
    fprintf(out, "],\"checked\":%d,\"accepted\":%d}\n", checked, accepted);
    return 1;
}

static int op_clear(Registry *r, const char *provider, const char *field, const char *actor) {
    sqlite3_stmt *st = NULL; int ok;
    ok = sqlite3_prepare_v2(r->db, "DELETE FROM sealed_field WHERE provider=? AND field=? AND account=?", -1, &st, NULL) == SQLITE_OK;
    if (ok) { sqlite3_bind_text(st, 1, provider, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, field, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 3, g_account, -1, SQLITE_STATIC); ok = sqlite3_step(st) == SQLITE_DONE; }
    sqlite3_finalize(st);
    audit(r->db, actor, provider, field, "clear", ok ? "cleared" : "failed");
    return ok;
}

/* ------------------------------------------------------------ loopback page */

typedef struct {
    const char *auth_binary, *auth_db;
    int port;
    Registry *registry;
    unsigned char key[32];
} Server;

static int run_capture(char *const argv[], const char *input, char *out, size_t cap) {
    int in[2], outp[2]; pid_t pid; size_t used = 0; ssize_t n; int status = 0;
    if (pipe(in) || pipe(outp)) return -1;
    if ((pid = fork()) == 0) {
        dup2(in[0], 0); dup2(outp[1], 1); close(in[1]); close(outp[0]);
        { int devnull = open("/dev/null", O_WRONLY); if (devnull >= 0) dup2(devnull, 2); }
        execv(argv[0], argv); _exit(127);
    }
    close(in[0]); close(outp[1]);
    if (input) { ssize_t w = write(in[1], input, strlen(input)); (void)w; }
    close(in[1]);
    while (used + 1 < cap && (n = read(outp[0], out + used, cap - used - 1)) > 0) used += (size_t)n;
    out[used] = '\0';
    close(outp[0]);
    waitpid(pid, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

static int json_true(const char *text, const char *key) {
    char want[64]; const char *p;
    snprintf(want, sizeof(want), "\"%s\":", key);
    if (!(p = strstr(text, want))) return 0;
    p += strlen(want); while (*p == ' ') p++;
    return !strncmp(p, "true", 4);
}

/* A session is admitted only while BonfyreAuth says its holder is a Bonfyre admin. */
static int session_admin(Server *s, const char *token, char *actor, size_t cap) {
    char out[4096], id[32] = "";
    char *const verify[] = {(char *)s->auth_binary, "--db", (char *)s->auth_db, "verify", "--token", (char *)token, NULL};
    char *const authorize[] = {(char *)s->auth_binary, "--db", (char *)s->auth_db, "institution-authorize", "--token", (char *)token, "--institution", "bonfyre", "--role", "admin", NULL};
    const char *p;
    if (!token || strlen(token) < 16 || strspn(token, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_") != strlen(token)) return 0;
    if (run_capture(verify, NULL, out, sizeof(out)) != 0 || !json_true(out, "valid")) return 0;
    if ((p = strstr(out, "\"id\":"))) snprintf(id, sizeof(id), "%d", atoi(p + 5));
    if (run_capture(authorize, NULL, out, sizeof(out)) != 0 || !json_true(out, "authorized")) return 0;
    snprintf(actor, cap, "auth-user:%s", id);
    return 1;
}

static void csrf_for(Server *s, const char *token, char *out) {
    unsigned char mac[32]; unsigned int len = 0; int i;
    HMAC(EVP_sha256(), s->key, 32, (const unsigned char *)token, strlen(token), mac, &len);
    for (i = 0; i < 32; i++) snprintf(out + 2 * i, 3, "%02x", mac[i]);
}

static int form_value(const char *body, const char *name, char *out, size_t cap) {
    size_t nl = strlen(name); const char *p = body;
    out[0] = '\0';
    while (p && *p) {
        if (!strncmp(p, name, nl) && p[nl] == '=') {
            const char *v = p + nl + 1; size_t n = 0;
            while (*v && *v != '&' && n + 1 < cap) {
                if (*v == '+') { out[n++] = ' '; v++; }
                else if (*v == '%' && isxdigit((unsigned char)v[1]) && isxdigit((unsigned char)v[2])) { char hex[3] = {v[1], v[2], 0}; out[n++] = (char)strtol(hex, NULL, 16); v += 3; }
                else out[n++] = *v++;
            }
            out[n] = '\0';
            return 1;
        }
        p = strchr(p, '&'); if (p) p++;
    }
    return 0;
}

static void respond_headers(FILE *out, int code, const char *reason, const char *extra) {
    fprintf(out, "HTTP/1.1 %d %s\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: no-store\r\nX-Frame-Options: DENY\r\n"
                 "Content-Security-Policy: default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'\r\n"
                 "Referrer-Policy: no-referrer\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n%s\r\n", code, reason, extra ? extra : "");
}

static void page_start(FILE *out, const char *title) {
    fputs("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>", out);
    html(out, title);
    fputs("</title><style>"
          "body{margin:0;background:#101611;color:#e9eee6;font:15px/1.5 system-ui,-apple-system,sans-serif}"
          "main{max-width:62rem;margin:0 auto;padding:2rem 1.2rem 4rem}h1{font-size:1.7rem;margin:0 0 .3rem}"
          ".muted{color:#9fb09d}.note{border-left:3px solid #c5e47b;padding:.4rem .8rem;margin:1rem 0;color:#cfdccb}"
          "section.provider{border-top:1px solid #33422f;padding:1.2rem 0}section.provider h2{font-size:1.15rem;margin:0}"
          ".row{display:grid;grid-template-columns:minmax(0,14rem) minmax(0,1fr);gap:.4rem 1rem;align-items:center;margin:.5rem 0}"
          ".state{font:12px ui-monospace,monospace;color:#9fb09d}.state.set,.state.generated{color:#c5e47b}.state.missing{color:#ffb4a4}"
          "form.inline{display:flex;flex-wrap:wrap;gap:.4rem;margin:0}input,textarea{background:#0b100b;color:inherit;border:1px solid #4a5c47;border-radius:7px;padding:.45rem .6rem;font:inherit}"
          "input[type=password],input[type=text]{min-width:16rem;flex:1}textarea{width:100%;min-height:8rem;box-sizing:border-box}"
          "button{background:#c5e47b;color:#15200e;border:0;border-radius:7px;padding:.45rem .8rem;font-weight:600;cursor:pointer}button.quiet{background:transparent;color:#c5e47b;border:1px solid #4a5c47}"
          "a{color:#c5e47b}.reveal{font:13px ui-monospace,monospace;background:#0b100b;border:1px solid #c5e47b;padding:.6rem;word-break:break-all}"
          ".tell{font:13px ui-monospace,monospace}.ready{color:#c5e47b}.notready{color:#ffb4a4}"
          "</style></head><body><main>", out);
}

static void render_signin(FILE *out, const char *message) {
    page_start(out, "Credentials — sign in");
    fputs("<h1>Credentials</h1><p class=\"muted\">This page answers only on this machine's loopback address. Sign in with a Bonfyre admin account.</p>", out);
    if (message) { fputs("<p class=\"note\">", out); html(out, message); fputs("</p>", out); }
    fputs("<form method=\"post\" action=\"/login\"><div class=\"row\"><label>Email</label><input type=\"text\" name=\"email\" autocomplete=\"username\" required></div>"
          "<div class=\"row\"><label>Password</label><input type=\"password\" name=\"password\" autocomplete=\"current-password\" required></div><button>Sign in</button></form></main></body></html>", out);
}

static void render_page(Server *s, FILE *out, const char *csrf, const char *flash, const char *reveal_label, const char *reveal_value) {
    char *status_text; size_t len; FILE *mem = open_memstream(&status_text, &len); char err[160];
    bf_json_doc_t *doc; const bf_json_node_t *providers, *p;
    status_json(s->registry, mem); fclose(mem);
    doc = bf_json_parse_str(status_text, err, sizeof(err));
    page_start(out, "Credentials");
    fputs("<h1>Credentials</h1><p class=\"muted\">What each provider needs before Aurekai can read from it. Stored values are sealed and never shown again; a value the estate generates can be copied once, when it is made.</p>", out);
    if (flash) { fputs("<p class=\"note\">", out); html(out, flash); fputs("</p>", out); }
    if (reveal_value) {
        fputs("<p class=\"note\">Copy this now; it will not be shown again. ", out); html(out, reveal_label); fputs("</p><p class=\"reveal\">", out); html(out, reveal_value); fputs("</p>", out);
    }
    providers = doc ? bf_json_obj_get(doc, bf_json_root(doc), "providers") : NULL;
    for (p = providers ? bf_json_child_first(doc, providers) : NULL; p; p = bf_json_child_next(doc, p)) {
        char id[128], name[256], purpose[1024], get_it[512]; const bf_json_node_t *fields, *f, *verified, *ready;
        const bf_json_doc_t *decl_doc; bf_json_doc_t *owned; const bf_json_node_t *decl;
        string_of(doc, p, "provider", id, sizeof(id)); string_of(doc, p, "name", name, sizeof(name));
        string_of(doc, p, "purpose", purpose, sizeof(purpose)); string_of(doc, p, "get_it", get_it, sizeof(get_it));
        ready = bf_json_obj_get(doc, p, "ready");
        fputs("<section class=\"provider\" id=\"", out); html(out, id); fputs("\"><h2>", out); html(out, name);
        fputs(ready && bf_json_get_bool(ready) ? " <span class=\"ready\">· ready</span>" : " <span class=\"notready\">· needs something</span>", out);
        fputs("</h2><p class=\"muted\">", out); html(out, purpose); fputs(" <a rel=\"noopener noreferrer\" target=\"_blank\" href=\"", out); html(out, get_it); fputs("\">Where to get it</a></p>", out);
        decl = find_provider(s->registry, id, &decl_doc, &owned);
        if (decl) {
            const bf_json_node_t *tell = bf_json_obj_get(decl_doc, decl, "tell_them"), *t;
            for (t = tell ? bf_json_child_first(decl_doc, tell) : NULL; t; t = bf_json_child_next(decl_doc, t)) {
                char label[256], value[512];
                string_of(decl_doc, t, "label", label, sizeof(label)); string_of(decl_doc, t, "value", value, sizeof(value));
                fputs("<div class=\"row\"><span>", out); html(out, label); fputs("</span><span class=\"tell\">", out); html(out, value); fputs("</span></div>", out);
            }
        }
        if (owned) bf_json_free(owned);
        fields = bf_json_obj_get(doc, p, "fields");
        for (f = fields ? bf_json_child_first(doc, fields) : NULL; f; f = bf_json_child_next(doc, f)) {
            char field[64], label[256], state[16], shows[16], set_at[32], generator[32]; const bf_json_node_t *sens = bf_json_obj_get(doc, f, "sensitive");
            string_of(doc, f, "field", field, sizeof(field)); string_of(doc, f, "label", label, sizeof(label));
            string_of(doc, f, "state", state, sizeof(state)); string_of(doc, f, "shows", shows, sizeof(shows));
            string_of(doc, f, "set_at", set_at, sizeof(set_at)); string_of(doc, f, "generator", generator, sizeof(generator));
            fputs("<div class=\"row\"><span>", out); html(out, label); fputs("<br><span class=\"state ", out); html(out, state); fputs("\">", out);
            if (!strcmp(state, "missing")) fputs("not set", out);
            else { html(out, state); if (shows[0]) { fputs(sens && bf_json_get_bool(sens) ? " · ends in " : " · ", out); html(out, shows); } fputs(" · ", out); html(out, set_at); }
            fputs("</span></span><form class=\"inline\" method=\"post\" action=\"/set\"><input type=\"hidden\" name=\"csrf\" value=\"", out); html(out, csrf);
            fputs("\"><input type=\"hidden\" name=\"provider\" value=\"", out); html(out, id); fputs("\"><input type=\"hidden\" name=\"field\" value=\"", out); html(out, field);
            fputs(sens && bf_json_get_bool(sens) ? "\"><input type=\"password\" autocomplete=\"off\" name=\"value\" placeholder=\"Paste to replace\">" : "\"><input type=\"text\" autocomplete=\"off\" name=\"value\" placeholder=\"Paste to replace\">", out);
            fputs("<button>Save</button></form></div>", out);
            if (generator[0] || strcmp(state, "missing")) {
                fputs("<div class=\"row\"><span></span><span>", out);
                if (generator[0]) {
                    fputs("<form class=\"inline\" method=\"post\" action=\"/generate\"><input type=\"hidden\" name=\"csrf\" value=\"", out); html(out, csrf);
                    fputs("\"><input type=\"hidden\" name=\"provider\" value=\"", out); html(out, id); fputs("\"><input type=\"hidden\" name=\"field\" value=\"", out); html(out, field);
                    fputs(strcmp(state, "missing") ? "\"><button class=\"quiet\">Generate a new one</button></form> " : "\"><button class=\"quiet\">Generate it</button></form> ", out);
                }
                if (strcmp(state, "missing")) {
                    fputs("<form class=\"inline\" method=\"post\" action=\"/clear\"><input type=\"hidden\" name=\"csrf\" value=\"", out); html(out, csrf);
                    fputs("\"><input type=\"hidden\" name=\"provider\" value=\"", out); html(out, id); fputs("\"><input type=\"hidden\" name=\"field\" value=\"", out); html(out, field);
                    fputs("\"><button class=\"quiet\">Clear</button></form>", out);
                }
                fputs("</span></div>", out);
            }
        }
        verified = bf_json_obj_get(doc, p, "verified");
        fputs("<div class=\"row\"><span class=\"state\">", out);
        if (verified) {
            char at[32], outcome[16]; const bf_json_node_t *code = bf_json_obj_get(doc, verified, "http_status");
            string_of(doc, verified, "at", at, sizeof(at)); string_of(doc, verified, "outcome", outcome, sizeof(outcome));
            html(out, outcome); fprintf(out, " (%d) · ", code ? (int)bf_json_get_int(code) : 0); html(out, at);
        } else fputs("not checked yet", out);
        fputs("</span><form class=\"inline\" method=\"post\" action=\"/verify\"><input type=\"hidden\" name=\"csrf\" value=\"", out); html(out, csrf);
        fputs("\"><input type=\"hidden\" name=\"provider\" value=\"", out); html(out, id); fputs("\"><button class=\"quiet\">Check it works</button></form></div></section>", out);
    }
    fputs("<section class=\"provider\"><h2>Add a service from its OpenAPI document</h2><p class=\"muted\">Paste a JSON OpenAPI document. Its security schemes decide the fields: an API key in a header or query, a bearer token, basic credentials, or OAuth client credentials (whose token exchange is noted, not automated).</p>"
          "<form method=\"post\" action=\"/openapi\"><input type=\"hidden\" name=\"csrf\" value=\"", out);
    html(out, csrf);
    fputs("\"><div class=\"row\"><label>Short name (a-z, 0-9, -)</label><input type=\"text\" name=\"provider\" required></div><div class=\"row\"><label>Display name</label><input type=\"text\" name=\"name\"></div>"
          "<textarea name=\"spec\" placeholder=\"{ &quot;openapi&quot;: &quot;3.1.0&quot;, ... }\"></textarea><p><button>Read its security schemes</button></p></form></section>"
          "<form method=\"post\" action=\"/logout\"><input type=\"hidden\" name=\"csrf\" value=\"", out);
    html(out, csrf);
    fputs("\"><button class=\"quiet\">Sign out</button></form></main></body></html>", out);
    if (doc) bf_json_free(doc);
    free(status_text);
}

static int cookie_value(const char *headers, const char *name, char *out, size_t cap) {
    const char *c = strcasestr(headers, "\ncookie:"); char want[64];
    out[0] = '\0';
    if (!c) return 0;
    snprintf(want, sizeof(want), "%s=", name);
    for (c = strstr(c, want); c; c = strstr(c + 1, want)) {
        if (c[-1] == ' ' || c[-1] == ';' || c[-1] == ':') {
            size_t n = strcspn(c + strlen(want), ";\r\n");
            if (n >= cap) return 0;
            memcpy(out, c + strlen(want), n); out[n] = '\0';
            return 1;
        }
    }
    return 0;
}

static void handle(Server *s, int fd) {
    char *req = malloc(MAX_BODY + 8192); size_t used = 0; ssize_t n; FILE *out;
    char method[8] = "", path[256] = "", host[128] = "", token[256] = "", actor[64] = "", csrf[65] = "";
    const char *body = NULL, *header_end; long length = 0;
    if (!req) { close(fd); return; }
    while (used < MAX_BODY + 8190 && (n = read(fd, req + used, MAX_BODY + 8191 - used)) > 0) {
        used += (size_t)n; req[used] = '\0';
        if ((header_end = strstr(req, "\r\n\r\n"))) {
            const char *cl = strcasestr(req, "\ncontent-length:");
            length = cl && cl < header_end ? atol(cl + 16) : 0;
            if (length < 0 || length > MAX_BODY) break;
            if (used >= (size_t)(header_end - req) + 4 + (size_t)length) break;
        }
    }
    req[used] = '\0';
    out = fdopen(fd, "w");
    if (!out) { close(fd); free(req); return; }
    if (sscanf(req, "%7s %255s", method, path) != 2 || !(header_end = strstr(req, "\r\n\r\n"))) { respond_headers(out, 400, "Bad Request", NULL); goto done; }
    body = header_end + 4;
    {
        const char *h = strcasestr(req, "\nhost:");
        if (h) sscanf(h + 6, " %127[^\r\n]", host);
    }
    {
        /* DNS rebinding: only a loopback name reaches this page. */
        char a[64], b[64];
        snprintf(a, sizeof(a), "127.0.0.1:%d", s->port); snprintf(b, sizeof(b), "localhost:%d", s->port);
        if (strcmp(host, a) && strcmp(host, b)) { respond_headers(out, 421, "Misdirected Request", NULL); fputs("This page answers only on loopback.\n", out); goto done; }
    }
    if (!strcmp(method, "POST")) {
        const char *origin = strcasestr(req, "\norigin:");
        if (origin && origin < header_end) {
            char value[128] = "", a[64], b[64];
            sscanf(origin + 8, " %127[^\r\n]", value);
            snprintf(a, sizeof(a), "http://127.0.0.1:%d", s->port); snprintf(b, sizeof(b), "http://localhost:%d", s->port);
            if (strcmp(value, a) && strcmp(value, b)) { respond_headers(out, 403, "Forbidden", NULL); goto done; }
        }
    }
    if (!strcmp(method, "POST") && !strcmp(path, "/login")) {
        char email[256], password[512], json[1024], answer[8192], *p;
        form_value(body, "email", email, sizeof(email)); form_value(body, "password", password, sizeof(password));
        {
            FILE *mem; char *text; size_t len;
            mem = open_memstream(&text, &len);
            fputs("{\"email\":", mem); jstr(mem, email); fputs(",\"password\":", mem); jstr(mem, password); fputs("}", mem);
            fclose(mem);
            snprintf(json, sizeof(json), "%s", text);
            OPENSSL_cleanse(text, len); free(text);
        }
        OPENSSL_cleanse(password, sizeof(password));
        {
            char *const argv[] = {(char *)s->auth_binary, "--db", (char *)s->auth_db, "http-login", NULL};
            int rc = run_capture(argv, json, answer, sizeof(answer));
            OPENSSL_cleanse(json, sizeof(json));
            if (rc == 0 && (p = strstr(answer, "\"token\":\"")) && sscanf(p + 9, "%255[^\"]", token) == 1 && session_admin(s, token, actor, sizeof(actor))) {
                char cookie[512];
                snprintf(cookie, sizeof(cookie), "Set-Cookie: bonfyre_credentials=%s; HttpOnly; SameSite=Strict; Path=/; Max-Age=28800\r\nLocation: /\r\n", token);
                respond_headers(out, 303, "See Other", cookie);
                audit(s->registry->db, actor, "", "", "sign-in", "admitted");
            } else {
                respond_headers(out, 403, "Forbidden", NULL);
                render_signin(out, "That account is not a Bonfyre admin, or the sign-in failed.");
                audit(s->registry->db, "unknown", "", "", "sign-in", "refused");
            }
            OPENSSL_cleanse(answer, sizeof(answer));
        }
        goto done;
    }
    if (!cookie_value(req, "bonfyre_credentials", token, sizeof(token)) || !session_admin(s, token, actor, sizeof(actor))) {
        respond_headers(out, 200, "OK", NULL);
        render_signin(out, NULL);
        goto done;
    }
    csrf_for(s, token, csrf);
    if (!strcmp(method, "GET") && !strcmp(path, "/")) { respond_headers(out, 200, "OK", NULL); render_page(s, out, csrf, NULL, NULL, NULL); goto done; }
    if (strcmp(method, "POST")) { respond_headers(out, 405, "Method Not Allowed", NULL); goto done; }
    {
        char sent[65], provider[128], field[64];
        form_value(body, "csrf", sent, sizeof(sent));
        if (strlen(sent) != 64 || CRYPTO_memcmp(sent, csrf, 64)) { respond_headers(out, 403, "Forbidden", NULL); render_page(s, out, csrf, "That form had expired; nothing was changed.", NULL, NULL); goto done; }
        form_value(body, "provider", provider, sizeof(provider)); form_value(body, "field", field, sizeof(field));
        if (!strcmp(path, "/logout")) {
            respond_headers(out, 303, "See Other", "Set-Cookie: bonfyre_credentials=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0\r\nLocation: /\r\n");
            goto done;
        }
        if (!strcmp(path, "/openapi")) {
            char name[256], *spec = malloc(MAX_BODY);
            form_value(body, "name", name, sizeof(name));
            if (spec && valid_id(provider, 64) && form_value(body, "spec", spec, MAX_BODY) && import_openapi(s->registry->db, provider, name, spec, "pasted on the credentials page", actor)) {
                respond_headers(out, 200, "OK", NULL); render_page(s, out, csrf, "Its security schemes were read; the fields it needs are listed below.", NULL, NULL);
            } else { respond_headers(out, 400, "Bad Request", NULL); render_page(s, out, csrf, "That document could not be read as JSON OpenAPI with security schemes.", NULL, NULL); }
            free(spec);
            goto done;
        }
        if (!valid_id(provider, 64) || (strcmp(path, "/verify") && !valid_id(field, 64))) { respond_headers(out, 400, "Bad Request", NULL); render_page(s, out, csrf, "Unknown provider or field.", NULL, NULL); goto done; }
        if (!strcmp(path, "/set")) {
            char value[MAX_VALUE];
            form_value(body, "value", value, sizeof(value));
            { size_t l = strlen(value); while (l && isspace((unsigned char)value[l - 1])) value[--l] = '\0'; }
            respond_headers(out, 200, "OK", NULL);
            render_page(s, out, csrf, value[0] && op_set(s->registry, s->key, provider, field, value, actor) ? "Saved and sealed." : "Nothing was saved.", NULL, NULL);
            OPENSSL_cleanse(value, sizeof(value));
        } else if (!strcmp(path, "/generate")) {
            char value[128], label[256];
            respond_headers(out, 200, "OK", NULL);
            if (op_generate(s->registry, s->key, provider, field, actor, value, sizeof(value))) {
                snprintf(label, sizeof(label), "%s · %s", provider, field);
                render_page(s, out, csrf, "Generated and sealed.", label, value);
            } else render_page(s, out, csrf, "That field is not one the estate can generate.", NULL, NULL);
            OPENSSL_cleanse(value, sizeof(value));
        } else if (!strcmp(path, "/clear")) {
            respond_headers(out, 200, "OK", NULL);
            render_page(s, out, csrf, op_clear(s->registry, provider, field, actor) ? "Cleared." : "Nothing was cleared.", NULL, NULL);
        } else if (!strcmp(path, "/verify")) {
            int status = 0, rc = verify_provider(s->registry, s->key, provider, actor, &status);
            char flash[256];
            snprintf(flash, sizeof(flash), rc == 0 ? "It works: the provider answered %d." : rc == 4 ? "It needs its required fields first." : rc == 2 ? "This provider has no check declared." : "The provider refused it (answered %d).", status);
            respond_headers(out, 200, "OK", NULL);
            render_page(s, out, csrf, flash, NULL, NULL);
        } else respond_headers(out, 404, "Not Found", NULL);
    }
done:
    fflush(out);
    fclose(out);
    OPENSSL_cleanse(req, used);
    free(req);
}

static int serve(Server *s) {
    int fd = socket(AF_INET, SOCK_STREAM, 0), one = 1; struct sockaddr_in addr;
    if (fd < 0) return 0;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET; addr.sin_port = htons((uint16_t)s->port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);   /* loopback only, always */
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) || listen(fd, 16)) { perror("credentials: bind"); close(fd); return 0; }
    signal(SIGPIPE, SIG_IGN);
    signal(SIGCHLD, SIG_DFL);
    fprintf(stderr, "credentials page on http://127.0.0.1:%d/ (loopback only)\n", s->port);
    for (;;) {
        int client = accept(fd, NULL, NULL);
        if (client < 0) { if (errno == EINTR) continue; break; }
        { struct timeval tv = {15, 0}; setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); }
        handle(s, client);
    }
    close(fd);
    return 1;
}

/* ------------------------------------------------------------ main */

static void usage(void) {
    fputs("usage: bonfyre-credentials init-key --key PATH\n"
          "       every command except init-key and serve takes [--account A] to keep one account's values apart from another's\n"
          "       bonfyre-credentials status   --db DB --providers JSON\n"
          "       bonfyre-credentials set      --db DB --key PATH --providers JSON --provider P --field F [--actor A]   (value on stdin)\n"
          "       bonfyre-credentials generate --db DB --key PATH --providers JSON --provider P --field F [--actor A] [--reveal]\n"
          "       bonfyre-credentials clear    --db DB --providers JSON --provider P --field F [--actor A]\n"
          "       bonfyre-credentials apply    --db DB --key PATH --providers JSON --provider P --url URL          (for internal fetchers)\n"
          "       bonfyre-credentials verify   --db DB --key PATH --providers JSON --provider P [--actor A]\n"
          "       bonfyre-credentials openapi  --db DB --provider P --spec FILE [--name N] [--actor A]\n"
          "       bonfyre-credentials bootstrap --db DB --key PATH --providers JSON      (generate what the estate can make)\n"
          "       bonfyre-credentials verify-all --db DB --key PATH --providers JSON     (check every ready provider)\n"
          "       bonfyre-credentials ready --db DB --key PATH --providers JSON --provider P\n"
          "       bonfyre-credentials serve    --db DB --key PATH --providers JSON --auth-db AUTH --auth-binary BIN [--port 18910]\n", stderr);
}

/* ------------------------------------------------------------ GitHub App manifest flow
 * GitHub creates an app from a manifest the person approves on github.com, then
 * returns a one-time code. The code is exchanged here for the app's identity and
 * secrets, which are sealed at once; only the non-secret identity is printed.
 * Verification signs a ten-minute app JWT with the sealed key and asks GitHub
 * who the app is: a stored key that GitHub does not accept is reported, not kept
 * quiet. The API base is https://api.github.com unless a loopback test names one. */
static int gh_api_base(const char *api, char *out, size_t cap) {
    const char *base = api && *api ? api : "https://api.github.com";
    if (strncmp(base, "https://", 8) && strncmp(base, "http://127.0.0.1:", 17)) return 0;
    if (strchr(base, '"') || strchr(base, '\\') || strchr(base, '\n') || strlen(base) >= cap) return 0;
    snprintf(out, cap, "%s", base); return 1;
}
/* One curl call configured on stdin; answer body and HTTP status. */
static int gh_curl(const char *config, char *answer, size_t cap, int *status) {
    int in[2], outp[2]; pid_t pid; size_t used = 0; ssize_t n; char *nl;
    if (pipe(in) || pipe(outp)) return 0;
    if ((pid = fork()) == 0) {
        dup2(in[0], 0); dup2(outp[1], 1); close(in[1]); close(outp[0]);
        { int devnull = open("/dev/null", O_WRONLY); if (devnull >= 0) dup2(devnull, 2); }
        execlp("curl", "curl", "--config", "-", NULL); _exit(127);
    }
    close(in[0]); close(outp[1]);
    if (write(in[1], config, strlen(config)) < 0) { close(in[1]); close(outp[0]); waitpid(pid, NULL, 0); return 0; }
    close(in[1]);
    while (used + 1 < cap && (n = read(outp[0], answer + used, cap - used - 1)) > 0) used += (size_t)n;
    answer[used] = '\0'; close(outp[0]); waitpid(pid, NULL, 0);
    if (!(nl = strrchr(answer, '\n'))) return 0;
    *status = atoi(nl + 1); *nl = '\0';
    return 1;
}
static int gh_store(Registry *r, const unsigned char key[32], const char *actor, const char *field, const char *value, int sensitive) {
    return value && *value && store_value(r->db, key, "github-app", field, value, 0, actor, sensitive);
}
static int op_github_app_convert(Registry *r, const unsigned char key[32], const char *api, const char *actor, FILE *out) {
    char base[512], code[256], config[1024], err[160], answer[32768], slug[128], client_id[128], html_url[512], app_id[32];
    char *pem = NULL, *client_secret = NULL, *webhook = NULL; int status = 0, ok = 0; size_t n;
    bf_json_doc_t *doc = NULL; const bf_json_node_t *root, *id;
    if (!gh_api_base(api, base, sizeof(base))) { fputs("credentials: github api base must be https\n", stderr); return 0; }
    n = fread(code, 1, sizeof(code) - 1, stdin); code[n] = '\0';
    while (n && isspace((unsigned char)code[n - 1])) code[--n] = '\0';
    if (!n || strspn(code, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") != n) { fputs("credentials: give the one-time code on stdin\n", stderr); return 0; }
    snprintf(config, sizeof(config), "silent\nmax-time = 30\nrequest = \"POST\"\nheader = \"Accept: application/vnd.github+json\"\n"
             "user-agent = \"BonfyreProject/1.0 (+https://aurekai.work/)\"\nwrite-out = \"\\n%%{http_code}\"\nurl = \"%s/app-manifests/%s/conversions\"\n", base, code);
    OPENSSL_cleanse(code, sizeof(code));
    if (!gh_curl(config, answer, sizeof(answer), &status)) { audit(r->db, actor, "github-app", "", "convert", "no answer"); return 0; }
    OPENSSL_cleanse(config, sizeof(config));
    if (status != 201 || !(doc = bf_json_parse_str(answer, err, sizeof(err))) || !(root = bf_json_root(doc)) || !(id = bf_json_obj_get(doc, root, "id"))) {
        audit(r->db, actor, "github-app", "", "convert", "refused by github");
        fprintf(stderr, "credentials: GitHub did not convert the code (HTTP %d)\n", status);
        goto done;
    }
    snprintf(app_id, sizeof(app_id), "%lld", (long long)bf_json_get_int(id));
    pem = calloc(1, MAX_VALUE); client_secret = calloc(1, MAX_VALUE); webhook = calloc(1, MAX_VALUE);
    if (!pem || !client_secret || !webhook || !string_of(doc, root, "slug", slug, sizeof(slug)) || !string_of(doc, root, "client_id", client_id, sizeof(client_id)) ||
        !string_of(doc, root, "pem", pem, MAX_VALUE) || !string_of(doc, root, "client_secret", client_secret, MAX_VALUE)) {
        audit(r->db, actor, "github-app", "", "convert", "answer missing fields"); goto done;
    }
    string_of(doc, root, "webhook_secret", webhook, MAX_VALUE); string_of(doc, root, "html_url", html_url, sizeof(html_url));
    /* The JSON copy keeps escapes; a PEM is only a key once its line breaks are real. */
    { char *w = pem; for (const char *c = pem; *c; c++) { if (c[0] == '\\' && c[1] == 'n') { *w++ = '\n'; c++; } else if (c[0] == '\\' && c[1] == '/') { *w++ = '/'; c++; } else *w++ = *c; } *w = '\0'; }
    ok = gh_store(r, key, actor, "app_id", app_id, 0) && gh_store(r, key, actor, "slug", slug, 0) && gh_store(r, key, actor, "client_id", client_id, 0) &&
         gh_store(r, key, actor, "private_key", pem, 1) && gh_store(r, key, actor, "client_secret", client_secret, 1) &&
         (!webhook[0] || gh_store(r, key, actor, "webhook_secret", webhook, 1));
    audit(r->db, actor, "github-app", "", "convert", ok ? "stored" : "store failed");
    if (ok) fprintf(out, "{\"stored\":true,\"app_id\":%s,\"slug\":\"%s\",\"html_url\":\"%s\"}\n", app_id, slug, html_url);
done:
    OPENSSL_cleanse(answer, sizeof(answer));
    if (pem) { OPENSSL_cleanse(pem, MAX_VALUE); free(pem); }
    if (client_secret) { OPENSSL_cleanse(client_secret, MAX_VALUE); free(client_secret); }
    if (webhook) { OPENSSL_cleanse(webhook, MAX_VALUE); free(webhook); }
    if (doc) bf_json_free(doc);
    return ok;
}
static void b64url(const unsigned char *in, size_t n, char *out, size_t cap) {
    static const char t[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    size_t w = 0, i;
    for (i = 0; i + 2 < n && w + 5 < cap; i += 3) {
        unsigned v = (unsigned)in[i] << 16 | (unsigned)in[i + 1] << 8 | in[i + 2];
        out[w++] = t[v >> 18 & 63]; out[w++] = t[v >> 12 & 63]; out[w++] = t[v >> 6 & 63]; out[w++] = t[v & 63];
    }
    if (i < n && w + 4 < cap) {
        unsigned v = (unsigned)in[i] << 16 | (i + 1 < n ? (unsigned)in[i + 1] << 8 : 0);
        out[w++] = t[v >> 18 & 63]; out[w++] = t[v >> 12 & 63]; if (i + 1 < n) out[w++] = t[v >> 6 & 63];
    }
    out[w] = '\0';
}
/* An RS256 app JWT from the sealed key, valid ten minutes. */
static int gh_app_jwt(Registry *r, const unsigned char key[32], char *jwt, size_t cap, char app_id[32], char slug[128]) {
    char head[128], body[160], signing[512], sigtxt[512]; unsigned char sig[512]; size_t siglen = sizeof(sig); char *pem = calloc(1, MAX_VALUE);
    EVP_PKEY *pkey = NULL; EVP_MD_CTX *ctx = NULL; BIO *bio = NULL; int ok = 0; time_t now = time(NULL);
    if (!pem) return 0;
    if (!load_value(r->db, key, "github-app", "app_id", app_id, 32) || !load_value(r->db, key, "github-app", "private_key", pem, MAX_VALUE)) goto done;
    if (slug) load_value(r->db, key, "github-app", "slug", slug, 128);
    b64url((const unsigned char *)"{\"alg\":\"RS256\",\"typ\":\"JWT\"}", 27, head, sizeof(head));
    { char payload[128]; int pl = snprintf(payload, sizeof(payload), "{\"iat\":%lld,\"exp\":%lld,\"iss\":\"%s\"}", (long long)now - 60, (long long)now + 540, app_id);
      b64url((const unsigned char *)payload, (size_t)pl, body, sizeof(body)); }
    snprintf(signing, sizeof(signing), "%s.%s", head, body);
    if (!(bio = BIO_new_mem_buf(pem, -1)) || !(pkey = PEM_read_bio_PrivateKey(bio, NULL, NULL, NULL)) || !(ctx = EVP_MD_CTX_new()) ||
        EVP_DigestSignInit(ctx, NULL, EVP_sha256(), NULL, pkey) != 1 || EVP_DigestSign(ctx, sig, &siglen, (const unsigned char *)signing, strlen(signing)) != 1) goto done;
    b64url(sig, siglen, sigtxt, sizeof(sigtxt));
    ok = snprintf(jwt, cap, "%s.%s", signing, sigtxt) < (int)cap;
done:
    EVP_MD_CTX_free(ctx); EVP_PKEY_free(pkey); BIO_free(bio); OPENSSL_cleanse(pem, MAX_VALUE); free(pem);
    return ok;
}
/* Each customer who installed the app: GitHub account, its type and what it chose to share. */
static int op_github_app_installations(Registry *r, const unsigned char key[32], const char *api, const char *actor, FILE *out) {
    char base[512], jwt[1024], app_id[32], config[1600], err[160]; char *answer = malloc(262144); int status = 0, ok = 0, n = 0;
    bf_json_doc_t *doc = NULL; const bf_json_node_t *root;
    if (!answer || !gh_api_base(api, base, sizeof(base)) || !gh_app_jwt(r, key, jwt, sizeof(jwt), app_id, NULL)) { free(answer); fputs("credentials: no usable GitHub app is stored\n", stderr); return 0; }
    snprintf(config, sizeof(config), "silent\nmax-time = 30\nheader = \"Accept: application/vnd.github+json\"\nheader = \"Authorization: Bearer %s\"\n"
             "user-agent = \"BonfyreProject/1.0 (+https://aurekai.work/)\"\nwrite-out = \"\\n%%{http_code}\"\nurl = \"%s/app/installations?per_page=100\"\n", jwt, base);
    OPENSSL_cleanse(jwt, sizeof(jwt));
    if (gh_curl(config, answer, 262144, &status) && status == 200 && (doc = bf_json_parse_str(answer, err, sizeof(err))) && (root = bf_json_root(doc)) && root->type == BF_JSON_ARRAY) {
        fputs("{\"installations\":[", out);
        for (const bf_json_node_t *it = bf_json_child_first(doc, root); it; it = bf_json_child_next(doc, it)) {
            const bf_json_node_t *acct = bf_json_obj_get(doc, it, "account"), *id = bf_json_obj_get(doc, it, "id"); char login[128] = "", type[32] = "", sel[16] = "";
            string_of(doc, acct, "login", login, sizeof(login)); string_of(doc, acct, "type", type, sizeof(type)); string_of(doc, it, "repository_selection", sel, sizeof(sel));
            fprintf(out, "%s{\"id\":%lld,\"account\":\"%s\",\"type\":\"%s\",\"repositories\":\"%s\"}", n++ ? "," : "", id ? (long long)bf_json_get_int(id) : 0LL, login, type, sel);
        }
        fprintf(out, "],\"app_id\":%s}\n", app_id); ok = 1;
    } else fprintf(stderr, "credentials: GitHub did not list installations (HTTP %d)\n", status);
    OPENSSL_cleanse(config, sizeof(config));
    audit(r->db, actor, "github-app", "", "installations", ok ? "listed" : "refused");
    if (doc) bf_json_free(doc); free(answer);
    return ok;
}
/* Act for one customer: mint that installation's one-hour token (never printed),
 * read the repositories it reaches, and report what Aurekai may now operate. */
static int op_github_app_customer(Registry *r, const unsigned char key[32], const char *api, const char *installation, const char *actor, FILE *out) {
    char base[512], jwt[1024], app_id[32], config[4096], err[160], token[2048] = "", expires[40] = ""; char *answer = malloc(262144); int status = 0, ok = 0, count = 0;
    bf_json_doc_t *doc = NULL; const bf_json_node_t *root;
    if (!answer || !installation || strspn(installation, "0123456789") != strlen(installation) || !*installation || strlen(installation) > 20 ||
        !gh_api_base(api, base, sizeof(base)) || !gh_app_jwt(r, key, jwt, sizeof(jwt), app_id, NULL)) { free(answer); fputs("credentials: name one installation id of a stored app\n", stderr); return 0; }
    snprintf(config, sizeof(config), "silent\nmax-time = 30\nrequest = \"POST\"\nheader = \"Accept: application/vnd.github+json\"\nheader = \"Authorization: Bearer %s\"\n"
             "user-agent = \"BonfyreProject/1.0 (+https://aurekai.work/)\"\nwrite-out = \"\\n%%{http_code}\"\nurl = \"%s/app/installations/%s/access_tokens\"\n", jwt, base, installation);
    OPENSSL_cleanse(jwt, sizeof(jwt));
    if (!gh_curl(config, answer, 262144, &status) || status != 201 || !(doc = bf_json_parse_str(answer, err, sizeof(err))) || !(root = bf_json_root(doc)) ||
        !string_of(doc, root, "token", token, sizeof(token))) { fprintf(stderr, "credentials: GitHub did not issue a token for installation %s (HTTP %d)\n", installation, status); goto done; }
    string_of(doc, root, "expires_at", expires, sizeof(expires));
    bf_json_free(doc); doc = NULL; OPENSSL_cleanse(answer, 262144);
    snprintf(config, sizeof(config), "silent\nmax-time = 30\nheader = \"Accept: application/vnd.github+json\"\nheader = \"Authorization: Bearer %s\"\n"
             "user-agent = \"BonfyreProject/1.0 (+https://aurekai.work/)\"\nwrite-out = \"\\n%%{http_code}\"\nurl = \"%s/installation/repositories?per_page=100\"\n", token, base);
    OPENSSL_cleanse(token, sizeof(token));
    if (gh_curl(config, answer, 262144, &status) && status == 200 && (doc = bf_json_parse_str(answer, err, sizeof(err))) && (root = bf_json_root(doc))) {
        const bf_json_node_t *tc = bf_json_obj_get(doc, root, "total_count"), *repos = bf_json_obj_get(doc, root, "repositories"); int n = 0;
        count = tc ? (int)bf_json_get_int(tc) : 0;
        fprintf(out, "{\"installation\":%s,\"token_issued\":true,\"token_expires\":\"%s\",\"repositories\":%d,\"names\":[", installation, expires, count);
        for (const bf_json_node_t *it = repos ? bf_json_child_first(doc, repos) : NULL; it && n < 20; it = bf_json_child_next(doc, it)) {
            char name[200] = ""; string_of(doc, it, "full_name", name, sizeof(name)); fprintf(out, "%s\"%s\"", n++ ? "," : "", name);
        }
        fputs("]}\n", out); ok = 1;
    } else fprintf(stderr, "credentials: the installation token did not read repositories (HTTP %d)\n", status);
done:
    OPENSSL_cleanse(config, sizeof(config)); OPENSSL_cleanse(answer, 262144);
    audit(r->db, actor, "github-app", "", "customer", ok ? "token issued and used" : "refused");
    if (doc) bf_json_free(doc); free(answer);
    return ok;
}
static int op_github_app_verify(Registry *r, const unsigned char key[32], const char *api, const char *actor, FILE *out) {
    char base[512], app_id[32], slug[128], head[128], body[160], signing[512], jwt[1024], sigtxt[512], config[1600], answer[16384], err[160];
    unsigned char sig[512]; size_t siglen = sizeof(sig); char *pem = NULL; int status = 0, ok = 0; time_t now = time(NULL);
    EVP_PKEY *pkey = NULL; EVP_MD_CTX *ctx = NULL; BIO *bio = NULL; bf_json_doc_t *doc = NULL; const bf_json_node_t *root, *id;
    if (!gh_api_base(api, base, sizeof(base)) || !(pem = calloc(1, MAX_VALUE))) return 0;
    if (!load_value(r->db, key, "github-app", "app_id", app_id, sizeof(app_id)) || !load_value(r->db, key, "github-app", "private_key", pem, MAX_VALUE)) {
        fputs("credentials: no GitHub app is stored\n", stderr); goto done;
    }
    load_value(r->db, key, "github-app", "slug", slug, sizeof(slug));
    b64url((const unsigned char *)"{\"alg\":\"RS256\",\"typ\":\"JWT\"}", 27, head, sizeof(head));
    { char payload[128]; int pl = snprintf(payload, sizeof(payload), "{\"iat\":%lld,\"exp\":%lld,\"iss\":\"%s\"}", (long long)now - 60, (long long)now + 540, app_id);
      b64url((const unsigned char *)payload, (size_t)pl, body, sizeof(body)); }
    snprintf(signing, sizeof(signing), "%s.%s", head, body);
    if (!(bio = BIO_new_mem_buf(pem, -1)) || !(pkey = PEM_read_bio_PrivateKey(bio, NULL, NULL, NULL)) || !(ctx = EVP_MD_CTX_new()) ||
        EVP_DigestSignInit(ctx, NULL, EVP_sha256(), NULL, pkey) != 1 || EVP_DigestSign(ctx, sig, &siglen, (const unsigned char *)signing, strlen(signing)) != 1) {
        audit(r->db, actor, "github-app", "private_key", "verify", "key unusable"); fputs("credentials: the stored GitHub app key cannot sign\n", stderr); goto done;
    }
    b64url(sig, siglen, sigtxt, sizeof(sigtxt));
    snprintf(jwt, sizeof(jwt), "%s.%s", signing, sigtxt);
    snprintf(config, sizeof(config), "silent\nmax-time = 30\nheader = \"Accept: application/vnd.github+json\"\nheader = \"Authorization: Bearer %s\"\n"
             "user-agent = \"BonfyreProject/1.0 (+https://aurekai.work/)\"\nwrite-out = \"\\n%%{http_code}\"\nurl = \"%s/app\"\n", jwt, base);
    OPENSSL_cleanse(jwt, sizeof(jwt));
    if (gh_curl(config, answer, sizeof(answer), &status) && status == 200 && (doc = bf_json_parse_str(answer, err, sizeof(err))) &&
        (root = bf_json_root(doc)) && (id = bf_json_obj_get(doc, root, "id")) && bf_json_get_int(id) == atoll(app_id)) ok = 1;
    OPENSSL_cleanse(config, sizeof(config));
    audit(r->db, actor, "github-app", "", "verify", ok ? "accepted" : "refused");
    fprintf(out, "{\"verified\":%s,\"app_id\":%s,\"slug\":\"%s\",\"status\":%d}\n", ok ? "true" : "false", app_id, slug, status);
done:
    if (doc) bf_json_free(doc);
    EVP_MD_CTX_free(ctx); EVP_PKEY_free(pkey); BIO_free(bio);
    if (pem) { OPENSSL_cleanse(pem, MAX_VALUE); free(pem); }
    return ok;
}

int main(int argc, char **argv) {
    Registry reg = {0}; unsigned char key[32]; int rc = 1; const char *cmd = argc > 1 ? argv[1] : "";
    const char *providers_path = argc > 2 ? arg(argc, argv, "--providers") : NULL, *actor = argc > 2 ? arg(argc, argv, "--actor") : NULL;
    char *providers_text = NULL;
    umask(077);
    if (argc < 2) { usage(); return 2; }
    if (argc > 2 && arg(argc, argv, "--account")) {
        const char *a = arg(argc, argv, "--account"); size_t n = strlen(a);
        if (!n || n > 128 || strspn(a, "abcdefghijklmnopqrstuvwxyz0123456789-_.@+") != n) { fprintf(stderr, "credentials: --account takes lowercase letters, digits and - _ . @ +\n"); return 2; }
        if (!strcmp(cmd, "serve")) { fputs("credentials: the loopback page serves the estate account only\n", stderr); return 2; }
        g_account = a;
    }
    if (!strcmp(cmd, "init-key")) return init_key(arg(argc, argv, "--key")) ? 0 : 1;
    if (!(reg.db = open_store(arg(argc, argv, "--db")))) { usage(); return 2; }
    if (providers_path) {
        char err[160] = "";
        if (!(providers_text = read_file(providers_path, NULL))) { fprintf(stderr, "credentials: providers: %s: %s\n", providers_path, strerror(errno ? errno : EIO)); sqlite3_close(reg.db); return 2; }
        if (!(reg.declared = bf_json_parse_str(providers_text, err, sizeof(err)))) { fprintf(stderr, "credentials: providers: %s: %s\n", providers_path, err[0] ? err : "not valid JSON"); free(providers_text); sqlite3_close(reg.db); return 2; }
    }
    if (!actor) actor = "operator-cli";
    if (!strcmp(cmd, "status")) { status_json(&reg, stdout); rc = 0; goto out; }
    if (!strcmp(cmd, "clear")) { rc = arg(argc, argv, "--provider") && arg(argc, argv, "--field") && op_clear(&reg, arg(argc, argv, "--provider"), arg(argc, argv, "--field"), actor) ? 0 : 1; if (!rc) printf("{\"cleared\":true}\n"); goto out; }
    if (!strcmp(cmd, "openapi")) {
        char *spec = arg(argc, argv, "--spec") ? read_file(arg(argc, argv, "--spec"), NULL) : NULL;
        rc = spec && valid_id(arg(argc, argv, "--provider"), 64) && import_openapi(reg.db, arg(argc, argv, "--provider"), arg(argc, argv, "--name"), spec, arg(argc, argv, "--spec"), actor) ? 0 : 1;
        free(spec); goto out;
    }
    if (!load_key(arg(argc, argv, "--key"), key)) { fprintf(stderr, "credentials: key unreadable\n"); rc = 2; goto out; }
    if (!strcmp(cmd, "bootstrap")) { rc = op_bootstrap(&reg, key, stdout) ? 0 : 1; }
    else if (!strcmp(cmd, "verify-all")) { rc = op_verify_all(&reg, key, stdout) ? 0 : 1; }
    else if (!strcmp(cmd, "github-app-convert")) { rc = op_github_app_convert(&reg, key, arg(argc, argv, "--api"), actor, stdout) ? 0 : 1; }
    else if (!strcmp(cmd, "github-app-installations")) { rc = op_github_app_installations(&reg, key, arg(argc, argv, "--api"), actor, stdout) ? 0 : 1; }
    else if (!strcmp(cmd, "github-app-customer")) { rc = op_github_app_customer(&reg, key, arg(argc, argv, "--api"), arg(argc, argv, "--installation"), actor, stdout) ? 0 : 1; }
    else if (!strcmp(cmd, "github-app-verify")) { rc = op_github_app_verify(&reg, key, arg(argc, argv, "--api"), actor, stdout) ? 0 : 1; }
    else if (!strcmp(cmd, "ready") && arg(argc, argv, "--provider")) { int ready = provider_ready(&reg, key, arg(argc, argv, "--provider")); printf("{\"ready\":%s}\n", ready ? "true" : "false"); rc = ready ? 0 : 4; }
    else if (!strcmp(cmd, "set")) {
        char value[MAX_VALUE]; size_t n = fread(value, 1, sizeof(value) - 1, stdin);
        value[n] = '\0';
        while (n && isspace((unsigned char)value[n - 1])) value[--n] = '\0';
        rc = n && arg(argc, argv, "--provider") && arg(argc, argv, "--field") && op_set(&reg, key, arg(argc, argv, "--provider"), arg(argc, argv, "--field"), value, actor) ? 0 : 1;
        OPENSSL_cleanse(value, sizeof(value));
        if (!rc) printf("{\"stored\":true,\"shown_again\":false}\n");
    } else if (!strcmp(cmd, "generate")) {
        char value[128];
        rc = arg(argc, argv, "--provider") && arg(argc, argv, "--field") && op_generate(&reg, key, arg(argc, argv, "--provider"), arg(argc, argv, "--field"), actor, value, sizeof(value)) ? 0 : 1;
        if (!rc) { printf("{\"generated\":true"); if (flag(argc, argv, "--reveal")) { printf(",\"value_shown_once\":"); jstr(stdout, value); } printf("}\n"); }
        OPENSSL_cleanse(value, sizeof(value));
    } else if (!strcmp(cmd, "apply") && arg(argc, argv, "--provider") && arg(argc, argv, "--url")) {
        rc = apply_provider(&reg, key, arg(argc, argv, "--provider"), arg(argc, argv, "--url"), stdout, 0);
        if (!rc) audit(reg.db, actor, arg(argc, argv, "--provider"), "", "apply", "headers issued to a fetcher");
    } else if (!strcmp(cmd, "verify") && arg(argc, argv, "--provider")) {
        int status = 0;
        rc = verify_provider(&reg, key, arg(argc, argv, "--provider"), actor, &status);
        printf("{\"provider\":"); jstr(stdout, arg(argc, argv, "--provider")); printf(",\"http_status\":%d,\"outcome\":\"%s\"}\n", status, rc == 0 ? "accepted" : rc == 4 ? "missing required field" : rc == 2 ? "no check declared" : "refused");
    } else if (!strcmp(cmd, "serve") && arg(argc, argv, "--auth-db") && arg(argc, argv, "--auth-binary")) {
        Server s; memset(&s, 0, sizeof(s));
        s.auth_binary = arg(argc, argv, "--auth-binary"); s.auth_db = arg(argc, argv, "--auth-db");
        s.port = arg(argc, argv, "--port") ? atoi(arg(argc, argv, "--port")) : 18910;
        s.registry = &reg; memcpy(s.key, key, 32);
        rc = serve(&s) ? 0 : 1;
    } else { usage(); rc = 2; }
    OPENSSL_cleanse(key, sizeof(key));
out:
    if (reg.declared) bf_json_free(reg.declared);
    free(providers_text);
    sqlite3_close(reg.db);
    return rc;
}
