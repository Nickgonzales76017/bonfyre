#define _POSIX_C_SOURCE 200809L
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sqlite3.h>
#include "owner_resolution.h"
#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif

#define FIELD_COUNT 8
#define DEFAULT_LIMIT 40

static char inferred_root[1024];

static const char *root_dir(void) {
    const char *p = getenv("BONFYRE_EXPERIENCE_MAP_ROOT");
    return (p && *p) ? p : (inferred_root[0] ? inferred_root : "generated");
}

/* Never accept a guessed data location without a readable release manifest. */
static int select_corpus(const char *dir) {
    char file[1024];
    int n;
    if (!dir || !*dir) return 0;
    n = snprintf(file, sizeof(file), "%s/manifest.json", dir);
    if (n < 1 || (size_t)n >= sizeof(file) || access(file, R_OK) != 0) return 0;
    if (dir == inferred_root) return 1; /* Avoid copying a buffer onto itself. */
    n = snprintf(inferred_root, sizeof(inferred_root), "%s", dir);
    return n > 0 && (size_t)n < sizeof(inferred_root);
}

static int corpus_for_binary(const char *binary) {
    char location[1024];
    const char *slash;
    int n;
    if (!binary || !(slash = strrchr(binary, '/'))) return 0;
    n = snprintf(location, sizeof(location), "%.*s/generated", (int)(slash - binary), binary);
    if (n > 0 && (size_t)n < sizeof(location) && select_corpus(location)) return 1;
    n = snprintf(location, sizeof(location), "%.*s/../share/bonfyre/experience-map/current",
                 (int)(slash - binary), binary);
    return n > 0 && (size_t)n < sizeof(location) && select_corpus(location);
}

static void infer_root(const char *argv0) {
    const char *slash;
    size_t prefix;
    if (!argv0 || !(slash = strrchr(argv0, '/'))) return;
    prefix = (size_t)(slash - argv0);
    if (prefix + sizeof("/generated") > sizeof(inferred_root)) return;
    memcpy(inferred_root, argv0, prefix);
    memcpy(inferred_root + prefix, "/generated", sizeof("/generated"));
}

/* Locate the corpus from the actual executable or an explicit deployment.
 * Keep the old path inference for source-checkout binaries, but do not
 * retain a guessed non-existent directory when launched from an MCP host. */
static void infer_root_release(const char *argv0) {
    char binary[1024];
    const char *home, *chosen = getenv("BONFYRE_EXPERIENCE_MAP_ROOT");
    if (chosen && *chosen) return;
    infer_root(argv0);
    if (inferred_root[0] && select_corpus(inferred_root)) return;
    inferred_root[0] = 0;
    if (corpus_for_binary(argv0)) return;
#if defined(__APPLE__)
    {
        uint32_t n = (uint32_t)sizeof(binary);
        if (_NSGetExecutablePath(binary, &n) == 0 && corpus_for_binary(binary)) return;
    }
#else
    {
        ssize_t n = readlink("/proc/self/exe", binary, sizeof(binary) - 1);
        if (n > 0) { binary[n] = '\0'; if (corpus_for_binary(binary)) return; }
    }
#endif
    home = getenv("HOME");
    if (home && *home) {
        int n = snprintf(binary, sizeof(binary), "%s/.bonfyre/share/bonfyre/experience-map/current", home);
        if (n > 0 && (size_t)n < sizeof(binary) && select_corpus(binary)) return;
    }
    if (select_corpus("generated") || select_corpus("cmd/BonfyreExperienceMap/generated")) return;
}

static int path_for(char *out, size_t n, const char *file) {
    int rc = snprintf(out, n, "%s/%s", root_dir(), file);
    return rc >= 0 && (size_t)rc < n;
}

static int contains_ci(const char *s, const char *q) {
    size_t n = strlen(q);
    if (!n) return 1;
    for (; *s; s++) {
        size_t i = 0;
        while (i < n && s[i] &&
               tolower((unsigned char)s[i]) == tolower((unsigned char)q[i])) i++;
        if (i == n) return 1;
    }
    return 0;
}

static int has_token(const char *list, const char *q) {
    size_t n = strlen(q);
    const char *p = list;
    while (*p) {
        const char *e;
        while (*p == ' ') p++;
        e = strchr(p, ' ');
        if (!e) e = p + strlen(p);
        if ((size_t)(e - p) == n && strncmp(p, q, n) == 0) return 1;
        p = *e ? e + 1 : e;
    }
    return 0;
}

static long limit_of(const char *s) {
    char *end = NULL;
    long n;
    if (!s) return DEFAULT_LIMIT;
    errno = 0;
    n = strtol(s, &end, 10);
    return errno || !end || *end || n < 1 || n > 10000 ? DEFAULT_LIMIT : n;
}

static int cat_file(const char *name) {
    char path[1024], buf[16384];
    size_t n;
    FILE *fp;
    if (!path_for(path, sizeof(path), name) || !(fp = fopen(path, "rb"))) {
        fprintf(stderr, "experience-map: cannot open %s\n", name);
        return 2;
    }
    while ((n = fread(buf, 1, sizeof(buf), fp)) != 0) fwrite(buf, 1, n, stdout);
    fclose(fp);
    return 0;
}

static int json_match(const char *file, const char *q) {
    char path[1024], *line = NULL;
    size_t cap = 0;
    FILE *fp;
    if (!path_for(path, sizeof(path), file) || !(fp = fopen(path, "r"))) return 2;
    while (getline(&line, &cap, fp) >= 0) {
        if (contains_ci(line, q)) {
            fputs(line, stdout);
            free(line);
            fclose(fp);
            return 0;
        }
    }
    free(line);
    fclose(fp);
    return 1;
}

/* Definition addresses are exact top-level IDs, not words in another record's
 * stages or description. SQLite's existing JSON reader handles escapes and
 * nesting; the in-memory connection never opens an estate database. */
static int json_nul_escape(const char *line) {
    /* Older system SQLite JSON readers truncate decoded strings at U+0000.
     * Refuse that character, including in keys; a literal \\u0000 stays text. */
    for (const char *p = line; *p; p++) {
        if (*p != '\\') continue;
        if (!strncmp(p + 1, "u0000", 5)) return 1;
        if (p[1]) p++;
    }
    return 0;
}

static int json_definition(const char *file, const char *q) {
    char path[1024], *line = NULL, *matched = NULL;
    size_t cap = 0, number = 0, qlen = strlen(q);
    ssize_t len;
    FILE *fp = NULL;
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    int rc = 2;
    if (!qlen || qlen > 256) {
        fprintf(stderr, "experience-map: definition ID must be 1-256 bytes\n");
        return 2;
    }
    if (!path_for(path, sizeof(path), file) || !(fp = fopen(path, "r"))) {
        fprintf(stderr, "experience-map: cannot open %s\n", file);
        return 2;
    }
    if (sqlite3_open(":memory:", &db) != SQLITE_OK ||
        sqlite3_prepare_v2(db,
            "SELECT json_type(?1),count(*),min(type),min(value) "
            "FROM json_each(?1) WHERE key='id'", -1, &st, NULL) != SQLITE_OK) {
        fprintf(stderr, "experience-map: JSON reader unavailable\n");
        goto done;
    }
    while ((len = getline(&line, &cap, fp)) >= 0) {
        const char *root, *type, *id;
        int bytes;
        number++;
        if (len > 1024 * 1024 || memchr(line, '\0', (size_t)len) || json_nul_escape(line)) goto invalid;
        sqlite3_reset(st);
        if (sqlite3_bind_text(st, 1, line, (int)len, SQLITE_TRANSIENT) != SQLITE_OK ||
            sqlite3_step(st) != SQLITE_ROW) goto invalid;
        root = (const char *)sqlite3_column_text(st, 0);
        type = (const char *)sqlite3_column_text(st, 2);
        id = (const char *)sqlite3_column_text(st, 3);
        bytes = sqlite3_column_bytes(st, 3);
        if (!root || strcmp(root, "object") || sqlite3_column_int(st, 1) != 1 ||
            !type || strcmp(type, "text") || !id || bytes < 1 || bytes > 256 ||
            memchr(id, '\0', (size_t)bytes)) goto invalid;
        if ((size_t)bytes != qlen || memcmp(id, q, qlen)) continue;
        if (matched) {
            fprintf(stderr, "experience-map: duplicate definition ID in %s\n", file);
            goto done;
        }
        matched = strdup(line);
        if (!matched) goto done;
    }
    if (ferror(fp)) {
        fprintf(stderr, "experience-map: cannot read %s\n", file);
        goto done;
    }
    rc = matched ? 0 : 1;
    if (matched) {
        fputs(matched, stdout);
        if (matched[strlen(matched) - 1] != '\n') putchar('\n');
    }
    goto done;
invalid:
    fprintf(stderr, "experience-map: invalid definition in %s at line %zu\n", file, number);
done:
    sqlite3_finalize(st);
    sqlite3_close(db);
    free(matched);
    free(line);
    fclose(fp);
    return rc;
}

static size_t fields(char *line, char **f) {
    size_t n = 0;
    char *p = line;
    while (n < FIELD_COUNT) {
        char *tab;
        f[n++] = p;
        tab = strchr(p, '\t');
        if (!tab) break;
        *tab = 0;
        p = tab + 1;
    }
    if (n) f[n - 1][strcspn(f[n - 1], "\r\n")] = 0;
    return n;
}

enum Mode { FIND, CHAIN, PROTOCOL, HOME, KIND };

static int matches(enum Mode m, char **f, size_t n, const char *q) {
    size_t i;
    if (n < FIELD_COUNT) return 0;
    if (m == CHAIN) return has_token(f[3], q);
    if (m == PROTOCOL) return has_token(f[4], q);
    if (m == HOME) return has_token(f[6], q);
    if (m == KIND) return strcmp(f[0], q) == 0;
    for (i = 0; i < n; i++) if (contains_ci(f[i], q)) return 1;
    return 0;
}

static int query(enum Mode m, const char *q, long limit) {
    char path[1024], *line = NULL;
    size_t cap = 0;
    long shown = 0;
    FILE *fp;
    if (!path_for(path, sizeof(path), "all_bindings.tsv") || !(fp = fopen(path, "r"))) return 2;
    (void)getline(&line, &cap, fp);
    while (shown < limit && getline(&line, &cap, fp) >= 0) {
        char *f[FIELD_COUNT] = {0};
        size_t n = fields(line, f);
        if (!matches(m, f, n, q)) continue;
        printf("%s\t%s\t%s\tchains=%s", f[0], f[1], f[2], f[3]);
        if (*f[4]) printf("\tprotocols=%s", f[4]);
        if (*f[5]) printf("\tterritories=%s", f[5]);
        if (*f[6]) printf("\thomes=%s", f[6]);
        if (*f[7]) printf("\tsource=%s", f[7]);
        putchar('\n');
        shown++;
    }
    free(line);
    fclose(fp);
    fprintf(stderr, "experience-map: shown=%ld limit=%ld\n", shown, limit);
    return shown ? 0 : 1;
}

static void usage(FILE *out) {
    fputs("BonfyreExperienceMap\n"
          "  stats\n"
          "  find TERM [limit]\n"
          "  resolve OBJECTIVE [anchors]  candidate owners and declared connections\n"
          "  chain CHAIN [limit]\n"
          "  protocol PROTOCOL [limit]\n"
          "  home BLUEPRINT [limit]\n"
          "  kind RECORD_KIND [limit]\n"
          "  chain-def CHAIN       exact definition ID\n"
          "  protocol-def PROTOCOL exact definition ID\n"
          "  home-def BLUEPRINT    exact definition ID\n"
          "  record-def RECORD     exact local/reference record ID\n"
          "  url-def TERM\n", out);
}

int main(int argc, char **argv) {
    const char *op, *q;
    long lim;
    infer_root_release(argv[0]);
    if (argc < 2) { usage(stderr); return 2; }
    op = argv[1];
    if (!strcmp(op, "stats")) return cat_file("manifest.json");
    if (!strcmp(op, "help") || !strcmp(op, "--help") || !strcmp(op, "-h")) { usage(stdout); return 0; }
    if (argc < 3) { usage(stderr); return 2; }
    q = argv[2];
    if (!strcmp(op, "resolve")) return experience_resolve(root_dir(), q, argc > 3 ? argv[3] : NULL);
    lim = limit_of(argc > 3 ? argv[3] : NULL);
    if (!strcmp(op, "find")) return query(FIND, q, lim);
    if (!strcmp(op, "chain")) return query(CHAIN, q, lim);
    if (!strcmp(op, "protocol")) { int rc = json_definition("protocols.jsonl", q); return rc ? rc : query(PROTOCOL, q, lim); }
    if (!strcmp(op, "home")) { int rc = json_definition("home_blueprints.jsonl", q); return rc ? rc : query(HOME, q, lim); }
    if (!strcmp(op, "kind")) return query(KIND, q, lim);
    if (!strcmp(op, "chain-def")) return json_definition("chains.jsonl", q);
    if (!strcmp(op, "protocol-def")) return json_definition("protocols.jsonl", q);
    if (!strcmp(op, "home-def")) return json_definition("home_blueprints.jsonl", q);
    if (!strcmp(op, "record-def")) return json_definition("app_bindings.jsonl", q);
    if (!strcmp(op, "url-def")) return json_match("url_grammars.jsonl", q);
    usage(stderr);
    return 2;
}
