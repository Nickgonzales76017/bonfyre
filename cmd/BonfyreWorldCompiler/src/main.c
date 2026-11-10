#define _POSIX_C_SOURCE 200809L
#include "native_world_fragment_lowerers.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define VERSION "0.1.0"

typedef struct {
    char namespace[128];
    char fragment_id[384];
    char *attributes;
} Candidate;

static char cmd_root[PATH_MAX];

static const char *opt(int argc, char **argv, const char *name) {
    for (int i = 1; i + 1 < argc; i++)
        if (!strcmp(argv[i], name)) return argv[i + 1];
    return NULL;
}

static int flag(int argc, char **argv, const char *name) {
    for (int i = 1; i < argc; i++) if (!strcmp(argv[i], name)) return 1;
    return 0;
}

static void init_root(const char *argv0) {
    char self[PATH_MAX];
    if (!argv0 || !realpath(argv0, self)) return;
    char *slash = strrchr(self, '/');
    if (!slash) return;
    *slash = '\0';
    slash = strrchr(self, '/');
    if (!slash) return;
    *slash = '\0';
    snprintf(cmd_root, sizeof(cmd_root), "%s", self);
}

static const char *cms_binary(const char *explicit_path, char *buf, size_t cap) {
    const char *env = getenv("BONFYRE_CMS_BINARY");
    const char *home;
    if (explicit_path && *explicit_path) return explicit_path;
    if (env && *env) return env;
    if (cmd_root[0]) {
        snprintf(buf, cap, "%s/BonfyreCMS/bonfyre-cms", cmd_root);
        if (access(buf, X_OK) == 0) return buf;
    }
    home = getenv("HOME");
    if (home && *home) {
        snprintf(buf, cap, "%s/.bonfyre/bin/bonfyre-cms", home);
        if (access(buf, X_OK) == 0) return buf;
    }
    snprintf(buf, cap, "/usr/local/bin/bonfyre-cms");
    return buf;
}

static void json_file(FILE *out, const char *text) {
    fputc('"', out);
    for (; text && *text; text++) {
        unsigned char c = (unsigned char)*text;
        if (c == '"' || c == '\\') {
            fputc('\\', out);
            fputc(c, out);
        } else if (c == '\n') fputs("\\n", out);
        else if (c == '\r') fputs("\\r", out);
        else if (c == '\t') fputs("\\t", out);
        else if (c < 0x20) fprintf(out, "\\u%04x", c);
        else fputc(c, out);
    }
    fputc('"', out);
}

static const BfWorldFragmentLowerer *lowerer_for(const char *grammar) {
    for (size_t i = 0; i < BF_WORLD_FRAGMENT_LOWERER_COUNT; i++)
        if (!strcmp(bf_world_fragment_lowerers[i].input_grammar, grammar))
            return &bf_world_fragment_lowerers[i];
    return NULL;
}

static char *trim(char *text) {
    char *end;
    while (*text && isspace((unsigned char)*text)) text++;
    end = text + strlen(text);
    while (end > text && isspace((unsigned char)end[-1])) *--end = '\0';
    return text;
}

static int fragment_exists(sqlite3_stmt *st, const char *fragment_id) {
    int found;
    sqlite3_reset(st);
    sqlite3_clear_bindings(st);
    sqlite3_bind_text(st, 1, fragment_id, -1, SQLITE_TRANSIENT);
    found = sqlite3_step(st) == SQLITE_ROW;
    return found;
}

static int candidate_seen(const Candidate *items, size_t count, const char *fragment_id) {
    for (size_t i = 0; i < count; i++)
        if (!strcmp(items[i].fragment_id, fragment_id)) return 1;
    return 0;
}

static char *fragment_attributes(const char *world, const char *subject,
                                 const char *template_name, const char *grammar,
                                 int ordinal, const BfWorldFragmentLowerer *lowerer) {
    char *body = NULL;
    size_t size = 0;
    FILE *out = open_memstream(&body, &size);
    char subject_fallback[512], projection[1024], fragment_id[384];
    if (!out) return NULL;
    snprintf(fragment_id, sizeof(fragment_id), "%s:%s", world, grammar);
    snprintf(subject_fallback, sizeof(subject_fallback), "world:%s", world);
    snprintf(projection, sizeof(projection),
             "/_surface/world-material?world=%s&fragment=%s", world, grammar);
    fputs("{\"fragment_id\":", out); json_file(out, fragment_id);
    fputs(",\"world\":", out); json_file(out, world);
    fprintf(out, ",\"ordinal\":%d,\"subject_address\":", ordinal);
    json_file(out, subject && *subject ? subject : subject_fallback);
    fputs(",\"surface_grammar\":", out); json_file(out, lowerer->surface_grammar);
    fputs(",\"renderer_family\":", out); json_file(out, lowerer->renderer_family);
    fputs(",\"renderer_variant\":", out); json_file(out, lowerer->renderer_variant);
    fputs(",\"extent\":", out); json_file(out, lowerer->extent);
    fputs(",\"template\":", out); json_file(out, template_name ? template_name : "");
    fputs(",\"source_projection\":", out); json_file(out, projection);
    fputs(",\"media_posture\":\"native\",\"temporal_posture\":\"retained_observation\"", out);
    fputs(",\"continuity_group\":", out); json_file(out, world);
    fputs(",\"visible_sections\":\"material,provenance,why_here\",\"action_aperture\":\"\"", out);
    fputs(",\"provenance\":\"compiled from World.grammars; source bodies remain owned by ForeignTwin receipts\"", out);
    fputs(",\"cannot_infer\":\"a rendered World fragment is not source truth, actor authority, provider admission, payment authority, or an effect grant\"", out);
    fputs(",\"status\":\"published\"}", out);
    fclose(out);
    return body;
}

static int run_cms_create(const char *cms, const char *db, const char *schemas,
                          const Candidate *candidate) {
    pid_t pid = fork();
    int status = 0;
    if (pid < 0) return 1;
    if (pid == 0) {
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) {
            dup2(devnull, STDOUT_FILENO);
            close(devnull);
        }
        char *const av[] = {
            (char *)cms, "entry", "create", "world_fragment",
            candidate->attributes, "--ns", (char *)candidate->namespace,
            "--db", (char *)db, "--schemas", (char *)schemas, NULL
        };
        execv(cms, av);
        _exit(127);
    }
    if (waitpid(pid, &status, 0) < 0) return 1;
    return !(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

static void free_candidates(Candidate *items, size_t count) {
    for (size_t i = 0; i < count; i++) free(items[i].attributes);
    free(items);
}

static void print_ids(const Candidate *items, size_t count) {
    fputc('[', stdout);
    for (size_t i = 0; i < count; i++) {
        if (i) fputc(',', stdout);
        json_file(stdout, items[i].fragment_id);
    }
    fputc(']', stdout);
}

int main(int argc, char **argv) {
    const char *db_path = opt(argc, argv, "--db");
    const char *schemas = opt(argc, argv, "--schemas");
    const char *cms_arg = opt(argc, argv, "--cms");
    int check = flag(argc, argv, "--check");
    int apply = flag(argc, argv, "--apply");
    sqlite3 *db = NULL;
    sqlite3_stmt *worlds = NULL, *exists = NULL;
    Candidate *items = NULL;
    size_t count = 0, cap = 0;
    int rc = 0;

    init_root(argv[0]);
    if (!db_path || !*db_path || (check && apply)) {
        fprintf(stderr,
                "BonfyreWorldCompiler %s\n"
                "  bonfyre-world-compiler --db CMS_DB [--check]\n"
                "  bonfyre-world-compiler --db CMS_DB --schemas DIR [--cms BIN] --apply\n",
                VERSION);
        return 2;
    }
    /* WAL-backed CMS stores need writable shared-memory bookkeeping even for a
     * reader. Open the connection RW, then make SQL itself fail closed. */
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READWRITE, NULL) != SQLITE_OK ||
        sqlite3_exec(db, "PRAGMA query_only=ON", NULL, NULL, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db,
            "SELECT namespace,world,COALESCE(subject,''),COALESCE(template,''),"
            "COALESCE(grammars,'') FROM world WHERE status='published' ORDER BY id",
            -1, &worlds, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db,
            "SELECT 1 FROM world_fragment WHERE fragment_id=?1 AND status='published' LIMIT 1",
            -1, &exists, NULL) != SQLITE_OK) {
        fprintf(stderr, "world compiler cannot read CMS: %s\n",
                db ? sqlite3_errmsg(db) : "open failed");
        rc = 3;
        goto done;
    }

    while (sqlite3_step(worlds) == SQLITE_ROW) {
        const char *ns = (const char *)sqlite3_column_text(worlds, 0);
        const char *world = (const char *)sqlite3_column_text(worlds, 1);
        const char *subject = (const char *)sqlite3_column_text(worlds, 2);
        const char *template_name = (const char *)sqlite3_column_text(worlds, 3);
        const char *grammars = (const char *)sqlite3_column_text(worlds, 4);
        char grammar_buf[4096], *save = NULL, *token;
        int ordinal = 10;
        if (!ns || !world || !*world || !grammars || !*grammars) continue;
        snprintf(grammar_buf, sizeof(grammar_buf), "%s", grammars);
        for (char *p = grammar_buf; *p; p++) if (*p == '|') *p = ',';
        for (token = strtok_r(grammar_buf, ",", &save);
             token;
             token = strtok_r(NULL, ",", &save), ordinal++) {
            char fragment_id[384];
            char *grammar = trim(token);
            const BfWorldFragmentLowerer *lowerer;
            if (!*grammar) continue;
            lowerer = lowerer_for(grammar);
            if (!lowerer) {
                fprintf(stderr, "unsupported World grammar: %s:%s\n", world, grammar);
                rc = 4;
                goto done;
            }
            snprintf(fragment_id, sizeof(fragment_id), "%s:%s", world, grammar);
            if (fragment_exists(exists, fragment_id) ||
                candidate_seen(items, count, fragment_id)) continue;
            if (count == cap) {
                size_t next = cap ? cap * 2 : 32;
                Candidate *grown = realloc(items, next * sizeof(*items));
                if (!grown) { rc = 5; goto done; }
                items = grown;
                cap = next;
            }
            memset(&items[count], 0, sizeof(items[count]));
            snprintf(items[count].namespace, sizeof(items[count].namespace), "%s", ns);
            snprintf(items[count].fragment_id, sizeof(items[count].fragment_id), "%s", fragment_id);
            items[count].attributes = fragment_attributes(
                world, subject, template_name, grammar, ordinal, lowerer);
            if (!items[count].attributes) { rc = 5; goto done; }
            count++;
        }
    }

    if (check) {
        printf("{\"schema\":\"bonfyre.world-fragment-compile-check.v1\",\"missing\":%zu,\"fragment_ids\":",
               count);
        print_ids(items, count);
        fputs(",\"truth\":\"presentation_plan\",\"world_fragment_publication_effect\":false}\n", stdout);
        rc = count ? 1 : 0;
        goto done;
    }

    if (!apply) {
        printf("{\"schema\":\"bonfyre.world-fragment-compile-preview.v1\",\"apply\":false,"
               "\"candidate_count\":%zu,\"fragment_ids\":", count);
        print_ids(items, count);
        fputs(",\"compiler_source\":\"world-fragment-lowering.cue\","
              "\"canonical_owner\":\"BonfyreCMS\",\"truth\":\"presentation_plan\","
              "\"world_fragment_publication_effect\":false}\n", stdout);
        goto done;
    }

    if (!schemas || !*schemas) {
        fputs("--apply requires --schemas DIR\n", stderr);
        rc = 2;
        goto done;
    }
    {
        char cms_buf[PATH_MAX];
        const char *cms = cms_binary(cms_arg, cms_buf, sizeof(cms_buf));
        size_t created = 0, failed = 0;
        if (access(cms, X_OK) != 0) {
            fprintf(stderr, "BonfyreCMS owner unavailable: %s\n", cms);
            rc = 6;
            goto done;
        }
        for (size_t i = 0; i < count; i++) {
            if (run_cms_create(cms, db_path, schemas, &items[i])) failed++;
            else created++;
        }
        printf("{\"schema\":\"bonfyre.world-fragment-compile-result.v1\",\"apply\":true,"
               "\"requested\":%zu,\"created\":%zu,\"failed\":%zu,"
               "\"compiler_source\":\"world-fragment-lowering.cue\","
               "\"canonical_owner\":\"BonfyreCMS\",\"truth\":\"presentation_plan\","
               "\"world_fragment_publication_effect\":true,\"source_publication_effect\":false,\"cannot_infer\":["
               "\"World grammar -> source truth\",\"World fragment -> actor authority\","
               "\"World fragment -> provider admission, payment authority or effect grant\"]}\n",
               count, created, failed);
        if (failed) rc = 1;
    }

done:
    if (worlds) sqlite3_finalize(worlds);
    if (exists) sqlite3_finalize(exists);
    if (db) sqlite3_close(db);
    free_candidates(items, count);
    return rc;
}
