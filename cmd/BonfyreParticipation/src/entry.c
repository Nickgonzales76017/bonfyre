#include "activation.h"
#include "dating.h"
#include "evidence.h"
#include "projection.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#include <limits.h>

static const char *opt(int argc, char **argv, const char *name, const char *fallback) {
    for (int i = 2; i + 1 < argc; ++i) {
        if (!strcmp(argv[i], name)) return argv[i + 1];
    }
    return fallback;
}

static void usage(FILE *out) {
    fprintf(out,
        "Usage:\n"
        "  bonfyre-participation compile [options]\n"
        "  bonfyre-participation activate [options]\n"
        "  bonfyre-participation dating-init --db /private/path.db\n\n"
        "Options:\n"
        "  --external-bindings FILE  ExternalEstate compiled bindings JSONL\n"
        "  --evidence PATH           evidence JSONL file or recursive directory\n"
        "  --manifest FILE           output manifest\n"
        "  --frontier FILE           activation frontier JSONL\n"
        "  --limit N                 activation frontier size (default 250)\n");
}

int main(int argc, char **argv) {
    sqlite3 *db = NULL;
    long long raw_rows = 0;
    int rc = 1;
    int activate = 0;
    int limit = 250;
    const char *external;
    const char *evidence;
    const char *manifest;
    const char *frontier;

    enum {
        PA_HELP, PA_ACTIVATE, PA_COMPILE, PA_DATING_INIT,
    };
    static const BfCommandEntry participation_commands[] = {
        {"--help", PA_HELP, 0, NULL},
        {"activate", PA_ACTIVATE, 0, NULL},
        {"compile", PA_COMPILE, 0, NULL},
        {"dating-init", PA_DATING_INIT, 0, NULL},
        {"help", PA_HELP, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(participation_commands,
        sizeof(participation_commands) / sizeof(participation_commands[0]), argv[1]);

    if (argc < 2 || (command && command->id == PA_HELP)) {
        usage(stdout);
        return argc < 2 ? 1 : 0;
    }
    if (command && command->id == PA_DATING_INIT) {
        const char *path = argc == 4 && !strcmp(argv[2], "--db") ? argv[3] : NULL;
        struct stat st;
        int made = 0, ok = 0;
        if (!path || path[0] != '/' || strlen(path) > 4096 ||
            strstr(path, "/../") || strstr(path, "/./") ||
            (strlen(path) > 2 && (!strcmp(path + strlen(path)-3, "/..") ||
                                   !strcmp(path + strlen(path)-2, "/.")))) {
            fputs("dating-init requires an absolute, private SQLite path\\n", stderr);
            return 2;
        }
        errno = 0;
        if (lstat(path, &st) == 0) {
            if (!S_ISREG(st.st_mode) || (st.st_mode & 0077)) {
                fputs("dating-init refuses symlinks and shared files\\n", stderr);
                return 2;
            }
        } else if (errno != ENOENT) {
            fputs("dating-init cannot inspect the target path\\n", stderr);
            return 2;
        } else {
            made = 1;
        }
        umask(0077);
        if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) == SQLITE_OK) {
            sqlite3_busy_timeout(db, 3000);
            ok = bf_dating_store_init(db);
        }
        if (db) sqlite3_close(db);
        if (!ok) {
            /* An existing database is never removed by a failed migration. */
            if (made && lstat(path, &st) == 0 && S_ISREG(st.st_mode) &&
                st.st_nlink == 1 && !(st.st_mode & 0077)) remove(path);
            fputs("dating-init could not initialize the private owner\\n", stderr);
            return 2;
        }
        puts("Dating owner initialized; no eligibility, member or message records created.");
        return 0;
    }
    if (command && command->id == PA_ACTIVATE) activate = 1;
    else if (!command || command->id != PA_COMPILE) { usage(stderr); return 1; }

    external = opt(argc, argv, "--external-bindings",
                   "../BonfyreExperienceMap/generated/external_bindings.jsonl");
    evidence = opt(argc, argv, "--evidence",
                   "../BonfyreExperienceMap/generated/participation_evidence.jsonl");
    manifest = opt(argc, argv, "--manifest",
                   activate ? "generated/participation_activation_manifest.native.json"
                            : "generated/participation_manifest.json");
    frontier = opt(argc, argv, "--frontier", "generated/participation_activation_frontier.native.jsonl");
    if (activate) {
        limit = atoi(opt(argc, argv, "--limit", "250"));
        if (limit < 1) { fprintf(stderr, "--limit must be >= 1\n"); return 1; }
    }

    if (sqlite3_open(":memory:", &db) != SQLITE_OK) goto done;
    if (!bf_part_evidence_init(db)) goto done;
    if (!bf_part_evidence_load(db, evidence, &raw_rows)) {
        fprintf(stderr, "participation: evidence load failed: %s\n", evidence);
        goto done;
    }
    rc = activate
        ? bf_part_activation_compile(db, external, manifest, frontier, limit)
        : bf_part_project_manifest(db, external, raw_rows, manifest);
    if (rc) fprintf(stderr, "participation: %s projection failed: %s\n", activate ? "activation" : "manifest", external);
done:
    if (db) sqlite3_close(db);
    return rc;
}
