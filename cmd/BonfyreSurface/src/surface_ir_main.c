#include "surface_ir.h"

#include <stdio.h>
#include <string.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

static const char *arg(int argc, char **argv, const char *name) {
    for (int i = 2; i + 1 < argc; ++i)
        if (!strcmp(argv[i], name)) return argv[i + 1];
    return NULL;
}

static void usage(void) {
    fputs(
        "BonfyreSurface SurfaceIR compiler\n\n"
        "Usage:\n"
        "  bonfyre-surface-ir compile --db FILE --namespace NS --page PAGE "
        "--audience AUDIENCE [--slot SLOT]\n",
        stderr);
}

int main(int argc, char **argv) {
    sqlite3 *db = NULL;
    const char *db_path, *namespace_name, *page, *slot, *audience;
    int rc;

    enum { SI_COMPILE };
    static const BfCommandEntry surface_ir_commands[] = {
        {"compile", SI_COMPILE, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(surface_ir_commands,
        sizeof(surface_ir_commands) / sizeof(surface_ir_commands[0]), argv[1]);

    if (argc < 2 || !command || command->id != SI_COMPILE) {
        usage();
        return 2;
    }

    db_path = arg(argc, argv, "--db");
    namespace_name = arg(argc, argv, "--namespace");
    page = arg(argc, argv, "--page");
    audience = arg(argc, argv, "--audience");
    slot = arg(argc, argv, "--slot");

    if (!db_path || !namespace_name || !page || !audience) {
        usage();
        return 2;
    }

    rc = sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "bonfyre-surface-ir: cannot open %s: %s\n",
                db_path, db ? sqlite3_errmsg(db) : "sqlite open failed");
        if (db) sqlite3_close(db);
        return 3;
    }

    rc = bf_surface_ir_compile(db, namespace_name, page, slot, audience, stdout);
    sqlite3_close(db);
    return rc;
}
