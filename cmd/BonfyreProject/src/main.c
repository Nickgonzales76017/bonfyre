/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

static void resolve_executable_sibling(char *buffer, size_t size, const char *argv0, const char *sibling_dir, const char *binary_name) {
    if (argv0 && argv0[0] == '/') snprintf(buffer, size, "%s", argv0);
    else if (argv0 && strstr(argv0, "/")) {
        char cwd[PATH_MAX];
        if (getcwd(cwd, sizeof(cwd))) snprintf(buffer, size, "%s/%s", cwd, argv0);
        else snprintf(buffer, size, "%s", argv0);
    } else {
        buffer[0] = '\0';
        return;
    }
    char *last = strrchr(buffer, '/');
    if (!last) { buffer[0] = '\0'; return; }
    *last = '\0';
    last = strrchr(buffer, '/');
    if (!last) { buffer[0] = '\0'; return; }
    *last = '\0';
    snprintf(buffer, size, "%s/%s/%s", buffer, sibling_dir, binary_name);
}

static const char *default_binary(const char *env_name, const char *argv0, char *resolved, size_t resolved_size, const char *dir, const char *name, const char *fallback) {
    const char *env = getenv(env_name);
    if (env && env[0] != '\0') return env;
    resolve_executable_sibling(resolved, resolved_size, argv0, dir, name);
    if (resolved[0] != '\0' && access(resolved, X_OK) == 0) return resolved;
    return fallback;
}

static int run_command(char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) return 1;
    if (pid == 0) {
        execv(argv[0], argv);
        perror(argv[0]);
        _exit(127);
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) return 1;
    if (!WIFEXITED(status)) return 1;
    return WEXITSTATUS(status);
}

static void print_usage(void) {
    fprintf(stderr,
            "bonfyre-project\n\n"
            "Usage:\n"
            "  bonfyre-project cms <cms args...>\n"
            "  bonfyre-project index <index args...>\n"
            "  bonfyre-project stitch <stitch args...>\n"
            "  bonfyre-project refresh <artifact-dir> [--db FILE]\n");
}

int main(int argc, char **argv) {
    if (argc < 2) {
        print_usage();
        return 1;
    }

    char cms_resolved[PATH_MAX];
    char index_resolved[PATH_MAX];
    char stitch_resolved[PATH_MAX];
    const char *cms_bin = default_binary("BONFYRE_CMS_BINARY", argv[0], cms_resolved, sizeof(cms_resolved), "BonfyreCMS", "bonfyre-cms", "../BonfyreCMS/bonfyre-cms");
    const char *index_bin = default_binary("BONFYRE_INDEX_BINARY", argv[0], index_resolved, sizeof(index_resolved), "BonfyreIndex", "bonfyre-index", "../BonfyreIndex/bonfyre-index");
    const char *stitch_bin = default_binary("BONFYRE_STITCH_BINARY", argv[0], stitch_resolved, sizeof(stitch_resolved), "BonfyreStitch", "bonfyre-stitch", "../BonfyreStitch/bonfyre-stitch");

    enum {
        PR_CMS, PR_INDEX, PR_REFRESH, PR_STITCH,
    };
    static const BfCommandEntry project_commands[] = {
        {"cms", PR_CMS, 0, NULL},
        {"index", PR_INDEX, 0, NULL},
        {"refresh", PR_REFRESH, 0, NULL},
        {"stitch", PR_STITCH, 0, NULL},
    };
    const BfCommandEntry *command = bf_command_lookup(project_commands,
        sizeof(project_commands) / sizeof(project_commands[0]), argv[1]);

    if (command && (command->id == PR_CMS || command->id == PR_INDEX || command->id == PR_STITCH)) {
        if (argc < 3) return 1;
        const char *target = command->id == PR_CMS ? cms_bin : (command->id == PR_INDEX ? index_bin : stitch_bin);
        char **child = calloc((size_t)argc, sizeof(char *));
        if (!child) return 1;
        child[0] = (char *)target;
        for (int i = 2; i < argc; i++) child[i - 1] = argv[i];
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (command && command->id == PR_REFRESH) {
        if (argc < 3) {
            print_usage();
            return 1;
        }
        char **child = calloc((size_t)argc + 2, sizeof(char *));
        if (!child) return 1;
        child[0] = (char *)index_bin;
        child[1] = "build";
        child[2] = argv[2];
        for (int i = 3; i < argc; i++) child[i] = argv[i];
        int rc = run_command(child);
        free(child);
        return rc;
    }

    print_usage();
    return 1;
}
