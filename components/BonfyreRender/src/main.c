#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int ensure_dir(const char *path) {
    char tmp[PATH_MAX];
    size_t len = strlen(path);
    if (len == 0 || len >= sizeof(tmp)) return 1;
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (size_t i = 1; i < len; i++) {
        if (tmp[i] == '/') {
            tmp[i] = '\0';
            if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return 1;
            tmp[i] = '/';
        }
    }
    if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return 1;
    return 0;
}

static void path_join(char *buffer, size_t size, const char *left, const char *right) {
    snprintf(buffer, size, "%s/%s", left, right);
}

static void iso_timestamp(char *buffer, size_t size) {
    time_t now = time(NULL);
    struct tm tm_utc;
    gmtime_r(&now, &tm_utc);
    strftime(buffer, size, "%Y-%m-%dT%H:%M:%SZ", &tm_utc);
}

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

static int write_status(const char *out_dir, const char *brief_dir, const char *narrate_dir) {
    char path[PATH_MAX];
    char ts[64];
    path_join(path, sizeof(path), out_dir, "render-status.json");
    iso_timestamp(ts, sizeof(ts));
    FILE *fp = fopen(path, "w");
    if (!fp) return 1;
    fprintf(fp,
            "{\n"
            "  \"status\": \"ok\",\n"
            "  \"updatedAt\": \"%s\",\n"
            "  \"briefDir\": \"%s\",\n"
            "  \"narrateDir\": \"%s\"\n"
            "}\n",
            ts, brief_dir, narrate_dir);
    fclose(fp);
    return 0;
}

static void print_usage(void) {
    fprintf(stderr,
            "bonfyre-render\n\n"
            "Usage:\n"
            "  bonfyre-render artifact <transcript-file> <output-dir> [--title TITLE]\n"
            "  bonfyre-render package <proof-dir> <offer-dir> <output-dir>\n");
}

int main(int argc, char **argv) {
    if (argc < 2) {
        print_usage();
        return 1;
    }

    char brief_resolved[PATH_MAX];
    char narrate_resolved[PATH_MAX];
    char pack_resolved[PATH_MAX];
    const char *brief_bin = default_binary("BONFYRE_BRIEF_BINARY", argv[0], brief_resolved, sizeof(brief_resolved), "BonfyreBrief", "bonfyre-brief", "../BonfyreBrief/bonfyre-brief");
    const char *narrate_bin = default_binary("BONFYRE_NARRATE_BINARY", argv[0], narrate_resolved, sizeof(narrate_resolved), "BonfyreNarrate", "bonfyre-narrate", "../BonfyreNarrate/bonfyre-narrate");
    const char *pack_bin = default_binary("BONFYRE_PACK_BINARY", argv[0], pack_resolved, sizeof(pack_resolved), "BonfyrePack", "bonfyre-pack", "../BonfyrePack/bonfyre-pack");

    if (strcmp(argv[1], "package") == 0) {
        if (argc != 5) {
            print_usage();
            return 1;
        }
        char *pack_argv[] = {
            (char *)pack_bin,
            "assemble",
            argv[2],
            argv[3],
            argv[4],
            NULL
        };
        return run_command(pack_argv);
    }

    if (strcmp(argv[1], "artifact") == 0) {
        if (argc < 4) {
            print_usage();
            return 1;
        }
        const char *transcript = argv[2];
        const char *out_dir = argv[3];
        const char *title = NULL;
        for (int i = 4; i < argc - 1; i++) {
            if (strcmp(argv[i], "--title") == 0) title = argv[i + 1];
        }

        char brief_dir[PATH_MAX];
        char narrate_dir[PATH_MAX];
        char brief_md[PATH_MAX];
        path_join(brief_dir, sizeof(brief_dir), out_dir, "brief");
        path_join(narrate_dir, sizeof(narrate_dir), out_dir, "narrate");
        path_join(brief_md, sizeof(brief_md), brief_dir, "brief.md");
        if (ensure_dir(out_dir) != 0 || ensure_dir(brief_dir) != 0 || ensure_dir(narrate_dir) != 0) return 1;

        char *brief_plain[] = {
            (char *)brief_bin,
            (char *)transcript,
            brief_dir,
            NULL
        };
        char *brief_titled[] = {
            (char *)brief_bin,
            (char *)transcript,
            brief_dir,
            "--title",
            (char *)title,
            NULL
        };
        if (run_command(title ? brief_titled : brief_plain) != 0) return 1;

        char *narrate_argv[] = {
            (char *)narrate_bin,
            brief_md,
            narrate_dir,
            NULL
        };
        if (run_command(narrate_argv) != 0) return 1;
        if (write_status(out_dir, brief_dir, narrate_dir) != 0) return 1;
        printf("Rendered artifact family: %s\n", out_dir);
        return 0;
    }

    print_usage();
    return 1;
}
