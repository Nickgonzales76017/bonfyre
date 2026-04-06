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

static void path_join(char *buffer, size_t size, const char *left, const char *right) {
    snprintf(buffer, size, "%s/%s", left, right);
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

static int run_command_to_file(char *const argv[], const char *output_path) {
    pid_t pid = fork();
    if (pid < 0) return 1;
    if (pid == 0) {
        FILE *fp = fopen(output_path, "w");
        if (!fp) _exit(127);
        if (dup2(fileno(fp), STDOUT_FILENO) < 0) _exit(127);
        fclose(fp);
        execv(argv[0], argv);
        _exit(127);
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) return 1;
    if (!WIFEXITED(status)) return 1;
    return WEXITSTATUS(status);
}

static void print_usage(void) {
    fprintf(stderr,
            "bonfyre-runtime\n\n"
            "Usage:\n"
            "  bonfyre-runtime run <input> [pipeline args...]\n"
            "  bonfyre-runtime run-ledger <input> [pipeline args...]\n"
            "  bonfyre-runtime queue <queue args...>\n"
            "  bonfyre-runtime ledger <ledger args...>\n");
}

int main(int argc, char **argv) {
    if (argc < 2) {
        print_usage();
        return 1;
    }

    char queue_resolved[PATH_MAX];
    char pipeline_resolved[PATH_MAX];
    char ledger_resolved[PATH_MAX];
    const char *queue_bin = default_binary("BONFYRE_QUEUE_BINARY", argv[0], queue_resolved, sizeof(queue_resolved), "BonfyreQueue", "bonfyre-queue", "../BonfyreQueue/bonfyre-queue");
    const char *pipeline_bin = default_binary("BONFYRE_PIPELINE_BINARY", argv[0], pipeline_resolved, sizeof(pipeline_resolved), "BonfyrePipeline", "bonfyre-pipeline", "../BonfyrePipeline/bonfyre-pipeline");
    const char *ledger_bin = default_binary("BONFYRE_LEDGER_BINARY", argv[0], ledger_resolved, sizeof(ledger_resolved), "BonfyreLedger", "bonfyre-ledger", "../BonfyreLedger/bonfyre-ledger");

    if (strcmp(argv[1], "queue") == 0) {
        if (argc < 3) return 1;
        char **child = calloc((size_t)argc, sizeof(char *));
        if (!child) return 1;
        child[0] = (char *)queue_bin;
        for (int i = 2; i < argc; i++) child[i - 1] = argv[i];
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (strcmp(argv[1], "ledger") == 0) {
        if (argc < 3) return 1;
        char **child = calloc((size_t)argc, sizeof(char *));
        if (!child) return 1;
        child[0] = (char *)ledger_bin;
        for (int i = 2; i < argc; i++) child[i - 1] = argv[i];
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (strcmp(argv[1], "run") == 0 || strcmp(argv[1], "run-ledger") == 0) {
        if (argc < 3) {
            print_usage();
            return 1;
        }
        const int with_ledger = (strcmp(argv[1], "run-ledger") == 0);
        const char *input = argv[2];
        const char *out_dir = NULL;
        for (int i = 3; i < argc - 1; i++) {
            if (strcmp(argv[i], "--out") == 0) out_dir = argv[i + 1];
        }
        if (!out_dir) {
            fprintf(stderr, "run and run-ledger require --out DIR\n");
            return 1;
        }

        char **pipeline_argv = calloc((size_t)argc + 2, sizeof(char *));
        if (!pipeline_argv) return 1;
        int p = 0;
        pipeline_argv[p++] = (char *)pipeline_bin;
        pipeline_argv[p++] = "run";
        pipeline_argv[p++] = (char *)input;
        for (int i = 3; i < argc; i++) pipeline_argv[p++] = argv[i];
        pipeline_argv[p] = NULL;
        int rc = run_command(pipeline_argv);
        free(pipeline_argv);
        if (rc != 0 || !with_ledger) return rc;

        char artifact_path[PATH_MAX];
        char ledger_json[PATH_MAX];
        path_join(artifact_path, sizeof(artifact_path), out_dir, "artifact.json");
        path_join(ledger_json, sizeof(ledger_json), out_dir, "ledger-assessment.json");
        char *ledger_argv[] = {
            (char *)ledger_bin,
            "assess-json",
            artifact_path,
            NULL
        };
        return run_command_to_file(ledger_argv, ledger_json);
    }

    print_usage();
    return 1;
}
