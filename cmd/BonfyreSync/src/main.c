/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include <ctype.h>
#include "bf_lex.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <spawn.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "bonfyre.h"
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

extern char **environ;

static const char *layeros_binary(void) {
    return "layeros/bin/bonfyre-layeros";
}

static int run_execvp(char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return 1;
    }
    if (pid == 0) {
        execvp(argv[0], argv);
        perror("execvp");
        _exit(127);
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
        perror("waitpid");
        return 1;
    }
    return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
}

static int delegate_layeros_sync(int argc, char **argv) {
    char *exec_argv[16];
    int n = 0;
    exec_argv[n++] = (char *)layeros_binary();
    const char *root = NULL;
    for (int i = 2; i < argc - 1; i++) {
        if (strcmp(argv[i], "--root") == 0) {
            root = argv[i + 1];
            break;
        }
    }
    if (root) {
        exec_argv[n++] = "--root";
        exec_argv[n++] = (char *)root;
    }
    exec_argv[n++] = "sync";
    exec_argv[n++] = "layer";
    if (argc >= 3) exec_argv[n++] = argv[2];
    for (int i = 3; i < argc; i++) {
        if ((strcmp(argv[i], "--root") == 0 && i + 1 < argc)) {
            i++;
            continue;
        }
        exec_argv[n++] = argv[i];
    }
    exec_argv[n] = NULL;
    return run_execvp(exec_argv);
}

static char *read_file(const char *path, long *size_out) {
    size_t sz = 0;
    char *buf = bf_read_file(path, &sz);
    if (!buf) perror("fopen");
    if (size_out) *size_out = (long)sz;
    return buf;
}

/* Delegates to the one implementation. This asked strstr whether the text
 * "key" appeared anywhere, so a manifest whose sourceFile was named jobSlug.txt
 * answered yes to a key it did not have. */
static int has_key(const char *json, const char *key) {
    return bf_lex_json_seek(json, key) != NULL;
}

static int extract_string_value(const char *json, const char *key, char *buffer, size_t size) {
    /* Delegates to the one implementation. This copy stopped at the first
     * quote, and truncated silently on overflow rather than reporting that the
     * value did not fit -- a prefix returned as if it were whole. */
    return bf_lex_json_string(json, key, buffer, size);
}

static int command_inspect_intake(const char *path) {
    long size = 0;
    char *json = read_file(path, &size);
    if (!json) return 1;

    const char *legacy_required[] = {
        "schemaVersion", "manifest", "sourceFile", "jobId", "jobSlug",
        "jobTitle", "fileName", "dataBase64", NULL
    };
    const char *canonical_required[] = {
        "ingest_version", "source_file", "source_bytes", "detected_type",
        "normalized_path", "normalized_bytes", "content_hash", "media_type",
        "status", NULL
    };
    int legacy_valid = 1;
    int canonical_valid = 1;
    for (int i = 0; legacy_required[i]; i++) {
        if (!has_key(json, legacy_required[i])) legacy_valid = 0;
    }
    for (int i = 0; canonical_required[i]; i++) {
        if (!has_key(json, canonical_required[i])) canonical_valid = 0;
    }
    int valid = legacy_valid || canonical_valid;

    char job_slug[256] = "";
    char job_title[256] = "";
    char file_name[256] = "";
    char content_hash[128] = "";
    char status[64] = "";
    extract_string_value(json, "jobSlug", job_slug, sizeof(job_slug));
    extract_string_value(json, "jobTitle", job_title, sizeof(job_title));
    extract_string_value(json, "fileName", file_name, sizeof(file_name));
    if (!file_name[0])
        extract_string_value(json, "source_file", file_name, sizeof(file_name));
    extract_string_value(json, "content_hash", content_hash, sizeof(content_hash));
    extract_string_value(json, "status", status, sizeof(status));

    printf("{\n");
    printf("  \"kind\": \"intake-package\",\n");
    printf("  \"format\": \"%s\",\n", canonical_valid ? "bonfyre-intake-v1" : "legacy-intake");
    printf("  \"path\": \"%s\",\n", path);
    printf("  \"valid\": %s,\n", valid ? "true" : "false");
    printf("  \"jobSlug\": \"%s\",\n", job_slug);
    printf("  \"jobTitle\": \"%s\",\n", job_title);
    printf("  \"fileName\": \"%s\",\n", file_name);
    printf("  \"contentHash\": \"%s\",\n", content_hash);
    printf("  \"status\": \"%s\",\n", status);
    printf("  \"sizeBytes\": %ld\n", size);
    printf("}\n");

    free(json);
    return valid ? 0 : 1;
}

static int command_inspect_status(const char *path) {
    long size = 0;
    char *json = read_file(path, &size);
    if (!json) return 1;

    const char *required[] = {
        "sourceSystem", "jobSlug", "status", "deliverableMarkdown", "quality", NULL
    };
    int valid = 1;
    for (int i = 0; required[i]; i++) {
        if (!has_key(json, required[i])) {
            valid = 0;
        }
    }

    char source_system[256] = "";
    char job_slug[256] = "";
    char status[256] = "";
    extract_string_value(json, "sourceSystem", source_system, sizeof(source_system));
    extract_string_value(json, "jobSlug", job_slug, sizeof(job_slug));
    extract_string_value(json, "status", status, sizeof(status));

    printf("{\n");
    printf("  \"kind\": \"browser-status\",\n");
    printf("  \"path\": \"%s\",\n", path);
    printf("  \"valid\": %s,\n", valid ? "true" : "false");
    printf("  \"sourceSystem\": \"%s\",\n", source_system);
    printf("  \"jobSlug\": \"%s\",\n", job_slug);
    printf("  \"status\": \"%s\",\n", status);
    printf("  \"sizeBytes\": %ld\n", size);
    printf("}\n");

    free(json);
    return valid ? 0 : 1;
}

/* ── push: upload a JSON file to a remote endpoint via curl ── */
static int command_push(const char *local_path, const char *remote_url) {
    long size = 0;
    char *json = read_file(local_path, &size);
    if (!json) {
        fprintf(stderr, "Failed to read: %s\n", local_path);
        return 1;
    }
    free(json);

    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    char at_path[2048];
    snprintf(at_path, sizeof(at_path), "@%s", local_path);

    pid_t pid;
    posix_spawnattr_t attr;
    posix_spawnattr_init(&attr);
    char *argv[] = {
        "curl", "-s", "-S", "-f",
        "-X", "PUT",
        "-H", "Content-Type: application/json",
        "-d", at_path,
        (char *)remote_url,
        NULL
    };
    int rc = posix_spawnp(&pid, "curl", NULL, &attr, argv, environ);
    posix_spawnattr_destroy(&attr);
    if (rc != 0) {
        fprintf(stderr, "Failed to spawn curl: %s\n", strerror(rc));
        return 1;
    }
    int status = 0;
    waitpid(pid, &status, 0);

    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = (double)(t1.tv_sec - t0.tv_sec) +
                     (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;

    int exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : 1;
    printf("{\"kind\":\"sync-push\",\"local\":\"%s\",\"remote\":\"%s\","
           "\"bytes\":%ld,\"success\":%s,\"elapsed_ms\":%.1f}\n",
           local_path, remote_url, size,
           exit_code == 0 ? "true" : "false", elapsed * 1000.0);
    return exit_code;
}

/* ── pull: download a JSON file from a remote endpoint via curl ── */
static int command_pull(const char *remote_url, const char *local_path) {
    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    pid_t pid;
    posix_spawnattr_t attr;
    posix_spawnattr_init(&attr);
    char *argv[] = {
        "curl", "-s", "-S", "-f",
        "-o", (char *)local_path,
        (char *)remote_url,
        NULL
    };
    int rc = posix_spawnp(&pid, "curl", NULL, &attr, argv, environ);
    posix_spawnattr_destroy(&attr);
    if (rc != 0) {
        fprintf(stderr, "Failed to spawn curl: %s\n", strerror(rc));
        return 1;
    }
    int status = 0;
    waitpid(pid, &status, 0);

    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = (double)(t1.tv_sec - t0.tv_sec) +
                     (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;

    int exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : 1;
    long pulled_size = 0;
    if (exit_code == 0) {
        struct stat st;
        if (stat(local_path, &st) == 0) pulled_size = st.st_size;
    }

    printf("{\"kind\":\"sync-pull\",\"remote\":\"%s\",\"local\":\"%s\","
           "\"bytes\":%ld,\"success\":%s,\"elapsed_ms\":%.1f}\n",
           remote_url, local_path, pulled_size,
           exit_code == 0 ? "true" : "false", elapsed * 1000.0);
    return exit_code;
}

static void usage(void) {
    fprintf(stderr,
            "Usage:\n"
            "  bonfyre-sync inspect-intake <path>\n"
            "  bonfyre-sync inspect-status <path>\n"
            "  bonfyre-sync push <local.json> <remote-url>\n"
            "  bonfyre-sync pull <remote-url> <local.json>\n"
            "  bonfyre-sync layer <artifact_id> [--manifest-only|--include-payload] [--root DIR]\n");
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    if (argc < 2) goto usage;

    enum {
        SY_INSPECT_INTAKE, SY_INSPECT_STATUS, SY_LAYER, SY_PULL, SY_PUSH,
    };
    static const BfCommandEntry sync_commands[] = {
        {"inspect-intake", SY_INSPECT_INTAKE, 0, NULL},
        {"inspect-status", SY_INSPECT_STATUS, 0, NULL},
        {"layer", SY_LAYER, 0, NULL},
        {"pull", SY_PULL, 0, NULL},
        {"push", SY_PUSH, 0, NULL},
    };
    const BfCommandEntry *command = bf_command_lookup(sync_commands,
        sizeof(sync_commands) / sizeof(sync_commands[0]), argv[1]);

    if (argc >= 3 && command && command->id == SY_LAYER) {
        return delegate_layeros_sync(argc, argv);
    }

    if (argc == 3 && command && command->id == SY_INSPECT_INTAKE) {
        return command_inspect_intake(argv[2]);
    }
    if (argc == 3 && command && command->id == SY_INSPECT_STATUS) {
        return command_inspect_status(argv[2]);
    }
    if (argc == 4 && command && command->id == SY_PUSH) {
        return command_push(argv[2], argv[3]);
    }
    if (argc == 4 && command && command->id == SY_PULL) {
        return command_pull(argv[2], argv[3]);
    }

usage:
    usage();
    return 1;
}
