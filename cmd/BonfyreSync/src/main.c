#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <spawn.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

extern char **environ;

static char *read_file(const char *path, long *size_out) {
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        perror("fopen");
        return NULL;
    }
    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    if (size < 0) {
        fclose(fp);
        return NULL;
    }
    char *buffer = malloc((size_t)size + 1);
    if (!buffer) {
        fclose(fp);
        return NULL;
    }
    fread(buffer, 1, (size_t)size, fp);
    fclose(fp);
    buffer[size] = '\0';
    if (size_out) *size_out = size;
    return buffer;
}

static int has_key(const char *json, const char *key) {
    char needle[256];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    return strstr(json, needle) != NULL;
}

static int extract_string_value(const char *json, const char *key, char *buffer, size_t size) {
    char needle[256];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *pos = strstr(json, needle);
    if (!pos) return 0;
    pos = strchr(pos + strlen(needle), ':');
    if (!pos) return 0;
    pos++;
    while (*pos && isspace((unsigned char)*pos)) pos++;
    if (*pos != '"') return 0;
    pos++;
    const char *end = strchr(pos, '"');
    if (!end) return 0;
    size_t len = (size_t)(end - pos);
    if (len >= size) len = size - 1;
    memcpy(buffer, pos, len);
    buffer[len] = '\0';
    return 1;
}

static int command_inspect_intake(const char *path) {
    long size = 0;
    char *json = read_file(path, &size);
    if (!json) return 1;

    const char *required[] = {
        "schemaVersion", "manifest", "sourceFile", "jobId", "jobSlug",
        "jobTitle", "fileName", "dataBase64", NULL
    };
    int valid = 1;
    for (int i = 0; required[i]; i++) {
        if (!has_key(json, required[i])) {
            valid = 0;
        }
    }

    char job_slug[256] = "";
    char job_title[256] = "";
    char file_name[256] = "";
    extract_string_value(json, "jobSlug", job_slug, sizeof(job_slug));
    extract_string_value(json, "jobTitle", job_title, sizeof(job_title));
    extract_string_value(json, "fileName", file_name, sizeof(file_name));

    printf("{\n");
    printf("  \"kind\": \"intake-package\",\n");
    printf("  \"path\": \"%s\",\n", path);
    printf("  \"valid\": %s,\n", valid ? "true" : "false");
    printf("  \"jobSlug\": \"%s\",\n", job_slug);
    printf("  \"jobTitle\": \"%s\",\n", job_title);
    printf("  \"fileName\": \"%s\",\n", file_name);
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

int main(int argc, char **argv) {
    if (argc < 2) goto usage;

    if (argc == 3 && strcmp(argv[1], "inspect-intake") == 0) {
        return command_inspect_intake(argv[2]);
    }
    if (argc == 3 && strcmp(argv[1], "inspect-status") == 0) {
        return command_inspect_status(argv[2]);
    }
    if (argc == 4 && strcmp(argv[1], "push") == 0) {
        return command_push(argv[2], argv[3]);
    }
    if (argc == 4 && strcmp(argv[1], "pull") == 0) {
        return command_pull(argv[2], argv[3]);
    }

usage:
    fprintf(stderr,
            "Usage:\n"
            "  bonfyre-sync inspect-intake <path>\n"
            "  bonfyre-sync inspect-status <path>\n"
            "  bonfyre-sync push <local.json> <remote-url>\n"
            "  bonfyre-sync pull <remote-url> <local.json>\n");
    return 1;
}
