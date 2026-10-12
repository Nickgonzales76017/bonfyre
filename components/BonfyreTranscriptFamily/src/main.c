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
    if (argv0 && argv0[0] == '/') {
        snprintf(buffer, size, "%s", argv0);
    } else if (argv0 && strstr(argv0, "/")) {
        char cwd[PATH_MAX];
        if (getcwd(cwd, sizeof(cwd))) snprintf(buffer, size, "%s/%s", cwd, argv0);
        else snprintf(buffer, size, "%s", argv0);
    } else {
        buffer[0] = '\0';
        return;
    }

    char *last_slash = strrchr(buffer, '/');
    if (!last_slash) { buffer[0] = '\0'; return; }
    *last_slash = '\0';
    last_slash = strrchr(buffer, '/');
    if (!last_slash) { buffer[0] = '\0'; return; }
    *last_slash = '\0';
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

static int write_status(const char *out_dir, const char *transcribe_dir, const char *cleaned, const char *paragraphed) {
    char path[PATH_MAX];
    char ts[64];
    path_join(path, sizeof(path), out_dir, "family-status.json");
    iso_timestamp(ts, sizeof(ts));
    FILE *fp = fopen(path, "w");
    if (!fp) return 1;
    fprintf(fp,
            "{\n"
            "  \"status\": \"ok\",\n"
            "  \"updatedAt\": \"%s\",\n"
            "  \"transcribeDir\": \"%s\",\n"
            "  \"cleanedPath\": \"%s\",\n"
            "  \"paragraphedPath\": \"%s\"\n"
            "}\n",
            ts, transcribe_dir, cleaned, paragraphed);
    fclose(fp);
    return 0;
}

static void print_usage(void) {
    fprintf(stderr,
            "bonfyre-transcript-family\n\n"
            "Usage:\n"
            "  bonfyre-transcript-family <input-audio> <output-dir> [--with-headers] [transcribe flags...]\n");
}

int main(int argc, char **argv) {
    if (argc < 3) {
        print_usage();
        return 1;
    }

    const char *input = argv[1];
    const char *out_dir = argv[2];
    int with_headers = 0;

    char transcribe_resolved[PATH_MAX];
    char clean_resolved[PATH_MAX];
    char paragraph_resolved[PATH_MAX];
    const char *transcribe_bin = default_binary("BONFYRE_TRANSCRIBE_BINARY", argv[0], transcribe_resolved, sizeof(transcribe_resolved), "BonfyreTranscribe", "bonfyre-transcribe", "../BonfyreTranscribe/bonfyre-transcribe");
    const char *clean_bin = default_binary("BONFYRE_TRANSCRIPT_CLEAN_BINARY", argv[0], clean_resolved, sizeof(clean_resolved), "BonfyreTranscriptClean", "bonfyre-transcript-clean", "../BonfyreTranscriptClean/bonfyre-transcript-clean");
    const char *paragraph_bin = default_binary("BONFYRE_PARAGRAPH_BINARY", argv[0], paragraph_resolved, sizeof(paragraph_resolved), "BonfyreParagraph", "bonfyre-paragraph", "../BonfyreParagraph/bonfyre-paragraph");

    char transcribe_dir[PATH_MAX];
    char cleaned_path[PATH_MAX];
    char paragraphed_path[PATH_MAX];
    char transcript_path[PATH_MAX];
    path_join(transcribe_dir, sizeof(transcribe_dir), out_dir, "transcribe");
    path_join(cleaned_path, sizeof(cleaned_path), out_dir, "cleaned.txt");
    path_join(paragraphed_path, sizeof(paragraphed_path), out_dir, "paragraphed.md");
    path_join(transcript_path, sizeof(transcript_path), transcribe_dir, "normalized.txt");

    if (ensure_dir(out_dir) != 0 || ensure_dir(transcribe_dir) != 0) {
        fprintf(stderr, "Failed to create output directories.\n");
        return 1;
    }

    char **transcribe_argv = calloc((size_t)argc + 4, sizeof(char *));
    if (!transcribe_argv) return 1;
    int t = 0;
    transcribe_argv[t++] = (char *)transcribe_bin;
    transcribe_argv[t++] = (char *)input;
    transcribe_argv[t++] = transcribe_dir;
    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "--with-headers") == 0) {
            with_headers = 1;
            continue;
        }
        transcribe_argv[t++] = argv[i];
    }
    transcribe_argv[t] = NULL;

    if (run_command(transcribe_argv) != 0) {
        free(transcribe_argv);
        return 1;
    }
    free(transcribe_argv);

    char *clean_argv[] = {
        (char *)clean_bin,
        "--transcript",
        transcript_path,
        "--out",
        cleaned_path,
        NULL
    };
    if (run_command(clean_argv) != 0) return 1;

    char *paragraph_with_headers[] = {
        (char *)paragraph_bin,
        "--input",
        cleaned_path,
        "--out",
        paragraphed_path,
        "--with-headers",
        NULL
    };
    char *paragraph_plain[] = {
        (char *)paragraph_bin,
        "--input",
        cleaned_path,
        "--out",
        paragraphed_path,
        NULL
    };
    if (run_command(with_headers ? paragraph_with_headers : paragraph_plain) != 0) return 1;

    if (write_status(out_dir, transcribe_dir, cleaned_path, paragraphed_path) != 0) return 1;
    printf("Transcript family: %s\n", out_dir);
    return 0;
}
