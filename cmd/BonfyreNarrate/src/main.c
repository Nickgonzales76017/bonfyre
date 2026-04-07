#define _POSIX_C_SOURCE 200809L

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <bonfyre.h>

static int ensure_dir(const char *path) { return bf_ensure_dir(path); }
static void iso_timestamp(char *buffer, size_t size) {
    time_t now = time(NULL);
    struct tm tm_utc;
    gmtime_r(&now, &tm_utc);
    strftime(buffer, size, "%Y-%m-%dT%H:%M:%SZ", &tm_utc);
}

static char *read_file(const char *path) {
    FILE *in = fopen(path, "rb");
    char *buffer;
    long size;
    if (!in) return NULL;
    if (fseek(in, 0, SEEK_END) != 0) {
        fclose(in);
        return NULL;
    }
    size = ftell(in);
    if (size < 0) {
        fclose(in);
        return NULL;
    }
    rewind(in);
    buffer = malloc((size_t)size + 1);
    if (!buffer) {
        fclose(in);
        return NULL;
    }
    if (size > 0 && fread(buffer, 1, (size_t)size, in) != (size_t)size) {
        free(buffer);
        fclose(in);
        return NULL;
    }
    buffer[size] = '\0';
    fclose(in);
    return buffer;
}

static int write_file(const char *path, const char *text) {
    FILE *out = fopen(path, "w");
    if (!out) return 1;
    if (fputs(text, out) == EOF) {
        fclose(out);
        return 1;
    }
    if (fputc('\n', out) == EOF) {
        fclose(out);
        return 1;
    }
    fclose(out);
    return 0;
}

static void append_char(char **buffer, size_t *len, size_t *cap, char c) {
    if (*len + 2 >= *cap) {
        size_t new_cap = (*cap == 0) ? 2048 : (*cap * 2);
        char *next = realloc(*buffer, new_cap);
        if (!next) return;
        *buffer = next;
        *cap = new_cap;
    }
    (*buffer)[(*len)++] = c;
    (*buffer)[*len] = '\0';
}

static char *normalize_for_narration(const char *text) {
    char *out = calloc(1, strlen(text) * 2 + 32);
    size_t len = 0;
    size_t cap = strlen(text) * 2 + 32;
    int at_line_start = 1;

    if (!out) return NULL;
    out[0] = '\0';

    for (size_t i = 0; text[i]; i++) {
        char c = text[i];
        if (at_line_start && c == '#') {
            while (text[i] == '#') i++;
            while (text[i] == ' ') i++;
            append_char(&out, &len, &cap, '\n');
            at_line_start = 0;
            c = text[i];
        }
        if (at_line_start && (c == '-' || c == '*')) {
            append_char(&out, &len, &cap, ' ');
            append_char(&out, &len, &cap, '-');
            append_char(&out, &len, &cap, ' ');
            while (text[i + 1] == ' ') i++;
            at_line_start = 0;
            continue;
        }
        if (c == '\r') continue;
        if (c == '\n') {
            if (len > 0 && out[len - 1] != '\n') append_char(&out, &len, &cap, '\n');
            at_line_start = 1;
            continue;
        }
        if (c == '`') continue;
        append_char(&out, &len, &cap, c);
        at_line_start = 0;
    }

    return out;
}

static int command_exists(const char *name) {
    char *path = getenv("PATH");
    char *copy;
    char *dir;
    char *saveptr = NULL;
    char candidate[PATH_MAX];
    if (!path) return 0;
    copy = strdup(path);
    if (!copy) return 0;
    dir = strtok_r(copy, ":", &saveptr);
    while (dir) {
        snprintf(candidate, sizeof(candidate), "%s/%s", dir, name);
        if (access(candidate, X_OK) == 0) {
            free(copy);
            return 1;
        }
        dir = strtok_r(NULL, ":", &saveptr);
    }
    free(copy);
    return 0;
}

static int run_piper(const char *voice_model,
                     const char *narration_path,
                     const char *audio_path,
                     const char *render_log_path) {
    FILE *log = fopen(render_log_path, "w");
    pid_t pid;
    int status = 0;
    if (!log) return 1;

    pid = fork();
    if (pid < 0) {
        fclose(log);
        return 1;
    }
    if (pid == 0) {
        int fd = fileno(log);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        execlp("piper",
               "piper",
               "--model", voice_model,
               "--output_file", audio_path,
               "--file", narration_path,
               (char *)NULL);
        _exit(127);
    }

    waitpid(pid, &status, 0);
    fclose(log);
    return (WIFEXITED(status) && WEXITSTATUS(status) == 0) ? 0 : 1;
}

static void usage(void) {
    fprintf(stderr,
            "Usage: bonfyre-narrate <artifact-text> <output-dir> "
            "[--voice-model PATH] [--audio-format wav] [--title TITLE] [--dry-run]\n");
}

int main(int argc, char **argv) {
    const char *source_path;
    const char *output_dir;
    const char *voice_model = "";
    const char *audio_format = "wav";
    const char *title = "Bonfyre Artifact";
    char *source_text = NULL;
    char *narration_text = NULL;
    char narration_path[PATH_MAX];
    char audio_path[PATH_MAX];
    char manifest_path[PATH_MAX];
    char render_log_path[PATH_MAX];
    char timestamp[32];
    const char *render_status = "skipped";
    const char *render_reason = "piper_unavailable";
    int dry_run = 0;
    FILE *manifest;

    if (argc < 3) {
        usage();
        return 1;
    }

    source_path = argv[1];
    output_dir = argv[2];
    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "--voice-model") == 0 && i + 1 < argc) {
            voice_model = argv[++i];
        } else if (strcmp(argv[i], "--audio-format") == 0 && i + 1 < argc) {
            audio_format = argv[++i];
        } else if (strcmp(argv[i], "--title") == 0 && i + 1 < argc) {
            title = argv[++i];
        } else if (strcmp(argv[i], "--dry-run") == 0) {
            dry_run = 1;
        } else {
            usage();
            return 1;
        }
    }

    if (ensure_dir(output_dir) != 0) {
        fprintf(stderr, "Failed to create output dir: %s\n", output_dir);
        return 1;
    }

    snprintf(narration_path, sizeof(narration_path), "%s/narration.txt", output_dir);
    snprintf(audio_path, sizeof(audio_path), "%s/artifact.%s", output_dir, audio_format);
    snprintf(manifest_path, sizeof(manifest_path), "%s/artifact.manifest.json", output_dir);
    snprintf(render_log_path, sizeof(render_log_path), "%s/render.log", output_dir);

    if (dry_run) {
        printf("Would narrate artifact: %s -> %s\n", source_path, output_dir);
        return 0;
    }

    source_text = read_file(source_path);
    if (!source_text) {
        fprintf(stderr, "Missing artifact text: %s\n", source_path);
        return 2;
    }

    narration_text = normalize_for_narration(source_text);
    if (!narration_text) {
        free(source_text);
        return 1;
    }

    if (write_file(narration_path, narration_text) != 0) {
        fprintf(stderr, "Failed to write narration text\n");
        free(source_text);
        free(narration_text);
        return 1;
    }

    if (command_exists("piper") && voice_model[0] != '\0') {
        if (run_piper(voice_model, narration_path, audio_path, render_log_path) == 0) {
            render_status = "completed";
            render_reason = "rendered";
        } else {
            render_status = "failed";
            render_reason = "piper_render_failed";
        }
    } else if (command_exists("piper")) {
        render_status = "skipped";
        render_reason = "missing_voice_model";
    }

    iso_timestamp(timestamp, sizeof(timestamp));
    manifest = fopen(manifest_path, "w");
    if (!manifest) {
        free(source_text);
        free(narration_text);
        return 1;
    }

    fprintf(manifest,
            "{\n"
            "  \"sourceSystem\": \"BonfyreNarrate\",\n"
            "  \"artifactType\": \"narrated-artifact\",\n"
            "  \"title\": \"%s\",\n"
            "  \"createdAt\": \"%s\",\n"
            "  \"sourceTextPath\": \"%s\",\n"
            "  \"narrationTextPath\": \"%s\",\n"
            "  \"audioPath\": \"%s\",\n"
            "  \"audioFormat\": \"%s\",\n"
            "  \"voiceModel\": \"%s\",\n"
            "  \"renderStatus\": \"%s\",\n"
            "  \"renderReason\": \"%s\",\n"
            "  \"renderLogPath\": \"%s\"\n"
            "}\n",
            title,
            timestamp,
            source_path,
            narration_path,
            audio_path,
            audio_format,
            voice_model,
            render_status,
            render_reason,
            render_log_path);
    fclose(manifest);

    printf("Narration text: %s\n", narration_path);
    printf("Manifest: %s\n", manifest_path);
    if (strcmp(render_status, "completed") == 0) printf("Audio: %s\n", audio_path);

    free(source_text);
    free(narration_text);
    return 0;
}
