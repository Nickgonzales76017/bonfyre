/*
 * bonfyre-frame-extract
 *
 * Thin ffmpeg wrapper that extracts frames at a target FPS.
 */

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static int ensure_dir(const char *path) {
    char tmp[PATH_MAX];
    size_t len = 0;

    if (!path || !path[0]) return -1;
    snprintf(tmp, sizeof(tmp), "%s", path);
    len = strlen(tmp);
    if (len > 1 && tmp[len - 1] == '/') tmp[len - 1] = '\0';

    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return -1;
            *p = '/';
        }
    }
    if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return -1;
    return 0;
}

static int run_process(char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return 1;
    }
    if (pid == 0) {
        execvp(argv[0], argv);
        perror(argv[0]);
        _exit(127);
    }

    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
        perror("waitpid");
        return 1;
    }
    if (WIFEXITED(status)) return WEXITSTATUS(status);
    return 1;
}

static void usage(void) {
    fprintf(stderr,
            "bonfyre-frame-extract\n\n"
            "Usage:\n"
            "  bonfyre-frame-extract --input <video> --out <dir> "
            "[--fps N] [--format jpg|png]\n");
}

int main(int argc, char **argv) {
    const char *input = NULL;
    const char *out_dir = NULL;
    const char *fps = "1";
    const char *format = "jpg";

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--input") == 0 && i + 1 < argc) {
            input = argv[++i];
        } else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) {
            out_dir = argv[++i];
        } else if (strcmp(argv[i], "--fps") == 0 && i + 1 < argc) {
            fps = argv[++i];
        } else if (strcmp(argv[i], "--format") == 0 && i + 1 < argc) {
            format = argv[++i];
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            usage();
            return 1;
        }
    }

    if (!input || !out_dir) {
        usage();
        return 1;
    }
    if (strcmp(format, "jpg") != 0 && strcmp(format, "png") != 0) {
        fprintf(stderr, "Unsupported format: %s\n", format);
        return 1;
    }
    if (access(input, F_OK) != 0) {
        fprintf(stderr, "Input not found: %s\n", input);
        return 1;
    }
    if (ensure_dir(out_dir) != 0) {
        fprintf(stderr, "Failed to create output dir: %s (%s)\n",
                out_dir, strerror(errno));
        return 1;
    }

    char fps_filter[64];
    char output_pattern[PATH_MAX];
    snprintf(fps_filter, sizeof(fps_filter), "fps=%s", fps);
    snprintf(output_pattern, sizeof(output_pattern), "%s/frame-%%06d.%s", out_dir, format);

    char *const ffmpeg_argv[] = {
        "ffmpeg",
        "-y",
        "-i", (char *)input,
        "-vf", fps_filter,
        output_pattern,
        NULL
    };

    return run_process(ffmpeg_argv);
}
