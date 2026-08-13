/*
 * bonfyre-video-demux
 *
 * Thin ffmpeg wrapper that separates an input video into:
 * - <out>/video.mp4
 * - <out>/audio.wav
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
            "bonfyre-video-demux\n\n"
            "Usage:\n"
            "  bonfyre-video-demux --input <video> --out <dir>\n");
}

int main(int argc, char **argv) {
    const char *input = NULL;
    const char *out_dir = NULL;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--input") == 0 && i + 1 < argc) {
            input = argv[++i];
        } else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) {
            out_dir = argv[++i];
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
    if (access(input, F_OK) != 0) {
        fprintf(stderr, "Input not found: %s\n", input);
        return 1;
    }
    if (ensure_dir(out_dir) != 0) {
        fprintf(stderr, "Failed to create output dir: %s (%s)\n",
                out_dir, strerror(errno));
        return 1;
    }

    char video_out[PATH_MAX];
    char audio_out[PATH_MAX];
    snprintf(video_out, sizeof(video_out), "%s/video.mp4", out_dir);
    snprintf(audio_out, sizeof(audio_out), "%s/audio.wav", out_dir);

    char *const ffmpeg_argv[] = {
        "ffmpeg",
        "-y",
        "-i", (char *)input,
        "-map", "0:v:0",
        "-c:v", "copy",
        "-an",
        video_out,
        "-map", "0:a:0?",
        "-vn",
        "-acodec", "pcm_s16le",
        audio_out,
        NULL
    };

    return run_process(ffmpeg_argv);
}
