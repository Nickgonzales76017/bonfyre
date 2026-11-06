#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

static void print_usage(void) {
    fprintf(stderr,
            "bonfyre-media-prep\n"
            "\n"
            "Usage:\n"
            "  bonfyre-media-prep inspect <input>\n"
            "  bonfyre-media-prep normalize <input> <output> [--sample-rate N] [--channels N] [--trim-silence] [--loudnorm]\n"
            "  bonfyre-media-prep chunk <input> <output-pattern> [--segment-seconds N]\n");
}

static int run_process(char *const argv[]) {
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

    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    }
    return 1;
}

static int command_inspect(const char *input) {
    char *const argv[] = {
        "ffprobe",
        "-v", "error",
        "-show_entries", "format=duration:stream=codec_name,codec_type,sample_rate,channels",
        "-of", "json",
        (char *)input,
        NULL
    };
    return run_process(argv);
}

static int command_normalize(int argc, char **argv) {
    const char *input = argv[2];
    const char *output = argv[3];
    const char *sample_rate = "16000";
    const char *channels = "1";
    int trim_silence = 0;
    int loudnorm = 0;

    for (int i = 4; i < argc; i++) {
        if (strcmp(argv[i], "--sample-rate") == 0 && i + 1 < argc) {
            sample_rate = argv[++i];
        } else if (strcmp(argv[i], "--channels") == 0 && i + 1 < argc) {
            channels = argv[++i];
        } else if (strcmp(argv[i], "--trim-silence") == 0) {
            trim_silence = 1;
        } else if (strcmp(argv[i], "--loudnorm") == 0) {
            loudnorm = 1;
        } else {
            fprintf(stderr, "Unknown normalize option: %s\n", argv[i]);
            return 1;
        }
    }

    char filters[512] = {0};
    if (trim_silence) {
        strncat(filters, "silenceremove=start_periods=1:start_silence=0.3:start_threshold=-35dB", sizeof(filters) - strlen(filters) - 1);
    }
    if (loudnorm) {
        if (filters[0] != '\0') {
            strncat(filters, ",", sizeof(filters) - strlen(filters) - 1);
        }
        strncat(filters, "loudnorm=I=-16:TP=-1.5:LRA=11", sizeof(filters) - strlen(filters) - 1);
    }

    char *ffmpeg_argv[20];
    int idx = 0;
    ffmpeg_argv[idx++] = "ffmpeg";
    ffmpeg_argv[idx++] = "-y";
    ffmpeg_argv[idx++] = "-i";
    ffmpeg_argv[idx++] = (char *)input;
    ffmpeg_argv[idx++] = "-ar";
    ffmpeg_argv[idx++] = (char *)sample_rate;
    ffmpeg_argv[idx++] = "-ac";
    ffmpeg_argv[idx++] = (char *)channels;
    if (filters[0] != '\0') {
        ffmpeg_argv[idx++] = "-af";
        ffmpeg_argv[idx++] = filters;
    }
    ffmpeg_argv[idx++] = (char *)output;
    ffmpeg_argv[idx] = NULL;

    return run_process(ffmpeg_argv);
}

static int command_chunk(int argc, char **argv) {
    const char *input = argv[2];
    const char *pattern = argv[3];
    const char *segment_seconds = "300";

    for (int i = 4; i < argc; i++) {
        if (strcmp(argv[i], "--segment-seconds") == 0 && i + 1 < argc) {
            segment_seconds = argv[++i];
        } else {
            fprintf(stderr, "Unknown chunk option: %s\n", argv[i]);
            return 1;
        }
    }

    char *const ffmpeg_argv[] = {
        "ffmpeg",
        "-y",
        "-i", (char *)input,
        "-f", "segment",
        "-segment_time", (char *)segment_seconds,
        "-c", "copy",
        (char *)pattern,
        NULL
    };
    return run_process(ffmpeg_argv);
}

int main(int argc, char **argv) {
    if (argc < 3) {
        print_usage();
        return 1;
    }

    if (strcmp(argv[1], "inspect") == 0) {
        if (argc != 3) {
            print_usage();
            return 1;
        }
        return command_inspect(argv[2]);
    }

    if (strcmp(argv[1], "normalize") == 0) {
        if (argc < 4) {
            print_usage();
            return 1;
        }
        return command_normalize(argc, argv);
    }

    if (strcmp(argv[1], "chunk") == 0) {
        if (argc < 4) {
            print_usage();
            return 1;
        }
        return command_chunk(argc, argv);
    }

    print_usage();
    return 1;
}
