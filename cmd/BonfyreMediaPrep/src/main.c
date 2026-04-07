#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <bonfyre.h>

#define MAX_SILENCES 2048

typedef struct {
    double start;
    double end;
} SilenceRange;

static void print_usage(void) {
    fprintf(stderr,
            "bonfyre-media-prep\n"
            "\n"
            "Usage:\n"
            "  bonfyre-media-prep inspect <input>\n"
            "  bonfyre-media-prep normalize <input> <output> [--sample-rate N] [--channels N] [--trim-silence] [--loudnorm]\n"
            "  bonfyre-media-prep denoise <input> <output>\n"
            "  bonfyre-media-prep chunk <input> <output-pattern> [--segment-seconds N]\n"
            "  bonfyre-media-prep split-speech <input> <output-pattern> [--noise-threshold DB] [--min-silence SEC] [--min-speech SEC] [--padding SEC]\n");
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

static int command_denoise(int argc, char **argv) {
    const char *input = argv[2];
    const char *output = argv[3];
    if (access(output, F_OK) == 0) {
        return 0;
    }
    if (access("/opt/homebrew/bin/rnnoise_demo", X_OK) == 0) {
        char *const rnnoise_argv[] = {
            "/opt/homebrew/bin/rnnoise_demo",
            (char *)input,
            (char *)output,
            NULL
        };
        return run_process(rnnoise_argv);
    }
    if (access("/usr/local/bin/rnnoise_demo", X_OK) == 0) {
        char *const rnnoise_argv[] = {
            "/usr/local/bin/rnnoise_demo",
            (char *)input,
            (char *)output,
            NULL
        };
        return run_process(rnnoise_argv);
    }
    if (access("rnnoise_demo", X_OK) == 0) {
        char *const rnnoise_argv[] = {
            "rnnoise_demo",
            (char *)input,
            (char *)output,
            NULL
        };
        return run_process(rnnoise_argv);
    }
    char *const fallback_argv[] = {
        "cp",
        (char *)input,
        (char *)output,
        NULL
    };
    return run_process(fallback_argv);
}

static double parse_time_hms(const char *value) {
    int hours = 0;
    int minutes = 0;
    double seconds = 0.0;
    if (sscanf(value, "%d:%d:%lf", &hours, &minutes, &seconds) != 3) {
        return -1.0;
    }
    return (hours * 3600.0) + (minutes * 60.0) + seconds;
}

static int extract_output_dir(char *buffer, size_t size, const char *pattern) {
    const char *last_slash = strrchr(pattern, '/');
    if (!last_slash) {
        snprintf(buffer, size, ".");
        return 0;
    }
    size_t len = (size_t)(last_slash - pattern);
    if (len + 1 > size) return 1;
    memcpy(buffer, pattern, len);
    buffer[len] = '\0';
    return 0;
}

static int ensure_dir_recursive(const char *path) { return bf_ensure_dir(path); }
static int extract_segment(const char *input, const char *output, double start_time, double end_time) {
    char start_arg[64];
    char end_arg[64];
    snprintf(start_arg, sizeof(start_arg), "%.3f", start_time);
    snprintf(end_arg, sizeof(end_arg), "%.3f", end_time);
    char *const ffmpeg_argv[] = {
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel", "error",
        "-ss", start_arg,
        "-to", end_arg,
        "-i", (char *)input,
        "-ar", "16000",
        "-ac", "1",
        (char *)output,
        NULL
    };
    return run_process(ffmpeg_argv);
}

static int write_split_manifest(
    const char *output_dir,
    const char *input,
    const char *noise_threshold,
    double min_silence,
    double min_speech,
    double padding,
    char chunk_paths[][PATH_MAX],
    int chunk_count
) {
    char manifest_path[PATH_MAX];
    snprintf(manifest_path, sizeof(manifest_path), "%s/speech-chunks.json", output_dir);

    FILE *fp = fopen(manifest_path, "w");
    if (!fp) {
        perror("fopen");
        return 1;
    }

    fprintf(fp,
            "{\n"
            "  \"sourceSystem\": \"BonfyreMediaPrep\",\n"
            "  \"sourceAudio\": \"%s\",\n"
            "  \"noiseThreshold\": \"%s\",\n"
            "  \"minSilence\": %.3f,\n"
            "  \"minSpeech\": %.3f,\n"
            "  \"padding\": %.3f,\n"
            "  \"chunkCount\": %d,\n"
            "  \"chunks\": [\n",
            input,
            noise_threshold,
            min_silence,
            min_speech,
            padding,
            chunk_count);

    for (int i = 0; i < chunk_count; i++) {
        fprintf(fp,
                "    {\n"
                "      \"index\": %d,\n"
                "      \"path\": \"%s\"\n"
                "    }%s\n",
                i,
                chunk_paths[i],
                i == chunk_count - 1 ? "" : ",");
    }

    fprintf(fp, "  ]\n}\n");
    fclose(fp);
    return 0;
}

static int command_split_speech(int argc, char **argv) {
    const char *input = argv[2];
    const char *pattern = argv[3];
    const char *noise_threshold = "-35dB";
    double min_silence = 0.35;
    double min_speech = 0.75;
    double padding = 0.15;

    for (int i = 4; i < argc; i++) {
        if (strcmp(argv[i], "--noise-threshold") == 0 && i + 1 < argc) {
            noise_threshold = argv[++i];
        } else if (strcmp(argv[i], "--min-silence") == 0 && i + 1 < argc) {
            min_silence = atof(argv[++i]);
        } else if (strcmp(argv[i], "--min-speech") == 0 && i + 1 < argc) {
            min_speech = atof(argv[++i]);
        } else if (strcmp(argv[i], "--padding") == 0 && i + 1 < argc) {
            padding = atof(argv[++i]);
        } else {
            fprintf(stderr, "Unknown split-speech option: %s\n", argv[i]);
            return 1;
        }
    }

    char output_dir[PATH_MAX];
    if (extract_output_dir(output_dir, sizeof(output_dir), pattern) != 0) {
        fprintf(stderr, "Failed to derive output directory.\n");
        return 1;
    }
    if (ensure_dir_recursive(output_dir) != 0) {
        fprintf(stderr, "Failed to create output directory: %s\n", output_dir);
        return 1;
    }

    char command[PATH_MAX * 2];
    snprintf(command, sizeof(command),
             "ffmpeg -hide_banner -i \"%s\" -af silencedetect=n=%s:d=%.3f -f null - 2>&1",
             input,
             noise_threshold,
             min_silence);

    FILE *pipe = popen(command, "r");
    if (!pipe) {
        perror("popen");
        return 1;
    }

    SilenceRange silences[MAX_SILENCES];
    char chunk_paths[MAX_SILENCES][PATH_MAX];
    int silence_count = 0;
    double current_silence_start = -1.0;
    double duration = -1.0;
    char line[4096];

    while (fgets(line, sizeof(line), pipe)) {
        char *duration_pos = strstr(line, "Duration:");
        if (duration_pos) {
            duration_pos += strlen("Duration:");
            while (*duration_pos == ' ') duration_pos++;
            duration = parse_time_hms(duration_pos);
        }

        char *start_pos = strstr(line, "silence_start:");
        if (start_pos) {
            start_pos += strlen("silence_start:");
            current_silence_start = atof(start_pos);
        }

        char *end_pos = strstr(line, "silence_end:");
        if (end_pos && silence_count < MAX_SILENCES) {
            end_pos += strlen("silence_end:");
            silences[silence_count].start = current_silence_start >= 0.0 ? current_silence_start : 0.0;
            silences[silence_count].end = atof(end_pos);
            silence_count++;
            current_silence_start = -1.0;
        }
    }
    pclose(pipe);

    if (duration <= 0.0) {
        fprintf(stderr, "Unable to determine audio duration for speech splitting.\n");
        return 1;
    }

    double speech_start = 0.0;
    int chunk_index = 0;

    for (int i = 0; i < silence_count; i++) {
        double speech_end = silences[i].start;
        double clipped_start = speech_start - padding;
        double clipped_end = speech_end + padding;
        if (clipped_start < 0.0) clipped_start = 0.0;
        if (clipped_end > duration) clipped_end = duration;
        if ((clipped_end - clipped_start) >= min_speech) {
            char output_path[PATH_MAX];
            snprintf(output_path, sizeof(output_path), pattern, chunk_index);
            if (extract_segment(input, output_path, clipped_start, clipped_end) != 0) {
                fprintf(stderr, "Failed to extract speech segment %d.\n", chunk_index);
                return 1;
            }
            snprintf(chunk_paths[chunk_index], sizeof(chunk_paths[chunk_index]), "%s", output_path);
            printf("%s\n", output_path);
            chunk_index++;
        }
        speech_start = silences[i].end;
    }

    double clipped_start = speech_start - padding;
    double clipped_end = duration;
    if (clipped_start < 0.0) clipped_start = 0.0;
    if ((clipped_end - clipped_start) >= min_speech) {
        char output_path[PATH_MAX];
        snprintf(output_path, sizeof(output_path), pattern, chunk_index);
        if (extract_segment(input, output_path, clipped_start, clipped_end) != 0) {
            fprintf(stderr, "Failed to extract final speech segment.\n");
            return 1;
        }
        snprintf(chunk_paths[chunk_index], sizeof(chunk_paths[chunk_index]), "%s", output_path);
        printf("%s\n", output_path);
        chunk_index++;
    }

    if (chunk_index == 0) {
        char output_path[PATH_MAX];
        snprintf(output_path, sizeof(output_path), pattern, 0);
        if (extract_segment(input, output_path, 0.0, duration) != 0) {
            fprintf(stderr, "Failed to extract fallback full-length segment.\n");
            return 1;
        }
        snprintf(chunk_paths[0], sizeof(chunk_paths[0]), "%s", output_path);
        printf("%s\n", output_path);
        chunk_index = 1;
    }

    if (write_split_manifest(
            output_dir,
            input,
            noise_threshold,
            min_silence,
            min_speech,
            padding,
            chunk_paths,
            chunk_index
        ) != 0) {
        fprintf(stderr, "Failed to write speech split manifest.\n");
        return 1;
    }

    return 0;
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

    if (strcmp(argv[1], "denoise") == 0) {
        if (argc != 4) {
            print_usage();
            return 1;
        }
        return command_denoise(argc, argv);
    }

    if (strcmp(argv[1], "split-speech") == 0) {
        if (argc < 4) {
            print_usage();
            return 1;
        }
        return command_split_speech(argc, argv);
    }

    print_usage();
    return 1;
}
