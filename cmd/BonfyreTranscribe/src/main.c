#include <errno.h>
#include <dirent.h>
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
    if (WIFEXITED(status)) return WEXITSTATUS(status);
    return 1;
}

static int copy_file_to_stream(const char *path, FILE *out) {
    FILE *in = fopen(path, "rb");
    if (!in) {
        perror("fopen");
        return 1;
    }
    char buffer[4096];
    size_t bytes = 0;
    while ((bytes = fread(buffer, 1, sizeof(buffer), in)) > 0) {
        if (fwrite(buffer, 1, bytes, out) != bytes) {
            fclose(in);
            return 1;
        }
    }
    fclose(in);
    return 0;
}

static int write_chunk_progress(const char *path, int total_chunks, int completed_chunks, const char *status) {
    FILE *fp = fopen(path, "w");
    if (!fp) {
        perror("fopen");
        return 1;
    }
    fprintf(fp,
            "{\n"
            "  \"sourceSystem\": \"BonfyreTranscribe\",\n"
            "  \"status\": \"%s\",\n"
            "  \"totalChunks\": %d,\n"
            "  \"completedChunks\": %d\n"
            "}\n",
            status,
            total_chunks,
            completed_chunks);
    fclose(fp);
    return 0;
}

static void iso_timestamp(char *buffer, size_t size) {
    time_t now = time(NULL);
    struct tm tm_utc;
    gmtime_r(&now, &tm_utc);
    strftime(buffer, size, "%Y-%m-%dT%H:%M:%SZ", &tm_utc);
}

static const char *default_whisper_binary(void) {
    const char *env = getenv("BONFYRE_WHISPER_BINARY");
    if (env && env[0] != '\0') return env;
    if (access("/Users/nickgonzales/Library/Python/3.9/bin/whisper", X_OK) == 0) {
        return "/Users/nickgonzales/Library/Python/3.9/bin/whisper";
    }
    return "whisper";
}

static void resolve_executable_sibling(char *buffer, size_t size, const char *argv0, const char *sibling_dir, const char *binary_name) {
    if (argv0 && argv0[0] == '/') {
        snprintf(buffer, size, "%s", argv0);
    } else if (argv0 && strstr(argv0, "/")) {
        char cwd[PATH_MAX];
        if (getcwd(cwd, sizeof(cwd))) {
            snprintf(buffer, size, "%s/%s", cwd, argv0);
        } else {
            snprintf(buffer, size, "%s", argv0);
        }
    } else {
        buffer[0] = '\0';
        return;
    }

    char *last_slash = strrchr(buffer, '/');
    if (!last_slash) {
        buffer[0] = '\0';
        return;
    }
    *last_slash = '\0';
    last_slash = strrchr(buffer, '/');
    if (!last_slash) {
        buffer[0] = '\0';
        return;
    }
    *last_slash = '\0';
    snprintf(buffer, size, "%s/%s/%s", buffer, sibling_dir, binary_name);
}

static const char *default_media_prep_binary(const char *argv0, char *resolved_path, size_t resolved_size) {
    const char *env = getenv("BONFYRE_MEDIA_PREP_BINARY");
    if (env && env[0] != '\0') return env;
    resolve_executable_sibling(resolved_path, resolved_size, argv0, "BonfyreMediaPrep", "bonfyre-media-prep");
    if (resolved_path[0] != '\0' && access(resolved_path, X_OK) == 0) {
        return resolved_path;
    }
    return "../BonfyreMediaPrep/bonfyre-media-prep";
}

static const char *default_silero_vad_script(const char *argv0, char *resolved_path, size_t resolved_size) {
    const char *env = getenv("BONFYRE_SILERO_VAD_CLI");
    if (env && env[0] != '\0') return env;
    resolve_executable_sibling(resolved_path, resolved_size, argv0, "SileroVADCLI", "bin/silero_vad_cli.py");
    if (resolved_path[0] != '\0' && access(resolved_path, F_OK) == 0) {
        return resolved_path;
    }
    return "../SileroVADCLI/bin/silero_vad_cli.py";
}

static const char *path_basename(const char *path) {
    const char *slash = strrchr(path, '/');
    return slash ? slash + 1 : path;
}

static void strip_extension(char *name) {
    char *dot = strrchr(name, '.');
    if (dot) *dot = '\0';
}

static void print_usage(void) {
    fprintf(stderr,
            "bonfyre-transcribe\n\n"
            "Usage:\n"
            "  bonfyre-transcribe <input-audio> <output-dir> [--model NAME] [--language CODE]\n"
            "                      [--whisper-binary PATH] [--media-prep-binary PATH]\n"
            "                      [--silero-vad] [--silero-script PATH]\n"
            "                      [--split-speech] [--noise-threshold DB] [--min-silence SEC]\n"
            "                      [--min-speech SEC] [--padding SEC]\n");
}

int main(int argc, char **argv) {
    if (argc < 3) {
        print_usage();
        return 1;
    }

    const char *input_audio = argv[1];
    const char *output_dir = argv[2];
    const char *model = "base";
    const char *language = NULL;
    const char *whisper_binary = default_whisper_binary();
    int split_speech = 0;
    const char *noise_threshold = "-35dB";
    const char *min_silence = "0.35";
    const char *min_speech = "0.75";
    const char *padding = "0.15";
    int silero_vad = 0;
    char resolved_media_prep[PATH_MAX];
    char resolved_silero_script[PATH_MAX];
    const char *media_prep_binary = default_media_prep_binary(argv[0], resolved_media_prep, sizeof(resolved_media_prep));
    const char *silero_script = default_silero_vad_script(argv[0], resolved_silero_script, sizeof(resolved_silero_script));

    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "--model") == 0 && i + 1 < argc) {
            model = argv[++i];
        } else if (strcmp(argv[i], "--language") == 0 && i + 1 < argc) {
            language = argv[++i];
        } else if (strcmp(argv[i], "--whisper-binary") == 0 && i + 1 < argc) {
            whisper_binary = argv[++i];
        } else if (strcmp(argv[i], "--media-prep-binary") == 0 && i + 1 < argc) {
            media_prep_binary = argv[++i];
        } else if (strcmp(argv[i], "--silero-vad") == 0) {
            silero_vad = 1;
        } else if (strcmp(argv[i], "--silero-script") == 0 && i + 1 < argc) {
            silero_script = argv[++i];
        } else if (strcmp(argv[i], "--split-speech") == 0) {
            split_speech = 1;
        } else if (strcmp(argv[i], "--noise-threshold") == 0 && i + 1 < argc) {
            noise_threshold = argv[++i];
        } else if (strcmp(argv[i], "--min-silence") == 0 && i + 1 < argc) {
            min_silence = argv[++i];
        } else if (strcmp(argv[i], "--min-speech") == 0 && i + 1 < argc) {
            min_speech = argv[++i];
        } else if (strcmp(argv[i], "--padding") == 0 && i + 1 < argc) {
            padding = argv[++i];
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            return 1;
        }
    }

    if (ensure_dir(output_dir) != 0) {
        fprintf(stderr, "Failed to create output dir: %s\n", output_dir);
        return 1;
    }

    char normalized_path[PATH_MAX];
    char transcript_path[PATH_MAX];
    char meta_path[PATH_MAX];
    char status_path[PATH_MAX];
    char progress_path[PATH_MAX];
    char base_name[PATH_MAX];
    int chunk_count = 0;
    int completed_chunks = 0;
    int denoised = 0;

    snprintf(base_name, sizeof(base_name), "%s", path_basename(input_audio));
    strip_extension(base_name);
    snprintf(normalized_path, sizeof(normalized_path), "%s/normalized.wav", output_dir);
    snprintf(transcript_path, sizeof(transcript_path), "%s/normalized.txt", output_dir);
    snprintf(meta_path, sizeof(meta_path), "%s/meta.json", output_dir);
    snprintf(status_path, sizeof(status_path), "%s/transcribe-status.json", output_dir);
    snprintf(progress_path, sizeof(progress_path), "%s/chunk-progress.json", output_dir);

    char *normalize_argv[] = {
        (char *)media_prep_binary,
        "normalize",
        (char *)input_audio,
        normalized_path,
        "--sample-rate", "16000",
        "--channels", "1",
        NULL
    };

    if (run_process(normalize_argv) != 0) {
        fprintf(stderr, "Normalize failed.\n");
        return 1;
    }

    char denoised_path[PATH_MAX];
    snprintf(denoised_path, sizeof(denoised_path), "%s/normalized.denoised.wav", output_dir);
    char *denoise_argv[] = {
        (char *)media_prep_binary,
        "denoise",
        normalized_path,
        denoised_path,
        NULL
    };
    if (run_process(denoise_argv) == 0 && access(denoised_path, F_OK) == 0) {
        snprintf(normalized_path, sizeof(normalized_path), "%s", denoised_path);
        denoised = 1;
    }

    if (split_speech) {
        char chunk_dir[PATH_MAX];
        char chunk_pattern[PATH_MAX];
        snprintf(chunk_dir, sizeof(chunk_dir), "%s/chunks", output_dir);
        snprintf(chunk_pattern, sizeof(chunk_pattern), "%s/chunk-%%03d.wav", chunk_dir);

        if (ensure_dir(chunk_dir) != 0) {
            fprintf(stderr, "Failed to create chunk dir: %s\n", chunk_dir);
            return 1;
        }

        if (silero_vad && access(silero_script, F_OK) == 0) {
            char *silero_argv[] = {
                "python3",
                (char *)silero_script,
                "--audio",
                normalized_path,
                "--out",
                chunk_dir,
                "--min-speech",
                (char *)min_speech,
                "--padding",
                (char *)padding,
                NULL
            };
            if (run_process(silero_argv) != 0) {
                fprintf(stderr, "Silero VAD split failed.\n");
                return 1;
            }
        } else {
            char *split_argv[] = {
                (char *)media_prep_binary,
                "split-speech",
                normalized_path,
                chunk_pattern,
                "--noise-threshold", (char *)noise_threshold,
                "--min-silence", (char *)min_silence,
                "--min-speech", (char *)min_speech,
                "--padding", (char *)padding,
                NULL
            };

            if (run_process(split_argv) != 0) {
                fprintf(stderr, "Speech split failed.\n");
                return 1;
            }
        }

        for (int i = 0;; i++) {
            char chunk_audio[PATH_MAX];
            snprintf(chunk_audio, sizeof(chunk_audio), "%s/chunk-%03d.wav", chunk_dir, i);
            if (access(chunk_audio, F_OK) != 0) break;
            chunk_count++;
        }
        write_chunk_progress(progress_path, chunk_count, 0, "splitting-complete");

        FILE *combined = fopen(transcript_path, "w");
        if (!combined) {
            perror("fopen transcript");
            return 1;
        }

        for (int i = 0; i < chunk_count; i++) {
            char chunk_audio[PATH_MAX];
            char chunk_txt[PATH_MAX];
            snprintf(chunk_audio, sizeof(chunk_audio), "%s/chunk-%03d.wav", chunk_dir, i);
            snprintf(chunk_txt, sizeof(chunk_txt), "%s/chunk-%03d.txt", chunk_dir, i);

            char *whisper_argv[16];
            int idx = 0;
            whisper_argv[idx++] = (char *)whisper_binary;
            whisper_argv[idx++] = chunk_audio;
            whisper_argv[idx++] = "--task";
            whisper_argv[idx++] = "transcribe";
            whisper_argv[idx++] = "--model";
            whisper_argv[idx++] = (char *)model;
            whisper_argv[idx++] = "--output_format";
            whisper_argv[idx++] = "txt";
            whisper_argv[idx++] = "--output_dir";
            whisper_argv[idx++] = chunk_dir;
            if (language) {
                whisper_argv[idx++] = "--language";
                whisper_argv[idx++] = (char *)language;
            }
            whisper_argv[idx] = NULL;

            if (run_process(whisper_argv) != 0) {
                fclose(combined);
                fprintf(stderr, "Whisper failed on chunk %d.\n", i);
                return 1;
            }
            if (access(chunk_txt, F_OK) != 0) {
                fclose(combined);
                fprintf(stderr, "Expected chunk transcript not found: %s\n", chunk_txt);
                return 1;
            }

            if (copy_file_to_stream(chunk_txt, combined) != 0) {
                fclose(combined);
                fprintf(stderr, "Failed to append chunk transcript.\n");
                return 1;
            }
            fprintf(combined, "\n");
            completed_chunks++;
            write_chunk_progress(progress_path, chunk_count, completed_chunks, "transcribing");
        }
        fclose(combined);
        write_chunk_progress(progress_path, chunk_count, completed_chunks, "completed");
    } else {
        char *whisper_argv[16];
        int idx = 0;
        whisper_argv[idx++] = (char *)whisper_binary;
        whisper_argv[idx++] = normalized_path;
        whisper_argv[idx++] = "--task";
        whisper_argv[idx++] = "transcribe";
        whisper_argv[idx++] = "--model";
        whisper_argv[idx++] = (char *)model;
        whisper_argv[idx++] = "--output_format";
        whisper_argv[idx++] = "txt";
        whisper_argv[idx++] = "--output_dir";
        whisper_argv[idx++] = (char *)output_dir;
        if (language) {
            whisper_argv[idx++] = "--language";
            whisper_argv[idx++] = (char *)language;
        }
        whisper_argv[idx] = NULL;

        if (run_process(whisper_argv) != 0) {
            fprintf(stderr, "Whisper failed.\n");
            return 1;
        }

        if (access(transcript_path, F_OK) != 0) {
            fprintf(stderr, "Expected transcript not found: %s\n", transcript_path);
            return 1;
        }
        write_chunk_progress(progress_path, 1, 1, "completed");
    }

    char timestamp[32];
    iso_timestamp(timestamp, sizeof(timestamp));
    char language_json[256];
    if (language) {
        snprintf(language_json, sizeof(language_json), "\"%s\"", language);
    } else {
        snprintf(language_json, sizeof(language_json), "null");
    }

    FILE *meta = fopen(meta_path, "w");
    if (!meta) {
        perror("fopen meta");
        return 1;
    }
    fprintf(meta,
            "{\n"
            "  \"source_system\": \"BonfyreTranscribe\",\n"
            "  \"created_at\": \"%s\",\n"
            "  \"input_audio\": \"%s\",\n"
            "  \"normalized_audio\": \"%s\",\n"
            "  \"transcript_path\": \"%s\",\n"
            "  \"model\": \"%s\",\n"
            "  \"language\": %s,\n"
            "  \"split_speech\": %s,\n"
            "  \"silero_vad\": %s,\n"
            "  \"denoised\": %s,\n"
            "  \"chunk_count\": %d,\n"
            "  \"chunk_progress_path\": \"%s\",\n"
            "  \"whisper_binary\": \"%s\",\n"
            "  \"media_prep_binary\": \"%s\"\n"
            "}\n",
            timestamp,
            input_audio,
            normalized_path,
            transcript_path,
            model,
            language_json,
            split_speech ? "true" : "false",
            silero_vad ? "true" : "false",
            denoised ? "true" : "false",
            chunk_count,
            progress_path,
            whisper_binary,
            media_prep_binary);
    fclose(meta);

    FILE *status = fopen(status_path, "w");
    if (!status) {
        perror("fopen status");
        return 1;
    }
    fprintf(status,
            "{\n"
            "  \"sourceSystem\": \"BonfyreTranscribe\",\n"
            "  \"exportedAt\": \"%s\",\n"
            "  \"status\": \"transcribed\",\n"
            "  \"jobSlug\": \"%s\",\n"
            "  \"splitSpeech\": %s,\n"
            "  \"sileroVad\": %s,\n"
            "  \"denoised\": %s,\n"
            "  \"chunkCount\": %d,\n"
            "  \"chunkProgressPath\": \"%s\",\n"
            "  \"transcriptPath\": \"%s\",\n"
            "  \"metaPath\": \"%s\"\n"
            "}\n",
            timestamp,
            base_name,
            split_speech ? "true" : "false",
            silero_vad ? "true" : "false",
            denoised ? "true" : "false",
            chunk_count,
            progress_path,
            transcript_path,
            meta_path);
    fclose(status);

    printf("Normalized: %s\n", normalized_path);
    printf("Transcript: %s\n", transcript_path);
    printf("Meta: %s\n", meta_path);
    printf("Status: %s\n", status_path);
    printf("Progress: %s\n", progress_path);
    return 0;
}
