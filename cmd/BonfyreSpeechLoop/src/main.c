/*
 * BonfyreSpeechLoop — Whisper → Transform → Piper speech transformation
 *
 * Layer: Transform (orchestrator — chains pure operators)
 *
 * The speech transformation engine: takes audio, transcribes it,
 * transforms the text (clean, compress, rewrite), then synthesizes
 * new audio via Piper. Input speech → structured text → new speech.
 *
 * Usage:
 *   bonfyre-speechloop transform <input-audio> <out-dir> [--voice-model PATH]
 *                      [--model whisper-model] [--compress] [--brief]
 *   bonfyre-speechloop narrate-brief <brief-dir> <out-dir> [--voice-model PATH]
 *
 * Chains: transcribe → clean → [paragraph → brief] → narrate
 */

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* ── Utilities ────────────────────────────────────────────────────── */

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

static void iso_timestamp(char *buf, size_t sz) {
    time_t now = time(NULL);
    struct tm t;
    gmtime_r(&now, &t);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &t);
}

static int run_process(char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return 1; }
    if (pid == 0) {
        execvp(argv[0], argv);
        perror(argv[0]);
        _exit(127);
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) { perror("waitpid"); return 1; }
    if (WIFEXITED(status)) return WEXITSTATUS(status);
    return 1;
}

/* ── Binary resolution ────────────────────────────────────────────── */

static void resolve_sibling(char *buf, size_t sz, const char *argv0,
                            const char *dir, const char *name) {
    if (argv0 && argv0[0] == '/') {
        snprintf(buf, sz, "%s", argv0);
    } else if (argv0 && strstr(argv0, "/")) {
        char cwd[PATH_MAX];
        if (getcwd(cwd, sizeof(cwd)))
            snprintf(buf, sz, "%s/%s", cwd, argv0);
        else { buf[0] = '\0'; return; }
    } else { buf[0] = '\0'; return; }

    char *last = strrchr(buf, '/');
    if (!last) { buf[0] = '\0'; return; }
    *last = '\0';
    last = strrchr(buf, '/');
    if (!last) { buf[0] = '\0'; return; }
    *last = '\0';
    size_t prefix_len = strlen(buf);
    snprintf(buf + prefix_len, sz - prefix_len, "/%s/%s", dir, name);
}

static const char *find_binary(const char *env_name, const char *argv0,
                               char *resolved, size_t rsz,
                               const char *dir, const char *name,
                               const char *fallback) {
    const char *env = getenv(env_name);
    if (env && env[0]) return env;
    resolve_sibling(resolved, rsz, argv0, dir, name);
    if (resolved[0] && access(resolved, X_OK) == 0) return resolved;
    return fallback;
}

/* ── Write manifest ───────────────────────────────────────────────── */

static int write_manifest(const char *out_dir, const char *mode,
                          const char **steps, int nsteps, int success) {
    char path[PATH_MAX], ts[64];
    snprintf(path, sizeof(path), "%s/speechloop-manifest.json", out_dir);
    iso_timestamp(ts, sizeof(ts));

    FILE *fp = fopen(path, "w");
    if (!fp) return 1;

    fprintf(fp, "{\n");
    fprintf(fp, "  \"source_system\": \"BonfyreSpeechLoop\",\n");
    fprintf(fp, "  \"created_at\": \"%s\",\n", ts);
    fprintf(fp, "  \"mode\": \"%s\",\n", mode);
    fprintf(fp, "  \"status\": \"%s\",\n", success ? "complete" : "failed");
    fprintf(fp, "  \"steps\": [");
    for (int i = 0; i < nsteps; i++) {
        fprintf(fp, "\"%s\"%s", steps[i], i < nsteps - 1 ? ", " : "");
    }
    fprintf(fp, "]\n");
    fprintf(fp, "}\n");

    fclose(fp);
    return 0;
}

/* ── Main ─────────────────────────────────────────────────────────── */

static void print_usage(void) {
    fprintf(stderr,
        "bonfyre-speechloop — whisper → transform → piper speech engine\n\n"
        "Usage:\n"
        "  bonfyre-speechloop transform <input-audio> <out-dir>\n"
        "    [--voice-model PATH] [--model NAME] [--compress] [--brief]\n\n"
        "  bonfyre-speechloop narrate-brief <brief-dir> <out-dir>\n"
        "    [--voice-model PATH]\n\n"
        "The transform loop:\n"
        "  audio → transcribe → clean → [paragraph → brief] → narrate → new audio\n\n"
        "The narrate-brief shortcut:\n"
        "  existing brief.md → narrate → audio recap\n");
}

int main(int argc, char **argv) {
    if (argc < 4) { print_usage(); return 1; }

    const char *mode = argv[1];
    const char *input = argv[2];
    const char *out_dir = argv[3];
    const char *voice_model = NULL;
    const char *whisper_model = "base";
    int do_compress = 0; /* Run transcript-clean */
    int do_brief = 1;    /* Run paragraph + brief */

    for (int i = 4; i < argc; i++) {
        if (strcmp(argv[i], "--voice-model") == 0 && i + 1 < argc)
            voice_model = argv[++i];
        else if (strcmp(argv[i], "--model") == 0 && i + 1 < argc)
            whisper_model = argv[++i];
        else if (strcmp(argv[i], "--compress") == 0)
            do_compress = 1;
        else if (strcmp(argv[i], "--brief") == 0)
            do_brief = 1;
        else if (strcmp(argv[i], "--no-brief") == 0)
            do_brief = 0;
    }

    /* Resolve sibling binaries */
    char r1[PATH_MAX], r2[PATH_MAX], r3[PATH_MAX], r4[PATH_MAX], r5[PATH_MAX];
    const char *transcribe_bin = find_binary("BONFYRE_TRANSCRIBE_BINARY", argv[0],
        r1, sizeof(r1), "BonfyreTranscribe", "bonfyre-transcribe",
        "bonfyre-transcribe");
    const char *clean_bin = find_binary("BONFYRE_TRANSCRIPT_CLEAN_BINARY", argv[0],
        r2, sizeof(r2), "BonfyreTranscriptClean", "bonfyre-transcript-clean",
        "bonfyre-transcript-clean");
    const char *para_bin = find_binary("BONFYRE_PARAGRAPH_BINARY", argv[0],
        r3, sizeof(r3), "BonfyreParagraph", "bonfyre-paragraph",
        "bonfyre-paragraph");
    const char *brief_bin = find_binary("BONFYRE_BRIEF_BINARY", argv[0],
        r4, sizeof(r4), "BonfyreBrief", "bonfyre-brief",
        "bonfyre-brief");
    const char *narrate_bin = find_binary("BONFYRE_NARRATE_BINARY", argv[0],
        r5, sizeof(r5), "BonfyreNarrate", "bonfyre-narrate",
        "bonfyre-narrate");

    if (ensure_dir(out_dir) != 0) {
        fprintf(stderr, "error: cannot create %s\n", out_dir);
        return 1;
    }

    if (strcmp(mode, "narrate-brief") == 0) {
        /* Shortcut: just narrate an existing brief */
        char brief_path[PATH_MAX], narrate_dir[PATH_MAX];
        snprintf(brief_path, sizeof(brief_path), "%s/brief.md", input);
        snprintf(narrate_dir, sizeof(narrate_dir), "%s/narrate", out_dir);

        if (access(brief_path, F_OK) != 0) {
            snprintf(brief_path, sizeof(brief_path), "%s", input);
            if (access(brief_path, F_OK) != 0) {
                fprintf(stderr, "error: cannot find brief.md in %s\n", input);
                return 1;
            }
        }

        if (ensure_dir(narrate_dir) != 0) return 1;

        if (voice_model) {
            char *narr_argv[] = {
                (char *)narrate_bin, (char *)brief_path, narrate_dir,
                "--voice-model", (char *)voice_model, NULL
            };
            if (run_process(narr_argv) != 0) {
                fprintf(stderr, "Narration failed.\n");
                const char *steps[] = {"narrate"};
                write_manifest(out_dir, mode, steps, 1, 0);
                return 1;
            }
        } else {
            char *narr_argv[] = {
                (char *)narrate_bin, (char *)brief_path, narrate_dir, NULL
            };
            if (run_process(narr_argv) != 0) {
                fprintf(stderr, "Narration failed.\n");
                const char *steps[] = {"narrate"};
                write_manifest(out_dir, mode, steps, 1, 0);
                return 1;
            }
        }

        const char *steps[] = {"narrate"};
        write_manifest(out_dir, mode, steps, 1, 1);
        printf("SpeechLoop: narrate-brief complete → %s/narrate/\n", out_dir);
        return 0;
    }

    if (strcmp(mode, "transform") != 0) {
        fprintf(stderr, "error: unknown mode '%s'\n", mode);
        print_usage();
        return 1;
    }

    /* Full transform loop: audio → transcribe → clean → brief → narrate */
    const char *all_steps[8];
    int nsteps = 0;
    int rc = 0;

    /* Step 1: Transcribe */
    char transcribe_dir[PATH_MAX];
    snprintf(transcribe_dir, sizeof(transcribe_dir), "%s/transcribe", out_dir);
    printf("[1] Transcribing...\n");
    {
        char *t_argv[] = {
            (char *)transcribe_bin, (char *)input, transcribe_dir,
            "--model", (char *)whisper_model, NULL
        };
        rc = run_process(t_argv);
        all_steps[nsteps++] = "transcribe";
        if (rc != 0) {
            fprintf(stderr, "Transcription failed.\n");
            write_manifest(out_dir, mode, all_steps, nsteps, 0);
            return 1;
        }
    }

    char transcript_path[PATH_MAX];
    snprintf(transcript_path, sizeof(transcript_path), "%s/normalized.txt", transcribe_dir);

    /* Step 2: Clean */
    char clean_dir[PATH_MAX], clean_path[PATH_MAX];
    snprintf(clean_dir, sizeof(clean_dir), "%s/clean", out_dir);
    snprintf(clean_path, sizeof(clean_path), "%s/cleaned.txt", clean_dir);

    if (do_compress) {
        printf("[2] Cleaning transcript...\n");
        if (ensure_dir(clean_dir) != 0) return 1;
        char *c_argv[] = {
            (char *)clean_bin, "--transcript", transcript_path,
            "--output", clean_dir, NULL
        };
        rc = run_process(c_argv);
        all_steps[nsteps++] = "clean";
        if (rc != 0) {
            fprintf(stderr, "Cleaning failed (continuing with raw transcript).\n");
            snprintf(clean_path, sizeof(clean_path), "%s", transcript_path);
        }
    } else {
        snprintf(clean_path, sizeof(clean_path), "%s", transcript_path);
    }

    /* Step 3: Paragraph (if brief mode) */
    char para_dir[PATH_MAX], para_path[PATH_MAX];
    snprintf(para_dir, sizeof(para_dir), "%s/paragraph", out_dir);

    if (do_brief) {
        printf("[3] Structuring paragraphs...\n");
        if (ensure_dir(para_dir) != 0) return 1;
        char *p_argv[] = {
            (char *)para_bin, clean_path, para_dir, NULL
        };
        rc = run_process(p_argv);
        all_steps[nsteps++] = "paragraph";
        /* Non-fatal — brief can work from raw transcript */
    }

    /* Step 4: Brief */
    char brief_dir[PATH_MAX], brief_path[PATH_MAX];
    snprintf(brief_dir, sizeof(brief_dir), "%s/brief", out_dir);
    snprintf(brief_path, sizeof(brief_path), "%s/brief.md", brief_dir);

    if (do_brief) {
        printf("[4] Generating brief...\n");
        if (ensure_dir(brief_dir) != 0) return 1;
        char *b_argv[] = {
            (char *)brief_bin, clean_path, brief_dir, NULL
        };
        rc = run_process(b_argv);
        all_steps[nsteps++] = "brief";
        if (rc != 0) {
            fprintf(stderr, "Brief generation failed.\n");
            write_manifest(out_dir, mode, all_steps, nsteps, 0);
            return 1;
        }
    }

    /* Step 5: Narrate */
    char narrate_dir[PATH_MAX];
    snprintf(narrate_dir, sizeof(narrate_dir), "%s/narrate", out_dir);
    printf("[5] Narrating...\n");
    if (ensure_dir(narrate_dir) != 0) return 1;

    const char *narrate_input = do_brief ? brief_path : clean_path;

    if (voice_model) {
        char *n_argv[] = {
            (char *)narrate_bin, (char *)narrate_input, narrate_dir,
            "--voice-model", (char *)voice_model, NULL
        };
        rc = run_process(n_argv);
    } else {
        char *n_argv[] = {
            (char *)narrate_bin, (char *)narrate_input, narrate_dir, NULL
        };
        rc = run_process(n_argv);
    }
    all_steps[nsteps++] = "narrate";
    if (rc != 0) {
        fprintf(stderr, "Narration failed.\n");
        write_manifest(out_dir, mode, all_steps, nsteps, 0);
        return 1;
    }

    write_manifest(out_dir, mode, all_steps, nsteps, 1);
    printf("\nSpeechLoop complete: %s → %s\n", input, out_dir);
    printf("  Audio in:  %s\n", input);
    printf("  Text out:  %s\n", do_brief ? brief_path : clean_path);
    printf("  Audio out: %s/narrate/\n", out_dir);
    printf("  Steps:     ");
    for (int i = 0; i < nsteps; i++)
        printf("%s%s", all_steps[i], i < nsteps - 1 ? " → " : "\n");

    return 0;
}
