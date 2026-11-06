/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

/*
 * BonfyreTone — speech tone/emotion/rhythm extraction via OpenSMILE.
 *
 * Unlocks the speech metadata layer: energy, pitch, jitter, shimmer,
 * speaking rate, loudness contour, spectral balance.
 * Nobody else gives creators this data from their audio.
 *
 * Usage:
 *   bonfyre-tone extract <audio>              → tone.json  (88 eGeMAPSv02 features)
 *   bonfyre-tone profile <audio>              → profile.json (human-readable summary)
 *   bonfyre-tone compare <audio1> <audio2>    → diff.json (tone delta between two)
 */
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <bonfyre.h>
#include <bf_voice_fingerprint.h>
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

/* ── helpers ─────────────────────────────────────────────────── */

static int ensure_dir(const char *path) { return bf_ensure_dir(path); }

static void iso_ts(char *buf, size_t sz) {
    time_t t = time(NULL); struct tm tm; gmtime_r(&t, &tm);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &tm);
}

static int run_cmd(const char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return -1; }
    if (pid == 0) { execvp(argv[0], (char *const *)argv); _exit(127); }
    int st; waitpid(pid, &st, 0);
    return WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

/* ── Native OpenSMILE command boundary ───────────────────────── */

/* OpenSMILE is a native external organ. The configured eGeMAPS config is
 * explicit so no hidden package environment is reconstructed. */
static int run_opensmile_extract(const char *audio, const char *csv_out) {
    const char *binary = getenv("BONFYRE_OPENSMILE_BINARY");
    const char *config = getenv("BONFYRE_OPENSMILE_CONFIG");
    if (!binary || !binary[0] || !config || !config[0]) {
        fprintf(stderr,
                "[tone] Native OpenSMILE requires BONFYRE_OPENSMILE_BINARY and "
                "BONFYRE_OPENSMILE_CONFIG.\n");
        return -1;
    }
    const char *argv[] = { binary, "-C", config, "-I", audio, "-O", csv_out, NULL };
    return run_cmd(argv);
}

/* ── CSV → JSON conversion ───────────────────────────────────── */

#define MAX_FEATURES 256
#define MAX_NAME_LEN 128
#define MAX_LINE_LEN 65536

typedef struct {
    char name[MAX_NAME_LEN];
    double value;
} Feature;

static int parse_csv(const char *csv_path, Feature *out, int max) {
    FILE *f = fopen(csv_path, "r");
    if (!f) return 0;

    char header[MAX_LINE_LEN];
    char values[MAX_LINE_LEN];
    if (!fgets(header, sizeof(header), f)) { fclose(f); return 0; }
    if (!fgets(values, sizeof(values), f)) { fclose(f); return 0; }
    fclose(f);

    /* strip trailing newlines */
    header[strcspn(header, "\r\n")] = '\0';
    values[strcspn(values, "\r\n")] = '\0';

    /* parse header names */
    char *names[MAX_FEATURES];
    int ncols = 0;
    char *tok = strtok(header, ",");
    while (tok && ncols < max) {
        /* strip quotes */
        if (tok[0] == '"') tok++;
        size_t tl = strlen(tok);
        if (tl > 0 && tok[tl-1] == '"') tok[tl-1] = '\0';
        names[ncols] = tok;
        ncols++;
        tok = strtok(NULL, ",");
    }

    /* parse values */
    int nvals = 0;
    char *vtok = strtok(values, ",");
    while (vtok && nvals < ncols && nvals < max) {
        /* skip metadata columns (file, start, end) */
        if (nvals >= 3) {
            int idx = nvals - 3;
            strncpy(out[idx].name, names[nvals], MAX_NAME_LEN - 1);
            out[idx].name[MAX_NAME_LEN - 1] = '\0';
            out[idx].value = atof(vtok);
        }
        nvals++;
        vtok = strtok(NULL, ",");
    }

    return (nvals > 3) ? nvals - 3 : 0;
}

/* ── tone profile (human-readable) ───────────────────────────── */

typedef struct {
    const char *label;
    const char *feature_prefix;
    const char *low_desc;
    const char *high_desc;
} ProfileDimension;

static const ProfileDimension DIMENSIONS[] = {
    { "energy",    "loudness",         "quiet, subdued",       "loud, energetic" },
    { "pitch",     "F0semitone",       "low, calm",            "high, animated" },
    { "stability", "jitter",           "smooth, steady",       "rough, unsteady" },
    { "brightness","spectralFlux",     "dark, warm",           "bright, crisp" },
    { "pace",      "MeanVoicedSegLen", "slow, deliberate",     "fast, snappy" },
    { "variation", "loudness_sma3nz_stddevNorm", "monotone", "dynamic, expressive" },
};
#define N_DIMENSIONS (sizeof(DIMENSIONS) / sizeof(DIMENSIONS[0]))

static double find_feature(const Feature *feats, int n, const char *prefix) {
    for (int i = 0; i < n; i++) {
        if (strstr(feats[i].name, prefix)) return feats[i].value;
    }
    return 0.0;
}

static void write_profile(FILE *out, const Feature *feats, int n,
                          const char *audio_path) {
    char ts[64]; iso_ts(ts, sizeof(ts));
    fprintf(out, "{\n  \"type\": \"tone-profile\",\n");
    fprintf(out, "  \"source\": \"%s\",\n", audio_path);
    fprintf(out, "  \"timestamp\": \"%s\",\n", ts);
    fprintf(out, "  \"dimensions\": {\n");

    for (size_t d = 0; d < N_DIMENSIONS; d++) {
        double val = find_feature(feats, n, DIMENSIONS[d].feature_prefix);
        /* normalize to 0-100 scale (approximate) */
        double norm = fabs(val) * 100.0;
        if (norm > 100.0) norm = 100.0;
        const char *desc = (norm < 40.0) ? DIMENSIONS[d].low_desc : DIMENSIONS[d].high_desc;

        fprintf(out, "    \"%s\": {\n", DIMENSIONS[d].label);
        fprintf(out, "      \"raw\": %.6f,\n", val);
        fprintf(out, "      \"normalized\": %.1f,\n", norm);
        fprintf(out, "      \"description\": \"%s\"\n", desc);
        fprintf(out, "    }%s\n", (d < N_DIMENSIONS - 1) ? "," : "");
    }

    fprintf(out, "  },\n");
    fprintf(out, "  \"feature_count\": %d\n", n);
    fprintf(out, "}\n");
}

/* ── engines ────────────────────────────────────────────────── */

#define SET_EGEMAPS "eGeMAPSv02"

static int opensmile_configured(void) {
    const char *binary = getenv("BONFYRE_OPENSMILE_BINARY");
    const char *config = getenv("BONFYRE_OPENSMILE_CONFIG");
    return binary && binary[0] && access(binary, X_OK) == 0 &&
           config && config[0] && access(config, R_OK) == 0;
}

/* Measures one recording with whichever engine this machine has, and says
 * which. OpenSMILE, when it is configured, gives the eGeMAPS set. Without it
 * the estate's own extractor reads the samples directly and gives its own,
 * smaller set under its own name; it takes 16-bit PCM WAV, which is what the
 * media-prep organ writes. The two sets are different measurements and are
 * never passed off as each other. Returns the number of features, 0 on
 * failure. */
static int tone_features(const char *audio, const char *csv, Feature *out, int max, const char **set) {
    if (opensmile_configured()) {
        *set = SET_EGEMAPS;
        if (run_opensmile_extract(audio, csv) != 0) {
            fprintf(stderr, "[tone] OpenSMILE extraction failed\n");
            return 0;
        }
        return parse_csv(csv, out, max);
    }
    *set = BF_VOICE_FINGERPRINT_SET;
    WavAudio wav = wav_read(audio);
    if (!wav.samples || wav.n_samples <= 0) {
        fprintf(stderr, "[tone] %s is not 16-bit PCM WAV, and no OpenSMILE is configured to read anything else\n", audio);
        free(wav.samples);
        return 0;
    }
    VoiceFingerprint fp = extract_fingerprint_wav(wav.samples, wav.n_samples, wav.sample_rate);
    free(wav.samples);
    if (fp.count == 0) {
        fprintf(stderr, "[tone] %s is too short to measure\n", audio);
        return 0;
    }
    int n = fp.count < max ? fp.count : max;
    for (int i = 0; i < n; i++) {
        snprintf(out[i].name, sizeof(out[i].name), "%s", fp.features[i].name);
        out[i].value = fp.features[i].value;
    }
    return n;
}

/* ── commands ───────────────────────────────────────────────── */

static int cmd_extract(const char *audio, const char *out_dir) {
    ensure_dir(out_dir);
    char csv[PATH_MAX], json[PATH_MAX];
    snprintf(csv, sizeof(csv), "%s/tone-raw.csv", out_dir);
    snprintf(json, sizeof(json), "%s/tone.json", out_dir);

    Feature feats[MAX_FEATURES];
    const char *feature_set = "";
    int n = tone_features(audio, csv, feats, MAX_FEATURES, &feature_set);
    if (n == 0) return 1;
    fprintf(stderr, "[tone] Measured %d %s features from %s\n", n, feature_set, audio);

    /* write JSON */
    char ts[64]; iso_ts(ts, sizeof(ts));
    FILE *f = fopen(json, "w");
    if (!f) { perror("fopen"); return 1; }
    fprintf(f, "{\n  \"type\": \"tone-features\",\n");
    fprintf(f, "  \"source\": \"%s\",\n", audio);
    fprintf(f, "  \"timestamp\": \"%s\",\n", ts);
    fprintf(f, "  \"feature_set\": \"%s\",\n", feature_set);
    fprintf(f, "  \"engine\": \"%s\",\n", strcmp(feature_set, SET_EGEMAPS) ? "native" : "opensmile");
    fprintf(f, "  \"feature_count\": %d,\n", n);
    fprintf(f, "  \"features\": {\n");
    for (int i = 0; i < n; i++) {
        fprintf(f, "    \"%s\": %.6f%s\n", feats[i].name, feats[i].value,
                (i < n - 1) ? "," : "");
    }
    fprintf(f, "  }\n}\n");
    fclose(f);

    fprintf(stderr, "[tone] → %s (%d features)\n", json, n);
    return 0;
}

static int cmd_profile(const char *audio, const char *out_dir) {
    ensure_dir(out_dir);
    char csv[PATH_MAX], json[PATH_MAX];
    snprintf(csv, sizeof(csv), "%s/tone-raw.csv", out_dir);
    snprintf(json, sizeof(json), "%s/profile.json", out_dir);

    /* The profile's dimensions are defined on eGeMAPS feature names. Filling
     * them from another feature set would print numbers for dimensions that
     * were never measured, so without OpenSMILE there is no profile. */
    if (!opensmile_configured()) {
        fprintf(stderr, "[tone] a voice profile is defined on eGeMAPS features and needs OpenSMILE; "
                        "extract and compare work without it\n");
        return 2;
    }
    fprintf(stderr, "[tone] Building voice profile for %s\n", audio);
    Feature feats[MAX_FEATURES];
    const char *feature_set = "";
    int n = tone_features(audio, csv, feats, MAX_FEATURES, &feature_set);
    if (n == 0) return 1;

    FILE *f = fopen(json, "w");
    if (!f) { perror("fopen"); return 1; }
    write_profile(f, feats, n, audio);
    fclose(f);

    fprintf(stderr, "[tone] → %s (%zu dimensions)\n", json, N_DIMENSIONS);
    return 0;
}

static int cmd_compare(const char *audio1, const char *audio2, const char *out_dir) {
    ensure_dir(out_dir);
    char csv1[PATH_MAX], csv2[PATH_MAX], json[PATH_MAX];
    snprintf(csv1, sizeof(csv1), "%s/tone-a.csv", out_dir);
    snprintf(csv2, sizeof(csv2), "%s/tone-b.csv", out_dir);
    snprintf(json, sizeof(json), "%s/diff.json", out_dir);

    fprintf(stderr, "[tone] Comparing %s vs %s\n", audio1, audio2);
    Feature fa[MAX_FEATURES], fb[MAX_FEATURES];
    const char *set_a = "", *set_b = "";
    int na = tone_features(audio1, csv1, fa, MAX_FEATURES, &set_a);
    int nb = tone_features(audio2, csv2, fb, MAX_FEATURES, &set_b);
    if (na == 0 || nb == 0) return 1;
    int n = (na < nb) ? na : nb;

    char ts[64]; iso_ts(ts, sizeof(ts));
    FILE *f = fopen(json, "w");
    if (!f) { perror("fopen"); return 1; }
    fprintf(f, "{\n  \"type\": \"tone-diff\",\n");
    fprintf(f, "  \"source_a\": \"%s\",\n", audio1);
    fprintf(f, "  \"source_b\": \"%s\",\n", audio2);
    fprintf(f, "  \"timestamp\": \"%s\",\n", ts);
    fprintf(f, "  \"deltas\": {\n");
    for (int i = 0; i < n; i++) {
        double delta = fb[i].value - fa[i].value;
        fprintf(f, "    \"%s\": %.6f%s\n", fa[i].name, delta,
                (i < n - 1) ? "," : "");
    }
    fprintf(f, "  },\n");

    /* summary: biggest shifts */
    fprintf(f, "  \"biggest_shifts\": [\n");
    typedef struct { int idx; double mag; } Shift;
    Shift shifts[MAX_FEATURES];
    for (int i = 0; i < n; i++) {
        shifts[i].idx = i;
        shifts[i].mag = fabs(fb[i].value - fa[i].value);
    }
    /* simple top-5 selection */
    for (int pass = 0; pass < 5 && pass < n; pass++) {
        int best = pass;
        for (int j = pass + 1; j < n; j++) {
            if (shifts[j].mag > shifts[best].mag) best = j;
        }
        Shift tmp = shifts[pass]; shifts[pass] = shifts[best]; shifts[best] = tmp;
        int idx = shifts[pass].idx;
        fprintf(f, "    { \"feature\": \"%s\", \"delta\": %.6f }%s\n",
                fa[idx].name, fb[idx].value - fa[idx].value,
                (pass < 4 && pass < n - 1) ? "," : "");
    }
    fprintf(f, "  ]\n}\n");
    fclose(f);

    fprintf(stderr, "[tone] → %s (%d features compared)\n", json, n);
    return 0;
}

/* ── main ───────────────────────────────────────────────────── */

static void print_usage(void) {
    fprintf(stderr,
        "bonfyre-tone — speech tone/emotion/rhythm extraction\n" "  eGeMAPS through OpenSMILE when BONFYRE_OPENSMILE_BINARY and _CONFIG are set;\n" "  otherwise the estate's own voice features, read from 16-bit PCM WAV.\n\n"
        "Usage:\n"
        "  bonfyre-tone extract <audio> [output-dir]\n"
        "  bonfyre-tone profile <audio> [output-dir]\n"
        "  bonfyre-tone compare <audio1> <audio2> [output-dir]\n"
        "  bonfyre-tone status\n");
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { print_usage(); return 0; }
    enum { TN_STATUS };
    static const BfCommandEntry tone_commands[] = {
        {"status", TN_STATUS, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(tone_commands,
        sizeof(tone_commands) / sizeof(tone_commands[0]), argv[1]);
    if (command && command->id == TN_STATUS) {
        /* Status is whether the organ can do its work, which means whether
         * its engine is there. It used to print ok without looking. */
        int opensmile = opensmile_configured();
        printf("{\"binary\":\"bonfyre-tone\",\"status\":\"ok\",\"version\":\"1.1.0\","
               "\"engine\":\"%s\",\"feature_set\":\"%s\",\"profile_available\":%s}\n",
               opensmile ? "opensmile" : "native",
               opensmile ? SET_EGEMAPS : BF_VOICE_FINGERPRINT_SET,
               opensmile ? "true" : "false");
        return 0;
    }

    if (argc < 3) { print_usage(); return 1; }

    const char *cmd = argv[1];
    const char *audio = argv[2];
    const char *out_dir = (argc > 3 && argv[3][0] != '-') ? argv[3] : "output";

    if (strcmp(cmd, "extract") == 0) {
        return cmd_extract(audio, out_dir);
    } else if (strcmp(cmd, "profile") == 0) {
        return cmd_profile(audio, out_dir);
    } else if (strcmp(cmd, "compare") == 0) {
        if (argc < 4) { print_usage(); return 1; }
        const char *audio2 = argv[3];
        const char *dir = (argc > 4) ? argv[4] : "output";
        return cmd_compare(audio, audio2, dir);
    }

    print_usage();
    return 1;
}
