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

/* ── helpers ─────────────────────────────────────────────────── */

static int ensure_dir(const char *path) {
    char tmp[PATH_MAX]; size_t len = strlen(path);
    if (len == 0 || len >= sizeof(tmp)) return 1;
    memcpy(tmp, path, len + 1);
    for (size_t i = 1; i < len; i++) {
        if (tmp[i] == '/') { tmp[i] = '\0'; mkdir(tmp, 0755); tmp[i] = '/'; }
    }
    mkdir(tmp, 0755);
    return 0;
}

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

/* ── OpenSMILE Python wrapper ────────────────────────────────── */

/* Generates a tiny Python script that calls opensmile and dumps CSV.
 * This mirrors how BonfyreTranscribe calls whisper — subprocess of python3. */
static int run_opensmile_extract(const char *audio, const char *csv_out) {
    char script[2048];
    snprintf(script, sizeof(script),
        "import opensmile, sys\n"
        "smile = opensmile.Smile(\n"
        "    feature_set=opensmile.FeatureSet.eGeMAPSv02,\n"
        "    feature_level=opensmile.FeatureLevel.Functionals\n"
        ")\n"
        "y = smile.process_file('%s')\n"
        "y.to_csv('%s')\n"
        "print(len(y.columns))\n",
        audio, csv_out);

    /* write temp script */
    char tmp_py[PATH_MAX];
    snprintf(tmp_py, sizeof(tmp_py), "%s.opensmile_extract.py", csv_out);
    FILE *f = fopen(tmp_py, "w");
    if (!f) { perror("fopen script"); return -1; }
    fputs(script, f);
    fclose(f);

    const char *python = getenv("BONFYRE_PYTHON3");
    if (!python) python = "python3";
    const char *argv[] = { python, tmp_py, NULL };
    int rc = run_cmd(argv);
    unlink(tmp_py);
    return rc;
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

/* ── commands ───────────────────────────────────────────────── */

static int cmd_extract(const char *audio, const char *out_dir) {
    ensure_dir(out_dir);
    char csv[PATH_MAX], json[PATH_MAX];
    snprintf(csv, sizeof(csv), "%s/tone-raw.csv", out_dir);
    snprintf(json, sizeof(json), "%s/tone.json", out_dir);

    fprintf(stderr, "[tone] Extracting eGeMAPSv02 features from %s\n", audio);
    int rc = run_opensmile_extract(audio, csv);
    if (rc != 0) {
        fprintf(stderr, "[tone] OpenSMILE extraction failed (rc=%d)\n", rc);
        return 1;
    }

    Feature feats[MAX_FEATURES];
    int n = parse_csv(csv, feats, MAX_FEATURES);
    if (n == 0) {
        fprintf(stderr, "[tone] No features parsed from CSV\n");
        return 1;
    }

    /* write JSON */
    char ts[64]; iso_ts(ts, sizeof(ts));
    FILE *f = fopen(json, "w");
    if (!f) { perror("fopen"); return 1; }
    fprintf(f, "{\n  \"type\": \"tone-features\",\n");
    fprintf(f, "  \"source\": \"%s\",\n", audio);
    fprintf(f, "  \"timestamp\": \"%s\",\n", ts);
    fprintf(f, "  \"feature_set\": \"eGeMAPSv02\",\n");
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

    fprintf(stderr, "[tone] Building voice profile for %s\n", audio);
    int rc = run_opensmile_extract(audio, csv);
    if (rc != 0) return 1;

    Feature feats[MAX_FEATURES];
    int n = parse_csv(csv, feats, MAX_FEATURES);
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
    if (run_opensmile_extract(audio1, csv1) != 0) return 1;
    if (run_opensmile_extract(audio2, csv2) != 0) return 1;

    Feature fa[MAX_FEATURES], fb[MAX_FEATURES];
    int na = parse_csv(csv1, fa, MAX_FEATURES);
    int nb = parse_csv(csv2, fb, MAX_FEATURES);
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
        "bonfyre-tone — speech tone/emotion/rhythm extraction (OpenSMILE)\n\n"
        "Usage:\n"
        "  bonfyre-tone extract <audio> [output-dir]\n"
        "  bonfyre-tone profile <audio> [output-dir]\n"
        "  bonfyre-tone compare <audio1> <audio2> [output-dir]\n"
        "  bonfyre-tone status\n");
}

int main(int argc, char **argv) {
    if (argc >= 2 && strcmp(argv[1], "status") == 0) {
        printf("{\"binary\":\"bonfyre-tone\",\"status\":\"ok\",\"version\":\"1.0.0\"}\n");
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
