/*
 * BonfyreCompress — family-aware compression engine.
 *
 * Owns storage costs. Every byte saved = captured margin.
 * Trains zstd dictionaries per artifact family type, compresses with
 * family-specific dictionaries, reports savings, manages the dict library.
 *
 * Usage:
 *   bonfyre-compress train <samples_dir> [--dict-out family.dict]
 *   bonfyre-compress pack <input> <output.zst> [--dict family.dict] [--level 19]
 *   bonfyre-compress unpack <input.zst> <output> [--dict family.dict]
 *   bonfyre-compress family <family_dir> [--dict family.dict] [--out family.tar.zst]
 *   bonfyre-compress savings <family_dir>  — report compression savings
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
#include <dirent.h>

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

static void iso_timestamp(char *buf, size_t sz) {
    time_t now = time(NULL); struct tm t; gmtime_r(&now, &t);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &t);
}

static int run_cmd(const char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) { execvp(argv[0], (char *const *)argv); _exit(127); }
    int st; waitpid(pid, &st, 0);
    return WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

static unsigned long dir_size(const char *path) {
    unsigned long total = 0;
    DIR *d = opendir(path);
    if (!d) return 0;
    struct dirent *ent;
    while ((ent = readdir(d))) {
        if (ent->d_name[0] == '.') continue;
        char fp[PATH_MAX];
        snprintf(fp, sizeof(fp), "%s/%s", path, ent->d_name);
        struct stat st;
        if (stat(fp, &st) == 0 && S_ISREG(st.st_mode))
            total += (unsigned long)st.st_size;
    }
    closedir(d);
    return total;
}

static unsigned long file_sz(const char *p) {
    struct stat st; return (stat(p, &st) == 0) ? (unsigned long)st.st_size : 0;
}

/* ---------- commands ---------- */

static int cmd_train(const char *samples, const char *dict_out) {
    fprintf(stderr, "[compress] Training dict from %s -> %s\n", samples, dict_out);
    char glob[PATH_MAX + 4];
    snprintf(glob, sizeof(glob), "%s/*", samples);
    /* zstd --train samples/* -o dict */
    const char *argv[] = { "zstd", "--train", "-r", samples, "-o", dict_out, NULL };
    int rc = run_cmd(argv);
    if (rc == 0) fprintf(stderr, "[compress] Dict trained: %s (%lu bytes)\n", dict_out, file_sz(dict_out));
    return rc;
}

static int cmd_pack(const char *input, const char *output, const char *dict, int level) {
    char lvl_str[8]; snprintf(lvl_str, sizeof(lvl_str), "-%d", level);
    if (dict && dict[0]) {
        const char *argv[] = { "zstd", lvl_str, "-D", dict, input, "-o", output, NULL };
        return run_cmd(argv);
    } else {
        const char *argv[] = { "zstd", lvl_str, input, "-o", output, NULL };
        return run_cmd(argv);
    }
}

static int cmd_unpack(const char *input, const char *output, const char *dict) {
    if (dict && dict[0]) {
        const char *argv[] = { "zstd", "-d", "-D", dict, input, "-o", output, NULL };
        return run_cmd(argv);
    } else {
        const char *argv[] = { "zstd", "-d", input, "-o", output, NULL };
        return run_cmd(argv);
    }
}

static int cmd_family(const char *dir, const char *dict, const char *output) {
    fprintf(stderr, "[compress] Packing family %s -> %s\n", dir, output);
    /* tar + zstd with optional dict */
    if (dict && dict[0]) {
        char cmd[PATH_MAX * 3];
        snprintf(cmd, sizeof(cmd), "tar cf - '%s' | zstd -19 -D '%s' -o '%s'", dir, dict, output);
        return system(cmd);
    } else {
        char cmd[PATH_MAX * 2];
        snprintf(cmd, sizeof(cmd), "tar cf - '%s' | zstd -19 -o '%s'", dir, output);
        return system(cmd);
    }
}

static int cmd_savings(const char *dir) {
    unsigned long raw = dir_size(dir);
    fprintf(stderr, "[compress] Measuring savings for %s (%lu bytes raw)...\n", dir, raw);

    char tmp_out[PATH_MAX];
    snprintf(tmp_out, sizeof(tmp_out), "/tmp/bonfyre-compress-test-%d.tar.zst", (int)getpid());

    char cmd[PATH_MAX * 2];
    snprintf(cmd, sizeof(cmd), "tar cf - '%s' | zstd -19 -o '%s' 2>/dev/null", dir, tmp_out);
    system(cmd);

    unsigned long compressed = file_sz(tmp_out);
    unlink(tmp_out);

    double ratio = raw > 0 ? (double)compressed / (double)raw : 1.0;
    double saved_pct = (1.0 - ratio) * 100.0;

    printf("{\n");
    printf("  \"directory\": \"%s\",\n", dir);
    printf("  \"raw_bytes\": %lu,\n", raw);
    printf("  \"compressed_bytes\": %lu,\n", compressed);
    printf("  \"ratio\": %.4f,\n", ratio);
    printf("  \"savings_pct\": %.1f,\n", saved_pct);
    printf("  \"bytes_saved\": %lu\n", raw > compressed ? raw - compressed : 0);
    printf("}\n");

    fprintf(stderr, "[compress] %lu -> %lu (%.1f%% savings)\n", raw, compressed, saved_pct);
    return 0;
}

/* ---------- main ---------- */

int main(int argc, char *argv[]) {
    const char *dict = NULL;
    int level = 19;

    for (int i = 1; i < argc - 1; i++) {
        if (strcmp(argv[i], "--dict") == 0) dict = argv[i+1];
        if (strcmp(argv[i], "--level") == 0) level = atoi(argv[i+1]);
    }

    if (argc >= 3 && strcmp(argv[1], "train") == 0) {
        const char *dict_out = "family.dict";
        for (int i = 3; i < argc - 1; i++)
            if (strcmp(argv[i], "--dict-out") == 0) dict_out = argv[i+1];
        return cmd_train(argv[2], dict_out);
    }
    if (argc >= 4 && strcmp(argv[1], "pack") == 0)
        return cmd_pack(argv[2], argv[3], dict, level);
    if (argc >= 4 && strcmp(argv[1], "unpack") == 0)
        return cmd_unpack(argv[2], argv[3], dict);
    if (argc >= 3 && strcmp(argv[1], "family") == 0) {
        const char *out = NULL;
        for (int i = 3; i < argc - 1; i++)
            if (strcmp(argv[i], "--out") == 0) out = argv[i+1];
        if (!out) {
            static char defout[PATH_MAX];
            snprintf(defout, sizeof(defout), "%s.tar.zst", argv[2]);
            out = defout;
        }
        return cmd_family(argv[2], dict, out);
    }
    if (argc >= 3 && strcmp(argv[1], "savings") == 0)
        return cmd_savings(argv[2]);

    fprintf(stderr,
        "BonfyreCompress — family-aware compression engine\n\n"
        "Usage:\n"
        "  bonfyre-compress train <dir> [--dict-out F]    Train zstd dictionary\n"
        "  bonfyre-compress pack <in> <out> [--dict D]    Compress file\n"
        "  bonfyre-compress unpack <in> <out> [--dict D]  Decompress file\n"
        "  bonfyre-compress family <dir> [--out F]        Pack entire family\n"
        "  bonfyre-compress savings <dir>                 Report savings\n"
    );
    return 1;
}
