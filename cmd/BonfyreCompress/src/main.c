/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

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
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <dirent.h>
#include <bonfyre.h>
#include "physical.h"
#include "record_factor.h"
#ifdef BONFYRE_HAVE_ZSTD
#include <zstd.h>
#endif

static const char *layeros_binary(void) {
    return "layeros/bin/bonfyre-layeros";
}

static int wait_child(pid_t pid, int *status) {
    int rc;
    do { rc = waitpid(pid, status, 0); } while (rc < 0 && errno == EINTR);
    return rc;
}

static int run_cmd(const char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) { execvp(argv[0], (char *const *)argv); _exit(127); }
    int st;
    if (wait_child(pid, &st) < 0) return -1;
    return WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

/* Tar's bytes are piped directly to zstd. File names are argv values, never
 * shell source; both producers must succeed before a family is called packed. */
static int run_tar_zstd(const char *dir, const char *dict, const char *output, int force) {
    int fds[2];
    if (pipe(fds) != 0) return -1;
    pid_t tar_pid = fork();
    if (tar_pid < 0) { close(fds[0]); close(fds[1]); return -1; }
    if (tar_pid == 0) {
        close(fds[0]);
        if (dup2(fds[1], STDOUT_FILENO) < 0) _exit(127);
        close(fds[1]);
        execlp("tar", "tar", "cf", "-", "--", dir, (char *)NULL);
        _exit(127);
    }
    pid_t zstd_pid = fork();
    if (zstd_pid < 0) {
        close(fds[0]); close(fds[1]);
        wait_child(tar_pid, NULL);
        return -1;
    }
    if (zstd_pid == 0) {
        close(fds[1]);
        if (dup2(fds[0], STDIN_FILENO) < 0) _exit(127);
        close(fds[0]);
        const char *args[9];
        size_t n = 0;
        args[n++] = "zstd";
        args[n++] = "-19";
        if (force) args[n++] = "-f";
        if (dict && dict[0]) { args[n++] = "-D"; args[n++] = dict; }
        args[n++] = "-o";
        args[n++] = output;
        args[n] = NULL;
        execvp("zstd", (char *const *)args);
        _exit(127);
    }
    close(fds[0]); close(fds[1]);
    int tar_status = 0, zstd_status = 0;
    int tar_wait = wait_child(tar_pid, &tar_status);
    int zstd_wait = wait_child(zstd_pid, &zstd_status);
    if (tar_wait < 0 || zstd_wait < 0) return -1;
    return WIFEXITED(tar_status) && WEXITSTATUS(tar_status) == 0 &&
           WIFEXITED(zstd_status) && WEXITSTATUS(zstd_status) == 0 ? 0 : 1;
}

static int delegate_layeros_compress(int argc, char *argv[]) {
    char *exec_argv[24];
    int n = 0;
    exec_argv[n++] = (char *)layeros_binary();
    const char *root = NULL;
    for (int i = 1; i < argc - 1; i++) {
        if (strcmp(argv[i], "--root") == 0) {
            root = argv[i + 1];
            break;
        }
    }
    if (root) {
        exec_argv[n++] = "--root";
        exec_argv[n++] = (char *)root;
    }
    exec_argv[n++] = "compress";
    exec_argv[n++] = argv[3];
    exec_argv[n++] = argv[2];
    for (int i = 4; i < argc; i++) {
        if ((strcmp(argv[i], "--root") == 0 && i + 1 < argc)) {
            i++;
            continue;
        }
        exec_argv[n++] = argv[i];
    }
    exec_argv[n] = NULL;
    return run_cmd((const char *const *)exec_argv);
}

static int dir_size(const char *path, unsigned long *total) {
    DIR *d = opendir(path);
    if (!d) return -1;
    struct dirent *ent;
    while ((ent = readdir(d))) {
        if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
        size_t cap = strlen(path) + strlen(ent->d_name) + 2;
        char *fp = malloc(cap);
        if (!fp) { closedir(d); return -1; }
        snprintf(fp, cap, "%s/%s", path, ent->d_name);
        struct stat st;
        int rc = lstat(fp, &st);
        if (rc == 0 && S_ISREG(st.st_mode)) {
            if (st.st_size < 0 || (unsigned long)st.st_size > ULONG_MAX - *total) rc = -1;
            else *total += (unsigned long)st.st_size;
        } else if (rc == 0 && S_ISDIR(st.st_mode)) rc = dir_size(fp, total);
        free(fp);
        if (rc != 0) { closedir(d); return -1; }
    }
    closedir(d);
    return 0;
}

static unsigned long file_sz(const char *p) {
    struct stat st; return (stat(p, &st) == 0) ? (unsigned long)st.st_size : 0;
}

/* ---------- commands ---------- */

static int cmd_train(const char *samples, const char *dict_out) {
    fprintf(stderr, "[compress] Training dict from %s -> %s\n", samples, dict_out);
    char glob[PATH_MAX + 4];
    snprintf(glob, sizeof(glob), "%s/*", samples);
    /* zstd recursive training over the samples directory. */
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
    size_t cap = strlen(output) + sizeof(".tmp.XXXXXX");
    char *temporary = malloc(cap);
    if (!temporary) return 1;
    snprintf(temporary, cap, "%s.tmp.XXXXXX", output);
    int fd = mkstemp(temporary);
    if (fd < 0) { free(temporary); return 1; }
    close(fd);
    int rc = run_tar_zstd(dir, dict, temporary, 1);
    if (rc == 0 && link(temporary, output) != 0) rc = 1;
    unlink(temporary);
    free(temporary);
    return rc;
}

/* How many bytes the archive stream is before compression: the files' own
 * bytes plus tar's headers and padding.  For a directory of small files that
 * overhead can exceed the files themselves, which is why it is measured rather
 * than assumed away. */
#ifndef BONFYRE_HAVE_ZSTD
static int tar_stream_bytes(const char *dir, unsigned long *out) {
    int fds[2];
    if (pipe(fds) != 0) return -1;
    pid_t pid = fork();
    if (pid < 0) { close(fds[0]); close(fds[1]); return -1; }
    if (pid == 0) {
        close(fds[0]);
        if (dup2(fds[1], STDOUT_FILENO) < 0) _exit(127);
        close(fds[1]);
        execlp("tar", "tar", "cf", "-", "--", dir, (char *)NULL);
        _exit(127);
    }
    close(fds[1]);
    unsigned long total = 0;
    char buf[65536];
    ssize_t n;
    while ((n = read(fds[0], buf, sizeof(buf))) > 0) total += (unsigned long)n;
    close(fds[0]);
    int status = 0;
    if (wait_child(pid, &status) < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 0) return -1;
    *out = total;
    return 0;
}
#else
/* Count the same tar bytes that enter the compressor. The output is consumed
 * into a bounded buffer because savings needs its length, not a saved archive. */
static int tar_stream_measure(const char *dir, unsigned long *stream, unsigned long *compressed) {
    ZSTD_CCtx *codec = ZSTD_createCCtx();
    size_t in_size = ZSTD_CStreamInSize(), out_size = ZSTD_CStreamOutSize();
    void *input = malloc(in_size), *output = malloc(out_size);
    int result = -1, fds[2], status = 0;
    unsigned long input_bytes = 0, output_bytes = 0;
    if (!codec || !input || !output) goto done;
    /* Match the zstd CLI's default one-worker frame construction, including
     * job boundaries on larger streams, rather than its --single-thread mode. */
    if (ZSTD_isError(ZSTD_CCtx_setParameter(codec, ZSTD_c_compressionLevel, 19)) ||
        ZSTD_isError(ZSTD_CCtx_setParameter(codec, ZSTD_c_nbWorkers, 1)) ||
        ZSTD_isError(ZSTD_CCtx_setParameter(codec, ZSTD_c_checksumFlag, 1))) goto done;
    if (pipe(fds) != 0) goto done;
    pid_t child = fork();
    if (child < 0) { close(fds[0]); close(fds[1]); goto done; }
    if (child == 0) {
        close(fds[0]);
        if (dup2(fds[1], STDOUT_FILENO) < 0) _exit(127);
        close(fds[1]);
        execlp("tar", "tar", "cf", "-", "--", dir, (char *)NULL);
        _exit(127);
    }
    close(fds[1]);
    result = 0;
    for (;;) {
        ssize_t count;
        do { count = read(fds[0], input, in_size); } while (count < 0 && errno == EINTR);
        if (count < 0 || (unsigned long)count > ULONG_MAX - input_bytes) { result = -1; break; }
        input_bytes += (unsigned long)count;
        ZSTD_inBuffer in = {input, (size_t)count, 0};
        ZSTD_EndDirective directive = count ? ZSTD_e_continue : ZSTD_e_end;
        size_t pending;
        do {
            ZSTD_outBuffer out = {output, out_size, 0};
            pending = ZSTD_compressStream2(codec, &out, &in, directive);
            if (ZSTD_isError(pending) || out.pos > ULONG_MAX - output_bytes) { result = -1; break; }
            output_bytes += (unsigned long)out.pos;
        } while (in.pos < in.size || (!count && pending));
        if (result || !count) break;
    }
    close(fds[0]);
    if (wait_child(child, &status) < 0 || !WIFEXITED(status) || WEXITSTATUS(status)) result = -1;
    if (!result) { *stream = input_bytes; *compressed = output_bytes; }
done:
    ZSTD_freeCCtx(codec); free(input); free(output);
    return result;
}
#endif

/* Three different things were being called one number.
 *
 * `raw_bytes` is the files' contents.  What gets compressed is a tar stream,
 * which is larger than that by its headers and padding.  What gets delivered
 * is the compressed stream.  Dividing the last by the first mixed two
 * representations: for large files the difference is noise, and for a
 * directory of small ones it reported a saving or a loss that was mostly tar.
 *
 * So each is reported, and the two honest ratios are kept apart: what the
 * compressor did to the stream it was given, and what the delivered archive
 * costs against the files it carries.  The second can be worse than 1, and
 * says so instead of being clamped to "0 bytes saved". */
static int cmd_savings(const char *dir) {
    unsigned long raw = 0, stream = 0, compressed = 0;
    if (dir_size(dir, &raw) != 0) return 1;
#ifdef BONFYRE_HAVE_ZSTD
    if (tar_stream_measure(dir, &stream, &compressed) != 0) return 1;
#else
    if (tar_stream_bytes(dir, &stream) != 0) return 1;

    char tmp_out[] = "/tmp/bonfyre-compress-savings-XXXXXX";
    int fd = mkstemp(tmp_out);
    if (fd < 0) return 1;
    close(fd);
    if (run_tar_zstd(dir, NULL, tmp_out, 1) != 0) {
        unlink(tmp_out);
        return 1;
    }

    compressed = file_sz(tmp_out);
    unlink(tmp_out);
#endif

    double stream_ratio = stream > 0 ? (double)compressed / (double)stream : 1.0;
    double ratio = raw > 0 ? (double)compressed / (double)raw : 1.0;
    double saved_pct = (1.0 - ratio) * 100.0;

    printf("{\n");
    printf("  \"directory\": \"%s\",\n", dir);
    printf("  \"raw_bytes\": %lu,\n", raw);
    printf("  \"archive_stream_bytes\": %lu,\n", stream);
    printf("  \"archive_overhead_bytes\": %lu,\n", stream > raw ? stream - raw : 0);
    printf("  \"compressed_bytes\": %lu,\n", compressed);
    printf("  \"stream_ratio\": %.4f,\n", stream_ratio);
    printf("  \"ratio\": %.4f,\n", ratio);
    printf("  \"savings_pct\": %.1f,\n", saved_pct);
    printf("  \"bytes_saved\": %lu,\n", raw > compressed ? raw - compressed : 0);
    printf("  \"bytes_added\": %lu,\n", compressed > raw ? compressed - raw : 0);
    printf("  \"delivered_larger_than_files\": %s,\n", compressed > raw ? "true" : "false");
    printf("  \"cannot_infer\": \"ratio compares the delivered archive with the files' own bytes; stream_ratio is what compression did to the tar stream. Neither is a saving on anything but this directory as it stood.\"\n");
    printf("}\n");

    fprintf(stderr, "[compress] files %lu, stream %lu, delivered %lu (stream ratio %.4f)\n", raw, stream, compressed, stream_ratio);
    return 0;
}

/* ---------- main ---------- */

static void usage(void) {
    fprintf(stderr,
        "BonfyreCompress — family-aware compression engine\n\n"
        "Usage:\n"
        "  bonfyre-compress train <dir> [--dict-out F]    Train zstd dictionary\n"
        "  bonfyre-compress pack <in> <out> [--dict D]    Compress file\n"
        "  bonfyre-compress unpack <in> <out> [--dict D]  Decompress file\n"
        "  bonfyre-compress family <dir> [--out F]        Pack entire family\n"
        "  bonfyre-compress savings <dir>                 Report savings\n"
        "  bonfyre-compress compile-passports --db D --input P.jsonl\n"
        "  bonfyre-compress plan --db D --subject-family F --operation O --loss-budget N [--requires-exact]\n"
        "  bonfyre-compress record-receipt --db D --passport P --receipt R <runtime counters>\n"
        "  bonfyre-compress ledger --db D                Report representation and copy receipts\n"
        "  bonfyre-compress ledger-history --db D        Inspect archived CopyReceipt totals\n"
        "  bonfyre-compress read-receipt --db D --generation SHA --receipt ID  Read exact CopyReceipt\n"
        "  bonfyre-compress benchmark-structural --db REGISTRY  Measure a registry that exists\n"
        "  bonfyre-compress layer <artifact_id> <layer|layer-pack> [--fpq|--fpqx] [--root DIR]\n"
    );
}

int main(int argc, char *argv[]) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    int factored = bf_record_factor_command(argc, argv);
    if (factored >= 0) return factored;
    const char *dict = NULL;
    int level = 19;
    int physical = bf_physical_command(argc, argv);
    if (physical >= 0) return physical;

    enum {
        CM_FAMILY, CM_LAYER, CM_PACK, CM_SAVINGS, CM_TRAIN, CM_UNPACK,
    };
    static const BfCommandEntry compress_commands[] = {
        {"family", CM_FAMILY, 0, NULL},
        {"layer", CM_LAYER, 0, NULL},
        {"pack", CM_PACK, 0, NULL},
        {"savings", CM_SAVINGS, 0, NULL},
        {"train", CM_TRAIN, 0, NULL},
        {"unpack", CM_UNPACK, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(compress_commands,
        sizeof(compress_commands) / sizeof(compress_commands[0]), argv[1]);

    if (command && command->id == CM_LAYER && argc >= 4) {
        return delegate_layeros_compress(argc, argv);
    }

    for (int i = 1; i < argc - 1; i++) {
        if (strcmp(argv[i], "--dict") == 0) dict = argv[i+1];
        if (strcmp(argv[i], "--level") == 0) level = atoi(argv[i+1]);
    }

    if (command && command->id == CM_TRAIN && argc >= 3) {
        const char *dict_out = "family.dict";
        for (int i = 3; i < argc - 1; i++)
            if (strcmp(argv[i], "--dict-out") == 0) dict_out = argv[i+1];
        return cmd_train(argv[2], dict_out);
    }
    if (command && command->id == CM_PACK && argc >= 4)
        return cmd_pack(argv[2], argv[3], dict, level);
    if (command && command->id == CM_UNPACK && argc >= 4)
        return cmd_unpack(argv[2], argv[3], dict);
    if (command && command->id == CM_FAMILY && argc >= 3) {
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
    if (command && command->id == CM_SAVINGS && argc >= 3)
        return cmd_savings(argv[2]);

    usage();
    return 1;
}
