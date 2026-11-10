/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

/*
 * BonfyreDetectObjects — Object detection operator for V1/I1 pipelines
 *
 * Layer: Vision (multi-modal pipeline stage)
 *
 * Accepts a directory of frames (jpg/png) or a single image and writes
 * per-image detection JSON plus a merged objects.json summary.
 *
 * A detector is a program that reads one image and writes that image's
 * detections. The first of these that exists is the one that runs:
 *   1. the program BONFYRE_YOLO_BINARY names
 *   2. bonfyre-detect-vision beside this binary, in ~/.bonfyre/bin, or on PATH
 * With no detector there is nothing to report, and the organ says so and
 * writes nothing. It never stands a frame in for one it could not read.
 *
 * Output schema (per image):
 *   {
 *     "source": "frame_0001.jpg",
 *     "model": "yolo-v8",
 *     "detections": [
 *       { "label": "person", "confidence": 0.92,
 *         "bbox": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.5} }
 *     ]
 *   }
 *
 * Merged output (objects.json):
 *   {
 *     "source_dir": "...",
 *     "model": "yolo-v8",
 *     "frame_count": 42,
 *     "unique_labels": ["person", "car", "laptop"],
 *     "frames": [ { "file": "...", "detections": [...] } ]
 *   }
 *
 * Usage:
 *   bonfyre-detect-objects --input <dir|image> --out <dir>
 *     [--model NAME] [--confidence 0.5]
 */

#include <dirent.h>
#include "../../../lib/libbonfyre/include/bf_help.h"
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
/* strcasecmp is POSIX and lives here, not in <string.h>. Under -std=c11 it is
 * not declared, and the call compiled as an implicit declaration until -Werror
 * turned that into the error that kept this organ out of every build. */
#include <strings.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "fragment.h"

#define MAX_FRAMES  8192
#define MAX_LABELS  256
#define MAX_PATH    4096

/* ═══════════════════════════════════════════════════════════════════
 * Types
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    char label[64];
    float confidence;
    float x, y, w, h;   /* normalised [0,1] relative to image dims */
} Detection;

typedef struct {
    char file[MAX_PATH];
    Detection dets[128];
    int ndet;
    int detected;        /* 1 = a detector ran on this frame and its output was read */
    char model[128];     /* the model the detector says it used */
} FrameResult;

/* ═══════════════════════════════════════════════════════════════════
 * Utilities
 * ═══════════════════════════════════════════════════════════════════ */

static int ensure_dir(const char *path) {
    char tmp[MAX_PATH];
    snprintf(tmp, sizeof(tmp), "%s", path);
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

static int is_image(const char *name) {
    const char *ext = strrchr(name, '.');
    if (!ext) return 0;
    return (strcasecmp(ext, ".jpg")  == 0 ||
            strcasecmp(ext, ".jpeg") == 0 ||
            strcasecmp(ext, ".png")  == 0 ||
            strcasecmp(ext, ".webp") == 0);
}

/* A file handed over on its own is judged by its first bytes, since a stored
 * artifact need not carry the name it arrived with. */
static int is_image_content(const char *path) {
    unsigned char head[12];
    FILE *file = fopen(path, "rb");
    size_t got;
    if (!file) return 0;
    got = fread(head, 1, sizeof(head), file);
    fclose(file);
    if (got >= 3 && head[0] == 0xff && head[1] == 0xd8 && head[2] == 0xff) return 1;
    if (got >= 8 && !memcmp(head, "\x89PNG\r\n\x1a\n", 8)) return 1;
    if (got >= 12 && !memcmp(head, "RIFF", 4) && !memcmp(head + 8, "WEBP", 4)) return 1;
    return 0;
}

static int cmp_str(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

/* Collect image paths from a directory (or treat input as single image) */
static int collect_images(const char *input, char **paths, int max_paths) {
    struct stat st;
    if (stat(input, &st) != 0) {
        fprintf(stderr, "error: cannot stat %s\n", input);
        return -1;
    }

    if (S_ISREG(st.st_mode)) {
        if (!is_image(input) && !is_image_content(input)) {
            fprintf(stderr, "error: %s is not a supported image format\n", input);
            return -1;
        }
        paths[0] = strdup(input);
        return 1;
    }

    if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "error: %s is neither a file nor a directory\n", input);
        return -1;
    }

    DIR *d = opendir(input);
    if (!d) { perror(input); return -1; }

    int count = 0;
    struct dirent *ent;
    while ((ent = readdir(d)) != NULL && count < max_paths) {
        if (ent->d_name[0] == '.') continue;
        if (!is_image(ent->d_name)) continue;

        char *p = malloc(MAX_PATH);
        if (!p) { closedir(d); return -1; }
        snprintf(p, MAX_PATH, "%s/%s", input, ent->d_name);
        paths[count++] = p;
    }
    closedir(d);

    /* Sort for deterministic ordering */
    qsort(paths, count, sizeof(char *), cmp_str);
    return count;
}

/* ═══════════════════════════════════════════════════════════════════
 * JSON output helpers
 * ═══════════════════════════════════════════════════════════════════ */

static void write_merged_json(const char *out_path, const char *src_dir,
                              const char *model,
                              FrameResult *frames, int nframes) {
    /* Collect unique labels */
    char labels[MAX_LABELS][64];
    int nlabels = 0;

    for (int i = 0; i < nframes; i++) {
        for (int j = 0; j < frames[i].ndet; j++) {
            const char *lbl = frames[i].dets[j].label;
            int found = 0;
            for (int k = 0; k < nlabels; k++) {
                if (strcmp(labels[k], lbl) == 0) { found = 1; break; }
            }
            if (!found && nlabels < MAX_LABELS)
                strncpy(labels[nlabels++], lbl, 63);
        }
    }

    int detected = 0;
    for (int i = 0; i < nframes; i++) detected += frames[i].detected;

    FILE *fp = fopen(out_path, "w");
    if (!fp) { perror(out_path); return; }

    fprintf(fp, "{\n");
    fprintf(fp, "  \"source_dir\": \"%s\",\n", src_dir);
    fprintf(fp, "  \"model\": \"%s\",\n", model);
    fprintf(fp, "  \"frame_count\": %d,\n", detected);
    fprintf(fp, "  \"frames_not_detected\": %d,\n", nframes - detected);
    fprintf(fp, "  \"unique_labels\": [");
    for (int i = 0; i < nlabels; i++)
        fprintf(fp, "\"%s\"%s", labels[i], (i < nlabels - 1) ? ", " : "");
    fprintf(fp, "],\n");

    fprintf(fp, "  \"frames\": [\n");
    int emitted = 0;
    for (int i = 0; i < nframes; i++) {
        FrameResult *fr = &frames[i];
        if (!fr->detected) continue;
        const char *base = strrchr(fr->file, '/');
        base = base ? base + 1 : fr->file;

        fprintf(fp, "%s    {\"file\": \"%s\", \"detections\": [", emitted++ ? ",\n" : "", base);
        for (int j = 0; j < fr->ndet; j++) {
            Detection *d = &fr->dets[j];
            fprintf(fp, "{\"label\": \"%s\", \"confidence\": %.3f}%s",
                    d->label, d->confidence,
                    (j < fr->ndet - 1) ? ", " : "");
        }
        fprintf(fp, "]}");
    }
    fprintf(fp, "%s  ]\n}\n", emitted ? "\n" : "");
    fclose(fp);
}

/* ═══════════════════════════════════════════════════════════════════
 * Detection tiers
 * ═══════════════════════════════════════════════════════════════════ */

/* Which detector runs. Returns NULL when there is none. */
static const char *resolve_detector(const char *self, char *buf, size_t n) {
    const char *named = getenv("BONFYRE_YOLO_BINARY");
    if (named && named[0]) return access(named, X_OK) == 0 ? named : NULL;

    const char *slash = self ? strrchr(self, '/') : NULL;
    if (slash) {
        snprintf(buf, n, "%.*s/bonfyre-detect-vision", (int)(slash - self), self);
        if (access(buf, X_OK) == 0) return buf;
    }
    const char *home = getenv("HOME");
    if (home) {
        snprintf(buf, n, "%s/.bonfyre/bin/bonfyre-detect-vision", home);
        if (access(buf, X_OK) == 0) return buf;
    }
    const char *path = getenv("PATH");
    while (path && *path) {
        const char *end = strchr(path, ':');
        size_t len = end ? (size_t)(end - path) : strlen(path);
        if (len > 0 && len < n - 32) {
            snprintf(buf, n, "%.*s/bonfyre-detect-vision", (int)len, path);
            if (access(buf, X_OK) == 0) return buf;
        }
        path = end ? end + 1 : NULL;
    }
    return NULL;
}

static int run_detector(const char *detector, const char *image_path,
                        const char *out_json, const char *model, float confidence) {
    char conf_str[16];
    snprintf(conf_str, sizeof(conf_str), "%.2f", confidence);

    char *argv[10];
    int n = 0;
    argv[n++] = (char *)detector;
    argv[n++] = "--image";      argv[n++] = (char *)image_path;
    argv[n++] = "--out";        argv[n++] = (char *)out_json;
    if (model) { argv[n++] = "--model"; argv[n++] = (char *)model; }
    argv[n++] = "--confidence"; argv[n++] = conf_str;
    argv[n] = NULL;

    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) { execvp(argv[0], argv); _exit(127); }

    int status = 0;
    waitpid(pid, &status, 0);
    return (WIFEXITED(status) && WEXITSTATUS(status) == 0) ? 0 : -1;
}

/* The number after "key": inside [from, to). */
static int field_number(const char *from, const char *to, const char *key, float *out) {
    char needle[32];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *at = strstr(from, needle);
    if (!at || at >= to) return 0;
    at = strchr(at + strlen(needle), ':');
    if (!at || at >= to) return 0;
    char *end = NULL;
    double v = strtod(at + 1, &end);
    if (end == at + 1) return 0;
    *out = (float)v;
    return 1;
}

/* The string after "key": starting at from; returns where it ended. */
static const char *field_string(const char *from, const char *key, char *out, size_t n) {
    char needle[32];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *at = strstr(from, needle);
    if (!at) return NULL;
    at = strchr(at + strlen(needle), ':');
    if (!at) return NULL;
    at = strchr(at, '"');
    if (!at) return NULL;
    at++;
    size_t used = 0;
    while (*at && *at != '"') {
        if (*at == '\\' && at[1]) at++;
        if (used + 1 < n) out[used++] = *at;
        at++;
    }
    out[used] = '\0';
    return *at == '"' ? at + 1 : NULL;
}

/* Read what the detector wrote for one frame. A file that is absent or has no
 * detections list was not a detection run, whatever the exit status said. */
static int load_detections(const char *json_path, FrameResult *fr) {
    FILE *fp = fopen(json_path, "rb");
    if (!fp) return -1;
    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    if (size <= 0 || size > 8 * 1024 * 1024) { fclose(fp); return -1; }
    char *doc = malloc((size_t)size + 1);
    if (!doc) { fclose(fp); return -1; }
    size_t got = fread(doc, 1, (size_t)size, fp);
    fclose(fp);
    doc[got] = '\0';

    const char *list = strstr(doc, "\"detections\"");
    if (!list) { free(doc); return -1; }
    if (!field_string(doc, "model", fr->model, sizeof(fr->model)) || !fr->model[0]) {
        free(doc); return -1;
    }

    const char *at = list;
    fr->ndet = 0;
    while (fr->ndet < (int)(sizeof(fr->dets) / sizeof(fr->dets[0]))) {
        Detection d;
        memset(&d, 0, sizeof(d));
        const char *after = field_string(at, "label", d.label, sizeof(d.label));
        if (!after) break;
        const char *close = strstr(after, "}}");
        const char *next = strstr(after, "\"label\"");
        const char *to = close ? close : (next ? next : doc + got);
        if (next && next < to) to = next;
        if (!field_number(after, to, "confidence", &d.confidence)) { free(doc); return -1; }
        field_number(after, to, "x", &d.x);
        field_number(after, to, "y", &d.y);
        field_number(after, to, "w", &d.w);
        field_number(after, to, "h", &d.h);
        fr->dets[fr->ndet++] = d;
        at = to;
    }
    free(doc);
    fr->detected = 1;
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * Main detection loop
 * ═══════════════════════════════════════════════════════════════════ */

static int run_detection(const char *self, const char *input, const char *out_dir,
                         const char *model, float confidence) {
    char detector_buf[MAX_PATH];
    const char *detector = resolve_detector(self, detector_buf, sizeof(detector_buf));
    if (!detector) {
        fprintf(stderr, "error: no detector. Set BONFYRE_YOLO_BINARY to one, or install "
                        "bonfyre-detect-vision. Nothing was written.\n");
        return 1;
    }
    if (ensure_dir(out_dir) != 0) {
        fprintf(stderr, "error: cannot create %s\n", out_dir);
        return 1;
    }

    /* Collect image list */
    char *paths[MAX_FRAMES];
    int nframes = collect_images(input, paths, MAX_FRAMES);
    if (nframes < 0) return 1;
    if (nframes == 0) {
        fprintf(stderr, "warning: no images found in %s\n", input);
        return 0;
    }

    printf("BonfyreDetectObjects\n");
    printf("  Input:  %s (%d image%s)\n", input, nframes, nframes == 1 ? "" : "s");
    printf("  Detector: %s (confidence ≥ %.2f)\n", detector, confidence);
    printf("  Output: %s\n", out_dir);

    /* Allocate results */
    FrameResult *results = calloc(nframes, sizeof(FrameResult));
    if (!results) { perror("calloc"); return 1; }

    int success = 0, failed = 0;
    const char *used_model = NULL;

    for (int i = 0; i < nframes; i++) {
        FrameResult *fr = &results[i];
        strncpy(fr->file, paths[i], sizeof(fr->file) - 1);

        char frame_out[MAX_PATH];
        const char *base = strrchr(paths[i], '/');
        base = base ? base + 1 : paths[i];
        snprintf(frame_out, sizeof(frame_out), "%s/%s.json", out_dir, base);
        /* A file left from an earlier run is not this run's detection. */
        unlink(frame_out);

        if (run_detector(detector, paths[i], frame_out, model, confidence) == 0 &&
            load_detections(frame_out, fr) == 0) {
            if ((i % 50) == 0 || i == nframes - 1)
                printf("  [%4d/%4d] %s (%d found)\n", i + 1, nframes, base, fr->ndet);
            if (!used_model) used_model = fr->model;
            success++;
            continue;
        }
        fprintf(stderr, "  not detected: %s\n", base);
        failed++;
    }

    char merged_path[MAX_PATH];
    snprintf(merged_path, sizeof(merged_path), "%s/objects.json", out_dir);
    if (success == 0) {
        unlink(merged_path);
        fprintf(stderr, "error: the detector read none of %d frames. No summary was written.\n", nframes);
        for (int i = 0; i < nframes; i++) free(paths[i]);
        free(results);
        return 1;
    }
    write_merged_json(merged_path, input, used_model, results, nframes);

    printf("  ✓ %d of %d frames detected", success, nframes);
    if (failed > 0) printf(" (%d not detected)", failed);
    printf("\n");
    printf("  ✓ Merged: %s\n", merged_path);
    model = used_model;

    /* ── Fragment emission (opt-in via BONFYRE_FRAGMENT_STORE env) ── */
    const char *frag_store_path = getenv("BONFYRE_FRAGMENT_STORE");
    if (frag_store_path) {
        bf_fragment_store_t *fstore = bf_fragment_store_open(frag_store_path);
        if (fstore) {
            int emitted = 0;
            char frag_id[BF_FRAGMENT_ID_LEN];
            char payload[512];
            for (int i = 0; i < nframes; i++) {
                FrameResult *fr = &results[i];
                /* basename of frame file for payload */
                const char *fname = strrchr(fr->file, '/');
                fname = fname ? fname + 1 : fr->file;
                for (int j = 0; j < fr->ndet; j++) {
                    Detection *d = &fr->dets[j];
                    snprintf(payload, sizeof(payload),
                        "{\"label\":\"%s\",\"frame\":\"%s\","
                        "\"bbox\":{\"x\":%.4f,\"y\":%.4f,\"w\":%.4f,\"h\":%.4f}}",
                        d->label, fname, (double)d->x, (double)d->y,
                        (double)d->w, (double)d->h);
                    bf_fragment_create(fstore, BFK_DETECTION,
                                       model, d->confidence,
                                       -1, -1,
                                       payload, NULL, 0,
                                       frag_id);
                    emitted++;
                }
            }
            bf_fragment_store_close(fstore);
            fprintf(stderr, "[fragment] emitted %d detection fragments → %s\n",
                    emitted, frag_store_path);
        } else {
            fprintf(stderr, "[fragment] warning: cannot open store %s\n", frag_store_path);
        }
    }

    for (int i = 0; i < nframes; i++) free(paths[i]);
    free(results);
    return failed == 0 ? 0 : 1;
}

/* ═══════════════════════════════════════════════════════════════════
 * CLI
 * ═══════════════════════════════════════════════════════════════════ */

static void print_usage(void) {
    fprintf(stderr,
        "bonfyre-detect-objects — what a detector finds in each image\n\n"
        "Usage:\n"
        "  bonfyre-detect-objects --input <dir|image> --out <dir>\n"
        "    [--model NAME]       Model passed to the detector (default: its own)\n"
        "    [--confidence FLOAT] Min confidence (default: 0.5)\n\n"
        "Output:\n"
        "  <out>/<frame>.jpg.json  Per-frame detections\n"
        "  <out>/objects.json      Merged detections + unique labels\n\n"
        "Detector (first that exists):\n"
        "  1. the program BONFYRE_YOLO_BINARY names\n"
        "  2. bonfyre-detect-vision beside this binary, in ~/.bonfyre/bin, or on PATH\n"
        "With no detector, or when no frame could be read, nothing is written.\n");
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { print_usage(); return 0; }
    if (argc < 2) { print_usage(); return 1; }

    const char *input      = NULL;
    const char *out_dir    = NULL;
    const char *model      = NULL;
    float       confidence = 0.5f;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--input") == 0 && i + 1 < argc)
            input = argv[++i];
        else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc)
            out_dir = argv[++i];
        else if (strcmp(argv[i], "--model") == 0 && i + 1 < argc)
            model = argv[++i];
        else if (strcmp(argv[i], "--confidence") == 0 && i + 1 < argc)
            confidence = (float)atof(argv[++i]);
        else if (strcmp(argv[i], "--batch") == 0 && i + 1 < argc)
            i++;  /* accepted and unused: frames are detected one at a time */
        else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            print_usage(); return 0;
        }
    }

    if (!input || !out_dir) {
        fprintf(stderr, "error: --input and --out are required\n\n");
        print_usage();
        return 1;
    }

    return run_detection(argv[0], input, out_dir, model, confidence);
}
