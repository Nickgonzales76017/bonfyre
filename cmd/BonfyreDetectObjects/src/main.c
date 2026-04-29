/*
 * BonfyreDetectObjects — Object detection operator for V1/I1 pipelines
 *
 * Layer: Vision (multi-modal pipeline stage)
 *
 * Accepts a directory of frames (jpg/png) or a single image and writes
 * per-image detection JSON plus a merged objects.json summary.
 *
 * Strategy (three tiers, first available wins):
 *   1. BONFYRE_YOLO_BINARY env → delegate to yolo CLI wrapper
 *   2. Python + ultralytics installed → run inline inference script
 *   3. Heuristic fallback → write stub detections + "requires_model" flag
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
 *     [--model NAME] [--confidence 0.5] [--batch N]
 */

#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
    int is_stub;         /* 1 = real model not available */
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
        if (!is_image(input)) {
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

static void write_detection_json(const char *out_dir, const FrameResult *fr,
                                 const char *model) {
    /* Derive output filename from source basename */
    const char *base = strrchr(fr->file, '/');
    base = base ? base + 1 : fr->file;

    char out_path[MAX_PATH];
    snprintf(out_path, sizeof(out_path), "%s/%s.json", out_dir, base);

    FILE *fp = fopen(out_path, "w");
    if (!fp) return;

    fprintf(fp, "{\n");
    fprintf(fp, "  \"source\": \"%s\",\n", fr->file);
    fprintf(fp, "  \"model\": \"%s\",\n", model);
    if (fr->is_stub)
        fprintf(fp, "  \"requires_model\": true,\n");
    fprintf(fp, "  \"detections\": [\n");

    for (int i = 0; i < fr->ndet; i++) {
        const Detection *d = &fr->dets[i];
        fprintf(fp, "    {\"label\": \"%s\", \"confidence\": %.3f, "
                    "\"bbox\": {\"x\": %.3f, \"y\": %.3f, \"w\": %.3f, \"h\": %.3f}}%s\n",
                d->label, d->confidence, d->x, d->y, d->w, d->h,
                (i < fr->ndet - 1) ? "," : "");
    }

    fprintf(fp, "  ]\n}\n");
    fclose(fp);
}

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

    int any_stub = 0;
    for (int i = 0; i < nframes; i++)
        if (frames[i].is_stub) { any_stub = 1; break; }

    FILE *fp = fopen(out_path, "w");
    if (!fp) { perror(out_path); return; }

    fprintf(fp, "{\n");
    fprintf(fp, "  \"source_dir\": \"%s\",\n", src_dir);
    fprintf(fp, "  \"model\": \"%s\",\n", model);
    if (any_stub) fprintf(fp, "  \"requires_model\": true,\n");
    fprintf(fp, "  \"frame_count\": %d,\n", nframes);
    fprintf(fp, "  \"unique_labels\": [");
    for (int i = 0; i < nlabels; i++)
        fprintf(fp, "\"%s\"%s", labels[i], (i < nlabels - 1) ? ", " : "");
    fprintf(fp, "],\n");

    fprintf(fp, "  \"frames\": [\n");
    for (int i = 0; i < nframes; i++) {
        FrameResult *fr = &frames[i];
        const char *base = strrchr(fr->file, '/');
        base = base ? base + 1 : fr->file;

        fprintf(fp, "    {\"file\": \"%s\", \"detections\": [", base);
        for (int j = 0; j < fr->ndet; j++) {
            Detection *d = &fr->dets[j];
            fprintf(fp, "{\"label\": \"%s\", \"confidence\": %.3f}%s",
                    d->label, d->confidence,
                    (j < fr->ndet - 1) ? ", " : "");
        }
        fprintf(fp, "]}%s\n", (i < nframes - 1) ? "," : "");
    }
    fprintf(fp, "  ]\n}\n");
    fclose(fp);
}

/* ═══════════════════════════════════════════════════════════════════
 * Detection tiers
 * ═══════════════════════════════════════════════════════════════════ */

/* Tier 1: External YOLO binary (BONFYRE_YOLO_BINARY env) */
static int detect_via_yolo_binary(const char *image_path, const char *out_json,
                                  const char *model, float confidence) {
    const char *yolo_bin = getenv("BONFYRE_YOLO_BINARY");
    if (!yolo_bin || !yolo_bin[0]) return -1;
    if (access(yolo_bin, X_OK) != 0) return -1;

    char conf_str[16];
    snprintf(conf_str, sizeof(conf_str), "%.2f", confidence);

    char *argv[] = {
        (char *)yolo_bin,
        "--image",      (char *)image_path,
        "--out",        (char *)out_json,
        "--model",      (char *)model,
        "--confidence", conf_str,
        NULL
    };

    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) { execvp(argv[0], argv); _exit(127); }

    int status = 0;
    waitpid(pid, &status, 0);
    return (WIFEXITED(status) && WEXITSTATUS(status) == 0) ? 0 : -1;
}

/* Tier 2: Python + ultralytics */
static int detect_via_python(const char *image_path, FrameResult *out,
                              const char *model, float confidence) {
    /* Check Python availability */
    if (access("/usr/bin/python3", X_OK) != 0 &&
        access("/opt/homebrew/bin/python3", X_OK) != 0 &&
        system("python3 -c 'import ultralytics' 2>/dev/null") != 0) {
        return -1;
    }

    /* Build inline Python script */
    char tmpout[MAX_PATH];
    snprintf(tmpout, sizeof(tmpout), "/tmp/bonfyre_yolo_%d.json", (int)getpid());

    char script[4096];
    snprintf(script, sizeof(script),
        "python3 -c \""
        "from ultralytics import YOLO; import json, sys\n"
        "m = YOLO('%s')\n"
        "r = m('%s', conf=%.2f, verbose=False)[0]\n"
        "dets = []\n"
        "for b in r.boxes:\n"
        "    xywhn = b.xywhn[0].tolist()\n"
        "    dets.append({'label': r.names[int(b.cls)], "
                         "'confidence': float(b.conf), "
                         "'bbox': {'x': xywhn[0], 'y': xywhn[1], "
                                  "'w': xywhn[2], 'h': xywhn[3]}})\n"
        "json.dump({'detections': dets}, open('%s', 'w'))\n"
        "\" 2>/dev/null",
        model, image_path, confidence, tmpout);

    if (system(script) != 0) return -1;

    /* Parse the output JSON */
    FILE *fp = fopen(tmpout, "r");
    if (!fp) return -1;

    char buf[65536];
    size_t n = fread(buf, 1, sizeof(buf) - 1, fp);
    fclose(fp);
    unlink(tmpout);
    buf[n] = '\0';

    /* Simple JSON parse: find each "label" + "confidence" pair */
    const char *p = buf;
    while ((p = strstr(p, "\"label\":")) != NULL) {
        p += 9;
        while (*p == ' ' || *p == '"') p++;
        Detection d = {0};
        int li = 0;
        while (*p && *p != '"' && li < 63) d.label[li++] = *p++;
        d.label[li] = '\0';

        const char *cp = strstr(p, "\"confidence\":");
        if (cp) d.confidence = (float)atof(cp + 13);

        if (out->ndet < 128) out->dets[out->ndet++] = d;
    }

    return 0;
}

/* Tier 3: Stub (no model available) */
static void detect_stub(FrameResult *out) {
    /* Write a clear placeholder so downstream stages know model is absent */
    out->is_stub = 1;
    out->ndet = 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * Main detection loop
 * ═══════════════════════════════════════════════════════════════════ */

static int run_detection(const char *input, const char *out_dir,
                         const char *model, float confidence, int batch) {
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
    printf("  Model:  %s (confidence ≥ %.2f)\n", model, confidence);
    printf("  Output: %s\n", out_dir);

    /* Allocate results */
    FrameResult *results = calloc(nframes, sizeof(FrameResult));
    if (!results) { perror("calloc"); return 1; }

    int success = 0, stubs = 0;
    (void)batch;  /* parallel batch TODO: use fork pool */

    for (int i = 0; i < nframes; i++) {
        FrameResult *fr = &results[i];
        strncpy(fr->file, paths[i], sizeof(fr->file) - 1);

        /* Try tier 1 (external binary) — writes its own JSON, we just
         * record 0 detections in memory and rely on merged JSON */
        char tier1_out[MAX_PATH];
        const char *base = strrchr(paths[i], '/');
        base = base ? base + 1 : paths[i];
        snprintf(tier1_out, sizeof(tier1_out), "%s/%s.json", out_dir, base);

        if (detect_via_yolo_binary(paths[i], tier1_out, model, confidence) == 0) {
            if ((i % 50) == 0 || i == nframes - 1)
                printf("  [%4d/%4d] %s (yolo-binary)\n", i + 1, nframes, base);
            success++;
            continue;
        }

        /* Tier 2: Python + ultralytics */
        if (detect_via_python(paths[i], fr, model, confidence) == 0) {
            if ((i % 50) == 0 || i == nframes - 1)
                printf("  [%4d/%4d] %s (%d detections)\n",
                       i + 1, nframes, base, fr->ndet);
            write_detection_json(out_dir, fr, model);
            success++;
            continue;
        }

        /* Tier 3: Stub */
        detect_stub(fr);
        write_detection_json(out_dir, fr, model);
        stubs++;
    }

    /* Write merged objects.json */
    char merged_path[MAX_PATH];
    snprintf(merged_path, sizeof(merged_path), "%s/objects.json", out_dir);
    write_merged_json(merged_path, input, model, results, nframes);

    printf("  ✓ %d frames processed", nframes);
    if (stubs > 0)
        printf(" (%d stubs — model '%s' not installed)", stubs, model);
    printf("\n");
    printf("  ✓ Merged: %s\n", merged_path);

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
                if (fr->is_stub) {
                    /* emit a single stub annotation so the store records
                     * that this frame was processed without a live model */
                    snprintf(payload, sizeof(payload),
                        "{\"frame\":\"%s\",\"requires_model\":true}", fname);
                    bf_fragment_create(fstore, BFK_ANNOTATION,
                                       model, 0.0f,
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
    return (success > 0 || stubs == nframes) ? 0 : 1;
}

/* ═══════════════════════════════════════════════════════════════════
 * CLI
 * ═══════════════════════════════════════════════════════════════════ */

static void print_usage(void) {
    fprintf(stderr,
        "bonfyre-detect-objects — Object detection (YOLOv8 / fallback)\n\n"
        "Usage:\n"
        "  bonfyre-detect-objects --input <dir|image> --out <dir>\n"
        "    [--model NAME]       Model name (default: yolo-v8)\n"
        "    [--confidence FLOAT] Min confidence (default: 0.5)\n"
        "    [--batch N]          Parallel batch size (default: 4)\n\n"
        "Output:\n"
        "  <out>/<frame>.jpg.json  Per-frame detections\n"
        "  <out>/objects.json      Merged detections + unique labels\n\n"
        "Model resolving (first available):\n"
        "  1. BONFYRE_YOLO_BINARY env → external CLI\n"
        "  2. python3 + ultralytics installed\n"
        "  3. Stub (requires_model: true) — downstream can filter\n");
}

int main(int argc, char **argv) {
    if (argc < 2) { print_usage(); return 1; }

    const char *input      = NULL;
    const char *out_dir    = NULL;
    const char *model      = "yolo-v8";
    float       confidence = 0.5f;
    int         batch      = 4;

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
            batch = atoi(argv[++i]);
        else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            print_usage(); return 0;
        }
    }

    if (!input || !out_dir) {
        fprintf(stderr, "error: --input and --out are required\n\n");
        print_usage();
        return 1;
    }

    return run_detection(input, out_dir, model, confidence, batch);
}
