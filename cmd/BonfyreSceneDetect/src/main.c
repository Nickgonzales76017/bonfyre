/*
 * bonfyre-scene-detect
 *
 * Lightweight scene boundary detection over extracted frame files.
 * This keeps Phase 4 moving without a heavyweight CV dependency.
 */

#include <ctype.h>
#include <dirent.h>
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
#include "bonfyre.h"

#define MAX_FRAMES 100000

typedef struct {
    char path[PATH_MAX];
    char name[NAME_MAX + 1];
    int frame_number;
    double timestamp_s;
} FrameFile;

typedef struct {
    int index;
    int start_frame;
    int end_frame;
    double start_s;
    double end_s;
    double duration_s;
    double trigger_score;
} SceneBoundary;

typedef struct {
    double pts_time;
    double score;
} SceneCut;

static int ensure_dir(const char *path) {
    char tmp[PATH_MAX];
    size_t len = 0;

    if (!path || !path[0]) return -1;
    snprintf(tmp, sizeof(tmp), "%s", path);
    len = strlen(tmp);
    if (len > 1 && tmp[len - 1] == '/') tmp[len - 1] = '\0';

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

static void iso_timestamp(char *buffer, size_t size) { bf_iso_timestamp(buffer, size); }

static int has_image_ext(const char *name) {
    const char *dot = strrchr(name, '.');
    if (!dot) return 0;
    dot++;
    return strcasecmp(dot, "jpg") == 0 ||
           strcasecmp(dot, "jpeg") == 0 ||
           strcasecmp(dot, "png") == 0 ||
           strcasecmp(dot, "webp") == 0;
}

static int parse_frame_number(const char *name) {
    int value = 0;
    int found = 0;
    for (const char *p = name; *p; p++) {
        if (isdigit((unsigned char)*p)) {
            value = (value * 10) + (*p - '0');
            found = 1;
        } else if (found) {
            break;
        }
    }
    return found ? value : -1;
}

static int compare_frames(const void *a, const void *b) {
    const FrameFile *fa = (const FrameFile *)a;
    const FrameFile *fb = (const FrameFile *)b;
    if (fa->frame_number != fb->frame_number) {
        return (fa->frame_number > fb->frame_number) - (fa->frame_number < fb->frame_number);
    }
    return strcmp(fa->name, fb->name);
}

static int run_process(char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return 1;
    }
    if (pid == 0) {
        execvp(argv[0], argv);
        perror(argv[0]);
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

static int load_frames(const char *dir_path, FrameFile *frames, int *out_count) {
    DIR *dir = opendir(dir_path);
    if (!dir) {
        fprintf(stderr, "Failed to open frame dir: %s\n", dir_path);
        return 1;
    }

    int count = 0;
    struct dirent *ent = NULL;
    while ((ent = readdir(dir)) != NULL) {
        if (ent->d_name[0] == '.') continue;
        if (!has_image_ext(ent->d_name)) continue;
        if (count >= MAX_FRAMES) break;

        FrameFile *frame = &frames[count++];
        snprintf(frame->name, sizeof(frame->name), "%s", ent->d_name);
        snprintf(frame->path, sizeof(frame->path), "%s/%s", dir_path, ent->d_name);
        frame->frame_number = parse_frame_number(ent->d_name);
        if (frame->frame_number < 1) frame->frame_number = count;
        frame->timestamp_s = (double)(frame->frame_number - 1);
    }

    closedir(dir);

    qsort(frames, count, sizeof(FrameFile), compare_frames);
    *out_count = count;
    return 0;
}

static int read_file_bytes(const char *path, unsigned char **data_out, size_t *size_out) {
    FILE *fp = fopen(path, "rb");
    if (!fp) return 1;

    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return 1;
    }
    long size = ftell(fp);
    if (size < 0) {
        fclose(fp);
        return 1;
    }
    rewind(fp);

    unsigned char *data = malloc((size_t)size);
    if (!data) {
        fclose(fp);
        return 1;
    }

    if (size > 0 && fread(data, 1, (size_t)size, fp) != (size_t)size) {
        free(data);
        fclose(fp);
        return 1;
    }

    fclose(fp);
    *data_out = data;
    *size_out = (size_t)size;
    return 0;
}

static int build_sequence_pattern(const FrameFile *frame, char *pattern, size_t pattern_size) {
    const char *name = frame->name;
    const char *first_digit = NULL;
    const char *last_digit = NULL;

    for (const char *p = name; *p; p++) {
        if (isdigit((unsigned char)*p)) {
            if (!first_digit) first_digit = p;
            last_digit = p;
        } else if (first_digit) {
            break;
        }
    }

    if (!first_digit || !last_digit) return 1;

    size_t prefix_len = (size_t)(first_digit - name);
    size_t digits_len = (size_t)(last_digit - first_digit + 1);
    const char *suffix = last_digit + 1;

    snprintf(pattern, pattern_size, "%.*s%%0%dd%s",
             (int)prefix_len, name, (int)digits_len, suffix);
    return 0;
}

static int parse_scene_cuts_from_metadata(const char *meta_path,
                                          SceneCut *cuts, int max_cuts) {
    FILE *fp = fopen(meta_path, "r");
    if (!fp) return -1;

    int count = 0;
    char line[512];
    double pending_pts = -1.0;

    while (fgets(line, sizeof(line), fp)) {
        double pts = 0.0;
        double score = 0.0;

        if (sscanf(line, "frame:%*d pts:%*d pts_time:%lf", &pts) == 1) {
            pending_pts = pts;
            continue;
        }
        if (sscanf(line, "lavfi.scene_score=%lf", &score) == 1 && pending_pts >= 0.0) {
            if (count < max_cuts) {
                cuts[count].pts_time = pending_pts;
                cuts[count].score = score;
                count++;
            }
            pending_pts = -1.0;
        }
    }

    fclose(fp);
    return count;
}

static int detect_scene_cuts_ffmpeg(const char *frames_dir,
                                    const FrameFile *frames,
                                    double threshold,
                                    SceneCut *cuts, int max_cuts) {
    char pattern_name[PATH_MAX];
    char input_pattern[PATH_MAX];
    char filter_expr[256];
    char meta_template[] = "/tmp/bonfyre-scene-detect-XXXXXX";
    int fd = -1;
    int rc = 1;

    if (build_sequence_pattern(&frames[0], pattern_name, sizeof(pattern_name)) != 0) {
        return -1;
    }

    snprintf(input_pattern, sizeof(input_pattern), "%s/%s", frames_dir, pattern_name);
    snprintf(filter_expr, sizeof(filter_expr),
             "select='gt(scene,%.3f)',metadata=print:file=%s", threshold, meta_template);

    fd = mkstemp(meta_template);
    if (fd < 0) return -1;
    close(fd);

    snprintf(filter_expr, sizeof(filter_expr),
             "select='gt(scene,%.3f)',metadata=print:file=%s", threshold, meta_template);

    char *const argv[] = {
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-framerate", "1",
        "-start_number", "1",
        "-i", input_pattern,
        "-vf", filter_expr,
        "-an",
        "-f", "null",
        "-",
        NULL
    };

    if (run_process(argv) == 0) {
        rc = parse_scene_cuts_from_metadata(meta_template, cuts, max_cuts);
    }

    unlink(meta_template);
    return rc;
}

static double score_frame_change(const char *path_a, const char *path_b) {
    unsigned char *a = NULL;
    unsigned char *b = NULL;
    size_t size_a = 0;
    size_t size_b = 0;

    if (read_file_bytes(path_a, &a, &size_a) != 0 ||
        read_file_bytes(path_b, &b, &size_b) != 0) {
        free(a);
        free(b);
        return 0.0;
    }

    size_t shared = size_a < size_b ? size_a : size_b;
    size_t samples = shared < 2048 ? shared : 2048;
    double diff_sum = 0.0;

    if (samples > 0) {
        for (size_t i = 0; i < samples; i++) {
            size_t idx = (i * shared) / samples;
            int delta = (int)a[idx] - (int)b[idx];
            if (delta < 0) delta = -delta;
            diff_sum += (double)delta / 255.0;
        }
        diff_sum /= (double)samples;
    }

    double size_ratio = 0.0;
    if (size_a > 0 || size_b > 0) {
        double max_size = (double)(size_a > size_b ? size_a : size_b);
        double min_size = (double)(size_a < size_b ? size_a : size_b);
        size_ratio = max_size > 0.0 ? (max_size - min_size) / max_size : 0.0;
    }

    free(a);
    free(b);

    return (diff_sum * 0.8) + (size_ratio * 0.2);
}

static int emit_boundaries(const char *out_dir,
                           const SceneBoundary *scenes, int scene_count,
                           int frame_count, double threshold) {
    char out_path[PATH_MAX];
    char ts[64];
    snprintf(out_path, sizeof(out_path), "%s/boundaries.json", out_dir);
    iso_timestamp(ts, sizeof(ts));

    FILE *fp = fopen(out_path, "w");
    if (!fp) {
        fprintf(stderr, "Failed to write %s\n", out_path);
        return 1;
    }

    fprintf(fp, "{\n");
    fprintf(fp, "  \"source_system\": \"BonfyreSceneDetect\",\n");
    fprintf(fp, "  \"created_at\": \"%s\",\n", ts);
    fprintf(fp, "  \"threshold\": %.3f,\n", threshold);
    fprintf(fp, "  \"frame_count\": %d,\n", frame_count);
    fprintf(fp, "  \"scene_count\": %d,\n", scene_count);
    fprintf(fp, "  \"boundaries\": [\n");

    for (int i = 0; i < scene_count; i++) {
        const SceneBoundary *scene = &scenes[i];
        fprintf(fp, "    {\n");
        fprintf(fp, "      \"index\": %d,\n", scene->index);
        fprintf(fp, "      \"label\": \"Scene %d\",\n", scene->index);
        fprintf(fp, "      \"type\": \"scene\",\n");
        fprintf(fp, "      \"start\": %.2f,\n", scene->start_s);
        fprintf(fp, "      \"end\": %.2f,\n", scene->end_s);
        fprintf(fp, "      \"duration\": %.2f,\n", scene->duration_s);
        fprintf(fp, "      \"frames\": [%d, %d],\n", scene->start_frame, scene->end_frame);
        fprintf(fp, "      \"trigger_score\": %.4f\n", scene->trigger_score);
        fprintf(fp, "    }%s\n", i < scene_count - 1 ? "," : "");
    }

    fprintf(fp, "  ]\n");
    fprintf(fp, "}\n");
    fclose(fp);
    return 0;
}

static void usage(void) {
    fprintf(stderr,
            "bonfyre-scene-detect\n\n"
            "Usage:\n"
            "  bonfyre-scene-detect --input <frames-dir> --out <dir> [--threshold N]\n");
}

int main(int argc, char **argv) {
    const char *input_dir = NULL;
    const char *out_dir = NULL;
    double threshold = 0.30;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--input") == 0 && i + 1 < argc) {
            input_dir = argv[++i];
        } else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) {
            out_dir = argv[++i];
        } else if (strcmp(argv[i], "--threshold") == 0 && i + 1 < argc) {
            threshold = atof(argv[++i]);
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            usage();
            return 1;
        }
    }

    if (!input_dir || !out_dir) {
        usage();
        return 1;
    }
    if (threshold < 0.0) threshold = 0.0;
    if (threshold > 1.0) threshold = 1.0;

    FrameFile *frames = calloc(MAX_FRAMES, sizeof(FrameFile));
    SceneBoundary *scenes = calloc(MAX_FRAMES, sizeof(SceneBoundary));
    if (!frames || !scenes) {
        fprintf(stderr, "Out of memory\n");
        free(frames);
        free(scenes);
        return 1;
    }

    int frame_count = 0;
    if (load_frames(input_dir, frames, &frame_count) != 0) {
        free(frames);
        free(scenes);
        return 1;
    }
    if (frame_count == 0) {
        fprintf(stderr, "No frame images found in %s\n", input_dir);
        free(frames);
        free(scenes);
        return 1;
    }
    if (ensure_dir(out_dir) != 0) {
        fprintf(stderr, "Failed to create output dir: %s\n", out_dir);
        free(frames);
        free(scenes);
        return 1;
    }

    SceneCut *cuts = calloc(MAX_FRAMES, sizeof(SceneCut));
    if (!cuts) {
        fprintf(stderr, "Out of memory\n");
        free(frames);
        free(scenes);
        return 1;
    }

    int cut_count = detect_scene_cuts_ffmpeg(input_dir, frames, threshold, cuts, MAX_FRAMES);
    if (cut_count < 0) cut_count = 0;

    int scene_count = 0;
    int scene_start = 0;
    for (int c = 0; c < cut_count; c++) {
        int cut_frame_idx = (int)llround(cuts[c].pts_time);
        if (cut_frame_idx <= scene_start || cut_frame_idx >= frame_count) continue;

        SceneBoundary *scene = &scenes[scene_count++];
        scene->index = scene_count;
        scene->start_frame = frames[scene_start].frame_number;
        scene->end_frame = frames[cut_frame_idx - 1].frame_number;
        scene->start_s = frames[scene_start].timestamp_s;
        scene->end_s = frames[cut_frame_idx - 1].timestamp_s;
        scene->duration_s = scene->end_s - scene->start_s + 1.0;
        scene->trigger_score = cuts[c].score;
        scene_start = cut_frame_idx;
    }

    if (scene_count == 0) {
        double best_score = 0.0;
        for (int i = 1; i < frame_count; i++) {
            double score = score_frame_change(frames[i - 1].path, frames[i].path);
            if (score > best_score) best_score = score;
        }
        cuts[0].score = best_score;
    }

    SceneBoundary *last = &scenes[scene_count++];
    last->index = scene_count;
    last->start_frame = frames[scene_start].frame_number;
    last->end_frame = frames[frame_count - 1].frame_number;
    last->start_s = frames[scene_start].timestamp_s;
    last->end_s = frames[frame_count - 1].timestamp_s;
    last->duration_s = last->end_s - last->start_s + 1.0;
    last->trigger_score = cut_count > 0 ? cuts[cut_count - 1].score : cuts[0].score;

    if (emit_boundaries(out_dir, scenes, scene_count, frame_count, threshold) != 0) {
        free(frames);
        free(scenes);
        free(cuts);
        return 1;
    }

    printf("Detected %d scene%s across %d frames\n",
           scene_count, scene_count == 1 ? "" : "s", frame_count);
    printf("Output: %s/boundaries.json\n", out_dir);

    free(frames);
    free(scenes);
    free(cuts);
    return 0;
}
