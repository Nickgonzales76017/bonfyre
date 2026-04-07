/*
 * BonfyreTag — instant intent/topic tagging via fastText.
 *
 * 2ms, no GPU. Classify any text into topics, intents, or custom labels.
 * Train custom models on your own transcript corpus.
 *
 * Usage:
 *   bonfyre-tag predict <model.bin> <text-file>     → tags.json
 *   bonfyre-tag batch <model.bin> <dir>             → tag all text files
 *   bonfyre-tag train <training-data> <model-out>   → train custom model
 *   bonfyre-tag detect-lang <text-file>             → language detection
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

static int run_cmd_capture(const char *const argv[], char *out, size_t out_sz) {
    int pipefd[2];
    if (pipe(pipefd) < 0) return -1;
    pid_t pid = fork();
    if (pid < 0) { close(pipefd[0]); close(pipefd[1]); return -1; }
    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        close(pipefd[1]);
        execvp(argv[0], (char *const *)argv);
        _exit(127);
    }
    close(pipefd[1]);
    size_t total = 0;
    ssize_t rd;
    while ((rd = read(pipefd[0], out + total, out_sz - total - 1)) > 0)
        total += (size_t)rd;
    close(pipefd[0]);
    out[total] = '\0';
    int st; waitpid(pid, &st, 0);
    return WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

static char *read_file_contents(const char *path, size_t *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (sz < 0) { fclose(f); return NULL; }
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(f); return NULL; }
    size_t rd = fread(buf, 1, (size_t)sz, f);
    fclose(f);
    buf[rd] = '\0';
    if (out_len) *out_len = rd;
    return buf;
}

/* ── Python wrapper for fastText ──────────────────────────────── */

static int run_python_script(const char *script) {
    char tmp_path[PATH_MAX];
    snprintf(tmp_path, sizeof(tmp_path), "/tmp/bonfyre_tag_%d.py", getpid());
    FILE *f = fopen(tmp_path, "w");
    if (!f) { perror("fopen"); return -1; }
    fputs(script, f);
    fclose(f);

    const char *python = getenv("BONFYRE_PYTHON3");
    if (!python) python = "python3";
    const char *argv[] = { python, tmp_path, NULL };
    int rc = run_cmd(argv);
    unlink(tmp_path);
    return rc;
}

/* ── commands ───────────────────────────────────────────────── */

static int cmd_predict(const char *model_path, const char *text_file,
                       const char *out_dir, int top_k) {
    ensure_dir(out_dir);

    char json_out[PATH_MAX];
    snprintf(json_out, sizeof(json_out), "%s/tags.json", out_dir);

    char script[4096];
    snprintf(script, sizeof(script),
        "import fasttext, json, sys, warnings\n"
        "warnings.filterwarnings('ignore')\n"
        "model = fasttext.load_model('%s')\n"
        "with open('%s') as f:\n"
        "    lines = [l.strip() for l in f if l.strip()]\n"
        "results = []\n"
        "for line in lines:\n"
        "    labels, scores = model.predict(line, k=%d)\n"
        "    tags = []\n"
        "    for label, score in zip(labels, scores):\n"
        "        tag = label.replace('__label__', '')\n"
        "        tags.append({'tag': tag, 'confidence': round(float(score), 4)})\n"
        "    results.append({'text': line[:200], 'tags': tags})\n"
        "output = {\n"
        "    'type': 'text-tags',\n"
        "    'model': '%s',\n"
        "    'source': '%s',\n"
        "    'count': len(results),\n"
        "    'predictions': results\n"
        "}\n"
        "with open('%s', 'w') as f:\n"
        "    json.dump(output, f, indent=2)\n"
        "print(f'{len(results)} lines tagged -> %s')\n",
        model_path, text_file, top_k,
        model_path, text_file, json_out, json_out);

    fprintf(stderr, "[tag] Predicting with %s on %s\n", model_path, text_file);
    return run_python_script(script);
}

static int cmd_batch(const char *model_path, const char *dir, const char *out_dir) {
    ensure_dir(out_dir);
    fprintf(stderr, "[tag] Batch tagging all text files in %s\n", dir);

    DIR *d = opendir(dir);
    if (!d) { perror("opendir"); return 1; }
    struct dirent *ent;
    int count = 0;
    while ((ent = readdir(d))) {
        if (ent->d_name[0] == '.') continue;
        const char *ext = strrchr(ent->d_name, '.');
        if (!ext) continue;
        if (strcmp(ext, ".txt") != 0 && strcmp(ext, ".md") != 0) continue;

        char fp[PATH_MAX], out_sub[PATH_MAX];
        snprintf(fp, sizeof(fp), "%s/%s", dir, ent->d_name);

        /* create output subdir per file */
        char basename[256];
        strncpy(basename, ent->d_name, sizeof(basename) - 1);
        basename[sizeof(basename) - 1] = '\0';
        char *dot = strrchr(basename, '.');
        if (dot) *dot = '\0';
        snprintf(out_sub, sizeof(out_sub), "%s/%s", out_dir, basename);

        cmd_predict(model_path, fp, out_sub, 3);
        count++;
    }
    closedir(d);

    fprintf(stderr, "[tag] Batch complete: %d files tagged\n", count);
    return 0;
}

static int cmd_train(const char *training_data, const char *model_out) {
    fprintf(stderr, "[tag] Training model from %s -> %s\n", training_data, model_out);

    char script[2048];
    snprintf(script, sizeof(script),
        "import fasttext, warnings\n"
        "warnings.filterwarnings('ignore')\n"
        "model = fasttext.train_supervised(\n"
        "    input='%s',\n"
        "    epoch=25,\n"
        "    lr=1.0,\n"
        "    wordNgrams=2,\n"
        "    dim=100,\n"
        "    loss='softmax'\n"
        ")\n"
        "model.save_model('%s')\n"
        "result = model.test('%s')\n"
        "print(f'Trained: {result[0]} samples, precision={result[1]:.4f}, recall={result[2]:.4f}')\n",
        training_data, model_out, training_data);

    return run_python_script(script);
}

static int cmd_detect_lang(const char *text_file, const char *out_dir) {
    ensure_dir(out_dir);
    fprintf(stderr, "[tag] Detecting language in %s\n", text_file);

    char json_out[PATH_MAX];
    snprintf(json_out, sizeof(json_out), "%s/lang.json", out_dir);

    /* fastText's lid.176.bin is the standard language ID model */
    char script[4096];
    snprintf(script, sizeof(script),
        "import fasttext, json, sys, os, warnings\n"
        "warnings.filterwarnings('ignore')\n"
        "# look for language ID model\n"
        "model_paths = [\n"
        "    os.environ.get('BONFYRE_LANGID_MODEL', ''),\n"
        "    os.path.expanduser('~/.bonfyre/models/lid.176.bin'),\n"
        "    'lid.176.bin',\n"
        "    '/tmp/lid.176.bin'\n"
        "]\n"
        "model_path = None\n"
        "for p in model_paths:\n"
        "    if p and os.path.exists(p):\n"
        "        model_path = p\n"
        "        break\n"
        "if not model_path:\n"
        "    print(json.dumps({'error': 'lid.176.bin not found. Download from fasttext.cc/docs/en/language-identification.html'}))\n"
        "    sys.exit(1)\n"
        "model = fasttext.load_model(model_path)\n"
        "with open('%s') as f:\n"
        "    text = f.read().strip().replace('\\n', ' ')[:5000]\n"
        "labels, scores = model.predict(text, k=5)\n"
        "langs = []\n"
        "for label, score in zip(labels, scores):\n"
        "    lang = label.replace('__label__', '')\n"
        "    langs.append({'language': lang, 'confidence': round(float(score), 4)})\n"
        "output = {'type': 'language-detection', 'source': '%s', 'languages': langs}\n"
        "with open('%s', 'w') as f:\n"
        "    json.dump(output, f, indent=2)\n"
        "print(json.dumps(output, indent=2))\n",
        text_file, text_file, json_out);

    return run_python_script(script);
}

/* ── main ───────────────────────────────────────────────────── */

static void print_usage(void) {
    fprintf(stderr,
        "bonfyre-tag — instant topic/intent tagging (fastText)\n\n"
        "Usage:\n"
        "  bonfyre-tag predict <model.bin> <text-file> [output-dir] [--top N]\n"
        "  bonfyre-tag batch <model.bin> <dir> [output-dir]\n"
        "  bonfyre-tag train <training-data> <model-out>\n"
        "  bonfyre-tag detect-lang <text-file> [output-dir]\n"
        "  bonfyre-tag status\n");
}

int main(int argc, char **argv) {
    if (argc >= 2 && strcmp(argv[1], "status") == 0) {
        printf("{\"binary\":\"bonfyre-tag\",\"status\":\"ok\",\"version\":\"1.0.0\"}\n");
        return 0;
    }

    if (argc < 3) { print_usage(); return 1; }

    const char *cmd = argv[1];

    if (strcmp(cmd, "predict") == 0) {
        if (argc < 4) { print_usage(); return 1; }
        const char *model = argv[2];
        const char *text = argv[3];
        const char *out = (argc > 4 && argv[4][0] != '-') ? argv[4] : "output";
        int top_k = 3;
        for (int i = 4; i < argc; i++) {
            if (strcmp(argv[i], "--top") == 0 && i + 1 < argc)
                top_k = atoi(argv[++i]);
        }
        return cmd_predict(model, text, out, top_k);
    } else if (strcmp(cmd, "batch") == 0) {
        if (argc < 4) { print_usage(); return 1; }
        return cmd_batch(argv[2], argv[3], (argc > 4) ? argv[4] : "output");
    } else if (strcmp(cmd, "train") == 0) {
        if (argc < 4) { print_usage(); return 1; }
        return cmd_train(argv[2], argv[3]);
    } else if (strcmp(cmd, "detect-lang") == 0) {
        return cmd_detect_lang(argv[2], (argc > 3) ? argv[3] : "output");
    }

    print_usage();
    return 1;
}
