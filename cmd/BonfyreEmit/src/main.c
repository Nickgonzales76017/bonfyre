/*
 * BonfyreEmit — multi-format output engine.
 *
 * Owns the last mile. Every deliverable format goes through here.
 * Markdown, HTML, PDF, EPUB, VTT, JSON, RSS, plain text, audio manifest.
 * Each emission is a billable event. Each format is a product SKU.
 *
 * Usage:
 *   bonfyre-emit <artifact-dir> --format html [--out FILE]
 *   bonfyre-emit <artifact-dir> --format pdf [--out FILE]
 *   bonfyre-emit <artifact-dir> --format epub [--out FILE]
 *   bonfyre-emit <artifact-dir> --format rss [--out FILE]
 *   bonfyre-emit <artifact-dir> --format bundle [--out DIR]  — all formats
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
#include <bonfyre.h>

static int ensure_dir(const char *path) { return bf_ensure_dir(path); }
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

static int file_exists(const char *p) { struct stat st; return stat(p, &st) == 0; }

/* Find the primary markdown file in an artifact directory */
static int find_markdown(const char *dir, char *out, size_t sz) {
    const char *candidates[] = { "brief.md", "offer.md", "outreach.md", "proof-bundle.md", NULL };
    for (int i = 0; candidates[i]; i++) {
        snprintf(out, sz, "%s/%s", dir, candidates[i]);
        if (file_exists(out)) return 1;
    }
    /* fallback: first .md via readdir (no popen) */
    DIR *d = opendir(dir);
    if (!d) return 0;
    struct dirent *ent;
    while ((ent = readdir(d))) {
        size_t nlen = strlen(ent->d_name);
        if (nlen > 3 && strcmp(ent->d_name + nlen - 3, ".md") == 0) {
            snprintf(out, sz, "%s/%s", dir, ent->d_name);
            closedir(d);
            return 1;
        }
    }
    closedir(d);
    return 0;
}

/* ---------- format emitters ---------- */

static int emit_html(const char *md_path, const char *out) {
    fprintf(stderr, "[emit] HTML: %s -> %s\n", md_path, out);
    const char *argv[] = {
        "pandoc", md_path, "-o", out, "-t", "html5",
        "--standalone", "--metadata", "title=Bonfyre Deliverable", NULL
    };
    return run_cmd(argv);
}

static int emit_pdf(const char *md_path, const char *out) {
    fprintf(stderr, "[emit] PDF: %s -> %s\n", md_path, out);
    const char *argv[] = { "pandoc", md_path, "-o", out, "--pdf-engine=wkhtmltopdf", NULL };
    int rc = run_cmd(argv);
    if (rc != 0) {
        /* fallback: try weasyprint */
        const char *argv2[] = { "pandoc", md_path, "-o", out, "--pdf-engine=weasyprint", NULL };
        rc = run_cmd(argv2);
    }
    return rc;
}

static int emit_epub(const char *md_path, const char *out) {
    fprintf(stderr, "[emit] EPUB: %s -> %s\n", md_path, out);
    const char *argv[] = {
        "pandoc", md_path, "-o", out, "-t", "epub3",
        "--metadata", "title=Bonfyre Deliverable", NULL
    };
    return run_cmd(argv);
}

static int emit_rss(const char *md_path, const char *out, const char *art_dir) {
    fprintf(stderr, "[emit] RSS: %s -> %s\n", md_path, out);
    FILE *mf = fopen(md_path, "r");
    if (!mf) return 1;

    char ts[64]; iso_timestamp(ts, sizeof(ts));

    FILE *rf = fopen(out, "w");
    if (!rf) { fclose(mf); return 1; }
    fprintf(rf,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        "<rss version=\"2.0\">\n"
        "<channel>\n"
        "  <title>Bonfyre Feed</title>\n"
        "  <link>https://bonfyre.local</link>\n"
        "  <description>Artifact deliverables</description>\n"
        "  <item>\n"
        "    <title>%s</title>\n"
        "    <pubDate>%s</pubDate>\n"
        "    <description><![CDATA[\n",
        art_dir, ts);
    char buf[8192];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), mf)) > 0) {
        fwrite(buf, 1, n, rf);
    }
    fprintf(rf, "\n]]></description>\n  </item>\n</channel>\n</rss>\n");
    fclose(mf);
    fclose(rf);
    return 0;
}

static int emit_plain(const char *md_path, const char *out) {
    fprintf(stderr, "[emit] TXT: %s -> %s\n", md_path, out);
    const char *argv[] = { "pandoc", md_path, "-o", out, "-t", "plain", "--wrap=auto", NULL };
    return run_cmd(argv);
}

/* ---------- emission manifest ---------- */

static void write_emission_manifest(const char *outdir, const char **formats, int nf) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/emission-manifest.json", outdir);
    FILE *f = fopen(path, "w");
    if (!f) return;
    char ts[64]; iso_timestamp(ts, sizeof(ts));
    fprintf(f, "{\n  \"emitted_at\": \"%s\",\n  \"source_system\": \"BonfyreEmit\",\n  \"formats\": [", ts);
    for (int i = 0; i < nf; i++)
        fprintf(f, "%s\"%s\"", i ? ", " : "", formats[i]);
    fprintf(f, "],\n  \"count\": %d\n}\n", nf);
    fclose(f);
}

/* ---------- main ---------- */

int main(int argc, char *argv[]) {
    const char *art_dir = NULL;
    const char *format = NULL;
    const char *out = NULL;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--format") == 0 && i + 1 < argc) format = argv[++i];
        else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) out = argv[++i];
        else if (!art_dir) art_dir = argv[i];
    }

    if (!art_dir || !format) {
        fprintf(stderr,
            "BonfyreEmit — multi-format output engine\n\n"
            "Usage:\n"
            "  bonfyre-emit <artifact-dir> --format html|pdf|epub|rss|txt|bundle [--out FILE]\n"
        );
        return 1;
    }

    char md_path[PATH_MAX];
    if (!find_markdown(art_dir, md_path, sizeof(md_path))) {
        fprintf(stderr, "[emit] No markdown found in %s\n", art_dir);
        return 1;
    }

    if (strcmp(format, "bundle") == 0) {
        /* Emit all formats — parallel forks for pandoc-based ones */
        char bundle_dir[PATH_MAX];
        snprintf(bundle_dir, sizeof(bundle_dir), "%s/emit", out ? out : art_dir);
        ensure_dir(bundle_dir);

        char paths[5][PATH_MAX];
        snprintf(paths[0], PATH_MAX, "%s/deliverable.html", bundle_dir);
        snprintf(paths[1], PATH_MAX, "%s/deliverable.txt", bundle_dir);
        snprintf(paths[2], PATH_MAX, "%s/deliverable.epub", bundle_dir);
        snprintf(paths[3], PATH_MAX, "%s/feed.rss", bundle_dir);
        snprintf(paths[4], PATH_MAX, "%s/deliverable.pdf", bundle_dir);

        /* RSS is in-process (no pandoc), do it inline */
        int rss_ok = (emit_rss(md_path, paths[3], art_dir) == 0);

        /* Fork pandoc for html, txt, epub, pdf concurrently */
        pid_t pids[4] = {0};
        int slots[] = {0, 1, 2, 4}; /* html, txt, epub, pdf */
        const char *pandoc_args[4][10] = {
            {"pandoc", NULL, "-o", NULL, "-t", "html5", "--standalone", "--metadata", "title=Bonfyre Deliverable", NULL},
            {"pandoc", NULL, "-o", NULL, "-t", "plain", "--wrap=auto", NULL, NULL, NULL},
            {"pandoc", NULL, "-o", NULL, "-t", "epub3", "--metadata", "title=Bonfyre Deliverable", NULL, NULL},
            {"pandoc", NULL, "-o", NULL, "--pdf-engine=wkhtmltopdf", NULL, NULL, NULL, NULL, NULL}
        };

        for (int i = 0; i < 4; i++) {
            /* Patch in md_path and output path */
            pandoc_args[i][1] = md_path;
            pandoc_args[i][3] = paths[slots[i]];
            pid_t p = fork();
            if (p == 0) {
                execvp("pandoc", (char *const *)pandoc_args[i]);
                _exit(127);
            }
            pids[i] = p;
        }

        /* Collect results */
        const char *fmts[5];
        int nf = 0;
        const char *fmt_names[] = {"html", "txt", "epub", "pdf"};
        for (int i = 0; i < 4; i++) {
            if (pids[i] > 0) {
                int st; waitpid(pids[i], &st, 0);
                if (WIFEXITED(st) && WEXITSTATUS(st) == 0)
                    fmts[nf++] = fmt_names[i];
            }
        }
        if (rss_ok) fmts[nf++] = "rss";

        write_emission_manifest(bundle_dir, fmts, nf);
        fprintf(stderr, "[emit] Bundle complete: %d formats -> %s\n", nf, bundle_dir);
        return 0;
    }

    /* Single format */
    char default_out[PATH_MAX];
    if (!out) {
        const char *ext = format;
        if (strcmp(format, "rss") == 0) ext = "rss";
        snprintf(default_out, sizeof(default_out), "%s/deliverable.%s", art_dir, ext);
        out = default_out;
    }

    int rc = 1;
    if (strcmp(format, "html") == 0) rc = emit_html(md_path, out);
    else if (strcmp(format, "pdf") == 0) rc = emit_pdf(md_path, out);
    else if (strcmp(format, "epub") == 0) rc = emit_epub(md_path, out);
    else if (strcmp(format, "rss") == 0) rc = emit_rss(md_path, out, art_dir);
    else if (strcmp(format, "txt") == 0) rc = emit_plain(md_path, out);
    else fprintf(stderr, "[emit] Unknown format: %s\n", format);

    return rc;
}
