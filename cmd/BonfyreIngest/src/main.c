/*
 * BonfyreIngest — universal intake binary.
 *
 * Owns the door. Every asset that enters the system goes through here.
 * Routes by type, normalizes, stamps with intake manifest, feeds pipeline.
 *
 * Usage:
 *   bonfyre-ingest <input_path> <output_dir> [--type audio|text|image|url]
 *
 * Produces:
 *   <output_dir>/intake-manifest.json   — typed intake record
 *   <output_dir>/normalized.*           — normalized input file
 *   <output_dir>/artifact.json          — intake-stage artifact manifest
 */
#include <ctype.h>
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

#define MAX_LINE 8192
#define VERSION "1.0.0"

/* ---------- utilities (shared pattern) ---------- */

static int ensure_dir(const char *path) { return bf_ensure_dir(path); }
static void iso_timestamp(char *buf, size_t sz) {
    time_t now = time(NULL);
    struct tm t;
    gmtime_r(&now, &t);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &t);
}

static unsigned long file_size(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return 0;
    return (unsigned long)st.st_size;
}

static const char *extension(const char *path) {
    const char *dot = strrchr(path, '.');
    return dot ? dot + 1 : "";
}

static const char *basename_of(const char *path) {
    const char *s = strrchr(path, '/');
    return s ? s + 1 : path;
}

/* ---------- type detection ---------- */

typedef enum { TYPE_AUDIO, TYPE_TEXT, TYPE_IMAGE, TYPE_URL, TYPE_UNKNOWN } IngestType;

static IngestType detect_type(const char *path, const char *hint) {
    if (hint) {
        if (strcmp(hint, "audio") == 0) return TYPE_AUDIO;
        if (strcmp(hint, "text")  == 0) return TYPE_TEXT;
        if (strcmp(hint, "image") == 0) return TYPE_IMAGE;
        if (strcmp(hint, "url")   == 0) return TYPE_URL;
    }
    /* detect URLs */
    if (strncmp(path, "http://", 7) == 0 || strncmp(path, "https://", 8) == 0)
        return TYPE_URL;
    const char *ext = extension(path);
    if (strcasecmp(ext, "wav") == 0 || strcasecmp(ext, "mp3") == 0 ||
        strcasecmp(ext, "flac") == 0 || strcasecmp(ext, "m4a") == 0 ||
        strcasecmp(ext, "ogg") == 0 || strcasecmp(ext, "opus") == 0)
        return TYPE_AUDIO;
    if (strcasecmp(ext, "txt") == 0 || strcasecmp(ext, "md") == 0 ||
        strcasecmp(ext, "vtt") == 0 || strcasecmp(ext, "srt") == 0 ||
        strcasecmp(ext, "json") == 0)
        return TYPE_TEXT;
    if (strcasecmp(ext, "png") == 0 || strcasecmp(ext, "jpg") == 0 ||
        strcasecmp(ext, "jpeg") == 0 || strcasecmp(ext, "tiff") == 0 ||
        strcasecmp(ext, "bmp") == 0 || strcasecmp(ext, "pdf") == 0)
        return TYPE_IMAGE;
    return TYPE_UNKNOWN;
}

static const char *type_str(IngestType t) {
    switch (t) {
        case TYPE_AUDIO: return "audio";
        case TYPE_TEXT:  return "text";
        case TYPE_IMAGE: return "image";
        case TYPE_URL:   return "url";
        default:         return "unknown";
    }
}

static const char *media_type(IngestType t, const char *ext) {
    switch (t) {
        case TYPE_AUDIO:
            if (strcasecmp(ext, "wav") == 0)  return "audio/wav";
            if (strcasecmp(ext, "mp3") == 0)  return "audio/mpeg";
            if (strcasecmp(ext, "flac") == 0) return "audio/flac";
            return "audio/unknown";
        case TYPE_TEXT:
            if (strcasecmp(ext, "md") == 0)   return "text/markdown";
            if (strcasecmp(ext, "json") == 0) return "application/json";
            if (strcasecmp(ext, "vtt") == 0)  return "text/vtt";
            return "text/plain";
        case TYPE_IMAGE:
            if (strcasecmp(ext, "png") == 0)  return "image/png";
            if (strcasecmp(ext, "jpg") == 0 || strcasecmp(ext, "jpeg") == 0) return "image/jpeg";
            if (strcasecmp(ext, "pdf") == 0)  return "application/pdf";
            return "image/unknown";
        default: return "application/octet-stream";
    }
}

/* ---------- normalizers (fork/exec external tools) ---------- */

static int run_cmd(const char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        execvp(argv[0], (char *const *)argv);
        _exit(127);
    }
    int status;
    waitpid(pid, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

static int normalize_audio(const char *input, const char *outdir, char *out_path, size_t out_sz) {
    snprintf(out_path, out_sz, "%s/normalized.wav", outdir);
    const char *argv[] = {
        "ffmpeg", "-y", "-i", input,
        "-ac", "1", "-ar", "16000", "-sample_fmt", "s16",
        "-af", "loudnorm=I=-16:TP=-1.5:LRA=11",
        out_path, NULL
    };
    return run_cmd(argv);
}

static int normalize_text(const char *input, const char *outdir, char *out_path, size_t out_sz) {
    snprintf(out_path, out_sz, "%s/normalized.txt", outdir);
    /* Strip BOM, normalize newlines, trim trailing whitespace */
    FILE *in = fopen(input, "rb");
    if (!in) return 1;
    FILE *out = fopen(out_path, "wb");
    if (!out) { fclose(in); return 1; }
    char line[MAX_LINE];
    while (fgets(line, sizeof(line), in)) {
        /* skip BOM */
        char *p = line;
        if ((unsigned char)p[0] == 0xEF && (unsigned char)p[1] == 0xBB && (unsigned char)p[2] == 0xBF)
            p += 3;
        /* trim trailing whitespace */
        size_t len = strlen(p);
        while (len > 0 && (p[len-1] == '\r' || p[len-1] == ' ' || p[len-1] == '\t'))
            len--;
        p[len] = '\0';
        fprintf(out, "%s\n", p);
    }
    fclose(in);
    fclose(out);
    return 0;
}

static int normalize_image(const char *input, const char *outdir, char *out_path, size_t out_sz) {
    snprintf(out_path, out_sz, "%s/normalized.png", outdir);
    const char *argv[] = { "magick", input, "-strip", "-resize", "2048x2048>", out_path, NULL };
    return run_cmd(argv);
}

/* ---------- yt-dlp URL intake ---------- */

static const char *resolve_ytdlp(void) {
    const char *env = getenv("BONFYRE_YTDLP");
    if (env && env[0]) return env;
    /* common fallback paths */
    static const char *paths[] = {
        "/Users/nickgonzales/Library/Python/3.9/bin/yt-dlp",
        "/opt/homebrew/bin/yt-dlp",
        "/usr/local/bin/yt-dlp",
        NULL
    };
    for (int i = 0; paths[i]; i++) {
        if (access(paths[i], X_OK) == 0) return paths[i];
    }
    return "yt-dlp"; /* hope it's in PATH */
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

static int ingest_url(const char *url, const char *outdir, char *out_path, size_t out_sz) {
    const char *ytdlp = resolve_ytdlp();
    fprintf(stderr, "[ingest] Downloading from URL via yt-dlp: %s\n", url);

    /* Step 1: Extract audio as WAV */
    char audio_template[PATH_MAX];
    snprintf(audio_template, sizeof(audio_template), "%s/downloaded.%%(ext)s", outdir);
    snprintf(out_path, out_sz, "%s/downloaded.wav", outdir);

    const char *dl_argv[] = {
        ytdlp,
        "--no-playlist",
        "-x",                       /* extract audio */
        "--audio-format", "wav",    /* convert to WAV */
        "--audio-quality", "0",     /* best quality */
        "-o", audio_template,
        url,
        NULL
    };
    int rc = run_cmd(dl_argv);
    if (rc != 0) {
        fprintf(stderr, "[ingest] yt-dlp audio download failed (rc=%d)\n", rc);
        return rc;
    }

    /* Step 2: Extract metadata JSON */
    char meta_path[PATH_MAX];
    snprintf(meta_path, sizeof(meta_path), "%s/source-metadata.json", outdir);

    const char *meta_argv[] = {
        ytdlp,
        "--no-playlist",
        "--skip-download",
        "--write-info-json",
        "-o", meta_path,
        url,
        NULL
    };
    /* yt-dlp writes to <output>.info.json */
    run_cmd(meta_argv);  /* non-fatal if metadata fails */

    /* Rename .info.json if yt-dlp appended it */
    char info_path[PATH_MAX];
    snprintf(info_path, sizeof(info_path), "%s.info.json", meta_path);
    if (access(info_path, F_OK) == 0) {
        rename(info_path, meta_path);
    }

    /* Step 3: Extract key metadata fields for the manifest */
    char title_buf[512] = "unknown";
    char duration_buf[64] = "0";
    char uploader_buf[256] = "unknown";

    const char *title_argv[] = {
        ytdlp, "--no-playlist", "--skip-download",
        "--print", "%(title)s", url, NULL
    };
    run_cmd_capture(title_argv, title_buf, sizeof(title_buf));
    title_buf[strcspn(title_buf, "\r\n")] = '\0';

    const char *dur_argv[] = {
        ytdlp, "--no-playlist", "--skip-download",
        "--print", "%(duration)s", url, NULL
    };
    run_cmd_capture(dur_argv, duration_buf, sizeof(duration_buf));
    duration_buf[strcspn(duration_buf, "\r\n")] = '\0';

    const char *up_argv[] = {
        ytdlp, "--no-playlist", "--skip-download",
        "--print", "%(uploader)s", url, NULL
    };
    run_cmd_capture(up_argv, uploader_buf, sizeof(uploader_buf));
    uploader_buf[strcspn(uploader_buf, "\r\n")] = '\0';

    /* Write URL metadata as separate JSON */
    char url_meta_path[PATH_MAX];
    snprintf(url_meta_path, sizeof(url_meta_path), "%s/url-intake.json", outdir);
    FILE *uf = fopen(url_meta_path, "w");
    if (uf) {
        fprintf(uf,
            "{\n"
            "  \"url\": \"%s\",\n"
            "  \"title\": \"%s\",\n"
            "  \"duration_seconds\": %s,\n"
            "  \"uploader\": \"%s\",\n"
            "  \"audio_path\": \"%s\",\n"
            "  \"metadata_path\": \"%s\",\n"
            "  \"backend\": \"yt-dlp\"\n"
            "}\n",
            url, title_buf, duration_buf, uploader_buf,
            basename_of(out_path),
            access(meta_path, F_OK) == 0 ? "source-metadata.json" : "none");
        fclose(uf);
    }

    /* Step 4: Normalize the downloaded audio */
    char norm_path[PATH_MAX];
    snprintf(norm_path, sizeof(norm_path), "%s/normalized.wav", outdir);
    const char *norm_argv[] = {
        "ffmpeg", "-y", "-i", out_path,
        "-ac", "1", "-ar", "16000", "-sample_fmt", "s16",
        "-af", "loudnorm=I=-16:TP=-1.5:LRA=11",
        norm_path, NULL
    };
    if (run_cmd(norm_argv) == 0) {
        snprintf(out_path, out_sz, "%s", norm_path);
    }

    fprintf(stderr, "[ingest] URL intake complete: %s → %s\n", title_buf, basename_of(out_path));
    return 0;
}

/* ---------- SHA-256 (FIPS 180-4, inline, zero deps) ---------- */

static const unsigned int K256[64] = {
    0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
    0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
    0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
    0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
    0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
    0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
    0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
    0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
};
#define RR(x,n) (((x)>>(n))|((x)<<(32-(n))))
#define SHA_S0(x) (RR(x,2)^RR(x,13)^RR(x,22))
#define SHA_S1(x) (RR(x,6)^RR(x,11)^RR(x,25))
#define SHA_s0(x) (RR(x,7)^RR(x,18)^((x)>>3))
#define SHA_s1(x) (RR(x,17)^RR(x,19)^((x)>>10))
#define CH(e,f,g) (((e)&(f))^((~(e))&(g)))
#define MAJ(a,b,c) (((a)&(b))^((a)&(c))^((b)&(c)))

typedef struct { unsigned int h[8]; unsigned char buf[64]; unsigned long long total; } SHA256_CTX;

static void sha256_init(SHA256_CTX *c) {
    c->h[0]=0x6a09e667; c->h[1]=0xbb67ae85; c->h[2]=0x3c6ef372; c->h[3]=0xa54ff53a;
    c->h[4]=0x510e527f; c->h[5]=0x9b05688c; c->h[6]=0x1f83d9ab; c->h[7]=0x5be0cd19;
    c->total = 0;
}
static void sha256_block(SHA256_CTX *c, const unsigned char *data) {
    unsigned int w[64], st[8], t1, t2;
    for (int i = 0; i < 16; i++)
        w[i] = ((unsigned int)data[i*4]<<24)|((unsigned int)data[i*4+1]<<16)|
               ((unsigned int)data[i*4+2]<<8)|data[i*4+3];
    for (int i = 16; i < 64; i++)
        w[i] = SHA_s1(w[i-2]) + w[i-7] + SHA_s0(w[i-15]) + w[i-16];
    for (int i = 0; i < 8; i++) st[i] = c->h[i];
    for (int i = 0; i < 64; i++) {
        t1 = st[7] + SHA_S1(st[4]) + CH(st[4],st[5],st[6]) + K256[i] + w[i];
        t2 = SHA_S0(st[0]) + MAJ(st[0],st[1],st[2]);
        st[7]=st[6]; st[6]=st[5]; st[5]=st[4]; st[4]=st[3]+t1;
        st[3]=st[2]; st[2]=st[1]; st[1]=st[0]; st[0]=t1+t2;
    }
    for (int i = 0; i < 8; i++) c->h[i] += st[i];
}
static void sha256_update(SHA256_CTX *c, const unsigned char *data, size_t len) {
    size_t off = (size_t)(c->total % 64); c->total += len;
    for (size_t i = 0; i < len; i++) {
        c->buf[off++] = data[i];
        if (off == 64) { sha256_block(c, c->buf); off = 0; }
    }
}
static void sha256_final(SHA256_CTX *c, unsigned char out[32]) {
    unsigned long long bits = c->total * 8;
    size_t off = (size_t)(c->total % 64);
    c->buf[off++] = 0x80;
    if (off > 56) { while (off < 64) c->buf[off++] = 0; sha256_block(c, c->buf); off = 0; }
    while (off < 56) c->buf[off++] = 0;
    for (int i = 7; i >= 0; i--) c->buf[56+(7-i)] = (unsigned char)(bits >> (i*8));
    sha256_block(c, c->buf);
    for (int i = 0; i < 8; i++) {
        out[i*4]=(unsigned char)(c->h[i]>>24); out[i*4+1]=(unsigned char)(c->h[i]>>16);
        out[i*4+2]=(unsigned char)(c->h[i]>>8); out[i*4+3]=(unsigned char)(c->h[i]);
    }
}

static int compute_sha256(const char *path, char *hash_out, size_t hash_sz) {
    FILE *fp = fopen(path, "rb");
    if (!fp) return 1;
    SHA256_CTX ctx; sha256_init(&ctx);
    unsigned char buf[8192]; size_t n;
    while ((n = fread(buf, 1, sizeof(buf), fp)) > 0) sha256_update(&ctx, buf, n);
    fclose(fp);
    unsigned char h[32]; sha256_final(&ctx, h);
    if (hash_sz < 65) return 1;
    for (int i = 0; i < 32; i++) sprintf(hash_out + i*2, "%02x", h[i]);
    hash_out[64] = '\0';
    return 0;
}

/* ---------- main ---------- */

int main(int argc, char *argv[]) {
    const char *input = NULL;
    const char *outdir = NULL;
    const char *type_hint = NULL;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--type") == 0 && i + 1 < argc) {
            type_hint = argv[++i];
        } else if (!input) {
            input = argv[i];
        } else if (!outdir) {
            outdir = argv[i];
        }
    }
    if (!input || !outdir) {
        fprintf(stderr, "Usage: bonfyre-ingest <input> <output_dir> [--type audio|text|image|url]\n");
        return 1;
    }

    if (ensure_dir(outdir) != 0) {
        fprintf(stderr, "Cannot create output dir: %s\n", outdir);
        return 1;
    }

    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    IngestType itype = detect_type(input, type_hint);
    const char *ext = extension(input);
    unsigned long sz = file_size(input);

    fprintf(stderr, "[ingest] type=%s input=%s size=%lu\n", type_str(itype), basename_of(input), sz);

    /* ---- Normalize ---- */
    char norm_path[PATH_MAX] = {0};
    int norm_ok = 1;
    switch (itype) {
        case TYPE_AUDIO:
            norm_ok = normalize_audio(input, outdir, norm_path, sizeof(norm_path));
            break;
        case TYPE_TEXT:
            norm_ok = normalize_text(input, outdir, norm_path, sizeof(norm_path));
            break;
        case TYPE_IMAGE:
            norm_ok = normalize_image(input, outdir, norm_path, sizeof(norm_path));
            break;
        case TYPE_URL:
            norm_ok = ingest_url(input, outdir, norm_path, sizeof(norm_path));
            break;
        default:
            /* copy as-is using C I/O (no fork) */
            snprintf(norm_path, sizeof(norm_path), "%s/%s", outdir, basename_of(input));
            {
                FILE *src = fopen(input, "rb");
                FILE *dst = src ? fopen(norm_path, "wb") : NULL;
                if (src && dst) {
                    char cpbuf[8192]; size_t n;
                    while ((n = fread(cpbuf, 1, sizeof(cpbuf), src)) > 0) fwrite(cpbuf, 1, n, dst);
                    norm_ok = 0;
                } else { norm_ok = 1; }
                if (src) fclose(src);
                if (dst) fclose(dst);
            }
            break;
    }
    if (norm_ok != 0) {
        fprintf(stderr, "[ingest] WARNING: normalization returned %d, continuing with raw copy\n", norm_ok);
        snprintf(norm_path, sizeof(norm_path), "%s/%s", outdir, basename_of(input));
    }

    /* ---- Hash ---- */
    char hash[128] = "unknown";
    compute_sha256(norm_path[0] ? norm_path : input, hash, sizeof(hash));
    unsigned long norm_sz = file_size(norm_path);

    /* ---- Intake manifest ---- */
    char manifest_path[PATH_MAX];
    snprintf(manifest_path, sizeof(manifest_path), "%s/intake-manifest.json", outdir);
    FILE *mf = fopen(manifest_path, "w");
    if (!mf) { fprintf(stderr, "Cannot write manifest\n"); return 1; }
    fprintf(mf,
        "{\n"
        "  \"ingest_version\": \"%s\",\n"
        "  \"timestamp\": \"%s\",\n"
        "  \"source_file\": \"%s\",\n"
        "  \"source_bytes\": %lu,\n"
        "  \"detected_type\": \"%s\",\n"
        "  \"normalized_path\": \"%s\",\n"
        "  \"normalized_bytes\": %lu,\n"
        "  \"content_hash\": \"%s\",\n"
        "  \"media_type\": \"%s\",\n"
        "  \"status\": \"ready\"\n"
        "}\n",
        VERSION, ts, basename_of(input), sz, type_str(itype),
        basename_of(norm_path), norm_sz, hash,
        media_type(itype, ext));
    fclose(mf);

    /* ---- artifact.json ---- */
    char art_path[PATH_MAX];
    snprintf(art_path, sizeof(art_path), "%s/artifact.json", outdir);
    FILE *af = fopen(art_path, "w");
    if (!af) { fprintf(stderr, "Cannot write artifact.json\n"); return 1; }
    fprintf(af,
        "{\n"
        "  \"schema_version\": \"1.0.0\",\n"
        "  \"artifact_id\": \"ingest-%s\",\n"
        "  \"artifact_type\": \"ingest\",\n"
        "  \"created_at\": \"%s\",\n"
        "  \"source_system\": \"BonfyreIngest\",\n"
        "  \"atoms\": [\n"
        "    {\n"
        "      \"atom_id\": \"source-raw\",\n"
        "      \"content_hash\": \"%s\",\n"
        "      \"media_type\": \"%s\",\n"
        "      \"path\": \"%s\",\n"
        "      \"byte_size\": %lu,\n"
        "      \"label\": \"Raw intake asset\"\n"
        "    }\n"
        "  ],\n"
        "  \"operators\": [\n"
        "    {\n"
        "      \"operator_id\": \"op-normalize\",\n"
        "      \"op\": \"Normalize\",\n"
        "      \"inputs\": [\"source-raw\"],\n"
        "      \"output\": \"normalized\",\n"
        "      \"params\": { \"type\": \"%s\" },\n"
        "      \"version\": \"%s\",\n"
        "      \"deterministic\": true\n"
        "    }\n"
        "  ],\n"
        "  \"realizations\": [\n"
        "    {\n"
        "      \"realization_id\": \"normalized\",\n"
        "      \"media_type\": \"%s\",\n"
        "      \"path\": \"%s\",\n"
        "      \"content_hash\": \"%s\",\n"
        "      \"byte_size\": %lu,\n"
        "      \"pinned\": true,\n"
        "      \"produced_by\": \"op-normalize\",\n"
        "      \"label\": \"Normalized intake\"\n"
        "    }\n"
        "  ]\n"
        "}\n",
        hash, ts, hash, media_type(itype, ext),
        basename_of(input), sz, type_str(itype), VERSION,
        media_type(itype, ext), basename_of(norm_path), hash, norm_sz);
    fclose(af);

    fprintf(stderr, "[ingest] -> %s  hash=%s  bytes=%lu\n", basename_of(norm_path), hash, norm_sz);
    return 0;
}
